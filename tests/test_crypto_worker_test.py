import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest

from tradeagent import event_crypto_test as crypto
from tradeagent.alpaca_paper import (
    AlpacaOrderStatus,
    AlpacaPaperClient,
    AlpacaPaperOrder,
    AlpacaPaperSettings,
)
from tradeagent.event_store import EventStore
from tradeagent.persistence import Database, ProductionRepository

D = Decimal
NOW = datetime(2026, 9, 9, 6, tzinfo=UTC)


class Clock(datetime):
    current = NOW

    @classmethod
    def now(cls, tz=None):
        return cls.current


@pytest.fixture
def setup(monkeypatch):
    monkeypatch.setattr(crypto, "datetime", Clock)
    monkeypatch.setattr(crypto, "ACCOUNT", sha256(b"synthetic-paper").hexdigest())
    Clock.current = NOW
    for key in ("ALPACA_LIVE_KEY", "ALPACA_LIVE_KEY_ID", "ALPACA_LIVE_SECRET_KEY"):
        monkeypatch.delenv(key, raising=False)
    orders = {}
    broker = Mock()
    broker.broker_host = "https://paper-api.alpaca.markets"
    broker.account.return_value = SimpleNamespace(
        id="synthetic-paper",
        status="ACTIVE",
        account_blocked=False,
        trading_blocked=False,
        cash=D(100000),
    )
    broker.bitcoin_asset.return_value = {
        "class": "crypto",
        "symbol": "BTC/USD",
        "tradable": True,
        "status": "active",
        "min_order_size": "0.00001",
    }
    broker.find_order_by_client_id.side_effect = lambda key: orders.get(key)
    broker.open_orders.side_effect = lambda: [
        value for value in orders.values() if value.status.value not in crypto.FINAL
    ]

    def positions():
        qty = sum(
            (o.filled_quantity * (1 if o.side == "buy" else -1) for o in orders.values()), D(0)
        )
        return (
            []
            if qty == 0
            else [
                SimpleNamespace(
                    symbol="BTCUSD",
                    quantity=qty,
                    model_dump=lambda **_: {"symbol": "BTCUSD", "quantity": str(qty)},
                )
            ]
        )

    broker.positions.side_effect = positions

    def record(client, side, qty, price):
        order = AlpacaPaperOrder(
            id=f"broker-{client}",
            client_order_id=client,
            symbol="BTC/USD",
            side=side,
            qty=qty,
            filled_qty=qty,
            filled_avg_price=price,
            status="filled",
            created_at=Clock.current,
            filled_at=Clock.current,
        )
        orders[client] = order
        return order

    broker.submit_bitcoin_test_buy.side_effect = lambda client: record(
        client, "buy", D("0.0002"), D(100000)
    )
    broker.submit_market_order.side_effect = lambda request: record(
        request.client_order_id, "sell", request.quantity, D(100100)
    )

    def cancel(order_id):
        old = next(o for o in orders.values() if o.id == order_id)
        orders[old.client_order_id] = old.model_copy(update={"status": AlpacaOrderStatus.CANCELED})

    broker.cancel_order.side_effect = cancel
    market = Mock()
    market.bid = D(100000)

    def get(path, params):
        if path == "/v1beta1/news":
            return {
                "news": [
                    {
                        "id": 1,
                        "headline": "Bitcoin market report",
                        "url": "https://example.org/btc",
                        "source": "synthetic",
                        "created_at": (Clock.current - timedelta(minutes=1)).isoformat(),
                        "updated_at": Clock.current.isoformat(),
                        "symbols": ["BTCUSD"],
                    }
                ]
            }
        return {
            "quotes": {
                "BTC/USD": {
                    "bp": str(market.bid),
                    "ap": str(market.bid + 10),
                    "bs": "2",
                    "as": "2",
                    "t": Clock.current.isoformat(),
                }
            }
        }

    market.get.side_effect = get
    with Database("sqlite:///:memory:") as db:
        db.initialize()
        repo = ProductionRepository(db)
        repo.set_control("kill_switch", "active")
        repo.set_control(
            f"{crypto.COMMAND}:test",
            json.dumps(
                {
                    "test_id": crypto.TEST_ID,
                    "account_digest": crypto.ACCOUNT,
                    "code_sha": "code",
                    "config_hash": "hash",
                    "cohort_id": "test",
                    "action": "one_btc_paper_round_trip",
                }
            ),
        )
        args = {
            "owner_id": "worker",
            "code_sha": "code",
            "config_hash": "hash",
            "cohort_id": "test",
            "assert_owner": Mock(),
        }
        yield broker, market, repo, EventStore(db), args, orders


def step(setup):
    broker, market, repo, store, args, _ = setup
    return crypto.step_crypto_test(broker, market, repo, store, **args)


def test_running_worker_fetches_news_buys_once_exits_on_timeout_and_reconciles(setup):
    broker, _, repo, _, _, _ = setup
    state = step(setup)
    assert state["buy"]["broker"]["filled_quantity"] == "0.0002"
    assert state["news"][0]["id"] == 1
    assert state["confidence"] is None
    assert broker.submit_bitcoin_test_buy.call_count == 1
    Clock.current += timedelta(seconds=30)
    assert step(setup)["state"] == "monitoring"
    assert broker.submit_bitcoin_test_buy.call_count == 1
    Clock.current += timedelta(minutes=5)
    assert step(setup)["exit_reason"] == "five_minute_time_limit"
    Clock.current += timedelta(seconds=30)
    final = step(setup)
    assert final["state"] == "completed"
    assert final["ending_positions"] == []
    assert repo.get_control("kill_switch") == "active"
    step(setup)
    assert broker.submit_market_order.call_count == 1
    assert broker.submit_bitcoin_test_buy.call_count == 1


def test_positive_estimate_exits_without_waiting_for_deadline(setup):
    step(setup)
    setup[1].bid = D(101000)
    Clock.current += timedelta(seconds=30)
    assert step(setup)["exit_reason"] == "positive_net_estimate"
    assert setup[0].submit_market_order.call_count == 1


def test_loss_limit_is_preserved(setup):
    step(setup)
    setup[1].bid = D(98000)
    Clock.current += timedelta(seconds=30)
    assert step(setup)["exit_reason"] == "one_percent_loss_limit"


def test_unknown_buy_never_resubmits_and_recovers_original_order(setup):
    broker = setup[0]
    submit = broker.submit_bitcoin_test_buy.side_effect

    def timeout(client):
        submit(client)
        raise httpx.ReadTimeout("lost ack")

    broker.submit_bitcoin_test_buy.side_effect = timeout
    assert step(setup)["buy"]["submission_status"] == "outcome_unknown"
    Clock.current += timedelta(seconds=30)
    assert step(setup)["state"] == "monitoring"
    assert broker.submit_bitcoin_test_buy.call_count == 1


def test_unknown_without_broker_order_cannot_become_another_buy(setup):
    setup[0].submit_bitcoin_test_buy.side_effect = httpx.ReadTimeout("unknown")
    step(setup)
    Clock.current += timedelta(minutes=6)
    assert step(setup)["state"] == "entry_outcome_unknown"
    assert setup[0].submit_bitcoin_test_buy.call_count == 1
    assert setup[0].submit_market_order.call_count == 0


@pytest.mark.parametrize("failure", ["live", "account", "pause", "late", "other_order", "owner"])
def test_new_crypto_entry_guards(setup, failure):
    broker, _, repo, _, args, _ = setup
    if failure == "live":
        broker.broker_host = "https://api.alpaca.markets"
    elif failure == "account":
        broker.account.return_value.id = "other"
    elif failure == "pause":
        repo.set_control("kill_switch", "inactive")
    elif failure == "late":
        Clock.current = crypto.ENTRY_DEADLINE
    elif failure == "other_order":
        broker.open_orders.side_effect = lambda: [object()]
    else:
        args["assert_owner"].side_effect = ValueError("lost lease")
    with pytest.raises(ValueError):
        step(setup)
    assert broker.submit_bitcoin_test_buy.call_count == 0


def test_final_slow_bookkeeping_does_not_submit_stale_quote(setup, monkeypatch):
    original = crypto._save

    def slow(repo, store, state, kind="crypto_test"):
        original(repo, store, state, kind)
        if kind == "crypto_test_intent":
            Clock.current += timedelta(seconds=6)

    monkeypatch.setattr(crypto, "_save", slow)
    assert step(setup)["buy"]["submission_status"] == "rejected"
    assert setup[0].submit_bitcoin_test_buy.call_count == 0


def test_exact_paper_notional_payload_cannot_exceed_twenty_dollars():
    from pydantic import SecretStr

    def handler(request):
        assert request.url.host == "paper-api.alpaca.markets"
        body = json.loads(request.content)
        assert body == {
            "symbol": "BTC/USD",
            "notional": "20",
            "side": "buy",
            "type": "market",
            "time_in_force": "gtc",
            "client_order_id": crypto.BUY_ID,
        }
        return httpx.Response(
            200,
            json={
                "id": "paper-btc",
                "client_order_id": crypto.BUY_ID,
                "symbol": "BTC/USD",
                "side": "buy",
                "qty": None,
                "notional": "20",
                "filled_qty": "0.0002",
                "filled_avg_price": "100000",
                "status": "filled",
                "created_at": NOW.isoformat(),
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        broker = AlpacaPaperClient(
            AlpacaPaperSettings(
                key_id=SecretStr("synthetic"), secret_key=SecretStr("synthetic"), _env_file=None
            ),
            client=http,
        )
        assert broker.submit_bitcoin_test_buy(crypto.BUY_ID).filled_quantity == D("0.0002")
        with pytest.raises(ValueError):
            broker.submit_bitcoin_test_buy("arbitrary-order")


@pytest.mark.parametrize("overdue", [False, True])
def test_missing_protective_quote_cannot_block_exit(setup, overdue):
    step(setup)
    Clock.current += timedelta(minutes=6) if overdue else timedelta(seconds=30)
    setup[1].get.side_effect = lambda *args: {"quotes": {}}
    result = step(setup)
    assert result["exit_reason"] == (
        "five_minute_time_limit" if overdue else "protective_quote_unavailable"
    )
    assert setup[0].submit_market_order.call_count == 1


def test_notional_order_with_null_quantity_can_be_read_and_reconciled():
    from pydantic import SecretStr

    payload = {
        "id": "paper-btc",
        "client_order_id": crypto.BUY_ID,
        "symbol": "BTC/USD",
        "side": "buy",
        "qty": None,
        "notional": "20",
        "filled_qty": "0",
        "filled_avg_price": None,
        "status": "accepted",
        "created_at": NOW.isoformat(),
    }

    def handler(request):
        return httpx.Response(200, json=[payload] if request.url.path == "/v2/orders" else payload)

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        broker = AlpacaPaperClient(
            AlpacaPaperSettings(
                key_id=SecretStr("synthetic"), secret_key=SecretStr("synthetic"), _env_file=None
            ),
            client=http,
        )
        assert broker.open_orders()[0].quantity is None
        assert broker.find_order_by_client_id(crypto.BUY_ID).notional == 20


def test_partial_buy_cancels_remainder_before_sell(setup):
    broker, _, _, _, _, orders = setup
    original = broker.submit_bitcoin_test_buy.side_effect

    def partial(client):
        order = original(client).model_copy(
            update={"status": AlpacaOrderStatus.PARTIALLY_FILLED, "filled_quantity": D("0.0001")}
        )
        orders[client] = order
        return order

    broker.submit_bitcoin_test_buy.side_effect = partial
    step(setup)
    Clock.current += timedelta(minutes=6)
    step(setup)
    assert broker.cancel_order.call_count == 1
    assert broker.submit_market_order.call_count == 0
    Clock.current += timedelta(seconds=30)
    step(setup)
    assert broker.submit_market_order.call_args.args[0].quantity == D("0.0001")


def test_partial_sell_cancels_and_exits_only_remaining_quantity(setup):
    broker, _, _, _, _, orders = setup
    step(setup)
    original = broker.submit_market_order.side_effect

    def partial_first(request):
        order = original(request)
        if broker.submit_market_order.call_count == 1:
            order = order.model_copy(
                update={
                    "status": AlpacaOrderStatus.PARTIALLY_FILLED,
                    "filled_quantity": D("0.0001"),
                }
            )
            orders[request.client_order_id] = order
        return order

    broker.submit_market_order.side_effect = partial_first
    Clock.current += timedelta(minutes=6)
    step(setup)
    Clock.current += timedelta(seconds=31)
    step(setup)
    assert broker.submit_market_order.call_count == 1
    Clock.current += timedelta(seconds=30)
    step(setup)
    assert broker.submit_market_order.call_count == 2
    assert broker.submit_market_order.call_args.args[0].quantity == D("0.0001")
    Clock.current += timedelta(seconds=30)
    assert step(setup)["state"] == "completed"


def test_null_protective_timestamp_triggers_exit_not_worker_crash(setup):
    step(setup)
    Clock.current += timedelta(seconds=30)
    setup[1].get.side_effect = lambda *args: {
        "quotes": {
            "BTC/USD": {
                "bp": 100000,
                "ap": 100010,
                "bs": 1,
                "as": 1,
                "t": None,
            }
        }
    }
    assert step(setup)["exit_reason"] == "protective_quote_unavailable"
    assert setup[0].submit_market_order.call_count == 1


def test_rejected_exit_verification_resumes_after_lookup_timeout(setup):
    broker, _, repo, _, _, _ = setup
    step(setup)
    normal_lookup = broker.find_order_by_client_id.side_effect
    normal_sell = broker.submit_market_order.side_effect

    def reject_then_timeout(request):
        broker.find_order_by_client_id.side_effect = httpx.ReadTimeout("lookup unavailable")
        response = httpx.Response(
            422, request=httpx.Request("POST", broker.broker_host + "/v2/orders")
        )
        response.raise_for_status()

    broker.submit_market_order.side_effect = reject_then_timeout
    Clock.current += timedelta(minutes=6)
    with pytest.raises(httpx.ReadTimeout):
        step(setup)
    saved = json.loads(repo.get_control(crypto.STATE))
    assert saved["sell_0"]["submission_status"] == "rejection_requires_lookup"
    assert saved["sell_0"]["http_status"] == 422
    broker.find_order_by_client_id.side_effect = normal_lookup
    broker.submit_market_order.side_effect = normal_sell
    Clock.current += timedelta(seconds=30)
    assert step(setup)["state"] == "exiting"
    assert broker.submit_market_order.call_count == 2
    Clock.current += timedelta(seconds=30)
    assert step(setup)["state"] == "completed"
