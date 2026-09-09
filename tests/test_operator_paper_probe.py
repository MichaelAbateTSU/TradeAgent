import json
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest
from sqlalchemy import select

from tradeagent import event_operator_probe as probe
from tradeagent.alpaca_paper import AlpacaOrderStatus, AlpacaPaperOrder
from tradeagent.event_store import EventStore
from tradeagent.notifications import RoundTripNotificationRepository
from tradeagent.persistence import Database, ProductionRepository, events


@pytest.fixture
def setup(monkeypatch):
    monkeypatch.setattr(probe, "ACCOUNT", sha256(b"synthetic-paper").hexdigest())
    for name in ("ALPACA_LIVE_KEY", "ALPACA_LIVE_KEY_ID", "ALPACA_LIVE_SECRET_KEY"):
        monkeypatch.delenv(name, raising=False)
    now = datetime(2026, 9, 9, 4, 30, tzinfo=UTC)
    account = SimpleNamespace(
        id="synthetic-paper",
        status="ACTIVE",
        trading_blocked=False,
        account_blocked=False,
        cash=Decimal(100000),
    )
    clock = SimpleNamespace(
        timestamp=now, is_open=False, next_open=datetime(2026, 9, 9, 13, 30, tzinfo=UTC)
    )
    orders = {}
    broker = Mock()
    broker.broker_host = "https://paper-api.alpaca.markets"
    broker.account.return_value = account
    broker.clock.return_value = clock
    broker.asset.return_value = SimpleNamespace(
        symbol="AAPL", tradable=True, fractionable=True, status="active", asset_class="us_equity"
    )
    broker.positions.return_value = ()
    broker.find_order_by_client_id.side_effect = lambda key: orders.get(key)
    broker.open_orders.side_effect = lambda: [
        order for order in orders.values() if order.status is AlpacaOrderStatus.NEW
    ]

    def submit(request, limit):
        assert request.quantity * limit == Decimal(10)
        assert request.client_order_id == probe.CLIENT_ID
        value = AlpacaPaperOrder(
            id="actual-paper-id",
            client_order_id=request.client_order_id,
            symbol="AAPL",
            side="buy",
            qty=request.quantity,
            filled_qty=0,
            filled_avg_price=None,
            status=AlpacaOrderStatus.NEW,
            created_at=now,
        )
        orders[request.client_order_id] = value
        return value

    def cancel(order_id):
        assert order_id == "actual-paper-id"
        orders[probe.CLIENT_ID] = orders[probe.CLIENT_ID].model_copy(
            update={"status": AlpacaOrderStatus.CANCELED, "canceled_at": now}
        )

    broker.submit_limit_order.side_effect = submit
    broker.cancel_order.side_effect = cancel
    with Database("sqlite:///:memory:") as db:
        db.initialize()
        repo = ProductionRepository(db)
        repo.set_control("kill_switch", "active")
        command = {
            "test_id": probe.TEST_ID,
            "code_sha": "code",
            "config_hash": "config",
            "cohort_id": "demo",
            "account_digest": probe.ACCOUNT,
            "action": "submit_cancel_paper_limit",
        }
        repo.set_control(f"{probe.COMMAND_KEY}:demo", json.dumps(command))
        args = {
            "owner_id": "real-worker-owner",
            "code_sha": "code",
            "config_hash": "config",
            "cohort_id": "demo",
            "assert_owner": Mock(),
            "sleep": lambda _: None,
        }
        yield broker, repo, EventStore(db), args


def test_actual_adapter_submit_cancel_proof_preserves_pauses_and_never_claims_fill(setup):
    broker, repo, store, args = setup
    result = probe.run_probe(broker, repo, store, **args)
    assert result["state"] == "canceled_and_flat"
    assert result["order"]["id"] == "actual-paper-id"
    assert result["order"]["filled_quantity"] == "0"
    assert result["actual_filled_trade_proven"] is False
    assert result["worker_owner"] == "real-worker-owner"
    assert repo.get_control("kill_switch") == "active"
    assert broker.submit_limit_order.call_count == broker.cancel_order.call_count == 1
    probe.run_probe(broker, repo, store, **args)
    assert broker.submit_limit_order.call_count == 1
    assert RoundTripNotificationRepository(store.database).count() == 1
    with store.database.begin() as connection:
        assert len(list(connection.scalars(select(events.c.event_id)))) == 2


def test_timeout_after_acceptance_uses_original_id_and_cancels(setup):
    broker, repo, store, args = setup
    submit = broker.submit_limit_order.side_effect

    def lost_ack(request, limit):
        submit(request, limit)
        raise httpx.ReadTimeout("ack lost")

    broker.submit_limit_order.side_effect = lost_ack
    assert probe.run_probe(broker, repo, store, **args)["state"] == "canceled_and_flat"
    assert broker.submit_limit_order.call_count == 1


def test_prepared_unknown_never_resubmits(setup):
    broker, repo, store, args = setup
    repo.set_control(probe.CONTROL_KEY, json.dumps({"state": "submission_outcome_unconfirmed"}))
    result = probe.run_probe(broker, repo, store, **args)
    assert result["state"] == "not_proven"
    assert broker.submit_limit_order.call_count == 0


@pytest.mark.parametrize(
    "failure", ["host", "account", "market_open", "deadline", "pause", "command", "owner"]
)
def test_unauthorized_or_unsafe_context_never_posts(setup, failure):
    broker, repo, store, args = setup
    if failure == "host":
        broker.broker_host = "https://api.alpaca.markets"
    elif failure == "account":
        broker.account.return_value.id = "other-account"
    elif failure == "market_open":
        broker.clock.return_value.is_open = True
    elif failure == "deadline":
        broker.clock.return_value.timestamp = probe.DEADLINE
    elif failure == "pause":
        repo.set_control("kill_switch", "inactive")
    elif failure == "command":
        args["code_sha"] = "other"
    else:
        args["assert_owner"].side_effect = ValueError("lost owner")
    with pytest.raises(ValueError):
        probe.run_probe(broker, repo, store, **args)
    assert broker.submit_limit_order.call_count == 0


def test_definitive_rejection_not_mislabeled_success(setup):
    broker, repo, store, args = setup
    response = httpx.Response(422, request=httpx.Request("POST", broker.broker_host + "/v2/orders"))
    broker.submit_limit_order.side_effect = httpx.HTTPStatusError(
        "rejected", request=response.request, response=response
    )
    result = probe.run_probe(broker, repo, store, **args)
    assert result["state"] == "not_proven"
    assert result["post_result"] == "broker_rejected_http_422"
    assert result["order"] is None


def test_recovery_cancels_probe_even_if_other_activity_appears(setup):
    broker, repo, store, args = setup
    cancel = broker.cancel_order.side_effect
    broker.cancel_order.side_effect = httpx.ReadTimeout("cancel unconfirmed")
    assert probe.run_probe(broker, repo, store, **args)["state"] == "not_proven"
    own_orders = broker.open_orders.side_effect
    unrelated = SimpleNamespace(
        client_order_id="unrelated", model_dump=lambda **_: {"client_order_id": "unrelated"}
    )
    broker.open_orders.side_effect = lambda: [*own_orders(), unrelated]
    broker.cancel_order.side_effect = cancel
    broker.account.return_value.trading_blocked = True
    repo.set_control("kill_switch", "inactive")
    result = probe.run_probe(broker, repo, store, **args)
    assert result["order"]["status"] == "canceled"
    assert result["state"] == "not_proven"  # unrelated account activity is not hidden
    assert broker.submit_limit_order.call_count == 1
