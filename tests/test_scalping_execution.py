from __future__ import annotations

import copy
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from types import SimpleNamespace

import httpx
import pytest
from sqlalchemy import func, select, update

from tradeagent.alpaca_paper import AlpacaPaperOrder, AlpacaPaperPosition, PaperCryptoAsset
from tradeagent.persistence import (
    Database,
    ProductionRepository,
    orders,
    worker_locks,
)
from tradeagent.scalping_config import ScalpingConfig, ScalpQuote, ScalpSignal
from tradeagent.scalping_execution import ScalpOrderEngine
from tradeagent.scalping_store import (
    scalping_activities,
    scalping_cycles,
    scalping_order_links,
    scalping_runs,
)

D = Decimal
NOW = datetime(2026, 9, 11, 7, 0, tzinfo=UTC)
ACCOUNT = sha256(b"v30-fixture").hexdigest()


class Broker:
    broker_host = "https://paper-api.alpaca.markets"

    def __init__(self, clock):
        self.clock = clock
        self.account_id = "v30-fixture"
        self.cash = D("100000")
        self.values = {}
        self.balances = {}
        self.activities = []
        self.posts = []
        self.cancels = []
        self.partial = D(1)
        self.base_fee = D("0.0025")
        self.cash_fee = D("0.0025")
        self.emit_fees = True
        self.timeout = False
        self.lookup_visible = True
        self.cancel_pending = False
        self.cancel_extra_fill = D(0)
        self.page_calls = []
        self.account_hook = None
        self.before_post = None
        self.market_price = D(100)
        self.non_marginable_override = None

    def account(self):
        if self.account_hook:
            self.account_hook()
        return SimpleNamespace(
            id=self.account_id,
            currency="USD",
            status="ACTIVE",
            account_blocked=False,
            trading_blocked=False,
            cash=self.cash,
            buying_power=self.cash * 4,
            non_marginable_buying_power=(
                self.cash if self.non_marginable_override is None else self.non_marginable_override
            ),
        )

    def account_history(self):
        raise AssertionError("v30 must not invoke the capped legacy account history")

    def crypto_asset(self, symbol):
        return PaperCryptoAsset(
            id=symbol,
            symbol=symbol,
            **{"class": "crypto"},
            status="active",
            tradable=True,
            min_order_size="0.0001",
            min_trade_increment="0.00000001",
            price_increment="0.01",
        )

    def positions(self):
        return tuple(
            AlpacaPaperPosition(
                symbol=symbol.replace("/", ""),
                qty=quantity,
                qty_available=quantity,
                avg_entry_price="100",
                market_value=quantity * 100,
                unrealized_pl="0",
            )
            for symbol, quantity in self.balances.items()
            if quantity != 0
        )

    def open_orders(self):
        return tuple(
            order
            for order in self.values.values()
            if order.status.value not in {"filled", "canceled", "expired", "rejected"}
        )

    def _activity(self, order, quantity, price):
        at = self.clock()
        self.activities.append(
            {
                "id": f"fill-{len(self.activities)}",
                "activity_type": "FILL",
                "order_id": order.client_order_id,
                "symbol": order.symbol,
                "side": order.side,
                "qty": str(quantity),
                "price": str(price),
                "transaction_time": at.isoformat(),
            }
        )
        if self.emit_fees:
            self.activities.append(
                {
                    "id": f"fee-{len(self.activities)}",
                    "activity_type": "CFEE",
                    "order_id": order.client_order_id,
                    "symbol": order.symbol if order.side == "buy" else "",
                    "side": order.side,
                    "qty": str(-quantity * self.base_fee) if order.side == "buy" else "0",
                    "net_amount": "0"
                    if order.side == "buy"
                    else str(-quantity * price * self.cash_fee),
                    "currency": "USD",
                    "status": "executed",
                    "created_at": at.isoformat(),
                    "date": at.date().isoformat(),
                }
            )

    def _submit(self, request, price):
        if self.before_post:
            self.before_post(request)
        quantity = request.quantity * (self.partial if request.side.value == "buy" else 1)
        status = (
            "filled" if quantity == request.quantity else "partially_filled" if quantity else "new"
        )
        order = AlpacaPaperOrder(
            id=request.client_order_id,
            client_order_id=request.client_order_id,
            symbol=request.symbol,
            side=request.side.value,
            status=status,
            qty=request.quantity,
            filled_qty=quantity,
            filled_avg_price=price if quantity else None,
            created_at=self.clock(),
            submitted_at=self.clock(),
            filled_at=self.clock() if status == "filled" else None,
        )
        self.posts.append(request)
        self.values[request.client_order_id] = order
        if quantity:
            self._apply_fill(order, quantity, price)
        if self.timeout:
            raise httpx.ReadTimeout("lost acknowledgement")
        return order

    def _apply_fill(self, order, quantity, price):
        current = self.balances.get(order.symbol, D(0))
        if order.side == "buy":
            self.balances[order.symbol] = current + quantity * (1 - self.base_fee)
            self.cash -= quantity * price
        else:
            assert quantity <= current, "cannot sell unavailable broker coins"
            self.balances[order.symbol] = current - quantity
            self.cash += quantity * price * (1 - self.cash_fee)
        self._activity(order, quantity, price)

    def submit_crypto_limit_order(self, request, limit_price, *, asset):
        assert request.quantity % asset.min_trade_increment == 0
        assert limit_price % asset.price_increment == 0
        return self._submit(request, limit_price)

    def submit_crypto_market_order(self, request, *, asset):
        assert request.quantity % asset.min_trade_increment == 0
        return self._submit(request, self.market_price)

    def find_order_by_client_id(self, client_id):
        return self.values.get(client_id) if self.lookup_visible else None

    def fill_more(self, client_id, quantity):
        current = self.values[client_id]
        self._apply_fill(current, quantity, D(100))
        self.values[client_id] = current.model_copy(
            update={
                "filled_quantity": current.filled_quantity + quantity,
                "filled_average_price": D(100),
                "updated_at": self.clock(),
            }
        )

    def cancel_order(self, broker_id):
        self.cancels.append((broker_id, self.clock()))
        if self.cancel_extra_fill:
            self.fill_more(broker_id, self.cancel_extra_fill)
            self.cancel_extra_fill = D(0)
        self.values[broker_id] = self.values[broker_id].model_copy(
            update={
                "status": type(self.values[broker_id].status)(
                    "pending_cancel" if self.cancel_pending else "canceled"
                ),
                "updated_at": self.clock(),
            }
        )

    def account_activity_page(self, *, after, until, page_token, page_size, activity_types=None):
        self.page_calls.append((after, until, page_token))
        rows = [
            copy.deepcopy(row)
            for row in self.activities
            if after
            <= datetime.fromisoformat(
                row.get("transaction_time")
                or row.get("created_at")
                or row["date"] + "T00:00:00+00:00"
            )
            <= until
            and (not activity_types or row["activity_type"] in activity_types)
        ]
        index = (
            next((i + 1 for i, row in enumerate(rows) if row["id"] == page_token), 0)
            if page_token
            else 0
        )
        page = rows[index : index + page_size]
        complete = len(page) < page_size
        return {
            "activities": page,
            "next_page_token": None if complete else page[-1]["id"],
            "complete": complete,
        }


@pytest.fixture
def setup(tmp_path):
    with Database(f"sqlite:///{tmp_path / 'scalp.db'}") as database:
        database.initialize()
        clock = [NOW]
        broker = Broker(lambda: clock[0])
        repo = ProductionRepository(database)
        repo.acquire_worker_lock("tradeagent-event-worker", "worker", observed_at=NOW)

        def make(**changes):
            config = ScalpingConfig(
                cohort_id="v30-fixture",
                account_digest=ACCOUNT,
                approved_at=NOW - timedelta(hours=1),
                symbols=("BTC/USD",),
                **changes,
            )
            engine = ScalpOrderEngine(
                database,
                broker,
                config,
                owner_id="worker",
                code_sha="a" * 40,
                clock=lambda: clock[0],
            )
            return engine

        yield database, broker, repo, clock, make


def quote(at, *, symbol="BTC/USD", bid="100", ask="100.01"):
    return ScalpQuote(
        symbol=symbol,
        exchange_at=at,
        exchange_time_ns=int(at.timestamp() * 1e9),
        received_at=at,
        bid=bid,
        ask=ask,
        bid_size="10",
        ask_size="10",
    )


def signal(at, identity, action="buy", *, symbol="BTC/USD"):
    q = quote(at, symbol=symbol)
    return ScalpSignal(
        decision_id=identity,
        symbol=symbol,
        observed_at=at,
        action=action,
        family="momentum",
        score=1,
        reasons=("fixture",),
        quote=q,
        features={},
        estimated_round_trip_cost_bps=50,
    )


def advance(setup, seconds):
    _, _, repo, clock, _ = setup
    clock[0] = NOW + timedelta(seconds=seconds)
    repo.refresh_worker_lock("tradeagent-event-worker", "worker", observed_at=clock[0])
    return clock[0]


def test_unrestricted_profile_trades_above_old_cap_and_continues_after_losses(setup):
    database, broker, repo, _clock, make = setup
    repo.set_control("kill_switch", "active")
    repo.set_control("old-cohort:pause", "R1_REVIEW_REQUIRED")
    engine = make()
    engine.initialize()
    broker.market_price = D(80)
    for index in range(4):
        now = advance(setup, index * 20)
        result = engine.step((signal(now, f"buy-{index}"),), {"BTC/USD": quote(now)}, now=now)
        assert result["state"] == "running", result
        assert broker.posts[-1].quantity * 100 == 100
        now = advance(setup, index * 20 + 15)
        engine.step((), {}, now=now)
        assert not engine.inventory(), engine.status()
    assert len([request for request in broker.posts if request.side.value == "buy"]) == 4
    assert broker.cash < 99950
    assert repo.get_control("kill_switch") == "active"
    assert repo.get_control("old-cohort:pause") == "R1_REVIEW_REQUIRED"
    with database.begin() as connection:
        assert (
            connection.scalar(
                select(func.count())
                .select_from(scalping_cycles)
                .where(scalping_cycles.c.state == "closed_owned_flat")
            )
            == 4
        )
        assert connection.scalar(select(func.count()).select_from(orders)) == 8


def test_intent_committed_before_post_and_ids_deduplicate_decisions(setup):
    database, broker, _, _, make = setup
    engine = make()
    engine.initialize()

    def verify(request):
        with database.begin() as connection:
            row = (
                connection.execute(
                    select(orders).where(orders.c.client_order_id == request.client_order_id)
                )
                .mappings()
                .one()
            )
            assert row["status"] == "reconciliation_required"
        assert len(request.client_order_id) <= 48

    broker.before_post = verify
    decision = signal(NOW, "same-decision")
    engine.step((decision, decision), {"BTC/USD": quote(NOW)}, now=NOW)
    assert len(broker.posts) == 1


def test_unknown_submission_never_reposts_and_restart_uses_original_id(setup):
    database, broker, _, _, make = setup
    engine = make()
    engine.initialize()
    broker.timeout = True
    broker.lookup_visible = False
    engine.step((signal(NOW, "unknown"),), {"BTC/USD": quote(NOW)}, now=NOW)
    original_id = broker.posts[0].client_order_id
    for seconds in (2, 4, 8):
        now = advance(setup, seconds)
        engine.step((signal(now, f"new-{seconds}"),), {"BTC/USD": quote(now)}, now=now)
    assert len(broker.posts) == 1
    restarted = make()
    restarted.initialize()
    assert len(broker.posts) == 1
    broker.timeout = False
    broker.lookup_visible = True
    now = advance(setup, 25)
    restarted.step((), {}, now=now)
    assert len([row for row in broker.posts if row.side.value == "buy"]) == 1
    assert broker.posts[0].client_order_id == original_id
    assert not restarted.inventory()
    with database.begin() as connection:
        assert connection.scalar(select(func.count()).select_from(scalping_runs)) == 1


def test_partial_cancel_race_confirms_terminal_and_exits_only_net_owned_coins(setup):
    database, broker, _, _, make = setup
    engine = make(exit_after_seconds=None)
    engine.initialize()
    broker.partial = D("0.4")
    engine.step((signal(NOW, "partial"),), {"BTC/USD": quote(NOW)}, now=NOW)
    buy_id = broker.posts[0].client_order_id
    broker.cancel_pending = True
    broker.cancel_extra_fill = D("0.2")
    now = advance(setup, 3)
    engine.step((signal(now, "sell-partial", "sell"),), {}, now=now)
    assert len(broker.posts) == 1
    assert broker.cancels
    with database.begin() as connection:
        assert connection.scalar(
            select(orders.c.filled_quantity).where(orders.c.client_order_id == buy_id)
        ) == D("0.6")
        assert connection.scalar(
            select(scalping_order_links.c.expires_at).where(
                scalping_order_links.c.client_order_id == buy_id
            )
        ).replace(tzinfo=UTC) == NOW + timedelta(seconds=3)
    broker.values[buy_id] = broker.values[buy_id].model_copy(
        update={"status": type(broker.values[buy_id].status)("canceled")}
    )
    now = advance(setup, 4)
    engine.step((), {}, now=now)
    assert len(broker.posts) == 2
    assert broker.posts[1].quantity == D("0.5985")
    assert not engine.inventory()


def test_foreign_inventory_is_preserved_and_base_fees_are_not_double_counted(setup):
    _, broker, _, _, make = setup
    broker.balances["BTC/USD"] = D(10)
    engine = make()
    engine.initialize()
    engine.step((signal(NOW, "foreign-protection"),), {"BTC/USD": quote(NOW)}, now=NOW)
    now = advance(setup, 15)
    engine.step((), {}, now=now)
    assert broker.balances["BTC/USD"] == 10
    assert broker.posts[1].quantity == D("0.9975")
    result = engine.summary()["recent_cycles"][0]
    assert result["state"] == "closed_owned_flat"
    assert result["payload"]["broker_account_flat"] is False
    assert D(result["actual_net_pnl"]).quantize(D(".00000001")) == D("-.49937500")
    assert result["payload"]["base_fee_cash_deduction_repeated"] is False


@pytest.mark.parametrize("failure", ["account", "owner", "stale_owner"])
def test_technical_identity_fences_prevent_every_post(setup, failure):
    database, broker, _, clock, make = setup
    engine = make()
    engine.initialize()
    if failure == "account":
        broker.account_id = "different-paper-account"
    elif failure == "owner":
        with database.begin() as connection:
            connection.execute(update(worker_locks).values(owner_id="replacement"))
    else:
        clock[0] += timedelta(seconds=31)
    result = engine.step((signal(clock[0], "blocked"),), {"BTC/USD": quote(clock[0])}, now=clock[0])
    assert result["state"] == "technical_recovery", result
    assert not broker.posts


def test_late_fee_activities_revalue_closed_cycle_idempotently(setup):
    database, broker, _, _, make = setup
    broker.emit_fees = False
    engine = make()
    engine.initialize()
    engine.step((signal(NOW, "late-fees"),), {"BTC/USD": quote(NOW)}, now=NOW)
    now = advance(setup, 15)
    engine.step((), {}, now=now)
    first = engine.summary()["recent_cycles"][0]
    assert first["state"] == "closed_owned_flat" and first["fees_pending"] is True
    assert first["actual_net_pnl"] is None
    for index, request in enumerate(broker.posts):
        broker.activities.append(
            {
                "id": f"late-{index}",
                "activity_type": "CFEE",
                "order_id": request.client_order_id,
                "symbol": request.symbol if request.side.value == "buy" else "",
                "qty": "-0.0025" if request.side.value == "buy" else "0",
                "net_amount": "0" if request.side.value == "buy" else "-0.249375",
                "currency": "USD",
                "status": "executed",
                "created_at": now.isoformat(),
            }
        )
    for seconds in (30, 45):
        now = advance(setup, seconds)
        engine.reconcile(now=now)
    final = engine.summary()["recent_cycles"][0]
    assert final["fees_pending"] is False
    assert D(final["actual_net_pnl"]).quantize(D(".00000001")) == D("-.49937500")
    with database.begin() as connection:
        assert connection.scalar(select(func.count()).select_from(scalping_activities)) == 4


def test_incremental_activity_cursor_passes_500_records_without_legacy_history(setup):
    database, broker, _, _, make = setup
    for index in range(650):
        broker.activities.append(
            {
                "id": f"history-{index}",
                "activity_type": "JNLC",
                "created_at": (NOW - timedelta(days=1)).isoformat(),
                "net_amount": "0",
            }
        )
    engine = make()
    engine.initialize()
    assert engine._account_state()["activity_initial_complete"] is False
    now = advance(setup, 15)
    result = engine.step((signal(now, "after-650"),), {"BTC/USD": quote(now)}, now=now)
    assert result["state"] == "running", result
    assert len(broker.posts) == 1
    with database.begin() as connection:
        assert connection.scalar(select(func.count()).select_from(scalping_activities)) == 650
    assert any(token is not None for _, _, token in broker.page_calls)


def test_none_time_exit_stays_none_and_no_fill_is_not_round_trip(setup):
    database, broker, _, _, make = setup
    engine = make(exit_after_seconds=None)
    engine.initialize()
    broker.partial = D(0)
    engine.step((signal(NOW, "no-fill"),), {"BTC/USD": quote(NOW)}, now=NOW)
    now = advance(setup, 4)
    engine.step((), {}, now=now)
    with database.begin() as connection:
        cycle = connection.execute(select(scalping_cycles)).mappings().one()
    assert cycle["state"] == "no_fill"
    assert cycle["exit_due_at"] is None
    assert len(broker.posts) == 1


def test_filled_status_never_promotes_partial_cumulative_quantity(setup):
    database, broker, _, _, make = setup
    engine = make()
    engine.initialize()
    broker.partial = D("0.4")
    original = broker.submit_crypto_limit_order

    def short_terminal(request, limit_price, *, asset):
        response = original(request, limit_price, asset=asset)
        response = response.model_copy(update={"status": type(response.status)("filled")})
        broker.values[request.client_order_id] = response
        return response

    broker.submit_crypto_limit_order = short_terminal
    engine.step((signal(NOW, "short-terminal"),), {"BTC/USD": quote(NOW)}, now=NOW)
    with database.begin() as connection:
        row = connection.execute(select(orders)).mappings().one()
    assert row["quantity"] == 1 and row["filled_quantity"] == D("0.4")
    assert engine.inventory()["BTC/USD"].quantity == D("0.399")


def test_duplicate_and_regressive_updates_do_not_double_count(setup):
    database, broker, _, _, make = setup
    engine = make()
    engine.initialize()
    engine.step((signal(NOW, "duplicate"),), {"BTC/USD": quote(NOW)}, now=NOW)
    actual = next(iter(broker.values.values()))
    engine._update_order(actual.client_order_id, actual, NOW)
    regressive = actual.model_copy(
        update={"filled_quantity": D("0.5"), "status": type(actual.status)("partially_filled")}
    )
    engine._update_order(actual.client_order_id, regressive, NOW)
    with database.begin() as connection:
        row = connection.execute(select(orders)).mappings().one()
    assert row["filled_quantity"] == 1 and row["status"] == "filled"
    assert len(broker.posts) == 1


def test_account_or_owner_flip_at_final_submission_fence_is_known_unsent(setup):
    database, broker, _, _, make = setup
    engine = make()
    engine.initialize()
    original_asset = broker.crypto_asset

    def change_owner(symbol):
        with database.begin() as connection:
            connection.execute(update(worker_locks).values(owner_id="new-owner"))
        return original_asset(symbol)

    broker.crypto_asset = change_owner
    result = engine.step((signal(NOW, "owner-race"),), {"BTC/USD": quote(NOW)}, now=NOW)
    assert result["state"] == "technical_recovery"
    assert not broker.posts


def test_unknown_blocks_other_symbol_without_legacy_risk_gate(setup):
    database, broker, _, clock, _ = setup
    config = ScalpingConfig(
        cohort_id="two-symbols", account_digest=ACCOUNT, approved_at=NOW - timedelta(minutes=1)
    )
    engine = ScalpOrderEngine(
        database, broker, config, owner_id="worker", code_sha="a" * 40, clock=lambda: clock[0]
    )
    engine.initialize()
    broker.timeout, broker.lookup_visible = True, False
    engine.step((signal(NOW, "unknown-btc"),), {"BTC/USD": quote(NOW)}, now=NOW)
    now = advance(setup, 2)
    result = engine.step(
        (signal(now, "eth-waits", symbol="ETH/USD"),),
        {"ETH/USD": quote(now, symbol="ETH/USD")},
        now=now,
    )
    assert result["unresolved_order_count"] == 1
    assert len(broker.posts) == 1


def test_retry_after_is_respected_and_rejection_is_looked_up_before_terminal(setup):
    _, broker, _, _, make = setup
    engine = make()
    engine.initialize()
    original = broker.submit_crypto_limit_order
    attempts, lookups = [], []
    original_find = broker.find_order_by_client_id

    def limited(request, limit_price, *, asset):
        attempts.append(request.client_order_id)
        if len(attempts) == 1:
            response = httpx.Response(
                429,
                headers={"Retry-After": "5"},
                text="rate limited",
                request=httpx.Request("POST", broker.broker_host + "/v2/orders"),
            )
            raise httpx.HTTPStatusError("limited", request=response.request, response=response)
        return original(request, limit_price, asset=asset)

    def find(client_id):
        lookups.append(client_id)
        return original_find(client_id)

    broker.submit_crypto_limit_order, broker.find_order_by_client_id = limited, find
    result = engine.step((signal(NOW, "limited"),), {"BTC/USD": quote(NOW)}, now=NOW)
    assert result["state"] == "broker_backoff" and lookups == []
    now = advance(setup, 4)
    engine.step((signal(now, "too-soon"),), {"BTC/USD": quote(now)}, now=now)
    assert len(attempts) == 1 and lookups == []
    now = advance(setup, 5)
    engine.step((signal(now, "after-backoff"),), {"BTC/USD": quote(now)}, now=now)
    assert attempts[0] in lookups and len(attempts) == 2
    assert len(broker.posts) == 1


def test_conflict_response_resolves_original_order_instead_of_rejecting_it(setup):
    _, broker, _, _, make = setup
    engine = make()
    engine.initialize()
    original = broker.submit_crypto_limit_order

    def conflict(request, limit_price, *, asset):
        original(request, limit_price, asset=asset)
        response = httpx.Response(
            409,
            text="duplicate client id",
            request=httpx.Request("POST", broker.broker_host + "/v2/orders"),
        )
        raise httpx.HTTPStatusError("conflict", request=response.request, response=response)

    broker.submit_crypto_limit_order = conflict
    engine.step((signal(NOW, "conflict"),), {"BTC/USD": quote(NOW)}, now=NOW)
    now = advance(setup, 2)
    engine.step((), {"BTC/USD": quote(now)}, now=now)
    assert len(broker.posts) == 1 and engine.inventory()["BTC/USD"].quantity == D(".9975")


def test_new_code_recovers_old_cycle_without_rearming_or_resetting_timer(setup):
    database, broker, _, clock, make = setup
    first = make()
    first.initialize()
    first.step((signal(NOW, "old-run"),), {"BTC/USD": quote(NOW)}, now=NOW)
    original_run = first.run_id
    now = advance(setup, 5)
    second = ScalpOrderEngine(
        database, broker, first.config, owner_id="worker", code_sha="b" * 40, clock=lambda: clock[0]
    )
    second.initialize()
    assert second.run_id != original_run
    now = advance(setup, 15)
    second.step((), {}, now=now)
    assert len(broker.posts) == 2 and not second.inventory()
    with database.begin() as connection:
        assert connection.scalar(select(func.count()).select_from(scalping_runs)) == 2
        assert connection.scalar(select(scalping_cycles.c.exit_due_at)).replace(
            tzinfo=UTC
        ) == NOW + timedelta(seconds=15)


def test_late_fees_do_not_reopen_closed_inventory_or_consume_new_cycle_coins(setup):
    _, broker, _, _, make = setup
    broker.emit_fees = False
    engine = make()
    engine.initialize()
    engine.step((signal(NOW, "first"),), {"BTC/USD": quote(NOW)}, now=NOW)
    now = advance(setup, 15)
    engine.step((), {}, now=now)
    old_orders = list(broker.posts)
    now = advance(setup, 20)
    engine.step((signal(now, "second"),), {"BTC/USD": quote(now)}, now=now)
    for request in old_orders:
        broker.activities.append(
            {
                "id": f"late-owned-{request.side.value}",
                "activity_type": "CFEE",
                "order_id": request.client_order_id,
                "symbol": request.symbol if request.side.value == "buy" else "",
                "qty": "-.0025" if request.side.value == "buy" else "0",
                "net_amount": "0" if request.side.value == "buy" else "-.249375",
                "currency": "USD",
                "status": "executed",
                "created_at": now.isoformat(),
            }
        )
    now = advance(setup, 30)
    engine.reconcile(now=now)
    assert engine.inventory()["BTC/USD"].quantity == D(".9975")
    assert engine.status()["cycle_counts"]["closed_owned_flat"] == 1
    assert engine.status()["cycle_counts"]["open"] == 1


def test_actual_broker_net_quantity_is_not_reduced_by_a_second_fee_reserve(setup):
    _, broker, _, _, make = setup
    broker.base_fee = D(".0015")
    engine = make()
    engine.initialize()
    engine.step((signal(NOW, "maker"),), {"BTC/USD": quote(NOW)}, now=NOW)
    now = advance(setup, 15)
    engine.step((), {}, now=now)
    assert not engine.inventory() and len(broker.posts) == 2
    assert broker.posts[-1].quantity == D(".9985")


def test_unkeyed_base_fee_is_corroborated_without_claiming_confirmed_net_profit(setup):
    _, broker, _, _, make = setup
    broker.base_fee = D(".0015")
    engine = make()
    engine.initialize()
    engine.step((signal(NOW, "unkeyed"),), {"BTC/USD": quote(NOW)}, now=NOW)
    for raw in broker.activities:
        if raw["activity_type"] == "CFEE":
            raw.pop("order_id")
    now = advance(setup, 15)
    engine.step((), {}, now=now)
    now = advance(setup, 16)
    engine.step((), {}, now=now)
    assert not engine.inventory()
    cycle = engine.summary()["recent_cycles"][0]
    assert cycle["fees_pending"] is True and cycle["actual_net_pnl"] is None
    assert D(cycle["payload"]["inferred_account_base_fee_allocation"]) == D(".0015")


def test_price_drop_does_not_apply_legacy_stop_or_exit_when_timer_disabled(setup):
    _, broker, _, _, make = setup
    engine = make(exit_after_seconds=None)
    engine.initialize()
    engine.step((signal(NOW, "no-legacy-stop"),), {"BTC/USD": quote(NOW)}, now=NOW)
    now = advance(setup, 60)
    engine.step((), {"BTC/USD": quote(now, bid="50", ask="51")}, now=now)
    assert len(broker.posts) == 1 and engine.inventory()


def test_known_open_inventory_does_not_impose_an_exposure_or_trade_count_cap(setup):
    _, broker, _, _, make = setup
    engine = make(exit_after_seconds=None)
    engine.initialize()
    for seconds in range(4):
        now = advance(setup, seconds)
        result = engine.step((signal(now, f"add-{seconds}"),), {"BTC/USD": quote(now)}, now=now)
        assert result["state"] == "running"
    assert len(broker.posts) == 4
    assert engine.inventory()["BTC/USD"].quantity == D("3.99")


@pytest.mark.parametrize("flip", ["account", "owner"])
def test_final_post_fence_rechecks_after_durable_dispatch_marker(setup, flip):
    database, broker, _, _, make = setup
    engine = make()
    engine.initialize()

    def hook():
        with database.begin() as connection:
            pending = connection.scalar(
                select(scalping_order_links.c.client_order_id).where(
                    scalping_order_links.c.dispatch_state == "dispatching"
                )
            )
            if pending:
                if flip == "owner":
                    connection.execute(update(worker_locks).values(owner_id="replacement"))
                else:
                    broker.account_id = "replacement-account"

    broker.account_hook = hook
    result = engine.step((signal(NOW, "final-fence"),), {"BTC/USD": quote(NOW)}, now=NOW)
    assert result["state"] == "technical_recovery"
    assert not broker.posts
    with database.begin() as connection:
        assert connection.scalar(select(scalping_order_links.c.dispatch_state)) == "expired_unsent"
        assert connection.scalar(select(orders.c.filled_quantity)) == 0


def test_decision_identity_is_deduplicated_across_code_runs(setup):
    database, broker, _, clock, make = setup
    first = make(exit_after_seconds=None)
    first.initialize()
    first.step((signal(NOW, "global-decision"),), {"BTC/USD": quote(NOW)}, now=NOW)
    now = advance(setup, 2)
    second = ScalpOrderEngine(
        database, broker, first.config, owner_id="worker", code_sha="b" * 40, clock=lambda: clock[0]
    )
    second.initialize()
    second.step((signal(now, "global-decision"),), {"BTC/USD": quote(now)}, now=now)
    assert len(broker.posts) == 1
    with database.begin() as connection:
        assert connection.scalar(select(func.count()).select_from(scalping_cycles)) == 1


def test_missing_quote_does_not_prevent_manual_or_time_exit(setup):
    _, broker, repo, _, make = setup
    engine = make(exit_after_seconds=None)
    engine.initialize()
    engine.step((signal(NOW, "manual-exit"),), {"BTC/USD": quote(NOW)}, now=NOW)
    repo.set_control(engine.manual_stop_key, "active")
    now = advance(setup, 2)
    engine.step((), {}, now=now)
    assert len(broker.posts) == 2 and not engine.inventory()
    assert repo.get_control("kill_switch") is None


def test_new_fill_activity_reopens_and_recovers_an_earlier_no_fill_cycle(setup):
    _, broker, _, _, make = setup
    engine = make()
    engine.initialize()
    broker.partial = D(0)
    engine.step((signal(NOW, "late-fill"),), {"BTC/USD": quote(NOW)}, now=NOW)
    buy = broker.posts[0]
    now = advance(setup, 4)
    engine.step((), {}, now=now)
    assert engine.status()["cycle_counts"]["no_fill"] == 1
    now = advance(setup, 15)
    broker.fill_more(buy.client_order_id, D(".2"))
    engine.reconcile(now=now)
    assert engine.inventory()["BTC/USD"].quantity == D(".1995")
    assert engine.status()["cycle_counts"].get("no_fill", 0) == 0
    now = advance(setup, 30)
    engine.step((), {}, now=now)
    assert len([order for order in broker.posts if order.side.value == "buy"]) == 1
    assert not engine.inventory()


def test_fee_sweep_recovers_backdated_fees_outside_incremental_overlap(setup):
    _, broker, _, _, make = setup
    broker.emit_fees = False
    engine = make()
    engine.initialize()
    engine.step((signal(NOW, "backdated"),), {"BTC/USD": quote(NOW)}, now=NOW)
    now = advance(setup, 15)
    engine.step((), {}, now=now)
    for seconds in (180, 240):
        now = advance(setup, seconds)
        engine.reconcile(now=now)
    for request in broker.posts:
        broker.activities.append(
            {
                "id": "backdated-fee-" + request.side.value,
                "activity_type": "CFEE",
                "order_id": request.client_order_id,
                "symbol": request.symbol if request.side.value == "buy" else "",
                "qty": "-.0025" if request.side.value == "buy" else "0",
                "net_amount": "0" if request.side.value == "buy" else "-.249375",
                "status": "executed",
                "currency": "USD",
                "created_at": NOW.isoformat(),
            }
        )
    now = advance(setup, 300)
    engine.reconcile(now=now)
    cycle = engine.summary()["recent_cycles"][0]
    assert cycle["fees_pending"] is False
    assert D(cycle["actual_net_pnl"]).quantize(D(".00000001")) == D("-.499375")


def test_restart_respects_persisted_server_cooldown_before_broker_reads(setup):
    _, broker, _, _, make = setup
    engine = make()
    engine.initialize()
    engine._save_account_state({"retry_after": (NOW + timedelta(seconds=30)).isoformat()})

    def forbidden():
        raise AssertionError("broker reads must wait for persisted retry-after")

    broker.account = forbidden
    restarted = make()
    restarted.initialize()
    assert restarted.status()["state"] == "broker_backoff"
    assert not broker.posts


def test_external_order_filled_before_first_owned_trade_is_preserved(setup):
    from tradeagent.domain import OrderRequest, OrderType, Side

    _, broker, _, _, make = setup
    broker.partial = D(0)
    manual = OrderRequest(
        client_order_id="manual-before-v30",
        decision_id="manual",
        strategy_id="manual",
        symbol="BTC/USD",
        side=Side.BUY,
        quantity=D(5),
        order_type=OrderType.LIMIT,
        submitted_at=NOW,
    )
    broker._submit(manual, D(100))
    engine = make()
    engine.initialize()
    assert engine._account_state()["initial_unowned_open_orders"] == [manual.client_order_id]
    engine.step((signal(NOW, "wait-external"),), {"BTC/USD": quote(NOW)}, now=NOW)
    assert len(broker.posts) == 1
    now = advance(setup, 15)
    broker.fill_more(manual.client_order_id, D(5))
    broker.values[manual.client_order_id] = broker.values[manual.client_order_id].model_copy(
        update={"status": type(broker.values[manual.client_order_id].status)("filled")}
    )
    broker.partial = D(1)
    engine.step((signal(now, "own-after-external"),), {"BTC/USD": quote(now)}, now=now)
    assert len(broker.posts) == 2
    now = advance(setup, 30)
    engine.step((), {}, now=now)
    assert broker.balances["BTC/USD"] == D("4.9875")
    assert engine._account_state()["initial_unowned_positions"] == {}
    assert D(engine._account_state()["unowned_positions"]["BTC/USD"]) == D("4.9875")


def test_date_only_non_trade_activity_is_retained_without_faking_an_exact_time(setup):
    database, broker, _, _, make = setup
    broker.activities.append(
        {
            "id": "dated-journal",
            "activity_type": "JNLC",
            "date": NOW.date().isoformat(),
            "net_amount": "0",
        }
    )
    engine = make()
    engine.initialize()
    with database.begin() as connection:
        row = connection.execute(select(scalping_activities)).mappings().one()
    assert row["time_precision"] == "date"
    assert row["payload"] == broker.activities[0]


@pytest.mark.parametrize(
    "cash,non_marginable,expected_notional",
    [
        ("50", None, "50"),
        ("200", "30", "30"),
        ("200", "800", "100"),
    ],
)
def test_crypto_sizing_uses_cash_not_leveraged_buying_power(
    setup, cash, non_marginable, expected_notional
):
    _, broker, _, _, make = setup
    broker.cash = D(cash)
    broker.non_marginable_override = D(non_marginable) if non_marginable else None
    engine = make()
    engine.initialize()
    result = engine.step((signal(NOW, "cash-only"),), {"BTC/USD": quote(NOW)}, now=NOW)
    assert result["state"] == "running", result
    assert len(broker.posts) == 1
    assert broker.posts[0].quantity * 100 == D(expected_notional)


def test_pending_buys_reserve_cash_without_double_subtracting_nonmarginable_power(setup):
    database, broker, _, clock, _ = setup
    broker.cash = D(150)
    config = ScalpingConfig(
        cohort_id="cash-reservations",
        account_digest=ACCOUNT,
        approved_at=NOW - timedelta(minutes=1),
    )
    engine = ScalpOrderEngine(
        database, broker, config, owner_id="worker", code_sha="a" * 40, clock=lambda: clock[0]
    )
    engine.initialize()
    broker.partial = D(0)
    engine.step((signal(NOW, "resting-btc"),), {"BTC/USD": quote(NOW)}, now=NOW)
    assert broker.posts[0].quantity * 100 == 100
    broker.partial = D(1)
    broker.non_marginable_override = D(50)
    now = advance(setup, 1)
    result = engine.step(
        (signal(now, "cash-eth", symbol="ETH/USD"),),
        {"ETH/USD": quote(now, symbol="ETH/USD")},
        now=now,
    )
    assert result["state"] == "running", result
    assert len(broker.posts) == 2
    assert broker.posts[1].quantity * 100 == 50


def test_cash_is_rechecked_after_intent_commit_before_broker_post(setup):
    database, broker, _, _, make = setup
    engine = make()
    engine.initialize()

    def change_cash():
        with database.begin() as connection:
            dispatching = connection.scalar(
                select(scalping_order_links.c.client_order_id).where(
                    scalping_order_links.c.dispatch_state == "dispatching"
                )
            )
        if dispatching:
            broker.cash = D(50)

    broker.account_hook = change_cash
    engine.step((signal(NOW, "cash-changed"),), {"BTC/USD": quote(NOW)}, now=NOW)
    assert not broker.posts
    with database.begin() as connection:
        assert connection.scalar(select(scalping_order_links.c.dispatch_state)) == "expired_unsent"


@pytest.mark.parametrize(
    "symbol,bid,ask,bid_size,ask_size,minimum",
    [
        ("BTC/USD", "77235.85", "77259.346", ".0019862", ".0010034", ".00001299"),
        ("ETH/USD", "2465.358", "2465.85", ".06713", ".03339", ".000407688"),
    ],
)
def test_actual_probed_crypto_steps_and_quotes_fund_a_100_dollar_ticket(
    setup, symbol, bid, ask, bid_size, ask_size, minimum
):
    database, broker, _, clock, _ = setup
    broker.crypto_asset = lambda requested: PaperCryptoAsset(
        id=requested,
        symbol=requested,
        **{"class": "crypto"},
        status="active",
        tradable=True,
        min_order_size=minimum,
        min_trade_increment=".000000001",
        price_increment=".000000001",
    )
    config = ScalpingConfig(
        cohort_id="actual-steps", account_digest=ACCOUNT, approved_at=NOW - timedelta(minutes=1)
    )
    engine = ScalpOrderEngine(
        database, broker, config, owner_id="worker", code_sha="a" * 40, clock=lambda: clock[0]
    )
    engine.initialize()
    q = quote(NOW, symbol=symbol, bid=bid, ask=ask).model_copy(
        update={"bid_size": D(bid_size), "ask_size": D(ask_size)}
    )
    entry = signal(NOW, "actual-quote", symbol=symbol).model_copy(update={"quote": q})
    result = engine.step((entry,), {symbol: q}, now=NOW)
    assert result["state"] == "running", result
    request = broker.posts[0]
    assert request.quantity >= D(minimum)
    assert request.quantity % D(".000000001") == 0
    assert request.quantity * D(bid) <= 100


def test_stream_updates_are_owned_monotonic_and_do_not_call_the_broker(setup, monkeypatch):
    database, broker, _, _, make = setup
    engine = make()
    engine.initialize()
    broker.partial = D(0)
    engine.step((signal(NOW, "streamed"),), {"BTC/USD": quote(NOW)}, now=NOW)
    client_id = broker.posts[0].client_order_id
    now = advance(setup, 2)
    broker.fill_more(client_id, D(".4"))
    partial = broker.values[client_id].model_copy(
        update={
            "status": type(broker.values[client_id].status)("partially_filled"),
            "updated_at": now,
        }
    )
    broker.values[client_id] = partial

    def forbidden(*args, **kwargs):
        raise AssertionError("stream consumption must not perform broker I/O")

    with monkeypatch.context() as patch:
        for method in (
            "account",
            "positions",
            "open_orders",
            "find_order_by_client_id",
            "submit_crypto_limit_order",
            "submit_crypto_market_order",
            "cancel_order",
        ):
            patch.setattr(broker, method, forbidden)
        engine.consume_order_update(partial, observed_at=now)
        engine.consume_order_update(partial, observed_at=now)
    with database.begin() as connection:
        assert connection.scalar(select(orders.c.filled_quantity)) == D(".4")
    engine.step((), {"BTC/USD": quote(now)}, now=now)
    assert engine.inventory()["BTC/USD"].quantity == D(".399")
    assert len(broker.posts) == 1


def test_unowned_order_stream_update_is_audited_not_adopted(setup):
    database, broker, _, _, make = setup
    engine = make()
    engine.initialize()
    foreign = AlpacaPaperOrder(
        id="external",
        client_order_id="external",
        symbol="BTCUSD",
        side="buy",
        status="filled",
        qty="2",
        filled_qty="2",
        filled_avg_price="100",
        created_at=NOW,
        filled_at=NOW,
    )
    engine.consume_order_update(foreign, observed_at=NOW)
    with database.begin() as connection:
        assert connection.scalar(select(func.count()).select_from(orders)) == 0
        assert connection.scalar(select(func.count()).select_from(scalping_cycles)) == 0
    assert not broker.posts


@pytest.mark.parametrize("failure", ["owner", "future"])
def test_stream_mutation_requires_current_owner_and_real_receipt(setup, failure):
    database, broker, _, _, make = setup
    engine = make()
    engine.initialize()
    engine.step((signal(NOW, "stream-fence"),), {"BTC/USD": quote(NOW)}, now=NOW)
    actual = next(iter(broker.values.values()))
    receipt = NOW
    if failure == "owner":
        with database.begin() as connection:
            connection.execute(update(worker_locks).values(owner_id="replacement"))
    else:
        receipt += timedelta(seconds=1)
    with pytest.raises(ValueError):
        engine.consume_order_update(actual, observed_at=receipt)
    assert len(broker.posts) == 1


def test_ordinary_pending_orders_use_fifteen_second_rest_recovery(setup, monkeypatch):
    _, broker, _, _, make = setup
    engine = make(entry_order_ttl_seconds=60)
    engine.initialize()
    broker.partial = D(0)
    engine.step((signal(NOW, "rest-cadence"),), {"BTC/USD": quote(NOW)}, now=NOW)
    lookups = []
    original = broker.find_order_by_client_id

    def find(client_id):
        lookups.append(client_id)
        return original(client_id)

    monkeypatch.setattr(broker, "find_order_by_client_id", find)
    for seconds in range(1, 15):
        now = advance(setup, seconds)
        engine.step((), {}, now=now)
    assert lookups == []
    now = advance(setup, 15)
    engine.step((), {}, now=now)
    assert 1 <= len(lookups) <= 2
    assert len(broker.posts) == 1


def test_delayed_stream_receipt_can_advance_but_never_slide_opening_timer(setup):
    database, broker, _, _, make = setup
    engine = make()
    engine.initialize()
    broker.partial = D(0)
    engine.step((signal(NOW, "earlier-receipt"),), {"BTC/USD": quote(NOW)}, now=NOW)
    client_id = broker.posts[0].client_order_id
    now = advance(setup, 2)
    broker.fill_more(client_id, D(".4"))
    partial = broker.values[client_id].model_copy(
        update={
            "status": type(broker.values[client_id].status)("partially_filled"),
            "updated_at": NOW + timedelta(seconds=1),
        }
    )
    engine._update_order(client_id, partial, now)
    engine.step((), {}, now=now)
    now = advance(setup, 3)
    engine.consume_order_update(partial, observed_at=NOW + timedelta(seconds=1))
    engine.step((), {}, now=now)
    with database.begin() as connection:
        cycle = connection.execute(select(scalping_cycles)).mappings().one()
        expiry = connection.scalar(
            select(scalping_order_links.c.expires_at).where(
                scalping_order_links.c.client_order_id == client_id
            )
        )
    assert cycle["opened_at"].replace(tzinfo=UTC) == NOW + timedelta(seconds=1)
    assert cycle["exit_due_at"].replace(tzinfo=UTC) == NOW + timedelta(seconds=16)
    assert expiry.replace(tzinfo=UTC) == NOW + timedelta(seconds=3)


def test_older_stream_version_does_not_replace_same_quantity_vwap(setup):
    database, broker, _, _, make = setup
    engine = make()
    engine.initialize()
    engine.step((signal(NOW, "ordered-vwap"),), {"BTC/USD": quote(NOW)}, now=NOW)
    original = next(iter(broker.values.values()))
    now = advance(setup, 2)
    fresh = original.model_copy(update={"updated_at": now})
    engine.consume_order_update(fresh, observed_at=now)
    now = advance(setup, 3)
    stale = original.model_copy(
        update={"updated_at": NOW + timedelta(seconds=1), "filled_average_price": D(120)}
    )
    engine.consume_order_update(stale, observed_at=now)
    with database.begin() as connection:
        stored = connection.scalar(select(scalping_order_links.c.broker))
    assert D(stored["filled_average_price"]) == 100
