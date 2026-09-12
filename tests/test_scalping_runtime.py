import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from tradeagent.alpaca_paper import AlpacaOrderStatus, AlpacaPaperClient
from tradeagent.event_order_stream import PaperTradeUpdate
from tradeagent.persistence import Database, ProductionRepository
from tradeagent.scalping_config import ScalpingConfig, ScalpQuote, ScalpSignal
from tradeagent.scalping_runtime import ScalpingRuntime, _blocking, run_scalping_service


def runtime_fixture(
    database: Database, monkeypatch: pytest.MonkeyPatch, now: datetime
) -> tuple[ScalpingRuntime, MagicMock, MagicMock, MagicMock]:
    config = ScalpingConfig(cohort_id="v30-runtime", account_digest="a" * 64, approved_at=now)
    quote = ScalpQuote(
        symbol="BTC/USD",
        exchange_at=now - timedelta(milliseconds=20),
        exchange_time_ns=int(now.timestamp()) * 1_000_000_000 - 20_000_000,
        received_at=now - timedelta(milliseconds=10),
        bid=100,
        ask=101,
        bid_size=2,
        ask_size=1,
    )
    signal = ScalpSignal(
        decision_id="v30-decision",
        symbol="BTC/USD",
        observed_at=now,
        action="buy",
        family="momentum",
        score=0.5,
        reasons=("book_pressure",),
        quote=quote,
        features={"ofi": 0.5},
        estimated_round_trip_cost_bps=40,
    )
    market, strategy, engine, store = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    market.quote.side_effect = lambda symbol: quote if symbol == "BTC/USD" else None
    market.features.side_effect = lambda symbol, at: object() if symbol == "BTC/USD" else None
    market.health_snapshot.return_value = {"state": "valid", "venue_sequence_available": False}
    strategy.decide.return_value = signal
    engine.status.return_value = {"state": "ready", "pending_orders": 0}
    engine.summary.return_value = {"closed_round_trips": 0}
    engine.inventory.return_value = {}
    engine.step.return_value = {"state": "observing"}
    monkeypatch.setattr("tradeagent.scalping_runtime.BookFeatureEngine", lambda *_: market)
    monkeypatch.setattr("tradeagent.scalping_runtime.ScalpStrategy", lambda *_: strategy)
    monkeypatch.setattr("tradeagent.scalping_runtime.ScalpStore", lambda *_: store)
    monkeypatch.setattr("tradeagent.scalping_runtime.ScalpOrderEngine", lambda *a, **k: engine)
    repo = ProductionRepository(database)
    repo.acquire_worker_lock("tradeagent-event-worker", "owner", observed_at=now)
    runtime = ScalpingRuntime(
        database,
        MagicMock(spec=AlpacaPaperClient),
        config,
        owner_id="owner",
        code_sha="c" * 40,
        clock=lambda: now,
    )
    runtime.notifications = MagicMock()
    runtime.initialize()
    return runtime, engine, market, store


def test_legacy_kill_does_not_gate_v30_but_operator_stop_keeps_supervision(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = datetime.now(UTC)
    with Database(f"sqlite:///{tmp_path / 'runtime.db'}") as database:
        database.initialize()
        runtime, engine, _, _ = runtime_fixture(database, monkeypatch, now)
        runtime.repo.set_control("kill_switch", "active")
        runtime.tick()
        assert engine.step.call_args.args[0][0].action == "buy"
        runtime.repo.set_control("scalping:v30-runtime:stop", '{"action":"stop_new_entries"}')
        runtime.tick()
        assert engine.step.call_args.args[0] == ()
        assert engine.step.call_count == 2
        assert runtime.snapshot()["operator_stop"] is True
        assert runtime.repo.get_control("kill_switch") == "active"


def test_durable_market_batch_precedes_features(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = datetime.now(UTC)
    with Database(f"sqlite:///{tmp_path / 'market.db'}") as database:
        database.initialize()
        runtime, _, market, store = runtime_fixture(database, monkeypatch, now)
        order = []
        store.persist_market_batch.side_effect = lambda *a, **k: order.append("persist")
        market.on_event.side_effect = lambda _: order.append("features")
        event = MagicMock()
        event.model_dump.return_value = {"kind": "book", "sequence": 1}
        runtime.process_market_batch((event,))
        assert order == ["persist", "features"]
        assert runtime.snapshot()["raw"]["committed_events"] == 1


def test_heartbeat_does_not_wait_for_blocked_broker_execution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = datetime.now(UTC)
    entered, release = threading.Event(), threading.Event()
    with Database(f"sqlite:///{tmp_path / 'heartbeat.db'}", pool_size=3) as database:
        database.initialize()
        runtime, engine, _, _ = runtime_fixture(database, monkeypatch, now)

        def slow_step(*args: object, **kwargs: object) -> dict[str, str]:
            entered.set()
            assert release.wait(5)
            return {"state": "submitted"}

        engine.step.side_effect = slow_step
        with ThreadPoolExecutor(max_workers=2) as executor:
            pending = executor.submit(runtime.tick)
            assert entered.wait(2)
            try:
                heartbeat = executor.submit(
                    runtime.heartbeat, feed={"state": "subscribed"}, queued_events=0
                )
                heartbeat.result(timeout=2)
                assert not pending.done()
                worker = runtime.repo.latest_heartbeat("tradeagent-event-worker")
                assert worker is not None and worker[2]["entry_policy"] == "v30-paper-unrestricted"
            finally:
                release.set()
            pending.result(timeout=2)


def test_lost_lease_cannot_publish_a_current_owned_heartbeat(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = datetime.now(UTC)
    with Database(f"sqlite:///{tmp_path / 'lease.db'}") as database:
        database.initialize()
        runtime, _, _, _ = runtime_fixture(database, monkeypatch, now)
        runtime.repo.release_worker_lock("tradeagent-event-worker", "owner")
        runtime.repo.acquire_worker_lock("tradeagent-event-worker", "replacement", observed_at=now)
        with pytest.raises(RuntimeError, match="ownership"):
            runtime.heartbeat(feed={"state": "subscribed"}, queued_events=0)
        assert runtime.repo.get_control("scalping:v30-runtime:status") is None


def test_order_stream_updates_are_applied_without_forcing_rest_reconciliation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = datetime.now(UTC)
    with Database(f"sqlite:///{tmp_path / 'stream.db'}") as database:
        database.initialize()
        runtime, engine, _, _ = runtime_fixture(database, monkeypatch, now)
        stream = MagicMock()
        stream.health_snapshot.return_value = {"gap_count": 0}
        stream.drain.return_value = (
            PaperTradeUpdate(
                event="partial_fill",
                timestamp=now,
                received_at=now,
                sequence=1,
                connection_id=1,
                order_id="broker",
                client_order_id="owned",
                symbol="BTCUSD",
                side="buy",
                status=AlpacaOrderStatus.PARTIALLY_FILLED,
                quantity=Decimal(1),
                notional=None,
                filled_quantity=Decimal(".25"),
                filled_average_price=Decimal(100),
                order_created_at=now - timedelta(seconds=1),
                order_updated_at=now,
                execution_id="execution",
                event_id="event",
                fill_quantity=Decimal(".25"),
                fill_price=Decimal(100),
                position_quantity=Decimal(".25"),
            ),
        )
        runtime.order_stream = stream
        runtime.tick()
        engine.consume_order_update.assert_called_once()
        assert engine.reconcile.call_count == 0


def test_cancel_waits_for_inflight_thread_before_releasing_caller() -> None:
    entered, release, completed = threading.Event(), threading.Event(), threading.Event()

    def slow_request() -> str:
        entered.set()
        assert release.wait(5)
        completed.set()
        return "broker response"

    async def exercise() -> None:
        task = asyncio.create_task(_blocking(slow_request))
        assert await asyncio.to_thread(entered.wait, 2)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert completed.is_set()

    asyncio.run(exercise())


def test_service_coordinates_background_tasks_and_releases_ownership(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    url = f"sqlite:///{tmp_path / 'service.db'}"
    now = datetime.now(UTC)
    with Database(url) as database:
        database.initialize()
    monkeypatch.setenv("TRADEAGENT_DATABASE_URL", url)
    monkeypatch.setenv("ALPACA_KEY_ID", "test-only-key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "test-only-secret")
    monkeypatch.setenv("NEWS_CONTACT_EMAIL", "test@example.test")
    monkeypatch.setenv("RENDER_INSTANCE_ID", "service-owner")
    monkeypatch.setenv("RENDER_GIT_COMMIT", "c" * 40)
    feed = MagicMock()
    feed.health_snapshot.return_value = {"state": "subscribed"}
    market = MagicMock()
    market.quote.return_value = None
    market.features.return_value = None
    market.health_snapshot.return_value = {"state": "waiting"}
    store, engine, updates, source = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    engine.status.return_value = {"state": "ready"}
    engine.summary.return_value = {"closed_round_trips": 0}
    engine.inventory.return_value = {}
    updates.health_snapshot.return_value = {"gap_count": 0, "state": "subscribed"}
    updates.drain.return_value = ()
    source.__enter__.return_value = source
    source.articles.return_value = ()
    monkeypatch.setattr("tradeagent.scalping_runtime.CryptoMarketFeed", lambda *a, **k: feed)
    monkeypatch.setattr("tradeagent.scalping_runtime.BookFeatureEngine", lambda *_: market)
    monkeypatch.setattr("tradeagent.scalping_runtime.ScalpStore", lambda *_: store)
    monkeypatch.setattr("tradeagent.scalping_runtime.ScalpOrderEngine", lambda *a, **k: engine)
    monkeypatch.setattr(
        "tradeagent.scalping_runtime.AlpacaPaperTradeUpdatesStream", lambda *_: updates
    )
    monkeypatch.setattr("tradeagent.scalping_runtime.AlpacaNewsClient", lambda *_: source)

    async def exercise() -> None:
        stop = asyncio.Event()

        async def stream():
            event = MagicMock()
            event.model_dump.return_value = {"kind": "book", "sequence": 1}
            yield event
            await stop.wait()

        async def order_updates(stopping: asyncio.Event) -> None:
            await stopping.wait()

        def step(*args: object, **kwargs: object) -> dict[str, str]:
            if engine.step.call_count >= 3:
                loop.call_soon_threadsafe(stop.set)
            return {"state": "observing"}

        loop = asyncio.get_running_loop()
        feed.stream.side_effect = stream
        updates.run.side_effect = order_updates
        engine.step.side_effect = step
        config = ScalpingConfig(
            cohort_id="v30-service",
            account_digest="a" * 64,
            approved_at=now,
            decision_interval_seconds=0.05,
            heartbeat_interval_seconds=0.02,
        )
        await asyncio.wait_for(run_scalping_service(config, stop_event=stop), timeout=5)

    asyncio.run(exercise())
    assert store.persist_market_batch.called
    assert market.on_event.called
    assert market.on_resnapshot is feed.request_resnapshot
    assert feed.stop.called
    with Database(url) as database:
        repo = ProductionRepository(database)
        assert repo.get_control("scalping:v30-service:status") is not None
        assert repo.acquire_worker_lock("tradeagent-event-worker", "next-owner")
