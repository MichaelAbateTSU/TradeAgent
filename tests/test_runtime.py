from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import SecretStr
from websockets.asyncio.client import connect as websocket_connect
from websockets.asyncio.server import serve

from tradeagent.alpaca_paper import (
    AlpacaPaperAccount,
    AlpacaPaperOrder,
    AlpacaPaperPosition,
)
from tradeagent.alpaca_stream import AlpacaMarketStream, AlpacaStreamSettings, MarketQuote
from tradeagent.config import AppConfig, IntradayConfig
from tradeagent.data import synthetic_bars
from tradeagent.persistence import Database, ProductionRepository
from tradeagent.runtime import (
    MarketFeedStatusMonitor,
    ProductionPaperReconciler,
    ShadowAuditProcessor,
    run_shadow_runtime,
)
from tradeagent.shadow_recorder import ShadowRecorderSettings
from tradeagent.worker import AutonomousPaperWorker, WorkerMode


class FakePaperClient:
    def account(self) -> AlpacaPaperAccount:
        return AlpacaPaperAccount(
            id="account-1",
            status="ACTIVE",
            currency="USD",
            cash=Decimal("100000"),
            portfolio_value=Decimal("100000"),
            buying_power=Decimal("100000"),
            trading_blocked=False,
            transfers_blocked=False,
            account_blocked=False,
        )

    def positions(self) -> tuple[AlpacaPaperPosition, ...]:
        return ()

    def open_orders(self) -> tuple[AlpacaPaperOrder, ...]:
        return ()


def test_production_reconciler_and_shadow_audit(tmp_path: Path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'runtime.db'}")
    database.initialize()
    repository = ProductionRepository(database)
    reconciler = ProductionPaperReconciler(FakePaperClient(), repository)
    now = datetime(2026, 9, 4, 15, tzinfo=UTC)

    status = reconciler.reconcile(observed_at=now)
    processor = ShadowAuditProcessor(repository)
    bar = next(synthetic_bars(count=1)).model_copy(update={"timestamp": now})
    quote = MarketQuote(
        symbol="SPY",
        timestamp=now,
        bid_price=Decimal("99.9"),
        ask_price=Decimal("100.1"),
        bid_size=Decimal("10"),
        ask_size=Decimal("10"),
    )
    asyncio.run(processor.on_bar(bar, can_enter=False))
    asyncio.run(processor.on_quote(quote, can_enter=False))

    assert status.healthy
    assert status.position_count == 0
    assert status.open_order_count == 0
    assert repository.event_count() == 3
    database.dispose()


def test_market_feed_monitor_marks_open_session_stale_and_closed_session_safe(
    tmp_path: Path,
) -> None:
    database = Database(f"sqlite:///{tmp_path / 'feed-monitor.db'}")
    database.initialize()
    repository = ProductionRepository(database)
    now = datetime(2026, 9, 4, 15, tzinfo=UTC)
    monitor = MarketFeedStatusMonitor(
        repository,
        IntradayConfig(),
        instance_id="feed-1",
        clock=lambda: now,
    )

    assert monitor.check() == "stale"
    assert repository.get_control("kill_switch") is None
    repository.heartbeat(
        "tradeagent-shadow-recorder",
        "worker-1",
        {
            "state": "healthy",
            "healthy": True,
            "last_event_at": now.isoformat(),
            "last_committed_event_at": now.isoformat(),
            "last_market_commit_at": now.isoformat(),
        },
        observed_at=now,
    )
    assert monitor.check() == "healthy"
    repository.heartbeat(
        "tradeagent-shadow-recorder",
        "worker-1",
        {
            "state": "healthy",
            "healthy": True,
            "last_event_at": (now - timedelta(seconds=11)).isoformat(),
            "last_committed_event_at": now.isoformat(),
            "last_market_commit_at": now.isoformat(),
        },
        observed_at=now,
    )
    assert monitor.check() == "stale"
    assert repository.get_control("kill_switch") is None

    closed = MarketFeedStatusMonitor(
        repository,
        IntradayConfig(),
        instance_id="feed-1",
        clock=lambda: datetime(2026, 9, 6, 15, tzinfo=UTC),
    )
    assert closed.check() == "market_closed"
    database.dispose()


@pytest.mark.parametrize(
    "unhealthy",
    [
        {"state": "stopped"},
        {"state": "failed"},
        {"state": "starting"},
        {"last_committed_event_at": "2026-09-08T14:29:11+00:00"},
        {"last_committed_event_at": None},
        {"last_market_commit_at": "2026-09-08T14:29:00+00:00"},
    ],
)
def test_recent_heartbeat_and_receipts_cannot_mask_stopped_or_stalled_commit_progress(
    tmp_path: Path,
    unhealthy,
) -> None:
    database = Database(f"sqlite:///{tmp_path / 'true-feed-health.db'}")
    database.initialize()
    repository = ProductionRepository(database)
    repository.set_control("kill_switch", "inactive")
    now = datetime(2026, 9, 8, 14, 34, tzinfo=UTC)
    repository.heartbeat(
        "tradeagent-shadow-recorder",
        "shadow",
        {
            "state": "healthy",
            "healthy": True,
            "last_event_at": now.isoformat(),
            "last_received_at": now.isoformat(),
            "last_commit_at": now.isoformat(),
            "last_committed_event_at": now.isoformat(),
            "last_market_commit_at": now.isoformat(),
            **unhealthy,
        },
        observed_at=now,
    )
    repository.heartbeat(
        "tradeagent-market-feed",
        "legacy",
        {
            "state": "healthy",
        },
        observed_at=datetime(2026, 9, 8, 14, 29, tzinfo=UTC),
    )
    monitor = MarketFeedStatusMonitor(
        repository,
        IntradayConfig(),
        instance_id="monitor",
        clock=lambda: now,
    )
    assert monitor.check() == "stale"
    assert repository.latest_heartbeat("tradeagent-shadow-market-feed")[2]["healthy"] is False
    assert repository.get_control("kill_switch") == "inactive"
    database.dispose()


def test_shadow_runtime_records_stale_packets_and_never_runs_broker_scheduler(
    tmp_path: Path,
    monkeypatch,
) -> None:
    now = datetime(2026, 9, 8, 14, 30, tzinfo=UTC)
    database = Database(f"sqlite:///{tmp_path / 'integrated-shadow.db'}")
    database.initialize()
    repository = ProductionRepository(database)
    repository.set_control("kill_switch", "operator-paused")
    delivered = []

    class Socket:
        def __init__(self):
            self.frames = [
                [{"T": "success", "msg": "connected"}],
                [{"T": "success", "msg": "authenticated"}],
                [{"T": "subscription", "bars": ["SPY"], "quotes": ["SPY"], "trades": ["SPY"]}],
                [
                    {
                        "T": "q",
                        "S": "SPY",
                        "t": (now - timedelta(seconds=11)).isoformat(),
                        "bp": 100,
                        "ap": 101,
                        "bs": 10,
                        "as": 10,
                    }
                ],
                [
                    {
                        "T": "q",
                        "S": "SPY",
                        "t": now.isoformat(),
                        "bp": 100,
                        "ap": 101,
                        "bs": 10,
                        "as": 10,
                    }
                ],
            ]

        async def recv(self, decode=None):
            if self.frames:
                frame = self.frames.pop(0)
                delivered.extend(frame)
                return json.dumps(frame)
            await asyncio.Event().wait()

        async def send(self, message):
            return None

    class Connection:
        async def __aenter__(self):
            return Socket()

        async def __aexit__(self, *args):
            return False

    class ForbiddenReconciler:
        def reconcile(self, **kwargs):
            pytest.fail("shadow must not run broker reconciliation or change its global control")

    class ForbiddenScheduler:
        async def run(self, stop_event):
            pytest.fail("shadow must not run the broker/global watchdog scheduler")

    monkeypatch.setattr("tradeagent.alpaca_stream.connect", lambda *args, **kwargs: Connection())
    worker = AutonomousPaperWorker(
        AppConfig(),
        repository,
        ForbiddenReconciler(),
        ShadowAuditProcessor(repository),
        mode=WorkerMode.SHADOW,
        instance_id="integration-shadow",
        strategy_authorized=lambda: False,
        clock=lambda: now,
        recorder_settings=ShadowRecorderSettings(
            flush_interval_seconds=0.01,
            heartbeat_interval_seconds=0.01,
        ),
    )
    stream = AlpacaMarketStream(
        AlpacaStreamSettings(key_id=SecretStr("test-key"), secret_key=SecretStr("test-secret")),
        clock=lambda: now,
    )

    async def run():
        runtime = asyncio.create_task(
            run_shadow_runtime(
                stream,
                worker,
                ForbiddenScheduler(),
                symbols=("SPY",),
                feed_monitor=MarketFeedStatusMonitor(
                    repository,
                    IntradayConfig(),
                    instance_id="final-health",
                    clock=lambda: now,
                ),
            )
        )
        for _ in range(100):
            heartbeat = await asyncio.to_thread(
                repository.latest_heartbeat, "tradeagent-shadow-recorder"
            )
            if heartbeat and heartbeat[2]["committed"] == 2:
                break
            await asyncio.sleep(0.01)
        assert heartbeat is not None
        assert heartbeat[2]["committed"] == 2
        assert heartbeat[2]["late_events"] == 1
        assert heartbeat[2]["healthy"] is True
        runtime.cancel()
        with pytest.raises(asyncio.CancelledError):
            await runtime

    asyncio.run(run())
    assert len(delivered) == 5
    assert repository.get_control("kill_switch") == "operator-paused"
    assert repository.market_data_counts() == (0, 2, 0)
    assert repository.latest_event_payload("shadow_market_quote") is None
    assert repository.latest_event_payload("shadow_recorder_batch")["received"] == 2
    final_feed = repository.latest_heartbeat("tradeagent-shadow-market-feed")[2]
    assert final_feed["state"] == "stale"
    assert final_feed["healthy"] is False
    assert final_feed["recorder_alive"] is False
    database.dispose()


def test_local_websocket_connection_limit_recovery_and_persistence_burst(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Exercise real websocket transport and database commits without contacting Alpaca."""
    now = datetime(2026, 9, 8, 14, 30, tzinfo=UTC)
    database = Database(f"sqlite:///{tmp_path / 'local-websocket.db'}")
    database.initialize()
    repository = ProductionRepository(database)
    repository.set_control("kill_switch", "inactive")
    connections = []
    messages = []

    async def serve_feed(socket):
        connections.append(socket)
        if len(connections) == 1:
            await socket.send(
                json.dumps(
                    [
                        {
                            "T": "error",
                            "code": 406,
                            "msg": "connection limit exceeded",
                        }
                    ]
                )
            )
            await socket.close()
            return
        await socket.send(json.dumps([{"T": "success", "msg": "connected"}]))
        messages.append(json.loads(await socket.recv()))
        await socket.send(json.dumps([{"T": "success", "msg": "authenticated"}]))
        subscription = json.loads(await socket.recv())
        messages.append(subscription)
        await socket.send(
            json.dumps(
                [
                    {
                        "T": "subscription",
                        **{
                            channel: subscription[channel]
                            for channel in ("bars", "quotes", "trades")
                        },
                    }
                ]
            )
        )
        for batch in range(10):
            await socket.send(
                json.dumps(
                    [
                        {
                            "T": "q",
                            "S": "SPY",
                            "t": (
                                now - timedelta(seconds=1) + timedelta(microseconds=index)
                            ).isoformat(),
                            "bp": 100,
                            "ap": 101,
                            "bs": 10,
                            "as": 20,
                        }
                        for index in range(batch * 100, (batch + 1) * 100)
                    ]
                )
            )
        await socket.wait_closed()

    class ForbiddenBroker:
        def reconcile(self, **kwargs):
            pytest.fail("recording may not run broker reconciliation")

        async def run(self, stop_event):
            pytest.fail("recording may not start the trading scheduler")

    async def run():
        async with serve(serve_feed, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            monkeypatch.setattr(
                "tradeagent.alpaca_stream.connect",
                lambda url, **kwargs: websocket_connect(f"ws://127.0.0.1:{port}", **kwargs),
            )
            settings = AlpacaStreamSettings(
                key_id=SecretStr("offline-key"),
                secret_key=SecretStr("offline-secret"),
                reconnect_initial_seconds=0.001,
                reconnect_max_seconds=0.002,
            )
            worker = AutonomousPaperWorker(
                AppConfig(),
                repository,
                ForbiddenBroker(),
                ShadowAuditProcessor(repository),
                mode=WorkerMode.SHADOW,
                instance_id="local-wire-test",
                strategy_authorized=lambda: False,
                clock=lambda: now,
                recorder_settings=ShadowRecorderSettings(
                    batch_size=100,
                    flush_interval_seconds=0.01,
                    heartbeat_interval_seconds=0.01,
                ),
            )
            runtime = asyncio.create_task(
                run_shadow_runtime(
                    AlpacaMarketStream(settings, clock=lambda: now),
                    worker,
                    ForbiddenBroker(),
                    symbols=("SPY",),
                )
            )
            try:
                for _ in range(200):
                    heartbeat = await asyncio.to_thread(
                        repository.latest_heartbeat, "tradeagent-shadow-recorder"
                    )
                    if heartbeat and heartbeat[2]["committed"] == 1000:
                        break
                    if runtime.done():
                        await runtime
                    await asyncio.sleep(0.02)
                assert heartbeat is not None
                details = heartbeat[2]
                assert details["committed"] == details["received"] == 1000
                assert details["dropped_events"] == details["queue_depth"] == 0
                assert details["gaps"] == 1
                assert details["stream"]["reconnects"] == 1
                assert details["stream"]["authenticated"] is True
                assert details["stream"]["subscribed"] is True
                assert details["healthy"] is True
                assert details["last_committed_received_at"] == now.isoformat()
                assert 0.99 <= details["receive_lag_seconds"] <= 1
            finally:
                runtime.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await runtime

    asyncio.run(run())
    assert len(connections) == 2
    assert [message["action"] for message in messages] == ["auth", "subscribe"]
    assert repository.market_data_counts() == (0, 1000, 0)
    assert repository.get_control("kill_switch") == "inactive"
    database.dispose()
