from __future__ import annotations

import asyncio
import threading
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from tradeagent.alpaca_stream import MarketQuote, MarketTrade, StreamEvent
from tradeagent.config import AppConfig, IntradayConfig
from tradeagent.domain import MarketBar
from tradeagent.persistence import Database, ProductionRepository
from tradeagent.shadow_recorder import ShadowRecorderSettings
from tradeagent.worker import (
    AutonomousPaperWorker,
    StaleMarketDataError,
    WorkerMode,
    WorkerStartupError,
)

NOW = datetime(2026, 9, 4, 14, 35, tzinfo=UTC)


class Reconciliation:
    def __init__(self, healthy: bool) -> None:
        self.healthy = healthy


class FakeReconciler:
    def __init__(self, healthy: bool = True) -> None:
        self.healthy = healthy
        self.calls = 0

    def reconcile(self, *, observed_at: datetime) -> Reconciliation:
        self.calls += 1
        return Reconciliation(self.healthy)


class FakeProcessor:
    def __init__(self, repository: ProductionRepository) -> None:
        self.repository = repository
        self.bars: list[tuple[MarketBar, bool, str | None]] = []
        self.quotes: list[tuple[MarketQuote, bool]] = []
        self.trades: list[tuple[MarketTrade, bool]] = []

    async def on_bar(self, bar: MarketBar, *, can_enter: bool) -> None:
        self.bars.append((bar, can_enter, self.repository.get_control("kill_switch")))

    async def on_quote(self, quote: MarketQuote, *, can_enter: bool) -> None:
        self.quotes.append((quote, can_enter))

    async def on_trade(self, trade: MarketTrade, *, can_enter: bool) -> None:
        self.trades.append((trade, can_enter))


def _bar(timestamp: datetime = NOW) -> MarketBar:
    return MarketBar(
        symbol="SPY",
        timestamp=timestamp,
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100"),
        volume=Decimal("1000"),
    )


def _quote(timestamp: datetime = NOW) -> MarketQuote:
    return MarketQuote(
        symbol="SPY",
        timestamp=timestamp,
        bid_price=Decimal("99.9"),
        ask_price=Decimal("100.1"),
        bid_size=Decimal("10"),
        ask_size=Decimal("10"),
    )


def _trade(timestamp: datetime = NOW) -> MarketTrade:
    return MarketTrade(
        symbol="SPY",
        timestamp=timestamp,
        price=Decimal("100"),
        size=Decimal("5"),
        exchange="V",
        trade_id="42",
    )


async def _events(values: list[StreamEvent]):
    for value in values:
        yield value


def _worker(
    tmp_path: Path,
    *,
    mode: WorkerMode,
    enabled: bool,
    authorized: bool,
    healthy: bool = True,
    clock=lambda: NOW,
) -> tuple[Database, ProductionRepository, FakeProcessor, AutonomousPaperWorker]:
    database = Database(f"sqlite:///{tmp_path / 'worker.db'}")
    database.initialize()
    repository = ProductionRepository(database)
    processor = FakeProcessor(repository)
    worker = AutonomousPaperWorker(
        AppConfig(intraday=IntradayConfig(enabled=enabled)),
        repository,
        FakeReconciler(healthy),
        processor,
        mode=mode,
        instance_id="worker-1",
        strategy_authorized=lambda: authorized,
        clock=clock,
        recorder_settings=ShadowRecorderSettings(
            flush_interval_seconds=0.01,
            heartbeat_interval_seconds=0.01,
        ),
    )
    return database, repository, processor, worker


def test_shadow_worker_processes_events_without_enabling_entries(
    tmp_path: Path,
) -> None:
    database, repository, processor, worker = _worker(
        tmp_path,
        mode=WorkerMode.SHADOW,
        enabled=False,
        authorized=False,
    )

    result = asyncio.run(worker.run(_events([_bar(), _quote(), _trade()])))

    assert result.events_seen == 3
    assert result.bars_processed == 1
    assert result.quotes_processed == 1
    assert result.trades_processed == 1
    assert processor.bars[0][1:] == (False, None)
    assert repository.get_control("kill_switch") is None
    assert repository.acquire_worker_lock("tradeagent-paper-worker", "worker-2")
    database.dispose()


def test_authorized_worker_releases_kill_switch_only_while_running(
    tmp_path: Path,
) -> None:
    database, repository, processor, worker = _worker(
        tmp_path,
        mode=WorkerMode.AUTONOMOUS_PAPER,
        enabled=True,
        authorized=True,
    )

    result = asyncio.run(worker.run(_events([_bar()])))

    assert result.mode is WorkerMode.AUTONOMOUS_PAPER
    assert processor.bars[0][1:] == (True, "inactive")
    assert repository.get_control("kill_switch") == "active"
    database.dispose()


def test_worker_fails_closed_on_authorization_and_reconciliation(
    tmp_path: Path,
) -> None:
    database, repository, _, unauthorized = _worker(
        tmp_path,
        mode=WorkerMode.AUTONOMOUS_PAPER,
        enabled=True,
        authorized=False,
    )
    with pytest.raises(WorkerStartupError, match="not authorized"):
        asyncio.run(unauthorized.run(_events([])))
    assert repository.get_control("kill_switch") == "active"
    database.dispose()

    database, repository, _, unhealthy = _worker(
        tmp_path,
        mode=WorkerMode.AUTONOMOUS_PAPER,
        enabled=True,
        authorized=True,
        healthy=False,
    )
    with pytest.raises(WorkerStartupError, match="reconciliation"):
        asyncio.run(unhealthy.run(_events([])))
    assert repository.get_control("kill_switch") == "active"
    database.dispose()


def test_worker_fails_closed_on_stale_or_future_data(tmp_path: Path) -> None:
    def stale_clock() -> datetime:
        return NOW + timedelta(minutes=2)

    database, repository, _, worker = _worker(
        tmp_path,
        mode=WorkerMode.AUTONOMOUS_PAPER,
        enabled=True,
        authorized=True,
        clock=stale_clock,
    )
    with pytest.raises(StaleMarketDataError, match="stale"):
        asyncio.run(worker.run(_events([_quote()])))
    assert repository.get_control("kill_switch") == "active"
    database.dispose()


@pytest.mark.parametrize("operator_value", ["active", "inactive", "operator-paused"])
def test_shadow_records_late_and_future_packets_without_touching_operator_control(
    tmp_path: Path,
    operator_value: str,
) -> None:
    database, repository, processor, worker = _worker(
        tmp_path,
        mode=WorkerMode.SHADOW,
        enabled=False,
        authorized=False,
        healthy=False,
    )
    repository.set_control("kill_switch", operator_value)
    assert repository.acquire_worker_lock("tradeagent-paper-worker", "separate-executor")
    result = asyncio.run(
        worker.run(
            _events(
                [
                    _quote(NOW - timedelta(seconds=11)),
                    _quote(NOW + timedelta(seconds=1)),
                    _quote(),
                ]
            )
        )
    )
    assert result.events_seen == 3
    assert result.quotes_processed == 1
    assert processor.quotes[0][1] is False
    assert repository.market_data_counts() == (0, 3, 0)
    assert repository.get_control("kill_switch") == operator_value
    heartbeat = repository.latest_heartbeat("tradeagent-shadow-recorder")
    assert heartbeat is not None
    assert heartbeat[2]["late_events"] == 1
    assert heartbeat[2]["future_events"] == 1
    assert heartbeat[2]["committed"] == 3
    assert heartbeat[2]["healthy"] is False
    database.dispose()


def test_shadow_heartbeats_continue_without_events_and_stop_interrupts_receive(
    tmp_path: Path,
) -> None:
    database, repository, _, worker = _worker(
        tmp_path,
        mode=WorkerMode.SHADOW,
        enabled=False,
        authorized=False,
    )
    repository.set_control("kill_switch", "inactive")
    beats = []
    original = repository.heartbeat

    def record(*args, **kwargs):
        beats.append(args[2])
        return original(*args, **kwargs)

    repository.heartbeat = record

    async def idle():
        await asyncio.Event().wait()
        yield _quote()

    async def run():
        stop = asyncio.Event()
        task = asyncio.create_task(worker.run(idle(), stop_event=stop))
        await asyncio.sleep(0.15)
        stop.set()
        await asyncio.wait_for(task, 2)

    asyncio.run(run())
    assert len(beats) >= 3
    assert all(beat["healthy"] is False for beat in beats)
    assert beats[-1]["state"] == "stopped"
    assert repository.get_control("kill_switch") == "inactive"
    database.dispose()


def test_autonomous_future_packet_remains_strict(tmp_path: Path) -> None:
    database, repository, processor, worker = _worker(
        tmp_path,
        mode=WorkerMode.AUTONOMOUS_PAPER,
        enabled=True,
        authorized=True,
    )
    with pytest.raises(StaleMarketDataError, match="future"):
        asyncio.run(worker.run(_events([_quote(NOW + timedelta(seconds=1))])))
    assert not processor.quotes
    assert repository.get_control("kill_switch") == "active"
    database.dispose()


def test_shadow_error_waits_for_inflight_heartbeat_before_terminal_state(tmp_path: Path) -> None:
    database, repository, _, worker = _worker(
        tmp_path,
        mode=WorkerMode.SHADOW,
        enabled=False,
        authorized=False,
    )
    repository.set_control("kill_switch", "inactive")
    entered = threading.Event()
    release = threading.Event()
    original = repository.heartbeat
    states = []

    def delayed(*args, **kwargs):
        state = args[2]["state"]
        if state in {"healthy", "degraded"}:
            entered.set()
            assert release.wait(5)
        result = original(*args, **kwargs)
        states.append(state)
        return result

    repository.heartbeat = delayed

    async def broken():
        yield _quote()
        while not entered.is_set():
            await asyncio.sleep(0.002)
        raise ValueError("test stream failure")

    async def run():
        task = asyncio.create_task(worker.run(broken()))
        while not entered.is_set():
            await asyncio.sleep(0.002)
        await asyncio.sleep(0.05)
        assert not task.done()
        release.set()
        with pytest.raises(ValueError, match="test stream failure"):
            await task

    asyncio.run(run())
    assert states[-1] == "failed"
    assert repository.latest_heartbeat("tradeagent-shadow-recorder")[2]["healthy"] is False
    assert repository.get_control("kill_switch") == "inactive"
    database.dispose()
