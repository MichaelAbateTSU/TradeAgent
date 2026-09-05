from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from tradeagent.alpaca_stream import MarketQuote, StreamEvent
from tradeagent.config import AppConfig, IntradayConfig
from tradeagent.domain import MarketBar
from tradeagent.persistence import Database, ProductionRepository
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

    async def on_bar(self, bar: MarketBar, *, can_enter: bool) -> None:
        self.bars.append((bar, can_enter, self.repository.get_control("kill_switch")))

    async def on_quote(self, quote: MarketQuote, *, can_enter: bool) -> None:
        self.quotes.append((quote, can_enter))


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

    result = asyncio.run(worker.run(_events([_bar(), _quote()])))

    assert result.events_seen == 2
    assert result.bars_processed == 1
    assert result.quotes_processed == 1
    assert processor.bars[0][1:] == (False, "active")
    assert repository.get_control("kill_switch") == "active"
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
        mode=WorkerMode.SHADOW,
        enabled=False,
        authorized=False,
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
        mode=WorkerMode.SHADOW,
        enabled=False,
        authorized=False,
        clock=stale_clock,
    )
    with pytest.raises(StaleMarketDataError, match="stale"):
        asyncio.run(worker.run(_events([_quote()])))
    assert repository.get_control("kill_switch") == "active"
    database.dispose()
