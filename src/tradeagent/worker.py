from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol

from pydantic import BaseModel, ConfigDict

from tradeagent.alpaca_stream import MarketQuote, StreamEvent
from tradeagent.config import AppConfig
from tradeagent.domain import MarketBar
from tradeagent.intraday import NyseSessionCalendar, SessionPhase
from tradeagent.persistence import ProductionRepository


class WorkerMode(StrEnum):
    SHADOW = "shadow"
    AUTONOMOUS_PAPER = "autonomous_paper"


class WorkerStartupError(RuntimeError):
    pass


class StaleMarketDataError(RuntimeError):
    pass


class ReconciliationStatus(Protocol):
    @property
    def healthy(self) -> bool: ...


class WorkerReconciler(Protocol):
    def reconcile(self, *, observed_at: datetime) -> ReconciliationStatus: ...


class WorkerEventProcessor(Protocol):
    async def on_bar(self, bar: MarketBar, *, can_enter: bool) -> None: ...

    async def on_quote(self, quote: MarketQuote, *, can_enter: bool) -> None: ...


class WorkerRunResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    mode: WorkerMode
    events_seen: int
    bars_processed: int
    quotes_processed: int
    closed_session_events: int
    stopped_at: datetime


class AutonomousPaperWorker:
    """Single-instance event loop that remains fail-closed around execution."""

    def __init__(
        self,
        config: AppConfig,
        repository: ProductionRepository,
        reconciler: WorkerReconciler,
        processor: WorkerEventProcessor,
        *,
        mode: WorkerMode,
        instance_id: str,
        strategy_authorized: Callable[[], bool],
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._config = config
        self._repository = repository
        self._reconciler = reconciler
        self._processor = processor
        self._mode = mode
        self._instance_id = instance_id
        self._strategy_authorized = strategy_authorized
        self._clock = clock
        self._calendar = NyseSessionCalendar(config.intraday)

    async def run(
        self,
        events: AsyncIterator[StreamEvent],
        *,
        stop_event: asyncio.Event | None = None,
    ) -> WorkerRunResult:
        if not self._repository.acquire_worker_lock(
            "tradeagent-paper-worker",
            self._instance_id,
            stale_after_seconds=self._config.intraday.heartbeat_max_age_seconds * 2,
        ):
            raise WorkerStartupError("another paper worker owns the trading lock")
        counters = {
            "events": 0,
            "bars": 0,
            "quotes": 0,
            "closed": 0,
        }
        try:
            self._repository.set_control("kill_switch", "active")
            self._heartbeat("starting", counters)
            now = self._clock()
            reconciliation = self._reconciler.reconcile(observed_at=now)
            if not reconciliation.healthy:
                raise WorkerStartupError("startup broker reconciliation failed")
            if self._mode is WorkerMode.AUTONOMOUS_PAPER:
                if not self._config.intraday.enabled:
                    raise WorkerStartupError("intraday autonomous mode is disabled")
                if not self._strategy_authorized():
                    raise WorkerStartupError("strategy is not authorized")
                self._repository.set_control("kill_switch", "inactive")
            self._heartbeat("running", counters)

            async for event in events:
                if stop_event is not None and stop_event.is_set():
                    break
                counters["events"] += 1
                observed_at = self._clock()
                self._validate_freshness(event, observed_at)
                gate = self._calendar.gate(event.timestamp)
                if gate.phase is SessionPhase.CLOSED:
                    counters["closed"] += 1
                    self._heartbeat("running", counters)
                    continue
                can_enter = self._mode is WorkerMode.AUTONOMOUS_PAPER and gate.can_enter
                if isinstance(event, MarketBar):
                    await self._processor.on_bar(event, can_enter=can_enter)
                    counters["bars"] += 1
                else:
                    await self._processor.on_quote(event, can_enter=can_enter)
                    counters["quotes"] += 1
                self._heartbeat("running", counters)

            stopped_at = self._clock()
            return WorkerRunResult(
                mode=self._mode,
                events_seen=counters["events"],
                bars_processed=counters["bars"],
                quotes_processed=counters["quotes"],
                closed_session_events=counters["closed"],
                stopped_at=stopped_at,
            )
        finally:
            self._repository.set_control("kill_switch", "active")
            self._heartbeat("stopped", counters)
            self._repository.release_worker_lock("tradeagent-paper-worker", self._instance_id)

    def _validate_freshness(
        self,
        event: StreamEvent,
        observed_at: datetime,
    ) -> None:
        if observed_at < event.timestamp:
            self._repository.set_control("kill_switch", "active")
            raise StaleMarketDataError("market event timestamp is in the future")
        age = (observed_at - event.timestamp).total_seconds()
        maximum_age = (
            self._config.intraday.bar_max_age_seconds
            if isinstance(event, MarketBar)
            else self._config.intraday.quote_max_age_seconds
        )
        if age > maximum_age:
            self._repository.set_control("kill_switch", "active")
            raise StaleMarketDataError(f"{type(event).__name__} is stale by {age:.3f} seconds")

    def _heartbeat(self, state: str, counters: dict[str, int]) -> None:
        refreshed = self._repository.refresh_worker_lock(
            "tradeagent-paper-worker",
            self._instance_id,
            observed_at=self._clock(),
        )
        if state == "running" and not refreshed:
            self._repository.set_control("kill_switch", "active")
            raise WorkerStartupError("paper worker lost its ownership lease")
        self._repository.heartbeat(
            "tradeagent-worker",
            self._instance_id,
            {
                "state": state,
                "mode": self._mode.value,
                **counters,
            },
            observed_at=self._clock(),
        )
