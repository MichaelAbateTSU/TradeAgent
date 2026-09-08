from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import suppress
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict

from tradeagent.alpaca_stream import MarketQuote, MarketTrade, ReceivedStreamEvent, StreamEvent
from tradeagent.config import AppConfig
from tradeagent.domain import MarketBar
from tradeagent.intraday import NyseSessionCalendar, SessionPhase
from tradeagent.persistence import ProductionRepository
from tradeagent.shadow_recorder import (
    ShadowBatchRecorder,
    ShadowDecisionBatchResult,
    ShadowRecorderSettings,
    raw_already_recorded,
)


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

    async def on_trade(self, trade: MarketTrade, *, can_enter: bool) -> None: ...


@runtime_checkable
class RecordedShadowBatchProcessor(Protocol):
    async def on_recorded_batch(
        self, receipts: Sequence[ReceivedStreamEvent]
    ) -> ShadowDecisionBatchResult: ...

    def derived_health(self, *, observed_at: datetime) -> dict[str, object]: ...

    def finish_recorded_batches(self, *, observed_at: datetime) -> None: ...


class WorkerRunResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    mode: WorkerMode
    events_seen: int
    bars_processed: int
    quotes_processed: int
    trades_processed: int
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
        recorder_settings: ShadowRecorderSettings | None = None,
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
        self._recorder_settings = recorder_settings
        self._recorder: ShadowBatchRecorder | None = None

    @property
    def mode(self) -> WorkerMode:
        return self._mode

    def observe_stream_status(self, status: dict[str, object]) -> None:
        if self._recorder is not None:
            self._recorder.stream_status(status)

    async def run(
        self,
        events: AsyncIterator[StreamEvent | ReceivedStreamEvent],
        *,
        stop_event: asyncio.Event | None = None,
        stream_health: Callable[[], dict[str, object]] | None = None,
    ) -> WorkerRunResult:
        if self._mode is WorkerMode.SHADOW:
            return await self._run_shadow(
                events, stop_event=stop_event, stream_health=stream_health
            )
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
            "trades": 0,
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

            async for incoming in events:
                if stop_event is not None and stop_event.is_set():
                    break
                event = incoming.event if isinstance(incoming, ReceivedStreamEvent) else incoming
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
                elif isinstance(event, MarketQuote):
                    await self._processor.on_quote(event, can_enter=can_enter)
                    counters["quotes"] += 1
                else:
                    await self._processor.on_trade(event, can_enter=can_enter)
                    counters["trades"] += 1
                self._heartbeat("running", counters)

            stopped_at = self._clock()
            return WorkerRunResult(
                mode=self._mode,
                events_seen=counters["events"],
                bars_processed=counters["bars"],
                quotes_processed=counters["quotes"],
                trades_processed=counters["trades"],
                closed_session_events=counters["closed"],
                stopped_at=stopped_at,
            )
        finally:
            self._repository.set_control("kill_switch", "active")
            self._heartbeat("stopped", counters)
            self._repository.release_worker_lock("tradeagent-paper-worker", self._instance_id)

    async def _run_shadow(
        self,
        events: AsyncIterator[StreamEvent | ReceivedStreamEvent],
        *,
        stop_event: asyncio.Event | None,
        stream_health: Callable[[], dict[str, object]] | None,
    ) -> WorkerRunResult:
        lock_name = "tradeagent-shadow-recorder"
        if not await asyncio.to_thread(
            self._repository.acquire_worker_lock,
            lock_name,
            self._instance_id,
            stale_after_seconds=self._config.intraday.heartbeat_max_age_seconds * 2,
        ):
            raise WorkerStartupError("another shadow recorder owns the recording lock")
        counters = {"events": 0, "bars": 0, "quotes": 0, "trades": 0, "closed": 0}
        stopping = asyncio.Event()
        recorder = ShadowBatchRecorder(
            self._repository,
            settings=self._recorder_settings,
            clock=self._clock,
            instance_id=self._instance_id,
        )
        self._recorder = recorder

        async def after_commit(receipts: Sequence[ReceivedStreamEvent]) -> None:
            errors = await asyncio.to_thread(self._shadow_decisions, receipts, counters)
            for error in errors:
                recorder.decision_errors += 1
                recorder.notice("shadow_decision_error", {"error_type": error})

        recorder.after_commit = after_commit

        async def receive() -> None:
            try:
                async for incoming in events:
                    if stop_event is not None and stop_event.is_set():
                        return
                    receipt = (
                        incoming
                        if isinstance(incoming, ReceivedStreamEvent)
                        else ReceivedStreamEvent(incoming, self._clock())
                    )
                    counters["events"] += 1
                    problem = self._freshness_problem(receipt.event, receipt.received_at)
                    if problem is not None:
                        if receipt.event.timestamp > receipt.received_at:
                            recorder.future_events += 1
                        else:
                            recorder.late_events += 1
                    recorder.offer(receipt)
                    # Some sources produce large frames without an await between events.
                    if counters["events"] % recorder.settings.batch_size == 0:
                        await asyncio.sleep(0)
            finally:
                close = getattr(events, "aclose", None)
                if close is not None:
                    await close()

        async def pulse() -> None:
            while not stopping.is_set():
                await heartbeat("running")
                with suppress(TimeoutError):
                    await asyncio.wait_for(
                        stopping.wait(),
                        timeout=min(
                            recorder.settings.heartbeat_interval_seconds,
                            self._config.intraday.heartbeat_max_age_seconds / 2,
                        ),
                    )

        async def heartbeat(lifecycle: str) -> None:
            now = self._clock()
            details = recorder.health()
            source = stream_health() if stream_health is not None else {}
            age = details["receive_age_seconds"]
            event_age = details["event_age_seconds"]
            committed_age = details["committed_event_age_seconds"]
            market_commit_age = details["market_commit_age_seconds"]
            queue_age = details["oldest_uncommitted_age_seconds"]
            receive_lag = details["receive_lag_seconds"]
            # A minute bar carries its interval's opening timestamp. Keep its full
            # receipt lag, but assess feed freshness using confirmed exchange progress;
            # per-event execution freshness validation remains separate and strict.
            healthy = (
                lifecycle == "running"
                and source.get("state") == "subscribed"
                and recorder.committed > 0
                and recorder.persistence_error is None
                and recorder.dropped == 0
                and recorder.decision_errors == 0
                and recorder.notice_overflow == 0
                and isinstance(age, (int, float))
                and 0 <= age <= self._config.intraday.heartbeat_max_age_seconds
                and isinstance(event_age, (int, float))
                and 0 <= event_age <= self._config.intraday.quote_max_age_seconds
                and isinstance(committed_age, (int, float))
                and 0 <= committed_age <= self._config.intraday.quote_max_age_seconds
                and isinstance(market_commit_age, (int, float))
                and 0 <= market_commit_age <= self._config.intraday.quote_max_age_seconds
                and isinstance(queue_age, (int, float))
                and queue_age <= self._config.intraday.quote_max_age_seconds
                and isinstance(receive_lag, (int, float))
                and receive_lag >= 0
            )
            refreshed = await asyncio.to_thread(
                self._repository.refresh_worker_lock, lock_name, self._instance_id, observed_at=now
            )
            if lifecycle == "running" and not refreshed:
                raise WorkerStartupError("shadow recorder lost its ownership lease")
            await asyncio.to_thread(
                self._repository.heartbeat,
                lock_name,
                self._instance_id,
                {
                    **details,
                    **counters,
                    "state": lifecycle
                    if lifecycle != "running"
                    else "healthy"
                    if healthy
                    else "degraded",
                    "healthy": healthy,
                    "mode": "shadow",
                    "stream": source,
                    "derived": (
                        self._processor.derived_health(observed_at=now)
                        if isinstance(self._processor, RecordedShadowBatchProcessor)
                        else None
                    ),
                    "execution_enabled": False,
                },
                observed_at=now,
            )

        tasks: list[asyncio.Task[object]] = []
        writer: asyncio.Task[None] | None = None
        liveness: asyncio.Task[None] | None = None
        lifecycle = "stopped"
        try:
            previous = await asyncio.to_thread(self._repository.latest_heartbeat, lock_name)
            recorder.notice(
                "shadow_recorder_lifecycle",
                {
                    "state": "starting",
                    "replay_available": False,
                },
            )
            if previous is not None:
                recorder.gaps += 1
                recorder.notice(
                    "shadow_recorder_gap",
                    {
                        "reason": "process_restart_without_replay",
                        "last_committed_received_at": previous[2].get("last_committed_received_at"),
                        "previous_heartbeat_at": previous[1].isoformat(),
                        "recording_resumed_at": self._clock().isoformat(),
                        "lost_events": None,
                    },
                )
            await heartbeat("starting")
            reader = asyncio.create_task(receive())
            writer = asyncio.create_task(recorder.run(stopping))
            liveness = asyncio.create_task(pulse())
            tasks.extend((reader, writer, liveness))
            if stop_event is not None:
                tasks.append(asyncio.create_task(stop_event.wait()))
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
            reader.cancel()
            await asyncio.gather(reader, return_exceptions=True)
            stopping.set()
            await asyncio.shield(writer)
            await asyncio.shield(liveness)
            return WorkerRunResult(
                mode=self._mode,
                events_seen=counters["events"],
                bars_processed=counters["bars"],
                quotes_processed=counters["quotes"],
                trades_processed=counters["trades"],
                closed_session_events=counters["closed"],
                stopped_at=self._clock(),
            )
        except BaseException:
            lifecycle = "failed"
            raise
        finally:
            stopping.set()
            # Let an in-flight transaction finish before the caller disposes its engine.
            for task in tasks:
                if task not in (writer, liveness):
                    task.cancel()
            await asyncio.gather(
                *(task for task in tasks if task not in (writer, liveness)),
                return_exceptions=True,
            )
            if writer is not None:
                try:
                    await asyncio.shield(writer)
                except Exception as exc:
                    logging.getLogger(__name__).error(
                        "Shadow recorder failed with uncommitted data: %s; %s",
                        type(exc).__name__,
                        recorder.health(),
                    )
            if liveness is not None:
                # Cancelling to_thread doesn't cancel SQL. Finish the old pulse before
                # publishing the terminal state so it cannot overwrite "stopped"/"failed".
                await asyncio.gather(asyncio.shield(liveness), return_exceptions=True)
            try:
                if isinstance(self._processor, RecordedShadowBatchProcessor):
                    try:
                        await asyncio.to_thread(
                            self._processor.finish_recorded_batches, observed_at=self._clock()
                        )
                    except Exception as exc:
                        lifecycle = "failed"
                        recorder.decision_errors += 1
                        logging.getLogger(__name__).error(
                            "Shadow derived finalization failed: %s",
                            type(exc).__name__,
                        )
                await heartbeat(lifecycle)
            finally:
                await asyncio.to_thread(
                    self._repository.release_worker_lock, lock_name, self._instance_id
                )

    def _shadow_decisions(
        self, receipts: Sequence[ReceivedStreamEvent], counters: dict[str, int]
    ) -> list[str]:
        async def process() -> list[str]:
            errors: list[str] = []
            token = raw_already_recorded.set(True)
            try:
                eligible: list[ReceivedStreamEvent] = []
                for receipt in receipts:
                    event = receipt.event
                    if self._calendar.gate(event.timestamp).phase is SessionPhase.CLOSED:
                        counters["closed"] += 1
                        continue
                    # Raw evidence is retained, but late/future packets never enter a strategy.
                    if (
                        self._freshness_problem(event, receipt.received_at) is not None
                        or self._freshness_problem(event, self._clock()) is not None
                    ):
                        continue
                    eligible.append(receipt)
                if isinstance(self._processor, RecordedShadowBatchProcessor):
                    result = await self._processor.on_recorded_batch(eligible)
                    counters["bars"] += result.bars
                    counters["quotes"] += result.quotes
                    counters["trades"] += result.trades
                    return result.errors
                for receipt in eligible:
                    event = receipt.event
                    try:
                        if isinstance(event, MarketBar):
                            await self._processor.on_bar(event, can_enter=False)
                            counters["bars"] += 1
                        elif isinstance(event, MarketQuote):
                            await self._processor.on_quote(event, can_enter=False)
                            counters["quotes"] += 1
                        else:
                            await self._processor.on_trade(event, can_enter=False)
                            counters["trades"] += 1
                    except Exception as exc:
                        errors.append(type(exc).__name__)
            finally:
                raw_already_recorded.reset(token)
            return errors

        return asyncio.run(process())

    def _validate_freshness(
        self,
        event: StreamEvent,
        observed_at: datetime,
    ) -> None:
        problem = self._freshness_problem(event, observed_at)
        if problem is not None:
            self._repository.set_control("kill_switch", "active")
            raise StaleMarketDataError(problem)

    def _freshness_problem(self, event: StreamEvent, observed_at: datetime) -> str | None:
        if observed_at < event.timestamp:
            return "market event timestamp is in the future"
        age = (observed_at - event.timestamp).total_seconds()
        maximum_age = (
            self._config.intraday.bar_max_age_seconds
            if isinstance(event, MarketBar)
            else self._config.intraday.quote_max_age_seconds
        )
        if age > maximum_age:
            return f"{type(event).__name__} is stale by {age:.3f} seconds"
        return None

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
