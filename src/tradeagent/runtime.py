from __future__ import annotations

import asyncio
import signal
from collections.abc import Callable
from contextlib import suppress
from datetime import UTC, datetime
from typing import Protocol
from uuid import uuid4

from pydantic import BaseModel, ConfigDict

from tradeagent.alpaca_paper import (
    AlpacaPaperAccount,
    AlpacaPaperOrder,
    AlpacaPaperPosition,
)
from tradeagent.alpaca_stream import (
    AlpacaMarketStream,
    MarketQuote,
    MarketTrade,
    ReceivedStreamEvent,
    StreamEvent,
)
from tradeagent.config import IntradayConfig
from tradeagent.domain import MarketBar
from tradeagent.intraday import NyseSessionCalendar, SessionPhase
from tradeagent.persistence import ProductionRepository
from tradeagent.scheduler import ReconciliationScheduler
from tradeagent.shadow_recorder import persist_shadow_batch, raw_already_recorded
from tradeagent.worker import AutonomousPaperWorker, WorkerMode


class RuntimeReconciliationStatus(BaseModel):
    model_config = ConfigDict(frozen=True)

    healthy: bool
    position_count: int
    open_order_count: int
    mismatches: tuple[str, ...]


class RuntimePaperClient(Protocol):
    def account(self) -> AlpacaPaperAccount: ...

    def positions(self) -> tuple[AlpacaPaperPosition, ...]: ...

    def open_orders(self) -> tuple[AlpacaPaperOrder, ...]: ...


class ProductionPaperReconciler:
    def __init__(
        self,
        client: RuntimePaperClient,
        repository: ProductionRepository,
    ) -> None:
        self._client = client
        self._repository = repository

    def reconcile(self, *, observed_at: datetime) -> RuntimeReconciliationStatus:
        account = self._client.account()
        positions = self._client.positions()
        open_orders = self._client.open_orders()
        mismatches: list[str] = []
        if account.account_blocked or account.trading_blocked:
            mismatches.append("BROKER_TRADING_BLOCKED")
        client_order_ids: set[str] = set()
        for order in open_orders:
            if order.client_order_id in client_order_ids:
                mismatches.append(f"DUPLICATE_BROKER_CLIENT_ORDER_ID:{order.client_order_id}")
            client_order_ids.add(order.client_order_id)
        status = RuntimeReconciliationStatus(
            healthy=not mismatches,
            position_count=len(positions),
            open_order_count=len(open_orders),
            mismatches=tuple(mismatches),
        )
        self._repository.append_event(
            "broker_reconciliation",
            status.model_dump(mode="json"),
            occurred_at=observed_at,
            trace_id=f"reconcile:{observed_at.isoformat()}",
        )
        if not status.healthy:
            self._repository.set_control("kill_switch", "active")
        return status


class ShadowAuditProcessor:
    def __init__(self, repository: ProductionRepository) -> None:
        self._repository = repository

    async def on_bar(self, bar: MarketBar, *, can_enter: bool) -> None:
        await self._record(bar)

    async def on_quote(self, quote: MarketQuote, *, can_enter: bool) -> None:
        await self._record(quote)

    async def on_trade(self, trade: MarketTrade, *, can_enter: bool) -> None:
        await self._record(trade)

    async def _record(self, event: StreamEvent) -> None:
        if raw_already_recorded.get():
            return
        receipt = ReceivedStreamEvent(event, datetime.now(UTC))
        await asyncio.to_thread(
            persist_shadow_batch,
            self._repository,
            [receipt],
            [],
            str(uuid4()),
        )


class MarketFeedStatusMonitor:
    def __init__(
        self,
        repository: ProductionRepository,
        intraday: IntradayConfig,
        *,
        instance_id: str,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._repository = repository
        self._calendar = NyseSessionCalendar(intraday)
        self._maximum_age_seconds = intraday.heartbeat_max_age_seconds
        self._maximum_event_age_seconds = intraday.quote_max_age_seconds
        self._instance_id = instance_id
        self._clock = clock

    def check(self) -> str:
        now = self._clock()
        phase = self._calendar.gate(now).phase
        worker = self._repository.latest_heartbeat("tradeagent-shadow-recorder")
        details = worker[2] if worker is not None else {}
        last_event_at = details.get("last_committed_event_at")

        def age_of(field: str) -> float | None:
            value = details.get(field)
            try:
                return (
                    (now - datetime.fromisoformat(value)).total_seconds()
                    if isinstance(value, str)
                    else None
                )
            except (ValueError, TypeError):
                return None

        event_age = age_of("last_committed_event_at")
        received_event_age = age_of("last_event_at")
        market_commit_age = age_of("last_market_commit_at")
        heartbeat_age = (now - worker[1]).total_seconds() if worker else None
        market_open = phase is not SessionPhase.CLOSED
        alive = (
            heartbeat_age is not None
            and 0 <= heartbeat_age <= self._maximum_age_seconds
            and details.get("state") in {"healthy", "degraded"}
        )
        healthy = (
            alive
            and details.get("healthy") is True
            and event_age is not None
            and 0 <= event_age <= self._maximum_event_age_seconds
            and received_event_age is not None
            and 0 <= received_event_age <= self._maximum_event_age_seconds
            and market_commit_age is not None
            and 0 <= market_commit_age <= self._maximum_event_age_seconds
        )
        state = "market_closed" if not market_open else "healthy" if healthy else "stale"
        self._repository.heartbeat(
            "tradeagent-shadow-market-feed",
            self._instance_id,
            {
                "state": state,
                "session_phase": phase.value,
                "healthy": healthy,
                "recorder_alive": alive,
                "last_market_event_at": last_event_at,
                "last_received_market_event_at": details.get("last_received_event_at"),
                "last_market_commit_at": details.get("last_market_commit_at"),
                "heartbeat_age_seconds": heartbeat_age,
                "event_age_seconds": event_age,
                "received_event_age_seconds": age_of("last_received_event_at"),
                "received_progress_age_seconds": received_event_age,
                "market_commit_age_seconds": market_commit_age,
                "recorder": details,
                "stale_after_seconds": self._maximum_event_age_seconds,
                "liveness_max_age_seconds": self._maximum_age_seconds,
            },
            observed_at=now,
        )
        return state

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            await asyncio.to_thread(self.check)
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=min(self._maximum_age_seconds, self._maximum_event_age_seconds / 2, 5),
                )
            except TimeoutError:
                continue


async def run_shadow_runtime(
    stream: AlpacaMarketStream,
    worker: AutonomousPaperWorker,
    scheduler: ReconciliationScheduler,
    *,
    symbols: tuple[str, ...],
    feed_monitor: MarketFeedStatusMonitor | None = None,
) -> None:
    stop_event = asyncio.Event()
    stream.on_status = worker.observe_stream_status
    loop = asyncio.get_running_loop()
    previous_sigterm = signal.getsignal(signal.SIGTERM)
    handles_sigterm = False
    if worker.mode is WorkerMode.SHADOW:
        with suppress(NotImplementedError, RuntimeError):
            loop.add_signal_handler(signal.SIGTERM, stop_event.set)
            handles_sigterm = True

    async def run_worker() -> None:
        try:
            await worker.run(
                stream.received_events(symbols), stop_event=stop_event, stream_health=stream.health
            )
        finally:
            stop_event.set()

    try:
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(run_worker())
            # A recorder is not a broker execution role. Its lifecycle must not control
            # the global/operator kill switch through broker reconciliation or watchdogs.
            if worker.mode is WorkerMode.AUTONOMOUS_PAPER:
                tasks.create_task(scheduler.run(stop_event))
            if feed_monitor is not None:
                tasks.create_task(feed_monitor.run(stop_event))
    finally:
        stream.on_status = None
        if handles_sigterm:
            loop.remove_signal_handler(signal.SIGTERM)
            signal.signal(signal.SIGTERM, previous_sigterm)
        if feed_monitor is not None:
            # The worker's terminal heartbeat is durable before TaskGroup exits. Do
            # not leave a previous healthy feed snapshot behind on a routine stop.
            await asyncio.to_thread(feed_monitor.check)
