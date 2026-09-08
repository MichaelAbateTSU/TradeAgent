from __future__ import annotations

import asyncio
import signal
from collections.abc import Callable
from contextlib import suppress
from datetime import UTC, datetime, timedelta
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
from tradeagent.shadow_health import (
    payload_timestamp,
    read_shadow_recorder_snapshot,
    stored_timestamp,
)
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
        snapshot = read_shadow_recorder_snapshot(
            self._repository,
            batch_since=self._clock() - timedelta(seconds=self._maximum_event_age_seconds),
        )
        # A heartbeat/batch can advance during the read. Never compare that new
        # observation to a request-start clock, or rewrite its own timestamp.
        now = self._clock()
        phase = self._calendar.gate(now).phase
        worker, lease, batch = snapshot.heartbeat, snapshot.lease, snapshot.batch
        details = worker["details"] if worker and isinstance(worker["details"], dict) else {}
        heartbeat_at = stored_timestamp(worker["observed_at"]) if worker else None
        lease_at = stored_timestamp(lease["acquired_at"]) if lease else None
        heartbeat_age = (now - heartbeat_at).total_seconds() if heartbeat_at else None
        lease_age = (now - lease_at).total_seconds() if lease_at else None
        commit_at = payload_timestamp(details.get("last_market_commit_at"))
        owner_matches = bool(worker and lease and worker["instance_id"] == lease["owner_id"])
        market_open = phase is not SessionPhase.CLOSED
        alive = (
            heartbeat_age is not None
            and 0 <= heartbeat_age <= self._maximum_age_seconds
            and details.get("state") in {"healthy", "degraded"}
            and owner_matches
            and lease_age is not None
            and 0 <= lease_age <= self._maximum_age_seconds * 2
        )
        recorder_ready = (
            alive
            and details.get("state") == "healthy"
            and details.get("healthy") is True
            and commit_at is not None
            and commit_at <= now
            and details.get("persistence_error") is None
            and all(
                details.get(key, 0) == 0
                for key in (
                    "dropped_events",
                    "notice_overflow",
                    "decision_errors",
                )
            )
        )
        payload = batch["payload"] if batch and isinstance(batch["payload"], dict) else {}
        event_at = payload_timestamp(payload.get("last_event_at"))
        received_at = payload_timestamp(payload.get("last_received_at"))
        processing_at = payload_timestamp(payload.get("processing_started_at"))
        row_at = stored_timestamp(batch["occurred_at"]) if batch else None
        batch_owner_matches = bool(
            owner_matches and worker and payload.get("instance_id") == worker["instance_id"]
        )
        received_count = payload.get("received")
        inserted_count = payload.get("inserted")
        duplicate_count = payload.get("duplicates")
        valid_counts = (
            isinstance(received_count, int)
            and not isinstance(received_count, bool)
            and isinstance(inserted_count, int)
            and not isinstance(inserted_count, bool)
            and isinstance(duplicate_count, int)
            and not isinstance(duplicate_count, bool)
            and received_count > 0
            and inserted_count >= 0
            and duplicate_count >= 0
            and received_count == inserted_count + duplicate_count
        )
        event_age = (now - event_at).total_seconds() if event_at else None
        valid_times = bool(
            event_at
            and received_at
            and processing_at
            and row_at
            and event_at <= received_at <= processing_at == row_at <= now
        )
        # Ordered exchange/receipt/processing times inside the unchanged freshness
        # window, plus row visibility, prove a recent completed market commit.
        # Processing start is only a lower bound on COMMIT time, never a substitute timestamp.
        durable_fresh = (
            valid_times
            and event_age is not None
            and 0 <= event_age <= self._maximum_event_age_seconds
        )
        healthy = bool(recorder_ready and batch_owner_matches and valid_counts and durable_fresh)
        reason = (
            "recorder_not_current_and_healthy"
            if not recorder_ready
            else "durable_market_batch_missing"
            if not batch
            else "durable_batch_owner_mismatch"
            if not batch_owner_matches
            else "durable_batch_has_no_valid_market_count"
            if not valid_counts
            else "durable_batch_timestamp_invalid_or_future"
            if not valid_times
            else "durable_exchange_progress_stale"
            if not durable_fresh
            else "fresh_durable_market_batch"
        )
        proof = {
            "event_id": str(batch["event_id"]) if batch else None,
            "instance_id": payload.get("instance_id") if batch_owner_matches else None,
            "owner_matches": batch_owner_matches,
            "market_events": received_count if valid_counts else None,
            "last_event_at": event_at.isoformat() if event_at else None,
            "last_received_at": received_at.isoformat() if received_at else None,
            "processing_started_at": processing_at.isoformat() if processing_at else None,
            "event_age_seconds": event_age,
            "received_age_seconds": ((now - received_at).total_seconds() if received_at else None),
            "processing_age_seconds": (
                (now - processing_at).total_seconds() if processing_at else None
            ),
            "visible_in_database": batch is not None,
        }
        state = "market_closed" if not market_open else "healthy" if healthy else "stale"
        self._repository.heartbeat(
            "tradeagent-shadow-market-feed",
            self._instance_id,
            {
                "state": state,
                "session_phase": phase.value,
                "healthy": healthy,
                "recorder_alive": alive,
                "freshness_basis": "durable_market_batch",
                "freshness_reason": reason,
                "durable_batch": proof,
                "recorder_heartbeat_at": heartbeat_at.isoformat() if heartbeat_at else None,
                "recorder_lease_age_seconds": lease_age,
                "last_market_event_at": event_at.isoformat() if event_at else None,
                "last_received_market_event_at": details.get("last_received_event_at"),
                "last_market_commit_at": details.get("last_market_commit_at"),
                "heartbeat_age_seconds": heartbeat_age,
                "event_age_seconds": event_age,
                "market_commit_age_seconds": (now - commit_at).total_seconds()
                if commit_at
                else None,
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
