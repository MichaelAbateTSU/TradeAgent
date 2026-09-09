from __future__ import annotations

import asyncio
import logging
from collections import deque
from collections.abc import Awaitable, Callable, Sequence
from contextlib import suppress
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import Table, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import SQLAlchemyError

from tradeagent.alpaca_stream import MarketQuote, ReceivedStreamEvent
from tradeagent.domain import MarketBar
from tradeagent.persistence import (
    ProductionRepository,
    append_reporting_metadata,
    events,
    market_bars,
    market_quotes,
    market_trades,
)

raw_already_recorded: ContextVar[bool] = ContextVar("shadow_raw_already_recorded", default=False)
logger = logging.getLogger(__name__)
_INSERT_PAGE_SIZE = 500


class ShadowRecorderSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SHADOW_RECORDER_", extra="ignore", frozen=True)

    queue_capacity: int = Field(default=20000, gt=0)
    batch_size: int = Field(default=500, gt=0)
    flush_interval_seconds: float = Field(default=0.25, gt=0)
    heartbeat_interval_seconds: float = Field(default=5, gt=0)
    retry_initial_seconds: float = Field(default=0.5, gt=0)
    retry_max_seconds: float = Field(default=10, gt=0)
    retry_attempts: int = Field(default=8, gt=0)


@dataclass(frozen=True)
class BatchWriteResult:
    inserted: int
    duplicates: int


@dataclass
class ShadowDecisionBatchResult:
    bars: int = 0
    quotes: int = 0
    trades: int = 0
    errors: list[str] = field(default_factory=list)


def persist_shadow_batch(
    repository: ProductionRepository,
    receipts: Sequence[ReceivedStreamEvent],
    notices: list[dict[str, Any]],
    batch_id: str,
    *,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    instance_id: str | None = None,
) -> BatchWriteResult:
    """Append raw rows and one batch audit atomically; retries never rewrite receipt times."""
    processed_at = clock()
    rows: dict[Table, list[dict[str, Any]]] = {
        market_bars: [],
        market_quotes: [],
        market_trades: [],
    }
    for receipt in receipts:
        event = receipt.event
        common = {
            "symbol": event.symbol,
            "feed_source": "iex",
            "event_at": event.timestamp,
            "received_at": receipt.received_at,
            "processed_at": processed_at,
        }
        if isinstance(event, MarketBar):
            rows[market_bars].append(
                {
                    **common,
                    "bar_id": str(uuid4()),
                    "timeframe": "1Min",
                    "open": event.open,
                    "high": event.high,
                    "low": event.low,
                    "close": event.close,
                    "volume": event.volume,
                }
            )
        elif isinstance(event, MarketQuote):
            rows[market_quotes].append(
                {
                    **common,
                    "quote_id": str(uuid4()),
                    "bid_price": event.bid_price,
                    "ask_price": event.ask_price,
                    "bid_size": event.bid_size,
                    "ask_size": event.ask_size,
                    "bid_exchange": event.bid_exchange,
                    "ask_exchange": event.ask_exchange,
                }
            )
        else:
            rows[market_trades].append(
                {
                    **common,
                    "market_trade_id": str(uuid4()),
                    "provider_trade_id": str(event.trade_id),
                    "price": event.price,
                    "size": event.size,
                    "exchange": event.exchange,
                    "conditions": list(event.conditions),
                    "tape": event.tape,
                }
            )
    inserted = 0
    # Use the repository's existing engine/pool, not another database or per-packet connection.
    with repository._database.begin() as connection:
        committed_batch = connection.scalar(
            select(events.c.payload).where(events.c.event_id == batch_id)
        )
        if committed_batch is not None:
            # A lost COMMIT acknowledgement is not a duplicate market-data delivery.
            return BatchWriteResult(
                inserted=int(committed_batch["inserted"]),
                duplicates=int(committed_batch["duplicates"]),
            )
        insert = pg_insert if connection.dialect.name == "postgresql" else sqlite_insert
        for table, values in rows.items():
            if values:
                # Parameter batches avoid rebuilding thousands of SQL expressions
                # per flush; insertmanyvalues retains bulk RETURNING/conflict semantics.
                result = connection.execute(
                    insert(table)
                    .on_conflict_do_nothing()
                    .returning(next(iter(table.primary_key.columns))),
                    values,
                    execution_options={"insertmanyvalues_page_size": _INSERT_PAGE_SIZE},
                )
                inserted += len(result.fetchall())
        audit = list(notices)
        if receipts:
            audit.append(
                {
                    "event_id": batch_id,
                    "event_type": "shadow_recorder_batch",
                    "occurred_at": processed_at,
                    "recorded_at": processed_at,
                    "trace_id": f"shadow-batch:{batch_id}",
                    "payload": {
                        "received": len(receipts),
                        "inserted": inserted,
                        "duplicates": len(receipts) - inserted,
                        "first_event_at": min(
                            item.event.timestamp for item in receipts
                        ).isoformat(),
                        "last_event_at": max(item.event.timestamp for item in receipts).isoformat(),
                        "first_received_at": receipts[0].received_at.isoformat(),
                        "last_received_at": receipts[-1].received_at.isoformat(),
                        "processing_started_at": processed_at.isoformat(),
                        "execution_enabled": False,
                        "instance_id": instance_id,
                    },
                }
            )
        if audit:
            # Deduplicate within this input as well as against the database: an
            # original ID always keeps its first body, including its projection.
            audit_by_id: dict[str, dict[str, Any]] = {}
            for row in audit:
                audit_by_id.setdefault(str(row["event_id"]), row)
            created_ids = connection.scalars(
                insert(events).on_conflict_do_nothing().returning(events.c.event_id),
                list(audit_by_id.values()),
                execution_options={"insertmanyvalues_page_size": _INSERT_PAGE_SIZE},
            ).all()
            for event_id in created_ids:
                original = audit_by_id[str(event_id)]
                append_reporting_metadata(
                    connection, str(event_id), original["event_type"], original["payload"]
                )
    return BatchWriteResult(inserted=inserted, duplicates=len(receipts) - inserted)


class ShadowBatchRecorder:
    def __init__(
        self,
        repository: ProductionRepository,
        *,
        settings: ShadowRecorderSettings | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        after_commit: Callable[[Sequence[ReceivedStreamEvent]], Awaitable[None]] | None = None,
        instance_id: str | None = None,
    ) -> None:
        self.repository = repository
        self.settings = settings or ShadowRecorderSettings()
        self.clock = clock
        self.after_commit = after_commit
        self.instance_id = instance_id
        self.queue: deque[ReceivedStreamEvent] = deque()
        self._notices: deque[dict[str, Any]] = deque()
        self._overflow: dict[str, Any] | None = None
        self._wake = asyncio.Event()
        self.received = 0
        self.committed = 0
        self.inserted = 0
        self.duplicates = 0
        self.dropped = 0
        self.gaps = 0
        self.notice_overflow = 0
        self.in_flight = 0
        self.late_events = 0
        self.future_events = 0
        self.decision_errors = 0
        self.persistence_error: str | None = None
        self.last_received_at: datetime | None = None
        self.last_event_at: datetime | None = None
        self.last_received_event_at: datetime | None = None
        self.last_commit_at: datetime | None = None
        self.last_market_commit_at: datetime | None = None
        self.last_committed_event_at: datetime | None = None
        self.last_committed_received_at: datetime | None = None
        self.last_commit_lag_seconds: float | None = None
        self.last_batch_write_seconds: float | None = None
        self.max_receive_lag_seconds: float | None = None
        self._in_flight_received_at: datetime | None = None

    def offer(self, receipt: ReceivedStreamEvent) -> bool:
        self.received += 1
        self.last_received_at = receipt.received_at
        self.last_received_event_at = receipt.event.timestamp
        self.last_event_at = max(
            self.last_event_at or receipt.event.timestamp, receipt.event.timestamp
        )
        lag = (receipt.received_at - receipt.event.timestamp).total_seconds()
        self.max_receive_lag_seconds = (
            lag if self.max_receive_lag_seconds is None else max(self.max_receive_lag_seconds, lag)
        )
        if len(self.queue) >= self.settings.queue_capacity:
            self.dropped += 1
            if self._overflow is None:
                self.gaps += 1
                self._overflow = {
                    "reason": "queue_overflow",
                    "dropped_events": 0,
                    "first_received_at": receipt.received_at.isoformat(),
                    "first_event_at": receipt.event.timestamp.isoformat(),
                }
                logger.error(
                    "Shadow recorder queue overflow; data gap, capacity=%s",
                    self.settings.queue_capacity,
                )
            self._overflow.update(
                dropped_events=self._overflow["dropped_events"] + 1,
                last_received_at=receipt.received_at.isoformat(),
                last_event_at=receipt.event.timestamp.isoformat(),
            )
            self._wake.set()
            return False
        self.queue.append(receipt)
        self._wake.set()
        return True

    def notice(self, event_type: str, payload: dict[str, object]) -> None:
        now = self.clock()
        if len(self._notices) >= 256:
            self.notice_overflow += 1
            logger.error("Shadow recorder notice queue overflow: %s", event_type)
            return
        event_id = str(uuid4())
        self._notices.append(
            {
                "event_id": event_id,
                "event_type": event_type,
                "occurred_at": now,
                "recorded_at": now,
                "trace_id": f"shadow-status:{event_id}",
                "payload": payload,
            }
        )
        self._wake.set()

    def stream_status(self, status: dict[str, object]) -> None:
        if status.get("state") in {"reconnecting", "failed"} and status.get("gap"):
            self.gaps += 1
        self.notice("shadow_stream_status", status)

    def health(self) -> dict[str, object]:
        now = self.clock()
        oldest = self._in_flight_received_at
        if oldest is None and self.queue:
            oldest = self.queue[0].received_at
        return {
            "received": self.received,
            "committed": self.committed,
            "inserted": self.inserted,
            "duplicates": self.duplicates,
            "queue_depth": len(self.queue),
            "queue_capacity": self.settings.queue_capacity,
            "in_flight": self.in_flight,
            "gaps": self.gaps,
            "dropped_events": self.dropped,
            "notice_overflow": self.notice_overflow,
            "late_events": self.late_events,
            "future_events": self.future_events,
            "decision_errors": self.decision_errors,
            "persistence_error": self.persistence_error,
            "last_received_at": self.last_received_at.isoformat()
            if self.last_received_at
            else None,
            "last_event_at": self.last_event_at.isoformat() if self.last_event_at else None,
            "last_received_event_at": (
                self.last_received_event_at.isoformat() if self.last_received_event_at else None
            ),
            "last_commit_at": self.last_commit_at.isoformat() if self.last_commit_at else None,
            "last_market_commit_at": (
                self.last_market_commit_at.isoformat() if self.last_market_commit_at else None
            ),
            "last_committed_event_at": (
                self.last_committed_event_at.isoformat() if self.last_committed_event_at else None
            ),
            "last_committed_received_at": (
                self.last_committed_received_at.isoformat()
                if self.last_committed_received_at
                else None
            ),
            "receive_lag_seconds": (
                (self.last_received_at - self.last_received_event_at).total_seconds()
                if self.last_received_at and self.last_received_event_at
                else None
            ),
            "event_age_seconds": (
                (now - self.last_event_at).total_seconds() if self.last_event_at else None
            ),
            "receive_age_seconds": (
                (now - self.last_received_at).total_seconds() if self.last_received_at else None
            ),
            "committed_event_age_seconds": (
                (now - self.last_committed_event_at).total_seconds()
                if self.last_committed_event_at
                else None
            ),
            "market_commit_age_seconds": (
                (now - self.last_market_commit_at).total_seconds()
                if self.last_market_commit_at
                else None
            ),
            "exchange_to_commit_lag_seconds": (
                (self.last_market_commit_at - self.last_committed_event_at).total_seconds()
                if self.last_market_commit_at and self.last_committed_event_at
                else None
            ),
            "max_receive_lag_seconds": self.max_receive_lag_seconds,
            "oldest_uncommitted_age_seconds": (now - oldest).total_seconds() if oldest else 0.0,
            "commit_lag_seconds": self.last_commit_lag_seconds,
            "batch_write_seconds": self.last_batch_write_seconds,
        }

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set() or self.queue or self._notices or self._overflow:
            if not stop_event.is_set() and len(self.queue) < self.settings.batch_size:
                # A timed flush is essential for quiet and steady feeds, not only full bursts.
                await asyncio.sleep(self.settings.flush_interval_seconds)
            if not self.queue and not self._notices and self._overflow is None:
                self._wake.clear()
                with suppress(TimeoutError):
                    await asyncio.wait_for(
                        self._wake.wait(), timeout=self.settings.flush_interval_seconds
                    )
                continue
            receipts = [
                self.queue.popleft() for _ in range(min(len(self.queue), self.settings.batch_size))
            ]
            notices = list(self._notices)
            self._notices.clear()
            if self._overflow is not None:
                self.notice("shadow_recorder_gap", dict(self._overflow))
                self._overflow = None
            notices.extend(self._notices)
            self._notices.clear()
            self.in_flight = len(receipts)
            self._in_flight_received_at = receipts[0].received_at if receipts else None
            batch_id = str(uuid4())
            delay = min(self.settings.retry_initial_seconds, self.settings.retry_max_seconds)
            started = asyncio.get_running_loop().time()
            for attempt in range(self.settings.retry_attempts):
                try:
                    result = await asyncio.to_thread(
                        persist_shadow_batch,
                        self.repository,
                        receipts,
                        notices,
                        batch_id,
                        clock=self.clock,
                        instance_id=self.instance_id,
                    )
                    break
                except SQLAlchemyError as exc:
                    self.persistence_error = type(exc).__name__
                    logger.error(
                        "Shadow persistence failed: %s, attempt=%s",
                        self.persistence_error,
                        attempt + 1,
                    )
                    if attempt + 1 == self.settings.retry_attempts:
                        raise
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, self.settings.retry_max_seconds)
            self.persistence_error = None
            self.last_batch_write_seconds = asyncio.get_running_loop().time() - started
            self.last_commit_at = self.clock()
            self.committed += len(receipts)
            self.inserted += result.inserted
            self.duplicates += result.duplicates
            if receipts:
                self.last_market_commit_at = self.last_commit_at
                batch_event_at = max(receipt.event.timestamp for receipt in receipts)
                self.last_committed_event_at = max(
                    self.last_committed_event_at or batch_event_at, batch_event_at
                )
                self.last_committed_received_at = receipts[-1].received_at
                self.last_commit_lag_seconds = (
                    self.last_commit_at - receipts[0].received_at
                ).total_seconds()
            self.in_flight = 0
            self._in_flight_received_at = None
            if receipts and self.after_commit:
                await self.after_commit(receipts)
