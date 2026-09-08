from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select

from tradeagent.persistence import ProductionRepository, events, heartbeats, worker_locks


@dataclass(frozen=True)
class ShadowRecorderSnapshot:
    heartbeat: dict[str, Any] | None
    lease: dict[str, Any] | None
    batch: dict[str, Any] | None


def read_shadow_recorder_snapshot(
    repository: ProductionRepository,
    *,
    batch_since: datetime,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> ShadowRecorderSnapshot:
    """Read bounded, durable observations without renewing any lease or heartbeat."""
    with repository._database.begin() as connection:
        heartbeat = (
            connection.execute(
                select(
                    heartbeats.c.instance_id, heartbeats.c.observed_at, heartbeats.c.details
                ).where(heartbeats.c.service_name == "tradeagent-shadow-recorder")
            )
            .mappings()
            .one_or_none()
        )
        lease = (
            connection.execute(
                select(worker_locks.c.owner_id, worker_locks.c.acquired_at).where(
                    worker_locks.c.lock_name == "tradeagent-shadow-recorder"
                )
            )
            .mappings()
            .one_or_none()
        )
        batch: dict[str, Any] | None = None
        best_progress: tuple[datetime, datetime, str] | None = None
        scan_until = clock()
        if heartbeat is not None:
            # Processing order is not exchange order: a newer minute-bar/delayed
            # packet batch must not erase a still-fresh quote. Scan the entire
            # recent type/time interval, buffering one compact row and one winner.
            result = connection.execute(
                select(events.c.event_id, events.c.occurred_at, events.c.payload)
                .where(
                    events.c.event_type == "shadow_recorder_batch",
                    events.c.occurred_at >= batch_since,
                    events.c.occurred_at <= scan_until,
                    events.c.payload["instance_id"].as_string() == heartbeat["instance_id"],
                )
                .execution_options(stream_results=True, yield_per=1, max_row_buffer=1)
            )
            try:
                for row in result.mappings():
                    candidate = dict(row)
                    progress = _valid_market_progress(
                        candidate, owner=heartbeat["instance_id"], observed_at=scan_until
                    )
                    if progress is not None and (best_progress is None or progress > best_progress):
                        batch, best_progress = candidate, progress
            finally:
                result.close()
    return ShadowRecorderSnapshot(
        heartbeat=dict(heartbeat) if heartbeat else None,
        lease=dict(lease) if lease else None,
        batch=batch,
    )


def _valid_market_progress(
    batch: dict[str, Any], *, owner: str, observed_at: datetime
) -> tuple[datetime, datetime, str] | None:
    payload = batch["payload"]
    if not isinstance(payload, dict) or payload.get("instance_id") != owner:
        return None
    received = payload.get("received")
    inserted = payload.get("inserted")
    duplicates = payload.get("duplicates")
    if not (
        isinstance(received, int)
        and not isinstance(received, bool)
        and isinstance(inserted, int)
        and not isinstance(inserted, bool)
        and isinstance(duplicates, int)
        and not isinstance(duplicates, bool)
        and received > 0
        and inserted >= 0
        and duplicates >= 0
        and received == inserted + duplicates
    ):
        return None
    event_at = payload_timestamp(payload.get("last_event_at"))
    received_at = payload_timestamp(payload.get("last_received_at"))
    processing_at = payload_timestamp(payload.get("processing_started_at"))
    row_at = stored_timestamp(batch["occurred_at"])
    if not (
        event_at
        and received_at
        and processing_at
        and row_at
        and event_at <= received_at <= processing_at == row_at <= observed_at
    ):
        return None
    return event_at, processing_at, str(batch["event_id"])


def stored_timestamp(value: object) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def payload_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return None
        return parsed.astimezone(UTC)
    except (ValueError, OverflowError):
        return None
