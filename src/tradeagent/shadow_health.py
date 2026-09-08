from __future__ import annotations

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
    repository: ProductionRepository, *, batch_since: datetime
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
        # This fixed event type contains only a compact batch summary, never raw
        # market packets/report bodies. The existing type/time index bounds this read.
        batch = (
            connection.execute(
                select(events.c.event_id, events.c.occurred_at, events.c.payload)
                .where(
                    events.c.event_type == "shadow_recorder_batch",
                    events.c.occurred_at >= batch_since,
                    events.c.payload["instance_id"].as_string()
                    == (heartbeat["instance_id"] if heartbeat else None),
                )
                .order_by(events.c.occurred_at.desc(), events.c.event_id)
                .limit(1)
            )
            .mappings()
            .one_or_none()
        )
    return ShadowRecorderSnapshot(
        heartbeat=dict(heartbeat) if heartbeat else None,
        lease=dict(lease) if lease else None,
        batch=dict(batch) if batch else None,
    )


def stored_timestamp(value: object) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def payload_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC)
