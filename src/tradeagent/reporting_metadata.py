"""Bounded, resumable construction of compact metadata from immutable originals."""

from __future__ import annotations

from typing import Any

from sqlalchemy import and_, func, select, text
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from tradeagent.persistence import Database, event_reporting_metadata, events
from tradeagent.reporting_reads import (
    REPORTING_PROJECTION_VERSION,
    event_reporting_projection,
)


def missing_reporting_metadata(database: Database) -> int:
    with database.begin() as connection:
        return int(
            connection.scalar(
                select(func.count())
                .select_from(
                    events.outerjoin(
                        event_reporting_metadata,
                        and_(
                            events.c.event_id == event_reporting_metadata.c.event_id,
                            event_reporting_metadata.c.projection_version
                            == REPORTING_PROJECTION_VERSION,
                        ),
                    )
                )
                .where(
                    events.c.event_type != "event_session_report",
                    event_reporting_metadata.c.event_id.is_(None),
                )
            )
            or 0
        )


def backfill_reporting_metadata_page(
    database: Database, *, after_event_id: str | None = None, batch_size: int = 32
) -> dict[str, Any]:
    if not 1 <= batch_size <= 32:
        raise ValueError("Metadata backfill pages must contain one to 32 originals")
    with database.begin() as connection:
        if connection.dialect.name == "postgresql":
            connection.execute(text("SET LOCAL statement_timeout = '20s'"))
            connection.execute(text("SET LOCAL lock_timeout = '5s'"))
        query = (
            select(events.c.event_id)
            .select_from(
                events.outerjoin(
                    event_reporting_metadata,
                    and_(
                        events.c.event_id == event_reporting_metadata.c.event_id,
                        event_reporting_metadata.c.projection_version
                        == REPORTING_PROJECTION_VERSION,
                    ),
                )
            )
            .where(
                events.c.event_type != "event_session_report",
                event_reporting_metadata.c.event_id.is_(None),
            )
            .order_by(events.c.event_id)
            .limit(batch_size)
        )
        if after_event_id is not None:
            query = query.where(events.c.event_id > after_event_id)
        identities = list(connection.execute(query).scalars())
        if not identities:
            return {"selected": 0, "inserted": 0, "last_event_id": after_event_id}
        # Deserialize only one original at a time; never materialize a page of heavy bodies.
        originals = connection.execute(
            select(events.c.event_id, events.c.event_type, events.c.payload)
            .where(events.c.event_id.in_(identities))
            .order_by(events.c.event_id)
            .execution_options(stream_results=True, yield_per=1)
        )
        projections = []
        try:
            for row in originals.mappings():
                projections.append(
                    {
                        "event_id": row["event_id"],
                        "projection_version": REPORTING_PROJECTION_VERSION,
                        "payload": event_reporting_projection(row["event_type"], row["payload"]),
                    }
                )
        finally:
            originals.close()
        statement = (
            postgresql_insert(event_reporting_metadata)
            if connection.dialect.name == "postgresql"
            else sqlite_insert(event_reporting_metadata)
        )
        inserted = list(
            connection.execute(
                statement.values(projections)
                .on_conflict_do_nothing(
                    index_elements=[
                        event_reporting_metadata.c.event_id,
                        event_reporting_metadata.c.projection_version,
                    ]
                )
                .returning(event_reporting_metadata.c.event_id)
            ).scalars()
        )
    return {
        "selected": len(identities),
        "inserted": len(inserted),
        "last_event_id": identities[-1],
    }
