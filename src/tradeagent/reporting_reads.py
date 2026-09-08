"""Read-only reporting projections; original evidence is never rewritten."""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from time import monotonic, sleep
from typing import Any

from sqlalchemy import JSON, Select, Table, column, create_engine, func, select, true
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool
from sqlalchemy.sql.elements import ColumnElement

READ_BATCH_SIZE = 32
DISPLAY_LIMIT = 100
REPORTING_PROJECTION_VERSION = 1
REPORT_PAYLOAD_MAX_BYTES = 8 * 1024 * 1024
REPORT_ADMISSION_LOCK_KEY = 0x5452414452455054
REPORT_ADMISSION_WAIT_SECONDS = 10.0
LOGGER = logging.getLogger(__name__)
AUDIT_SCOPE_FIELDS = (
    "planned_session_date",
    "session_date",
    "mode",
    "synthetic",
    "evidence_kind",
    "session_id",
    "account_digest",
)

POLL_FIELDS = (
    "poll_id",
    "observed_at",
    "requested_start",
    "requested_end",
    "interval_end",
    "coverage_watermark",
    "coverage_complete",
    "coverage",
    "coverage_scope",
    "coverage_basis",
    "coverage_excludes",
    "raw_items_received",
    "new_evidence_versions",
    "counts",
    "count_semantics",
    "counts_semantics",
    "healthy",
    "health",
    "status",
    "errors",
    "last_errors",
    "capability_gaps",
    "source_health",
    "planned_session_date",
    "session_date",
    "mode",
    "synthetic",
    "evidence_kind",
)

SOURCE_FIELDS = (
    "url",
    "document_id",
    "source_event_id",
    "source",
    "source_url",
    "source_version",
    "published_at",
    "first_received_at",
    "content_available_at",
    "provider_created_at",
    "provider_updated_at",
    "provider_received_at",
    "revision_at",
    "revised_at",
    "revision_observed_at",
    "content_sha256",
    "raw_payload_sha256",
    "event_cluster_id",
    "issuer_id",
    "cik",
    "related_instruments",
    "provider_symbols",
    "mapping_available_at",
    "is_primary_source",
    "rights_profile",
    "headline",
    "publisher",
    "symbol",
    "facts",
    "revision_of",
)


def projected_payload(column: Any, fields: Sequence[str]) -> list[Any]:
    """Project in SQL so excluded JSON bodies never reach the driver."""
    booleans = {"coverage_complete", "healthy", "synthetic", "is_primary_source"}
    return [
        (column[key].as_boolean() if key in booleans else column[key]).label(key) for key in fields
    ]


def projected_row_query(
    connection: Connection,
    table: Table,
    fields: Sequence[str],
    metadata_columns: Sequence[ColumnElement[Any]] = (),
) -> Select[Any]:
    """Parse a PostgreSQL JSON document once, not once per projected field.

    The audit payload column is JSON, not JSONB: repeated ``payload -> key``
    operators repeatedly detoast and parse large retained documents. A lateral
    json_to_record call extracts all requested fields in one pass and returns
    only those fields to the client.
    """
    if connection.dialect.name == "postgresql":
        document = (
            func.json_to_record(table.c.payload)
            .table_valued(*(column(key, JSON) for key in fields))
            .render_derived(with_types=True)
            .lateral("report_fields")
        )
        return select(*metadata_columns, *document.c).select_from(table.join(document, true()))
    return select(*metadata_columns, *projected_payload(table.c.payload, fields))


def stream_rows(connection: Connection, query: Select[Any]) -> Iterator[dict[str, Any]]:
    # yield_per activates server-side cursors on PostgreSQL, not just Python iteration
    # over a driver-buffered result containing every historical JSON document.
    with connection.execute(query.execution_options(yield_per=READ_BATCH_SIZE)) as result:
        for row in result.mappings():
            yield dict(row)


def payload_from_projection(row: dict[str, Any], fields: Sequence[str]) -> dict[str, Any]:
    return {key: row[key] for key in fields if row.get(key) is not None}


def compact_poll(payload: dict[str, Any]) -> dict[str, Any]:
    health_fields = (
        "status",
        "document_errors",
        "http_attempts",
        "http_successes",
        "last_http_received_at",
        "feed_verified_at",
        "prior_comparison_status",
        "prior_comparison_error",
    )
    result = {key: value for key, value in payload.items() if key in POLL_FIELDS}
    if isinstance(result.get("source_health"), dict):
        result["source_health"] = {
            name: {key: value[key] for key in health_fields if key in value}
            for name, value in result["source_health"].items()
            if isinstance(value, dict)
        }
        result["source_health_projection"] = (
            "Status, counts, confirmation times and errors; "
            "full provider metadata in immutable poll"
        )
    return result


def event_reporting_projection(event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Capture scope once when retaining an event, without copying historical bodies."""
    result = {"scope": {key: payload[key] for key in AUDIT_SCOPE_FIELDS if key in payload}}
    if event_type == "event_source_poll":
        result["poll"] = compact_poll(payload)
    return result


class ReportingReadModelIncompleteError(RuntimeError):
    """An immutable event has no usable projection; never replace missing history with zero."""

    def __init__(self, event_id: str) -> None:
        super().__init__(
            f"Reporting projection version {REPORTING_PROJECTION_VERSION} is missing or invalid "
            f"for events_v2:{event_id}; finish the bounded metadata backfill before reporting. "
            "The original event is retained; no partial report was generated."
        )


ReportingReadModelIncomplete = ReportingReadModelIncompleteError


def reporting_metadata_query(event_table: Table, projection_table: Table) -> Select[Any]:
    """Read the immutable sidecar without referencing the historical payload column."""
    return select(
        *(value for value in event_table.c if value.name != "payload"),
        projection_table.c.payload.label("reporting_projection"),
    ).select_from(
        event_table.outerjoin(
            projection_table,
            (projection_table.c.event_id == event_table.c.event_id)
            & (projection_table.c.projection_version == REPORTING_PROJECTION_VERSION),
        )
    )


def reporting_projection_from_row(row: dict[str, Any]) -> dict[str, Any]:
    projection = row["reporting_projection"]
    if (
        not isinstance(projection, dict)
        or not isinstance(projection.get("scope"), dict)
        or (
            row["event_type"] == "event_source_poll"
            and not isinstance(projection.get("poll"), dict)
        )
    ):
        raise ReportingReadModelIncompleteError(str(row["event_id"]))
    return projection


def compact_report_evidence(value: Any, immutable_reference: str, path: str = "") -> Any:
    """Keep every parsed fact while referencing, not duplicating, retained source bodies."""
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        references = {}
        source_document = any(
            key in value for key in ("content_sha256", "source_url", "immutable_reference")
        )
        for key, item in value.items():
            pointer = f"{path}/{str(key).replace('~', '~0').replace('/', '~1')}"
            raw_field = key in {"body_base64", "raw_metadata_json"} or (
                source_document
                and key
                in {"content", "body", "document", "provider_response", "supporting_excerpt"}
            )
            if raw_field and isinstance(item, str):
                references[key] = {
                    "immutable_reference": f"{immutable_reference}#{pointer}",
                    "characters": len(item),
                    "projection": "raw source text omitted; complete immutable original retained",
                }
            else:
                result[key] = compact_report_evidence(item, immutable_reference, pointer)
        if references:
            result["source_body_references"] = references
        return result
    if isinstance(value, list | tuple):
        return [
            compact_report_evidence(item, immutable_reference, f"{path}/{index}")
            for index, item in enumerate(value)
        ]
    return value


class ReportPayloadTooLargeError(RuntimeError):
    """Reject an oversized report before PostgreSQL must allocate its JSON input."""


def report_payload_size(report: dict[str, Any]) -> int:
    total = 0
    for fragment in json.JSONEncoder().iterencode(report):
        total += len(fragment.encode("utf-8"))
        if total > REPORT_PAYLOAD_MAX_BYTES:
            raise ReportPayloadTooLargeError(
                f"Report exceeds {REPORT_PAYLOAD_MAX_BYTES} UTF-8 bytes after source projection; "
                "no oversized snapshot was sent to PostgreSQL and no delivery was enqueued. "
                "Original evidence and complete accounting are retained, not truncated."
            )
    return total


class ReportBusyError(RuntimeError):
    """Another process owns the database-wide full-report admission slot."""


@contextmanager
def report_admission(engine: Engine, *, wait_seconds: float = 0) -> Iterator[None]:
    if not 0 <= wait_seconds <= 60:
        raise ValueError("report admission wait must be between zero and 60 seconds")
    if engine.dialect.name != "postgresql":
        yield
        return
    deadline = monotonic() + wait_seconds
    coordination = create_engine(
        engine.url,
        poolclass=NullPool,
        isolation_level="AUTOCOMMIT",
        connect_args={"connect_timeout": 5},
    )
    try:
        with coordination.connect() as connection:
            acquired = False
            try:
                while True:
                    acquired = (
                        connection.scalar(
                            select(func.pg_try_advisory_lock(REPORT_ADMISSION_LOCK_KEY))
                        )
                        is True
                    )
                    if acquired:
                        break
                    remaining = deadline - monotonic()
                    if remaining <= 0:
                        raise ReportBusyError(
                            "Another full session report is running; retry after it completes. "
                            "No partial report or delivery was created."
                        )
                    sleep(min(0.25, remaining))
                yield
            finally:
                if acquired:
                    try:
                        connection.scalar(
                            select(func.pg_advisory_unlock(REPORT_ADMISSION_LOCK_KEY))
                        )
                    except SQLAlchemyError:
                        LOGGER.warning(
                            "Report advisory unlock failed; closing its dedicated connection",
                            exc_info=True,
                        )
    finally:
        # NullPool physically closes the session, releasing its lock even after an error.
        coordination.dispose()


def forward_clause(payload: Any) -> ColumnElement[bool]:
    from sqlalchemy import func

    return (
        (func.coalesce(payload["mode"].as_string(), "") != "offline_replay")
        & (func.coalesce(payload["synthetic"].as_boolean(), False).is_(False))
        & (~func.coalesce(payload["evidence_kind"].as_string(), "").in_(("synthetic", "replay")))
    )
