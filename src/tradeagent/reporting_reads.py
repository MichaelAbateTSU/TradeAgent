"""Read-only reporting projections; original evidence is never rewritten."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import Any

from sqlalchemy import Select
from sqlalchemy.engine import Connection
from sqlalchemy.sql.elements import ColumnElement

READ_BATCH_SIZE = 32
DISPLAY_LIMIT = 100

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


def forward_clause(payload: Any) -> ColumnElement[bool]:
    from sqlalchemy import func

    return (
        (func.coalesce(payload["mode"].as_string(), "") != "offline_replay")
        & (func.coalesce(payload["synthetic"].as_boolean(), False).is_(False))
        & (~func.coalesce(payload["evidence_kind"].as_string(), "").in_(("synthetic", "replay")))
    )
