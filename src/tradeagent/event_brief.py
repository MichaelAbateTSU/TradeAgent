"""Append-only company-news preparation; collection never authorizes an entry."""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

from pydantic import ValidationError
from sqlalchemy import select

from tradeagent.event_research import (
    ANNUAL_RE,
    ExtractionResult,
    SourceEvent,
    config_hash,
    supported_issuer_mappings,
)
from tradeagent.event_sources import verify_primary_url
from tradeagent.event_store import EventStore, event_decisions, event_evidence
from tradeagent.persistence import Database, events
from tradeagent.reporting_reads import (
    DISPLAY_LIMIT,
    POLL_FIELDS,
    compact_poll,
    payload_from_projection,
    projected_payload,
    projected_row_query,
    stream_rows,
)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("brief boundaries and observation time must be timezone-aware")
    return value.astimezone(UTC)


def _time(value: object) -> datetime | None:
    if value is None:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _metadata(event: SourceEvent) -> dict[str, Any]:
    raw = json.loads(event.raw_metadata_json)
    return raw if isinstance(raw, dict) else {}


def _brief_metadata(event: SourceEvent) -> SourceEvent:
    """Compact a validated document for this read model only, never for extraction.

    The only content operation below is ANNUAL_RE presence. Retain that exact
    matched span and metadata used by the brief; hashes still refer to the full
    immutable original, not this internal projection.
    """
    annual = ANNUAL_RE.search(event.content or "")
    raw = _metadata(event)
    filing = raw.get("filing")
    metadata = {
        "source": raw.get("source", event.source),
        "filing": {"collection_role": filing.get("collection_role")}
        if isinstance(filing, dict)
        else None,
    }
    return event.model_copy(
        update={
            "content": annual.group(0) if annual else "" if event.content is not None else None,
            "raw_metadata_json": json.dumps(metadata),
        }
    )


def _older_context(event: SourceEvent, boundary: datetime) -> bool:
    filing = _metadata(event).get("filing")
    return (event.published_at is not None and event.published_at < boundary) or (
        isinstance(filing, dict) and filing.get("collection_role") == "older_comparison"
    )


def _verified(event: SourceEvent, now: datetime) -> bool:
    mapping = next(
        (item for item in supported_issuer_mappings() if item.issuer_id == event.issuer_id), None
    )
    if (
        mapping is None
        or not event.is_primary_source
        or event.cik != mapping.cik
        or event.mapping_available_at is None
        or event.mapping_available_at > now
        or mapping.available_at > now
        or mapping.symbol not in event.related_instruments
    ):
        return False
    try:
        verify_primary_url(event.source_url, mapping)
    except ValueError:
        return False
    return True


def available_prior_evidence(
    event: SourceEvent, evidence: Sequence[SourceEvent], *, now: datetime
) -> tuple[SourceEvent, ...]:
    """Return only observed pre-event annual-revenue context for the existing grammar.

    An older document downloaded today is historical context, not proof that its
    current version was available before a weekend event. No receipt is backdated.
    A newer version known at decision time invalidates its earlier comparison;
    corrections and retractions are not usable annual denominators.
    """
    now = _utc(now)
    if (
        event.published_at is None
        or event.first_received_at > now
        or event.content_available_at > now
        or not _verified(event, now)
    ):
        return ()
    if event.published_at > now:
        return ()
    cutoff = event.published_at
    latest: dict[tuple[str, str], SourceEvent] = {}
    invalidated: set[str] = set()
    for item in evidence:
        if (
            item.evidence_id == event.evidence_id
            or item.issuer_id != event.issuer_id
            or item.availability_basis != "observed_receipt"
            or not _verified(item, item.content_available_at)
            or item.first_received_at > now
            or item.content_available_at > now
            or (item.published_at is not None and item.published_at > now)
            or (item.provider_updated_at is not None and item.provider_updated_at > now)
        ):
            continue
        if item.revision_of is not None:
            invalidated.add(item.revision_of)
        key = (item.source, item.source_event_id)
        previous = latest.get(key)
        if previous is None or (item.content_available_at, item.evidence_id) > (
            previous.content_available_at,
            previous.evidence_id,
        ):
            latest[key] = item
    return tuple(
        sorted(
            (
                item
                for item in latest.values()
                if item.evidence_id not in invalidated
                and item.first_received_at < cutoff
                and item.content_available_at < cutoff
                and (item.published_at is None or item.published_at < cutoff)
                and (item.provider_updated_at is None or item.provider_updated_at < cutoff)
                and not item.is_correction
                and not item.is_retraction
                and item.revision_of is None
                and ANNUAL_RE.search(item.content or "")
            ),
            key=lambda item: (item.content_available_at, item.evidence_id),
        )
    )


def load_frozen_evidence_packet(
    database: Database, extraction: ExtractionResult, *, now: datetime
) -> tuple[SourceEvent, ...]:
    """Load exactly the extraction's ordered immutable packet, never later context.

    Missing versions, changed payloads, and not-yet-available packets raise a
    ``ValueError``; callers must record the failure and abstain, not re-extract.
    This is packet reconstruction only, not risk approval or a correction veto.
    """
    now = _utc(now)
    ids = extraction.evidence_ids
    if not ids or len(set(ids)) != len(ids) or ids[0] != extraction.source_event_id:
        raise ValueError("frozen_evidence_packet_ids_invalid")
    if extraction.completed_at > now or extraction.available_at > now:
        raise ValueError("frozen_evidence_packet_not_yet_available")
    with database.begin() as connection:
        rows = connection.execute(
            select(event_evidence.c.evidence_id, event_evidence.c.payload).where(
                event_evidence.c.evidence_id.in_(ids)
            )
        ).all()
    by_id = {str(evidence_id): payload for evidence_id, payload in rows}
    if set(by_id) != set(ids):
        raise ValueError("frozen_evidence_packet_missing_versions")
    try:
        packet = tuple(SourceEvent.model_validate(by_id[evidence_id]) for evidence_id in ids)
    except ValidationError as error:
        raise ValueError("frozen_evidence_packet_invalid_payload") from error
    if any(
        item.evidence_id != evidence_id
        or item.first_received_at > extraction.completed_at
        or item.content_available_at > extraction.completed_at
        or (item.published_at is not None and item.published_at > extraction.completed_at)
        or (
            item.provider_updated_at is not None
            and item.provider_updated_at > extraction.completed_at
        )
        for evidence_id, item in zip(ids, packet, strict=True)
    ):
        raise ValueError("frozen_evidence_packet_not_available_at_extraction")
    if config_hash([item.model_dump(mode="json") for item in packet]) != extraction.input_sha256:
        raise ValueError("frozen_evidence_packet_hash_mismatch")
    return packet


def _coverage(polls: Sequence[dict[str, Any]], start: datetime, target: datetime) -> dict[str, Any]:
    intervals: list[tuple[datetime, datetime]] = []
    for poll in polls:
        left = _time(poll.get("requested_start"))
        right = _time(poll.get("coverage_watermark"))
        observed = _time(poll.get("observed_at"))
        if (
            poll.get("coverage_complete") is True
            and left is not None
            and right is not None
            and observed is not None
            and left < right <= observed
        ):
            intervals.append((left, right))
    watermark = start
    for left, right in sorted(intervals):
        if left <= watermark:
            watermark = max(watermark, right)
    return {
        "scope": "configured_sources_only_not_all_company_news",
        "coverage_start": start.isoformat(),
        "coverage_watermark": watermark.isoformat() if watermark > start else None,
        "required_through": target.isoformat(),
        "complete_through_observation": watermark >= target and target > start,
        "uncovered_start": watermark.isoformat() if watermark < target else None,
        "uncovered_end": target.isoformat() if watermark < target else None,
        "incomplete_polls": sum(p.get("coverage_complete") is not True for p in polls),
    }


def persist_premarket_brief(
    database: Database,
    *,
    cohort_id: str,
    session_open: datetime,
    session_close: datetime,
    previous_session_close: datetime,
    now: datetime,
    source_capabilities: Mapping[str, Any],
    poll_stats: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Persist a real-time brief using broker-verified boundaries supplied by runtime.

    Call before open even on source failure, then after every source poll. Records
    use the existing audit table; earlier briefs/evidence/decisions are never edited.
    This helper reads recorded extractions, and never creates a trading extraction,
    changes freshness settings, accesses a broker, or infers missing consensus.
    """
    now, session_open, session_close, previous_session_close = (
        _utc(value) for value in (now, session_open, session_close, previous_session_close)
    )
    if not previous_session_close < session_open < session_close:
        raise ValueError("verified regular-session boundaries must increase")
    if now < previous_session_close:
        raise ValueError("cannot prepare a brief before the collection interval")
    if not cohort_id:
        raise ValueError("brief requires a cohort")
    local_day = session_open.astimezone(ZoneInfo("America/New_York")).date().isoformat()
    trace = f"{cohort_id}:{local_day}:premarket_brief"
    store = EventStore(database)
    stats = dict(
        poll_stats if poll_stats is not None else source_capabilities.get("last_poll_stats") or {}
    )
    stats_at = _time(stats.get("observed_at"))
    if stats and (stats_at is None or stats_at > now or not stats.get("poll_id")):
        raise ValueError(
            "poll statistics require a unique ID and actual nonfuture observation time"
        )
    with database.begin() as connection:
        if stats:
            poll_trace = f"{trace}:poll:{stats['poll_id']}"
            existing = connection.scalar(
                select(events.c.event_id).where(
                    events.c.event_type == "event_source_poll", events.c.trace_id == poll_trace
                )
            )
            if existing is None:
                store.audit("source_poll", stats, now, poll_trace, connection)
        previous_fields = ("snapshot_id", "prepared_at", "initial_prepared_at")
        previous_row = (
            connection.execute(
                select(*projected_payload(events.c.payload, previous_fields))
                .where(events.c.event_type == "event_premarket_brief", events.c.trace_id == trace)
                .order_by(events.c.occurred_at.desc(), events.c.recorded_at.desc())
                .limit(1)
            )
            .mappings()
            .one_or_none()
        )
        previous = dict(previous_row) if previous_row else None
        if previous and (_time(previous.get("prepared_at")) or now) > now:
            raise ValueError("brief observations cannot be backdated")
        polls = [
            compact_poll(payload_from_projection(row, POLL_FIELDS))
            for row in stream_rows(
                connection,
                projected_row_query(connection, events, POLL_FIELDS).where(
                    events.c.event_type == "event_source_poll",
                    events.c.trace_id.startswith(f"{trace}:poll:", autoescape=True),
                    events.c.occurred_at <= now,
                ),
            )
        ]

    latest_stats = (
        compact_poll(stats)
        if stats
        else max(
            polls,
            key=lambda row: _time(row.get("observed_at")) or previous_session_close,
            default={},
        )
    )
    evidence: list[SourceEvent] = []
    invalid_evidence = 0
    synthetic_excluded = 0
    with database.begin() as connection:
        for row in stream_rows(
            connection,
            select(event_evidence.c.payload).where(
                event_evidence.c.received_at >= previous_session_close - timedelta(days=550),
                event_evidence.c.received_at <= now,
            ),
        ):
            try:
                event = SourceEvent.model_validate(row["payload"])
            except ValidationError:
                invalid_evidence += 1
                continue
            if event.availability_basis != "observed_receipt":
                synthetic_excluded += 1
            elif event.first_received_at <= now and event.content_available_at <= now:
                evidence.append(_brief_metadata(event))
    extractions: dict[str, ExtractionResult] = {}
    evidence_by_id = {item.evidence_id: item for item in evidence}
    invalid_extractions = 0
    with database.begin() as connection:
        for row in stream_rows(
            connection,
            select(events.c.payload)
            .where(
                events.c.event_type == "event_extraction",
                events.c.trace_id.startswith(f"{cohort_id}:", autoescape=True),
                events.c.occurred_at >= previous_session_close,
                events.c.occurred_at <= now,
            )
            .order_by(events.c.occurred_at, events.c.recorded_at, events.c.event_id),
        ):
            try:
                parsed_extraction = ExtractionResult.model_validate(row["payload"])
            except ValidationError:
                invalid_extractions += 1
                continue
            if parsed_extraction.completed_at <= now and parsed_extraction.available_at <= now:
                extractions.setdefault(parsed_extraction.source_event_id, parsed_extraction)
        decision_fields = ("action", "reasons", "decided_at")
        decisions = {
            str(row["evidence_id"]): payload_from_projection(row, decision_fields)
            for row in stream_rows(
                connection,
                select(
                    event_decisions.c.evidence_id,
                    *projected_payload(event_decisions.c.payload, decision_fields),
                ).where(
                    event_decisions.c.cohort_id == cohort_id,
                    event_decisions.c.decided_at >= previous_session_close,
                    event_decisions.c.decided_at <= now,
                ),
            )
        }
    target = min(now, session_open)
    collected = [
        event
        for event in evidence
        if (
            not _older_context(event, previous_session_close)
            and (
                (
                    event.published_at is not None
                    and previous_session_close <= event.published_at <= min(now, session_close)
                )
                or (
                    event.published_at is None and event.first_received_at >= previous_session_close
                )
            )
        )
    ]
    collected.sort(key=lambda event: (event.first_received_at, event.evidence_id))
    older = [event for event in evidence if _older_context(event, previous_session_close)]
    symbols = {item.symbol for item in supported_issuer_mappings()}
    seen_clusters: dict[str, str] = {}
    rows: list[dict[str, Any]] = []
    reasons: Counter[str] = Counter()
    for event in collected:
        extraction = extractions.get(event.evidence_id)
        decision = decisions.get(event.evidence_id, {})
        duplicate_of = seen_clusters.setdefault(event.event_cluster_id, event.evidence_id)
        missing = list(extraction.missing_required_fields) if extraction else ["extraction_pending"]
        if event.published_at is None:
            missing.append("original_publication_time_unknown")
        if event.content is None:
            missing.append("retained_source_content_unavailable_under_rights_profile")
        if not _verified(event, now):
            missing.append("verified_primary_issuer")
        contrary = list(extraction.contradictions) if extraction else []
        decision_reasons = list(decision.get("reasons", []))
        raw = _metadata(event)
        revision_time = event.provider_updated_at
        previous_version = evidence_by_id.get(event.revision_of or "")
        if (
            event.revision_of is None
            and revision_time in {event.provider_created_at, event.published_at}
        ) or (
            previous_version is not None and revision_time == previous_version.provider_updated_at
        ):
            revision_time = None
        if event.revision_of is not None and revision_time is None:
            missing.append("revision_time_unknown_observed_change_is_not_provider_revision_clock")
        prior = available_prior_evidence(event, evidence, now=now)
        reasons.update(set(missing + contrary + decision_reasons))
        rows.append(
            {
                "evidence_id": event.evidence_id,
                "event_cluster_id": event.event_cluster_id,
                "source": event.source,
                "publisher": raw.get("source", event.source),
                "source_url": event.source_url,
                "document_id": event.source_event_id,
                "source_version": event.source_version,
                "headline": event.headline,
                "content_sha256": event.content_sha256,
                "raw_payload_sha256": event.raw_payload_sha256,
                "rights_profile": event.rights_profile,
                "immutable_reference": f"event_evidence:{event.evidence_id}",
                "publication_time": event.published_at.isoformat() if event.published_at else None,
                "provider_created_at": (
                    event.provider_created_at.isoformat() if event.provider_created_at else None
                ),
                "provider_receipt_time": None,
                "provider_receipt_status": "not_provided_created_at_is_not_receipt",
                "bot_first_receipt_time": event.first_received_at.isoformat(),
                "content_available_at": event.content_available_at.isoformat(),
                "provider_updated_at": (
                    event.provider_updated_at.isoformat() if event.provider_updated_at else None
                ),
                "revision_time": revision_time.isoformat() if revision_time else None,
                "revision_observed_at": (
                    event.first_received_at.isoformat() if event.revision_of else None
                ),
                "revision_of": event.revision_of,
                "duplicate_of": duplicate_of if duplicate_of != event.evidence_id else None,
                "independent_confirmations": "not_inferred_syndication_is_not_confirmation",
                "symbols": sorted(
                    set(event.related_instruments + event.provider_symbols) & symbols
                ),
                "issuer_id": event.issuer_id,
                "issuer_verified": _verified(event, now),
                "provider_symbols_are_discovery_only": True,
                "event_type": extraction.event_type if extraction else "extraction_pending",
                "quantitative_facts": (
                    [fact.model_dump(mode="json") for fact in extraction.facts]
                    if extraction and event.content is not None
                    else []
                ),
                "supporting_excerpt_status": (
                    "exact_spans_in_quantitative_facts"
                    if extraction and extraction.facts and event.content is not None
                    else "permitted_immutable_reference_only"
                ),
                "prior_evidence_ids": [item.evidence_id for item in prior[:DISPLAY_LIMIT]],
                "prior_evidence_total": len(prior),
                "prior_evidence_ids_truncated": len(prior) > DISPLAY_LIMIT,
                "prior_annual_revenue_status": (
                    "available_observed_pre_event"
                    if prior
                    else "missing_point_in_time_annual_revenue"
                ),
                "consensus": None,
                "consensus_status": "unavailable_not_inferred_from_prior_company_guidance",
                "contrary_evidence": contrary,
                "missing_information": sorted(set(missing)),
                "decision_time": decision.get("decided_at"),
                "decision_action": decision.get("action"),
                "decision_reasons": decision_reasons,
                "market_information": "see_immutable_decision_and_pre_context_audits",
                "trading_freshness": "not_granted_by_collection_recheck_frozen_rules_at_submission",
                "publication_age_seconds": (
                    (now - event.published_at).total_seconds() if event.published_at else None
                ),
            }
        )
    coverage = _coverage(polls, previous_session_close, target)
    gaps = []
    if not polls:
        gaps.append("source_poll_not_observed")
    if not coverage["complete_through_observation"]:
        gaps.append("collection_interval_not_fully_covered")
    if source_capabilities.get("news_entitlement") != "observed_available":
        gaps.append(f"news_entitlement:{source_capabilities.get('news_entitlement', 'unknown')}")
    if not source_capabilities.get("news_retention_enabled", False):
        gaps.append("licensed_news_metadata_only_no_body_extraction")
    if not source_capabilities.get("sec_enabled"):
        gaps.append("SEC_disabled_contact_not_configured")
    if not source_capabilities.get("primary_urls_configured") and not source_capabilities.get(
        "issuer_feeds_enabled"
    ):
        gaps.append("no_configured_issuer_primary_urls")
    if not source_capabilities.get("issuer_feeds_enabled"):
        gaps.append("official_issuer_feed_discovery_disabled_or_unavailable")
    for symbol, feed in source_capabilities.get("issuer_feed_status", {}).items():
        if feed.get("gap"):
            gaps.append(f"issuer_feed:{symbol}:{feed['gap']}")
        if feed.get("status") not in {"healthy"}:
            gaps.append(f"issuer_feed:{symbol}:{feed.get('status', 'unobserved')}")
        if feed.get("publication_timestamp_status") == (
            "feed_publication_missing_requires_explicit_document_timestamp"
        ):
            gaps.append(f"issuer_feed:{symbol}:publication_missing_updated_is_not_publication")
        if feed.get("rejected_link_count"):
            gaps.append(f"issuer_feed:{symbol}:entries_outside_existing_verified_URL_policy")
    if (
        source_capabilities.get("sec_enabled")
        and source_capabilities.get("max_sec_filings_per_symbol") == 0
    ):
        gaps.append("SEC_document_collection_disabled_batch_zero")
    gaps.extend(str(reason) for reason in latest_stats.get("errors", ()))
    for name, health in latest_stats.get("source_health", {}).items():
        context_status = health.get("prior_comparison_status")
        if context_status and context_status != (
            "collected_current_version_publication_may_be_unknown"
        ):
            gaps.append(f"{name}:prior_comparison:{context_status}")
        if health.get("prior_comparison_error"):
            gaps.append(f"{name}:prior_comparison:{health['prior_comparison_error']}")
    gaps.extend(
        (
            "finite_official_feeds_do_not_prove_all_company_news_or_archived_feed_coverage",
            "SEC_acceptance_is_not_proven_publication",
            "older_current_version_does_not_prove_pre_event_availability",
            "deterministic_extraction_supports_only_existing_explicit_numeric_grammar",
            "point_in_time_consensus_unavailable",
        )
    )
    initial_at = _time(previous.get("initial_prepared_at")) if previous else now
    prepared_before_open = initial_at is not None and initial_at < session_open
    status = (
        "premarket_prepared"
        if now < session_open
        else "premarket_prepared_updated"
        if prepared_before_open
        else "missed_session"
        if now >= session_close
        else "missed_premarket_deadline"
    )
    counts = {
        "raw_items_received": sum(int(p.get("raw_items_received", 0)) for p in polls),
        "raw_count_basis": (
            "provider_items_feed_entries_and_fetched_documents_including_duplicates_repolls_cache"
        ),
        "source_health_checks": sum(
            sum(
                s.get("status") in {"healthy", "incomplete_or_failed"}
                for s in p.get("source_health", {}).values()
            )
            for p in polls
        ),
        "new_evidence_versions_received": sum(
            int(p.get("new_evidence_versions", 0)) for p in polls
        ),
        "unique_evidence_versions": len(collected),
        "unique_events": len(seen_clusters),
        "supported_company_matches": sum(bool(row["symbols"]) for row in rows),
        "verified_issuer_matches": sum(row["issuer_verified"] for row in rows),
        "valid_quantitative_events": len(
            {
                event.event_cluster_id
                for event in collected
                if (output := extractions.get(event.evidence_id)) is not None
                and output.facts
                and output.reason_for_abstention is None
                and event.content is not None
            }
        ),
        "extractions_pending": sum(event.evidence_id not in extractions for event in collected),
        "duplicate_versions": sum(row["duplicate_of"] is not None for row in rows),
        "revisions": sum(event.revision_of is not None for event in collected),
        "older_context_documents": len(older),
        "strategy_candidates": sum(
            decisions.get(event.evidence_id, {}).get("action") == "eligible" for event in collected
        ),
        "risk_approved_decisions": None,
        "attempted_entries": None,
        "fills": None,
        "completed_round_trips": None,
        "execution_count_status": "not_counted_by_news_brief_use_order_ledger",
        "invalid_evidence_records": invalid_evidence,
        "invalid_extraction_records": invalid_extractions,
        "synthetic_or_replay_evidence_excluded": synthetic_excluded,
        "future_publication_timestamp_records": sum(
            event.published_at is not None and event.published_at > now for event in evidence
        ),
        "missing_publication_timestamps": sum(event.published_at is None for event in collected),
    }
    snapshot: dict[str, Any] = {
        "schema_version": "premarket-news-brief-v1",
        "snapshot_id": str(uuid4()),
        "supersedes_snapshot_id": previous.get("snapshot_id") if previous else None,
        "cohort_id": cohort_id,
        "session_date": local_day,
        "session_open": session_open.isoformat(),
        "session_close": session_close.isoformat(),
        "previous_session_close": previous_session_close.isoformat(),
        "calendar_basis": "caller_supplied_broker_verified_regular_session_boundaries",
        "prepared_at": now.isoformat(),
        "initial_prepared_at": initial_at.isoformat() if initial_at else None,
        "prepared_before_open": prepared_before_open,
        "preparation_status": status,
        "collection_target": "previous_regular_close_through_session_open",
        "coverage": coverage,
        "capability_gaps": gaps,
        "source_capabilities": {
            key: value for key, value in source_capabilities.items() if key != "last_poll_stats"
        },
        "latest_poll": latest_stats or None,
        "funnel": counts,
        "blocking_reason_counts": dict(sorted(reasons.items())),
        "news": rows[:DISPLAY_LIMIT],
        "display": {
            "news_total": len(rows),
            "news_shown": min(len(rows), DISPLAY_LIMIT),
            "news_truncated": len(rows) > DISPLAY_LIMIT,
            "limit": DISPLAY_LIMIT,
            "accounting": "all counters and company totals include every observed evidence version",
            "immutable_history": (
                "event_evidence (evidence_id), event_decisions and events_v2 extraction audits"
            ),
        },
        "companies": {
            symbol: {
                "discovery_matches": sum(symbol in row["symbols"] for row in rows),
                "verified_primary_matches": sum(
                    symbol in row["symbols"] and row["issuer_verified"] for row in rows
                ),
                "evidence_ids": [row["evidence_id"] for row in rows if symbol in row["symbols"]][
                    :DISPLAY_LIMIT
                ],
                "evidence_ids_truncated": sum(symbol in row["symbols"] for row in rows)
                > DISPLAY_LIMIT,
                "silence_means": "no_observed_match_not_proof_of_no_company_news",
            }
            for symbol in sorted(symbols)
        },
        "older_context_evidence_ids": [event.evidence_id for event in older[:DISPLAY_LIMIT]],
        "older_context_evidence_ids_truncated": len(older) > DISPLAY_LIMIT,
        "source_health_status": (
            "unobserved"
            if not polls
            else "incomplete_or_failed"
            if latest_stats and not latest_stats.get("coverage_complete")
            else "stale_no_recent_source_poll"
            if (_time(latest_stats.get("observed_at")) or previous_session_close)
            < now
            - timedelta(seconds=max(120, int(source_capabilities.get("cache_ttl_seconds", 60)) * 2))
            else "configured_sources_observed_silence"
            if not collected
            else "configured_sources_observed_items"
        ),
        "freshness_policy": "collection_eligibility_is_not_trading_freshness",
        "future_open_coverage": "pending_until_open" if now < session_open else "elapsed",
        "extraction_or_inference_performed": False,
    }
    store.audit("premarket_brief", snapshot, now, trace)
    return snapshot
