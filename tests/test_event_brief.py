from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import select, update

from tradeagent.event_brief import (
    available_prior_evidence,
    load_frozen_evidence_packet,
    persist_premarket_brief,
)
from tradeagent.event_research import (
    SourceEvent,
    extract_event,
    supported_issuer_mappings,
    text_hash,
)
from tradeagent.event_store import EventStore, event_evidence
from tradeagent.persistence import Database, events

NY = ZoneInfo("America/New_York")
OPEN = datetime(2026, 9, 8, 9, 30, tzinfo=NY)
CLOSE = datetime(2026, 9, 8, 16, tzinfo=NY)
PREVIOUS = datetime(2026, 9, 4, 16, tzinfo=NY)
NOW = datetime(2026, 9, 8, 9, 0, tzinfo=NY)
COHORT = "news-brief-test"
CAPABILITIES: dict[str, Any] = {
    "news_entitlement": "observed_available",
    "news_retention_enabled": False,
    "sec_enabled": True,
    "primary_urls_configured": 0,
}
GUIDANCE = (
    "For fiscal 2027, GAAP revenue guidance increased from USD 100 million to USD 110 million."
)
ANNUAL = "For fiscal 2025, GAAP annual revenue was USD 1000 million."


@pytest.fixture
def database() -> Iterator[Database]:
    with Database("sqlite:///:memory:") as db:
        db.initialize()
        EventStore(db).freeze(COHORT, "test-hash", {}, "shadow", NOW)
        yield db


def source(**overrides: Any) -> SourceEvent:
    mapping = supported_issuer_mappings()[0]
    data: dict[str, Any] = {
        "source_event_id": "release",
        "source": "issuer_primary",
        "source_url": "https://www.apple.com/newsroom/release/",
        "source_version": "version-one",
        "published_at": datetime(2026, 9, 5, 10, tzinfo=NY),
        "first_received_at": NOW - timedelta(minutes=1),
        "content_available_at": NOW - timedelta(minutes=1),
        "content": GUIDANCE,
        "content_sha256": text_hash(GUIDANCE),
        "raw_payload_sha256": text_hash(GUIDANCE),
        "raw_metadata_json": "{}",
        "event_cluster_id": "guidance-event",
        "issuer_id": mapping.issuer_id,
        "cik": mapping.cik,
        "related_instruments": ("AAPL",),
        "mapping_available_at": mapping.available_at,
        "is_primary_source": True,
        "rights_profile": "public_primary_document_research_retention",
        "headline": "Apple updates guidance",
    }
    data.update(overrides)
    return SourceEvent.model_validate(data)


def poll(
    *,
    identity: str = "poll-one",
    start: datetime = PREVIOUS,
    end: datetime = NOW,
    complete: bool = True,
    raw_count: int = 1,
) -> dict[str, Any]:
    return {
        "poll_id": identity,
        "requested_start": start.isoformat(),
        "requested_end": end.isoformat(),
        "observed_at": end.isoformat(),
        "coverage_complete": complete,
        "coverage_watermark": end.isoformat() if complete else None,
        "raw_items_received": raw_count,
        "new_evidence_versions": 1,
        "source_health": {
            "news": {
                "status": "healthy" if complete else "incomplete_or_failed",
                "http_attempts": 1,
                "http_successes": 1 if complete else 0,
            }
        },
    }


def brief(database: Database, **overrides: Any) -> dict[str, Any]:
    kwargs = {
        "cohort_id": COHORT,
        "session_open": OPEN,
        "session_close": CLOSE,
        "previous_session_close": PREVIOUS,
        "now": NOW,
        "source_capabilities": CAPABILITIES,
    }
    kwargs.update(overrides)
    return persist_premarket_brief(database, **kwargs)


def store_event(database: Database, event: SourceEvent, *, extraction: bool = True) -> None:
    store = EventStore(database)
    store.evidence(event.evidence_id, event.model_dump(mode="json"), event.first_received_at)
    if extraction:
        result = extract_event(event, now=NOW)
        store.audit(
            "extraction",
            result.model_dump(mode="json"),
            NOW,
            f"{COHORT}:{event.evidence_id}:extraction",
        )


def test_brief_covers_entire_holiday_weekend_without_making_news_fresh(database: Database) -> None:
    event = source()
    store_event(database, event)
    result = brief(database, poll_stats=poll())
    assert result["session_date"] == "2026-09-08"
    assert result["previous_session_close"] == "2026-09-04T20:00:00+00:00"
    assert result["session_open"] == "2026-09-08T13:30:00+00:00"
    assert result["preparation_status"] == "premarket_prepared"
    assert result["prepared_before_open"] is True
    assert result["coverage"]["complete_through_observation"] is True
    assert result["coverage"]["required_through"] == NOW.astimezone(UTC).isoformat()
    assert result["future_open_coverage"] == "pending_until_open"
    assert result["funnel"]["valid_quantitative_events"] == 1
    assert result["funnel"]["strategy_candidates"] == 0
    row = result["news"][0]
    assert row["publication_age_seconds"] > 60 * 60 * 48
    assert row["trading_freshness"].startswith("not_granted")
    span = row["quantitative_facts"][0]["source_offsets"][0]
    assert span["text"] == GUIDANCE[span["start"] : span["end"]]
    assert row["bot_first_receipt_time"] == event.first_received_at.isoformat()
    assert row["provider_receipt_time"] is None
    assert row["consensus"] is None
    with database.begin() as connection:
        saved = connection.scalar(
            select(events.c.payload).where(events.c.event_type == "event_premarket_brief")
        )
    assert saved == result


def test_brief_snapshots_and_poll_counts_are_append_only_restart_safe(database: Database) -> None:
    event = source()
    store_event(database, event)
    stats = poll(raw_count=3)
    initial = brief(database, poll_stats=stats)
    second = brief(database, now=NOW + timedelta(minutes=1), poll_stats=stats)
    assert second["supersedes_snapshot_id"] == initial["snapshot_id"]
    assert second["initial_prepared_at"] == initial["prepared_at"]
    assert second["funnel"]["raw_items_received"] == 3
    assert second["coverage"]["complete_through_observation"] is False
    with database.begin() as connection:
        snapshots = list(
            connection.scalars(
                select(events.c.payload).where(events.c.event_type == "event_premarket_brief")
            )
        )
        polls = list(
            connection.scalars(
                select(events.c.payload).where(events.c.event_type == "event_source_poll")
            )
        )
        persisted = connection.scalar(select(event_evidence.c.payload))
    assert len(snapshots) == 2
    assert snapshots[0] == initial
    assert len(polls) == 1
    assert persisted["first_received_at"] == event.model_dump(mode="json")["first_received_at"]


def test_duplicate_revision_links_do_not_rewrite_receipts_or_inflate_events(
    database: Database,
) -> None:
    first = source()
    duplicate = source(source_event_id="syndication", source_version="copy")
    revised = source(
        source_version="revised",
        revision_of=first.evidence_id,
        first_received_at=NOW,
        content_available_at=NOW,
        provider_updated_at=NOW - timedelta(seconds=1),
        content=GUIDANCE.replace("110", "90"),
        content_sha256=text_hash(GUIDANCE.replace("110", "90")),
    )
    for event in (first, duplicate, revised):
        store_event(database, event)
    result = brief(database, poll_stats=poll(raw_count=4))
    assert result["funnel"]["unique_evidence_versions"] == 3
    assert result["funnel"]["unique_events"] == 1
    assert result["funnel"]["duplicate_versions"] == 2
    assert result["funnel"]["revisions"] == 1
    row = next(row for row in result["news"] if row["evidence_id"] == revised.evidence_id)
    assert row["revision_of"] == first.evidence_id
    assert row["revision_time"] == revised.provider_updated_at.isoformat()
    assert row["bot_first_receipt_time"] == NOW.isoformat()
    first_row = next(row for row in result["news"] if row["evidence_id"] == first.evidence_id)
    assert first_row["bot_first_receipt_time"] == first.first_received_at.isoformat()


def test_incomplete_polls_or_discontinuous_intervals_never_claim_weekend_coverage(
    database: Database,
) -> None:
    partial = brief(database, poll_stats=poll(complete=False))
    assert partial["coverage"]["coverage_watermark"] is None
    assert partial["source_health_status"] == "incomplete_or_failed"
    now = NOW + timedelta(minutes=1)
    gap = brief(
        database,
        now=now,
        poll_stats=poll(
            identity="later",
            start=NOW - timedelta(minutes=2),
            end=now,
        ),
    )
    assert gap["coverage"]["complete_through_observation"] is False
    assert gap["coverage"]["coverage_watermark"] is None
    complete = brief(
        database,
        now=now,
        poll_stats=poll(
            identity="recovered",
            start=PREVIOUS,
            end=now,
        ),
    )
    assert complete["coverage"]["complete_through_observation"] is True


def test_unknown_health_is_not_silence_and_rights_do_not_leak_body(database: Database) -> None:
    empty = brief(database)
    assert empty["source_health_status"] == "unobserved"
    assert "source_poll_not_observed" in empty["capability_gaps"]
    event = source(
        source="alpaca:benzinga",
        source_url="https://news.invalid/story",
        content=None,
        issuer_id=None,
        cik=None,
        related_instruments=(),
        mapping_available_at=None,
        is_primary_source=False,
        provider_symbols=("AAPL",),
        rights_profile="metadata_only_retention_not_authorized",
        raw_metadata_json=json.dumps({"source": "benzinga", "id": 12}),
    )
    store_event(database, event)
    result = brief(database, poll_stats=poll())
    assert result["funnel"]["supported_company_matches"] == 1
    assert result["funnel"]["verified_issuer_matches"] == 0
    assert result["funnel"]["valid_quantitative_events"] == 0
    assert GUIDANCE not in json.dumps(result)
    assert result["news"][0]["quantitative_facts"] == []
    assert result["news"][0]["immutable_reference"].startswith("event_evidence:")
    assert "licensed_news_metadata_only_no_body_extraction" in result["capability_gaps"]


@pytest.mark.parametrize(
    "now,status",
    [
        (OPEN, "missed_premarket_deadline"),
        (CLOSE + timedelta(days=1), "missed_session"),
    ],
)
def test_late_start_does_not_backdate_or_roll_tuesday_forward(
    database: Database,
    now: datetime,
    status: str,
) -> None:
    result = brief(database, now=now)
    assert result["preparation_status"] == status
    assert result["prepared_before_open"] is False
    assert result["prepared_at"] == now.astimezone(UTC).isoformat()
    assert result["session_date"] == "2026-09-08"


def test_prior_context_must_be_verified_same_issuer_and_observed_before_event() -> None:
    event = source(published_at=NOW - timedelta(minutes=5))
    prior = source(
        source_event_id="annual",
        content=ANNUAL,
        content_sha256=text_hash(ANNUAL),
        published_at=NOW - timedelta(days=1),
        first_received_at=NOW - timedelta(hours=1),
        content_available_at=NOW - timedelta(hours=1),
    )
    old_but_received_later = prior.model_copy(
        update={
            "source_event_id": "downloaded-today",
            "first_received_at": NOW,
            "content_available_at": NOW,
        }
    )
    future = prior.model_copy(
        update={
            "source_event_id": "future",
            "published_at": NOW + timedelta(hours=1),
        }
    )
    wrong = prior.model_copy(
        update={
            "source_event_id": "wrong",
            "issuer_id": supported_issuer_mappings()[1].issuer_id,
        }
    )
    bad_url = prior.model_copy(
        update={
            "source_event_id": "bad-url",
            "source_url": "https://attacker.invalid/",
        }
    )
    missing_publication_event = event.model_copy(update={"published_at": None})
    assert available_prior_evidence(
        event, (prior, old_but_received_later, future, wrong, bad_url), now=NOW
    ) == (prior,)
    assert not available_prior_evidence(missing_publication_event, (prior,), now=NOW)
    contract = event.model_copy(
        update={
            "content": "Apple signed a binding contract valued at USD 100 million over 12 months.",
            "content_sha256": text_hash(
                "Apple signed a binding contract valued at USD 100 million over 12 months."
            ),
        }
    )
    result = extract_event(
        contract, now=NOW, prior_evidence=available_prior_evidence(contract, (prior,), now=NOW)
    )
    assert "point_in_time_annual_revenue" not in result.missing_required_fields


def test_synthetic_future_and_other_cohort_extractions_not_counted(database: Database) -> None:
    synthetic = source(availability_basis="synthetic", source_event_id="synthetic")
    future = source(
        source_event_id="future",
        published_at=NOW + timedelta(minutes=1),
        first_received_at=NOW + timedelta(minutes=1),
        content_available_at=NOW + timedelta(minutes=1),
    )
    current = source(source_event_id="unextracted")
    for event in (synthetic, future, current):
        store_event(database, event, extraction=False)
    output = extract_event(current, now=NOW)
    EventStore(database).audit(
        "extraction",
        output.model_dump(mode="json"),
        NOW,
        f"other-cohort:{current.evidence_id}:extraction",
    )
    result = brief(database)
    assert result["funnel"]["unique_evidence_versions"] == 1
    assert result["funnel"]["synthetic_or_replay_evidence_excluded"] == 1
    assert result["funnel"]["extractions_pending"] == 1
    assert result["funnel"]["valid_quantitative_events"] == 0


def test_aware_verified_boundaries_required_and_cannot_rewrite_time(database: Database) -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        brief(database, session_open=OPEN.replace(tzinfo=None))
    with pytest.raises(ValueError, match="increase"):
        brief(database, previous_session_close=OPEN)
    brief(database)
    with pytest.raises(ValueError, match="backdated"):
        brief(database, now=NOW - timedelta(minutes=1))
    with pytest.raises(ValueError, match="nonfuture"):
        brief(database, poll_stats=poll(end=NOW + timedelta(minutes=1)))


def test_missing_publication_prior_filing_is_context_not_new_company_event(
    database: Database,
) -> None:
    older = source(
        source_event_id="annual-filing",
        published_at=None,
        content=ANNUAL,
        content_sha256=text_hash(ANNUAL),
        raw_metadata_json=json.dumps(
            {
                "filing": {
                    "collection_role": "older_comparison",
                    "accepted_at": "2025-11-01T12:00:00Z",
                }
            }
        ),
    )
    store_event(database, older)
    result = brief(database, poll_stats=poll())
    assert result["funnel"]["older_context_documents"] == 1
    assert result["funnel"]["unique_events"] == 0
    assert result["older_context_evidence_ids"] == [older.evidence_id]
    assert not result["news"]
    assert set(result["companies"]) == {"AAPL", "MSFT", "NVDA"}


def test_stopped_polling_is_distinct_from_healthy_silence(database: Database) -> None:
    result = brief(database, poll_stats=poll(raw_count=0))
    assert result["source_health_status"] == "configured_sources_observed_silence"
    stale = brief(database, now=NOW + timedelta(minutes=3))
    assert stale["source_health_status"] == "stale_no_recent_source_poll"
    assert stale["coverage"]["complete_through_observation"] is False


def annual_source(**overrides: Any) -> SourceEvent:
    fields = {
        "source_event_id": "annual",
        "content": ANNUAL,
        "content_sha256": text_hash(ANNUAL),
        "published_at": NOW - timedelta(days=1),
        "first_received_at": NOW - timedelta(hours=1),
        "content_available_at": NOW - timedelta(hours=1),
    }
    fields.update(overrides)
    return source(**fields)


@pytest.mark.parametrize("flag", ["is_correction", "is_retraction", "revision_of"])
def test_prior_comparisons_never_use_corrections_or_retractions(flag: str) -> None:
    event = source(published_at=NOW - timedelta(minutes=5))
    prior = annual_source(**{flag: "old-id" if flag == "revision_of" else True})
    assert available_prior_evidence(event, (prior,), now=NOW) == ()


def test_later_observed_revision_invalidates_old_annual_context_not_future_versions() -> None:
    event = source(published_at=NOW - timedelta(minutes=5))
    old = annual_source()
    later = annual_source(
        source_version="version-two",
        revision_of=old.evidence_id,
        first_received_at=NOW,
        content_available_at=NOW,
    )
    assert available_prior_evidence(event, (old, later), now=NOW) == ()
    assert available_prior_evidence(event, (old, later), now=NOW - timedelta(seconds=1)) == (old,)
    later_other_document = later.model_copy(update={"source_event_id": "amendment-document"})
    assert available_prior_evidence(event, (old, later_other_document), now=NOW) == ()


def test_frozen_packet_loads_original_order_and_never_adds_later_evidence(
    database: Database,
) -> None:
    event = source(published_at=NOW - timedelta(minutes=5))
    prior = annual_source()
    extraction = extract_event(event, now=NOW, prior_evidence=(prior,))
    for item in (prior, event):
        store_event(database, item, extraction=False)
    later = annual_source(
        source_version="later",
        revision_of=prior.evidence_id,
        first_received_at=NOW + timedelta(minutes=1),
        content_available_at=NOW + timedelta(minutes=1),
    )
    store_event(database, later, extraction=False)
    assert load_frozen_evidence_packet(database, extraction, now=NOW + timedelta(minutes=2)) == (
        event,
        prior,
    )
    assert available_prior_evidence(event, (prior, later), now=NOW + timedelta(minutes=2)) == ()


def test_frozen_packet_missing_changed_or_future_inputs_fail_explicitly(database: Database) -> None:
    event = source()
    prior = annual_source()
    extraction = extract_event(event, now=NOW, prior_evidence=(prior,))
    store_event(database, event, extraction=False)
    with pytest.raises(ValueError, match="missing_versions"):
        load_frozen_evidence_packet(database, extraction, now=NOW)
    store_event(database, prior, extraction=False)
    with pytest.raises(ValueError, match="not_yet_available"):
        load_frozen_evidence_packet(database, extraction, now=NOW - timedelta(seconds=1))
    corrupted_payload = prior.model_dump(mode="json")
    corrupted_payload["first_received_at"] = (NOW - timedelta(hours=2)).isoformat()
    with database.begin() as connection:
        connection.execute(
            update(event_evidence)
            .where(event_evidence.c.evidence_id == prior.evidence_id)
            .values(payload=corrupted_payload)
        )
    with pytest.raises(ValueError, match="hash_mismatch"):
        load_frozen_evidence_packet(database, extraction, now=NOW)


def test_official_feed_gaps_are_per_issuer_not_false_claim_of_no_discovery(
    database: Database,
) -> None:
    capabilities = {
        **CAPABILITIES,
        "issuer_feeds_enabled": True,
        "issuer_feed_status": {
            "AAPL": {
                "status": "healthy",
                "publication_timestamp_status": (
                    "feed_publication_missing_requires_explicit_document_timestamp"
                ),
            },
            "MSFT": {"status": "healthy", "gap": "cloud_blog_only_not_full_corporate_news"},
            "NVDA": {"status": "healthy", "rejected_link_count": 1},
        },
    }
    result = brief(database, source_capabilities=capabilities, poll_stats=poll())
    gaps = result["capability_gaps"]
    assert "no_configured_issuer_primary_urls" not in gaps
    assert "official_issuer_feed_discovery_disabled_or_unavailable" not in gaps
    assert "issuer_feed:AAPL:publication_missing_updated_is_not_publication" in gaps
    assert "issuer_feed:MSFT:cloud_blog_only_not_full_corporate_news" in gaps
    assert "issuer_feed:NVDA:entries_outside_existing_verified_URL_policy" in gaps


def test_new_version_observation_does_not_invent_a_provider_revision_clock(
    database: Database,
) -> None:
    first = source(provider_updated_at=NOW - timedelta(days=2))
    revised = source(
        source_version="metadata-correction",
        revision_of=first.evidence_id,
        provider_updated_at=first.provider_updated_at,
        first_received_at=NOW,
        content_available_at=NOW,
        is_correction=True,
    )
    for item in (first, revised):
        store_event(database, item)
    result = brief(database)
    row = next(row for row in result["news"] if row["evidence_id"] == revised.evidence_id)
    assert row["revision_time"] is None
    assert row["provider_updated_at"] == first.provider_updated_at.isoformat()
    assert row["revision_observed_at"] == NOW.isoformat()
