from __future__ import annotations

import json
import tracemalloc
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import (
    create_engine,
    event,
    func,
    insert,
    select,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import OperationalError

from tradeagent import api, reporting_reads
from tradeagent.daily_status import DailyStatusScheduler, DailyStatusSettings, build_daily_status
from tradeagent.event_brief import persist_premarket_brief
from tradeagent.event_research import SourceEvent, supported_issuer_mappings, text_hash
from tradeagent.event_session_report import _audit_read_model, session_report
from tradeagent.event_store import EventStore, event_decisions, event_evidence, event_order_links
from tradeagent.persistence import (
    Database,
    ProductionRepository,
    event_reporting_metadata,
    events,
    notification_outbox,
    orders,
)
from tradeagent.reporting_reads import (
    AUDIT_SCOPE_FIELDS,
    POLL_FIELDS,
    READ_BATCH_SIZE,
    REPORTING_PROJECTION_VERSION,
    ReportingReadModelIncompleteError,
    ReportPayloadTooLargeError,
    event_reporting_projection,
    projected_row_query,
    reporting_metadata_query,
    reporting_projection_from_row,
)

DAY = date(2026, 9, 8)
NOW = datetime(2026, 9, 8, 22, tzinfo=UTC)
OPEN = datetime(2026, 9, 8, 13, 30, tzinfo=UTC)
PREVIOUS = datetime(2026, 9, 4, 20, tzinfo=UTC)
COHORT = "production-volume-report"


def seed_cohort(database: Database) -> None:
    EventStore(database).freeze(
        COHORT,
        "frozen",
        {
            "purpose": "iex-practice",
            "settings": {
                "purpose": "iex-practice",
                "planned_session_date": str(DAY),
                "virtual_equity": "10000",
            },
        },
        "experimental-paper",
        NOW,
    )
    ProductionRepository(database).heartbeat(
        "tradeagent-event-worker",
        "worker",
        {"cohort_id": COHORT, "purpose": "iex-practice", "state": "collecting"},
        observed_at=NOW,
    )


def seed_loss(database: Database) -> None:
    with database.begin() as connection:
        for side, price in (("buy", "100"), ("sell", "90")):
            at = OPEN + timedelta(minutes=1 if side == "buy" else 2)
            connection.execute(
                insert(orders).values(
                    order_id=side,
                    client_order_id=side,
                    strategy_version=COHORT,
                    symbol="AAPL",
                    side=side,
                    quantity=Decimal(".25"),
                    filled_quantity=Decimal(".25"),
                    status="filled",
                    created_at=at,
                    updated_at=at,
                )
            )
            connection.execute(
                insert(event_order_links).values(
                    client_order_id=side,
                    cohort_id=COHORT,
                    cluster_key=side,
                    payload={
                        "planned_session_date": str(DAY),
                        "trade_classification": "NEWS_STRATEGY",
                        "broker": {
                            "filled_quantity": ".25",
                            "filled_average_price": price,
                            "status": "filled",
                        },
                    },
                )
            )


def test_production_sized_snapshots_do_not_materialize_historical_bodies(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    record_property: Any,
) -> None:
    """8000 x 64 KiB audit bodies exceed the entire 512 MiB Render service limit."""
    decoded: Counter[str] = Counter()
    streamed: list[int] = []

    def decode(raw: str) -> Any:
        value = json.loads(raw)
        if isinstance(value, dict) and value.get("payload_marker"):
            decoded[value["payload_marker"]] += 1
        return value

    url = f"sqlite:///{tmp_path / 'accumulated.db'}"
    with Database(url) as database:
        database.dispose()
        database.engine = create_engine(url, json_deserializer=decode)
        database.initialize()
        seed_cohort(database)
        seed_loss(database)
        baseline = session_report(database, COHORT, DAY, observed_at=NOW)
        ticks = 2000
        body = "retained immutable source body " + "x" * 65536
        types = ("premarket_brief", "official_context", "performance", "source_poll")
        with database.begin() as connection:
            for offset in range(0, ticks, 20):
                batch = []
                for index in range(offset, offset + 20):
                    at = OPEN + timedelta(seconds=index)
                    for kind in types:
                        payload: dict[str, Any] = {
                            "payload_marker": kind,
                            "planned_session_date": str(DAY),
                            "serial": index,
                        }
                        if kind == "source_poll":
                            payload.update(
                                poll_id=f"poll-{index}",
                                observed_at=at.isoformat(),
                                raw_items_received=3,
                                healthy=True,
                                errors=[],
                                source_health={
                                    "news": {
                                        "status": "healthy",
                                        "http_successes": 1,
                                        "last_http_received_at": at.isoformat(),
                                        "provider_response": body,
                                    },
                                },
                            )
                        elif kind == "premarket_brief":
                            payload.update(
                                schema_version="premarket-news-brief-v1",
                                cohort_id=COHORT,
                                session_date=str(DAY),
                                prepared_at=at.isoformat(),
                                snapshot_id=f"brief-{index}",
                                funnel={"raw_items_received": (index + 1) * 3},
                                news=[
                                    {
                                        "evidence_id": "retained-news",
                                        "publisher": "Issuer",
                                        "quantitative_facts": [],
                                        "supporting_excerpt": body,
                                        "immutable_reference": "event_evidence:retained-news",
                                    }
                                ],
                            )
                        elif kind == "official_context":
                            payload["evidence"] = [{"source": "official", "document": body}]
                        else:
                            payload["news_strategy"] = {"source_context": body}
                        batch.append(
                            {
                                "event_id": f"{kind}-{index}",
                                "event_type": f"event_{kind}",
                                "occurred_at": at,
                                "recorded_at": at,
                                "trace_id": COHORT,
                                "payload": payload,
                            }
                        )
                connection.execute(insert(events), batch)
                connection.execute(
                    insert(event_reporting_metadata),
                    [
                        {
                            "event_id": row["event_id"],
                            "projection_version": REPORTING_PROJECTION_VERSION,
                            "payload": event_reporting_projection(
                                row["event_type"], row["payload"]
                            ),
                        }
                        for row in batch
                    ],
                )
        EventStore(database).audit("incident", {"error": "retained_real_incident"}, NOW, COHORT)

        def observe_query(
            connection: Any, clause: Any, multiparams: Any, params: Any, options: Any
        ) -> None:
            if options.get("yield_per"):
                streamed.append(options["yield_per"])

        event.listen(database.engine, "before_execute", observe_query)
        decoded.clear()
        tracemalloc.start()
        result = session_report(database, COHORT, DAY, observed_at=NOW, persist=True)
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        record_property("report_peak_python_bytes", peak)
        assert peak < 96 * 1024 * 1024
        assert decoded == {"premarket_brief": 1, "official_context": 1, "performance": 1}
        assert streamed and set(streamed) == {READ_BATCH_SIZE}
        assert result["funnel"]["items_received"]["count"] == ticks * 3
        assert result["health"]["poll_count"] == ticks
        assert result["news_strategy"] == baseline["news_strategy"]
        assert Decimal(result["news_strategy"]["broker_paper_pnl"]) == Decimal("-2.50")
        assert (
            result["funnel"]["fills"]["count"] is None
        )  # cumulative fills are not execution events
        assert result["ending_exposure"]["completion_confirmed"] is False
        counts = result["history_accounting"]["event_counts"]
        assert all(counts[f"event_{kind}"]["count"] == ticks for kind in types)
        assert counts["event_incident"]["count"] == 1
        assert any(row["kind"] == "incident" for row in result["timeline"])
        with database.begin() as connection:
            assert connection.scalar(select(func.count()).select_from(events)) == ticks * 4 + 2
            assert (
                connection.scalar(
                    select(events.c.payload["news"][0]["supporting_excerpt"].as_string()).where(
                        events.c.event_id == "premarket_brief-0",
                    )
                )
                == body
            )

        monkeypatch.setattr(api, "Database", lambda *args, **kwargs: database)
        monkeypatch.setattr(
            api,
            "session_report",
            lambda *args, **kwargs: pytest.fail("hot endpoint rebuilt EOD report"),
        )
        calls = 0
        original = api._event_overview

        def overview(*args: Any, **kwargs: Any) -> dict[str, Any]:
            nonlocal calls
            calls += 1
            return original(*args, **kwargs)

        monkeypatch.setattr(api, "_event_overview", overview)
        decoded.clear()
        with TestClient(api.create_app(production_database_url=url)) as client:
            with ThreadPoolExecutor(max_workers=16) as executor:
                responses = list(
                    executor.map(lambda _: client.get("/api/event-product"), range(48))
                )
            assert all(response.status_code == 200 for response in responses)
            assert calls == 1
            assert len({response.json()["as_of"] for response in responses}) == 1
            assert all(len(response.content) < 32768 for response in responses)
            assert not decoded
            assert all(response.json()["overview_only"] for response in responses)
        # Daily report must already be durable before the outbox can be delivered.
        assert DailyStatusScheduler(database, DailyStatusSettings(_env_file=None)).enqueue_due(
            observed_at=NOW
        )
        with database.begin() as connection:
            outbox = connection.execute(select(notification_outbox)).mappings().one()
            assert outbox["status"] == "pending"
            assert outbox["payload"]["session_report_id"] == result["report_id"]
            assert "session_report" not in outbox["payload"]
            assert (
                connection.scalar(
                    select(events.c.event_id).where(
                        events.c.event_id == outbox["payload"]["session_report_id"],
                    )
                )
                == result["report_id"]
            )


def test_brief_pages_validates_and_compacts_large_historical_documents(
    tmp_path: Path,
    record_property: Any,
) -> None:
    with Database(f"sqlite:///{tmp_path / 'brief.db'}") as database:
        database.initialize()
        seed_cohort(database)
        mapping = supported_issuer_mappings()[0]
        body = "For fiscal 2025, GAAP annual revenue was USD 1000 million.\n" + "x" * 65536
        received = PREVIOUS - timedelta(days=1)
        values = {
            "source": "issuer_primary",
            "source_url": "https://www.apple.com/newsroom/release/",
            "source_version": "v1",
            "published_at": received,
            "first_received_at": received,
            "content_available_at": received,
            "content": body,
            "content_sha256": text_hash(body),
            "raw_payload_sha256": text_hash(body),
            "issuer_id": mapping.issuer_id,
            "cik": mapping.cik,
            "related_instruments": ("AAPL",),
            "mapping_available_at": mapping.available_at,
            "is_primary_source": True,
            "rights_profile": "public_primary_document_research_retention",
        }
        with database.begin() as connection:
            for offset in range(0, 1200, 20):
                batch = []
                for index in range(offset, offset + 20):
                    source = SourceEvent.model_validate(
                        {
                            **values,
                            "source_event_id": f"old-{index}",
                            "event_cluster_id": f"cluster-{index}",
                        }
                    )
                    batch.append(
                        {
                            "evidence_id": source.evidence_id,
                            "received_at": received,
                            "payload": source.model_dump(mode="json"),
                        }
                    )
                connection.execute(insert(event_evidence), batch)
        at = OPEN - timedelta(minutes=1)
        with database.begin() as connection:
            for index in range(500):
                EventStore(database).audit(
                    "source_poll",
                    {
                        "poll_id": str(index),
                        "observed_at": at.isoformat(),
                        "raw_items_received": 3,
                        "source_health": {"news": {"status": "healthy", "provider_response": body}},
                    },
                    at,
                    f"{COHORT}:{DAY}:premarket_brief:poll:{index}",
                    connection,
                )
        sidecar_queries = []

        def observe(connection: Any, clause: Any, *args: Any) -> None:
            if getattr(clause, "is_select", False) and "event_reporting_metadata" in str(clause):
                sidecar_queries.append(str(clause))

        event.listen(database.engine, "before_execute", observe)
        tracemalloc.start()
        brief = persist_premarket_brief(
            database,
            cohort_id=COHORT,
            session_open=OPEN,
            session_close=NOW,
            previous_session_close=PREVIOUS,
            now=OPEN - timedelta(minutes=1),
            source_capabilities={},
        )
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        record_property("brief_peak_python_bytes", peak)
        assert peak < 40 * 1024 * 1024
        assert brief["funnel"]["older_context_documents"] == 1200
        assert brief["funnel"]["invalid_evidence_records"] == 0
        assert brief["funnel"]["unique_events"] == 0
        assert brief["funnel"]["raw_items_received"] == 1500
        assert len(sidecar_queries) == 1
        assert "events_v2.payload" not in sidecar_queries[0]
        assert brief["older_context_evidence_ids_truncated"] is True
        assert len(brief["older_context_evidence_ids"]) == 100
        with database.begin() as connection:
            assert connection.scalar(select(func.count()).select_from(event_evidence)) == 1200
            assert (
                connection.scalar(select(event_evidence.c.payload["content"].as_string()).limit(1))
                == body
            )


def test_production_controls_and_dependency_failure_never_fall_back_to_local(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    url = f"sqlite:///{tmp_path / 'production.db'}"
    with Database(url) as database:
        database.initialize()
        ProductionRepository(database).set_control("kill_switch", "active")
    client = TestClient(
        api.create_app(production_database_url=url, ledger_path=tmp_path / "local.db")
    )
    result = client.get("/api/status").json()
    assert result["state_source"] == "production_database" and result["kill_switch"] == "active"
    assert client.get("/ready").json()["database"] == "reachable"
    assert not (tmp_path / "local.db").exists()

    calls = 0

    def unavailable(_: str) -> Any:
        nonlocal calls
        calls += 1
        raise OperationalError("connect", {}, RuntimeError("offline"))

    monkeypatch.setattr(Database, "begin", unavailable)
    assert client.get("/health").status_code == 200
    assert client.get("/ready").status_code == 503
    assert client.get("/api/status").status_code == 503
    calls = 0
    for _ in range(10):
        assert client.get("/api/event-product").status_code == 503
    assert calls == 1


def test_ready_projects_roles_leases_controls_and_durable_progress(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    url = f"sqlite:///{tmp_path / 'roles.db'}"
    now = datetime.now(UTC)
    body = "large-heartbeat-body-" + "x" * (1024 * 1024)
    with Database(url) as database:
        database.initialize()
        seed_cohort(database)
        repository = ProductionRepository(database)
        repository.set_control("kill_switch", "active")
        repository.set_control(f"{COHORT}:pause", "reconciliation_required")
        repository.heartbeat(
            "tradeagent-event-worker",
            "event-owner",
            {
                "state": "collecting",
                "cohort_id": COHORT,
                "tick_latency_seconds": 7.7,
                "events_received": 3,
                "source_capabilities": {
                    "last_errors": ["news:incomplete_interval"],
                    "http_cache": {"bytes": 16000, "entries": 4, "evictions": 2},
                    "last_poll_stats": {"provider_response": body},
                },
                "premarket_brief": {
                    "funnel": {"raw_items_received": 90},
                    "coverage": {"coverage_watermark": now.isoformat()},
                    "preparation_status": "missed_premarket_deadline",
                    "news": [body],
                },
            },
            observed_at=now,
        )
        repository.acquire_worker_lock("tradeagent-event-worker", "event-owner", observed_at=now)
        repository.heartbeat(
            "tradeagent-shadow-recorder",
            "recorder-owner",
            {
                "state": "healthy",
                "committed": 75000,
                "inserted": 74998,
                "duplicates": 2,
                "queue_depth": 3,
                "last_commit_at": now.isoformat(),
                "last_committed_received_at": now.isoformat(),
                "unrelated_history": body,
            },
            observed_at=now,
        )
        repository.acquire_worker_lock(
            "tradeagent-shadow-recorder", "different-owner", observed_at=now
        )
        repository.heartbeat(
            "tradeagent-notifier",
            "notifier-owner",
            {"state": "running"},
            observed_at=now - timedelta(seconds=121),
        )
        repository.append_event(
            "shadow_recorder_batch",
            {
                "received": 500,
                "inserted": 499,
                "duplicates": 1,
                "last_received_at": now.isoformat(),
                "body": body,
            },
            occurred_at=now,
            trace_id="durable-batch",
        )

    def no_full_heartbeat(*_: Any, **__: Any) -> Any:
        pytest.fail("light operational endpoints loaded a full heartbeat")

    monkeypatch.setattr(ProductionRepository, "latest_heartbeat", no_full_heartbeat)
    with TestClient(api.create_app(production_database_url=url)) as client:
        response = client.get("/ready")
        assert response.status_code == 200 and len(response.content) < 16384
        status = response.json()["operational_status"]
        assert status["state_source"] == "production_database"
        assert status["controls"]["kill_switch"]["value"] == "active"
        assert status["controls"]["kill_switch"]["updated_at"]
        assert status["controls"]["cohort_pause"]["value"] == "reconciliation_required"
        roles = status["roles"]
        assert roles["tradeagent-event-worker"]["fresh"] is True
        assert roles["tradeagent-event-worker"]["lease"]["owner_matches_heartbeat"] is True
        assert roles["tradeagent-shadow-recorder"]["lease"]["owner_matches_heartbeat"] is False
        assert roles["tradeagent-shadow-recorder"]["reported"]["committed"] == 75000
        assert roles["tradeagent-notifier"]["fresh"] is False
        assert roles["tradeagent-notifier"]["lease"]["present"] is False
        assert roles["tradeagent-shadow-market-feed"]["heartbeat_at"] is None
        assert status["latest_committed_recorder_batch"]["inserted"] == 499
        product = client.get("/api/event-product")
        assert product.status_code == 200 and len(product.content) < 32768
        overview = product.json()
        assert overview["source_capabilities"]["http_cache"]["bytes"] == 16000
        assert "news:incomplete_interval" in overview["blockers"]
        assert overview["premarket_brief"]["funnel"]["raw_items_received"] == 90
        assert overview["premarket_brief"]["preparation_status"] == "missed_premarket_deadline"
        assert body[:100] not in response.text + product.text


def test_postgresql_poll_projection_parses_json_once_instead_of_per_field() -> None:
    connection = SimpleNamespace(dialect=postgresql.dialect())
    query = projected_row_query(
        connection,
        events,
        POLL_FIELDS,
        (events.c.event_id,),
    ).where(events.c.event_id.in_(["verified-poll-id"]))
    sql = str(query.compile(dialect=connection.dialect))
    assert sql.count("json_to_record(") == 1
    assert "JOIN LATERAL json_to_record(events_v2.payload)" in sql
    assert " -> " not in sql and " ->> " not in sql
    assert all(f"{field} JSON" in sql for field in POLL_FIELDS)
    assert "events_v2.event_id IN (" in sql
    assert "events_v2.payload" not in sql.split("FROM")[0]


def test_immutable_reporting_projection_preserves_scope_types_and_poll_accounting() -> None:
    scope = {
        "planned_session_date": None,
        "session_date": "2026-09-07",
        "mode": "offline_replay",
        "synthetic": False,
        "evidence_kind": "replay",
        "session_id": "",
        "account_digest": None,
    }
    payload = {
        **scope,
        "body": "heavy retained body " * 10000,
        "poll_id": "unchanged-receipts",
        "raw_items_received": 0,
        "healthy": False,
        "coverage_complete": None,
        "source_health": {"news": {"status": "failed", "provider_response": "x" * 65536}},
    }
    before = json.dumps(payload, sort_keys=True)
    state = event_reporting_projection("event_official_context", payload)
    assert state == {"scope": scope}
    assert set(state["scope"]) == set(AUDIT_SCOPE_FIELDS)
    assert event_reporting_projection("event_incident", {}) == {"scope": {}}
    poll = event_reporting_projection("event_source_poll", payload)
    assert poll["scope"] == scope
    assert poll["poll"]["raw_items_received"] == 0
    assert poll["poll"]["healthy"] is False
    assert poll["poll"]["coverage_complete"] is None
    assert poll["poll"]["source_health"] == {"news": {"status": "failed"}}
    assert len(json.dumps(poll)) < 2048
    assert json.dumps(payload, sort_keys=True) == before


def test_postgresql_audit_sidecar_query_never_parses_original_json() -> None:
    query = reporting_metadata_query(events, event_reporting_metadata).where(
        events.c.trace_id == COHORT, events.c.occurred_at <= NOW
    )
    sql = str(query.compile(dialect=postgresql.dialect()))
    assert "LEFT OUTER JOIN event_reporting_metadata" in sql
    assert "event_reporting_metadata.projection_version =" in sql
    assert "event_reporting_metadata.payload AS reporting_projection" in sql
    assert "events_v2.payload" not in sql
    assert "json_to_record" not in sql and " -> " not in sql and " ->> " not in sql


@pytest.mark.parametrize("projection", [None, [], {}, {"scope": []}, {"scope": {}, "poll": None}])
def test_missing_or_invalid_reporting_sidecar_fails_explicitly(projection: Any) -> None:
    with pytest.raises(ReportingReadModelIncompleteError, match="events_v2:retained-event"):
        reporting_projection_from_row(
            {
                "event_id": "retained-event",
                "event_type": "event_source_poll",
                "reporting_projection": projection,
            }
        )


@pytest.mark.parametrize("version,projection", [(None, None), (2, {"scope": {}}), (1, {})])
def test_incomplete_sidecar_never_falls_back_to_parsing_historical_bodies(
    version: int | None, projection: Any
) -> None:
    with Database("sqlite:///:memory:") as database:
        database.initialize()
        with database.begin() as connection:
            connection.execute(
                insert(events).values(
                    event_id="retained-unprojected",
                    event_type="event_official_context",
                    occurred_at=NOW,
                    recorded_at=NOW,
                    trace_id=COHORT,
                    payload={"body_base64": "x" * (4 * 1024 * 1024)},
                )
            )
            if version is not None:
                connection.execute(
                    insert(event_reporting_metadata).values(
                        event_id="retained-unprojected",
                        projection_version=version,
                        payload=projection,
                    )
                )
        queries = []

        def observe(connection: Any, clause: Any, *args: Any) -> None:
            queries.append(str(clause))

        event.listen(database.engine, "before_execute", observe)
        with (
            database.begin() as connection,
            pytest.raises(ReportingReadModelIncompleteError, match="retained-unprojected"),
        ):
            _audit_read_model(connection, events.c.trace_id == COHORT, DAY, OPEN, NOW)
        assert len(queries) == 1
        assert "events_v2.payload" not in queries[0]


def test_sidecar_preserves_absent_null_date_account_session_and_synthetic_semantics() -> None:
    payloads = {
        "absent": {},
        "null_planned": {
            "planned_session_date": None,
            "session_date": str(DAY - timedelta(days=1)),
        },
        "synthetic_true": {"synthetic": True},
        "synthetic_string": {"synthetic": "true"},
        "replay": {"mode": "offline_replay"},
        "null_account": {"account_digest": None},
        "other_account": {"account_digest": "other"},
        "null_session": {"session_id": None},
        "other_session": {"session_id": "other"},
        "other_date": {"planned_session_date": str(DAY - timedelta(days=1))},
    }
    with Database("sqlite:///:memory:") as database:
        database.initialize()
        store = EventStore(database)
        for identity, payload in payloads.items():
            store.audit("incident", {"case": identity, **payload}, NOW, COHORT)
        with database.begin() as connection:
            rows, history = _audit_read_model(
                connection,
                events.c.trace_id == COHORT,
                DAY,
                OPEN,
                NOW,
                session_id="current-session",
                account="current-account",
            )
        assert {row["payload"]["case"] for row in rows} == {
            "absent",
            "null_planned",
            "synthetic_true",
            "synthetic_string",
            "replay",
        }
        counts = history["event_counts"]["event_incident"]
        assert counts["count"] == 5
        assert counts["forward_count"] == 3
        assert counts["synthetic_or_replay_count"] == 2
        assert counts["first_at"] == counts["last_at"] == NOW.isoformat()


@pytest.mark.parametrize("decision_count,body_characters", [(6, 4 * 1024 * 1024), (60, 450 * 1024)])
def test_multimegabyte_official_bodies_are_referenced_before_report_persistence(
    tmp_path: Path,
    decision_count: int,
    body_characters: int,
    record_property: Any,
) -> None:
    body = "eA==" * (body_characters // 4)
    context = {
        "observed_at": NOW.isoformat(),
        "evidence": [
            {
                "evidence_id": "official-receipt",
                "source_url": "https://example.test/official",
                "content_sha256": "a" * 64,
                "body_base64": body,
                "first_received_at": NOW.isoformat(),
                "received_at": NOW.isoformat(),
                "error": None,
            }
        ],
        "macro_risk_windows": [{"start": OPEN.isoformat(), "end": NOW.isoformat()}],
        "halts": [{"symbol": "AAPL", "halted": None, "reason": "unknown_feed_status"}],
        "errors": ["unverified_source_coverage"],
    }
    with Database(f"sqlite:///{tmp_path / 'large-insert.db'}") as database:
        database.initialize()
        seed_cohort(database)
        seed_loss(database)
        store = EventStore(database)
        store.audit("official_context", context, NOW, COHORT)
        with database.begin() as connection:
            connection.execute(
                insert(event_evidence),
                [
                    {"evidence_id": f"source-{index}", "received_at": NOW, "payload": {}}
                    for index in range(decision_count)
                ],
            )
        decision_ids = [
            store.decision(
                COHORT,
                f"source-{index}",
                {
                    "symbol": "AAPL",
                    "action": "abstain",
                    "facts": [{"metric": "revenue", "value": "42", "unit": "USD"}],
                    "reasons": ["official_halt_status_unknown"],
                    "official_context": context,
                },
                NOW,
            )
            for index in range(decision_count)
        ]
        # Match the live 60 x 450 KiB decision expansion, as well as larger individual documents.
        assert len(body) * (decision_count + 1) > 27_000_000
        tracemalloc.start()
        report = session_report(database, COHORT, DAY, observed_at=NOW, persist=True)
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        serialized = json.dumps(report)
        record_property("decision_report_peak_python_bytes", peak)
        record_property("decision_report_bytes", len(serialized.encode("utf-8")))
        assert peak < 96 * 1024 * 1024
        assert len(serialized.encode("utf-8")) < 512 * 1024
        assert body[:100] not in serialized
        assert Decimal(report["news_strategy"]["broker_paper_pnl"]) == Decimal("-2.50")
        assert report["funnel"]["fills"]["count"] is None
        assert report["ending_exposure"]["completion_confirmed"] is False
        assert len(report["news_decisions"]) == decision_count
        assert report["news_display"]["recorded_decisions_total"] == decision_count
        assert report["failed_rules"] == {"official_halt_status_unknown": decision_count}
        assert all(
            row["facts"] == [{"metric": "revenue", "value": "42", "unit": "USD"}]
            and row["failed_rules"] == ["official_halt_status_unknown"]
            and row["original_decision_reference"] == f"event_decisions:{row['decision_id']}"
            for row in report["news_decisions"]
        )
        context_record = next(
            row for row in report["timeline"] if row["kind"] == "official_context"
        )
        for item in [
            context_record["evidence"],
            *[row["original_decision"]["official_context"] for row in report["news_decisions"]],
        ]:
            assert item["halts"] == context["halts"]
            assert item["macro_risk_windows"] == context["macro_risk_windows"]
            assert item["errors"] == context["errors"]
            receipt = item["evidence"][0]
            assert receipt["content_sha256"] == "a" * 64
            assert receipt["first_received_at"] == NOW.isoformat()
            assert receipt["source_body_references"]["body_base64"]["characters"] == len(body)
        pointer = report["news_decisions"][0]["original_decision"]["official_context"]["evidence"][
            0
        ]["source_body_references"]["body_base64"]["immutable_reference"]
        assert pointer.startswith("event_decisions:")
        assert pointer.endswith("#/official_context/evidence/0/body_base64")
        with database.begin() as connection:
            original = connection.scalar(
                select(event_decisions.c.payload).where(
                    event_decisions.c.decision_id == decision_ids[0]
                )
            )
            assert original["official_context"]["evidence"][0]["body_base64"] == body
            saved = connection.scalar(
                select(events.c.payload).where(events.c.event_id == report["report_id"])
            )
            assert saved == report
            assert connection.scalar(
                select(event_reporting_metadata.c.payload).where(
                    event_reporting_metadata.c.event_id == report["report_id"],
                    event_reporting_metadata.c.projection_version == REPORTING_PROJECTION_VERSION,
                )
            ) == event_reporting_projection("event_session_report", report)
            original_context = connection.scalar(
                select(events.c.payload).where(events.c.event_type == "event_official_context")
            )
            assert original_context == context
        digest = build_daily_status(database, NOW, "America/New_York")
        assert digest["session_report_id"] == report["report_id"]
        record_property("decision_digest_bytes", len(json.dumps(digest).encode("utf-8")))
        assert len(json.dumps(digest).encode("utf-8")) < 512 * 1024
        assert body[:100] not in digest["text"]


def test_oversized_report_is_rejected_before_insert_or_delivery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with Database("sqlite:///:memory:") as database:
        database.initialize()
        seed_cohort(database)
        writes = []

        def observe(connection: Any, clause: Any, *args: Any) -> None:
            if getattr(clause, "is_insert", False):
                writes.append(clause)

        event.listen(database.engine, "before_execute", observe)
        monkeypatch.setattr(reporting_reads, "REPORT_PAYLOAD_MAX_BYTES", 1024)
        with pytest.raises(ReportPayloadTooLargeError, match="no delivery was enqueued"):
            build_daily_status(database, NOW, "America/New_York")
        assert writes == []
        with database.begin() as connection:
            assert connection.scalar(select(func.count()).select_from(notification_outbox)) == 0


def test_audit_reads_metadata_once_then_bounded_verified_primary_keys() -> None:
    with Database("sqlite:///:memory:") as database:
        database.initialize()
        store = EventStore(database)
        for index in range(70):
            store.audit(
                "source_poll",
                {"poll_id": str(index), "raw_items_received": 1},
                NOW,
                COHORT,
            )
            store.audit("official_context", {"version": index}, NOW, COHORT)
        store.audit(
            "official_context",
            {"synthetic": True, "version": "excluded-newer"},
            NOW,
            COHORT,
        )
        store.audit("incident", {"planned_session_date": "invalid-date"}, NOW, COHORT)
        queries: list[Any] = []

        def observe_query(
            connection: Any,
            clause: Any,
            multiparams: Any,
            params: Any,
            options: Any,
        ) -> None:
            queries.append(clause)

        event.listen(database.engine, "before_execute", observe_query)
        with database.begin() as connection:
            rows, history = _audit_read_model(
                connection,
                events.c.trace_id == COHORT,
                DAY,
                OPEN,
                NOW,
            )
        # One sidecar query covers every poll/scope; only selected context/incident bodies follow.
        assert len(queries) == 2
        for query in queries[1:]:
            identities = query.compile().params["event_id_1"]
            assert 0 < len(identities) <= READ_BATCH_SIZE
        assert all("row_number" not in str(query).lower() for query in queries)
        assert not any(column is events.c.payload for column in queries[0].selected_columns)
        assert "events_v2.payload" not in str(queries[0])
        assert "event_reporting_metadata.payload" in str(queries[0])
        assert history["event_counts"]["event_official_context"]["count"] == 71
        assert history["event_counts"]["event_official_context"]["forward_count"] == 70
        assert history["event_counts"]["event_source_poll"]["count"] == 70
        assert sum(row["event_type"] == "event_official_context" for row in rows) == 1
        assert (
            next(row for row in rows if row["event_type"] == "event_official_context")["payload"][
                "version"
            ]
            == 69
        )
        # Malformed optional session dates still use actual occurrence time, not silent exclusion.
        assert any(row["event_type"] == "event_incident" for row in rows)


def test_full_report_never_requires_a_second_pool_connection(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'single-connection.db'}"
    with Database(url) as database:
        database.dispose()
        database.engine = create_engine(url, pool_size=1, max_overflow=0, pool_timeout=0.1)
        database.initialize()
        seed_cohort(database)
        seed_loss(database)
        checked_out = peak = 0

        def checkout(connection: Any, record: Any, proxy: Any) -> None:
            nonlocal checked_out, peak
            checked_out += 1
            peak = max(peak, checked_out)

        def checkin(connection: Any, record: Any) -> None:
            nonlocal checked_out
            checked_out -= 1

        event.listen(database.engine, "checkout", checkout)
        event.listen(database.engine, "checkin", checkin)
        report = session_report(database, COHORT, DAY, observed_at=NOW, persist=True)
        assert Decimal(report["news_strategy"]["broker_paper_pnl"]) == Decimal("-2.50")
        assert report["snapshot_persisted"] is True
        assert peak == 1 and checked_out == 0
