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
from sqlalchemy import create_engine, event, func, insert, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import OperationalError

from tradeagent import api
from tradeagent.daily_status import DailyStatusScheduler, DailyStatusSettings
from tradeagent.event_brief import persist_premarket_brief
from tradeagent.event_research import SourceEvent, supported_issuer_mappings, text_hash
from tradeagent.event_session_report import _audit_read_model, session_report
from tradeagent.event_store import EventStore, event_evidence, event_order_links
from tradeagent.persistence import (
    Database,
    ProductionRepository,
    events,
    notification_outbox,
    orders,
)
from tradeagent.reporting_reads import POLL_FIELDS, READ_BATCH_SIZE, projected_row_query

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
        # One narrow metadata query, one latest-state/incident page, and three poll pages.
        assert len(queries) == 5
        for query in queries[1:]:
            identities = query.compile().params["event_id_1"]
            assert 0 < len(identities) <= READ_BATCH_SIZE
        assert all("row_number" not in str(query).lower() for query in queries)
        assert not any(column is events.c.payload for column in queries[0].selected_columns)
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
