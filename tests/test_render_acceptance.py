from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest

from infra.render.acceptance_probe import email_status, market_window_counts, report_test
from infra.render.observe_release import (
    DATABASE,
    ROLE_STATES,
    SERVICES,
    acceptance_failures,
    dashboard_request,
    render_token,
)
from tradeagent.daily_status import daily_notification_id
from tradeagent.notifications import RoundTripNotificationRepository
from tradeagent.persistence import Database


def evidence() -> dict[str, Any]:
    roles = {
        name: {
            "fresh": True,
            "instance_id": name,
            "reported": {
                "state": sorted(states)[0],
                "code_sha": "abc123",
                "healthy": True,
                "gaps": 0,
                "dropped_events": 0,
                "last_committed_event_at": "2026-09-08T15:00:00+00:00",
            },
            "lease": {
                "present": True,
                "owner_matches_heartbeat": True,
                "recent_within_120_seconds": True,
            },
        }
        for name, states in ROLE_STATES.items()
    }
    later = deepcopy(roles)
    later["tradeagent-shadow-recorder"]["reported"]["last_committed_event_at"] = (
        "2026-09-08T15:10:00+00:00"
    )
    return {
        "started_at": "2026-09-08T15:00:00+00:00",
        "duration_seconds": 660,
        "expected_commit": "abc123",
        "requests": [
            {
                "status": 200,
                "path": "/ready",
                "payload": {"operational_status": {"roles": deepcopy(roles)}},
            },
            {
                "status": 200,
                "path": "/api/runtime",
                "payload": {"market_quotes": 10, "market_trades": 10, "market_bars": 10},
            },
            {
                "status": 200,
                "path": "/ready",
                "payload": {"operational_status": {"roles": deepcopy(later)}},
            },
            {
                "status": 200,
                "path": "/api/runtime",
                "payload": {"market_quotes": 20, "market_trades": 20, "market_bars": 20},
            },
        ],
        "database": {"status": "available", "ipAllowList": []},
        "final_deploys": {
            role: [{"deploy": {"status": "live", "commit": {"id": "abc123"}}}] for role in SERVICES
        },
        "events": {role: [] for role in SERVICES},
        "memory": [
            {
                "labels": [{"field": "resource", "value": service_id}],
                "unit": "bytes",
                "values": [{"value": 200_000_000} for _ in range(11)],
            }
            for service_id in [*SERVICES.values(), DATABASE]
        ],
    }


def test_acceptance_needs_more_than_process_health() -> None:
    good = evidence()
    assert not acceptance_failures(good)
    for key, value in (
        ("duration_seconds", 599),
        ("requests", []),
        ("requests", [{"status": 502}]),
        ("requests", [{"status": 200, "error": "ValueError"}]),
        ("memory", []),
        ("database", {"status": "available", "ipAllowList": ["0.0.0.0/0"]}),
    ):
        changed = {**deepcopy(good), key: value}
        assert acceptance_failures(changed)


def test_market_progress_probe_uses_fixed_indexed_window_without_history_scans() -> None:
    from uuid import uuid4

    from sqlalchemy import event, func, select

    from tradeagent.persistence import market_bars, market_quotes, market_trades

    since = datetime(2026, 9, 8, 18, tzinfo=UTC)
    tables = (market_bars, market_quotes, market_trades)
    queries = []

    def insert_row(connection, table, symbol, at):
        row = {
            next(iter(table.primary_key)).name: str(uuid4()),
            "symbol": symbol,
            "feed_source": "iex",
            "event_at": at,
            "received_at": at + timedelta(milliseconds=20),
            "processed_at": at + timedelta(milliseconds=50),
        }
        if table is market_bars:
            row.update(timeframe="1Min", open=1, high=1, low=1, close=1, volume=1)
        elif table is market_quotes:
            row.update(
                bid_price=1, ask_price=2, bid_exchange="V", ask_exchange="V", bid_size=1, ask_size=1
            )
        else:
            row.update(provider_trade_id=str(uuid4()), exchange="V", price=1, size=1, conditions=[])
        connection.execute(table.insert().values(**row))

    with Database("sqlite:///:memory:") as database:
        database.initialize()
        with database.begin() as connection:
            for table in tables:
                insert_row(connection, table, "SPY", since - timedelta(days=5))
                insert_row(connection, table, "AAPL", since + timedelta(seconds=1))
                insert_row(connection, table, "SPY", since + timedelta(seconds=1))
                insert_row(connection, table, "SPY", since + timedelta(days=1))

        @event.listens_for(database.engine, "before_cursor_execute")
        def record(connection, cursor, statement, parameters, context, executemany):
            if statement.startswith("SELECT") and "GROUP BY" in statement:
                queries.append((statement, parameters))

        first = market_window_counts(database, since=since, until=since + timedelta(minutes=1))
        with database.begin() as connection:
            for table in tables:
                insert_row(connection, table, "SPY", since + timedelta(seconds=2))
        later = market_window_counts(database, since=since, until=since + timedelta(minutes=1))
        for table in tables:
            assert first[table.name][0]["count"] == 1
            assert later[table.name][0]["count"] == 2
            assert later[table.name][0]["latest_exchange_at"].replace(tzinfo=UTC) == (
                since + timedelta(seconds=2)
            )
        with database.begin() as connection:
            for statement, parameters in queries:
                plan = str(
                    connection.exec_driver_sql("EXPLAIN QUERY PLAN " + statement, parameters).all()
                )
                assert "SEARCH" in plan and "symbol=?" in plan and "event_at>?" in plan
            assert all(
                connection.scalar(select(func.count()).select_from(table)) == 5 for table in tables
            )


def test_acceptance_rejects_oom_restarts_changed_code_and_high_memory() -> None:
    bad = evidence()
    bad["events"]["dashboard"] = [
        {"event": {"type": "server_failed", "timestamp": "2026-09-08T15:00:01Z"}}
    ]
    assert "dashboard:server_failed_during_observation" in acceptance_failures(bad)
    bad = evidence()
    bad["final_deploys"]["notifier"][0]["deploy"]["commit"]["id"] = "other"
    assert "notifier:deployment_changed" in acceptance_failures(bad)
    bad = evidence()
    bad["memory"][0]["values"][0]["value"] = 450 * 1024 * 1024
    assert "event:memory_above_400_mib_acceptance_bound" in acceptance_failures(bad)


def test_scoped_release_still_checks_every_role_and_actual_event_code() -> None:
    data = evidence()
    data["expected_commits"] = {"recorder": "new-recorder"}
    data["final_deploys"]["recorder"][0]["deploy"]["commit"]["id"] = "new-recorder"
    assert acceptance_failures(data) == []
    data["final_deploys"]["notifier"][0]["deploy"]["commit"]["id"] = "unexpected"
    assert "notifier:deployment_changed" in acceptance_failures(data)
    data["final_deploys"]["notifier"][0]["deploy"]["commit"]["id"] = "abc123"
    data["expected_commits"]["event"] = "new-event"
    data["final_deploys"]["event"][0]["deploy"]["commit"]["id"] = "new-event"
    assert "event:heartbeat_code_mismatch" in acceptance_failures(data)


def test_failed_dashboard_body_is_not_stored() -> None:
    transport = httpx.MockTransport(lambda _: httpx.Response(502, text="<html>" * 30_000))
    with httpx.Client(transport=transport) as client:
        result = dashboard_request(client, "/api/event-product")
    assert result["status"] == 502
    assert "payload" not in result
    assert "html" not in str(result)


def test_http_200_with_stale_worker_or_no_progress_is_not_acceptance() -> None:
    bad = evidence()
    role = bad["requests"][2]["payload"]["operational_status"]["roles"]["tradeagent-event-worker"]
    role["fresh"] = False
    role["reported"]["code_sha"] = "old"
    bad["requests"][-1]["payload"]["market_quotes"] = 10
    failed = acceptance_failures(bad)
    assert "tradeagent-event-worker:stale_or_unhealthy" in failed
    assert "event:heartbeat_code_mismatch" in failed
    assert "recorder:market_quotes_not_advancing" in failed


def test_render_token_override_never_reads_disk(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RENDER_API_KEY", "fixture-not-a-real-credential")
    assert render_token() == "fixture-not-a-real-credential"


def test_release_email_is_explicit_and_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "tradeagent.daily_status.build_daily_status",
        lambda *_: {"subject": "Daily status", "text": "MISSED", "cohort_id": "fixture"},
    )
    with Database("sqlite:///:memory:") as database:
        database.initialize()
        assert email_status(database, "fixture")["status"] == "missing"
        assert not report_test(database, "", email=False).get("notification_id")
        outbox = RoundTripNotificationRepository(database)
        assert outbox.count() == 0
        first = report_test(database, "fixture", email=True)
        second = report_test(database, "fixture", email=True)
        assert first["newly_enqueued"] is True
        assert second["newly_enqueued"] is False
        assert first["notification_id"] == second["notification_id"]
        assert first["notification_id"] != str(
            daily_notification_id(datetime.now(UTC).date(), "America/New_York")
        )
        assert outbox.count() == 1
        message = outbox.claim_next()
        assert message is not None
        assert message.payload["subject"].startswith("[RELEASE TEST")
        assert "MISSED" in message.payload["text"]
