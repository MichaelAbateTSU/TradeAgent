from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest

from infra.render.acceptance_probe import (
    RECORDER_SYMBOLS,
    email_status,
    market_window_counts,
    physical_progress_failures,
    report_test,
)
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


def physical_samples() -> tuple[dict[str, Any], dict[str, Any]]:
    start = datetime(2026, 9, 9, 17, tzinfo=UTC)
    output = []
    for minutes in (2, 5):
        exchange = start + timedelta(minutes=minutes, seconds=-1)
        output.append(
            {
                "market_count_scope": {
                    "since_exchange_at": start.isoformat(),
                    "until_exchange_at": (start + timedelta(minutes=minutes)).isoformat(),
                    "symbols": list(RECORDER_SYMBOLS),
                },
                "market": {
                    table: [
                        {
                            "symbol": symbol,
                            "count": minutes,
                            "latest_exchange_at": exchange.isoformat(),
                            "latest_received_at": exchange + timedelta(milliseconds=30),
                            "latest_committed_at": exchange + timedelta(milliseconds=100),
                        }
                        for symbol in RECORDER_SYMBOLS
                    ]
                    for table in ("market_quotes", "market_trades", "market_bars")
                },
            }
        )
    return output[0], output[1]


def test_physical_progress_requires_each_symbol_not_just_aggregate_flow() -> None:
    first, last = physical_samples()
    assert physical_progress_failures(first, last) == []
    for sample in (first, last):
        sample["market"]["market_trades"] = [
            row for row in sample["market"]["market_trades"] if row["symbol"] != "QQQ"
        ]
    assert physical_progress_failures(first, last) == ["physical:market_trades:QQQ:not_advancing"]
    first, last = physical_samples()
    first["market"]["market_trades"] = []
    assert physical_progress_failures(first, last) == []
    last["market"]["market_bars"][0] = deepcopy(physical_samples()[0]["market"]["market_bars"][0])
    assert "physical:market_bars:SPY:not_advancing" in physical_progress_failures(first, last)


@pytest.mark.parametrize(
    "mutation", ["window", "interval", "naive", "count", "duplicate", "receipt", "processing"]
)
def test_physical_progress_rejects_invalid_evidence(mutation: str) -> None:
    first, last = physical_samples()
    if mutation == "window":
        last["market_count_scope"]["since_exchange_at"] = "2026-09-09T17:01:00Z"
    elif mutation == "interval":
        last["market_count_scope"]["until_exchange_at"] = "2026-09-09T17:27:00Z"
    elif mutation == "naive":
        last["market"]["market_quotes"][0]["latest_exchange_at"] = "2026-09-09T17:04:59"
    elif mutation == "count":
        last["market"]["market_quotes"][0]["count"] = True
    elif mutation == "duplicate":
        last["market"]["market_quotes"].append(deepcopy(last["market"]["market_quotes"][0]))
    elif mutation == "receipt":
        last["market"]["market_quotes"][0]["latest_received_at"] = "2026-09-09T16:00:00Z"
    else:
        last["market"]["market_quotes"][0]["latest_received_at"] = "2026-09-09T17:04:59.500Z"
        last["market"]["market_quotes"][0]["latest_committed_at"] = "2026-09-09T17:04:59.100Z"
    assert physical_progress_failures(first, last)


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


def test_upgraded_database_profile_requires_actual_plan_and_unchanged_safety_gates() -> None:
    data = evidence()
    data.update(database_profile="pg-1gb-v1", duration_seconds=1800)
    data["database"]["plan"] = "0.5c-1g"
    data["final_database"] = deepcopy(data["database"])
    database_series = data["memory"][-1]
    database_series["values"] = [{"value": 799 * 1024 * 1024} for _ in range(30)]
    assert acceptance_failures(data) == []
    database_series["values"][0]["value"] = 800 * 1024 * 1024
    assert "database:missing_memory_or_above_800_mib_acceptance_bound" in acceptance_failures(data)
    database_series["values"][0]["value"] = 799 * 1024 * 1024
    for phase in ("database", "final_database"):
        wrong = deepcopy(data)
        wrong[phase]["plan"] = "0.1c-256mb"
        assert any(
            "capacity_or_availability_mismatch" in item for item in acceptance_failures(wrong)
        )
    short = {**data, "duration_seconds": 1799}
    assert "database:upgraded_capacity_requires_thirty_minutes" in acceptance_failures(short)
    missing_final = deepcopy(data)
    del missing_final["final_database"]
    assert "database:final_capacity_or_availability_mismatch" in acceptance_failures(missing_final)
    dropped = deepcopy(data)
    dropped["requests"][0]["payload"]["operational_status"]["roles"]["tradeagent-shadow-recorder"][
        "reported"
    ]["dropped_events"] = 1
    assert "recorder:unhealthy_or_dropped_packets" in acceptance_failures(dropped)
    assert "database:unknown_capacity_profile" in acceptance_failures(
        {**data, "database_profile": "unreviewed"}
    )


def test_legacy_database_memory_bound_is_not_reinterpreted() -> None:
    data = evidence()
    data["memory"][-1]["values"][0]["value"] = 231 * 1024 * 1024
    assert "database:missing_memory_or_above_230_mib_acceptance_bound" in acceptance_failures(data)
    data["database"]["plan"] = "0.5c-1g"
    assert "database:missing_memory_or_above_230_mib_acceptance_bound" in acceptance_failures(data)


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
