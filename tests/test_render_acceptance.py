from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest

from infra.render.acceptance_probe import email_status, report_test
from infra.render.observe_release import (
    SERVICES,
    acceptance_failures,
    dashboard_request,
    render_token,
)
from tradeagent.daily_status import daily_notification_id
from tradeagent.notifications import RoundTripNotificationRepository
from tradeagent.persistence import Database


def evidence() -> dict[str, Any]:
    return {
        "started_at": "2026-09-08T15:00:00+00:00",
        "duration_seconds": 660,
        "expected_commit": "abc123",
        "requests": [{"status": 200}],
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
            for service_id in SERVICES.values()
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


def test_failed_dashboard_body_is_not_stored() -> None:
    transport = httpx.MockTransport(lambda _: httpx.Response(502, text="<html>" * 30_000))
    with httpx.Client(transport=transport) as client:
        result = dashboard_request(client, "/api/event-product")
    assert result["status"] == 502
    assert "payload" not in result
    assert "html" not in str(result)


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
