from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace
from typing import Any

import pytest

from infra.render.agent_health import assess_agent_health, collect_agent_evidence, user_render_token
from infra.render.observe_release import SERVICES


def _evidence() -> dict[str, Any]:
    code = "a" * 40
    roles = {}
    hosting = {}
    for role, service_id in SERVICES.items():
        hosting[role] = {
            "service": {
                "id": service_id,
                "suspended": "suspended" if role == "recorder" else "not_suspended",
            },
            "deploy": {"status": "live", "commit": {"id": code}},
        }
    for role in ("event-worker", "notifier"):
        roles[f"tradeagent-{role}"] = {
            "fresh": True,
            "instance_id": "current-owner",
            "age_seconds": 2,
            "reported": {
                "state": "paused_invalid" if role == "event-worker" else "running",
                "code_sha": code,
            },
            "lease": {
                "present": True,
                "owner_matches_heartbeat": True,
                "recent_within_120_seconds": True,
            },
        }
    roles["tradeagent-shadow-recorder"] = {"fresh": False, "reported": {"state": "healthy"}}
    return {
        "observed_at": "2026-10-08T05:00:00+00:00",
        "hosting": hosting,
        "health": {"status": "ok"},
        "ready": {
            "ready": True,
            "database": "reachable",
            "operational_status": {"roles": roles},
        },
        "scalping": {
            "state": "paused_invalid",
            "entry_policy": "shadow-research-dataset-v1",
            "trading_authorization": "expired",
            "model_state": "no_support",
            "orders_submitted": 0,
            "economic_entries_enabled": False,
            "owner_id": "current-owner",
            "feed": {"state": "stopped", "authenticated": False, "subscribed": False},
        },
    }


def test_intentionally_suspended_and_preserved_pause_are_not_process_crashes() -> None:
    result = assess_agent_health(_evidence())
    assert result["operational_findings"] == []
    assert result["roles"]["recorder"]["state"] == "hosting_suspended_not_expected_running"
    assert result["roles"]["event"]["state"] == "process_healthy_preserved_failed_study_pause"
    assert not result["market_collection_ready"]
    assert not result["orders_authorized"]
    assert not result["model_profitability_validated"]


@pytest.mark.parametrize(
    "field", ["present", "owner_matches_heartbeat", "recent_within_120_seconds"]
)
def test_active_agent_requires_actual_owned_fresh_lease(field: str) -> None:
    evidence = _evidence()
    evidence["ready"]["operational_status"]["roles"]["tradeagent-notifier"]["lease"][field] = False
    result = assess_agent_health(evidence)
    assert "notifier:runtime_unavailable_or_unowned" in result["operational_findings"]


def test_suspension_must_come_from_hosting_not_stale_heartbeat() -> None:
    evidence = _evidence()
    evidence["hosting"]["recorder"]["service"]["suspended"] = "not_suspended"
    result = assess_agent_health(evidence)
    assert "recorder:runtime_unavailable_or_unowned" in result["operational_findings"]


@pytest.mark.parametrize(
    ("field", "value"),
    [("model_state", "validated"), ("trading_authorization", "active"), ("orders_submitted", 1)],
)
def test_preserved_pause_needs_explicit_safety_proof(field: str, value: object) -> None:
    evidence = _evidence()
    evidence["scalping"][field] = value
    assert (
        "event:paused_state_safety_proof_incomplete"
        in assess_agent_health(evidence)["operational_findings"]
    )


def test_unknown_hosting_and_wrong_identity_are_not_defaulted_healthy() -> None:
    evidence = _evidence()
    evidence["hosting"]["recorder"]["service"]["suspended"] = None
    assert (
        "recorder:hosting_suspension_unknown"
        in assess_agent_health(evidence)["operational_findings"]
    )
    changed = deepcopy(evidence)
    changed["hosting"]["event"]["service"]["id"] = "different-service"
    with pytest.raises(ValueError, match="identity"):
        assess_agent_health(changed)


def test_health_collector_uses_only_get_and_omits_hosting_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evidence = _evidence()
    requests = []

    class Response:
        status_code = 200

        def __init__(self, payload: object):
            self.payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self.payload

    class Client:
        def __init__(self, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def get(self, url, **kwargs):
            requests.append(url)
            for role, service_id in SERVICES.items():
                if url.endswith(f"services/{service_id}"):
                    return Response(
                        {**evidence["hosting"][role]["service"], "envVars": "must-not-export"}
                    )
                if url.endswith(f"services/{service_id}/deploys"):
                    return Response([{"deploy": evidence["hosting"][role]["deploy"]}])
            for name, path in (
                ("health", "/health"),
                ("ready", "/ready"),
                ("scalping", "/api/scalping"),
            ):
                if url.endswith(path):
                    return Response(evidence[name])
            raise AssertionError(url)

    monkeypatch.setattr("infra.render.agent_health.httpx.Client", Client)
    collected = collect_agent_evidence("fixture-only")
    assert len(requests) == 11
    assert "must-not-export" not in str(collected)
    assert not assess_agent_health(collected)["operational_findings"]


def test_user_credential_scope_is_explicit_and_does_not_use_stale_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import sys

    class Key:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

    monkeypatch.setattr("infra.render.agent_health.sys.platform", "win32")
    monkeypatch.setenv("RENDER_API_KEY", "stale-process-fixture")
    monkeypatch.setitem(
        sys.modules,
        "winreg",
        SimpleNamespace(
            HKEY_CURRENT_USER=object(),
            OpenKey=lambda *args: Key(),
            QueryValueEx=lambda *args: (" user-scoped-fixture ", 1),
        ),
    )
    assert user_render_token() == "user-scoped-fixture"


def test_user_credential_scope_has_no_silent_non_windows_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("infra.render.agent_health.sys.platform", "linux")
    with pytest.raises(RuntimeError, match="Windows"):
        user_render_token()
