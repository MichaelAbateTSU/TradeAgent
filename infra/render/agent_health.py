"""Read-only hosting and runtime diagnosis; never restarts a paused experiment."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from infra.render.observe_release import BASE_URL, SERVICES, api_get, render_token

ROLE_NAMES = {
    "event": "tradeagent-event-worker",
    "recorder": "tradeagent-shadow-recorder",
    "notifier": "tradeagent-notifier",
}


def user_render_token() -> str:
    if sys.platform != "win32":
        raise RuntimeError("User-scoped Render credentials require Windows; use process scope")
    import winreg

    with winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Environment") as environment:
        value, _ = winreg.QueryValueEx(environment, "RENDER_API_KEY")
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError("User-scoped Render credential is unavailable")
    return value.strip()


def assess_agent_health(evidence: dict[str, Any]) -> dict[str, Any]:
    hosting = evidence["hosting"]
    ready = evidence["ready"]
    snapshot = evidence["scalping"]
    operational = ready.get("operational_status", {})
    observations = operational.get("roles", {})
    findings: list[str] = []
    roles: dict[str, Any] = {}
    for role, service_id in SERVICES.items():
        item = hosting[role]
        service = item["service"]
        deploy = item["deploy"]
        if service.get("id") != service_id:
            raise ValueError("hosting service identity does not match its role")
        suspended = service.get("suspended") == "suspended"
        if service.get("suspended") not in ("suspended", "not_suspended"):
            findings.append(f"{role}:hosting_suspension_unknown")
        observed = observations.get(ROLE_NAMES.get(role, ""), {})
        lease = observed.get("lease") or {}
        fresh_owned = (
            observed.get("fresh") is True
            and lease.get("present") is True
            and lease.get("owner_matches_heartbeat") is True
            and lease.get("recent_within_120_seconds") is True
        )
        if role == "dashboard":
            process = (
                evidence["health"].get("status") == "ok"
                and ready.get("ready") is True
                and ready.get("database") == "reachable"
            )
        else:
            process = fresh_owned
        if suspended:
            state = "hosting_suspended_not_expected_running"
        elif deploy.get("status") != "live":
            state = "deployment_not_live"
            findings.append(f"{role}:deployment_not_live")
        elif not process:
            state = "runtime_unavailable_or_unowned"
            findings.append(f"{role}:runtime_unavailable_or_unowned")
        else:
            state = "process_healthy"
        if role == "event" and not suspended and process:
            reported = observed.get("reported", {})
            if reported.get("code_sha") != deploy.get("commit", {}).get("id"):
                findings.append("event:runtime_deployment_source_mismatch")
            preserved_pause = (
                snapshot.get("state") == "paused_invalid"
                and snapshot.get("entry_policy") == "shadow-research-dataset-v1"
                and snapshot.get("trading_authorization") == "expired"
                and snapshot.get("model_state") == "no_support"
                and snapshot.get("orders_submitted") == 0
                and snapshot.get("economic_entries_enabled") is False
                and snapshot.get("feed", {}).get("state") == "stopped"
                and snapshot.get("feed", {}).get("subscribed") is False
                and snapshot.get("feed", {}).get("authenticated") is False
            )
            if preserved_pause:
                state = "process_healthy_preserved_failed_study_pause"
            elif snapshot.get("state") == "paused_invalid":
                findings.append("event:paused_state_safety_proof_incomplete")
            if snapshot.get("owner_id") != observed.get("instance_id"):
                findings.append("event:runtime_snapshot_owner_mismatch")
        roles[role] = {
            "service_id": service_id,
            "state": state,
            "hosting_suspended": suspended,
            "deployment_status": deploy.get("status"),
            "deployment_commit": deploy.get("commit", {}).get("id"),
            "process_owned_and_fresh": process,
            "reported_state": observed.get("reported", {}).get("state"),
            "heartbeat_age_seconds": observed.get("age_seconds"),
            "automatic_restart_performed": False,
        }
    if ready.get("ready") is not True or ready.get("database") != "reachable":
        findings.append("dashboard:read_dependencies_unavailable")
    return {
        "schema": "render-agent-health-v1",
        "observed_at": evidence["observed_at"],
        "read_only": True,
        "roles": roles,
        "operational_findings": sorted(set(findings)),
        "operations_healthy_for_observed_hosting_configuration": not findings,
        "market_collection_ready": False,
        "model_profitability_validated": False,
        "orders_authorized": False,
        "actions_performed": ["GET"],
        "interpretation": (
            "Suspended hosting and preserved failed-study pauses are not process crashes. "
            "Fresh leases prove process ownership, not quote coverage or trading permission. "
            "Missing evidence is unavailable, never inferred healthy."
        ),
    }


def collect_agent_evidence(token: str) -> dict[str, Any]:
    with (
        httpx.Client(headers={"Authorization": f"Bearer {token}"}, timeout=60) as render,
        httpx.Client(timeout=30) as web,
    ):
        hosting = {}
        for role, service_id in SERVICES.items():
            service = api_get(render, f"services/{service_id}")
            deploys = api_get(render, f"services/{service_id}/deploys", params={"limit": 1})
            if not deploys:
                raise ValueError(f"{role}: no hosting deployment evidence")
            hosting[role] = {
                "service": {
                    key: service.get(key) for key in ("id", "name", "suspended", "autoDeploy")
                },
                "deploy": {
                    key: deploys[0]["deploy"].get(key)
                    for key in ("id", "status", "commit", "finishedAt")
                },
            }
        endpoints = {}
        for name, path in (
            ("health", "/health"),
            ("ready", "/ready"),
            ("scalping", "/api/scalping"),
        ):
            response = web.get(BASE_URL + path)
            response.raise_for_status()
            endpoints[name] = response.json()
    return {"observed_at": datetime.now(UTC).isoformat(), "hosting": hosting, **endpoints}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--credential-scope",
        choices=("process", "user"),
        default="process",
        help="Windows user scope avoids a stale process token without changing credentials",
    )
    args = parser.parse_args()
    token = user_render_token() if args.credential_scope == "user" else render_token()
    evidence = collect_agent_evidence(token)
    assessment = assess_agent_health(evidence)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps({"assessment": assessment, "evidence": evidence}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(assessment, sort_keys=True))
    if assessment["operational_findings"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
