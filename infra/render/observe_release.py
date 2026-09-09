"""Observe the existing Render release; output is evidence, not entry permission."""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

SERVICES = {
    "event": "srv-dae4tr7qj5pc73a9e0k0",
    "recorder": "srv-dadn8son74is73apqcc0",
    "notifier": "srv-dadnn6mq1p3s73ef7ef0",
    "dashboard": "srv-dadoa3740ujc73cb09c0",
}
DATABASE = "dpg-dadn7nht0dsc73f9jja0-a"
BASE_URL = "https://tradeagent-runtime-dashboard.onrender.com"
DATABASE_PROFILES = {
    "pg-256mb-v1": {"plan": "0.1c-256mb", "maximum_memory_mib": 230},
    "pg-1gb-v1": {"plan": "0.5c-1g", "maximum_memory_mib": 800},
}
PATHS = (
    "/health",
    "/ready",
    "/api/status",
    "/api/runtime",
    "/api/news?limit=20",
    "/api/experiments?limit=10",
    "/api/event-product",
)
ROLE_STATES = {
    "tradeagent-event-worker": {"collecting", "market_closed"},
    "tradeagent-shadow-recorder": {"healthy"},
    "tradeagent-notifier": {"running"},
    "tradeagent-news-worker": {"healthy"},
    "tradeagent-shadow-market-feed": {"healthy"},
}


def render_token() -> str:
    token = os.environ.get("RENDER_API_KEY")
    if token:
        return token
    directory = Path(os.environ.get("RENDER_CLI_CONFIG_DIR", str(Path.home() / ".render")))
    match = re.search(r"(?m)^\s+(?:key|token):\s*(.+)\s*$", (directory / "cli.yaml").read_text())
    if match is None:
        raise RuntimeError("Authenticated Render CLI token not found")
    return match.group(1).strip().strip("'\"")


def api_get(client: httpx.Client, path: str, **kwargs: Any) -> Any:
    for attempt in range(4):
        response = client.get("https://api.render.com/v1/" + path, **kwargs)
        if response.status_code in {429, 502, 503, 504} and attempt < 3:
            time.sleep(3 * (attempt + 1))
            continue
        response.raise_for_status()
        return response.json()
    raise AssertionError("unreachable")


def dashboard_request(client: httpx.Client, path: str) -> dict[str, Any]:
    started = time.monotonic()
    result: dict[str, Any] = {"path": path, "observed_at": datetime.now(UTC).isoformat()}
    try:
        response = client.get(BASE_URL + path)
        result.update(status=response.status_code, bytes=len(response.content))
        if response.status_code == 200:
            payload = response.json()
            if path in {"/ready", "/api/runtime", "/api/event-product", "/api/status"}:
                result["payload"] = payload
    except (httpx.HTTPError, ValueError) as error:
        result["error"] = type(error).__name__
    result["seconds"] = round(time.monotonic() - started, 3)
    return result


def acceptance_failures(evidence: dict[str, Any]) -> list[str]:
    failures = []
    start = datetime.fromisoformat(evidence["started_at"])
    if evidence["duration_seconds"] < 600:
        failures.append("observation_shorter_than_ten_minutes")
    if not evidence["requests"] or any(
        row.get("status") != 200 or "error" in row for row in evidence["requests"]
    ):
        failures.append("dashboard_request_failed")
    database = evidence["database"]
    if database.get("status") != "available" or database.get("ipAllowList") != []:
        failures.append("database_unavailable_or_public_allowlist_changed")
    for role, service_id in SERVICES.items():
        deploy = evidence["final_deploys"][role][0]["deploy"]
        expected = evidence.get("expected_commits", {}).get(role, evidence["expected_commit"])
        if deploy["status"] != "live" or deploy["commit"]["id"] != expected:
            failures.append(f"{role}:deployment_changed")
        for row in evidence["events"][role]:
            event = row["event"]
            if datetime.fromisoformat(event["timestamp"]) >= start and event["type"] in {
                "server_failed",
                "server_available",
            }:
                failures.append(f"{role}:{event['type']}_during_observation")
        series = [
            item
            for item in evidence["memory"]
            if any(
                label["field"] == "resource" and label["value"] == service_id
                for label in item["labels"]
            )
        ]
        values = [point["value"] for item in series for point in item["values"]]
        if len(series) != 1 or len(values) < 9:
            failures.append(f"{role}:missing_memory_or_instance_changed")
        if any(item["unit"] != "bytes" for item in series):
            failures.append(f"{role}:unexpected_memory_unit")
        if values and max(values) >= 400 * 1024 * 1024:
            failures.append(f"{role}:memory_above_400_mib_acceptance_bound")
    profile_name = evidence.get("database_profile", "pg-256mb-v1")
    if profile_name not in DATABASE_PROFILES:
        failures.append("database:unknown_capacity_profile")
    profile = DATABASE_PROFILES.get(profile_name, DATABASE_PROFILES["pg-256mb-v1"])
    maximum_memory_mib = profile["maximum_memory_mib"]
    if "database_profile" in evidence:
        for phase, actual in (
            ("initial", database),
            ("final", evidence.get("final_database", {})),
        ):
            if (
                actual.get("plan") != profile["plan"]
                or actual.get("status") != "available"
                or actual.get("ipAllowList") != []
            ):
                failures.append(f"database:{phase}_capacity_or_availability_mismatch")
        if profile_name == "pg-1gb-v1" and evidence["duration_seconds"] < 1800:
            failures.append("database:upgraded_capacity_requires_thirty_minutes")
    database_memory = [
        point["value"]
        for item in evidence["memory"]
        if any(
            label["field"] == "resource" and label["value"] == DATABASE for label in item["labels"]
        )
        for point in item["values"]
    ]
    if (
        len(database_memory) < 9
        or max(database_memory, default=0) >= maximum_memory_mib * 1024 * 1024
    ):
        failures.append(
            f"database:missing_memory_or_above_{maximum_memory_mib}_mib_acceptance_bound"
        )
    failures.extend(dependency_failures(evidence))
    return sorted(set(failures))


def dependency_failures(evidence: dict[str, Any]) -> list[str]:
    failures = []
    snapshots = [
        row.get("payload", {}).get("operational_status", {})
        for row in evidence["requests"]
        if row.get("path") == "/ready" and row.get("status") == 200
    ]
    if len(snapshots) < 2:
        failures.append("missing_sustained_dependency_snapshots")
    owners: dict[str, set[str]] = {name: set() for name in ROLE_STATES}
    recorder = []
    for snapshot in snapshots:
        for name, allowed in ROLE_STATES.items():
            role = snapshot.get("roles", {}).get(name, {})
            reported = role.get("reported", {})
            if not role.get("fresh") or reported.get("state") not in allowed:
                failures.append(f"{name}:stale_or_unhealthy")
            if role.get("instance_id"):
                owners[name].add(role["instance_id"])
            lease = role.get("lease")
            requires_lease = name in {
                "tradeagent-event-worker",
                "tradeagent-shadow-recorder",
                "tradeagent-notifier",
            }
            if requires_lease and (
                not lease
                or not lease.get("present")
                or not lease.get("owner_matches_heartbeat")
                or not lease.get("recent_within_120_seconds")
            ):
                failures.append(f"{name}:invalid_lease_observation")
            if name == "tradeagent-event-worker" and reported.get("code_sha") != evidence.get(
                "expected_commits", {}
            ).get("event", evidence["expected_commit"]):
                failures.append("event:heartbeat_code_mismatch")
            if name == "tradeagent-shadow-recorder":
                recorder.append(reported)
                if reported.get("healthy") is not True or reported.get("dropped_events", 0):
                    failures.append("recorder:unhealthy_or_dropped_packets")
    for name, observed in owners.items():
        if len(observed) != 1:
            failures.append(f"{name}:owner_missing_or_changed")
    if recorder and recorder[-1].get("gaps", 0) != recorder[0].get("gaps", 0):
        failures.append("recorder:new_coverage_gap")
    try:
        committed_times = [
            datetime.fromisoformat(row["last_committed_event_at"]) for row in recorder
        ]
        advanced = len(committed_times) >= 2 and committed_times[-1] > committed_times[0]
    except (KeyError, TypeError, ValueError):
        advanced = False
    if not advanced:
        failures.append("recorder:committed_exchange_time_not_advancing")
    runtime = [
        row["payload"]
        for row in evidence["requests"]
        if row.get("path") == "/api/runtime" and row.get("status") == 200 and "payload" in row
    ]
    for field in ("market_quotes", "market_trades", "market_bars"):
        values = [row.get(field) for row in runtime]
        if (
            len(values) < 2
            or not all(isinstance(value, int) for value in values)
            or values[-1] <= values[0]
        ):
            failures.append(f"recorder:{field}_not_advancing")
    return failures


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--commit", required=True)
    parser.add_argument(
        "--role-commit",
        action="append",
        default=[],
        help="Exact ROLE=SHA override for a scoped rollout",
    )
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--seconds", type=int, default=660)
    parser.add_argument("--interval", type=int, default=10)
    parser.add_argument(
        "--database-profile",
        choices=tuple(DATABASE_PROFILES),
        default="pg-256mb-v1",
        help="Explicit reviewed compute profile; never applied to historical evidence",
    )
    args = parser.parse_args()
    if args.seconds < 600 or args.interval < 1:
        parser.error("acceptance requires at least 600 seconds and a positive interval")
    if args.database_profile == "pg-1gb-v1" and args.seconds < 1800:
        parser.error("the upgraded database profile requires at least 1800 seconds")
    expected_commits = {role: args.commit for role in SERVICES}
    for override in args.role_commit:
        role, separator, commit = override.partition("=")
        if not separator or role not in SERVICES or not re.fullmatch(r"[0-9a-fA-F]{40}", commit):
            parser.error("--role-commit requires a known role and full 40-character SHA")
        expected_commits[role] = commit
    args.output.parent.mkdir(parents=True, exist_ok=True)
    start = datetime.now(UTC)
    evidence: dict[str, Any] = {
        "started_at": start.isoformat(),
        "expected_commit": args.commit,
        "expected_commits": expected_commits,
        "database_profile": args.database_profile,
        "requests": [],
        "services": {},
        "limitation": (
            "Readiness also requires same-service database/broker/progress probes and review."
        ),
    }
    with (
        httpx.Client(headers={"Authorization": f"Bearer {render_token()}"}, timeout=60) as render,
        httpx.Client(timeout=30) as web,
        ThreadPoolExecutor(max_workers=6) as executor,
    ):
        for role, service_id in SERVICES.items():
            service = api_get(render, "services/" + service_id)
            deploys = api_get(render, f"services/{service_id}/deploys", params={"limit": 1})
            evidence["services"][role] = {"service": service, "deploys": deploys}
            deploy = deploys[0]["deploy"]
            if deploy["status"] != "live" or deploy["commit"]["id"] != expected_commits[role]:
                raise RuntimeError(f"{role}: expected pinned live deploy before acceptance")
        evidence["database"] = api_get(render, "postgres/" + DATABASE)
        if evidence["database"].get("plan") != DATABASE_PROFILES[args.database_profile]["plan"]:
            raise RuntimeError("Actual database plan does not match the requested capacity profile")
        deadline = time.monotonic() + args.seconds
        while True:
            # Two simultaneous page-equivalent clients test server-side coalescing.
            batch = list(executor.map(lambda path: dashboard_request(web, path), PATHS * 2))
            evidence["requests"].extend(batch)
            args.output.write_text(json.dumps(evidence, indent=2, default=str) + "\n")
            failures = sum(row.get("status") != 200 for row in batch)
            print(
                f"{datetime.now(UTC).isoformat()} requests={len(batch)} failures={failures} "
                f"max_seconds={max(row['seconds'] for row in batch)}",
                flush=True,
            )
            if time.monotonic() >= deadline:
                break
            time.sleep(min(args.interval, max(0, deadline - time.monotonic())))
        end = datetime.now(UTC)
        evidence["finished_at"] = end.isoformat()
        evidence["duration_seconds"] = (end - start).total_seconds()
        evidence["events"] = {
            role: api_get(render, f"services/{service_id}/events", params={"limit": 100})
            for role, service_id in SERVICES.items()
        }
        evidence["memory"] = api_get(
            render,
            "metrics/memory",
            params=[
                *(("resource", service_id) for service_id in SERVICES.values()),
                ("resource", DATABASE),
                ("startTime", start.isoformat().replace("+00:00", "Z")),
                ("endTime", end.isoformat().replace("+00:00", "Z")),
                ("resolutionSeconds", "60"),
            ],
        )
        evidence["final_deploys"] = {
            role: api_get(render, f"services/{service_id}/deploys", params={"limit": 1})
            for role, service_id in SERVICES.items()
        }
        evidence["final_database"] = api_get(render, "postgres/" + DATABASE)
    evidence["http_failures"] = sum(row.get("status") != 200 for row in evidence["requests"])
    evidence["acceptance_failures"] = acceptance_failures(evidence)
    args.output.write_text(json.dumps(evidence, indent=2, default=str) + "\n")
    print(
        json.dumps(
            {key: evidence[key] for key in ("duration_seconds", "http_failures", "finished_at")}
        )
    )
    if evidence["acceptance_failures"]:
        print(json.dumps(evidence["acceptance_failures"]))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
