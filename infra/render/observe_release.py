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
PATHS = (
    "/health",
    "/ready",
    "/api/status",
    "/api/runtime",
    "/api/news?limit=20",
    "/api/experiments?limit=10",
    "/api/event-product",
)


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
        if deploy["status"] != "live" or deploy["commit"]["id"] != evidence["expected_commit"]:
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
    return failures


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--seconds", type=int, default=660)
    parser.add_argument("--interval", type=int, default=10)
    args = parser.parse_args()
    if args.seconds < 600 or args.interval < 1:
        parser.error("acceptance requires at least 600 seconds and a positive interval")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    start = datetime.now(UTC)
    evidence: dict[str, Any] = {
        "started_at": start.isoformat(),
        "expected_commit": args.commit,
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
            if deploy["status"] != "live" or deploy["commit"]["id"] != args.commit:
                raise RuntimeError(f"{role}: expected pinned live deploy before acceptance")
        evidence["database"] = api_get(render, "postgres/" + DATABASE)
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
                ("startTime", start.isoformat().replace("+00:00", "Z")),
                ("endTime", end.isoformat().replace("+00:00", "Z")),
                ("resolutionSeconds", "60"),
            ],
        )
        evidence["final_deploys"] = {
            role: api_get(render, f"services/{service_id}/deploys", params={"limit": 1})
            for role, service_id in SERVICES.items()
        }
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
