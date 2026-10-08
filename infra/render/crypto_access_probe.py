"""Bounded data-access discovery, never a label-quality or execution-venue test."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from infra.render.observe_release import BASE_URL
from tradeagent.alpaca_paper import AlpacaPaperSettings

LOCATIONS = ("us-1", "eu-1", "us")
ACCOUNT_DIGEST = "b3ddf697092ad516c7e68024af06916f7c5dc921c8dfd44ff35ea6618b44947f"
SOURCE_SHA = "94e80ec1e75390a284bb0dbe89f38977a3a5dd1b"
OWNER = "srv-dae4tr7qj5pc73a9e0k0-6467cc5744-4txxf"
CONNECTION = "1bb1b7ef-06e8-4c44-9d0b-086ce2298122"


def require_stopped_guard(snapshot: dict[str, Any]) -> tuple[Any, ...]:
    feed = snapshot.get("feed") or {}
    if not (
        snapshot.get("state") == "paused_invalid"
        and snapshot.get("entry_policy") == "shadow-research-dataset-v1"
        and snapshot.get("trading_authorization") == "expired"
        and snapshot.get("model_state") == "no_support"
        and snapshot.get("orders_submitted") == 0
        and snapshot.get("economic_entries_enabled") is False
        and feed.get("state") == "stopped"
        and feed.get("authenticated") is False
        and feed.get("subscribed") is False
        and feed.get("connection_id") == CONNECTION
        and snapshot.get("owner_id") == OWNER
        and snapshot.get("account_digest") == ACCOUNT_DIGEST
        and snapshot.get("code_sha") == SOURCE_SHA
        and snapshot.get("active") is True
        and isinstance(snapshot.get("config_hash"), str)
        and bool(snapshot["config_hash"])
    ):
        raise ValueError("Current stopped-study safety proof is incomplete")
    return (
        snapshot["owner_id"],
        snapshot["code_sha"],
        snapshot["config_hash"],
        snapshot.get("dataset_id"),
        feed["connection_id"],
    )


def current_guard() -> tuple[Any, ...]:
    response = httpx.get(BASE_URL + "/api/scalping", timeout=30)
    response.raise_for_status()
    return require_stopped_guard(response.json())


async def probe_location(settings: AlpacaPaperSettings, location: str) -> dict[str, Any]:
    if location not in LOCATIONS:
        raise ValueError("Only documented provider locations can be probed")
    endpoint = f"wss://stream.data.alpaca.markets/v1beta3/crypto/{location}"
    started = time.monotonic()
    result: dict[str, Any] = {
        "location": location,
        "endpoint": endpoint,
        "started_at": datetime.now(UTC).isoformat(),
        "authenticated": False,
        "subscription_acknowledged": False,
        "quote_counts": {"BTC/USD": 0, "ETH/USD": 0},
        "provider_errors": [],
        "frames": [],
        "status": "pending",
    }
    try:
        async with connect(
            endpoint, open_timeout=5, close_timeout=2, max_size=1024 * 1024
        ) as socket:
            deadline = time.monotonic() + 8
            charged = 0
            sent_auth = False
            while time.monotonic() < deadline:
                try:
                    raw = await asyncio.wait_for(socket.recv(), deadline - time.monotonic())
                except TimeoutError:
                    break
                encoded = raw.encode() if isinstance(raw, str) else raw
                charged += len(encoded)
                if charged > 1024 * 1024 or len(result["frames"]) >= 1024:
                    result["status"] = "bounded_capture_limit"
                    break
                messages = json.loads(raw)
                if not isinstance(messages, list) or any(
                    not isinstance(item, dict) for item in messages
                ):
                    raise ValueError("Provider message must be a list of objects")
                result["frames"].append(
                    {
                        "received_at": datetime.now(UTC).isoformat(),
                        "sha256": hashlib.sha256(encoded).hexdigest(),
                        "raw": raw if isinstance(raw, str) else raw.decode("utf-8"),
                    }
                )
                for message in messages:
                    kind = message.get("T")
                    if kind == "error":
                        result["provider_errors"].append(
                            {"code": message.get("code"), "message": message.get("msg")}
                        )
                        result["status"] = "provider_rejected"
                        break
                    if (
                        kind == "success"
                        and message.get("msg") == "connected"
                        and not sent_auth
                    ):
                        await socket.send(
                            json.dumps(
                                {
                                    "action": "auth",
                                    "key": settings.key_id.get_secret_value(),
                                    "secret": settings.secret_key.get_secret_value(),
                                }
                            )
                        )
                        sent_auth = True
                    if (
                        kind == "success"
                        and message.get("msg") == "authenticated"
                        and sent_auth
                        and not result["authenticated"]
                    ):
                        result["authenticated"] = True
                        await socket.send(
                            json.dumps(
                                {"action": "subscribe", "quotes": ["BTC/USD", "ETH/USD"]}
                            )
                        )
                    if kind == "subscription":
                        result["subscription_acknowledged"] = (
                            result["authenticated"]
                            and set(message.get("quotes") or []) >= {"BTC/USD", "ETH/USD"}
                        )
                    if kind == "q" and message.get("S") in result["quote_counts"]:
                        result["quote_counts"][message["S"]] += 1
                if result["provider_errors"]:
                    break
            if result["status"] == "pending":
                result["status"] = (
                    "access_observed"
                    if result["authenticated"] and result["subscription_acknowledged"]
                    else "authentication_or_subscription_not_established"
                )
    except (OSError, TimeoutError, WebSocketException) as error:
        result["status"] = "transport_failure"
        result["transport_error_type"] = type(error).__name__
    result["elapsed_seconds"] = time.monotonic() - started
    return result


def verify_paper_identity(settings: AlpacaPaperSettings) -> None:
    with httpx.Client(
        base_url=settings.paper_url,
        headers={
            "APCA-API-KEY-ID": settings.key_id.get_secret_value(),
            "APCA-API-SECRET-KEY": settings.secret_key.get_secret_value(),
        },
        timeout=30,
    ) as client:
        account = client.get("/v2/account")
        account.raise_for_status()
        digest = hashlib.sha256(str(account.json()["id"]).encode()).hexdigest()
        if digest != ACCOUNT_DIGEST:
            raise ValueError("Paper account identity differs from the frozen account")
        for path in ("/v2/positions", "/v2/orders?status=open&limit=500"):
            response = client.get(path)
            response.raise_for_status()
            if response.json() != []:
                raise ValueError("Paper account is not empty; access probe stopped")


async def collect_access(settings: AlpacaPaperSettings) -> dict[str, Any]:
    verify_paper_identity(settings)
    guard = current_guard()
    locations = []
    for location in LOCATIONS:
        if current_guard() != guard:
            raise ValueError("Stopped-study identity changed before a diagnostic connection")
        locations.append(await probe_location(settings, location))
        if current_guard() != guard:
            raise ValueError("Stopped-study identity changed during access discovery")
    verify_paper_identity(settings)
    return {
        "schema": "alpaca-crypto-bounded-access-v1",
        "finished_at": datetime.now(UTC).isoformat(),
        "locations": locations,
        "connections": "Sequential; only diagnostic sockets are opened and closed",
        "account_identity_matches": True,
        "stopped_guard_unchanged": True,
        "orders_submitted": 0,
        "study_started": False,
        "coverage_calculated": False,
        "venue_relationship_proven": False,
        "interpretation": (
            "Authentication and subscription prove bounded data access only. "
            "No short sample or absence of 406 proves execution venue, session quota, "
            "quote quality, account upgrade entitlement or profitability."
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = asyncio.run(collect_access(AlpacaPaperSettings.model_validate({})))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "schema": report["schema"],
                "locations": [
                    {key: value for key, value in item.items() if key != "frames"}
                    for item in report["locations"]
                ],
                "study_started": False,
                "venue_relationship_proven": False,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
