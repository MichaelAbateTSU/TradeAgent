"""Read-only same-service production probe; never opens a market-data socket."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from datetime import UTC, datetime
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import func, select, text

from tradeagent.alpaca_paper import AlpacaPaperClient, AlpacaPaperSettings
from tradeagent.config import AppConfig
from tradeagent.persistence import (
    Database,
    ProductionRepository,
    controls,
    events,
    heartbeats,
    market_bars,
    market_quotes,
    market_trades,
    notification_outbox,
    worker_locks,
)


def snapshot(database: Database) -> dict[str, Any]:
    now = datetime.now(UTC)
    repository = ProductionRepository(database)
    with database.begin() as connection:
        result: dict[str, Any] = {
            "observed_at": now.isoformat(),
            "code_sha": os.environ.get("RENDER_GIT_COMMIT"),
            "database_select_one": connection.scalar(text("SELECT 1")),
            "migration": connection.scalar(text("SELECT version_num FROM alembic_version")),
            "heartbeats": [dict(row) for row in connection.execute(select(heartbeats)).mappings()],
            "leases": [dict(row) for row in connection.execute(select(worker_locks)).mappings()],
            "controls": {
                row.control_key: row.control_value
                for row in connection.execute(
                    select(controls.c.control_key, controls.c.control_value).where(
                        (controls.c.control_key == "kill_switch")
                        | controls.c.control_key.endswith(":pause")
                    )
                )
            },
            "market": {},
            "outbox_counts": [
                dict(row)
                for row in connection.execute(
                    select(notification_outbox.c.status, func.count().label("count")).group_by(
                        notification_outbox.c.status
                    )
                ).mappings()
            ],
        }
        for table in (market_quotes, market_trades, market_bars):
            result["market"][table.name] = [
                dict(row)
                for row in connection.execute(
                    select(
                        table.c.symbol,
                        func.count().label("count"),
                        func.max(table.c.event_at).label("latest_exchange_at"),
                        func.max(table.c.received_at).label("latest_received_at"),
                        func.max(table.c.processed_at).label("latest_committed_at"),
                    ).group_by(table.c.symbol)
                ).mappings()
            ]
        result["latest_event_types"] = [
            dict(row)
            for row in connection.execute(
                select(events.c.event_type, func.max(events.c.occurred_at).label("latest_at"))
                .where(events.c.event_type.like("event_%"))
                .group_by(events.c.event_type)
            ).mappings()
        ]
    result["latest_calibration"] = repository.latest_event_payload("event_calibration_status")
    with AlpacaPaperClient(AlpacaPaperSettings.model_validate({})) as broker:
        account = broker.account()
        result["broker"] = {
            "host": "https://paper-api.alpaca.markets",
            "account_digest": hashlib.sha256(str(account.id).encode()).hexdigest(),
            "status": str(account.status),
            "trading_blocked": account.trading_blocked,
            "account_blocked": account.account_blocked,
            "positions": len(broker.positions()),
            "open_orders": len(broker.open_orders()),
            "clock": broker.clock().model_dump(mode="json"),
            "test_orders_submitted": 0,
        }
    return result


def report_test(database: Database, release: str, *, email: bool) -> dict[str, Any]:
    from tradeagent.daily_status import build_daily_status
    from tradeagent.notifications import RoundTripNotificationRepository

    started = time.monotonic()
    now = datetime.now(UTC)
    payload = build_daily_status(database, now, "America/New_York")
    result: dict[str, Any] = {
        "duration_seconds": time.monotonic() - started,
        "serialized_bytes": len(json.dumps(payload, default=str).encode()),
        "cohort_id": payload.get("cohort_id"),
        "daily_schedule_changed": False,
    }
    if email:
        notification_id = uuid5(NAMESPACE_URL, f"tradeagent:release-acceptance:{release}")
        payload["subject"] = "[RELEASE TEST — NOT A TRADE] " + payload["subject"]
        payload["text"] = (
            "Release acceptance test of the existing production report and notifier. "
            "No test order was submitted. Morning MISSED evidence is unchanged.\n\n"
            + payload["text"]
        )
        result["notification_id"] = str(notification_id)
        result["newly_enqueued"] = RoundTripNotificationRepository(database).enqueue_status(
            notification_id, payload, created_at=now
        )
    # Linux Render jobs expose their peak RSS without an additional dependency.
    try:
        import resource

        result["job_peak_rss_bytes"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
    except ImportError:
        result["job_peak_rss_bytes"] = None
    return result


def email_status(database: Database, release: str) -> dict[str, Any]:
    import httpx

    from tradeagent.notifications import EmailSettings

    notification_id = uuid5(NAMESPACE_URL, f"tradeagent:release-acceptance:{release}")
    with database.begin() as connection:
        row = (
            connection.execute(
                select(
                    notification_outbox.c.notification_id,
                    notification_outbox.c.status,
                    notification_outbox.c.attempts,
                    notification_outbox.c.sent_at,
                    notification_outbox.c.provider_message_id,
                ).where(notification_outbox.c.notification_id == str(notification_id))
            )
            .mappings()
            .one_or_none()
        )
    result = dict(row) if row else {"notification_id": str(notification_id), "status": "missing"}
    if result.get("provider_message_id"):
        settings = EmailSettings.model_validate({})
        with httpx.Client(timeout=30) as client:
            response = client.get(
                f"https://api.resend.com/emails/{result['provider_message_id']}",
                headers={"Authorization": f"Bearer {settings.api_key.get_secret_value()}"},
            )
            result["provider_status_http"] = response.status_code
            if response.status_code == 200:
                payload = response.json()
                result["provider"] = {
                    key: payload.get(key) for key in ("id", "created_at", "last_event")
                }
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", action="store_true")
    parser.add_argument(
        "--email-release", help="Explicitly enqueue one idempotent release-test email"
    )
    parser.add_argument("--email-status", help="Read the outbox/provider state for a release test")
    parser.add_argument("--samples", type=int, default=1)
    parser.add_argument("--interval", type=float, default=60)
    args = parser.parse_args()
    if args.samples < 1 or args.interval < 1:
        parser.error("samples and interval must be positive")
    with Database(AppConfig().database_url.get_secret_value()) as database:
        if args.email_status:
            print(
                "ACCEPTANCE_EMAIL "
                + json.dumps(
                    email_status(database, args.email_status), default=str, sort_keys=True
                ),
                flush=True,
            )
        if args.report or args.email_release:
            print(
                "ACCEPTANCE_REPORT "
                + json.dumps(
                    report_test(database, args.email_release or "", email=bool(args.email_release)),
                    default=str,
                    sort_keys=True,
                ),
                flush=True,
            )
        for index in range(args.samples):
            print(
                "ACCEPTANCE_SNAPSHOT "
                + json.dumps(snapshot(database), default=str, sort_keys=True),
                flush=True,
            )
            if index + 1 < args.samples:
                time.sleep(args.interval)


if __name__ == "__main__":
    main()
