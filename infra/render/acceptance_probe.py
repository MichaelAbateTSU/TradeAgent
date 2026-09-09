"""Read-only same-service production probe; never opens a market-data socket."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from datetime import UTC, datetime, timedelta
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

RECORDER_SYMBOLS = ("SPY", "QQQ", "IWM", "TLT", "GLD")


def market_window_counts(
    database: Database,
    *,
    since: datetime,
    until: datetime,
    symbols: tuple[str, ...] = RECORDER_SYMBOLS,
) -> dict[str, Any]:
    if since.tzinfo is None or until.tzinfo is None or since > until or not symbols:
        raise ValueError("An ordered aware window and explicit recorder symbols are required")
    result = {}
    with database.begin() as connection:
        for table in (market_quotes, market_trades, market_bars):
            result[table.name] = [
                dict(row)
                for row in connection.execute(
                    select(
                        table.c.symbol,
                        func.count().label("count"),
                        func.max(table.c.event_at).label("latest_exchange_at"),
                        func.max(table.c.received_at).label("latest_received_at"),
                        func.max(table.c.processed_at).label("latest_committed_at"),
                    )
                    .where(
                        table.c.symbol.in_(symbols),
                        table.c.event_at >= since,
                        table.c.event_at <= until,
                    )
                    .group_by(table.c.symbol)
                ).mappings()
            ]
    return result


def physical_progress_failures(first: dict[str, Any], last: dict[str, Any]) -> list[str]:
    """Validate indexed physical endpoints; aggregate counters cannot prove each symbol."""

    def timestamp(value: Any) -> datetime:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(value)
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("Physical evidence timestamps must be timezone-aware")
        return parsed

    try:
        scopes = (first["market_count_scope"], last["market_count_scope"])
        starts = [timestamp(scope["since_exchange_at"]) for scope in scopes]
        ends = [timestamp(scope["until_exchange_at"]) for scope in scopes]
        if (
            starts[0] != starts[1]
            or not timedelta(0) < ends[1] - ends[0] < timedelta(minutes=25)
            or any(
                not timedelta(0) <= end - start <= timedelta(minutes=30)
                for start, end in zip(starts, ends, strict=True)
            )
            or any(
                len(scope["symbols"]) != len(RECORDER_SYMBOLS)
                or set(scope["symbols"]) != set(RECORDER_SYMBOLS)
                for scope in scopes
            )
        ):
            return ["physical:invalid_or_changed_fixed_window"]
    except (KeyError, TypeError, ValueError):
        return ["physical:missing_or_invalid_window_metadata"]

    failures = []
    for table in ("market_quotes", "market_trades", "market_bars"):
        try:
            series = []
            for sample, end in zip((first, last), ends, strict=True):
                rows = sample["market"][table]
                by_symbol = {row["symbol"]: row for row in rows}
                if len(by_symbol) != len(rows) or set(by_symbol) - set(RECORDER_SYMBOLS):
                    raise ValueError("Duplicate or unexpected physical symbol")
                for row in rows:
                    if type(row["count"]) is not int or row["count"] <= 0:
                        raise ValueError("Physical row counts must be positive integers")
                    exchange = timestamp(row["latest_exchange_at"])
                    if not starts[0] <= exchange <= end:
                        raise ValueError("Exchange proof outside the fixed window")
                    received = timestamp(row["latest_received_at"])
                    committed = timestamp(row["latest_committed_at"])
                    if not exchange <= received <= committed:
                        raise ValueError("Invalid exchange/receipt/processing chronology")
                series.append(by_symbol)
            for symbol in RECORDER_SYMBOLS:
                before, after = (rows.get(symbol) for rows in series)
                if (
                    after is None
                    or after["count"] <= (before["count"] if before else 0)
                    or (
                        before is not None
                        and timestamp(after["latest_exchange_at"])
                        <= timestamp(before["latest_exchange_at"])
                    )
                ):
                    failures.append(f"physical:{table}:{symbol}:not_advancing")
        except (KeyError, TypeError, ValueError):
            failures.append(f"physical:{table}:invalid_rows_or_timestamps")
    return failures


def snapshot(database: Database, *, market_since: datetime | None = None) -> dict[str, Any]:
    from tradeagent.reporting_metadata import missing_reporting_metadata
    from tradeagent.reporting_reads import REPORTING_PROJECTION_VERSION

    now = datetime.now(UTC)
    market_since = market_since or now - timedelta(minutes=1)
    if market_since.tzinfo is None or not timedelta(0) <= now - market_since <= timedelta(
        minutes=30
    ):
        raise ValueError("The verification window must begin within the previous 30 minutes")
    repository = ProductionRepository(database)
    with database.begin() as connection:
        result: dict[str, Any] = {
            "observed_at": now.isoformat(),
            "code_sha": os.environ.get("RENDER_GIT_COMMIT"),
            "database_select_one": connection.scalar(text("SELECT 1")),
            "migration": connection.scalar(text("SELECT version_num FROM alembic_version")),
            "audit_lookup_index_valid": connection.scalar(
                text(
                    "SELECT indisvalid FROM pg_index "
                    "WHERE indexrelid=to_regclass('ix_events_v2_trace_type_time')"
                )
            ),
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
            "retained_calibration_controls": [
                dict(row)
                for row in connection.execute(
                    select(controls).where(
                        controls.c.control_key.endswith(":calibration_status", autoescape=True)
                    )
                ).mappings()
            ],
            "market_count_scope": {
                "since_exchange_at": market_since.isoformat(),
                "until_exchange_at": now.isoformat(),
                "symbols": list(RECORDER_SYMBOLS),
                "basis": "Exact persisted rows in a fixed indexed window; not full-history totals",
            },
            "outbox_counts": [
                dict(row)
                for row in connection.execute(
                    select(notification_outbox.c.status, func.count().label("count")).group_by(
                        notification_outbox.c.status
                    )
                ).mappings()
            ],
        }
        result["latest_event_types"] = [
            dict(row)
            for row in connection.execute(
                select(events.c.event_type, func.max(events.c.occurred_at).label("latest_at"))
                .where(events.c.event_type.like("event_%"))
                .group_by(events.c.event_type)
            ).mappings()
        ]
    result["market"] = market_window_counts(database, since=market_since, until=now)
    result["reporting_projection_version"] = REPORTING_PROJECTION_VERSION
    result["reporting_metadata_missing"] = missing_reporting_metadata(database)
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
            else:
                try:
                    error = response.json()
                    result["provider_error"] = {key: error.get(key) for key in ("name", "message")}
                except ValueError:
                    result["provider_error"] = {"name": "non_json_provider_response"}
                result["delivery_verification"] = "unavailable; sent is not inbox confirmation"
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
    parser.add_argument("--market-since", type=datetime.fromisoformat)
    args = parser.parse_args()
    if args.samples < 1 or args.interval < 1:
        parser.error("samples and interval must be positive")
    market_since = args.market_since or datetime.now(UTC) - timedelta(minutes=1)
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
                + json.dumps(
                    snapshot(database, market_since=market_since), default=str, sort_keys=True
                ),
                flush=True,
            )
            if index + 1 < args.samples:
                time.sleep(args.interval)


if __name__ == "__main__":
    main()
