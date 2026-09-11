from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from tradeagent.config import AppConfig
from tradeagent.persistence import Database
from tradeagent.scalping_config import ScalpingConfig

COMMANDS = {"scalp-run", "scalp-status", "scalp-stop", "scalp-replay"}


def register_scalping_commands(subparsers: Any) -> None:
    run = subparsers.add_parser("scalp-run", help="v30 always-on unrestricted PAPER crypto scalper")
    run.add_argument("--cohort-id", required=True)
    run.add_argument("--account-digest", required=True)
    run.add_argument("--approved-at", type=datetime.fromisoformat, required=True)
    run.add_argument("--symbols", default="BTC/USD,ETH/USD")
    run.add_argument("--notional", type=Decimal, default=Decimal("100"))
    run.add_argument("--entry-style", choices=("passive", "marketable"), default="passive")
    run.add_argument("--decision-seconds", type=float, default=1)
    run.add_argument("--horizon-seconds", type=float, default=5)
    run.add_argument("--holding-seconds", type=float, default=15)
    run.add_argument("--order-ttl-seconds", type=float, default=3)
    run.add_argument("--email-digest-seconds", type=int, default=1800)
    run.add_argument("--confirm-paper-unrestricted", action="store_true")
    status = subparsers.add_parser(
        "scalp-status", help="read v30 scalper state without sending orders"
    )
    status.add_argument("--cohort-id")
    stop = subparsers.add_parser(
        "scalp-stop", help="request v30 entry stop; owned recovery continues"
    )
    stop.add_argument("--cohort-id", required=True)
    stop.add_argument("--reason", required=True)
    replay = subparsers.add_parser(
        "scalp-replay", help="replay recorded v30 market events, not orders"
    )
    replay.add_argument("--events", type=Path, required=True)
    replay.add_argument("--output", type=Path)
    replay.add_argument("--symbols", default="BTC/USD,ETH/USD")
    replay.add_argument("--notional", type=Decimal, default=Decimal("100"))
    replay.add_argument("--latency-ms", type=float, default=25)


def configuration(args: argparse.Namespace) -> ScalpingConfig:
    if not args.confirm_paper_unrestricted:
        raise ValueError("scalp-run requires explicit selection of --confirm-paper-unrestricted")
    return ScalpingConfig(
        cohort_id=args.cohort_id,
        account_digest=args.account_digest,
        approved_at=args.approved_at,
        symbols=tuple(args.symbols.split(",")),
        order_notional_usd=args.notional,
        entry_style=args.entry_style,
        decision_interval_seconds=args.decision_seconds,
        feature_horizon_seconds=args.horizon_seconds,
        exit_after_seconds=args.holding_seconds,
        entry_order_ttl_seconds=args.order_ttl_seconds,
        email_digest_seconds=args.email_digest_seconds,
    )


def handle_scalping_command(args: argparse.Namespace) -> bool:
    if args.command not in COMMANDS:
        return False
    result: dict[str, Any]
    if args.command == "scalp-run":
        from tradeagent.scalping_runtime import run_scalping_service

        asyncio.run(run_scalping_service(configuration(args)))
        return True
    if args.command == "scalp-replay":
        from tradeagent.scalping_market import MarketEvent
        from tradeagent.scalping_replay import replay_events

        with args.events.open(encoding="utf-8") as source:
            events = [MarketEvent.model_validate_json(line) for line in source if line.strip()]
        config = ScalpingConfig(
            cohort_id="v30-replay",
            account_digest="0" * 64,
            approved_at=datetime(1970, 1, 1, tzinfo=UTC),
            symbols=tuple(args.symbols.split(",")),
            order_notional_usd=args.notional,
        )
        result = replay_events(events, config, latency_ms=args.latency_ms)
    else:
        from tradeagent.scalping_reporting import request_scalping_stop, scalping_status

        with Database(AppConfig().database_url.get_secret_value(), pool_size=1) as database:
            if args.command == "scalp-stop":
                result = request_scalping_stop(database, args.cohort_id, args.reason)
            else:
                result = scalping_status(database, cohort_id=args.cohort_id)
    serialized = json.dumps(result, indent=2, sort_keys=True, default=str) + "\n"
    output: Path | None = getattr(args, "output", None)
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(serialized, encoding="utf-8")
    else:
        print(serialized, end="")
    return True
