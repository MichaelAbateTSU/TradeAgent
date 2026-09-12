from __future__ import annotations

import argparse
import asyncio
import gzip
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from tradeagent.config import AppConfig
from tradeagent.persistence import Database
from tradeagent.scalping_config import ScalpingConfig

COMMANDS = {
    "scalp-run",
    "scalp-status",
    "scalp-stop",
    "scalp-replay",
    "scalp-diagnose",
    "scalp-calibrate",
}


def register_scalping_commands(subparsers: Any) -> None:
    run = subparsers.add_parser("scalp-run", help="always-on action-value PAPER crypto scalper")
    run.add_argument("--cohort-id", required=True)
    run.add_argument("--account-digest", required=True)
    run.add_argument("--approved-at", type=datetime.fromisoformat, required=True)
    run.add_argument("--symbols", default="BTC/USD,ETH/USD")
    run.add_argument("--notional", type=Decimal, default=Decimal("100"))
    run.add_argument("--decision-seconds", type=float, default=1)
    run.add_argument("--horizon-seconds", type=float, default=5)
    run.add_argument("--holding-seconds", type=float)
    run.add_argument("--latency-grace-seconds", type=float, default=0)
    run.add_argument("--catastrophic-stop-bps", type=Decimal, default=Decimal("100"))
    run.add_argument("--economic-model", type=Path)
    run.add_argument("--model-sha256")
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
    replay.add_argument("--account-digest")
    replay.add_argument("--latency-ms", type=float, default=25)
    replay.add_argument("--economic-model", type=Path)
    replay.add_argument("--model-sha256")
    replay.add_argument("--legacy-replay", action="store_true")
    diagnose = subparsers.add_parser(
        "scalp-diagnose", help="reconstruct recorded fills, fees, markouts and failure attribution"
    )
    diagnose.add_argument("--snapshot", type=Path, required=True)
    diagnose.add_argument("--output", type=Path)
    calibrate = subparsers.add_parser(
        "scalp-calibrate",
        help="freeze a chronological economic artifact or an explicit no-support result",
    )
    calibrate.add_argument("--diagnostics", type=Path, required=True)
    calibrate.add_argument("--model-output", type=Path, required=True)
    calibrate.add_argument("--audit-output", type=Path, required=True)
    calibrate.add_argument("--horizon-seconds", type=float, default=5)
    calibrate.add_argument("--maker-fee-bps", type=float, default=15)
    calibrate.add_argument("--taker-fee-bps", type=float, default=25)
    calibrate.add_argument("--calibrated-at", type=datetime.fromisoformat, required=True)
    calibrate.add_argument("--valid-until", type=datetime.fromisoformat, required=True)


def configuration(args: argparse.Namespace) -> ScalpingConfig:
    if not args.confirm_paper_unrestricted:
        raise ValueError("scalp-run requires explicit selection of --confirm-paper-unrestricted")
    holding = args.horizon_seconds + args.latency_grace_seconds
    if args.holding_seconds is not None and args.holding_seconds != holding:
        raise ValueError(
            "holding time must equal the prediction horizon plus declared latency grace"
        )
    return ScalpingConfig(
        cohort_id=args.cohort_id,
        account_digest=args.account_digest,
        approved_at=args.approved_at,
        symbols=tuple(args.symbols.split(",")),
        order_notional_usd=args.notional,
        decision_interval_seconds=args.decision_seconds,
        feature_horizon_seconds=args.horizon_seconds,
        exit_after_seconds=holding,
        entry_order_ttl_seconds=args.order_ttl_seconds,
        email_digest_seconds=args.email_digest_seconds,
        decision_policy="action-value-v1",
        catastrophic_stop_bps=args.catastrophic_stop_bps,
        latency_grace_seconds=args.latency_grace_seconds,
        economic_model_path=str(args.economic_model) if args.economic_model else None,
        economic_model_sha256=args.model_sha256,
    )


def handle_scalping_command(args: argparse.Namespace) -> bool:
    if args.command not in COMMANDS:
        return False
    result: dict[str, Any]
    if args.command == "scalp-run":
        from tradeagent.scalping_runtime import run_scalping_service

        asyncio.run(run_scalping_service(configuration(args)))
        return True
    if args.command == "scalp-calibrate":
        from tradeagent.scalping_policy import calibrate_from_diagnostics, write_model_artifact

        if args.diagnostics.suffix == ".gz":
            with gzip.open(args.diagnostics, "rt", encoding="utf-8") as source:
                report = json.load(source)
        else:
            report = json.loads(args.diagnostics.read_text(encoding="utf-8"))
        model, audit = calibrate_from_diagnostics(
            report,
            horizon_seconds=args.horizon_seconds,
            maker_fee_bps=args.maker_fee_bps,
            taker_fee_bps=args.taker_fee_bps,
            calibrated_at=args.calibrated_at,
            valid_until=args.valid_until,
        )
        digest = write_model_artifact(model, args.model_output)
        result = {
            **audit,
            "model_path": str(args.model_output),
            "model_file_sha256": digest,
        }
        args.audit_output.parent.mkdir(parents=True, exist_ok=True)
        args.audit_output.write_text(
            json.dumps(result, indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
            newline="\n",
        )
    elif args.command == "scalp-diagnose":
        from tradeagent.scalping_diagnostics import reconstruct_period

        if args.snapshot.suffix == ".gz":
            with gzip.open(args.snapshot, "rt", encoding="utf-8") as source:
                snapshot = json.load(source)
        else:
            snapshot = json.loads(args.snapshot.read_text(encoding="utf-8"))
        if "results" in snapshot:
            if snapshot.get("status") != "succeeded" or len(snapshot["results"]) != 1:
                raise ValueError("a single completed evidence export is required")
            snapshot = snapshot["results"][0]
        result = reconstruct_period(snapshot)
    elif args.command == "scalp-replay":
        from tradeagent.scalping_market import MarketEvent
        from tradeagent.scalping_replay import replay_events

        with args.events.open(encoding="utf-8") as source:
            events = [MarketEvent.model_validate_json(line) for line in source if line.strip()]
        if args.economic_model and not args.account_digest:
            raise ValueError("economic replay requires the artifact's paper account digest")
        config = ScalpingConfig(
            cohort_id="v30-replay",
            account_digest=args.account_digest or "0" * 64,
            approved_at=datetime(1970, 1, 1, tzinfo=UTC),
            symbols=tuple(args.symbols.split(",")),
            order_notional_usd=args.notional,
            decision_policy="legacy-v30" if args.legacy_replay else "action-value-v1",
            catastrophic_stop_bps=None if args.legacy_replay else Decimal("100"),
            economic_model_path=str(args.economic_model) if args.economic_model else None,
            economic_model_sha256=args.model_sha256,
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
