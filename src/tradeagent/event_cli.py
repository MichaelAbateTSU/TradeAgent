from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from tradeagent.config import AppConfig
from tradeagent.event_context import OfficialContextClient
from tradeagent.event_doctor import code_identity, source_capabilities
from tradeagent.event_outcomes import outcome_summary
from tradeagent.event_replay import evaluate_extraction_fixture, replay_event_pipeline
from tradeagent.event_runtime import cohort_manifest, run_event_service
from tradeagent.event_store import EventStore
from tradeagent.execution_reference import run_execution_accounting_audit
from tradeagent.experimental_policy import ExperimentalSettings, certificate
from tradeagent.persistence import Database, ProductionRepository

COMMANDS = {
    "doctor",
    "source-capabilities",
    "audit-execution",
    "event-replay",
    "evaluate-extraction",
    "paper-preflight",
    "run",
    "news-record",
    "experiment-freeze",
    "experiment-report",
    "risk-pause",
    "reconcile",
}


def register_event_commands(subparsers: Any) -> None:
    for name in sorted(COMMANDS):
        parser = subparsers.add_parser(name, help=f"v20 event experiment: {name}")
        parser.add_argument("--output", type=Path)
        parser.add_argument("--cohort-id")
        if name == "run":
            parser.add_argument(
                "--mode", choices=["shadow", "experimental-paper"], default="shadow"
            )
            parser.add_argument("--once", action="store_true")
        elif name == "news-record":
            parser.add_argument("--once", action="store_true")
        elif name == "evaluate-extraction":
            parser.add_argument(
                "--fixture",
                type=Path,
                default=Path("research/fixtures/event-v20-extraction-gold.json"),
            )
        elif name == "paper-preflight":
            parser.add_argument("--confirm-experimental-paper", action="store_true")


def handle_event_command(args: argparse.Namespace) -> bool:
    if args.command not in COMMANDS:
        return False
    settings = ExperimentalSettings.model_validate(
        {"cohort_id": args.cohort_id} if args.cohort_id else {}
    )
    result: Any
    if args.command in {"doctor", "source-capabilities"}:
        result = source_capabilities()
    elif args.command == "audit-execution":
        result = run_execution_accounting_audit(Path.cwd()).model_dump(mode="json")
    elif args.command == "event-replay":
        result = replay_event_pipeline()
    elif args.command == "evaluate-extraction":
        result = evaluate_extraction_fixture(args.fixture)
    elif args.command in {"run", "news-record"}:
        mode = args.mode if args.command == "run" else "shadow"
        settings = ExperimentalSettings.model_validate(
            {
                **settings.model_dump(),
                "mode": mode,
            }
        )
        result = asyncio.run(run_event_service(settings, once=args.once))
    elif args.command == "experiment-freeze":
        sha = code_identity()
        config_hash, manifest = cohort_manifest(settings, sha)
        with Database(AppConfig().database_url.get_secret_value()) as database:
            EventStore(database).freeze(
                settings.cohort_id, config_hash, manifest, settings.mode, datetime.now(UTC)
            )
        result = manifest
    elif args.command == "experiment-report":
        with Database(AppConfig().database_url.get_secret_value()) as database:
            store = EventStore(database)
            result = {
                **store.report(settings.cohort_id),
                "prospective_diagnostics": outcome_summary(store, settings.cohort_id),
            }
    elif args.command == "risk-pause":
        with Database(AppConfig().database_url.get_secret_value()) as database:
            repository = ProductionRepository(database)
            repository.set_control(f"{settings.cohort_id}:pause", "OPERATOR_PAUSE")
            repository.set_control("kill_switch", "active")
        result = {"state": "paused", "risk_exits_and_reconciliation": "remain active"}
    elif args.command == "reconcile":
        from tradeagent.alpaca_paper import AlpacaPaperClient, AlpacaPaperSettings
        from tradeagent.event_orders import ExperimentalOrderManager

        with (
            Database(AppConfig().database_url.get_secret_value()) as database,
            AlpacaPaperClient(AlpacaPaperSettings.model_validate({})) as broker,
        ):
            sha = code_identity()
            digest, _ = cohort_manifest(settings, sha)
            result = ExperimentalOrderManager(
                EventStore(database), broker, settings, AppConfig(), digest, sha
            ).reconcile(datetime.now(UTC))
    else:
        diagnostics = source_capabilities()
        replay = replay_event_pipeline()
        with OfficialContextClient() as context_client:
            context = context_client.poll(symbols=settings.symbols.split(","))
        experimental = ExperimentalSettings.model_validate(
            {
                **settings.model_dump(),
                "mode": "experimental-paper",
            }
        )
        sha = code_identity()
        digest, manifest = cohort_manifest(experimental, sha)
        from tradeagent.alpaca_paper import AlpacaPaperClient, AlpacaPaperSettings

        with AlpacaPaperClient(AlpacaPaperSettings.model_validate({})) as broker:
            account = broker.account()
        now = datetime.now(UTC)
        proof = certificate(
            experimental,
            config_hash=digest,
            code_sha=sha,
            account_id=account.id,
            now=now,
            checks={
                "paper_account_verified": diagnostics["broker_healthy"],
                "no_live_credential_environment": not diagnostics[
                    "live_credential_environment_present"
                ],
                "flat_unreserved_account": diagnostics["broker_positions"] == 0
                and diagnostics["broker_open_orders"] == 0,
                "synthetic_lifecycle": replay["reconciled"],
                "frozen_policy_feed_entitlement": diagnostics["market_data"]["sip_latest_quote"][
                    "accessible"
                ],
                "official_macro_context": not context.blocking_reasons(now=now),
                "halts_verified": all(
                    context.halted_for(s, now=now) is False for s in settings.symbols.split(",")
                ),
                "fractional_assets": all(
                    a["fractionable"] and a["tradable"] for a in diagnostics["assets"]
                ),
                "operator_confirmation": args.confirm_experimental_paper,
            },
            limitations=(
                "synthetic mechanics are not live exchange execution evidence",
                "each actual order rechecks source, quote, session, exposure and loss limits",
                "profitability unproven",
            ),
        )
        if args.confirm_experimental_paper:
            with Database(AppConfig().database_url.get_secret_value()) as database:
                store = EventStore(database)
                store.freeze(settings.cohort_id, digest, manifest, experimental.mode, now)
                store.audit(
                    "operational_certificate",
                    proof.model_dump(mode="json"),
                    now,
                    settings.cohort_id,
                )
                if proof.permits_paper:
                    repository = ProductionRepository(database)
                    repository.set_control(
                        f"{settings.cohort_id}:certificate", proof.model_dump_json()
                    )
                    repository.set_control("v20:mechanics_attestation", sha)
                    repository.set_control("kill_switch", "inactive")
        result = {
            "broker": diagnostics,
            "synthetic_lifecycle_reconciled": replay["reconciled"],
            "operational_certificate_issued": proof.permits_paper,
            "mode": "experimental-paper" if proof.permits_paper else "shadow",
            "certificate": proof.model_dump(mode="json"),
            "blockers": [key for key, passed in proof.checks.items() if not passed],
            "confirmation_received": args.confirm_experimental_paper,
            "edge_established": False,
        }
    text = json.dumps(result, indent=2, default=str, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + "\n", encoding="utf-8")
    print(text)
    return True
