from __future__ import annotations

import argparse
import asyncio
import json
import re
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from tradeagent import event_news_policy
from tradeagent.config import AppConfig
from tradeagent.event_context import OfficialContextClient
from tradeagent.event_demo import activate_demo, demo_control_state
from tradeagent.event_doctor import code_identity, source_capabilities
from tradeagent.event_outcomes import outcome_summary
from tradeagent.event_replay import evaluate_extraction_fixture, replay_event_pipeline
from tradeagent.event_runtime import cohort_manifest, run_event_service
from tradeagent.event_session import verify_session_plan
from tradeagent.event_store import EventStore
from tradeagent.execution_reference import run_execution_accounting_audit
from tradeagent.experimental_policy import ExperimentalSettings, certificate
from tradeagent.intraday import NyseSessionCalendar
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
        parser.add_argument("--purpose", choices=["research", "iex-practice"])
        parser.add_argument("--practice-start-date", type=date.fromisoformat)
        parser.add_argument(
            "--entry-policy",
            choices=["event-strategy", "equipment-only-demo", "news-paper", "scheduled-operator"],
        )
        parser.add_argument("--demo-account-digest")
        parser.add_argument("--news-account-digest")
        parser.add_argument("--max-entries-per-session", type=int)
        parser.add_argument("--symbols")
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
            parser.add_argument("--confirm-equipment-only-demo", action="store_true")
            parser.add_argument("--demo-acceptance-sha256")
            parser.add_argument("--demo-reviewed-code-sha")
            parser.add_argument("--confirm-news-paper", action="store_true")
            parser.add_argument("--news-acceptance-sha256")
            parser.add_argument("--news-reviewed-code-sha")


def handle_event_command(args: argparse.Namespace) -> bool:
    if args.command not in COMMANDS:
        return False
    overrides = {
        key: value
        for key in (
            "cohort_id",
            "purpose",
            "practice_start_date",
            "entry_policy",
            "demo_account_digest",
            "news_account_digest",
            "max_entries_per_session",
            "symbols",
        )
        if (value := getattr(args, key, None)) is not None
    }
    if args.command == "run":
        overrides["mode"] = args.mode
    settings = ExperimentalSettings.model_validate(overrides)
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
        from tradeagent.event_session_report import session_report

        with Database(AppConfig().database_url.get_secret_value()) as database:
            store = EventStore(database)
            result = {
                **store.report(settings.cohort_id),
                "prospective_diagnostics": outcome_summary(store, settings.cohort_id),
                "session_report": session_report(database, settings.cohort_id),
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
        plan = None
        calendar_error = None
        if experimental.purpose == "iex-practice":
            with AlpacaPaperClient(AlpacaPaperSettings.model_validate({})) as broker:
                try:
                    if experimental.practice_start_date is None:
                        raise ValueError("practice session date is missing")
                    plan = verify_session_plan(
                        broker, experimental.practice_start_date, AppConfig().intraday, now
                    )
                except ValueError as error:
                    calendar_error = str(error)
        demo_checks: dict[str, bool] = {}
        demo_controls: dict[str, tuple[Any, Any]] = {}
        news_policy = experimental.entry_policy == "news-paper"
        if experimental.entry_policy in {"equipment-only-demo", "news-paper"}:
            with (
                Database(AppConfig().database_url.get_secret_value()) as database,
                AlpacaPaperClient(AlpacaPaperSettings.model_validate({})) as demo_broker,
            ):
                from tradeagent.event_orders import ExperimentalOrderManager

                repository = ProductionRepository(database)
                demo_controls = (
                    event_news_policy.control_state(EventStore(database), settings.cohort_id)
                    if news_policy
                    else demo_control_state(EventStore(database), settings.cohort_id)
                )
                heartbeat = repository.latest_heartbeat("tradeagent-event-worker")
                budget = ExperimentalOrderManager(
                    EventStore(database), demo_broker, experimental, AppConfig(), digest, sha
                ).session_budget()
                clock = demo_broker.clock()
                current_account = demo_broker.account()
                demo_checks = {
                    (
                        "explicit_news_paper_confirmation"
                        if news_policy
                        else "explicit_equipment_only_confirmation"
                    ): bool(
                        getattr(
                            args,
                            "confirm_news_paper" if news_policy else "confirm_equipment_only_demo",
                            False,
                        )
                        and re.fullmatch(
                            r"[0-9a-f]{64}",
                            getattr(
                                args,
                                "news_acceptance_sha256"
                                if news_policy
                                else "demo_acceptance_sha256",
                                None,
                            )
                            or "",
                        )
                        and getattr(
                            args,
                            "news_reviewed_code_sha" if news_policy else "demo_reviewed_code_sha",
                            None,
                        )
                        == sha
                    ),
                    "pinned_demo_account": sha256(account.id.encode()).hexdigest()
                    == (
                        experimental.news_account_digest
                        if news_policy
                        else experimental.demo_account_digest
                    ),
                    "demo_broker_currently_ready": (
                        demo_broker.broker_host == "https://paper-api.alpaca.markets"
                        and current_account.id == account.id
                        and current_account.status == "ACTIVE"
                        and not current_account.trading_blocked
                        and not current_account.account_blocked
                        and not demo_broker.positions()
                        and not demo_broker.open_orders()
                        and clock.is_open
                        and abs((clock.timestamp - now).total_seconds()) <= 60
                    ),
                    "demo_post_acceptance_authorization_interval": bool(
                        plan
                        and plan.session_open + timedelta(minutes=35)
                        <= now
                        < plan.session_open + timedelta(minutes=60)
                    ),
                    "demo_paused_and_unused": (
                        repository.get_control("kill_switch") == "active"
                        and repository.get_control(f"{settings.cohort_id}:pause")
                        == "OPERATOR_PAUSE"
                        and repository.get_control(f"{settings.cohort_id}:demo-terminal") is None
                        and repository.get_control(f"{settings.cohort_id}:demo-authorization")
                        is None
                        and (
                            not news_policy
                            or (
                                repository.get_control(f"{settings.cohort_id}:news-authorization")
                                is None
                                and repository.get_control(f"{settings.cohort_id}:news-terminal")
                                is None
                                and repository.get_control(f"{settings.cohort_id}:certificate")
                                is None
                            )
                        )
                        and bool(
                            budget
                            and budget["total_entries_reserved"] == 0
                            and budget["equipment_client_order_id"] is None
                        )
                    ),
                    "deployed_demo_worker_matches": bool(
                        heartbeat
                        and timedelta(0) <= now - heartbeat[1] <= timedelta(seconds=120)
                        and heartbeat[2].get("code_sha") == sha
                        and heartbeat[2].get("config_hash") == digest
                        and heartbeat[2].get("cohort_id") == settings.cohort_id
                        and heartbeat[2].get("entry_policy") == experimental.entry_policy
                    ),
                }
                if news_policy:
                    from tradeagent.event_account_risk import account_risk

                    demo_checks["shared_account_economic_limits"] = not account_risk(
                        EventStore(database), experimental, {}, now
                    )["blocked"]
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
                "frozen_policy_feed_entitlement": diagnostics["market_data"][
                    f"{experimental.execution_feed}_latest_quote"
                ]["accessible"],
                "configured_execution_feed": (
                    diagnostics["active_execution_feed"] == experimental.execution_feed
                ),
                "practice_session_valid": experimental.purpose != "iex-practice"
                or (
                    experimental.practice_start_date is not None
                    and NyseSessionCalendar(AppConfig().intraday).session_bounds(
                        experimental.practice_start_date
                    )
                    is not None
                ),
                "broker_calendar_verified": experimental.purpose != "iex-practice"
                or plan is not None,
                "planned_session_not_missed": experimental.purpose != "iex-practice"
                or (
                    plan is not None
                    and now < plan.session_close
                    and now.astimezone(ZoneInfo("America/New_York")).date() <= plan.session_date
                ),
                "official_macro_context": not context.blocking_reasons(now=now),
                "halts_verified": all(
                    context.halted_for(s, now=now) is False for s in settings.symbols.split(",")
                ),
                "fractional_assets": all(
                    a["fractionable"] and a["tradable"] for a in diagnostics["assets"]
                ),
                "operator_confirmation": args.confirm_experimental_paper,
                **demo_checks,
            },
            limitations=(
                "synthetic mechanics are not live exchange execution evidence",
                "each actual order rechecks source, quote, session, exposure and loss limits",
                "profitability unproven",
                *(
                    ("IEX practice only; excluded from strategy qualification",)
                    if experimental.purpose == "iex-practice"
                    else ()
                ),
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
                    if plan is not None:
                        repository.set_control(
                            f"{settings.cohort_id}:session-plan", plan.model_dump_json()
                        )
                        store.audit(
                            "broker_calendar", plan.model_dump(mode="json"), now, settings.cohort_id
                        )
                    if experimental.entry_policy == "equipment-only-demo":
                        activate_demo(
                            store,
                            experimental,
                            proof,
                            args.demo_acceptance_sha256,
                            demo_controls,
                            now,
                        )
                    elif experimental.entry_policy == "news-paper":
                        event_news_policy.activate(
                            store,
                            experimental,
                            proof,
                            args.news_acceptance_sha256,
                            demo_controls,
                            now,
                        )
                    else:
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
            "purpose": experimental.purpose,
            "qualification_eligible": experimental.purpose != "iex-practice",
            "broker_calendar": plan.model_dump(mode="json") if plan else None,
            "calendar_error": calendar_error,
        }
    text = json.dumps(result, indent=2, default=str, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + "\n", encoding="utf-8")
    print(text)
    return True
