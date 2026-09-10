"""Explicit finite owner requests, consumed only by the running event worker's lease."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from typing import Any, Literal, Self
from uuid import NAMESPACE_URL, UUID, uuid5

import httpx
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import ColumnElement, exists, literal, select

from tradeagent.config import AppConfig
from tradeagent.event_context import OfficialContextSnapshot
from tradeagent.experimental_policy import ExperimentalSettings
from tradeagent.notifications import RoundTripNotificationRepository
from tradeagent.paper_account_history import EASTERN, history_identity, save_baseline
from tradeagent.persistence import ProductionRepository, controls, worker_locks

PROTOCOL = "owner-paper-calibration-v1"


class OperatorPaperRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    request_id: UUID
    action: Literal["one_paper_AAPL_round_trip"]
    worker_cohort_id: str = Field(min_length=1, max_length=51)
    worker_config_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    code_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    account_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    session_date: date
    approved_at: AwareDatetime
    entry_deadline: AwareDatetime
    acceptance_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    history_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    owner_authority: str = Field(min_length=20, max_length=1000)
    checks: dict[str, bool]
    control_versions: dict[str, tuple[str | None, str | None]]

    @model_validator(mode="after")
    def validate_scope(self) -> Self:
        if (
            self.approved_at.astimezone(EASTERN).date() != self.session_date
            or self.entry_deadline.astimezone(EASTERN).date() != self.session_date
            or not timedelta(0) < self.entry_deadline - self.approved_at <= timedelta(minutes=30)
            or not all(
                self.checks.get(key) is True
                for key in (
                    "reviewed_release",
                    "market_acceptance",
                    "account_history_complete",
                    "operator_confirmation",
                    "normal_entries_paused",
                )
            )
            or set(self.control_versions)
            != {"kill_switch", f"{self.worker_cohort_id}:pause", f"{self.cohort_id}:pause"}
            or self.control_versions["kill_switch"][0] != "active"
            or self.control_versions["kill_switch"][1] is None
            or self.control_versions[f"{self.cohort_id}:pause"][0] not in {None, ""}
            or any((value is None) != (at is None) for value, at in self.control_versions.values())
        ):
            raise ValueError("explicit finite reviewed owner scope required")
        return self

    @property
    def cohort_id(self) -> str:
        return "operator-paper-" + self.request_id.hex

    def in_window(self, now: datetime) -> bool:
        return self.approved_at <= now < self.entry_deadline


def control_versions(
    repo: ProductionRepository,
    cohort: str,
    operator_cohort: str | None = None,
) -> dict[str, tuple[str | None, str | None]]:
    keys = ("kill_switch", f"{cohort}:pause") + (
        (f"{operator_cohort}:pause",) if operator_cohort is not None else ()
    )
    with repo._database.begin() as connection:
        values = {
            row["control_key"]: (
                row["control_value"],
                (
                    row["updated_at"].replace(tzinfo=UTC)
                    if row["updated_at"].tzinfo is None
                    else row["updated_at"].astimezone(UTC)
                ).isoformat(),
            )
            for row in connection.execute(
                select(controls).where(controls.c.control_key.in_(keys))
            ).mappings()
        }
    return {key: values.get(key, (None, None)) for key in keys}


def configuration(
    request: OperatorPaperRequest,
    app: AppConfig | None = None,
) -> tuple[ExperimentalSettings, str, dict[str, Any]]:
    app = app or AppConfig()
    values: dict[str, Any] = {
        "mode": "experimental-paper",
        "purpose": "iex-practice",
        "entry_policy": "operator-calibration",
        "cohort_id": request.cohort_id,
        "practice_start_date": request.session_date,
        "symbols": "AAPL",
        "news_account_digest": request.account_digest,
        "_env_file": None,
    }
    settings = ExperimentalSettings(**values)
    manifest = {
        "settings": settings.model_dump(mode="json"),
        "code_sha": request.code_sha,
        "operator_scope": request.model_dump(mode="json"),
        "session_protocol_version": PROTOCOL,
        "qualification_eligible": False,
        "normal_entry_kill_unchanged": True,
        "protective_exits": {
            "broker_native": False,
            "stop_fraction": "0.005",
            "target_fraction": "0.01",
            "cancel_unfilled_after_seconds": 30,
            "liquidate_after_seconds": 60,
        },
        "operational_settings": {
            "intraday": app.intraday.model_dump(mode="json"),
            "risk": app.risk.model_dump(mode="json"),
        },
    }
    digest = sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest()
    manifest["config_hash"] = digest
    return settings, digest, manifest


def load_request(repo: ProductionRepository, cohort: str) -> OperatorPaperRequest | None:
    raw = repo.get_control(f"{cohort}:operator-scope")
    return OperatorPaperRequest.model_validate_json(raw) if raw else None


def authorized(
    repo: ProductionRepository,
    request: OperatorPaperRequest | None,
    settings: ExperimentalSettings,
    config_hash: str,
    code_sha: str,
    now: datetime,
    app: AppConfig,
) -> bool:
    if request is None:
        return False
    return bool(
        settings.entry_policy == "operator-calibration"
        and request.cohort_id == settings.cohort_id
        and request.code_sha == code_sha
        and configuration(request, app)[1] == config_hash
        and settings.news_account_digest == request.account_digest
        and request.in_window(now)
        and load_request(repo, settings.cohort_id) == request
        and control_versions(repo, request.worker_cohort_id, request.cohort_id)
        == request.control_versions
        and repo.get_control(f"{settings.cohort_id}:operator-terminal") is None
    )


def final_entry_fence(
    repo: ProductionRepository,
    request: OperatorPaperRequest | None,
    owner_id: str | None,
    now: datetime,
) -> bool:
    """One final database snapshot; callers must recheck local time after it commits."""
    if request is None or owner_id is None or not request.in_window(now):
        return False
    conditions: list[ColumnElement[bool]] = []
    for key, (value, updated_at) in request.control_versions.items():
        query = select(controls.c.control_key).where(controls.c.control_key == key)
        conditions.append(
            ~exists(query)
            if value is None
            else exists(
                query.where(
                    controls.c.control_value == value,
                    controls.c.updated_at == datetime.fromisoformat(str(updated_at)),
                )
            )
        )
    conditions.extend(
        [
            exists(
                select(controls.c.control_key).where(
                    controls.c.control_key == f"{request.cohort_id}:operator-scope",
                    controls.c.control_value == request.model_dump_json(),
                )
            ),
            ~exists(
                select(controls.c.control_key).where(
                    controls.c.control_key == f"{request.cohort_id}:operator-terminal"
                )
            ),
            exists(
                select(worker_locks.c.lock_name).where(
                    worker_locks.c.lock_name == "tradeagent-event-worker",
                    worker_locks.c.owner_id == owner_id,
                    worker_locks.c.acquired_at >= now - timedelta(seconds=180),
                    worker_locks.c.acquired_at <= now,
                )
            ),
        ]
    )
    with repo._database.begin() as connection:
        return bool(connection.scalar(select(literal(True)).where(*conditions)))


def _notice(runtime: Any, request: OperatorPaperRequest, state: dict[str, Any]) -> None:
    terminal = state["state"]
    RoundTripNotificationRepository(runtime.store.database).enqueue_status(
        uuid5(NAMESPACE_URL, f"{PROTOCOL}:{request.request_id}:{terminal}"),
        {
            "subject": f"[TradeAgent PAPER operator test] {terminal}",
            "text": (
                f"Explicit owner equipment test, not a news signal or qualification.\n"
                f"Request: {request.request_id}\nCohort: {request.cohort_id}\n"
                f"Worker: {runtime.instance_id}\nCode: {runtime.code_sha}\n"
                + json.dumps(state, sort_keys=True, default=str)
                + "\nNormal strategy entries remain paused. No expected return claim."
            ),
            "operator_request_id": str(request.request_id),
            "qualification_eligible": False,
        },
        created_at=datetime.now(UTC),
    )
    if runtime.repo.get_control(f"{request.cohort_id}:operator-terminal") is not None:
        with runtime.store.database.begin() as connection:
            rows = (
                connection.execute(
                    select(controls)
                    .where(
                        controls.c.control_key.in_(
                            [
                                "operator-paper-active",
                                f"operator-paper-request:{request.worker_cohort_id}",
                            ]
                        )
                    )
                    .with_for_update()
                )
                .mappings()
                .all()
            )
            for row in rows:
                try:
                    matches = (
                        OperatorPaperRequest.model_validate_json(row["control_value"]) == request
                    )
                except ValueError:
                    matches = False
                if matches:
                    connection.execute(
                        controls.delete().where(
                            controls.c.control_key == row["control_key"],
                            controls.c.control_value == row["control_value"],
                            controls.c.updated_at == row["updated_at"],
                        )
                    )


def context_valid(ticket: dict[str, Any] | None, now: datetime) -> bool:
    if not ticket:
        return False
    try:
        context = OfficialContextSnapshot.model_validate(ticket["context"])
        receipt = datetime.fromisoformat(ticket["news_receipt_at"])
        return bool(
            not context.errors
            and not context.blocking_reasons(now=now)
            and context.halted_for("AAPL", now=now) is False
            and timedelta(0) <= now - receipt <= timedelta(seconds=120)
            and not any(
                now - timedelta(minutes=30) <= at <= now + timedelta(minutes=31)
                for at in context.scheduled_macro_events
            )
        )
    except (ValueError, KeyError, TypeError):
        return False


def _step(runtime: Any) -> dict[str, Any] | None:
    from tradeagent.event_orders import FINAL, ExperimentalOrderManager
    from tradeagent.event_runtime import _execution_quote
    from tradeagent.event_store import event_cohorts
    from tradeagent.experimental_policy import certificate

    raw = runtime.repo.get_control(f"operator-paper-request:{runtime.settings.cohort_id}")
    active = runtime.repo.get_control("operator-paper-active")
    if active:
        prior = OperatorPaperRequest.model_validate_json(active)
        if not runtime.repo.get_control(f"{prior.cohort_id}:operator-terminal"):
            raw = active
    if raw is None:
        return None
    request = OperatorPaperRequest.model_validate_json(raw)
    settings, config_hash, manifest = configuration(request, runtime.app)
    existing_rows = runtime.store.linked_orders(request.cohort_id)
    frozen_app = runtime.app
    if existing_rows:
        with runtime.store.database.begin() as connection:
            manifest = connection.scalar(
                select(event_cohorts.c.manifest).where(
                    event_cohorts.c.cohort_id == request.cohort_id
                )
            )
        settings = ExperimentalSettings.model_validate(manifest["settings"])
        config_hash = manifest["config_hash"]
        frozen_app = AppConfig(**manifest["operational_settings"])
    terminal_key = f"{request.cohort_id}:operator-terminal"
    terminal = runtime.repo.get_control(terminal_key)
    if terminal:
        _notice(runtime, request, json.loads(terminal))
        return None
    if runtime.context is None and not existing_rows:
        return None
    now = datetime.now(UTC)
    runtime.oms.assert_owner(now)
    manager = None
    try:
        owns_code = (
            runtime.code_sha == request.code_sha
            and runtime.config_hash == request.worker_config_hash
            and runtime.settings.cohort_id == request.worker_cohort_id
        )
        if not owns_code and not existing_rows:
            raise ValueError("request does not match the running worker code/config/cohort")
        runtime.store.freeze(request.cohort_id, config_hash, manifest, settings.mode, now)
        scope_key = f"{request.cohort_id}:operator-scope"
        saved = runtime.repo.get_control(scope_key)
        if saved is None:
            runtime.repo.set_control(scope_key, request.model_dump_json())
        elif OperatorPaperRequest.model_validate_json(saved) != request:
            raise ValueError("immutable operator request changed")
        runtime.repo.set_control("operator-paper-active", raw)
        manager = ExperimentalOrderManager(
            runtime.store,
            runtime.broker,
            settings,
            frozen_app,
            config_hash,
            request.code_sha,
            runtime.instance_id,
            quote_provider=runtime._protective_quote,
            recovery_only=not owns_code,
        )
        rows = runtime.store.linked_orders(request.cohort_id)
        if rows:
            manager.supervise(now, feed_healthy=runtime._sources_fresh(now))
            rows = runtime.store.linked_orders(request.cohort_id)
            if all(row["status"] in FINAL for row in rows) and not manager.inventory(rows):
                result = manager.reconcile(datetime.now(UTC))
                if result["healthy"] and not result["positions"] and not result["open_orders"]:
                    state = {
                        "state": "completed_flat"
                        if any(row["filled_quantity"] for row in rows)
                        else "no_fill_flat",
                        "actual_filled_round_trip": any(row["filled_quantity"] for row in rows),
                        "orders": [
                            {
                                "client_order_id": row["client_order_id"],
                                "broker": row["link"].get("broker"),
                            }
                            for row in rows
                        ],
                        "reconciliation": result,
                        "timing": {
                            "target_exit_at": rows[0]["link"].get("exit_at"),
                            "cancel_unfilled_deadline": rows[0]["link"].get("expires_at"),
                            "entry_broker_submitted_at": (rows[0]["link"].get("broker") or {}).get(
                                "submitted_at"
                            ),
                            "exit_broker_submitted_at": [
                                (row["link"].get("broker") or {}).get("submitted_at")
                                for row in rows
                                if row["side"] == "sell"
                            ],
                            "exit_broker_filled_at": [
                                (row["link"].get("broker") or {}).get("filled_at")
                                for row in rows
                                if row["side"] == "sell"
                            ],
                            "flat_verified_at": datetime.now(UTC).isoformat(),
                            "timing_guaranteed": False,
                        },
                    }
                    runtime.repo.set_control(terminal_key, json.dumps(state, default=str))
                    runtime.store.audit("operator_paper_result", state, now, request.cohort_id)
                    _notice(runtime, request, state)
                    return state
            return {"state": "supervising", "_operator_active": True}
        if not authorized(
            runtime.repo, request, settings, config_hash, request.code_sha, now, runtime.app
        ):
            raise ValueError("operator authority expired or control versions changed")
        if runtime.broker.positions() or runtime.broker.open_orders():
            raise ValueError("flat account without open orders required")
        history = runtime.broker.account_history()
        if (
            history["account_digest"] != request.account_digest
            or history_identity(history) != request.history_sha256
        ):
            raise ValueError("broker history changed after explicit baseline review")
        baseline = save_baseline(runtime.store, history, datetime.now(UTC))
        now = datetime.now(UTC)
        context = runtime.context
        if (
            context.errors
            or context.blocking_reasons(now=now)
            or context.halted_for("AAPL", now=now) is not False
            or not runtime._sources_fresh(now)
            or any(
                now - timedelta(minutes=runtime.policy.macro_blackout_minutes)
                <= at
                <= now + timedelta(minutes=1 + runtime.policy.macro_blackout_minutes)
                for at in context.scheduled_macro_events
            )
        ):
            raise ValueError("fresh news/official context, clear halt and macro window required")
        market = runtime.market_states.get("AAPL")
        gate = manager.calendar.gate(now)
        if (
            market is None
            or market.completed_bar is None
            or gate.session_open is None
            or market.completed_bar.timestamp < gate.session_open + timedelta(minutes=5)
            or market.completed_bar.timestamp > now
            or market.pre_event_volatility_bps is None
            or market.completed_daily_sessions is None
            or market.completed_daily_sessions < runtime.policy.minimum_liquidity_sessions
        ):
            raise ValueError("completed regular-session observation and liquidity history required")
        proof = certificate(
            settings,
            config_hash=config_hash,
            code_sha=request.code_sha,
            account_id=runtime.broker.account().id,
            checks=request.checks,
            now=now,
            limitations=(
                "Explicit operator test; no news thesis, expected edge or qualification.",
            ),
        )
        poll_provenance = {
            key: runtime.source.last_poll_stats.get(key)
            for key in ("poll_id", "observed_at", "coverage_complete", "coverage_watermark")
        }
        quote = runtime._refresh_quote("AAPL")
        now = datetime.now(UTC)
        eligible_at = max(
            request.approved_at,
            market.observed_at + timedelta(seconds=runtime.policy.processing_latency_seconds),
        )
        result = manager.submit_entry(
            symbol="AAPL",
            cluster_key=f"opening-calibration:{request.session_date}:AAPL",
            decision_id=str(request.request_id),
            eligible_at=eligible_at,
            expires_at=min(request.entry_deadline, now + timedelta(seconds=30)),
            bid=quote.bid,
            ask=quote.ask,
            quote_at=quote.quote_at,
            median_dollar_volume=market.median_daily_dollar_volume or Decimal(0),
            source_valid=True,
            certificate=proof,
            now=now,
            execution_quote=_execution_quote(quote),
            entry_kind="calibration",
            decision_ticket={
                "classification": "OPERATOR_EQUIPMENT_TEST",
                "qualification_eligible": False,
                "source": "explicit owner request; NOT a news-based entry",
                "news_assessment": "existing worker collection and official risk context checked",
                "news_receipt_at": runtime.last_source_success.isoformat(),
                "news_poll": poll_provenance,
                "selected_news_evidence_ids": [],
                "observation_available_at": market.observed_at.isoformat(),
                "eligible_at": eligible_at.isoformat(),
                "context": context.model_dump(mode="json"),
                "baseline_identity": baseline["identity"],
                "expected_net_return_bps": None,
                "probability_of_profit": None,
                "operator_scope": request.model_dump(mode="json"),
            },
        )
        if result["state"] in {"risk_rejected", "duplicate_event", "expired", "rejected"}:
            attempted = runtime.store.linked_orders(request.cohort_id)
            if manager.inventory(attempted) or any(row["status"] not in FINAL for row in attempted):
                manager.pause("OPERATOR_TERMINAL_RESPONSE_REQUIRES_RECOVERY", now)
                return {**result, "_operator_active": True}
            state = {
                "state": "blocked_or_unfilled",
                "result": result,
                "actual_filled_round_trip": False,
            }
            runtime.repo.set_control(terminal_key, json.dumps(state))
            _notice(runtime, request, state)
            return state
        if result["state"] == "submission_outcome_unknown":
            _notice(runtime, request, {**result, "actual_filled_round_trip": False})
        return {**result, "_operator_active": True}
    except (ValueError, httpx.HTTPError) as error:
        state = {
            "state": "blocked_requires_review",
            "reason": str(error)[:500],
            "actual_filled_round_trip": False,
        }
        if existing_rows or (manager and runtime.store.linked_orders(request.cohort_id)):
            if manager:
                manager.pause("OPERATOR_REQUEST_ERROR", now)
            state["_operator_active"] = True
        else:
            runtime.repo.set_control(terminal_key, json.dumps(state))
        runtime.store.audit("operator_paper_blocked", state, now, request.cohort_id)
        _notice(runtime, request, state)
        return state


def step(runtime: Any) -> dict[str, Any] | None:
    try:
        return _step(runtime)
    except (ValueError, KeyError, TypeError) as error:
        raw = runtime.repo.get_control("operator-paper-active") or runtime.repo.get_control(
            f"operator-paper-request:{runtime.settings.cohort_id}"
        )
        identity = sha256((raw or "").encode()).hexdigest()
        state = {
            "state": "operator_command_invalid_requires_review",
            "error": type(error).__name__,
            "_operator_active": True,
        }
        RoundTripNotificationRepository(runtime.store.database).enqueue_status(
            uuid5(NAMESPACE_URL, f"{PROTOCOL}:invalid:{identity}"),
            {
                "subject": "[TradeAgent PAPER operator test] Invalid command; review required",
                "text": json.dumps(state)
                + "\nNo broker-flat or execution claim. Normal pauses unchanged.",
            },
            created_at=datetime.now(UTC),
        )
        return state
