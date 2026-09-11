"""One dated owner approval; genuine worker observations delegate one existing paper OMS run."""

from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime, time, timedelta
from hashlib import sha256
from typing import Any, Literal, Self, cast
from uuid import NAMESPACE_URL, UUID, uuid5

import httpx
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import case, exists, func, literal, or_, select

from tradeagent.event_demo import _write_control
from tradeagent.event_session import (
    locked_session_budget,
    save_session_budget,
    session_control_key,
)
from tradeagent.event_store import EventStore, event_cohorts, event_order_links
from tradeagent.notifications import RoundTripNotificationRepository
from tradeagent.operator_calibration import OperatorPaperRequest, control_versions
from tradeagent.paper_account_history import (
    EASTERN,
    history_identity,
    save_baseline,
    stamp,
    value_history,
)
from tradeagent.persistence import (
    ProductionRepository,
    controls,
    heartbeats,
    market_quotes,
    orders,
    worker_locks,
)
from tradeagent.shadow_health import read_shadow_recorder_snapshot

PROTOCOL = "scheduled-owner-paper-v1"
RECORDER_SYMBOLS = ("SPY", "QQQ", "IWM", "TLT", "GLD")


class ScheduledPaperSession(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    session_id: UUID
    nonce: UUID
    worker_cohort_id: str = Field(min_length=1, max_length=51)
    worker_config_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    code_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    account_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    session_date: date
    approved_at: AwareDatetime
    not_before: AwareDatetime
    entry_deadline: AwareDatetime
    owner_authority: str = Field(min_length=20, max_length=1000)
    reviewed_history: dict[str, Any]
    reviewed_history_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    nightly_acceptance_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    nightly_acceptance_at: AwareDatetime
    checks: dict[str, bool]
    control_versions: dict[str, tuple[str | None, str | None]]
    action: Literal["one_paper_AAPL_round_trip"] = "one_paper_AAPL_round_trip"
    maximum_notional_usd: Literal[25] = 25
    maximum_scheduled_entries: Literal[1] = 1
    maximum_account_day_buys: Literal[2] = 2
    continuous_readiness_seconds: Literal[1800] = 1800
    maximum_observation_gap_seconds: Literal[90] = 90
    physical_progress_window_seconds: Literal[600] = 600
    maximum_physical_progress_window_seconds: Literal[1200] = 1200
    readiness_certificate_ttl_seconds: Literal[90] = 90
    maximum_process_rss_mib: Literal[400] = 400
    infrastructure_capacity_basis: Literal["reviewed-nightly-not-live-market-metrics"] = (
        "reviewed-nightly-not-live-market-metrics"
    )

    @property
    def request_id(self) -> UUID:
        return uuid5(self.session_id, str(self.nonce))

    @property
    def operator_cohort_id(self) -> str:
        return "operator-paper-" + self.request_id.hex

    @property
    def identity(self) -> str:
        return sha256(self.model_dump_json().encode()).hexdigest()

    @property
    def key(self) -> str:
        return f"scheduled-paper:{self.session_id}"

    @model_validator(mode="after")
    def validate_authority(self) -> Self:
        versions = self.control_versions
        if (
            self.approved_at.astimezone(EASTERN).date() + timedelta(days=1) != self.session_date
            or self.not_before.astimezone(EASTERN)
            != datetime.combine(self.session_date, time(10, 5), EASTERN)
            or self.entry_deadline.astimezone(EASTERN)
            != datetime.combine(self.session_date, time(10, 30), EASTERN)
            or self.nightly_acceptance_at.astimezone(EASTERN).date()
            != self.approved_at.astimezone(EASTERN).date()
            or not all(
                self.checks.get(key) is True
                for key in (
                    "reviewed_release",
                    "reviewed_exact_host_pause",
                    "nightly_resource_acceptance",
                    "owner_tomorrow_authorization",
                    "complete_history_review",
                )
            )
            or set(versions)
            != {"kill_switch", f"{self.worker_cohort_id}:pause", f"{self.operator_cohort_id}:pause"}
            or versions["kill_switch"][0] != "active"
            or versions["kill_switch"][1] is None
            or versions[f"{self.operator_cohort_id}:pause"] != (None, None)
            or any((value is None) != (at is None) for value, at in versions.values())
            or self.reviewed_history.get("account_digest") != self.account_digest
            or history_identity(self.reviewed_history) != self.reviewed_history_sha256
            or stamp(self.reviewed_history["observed_at"]).astimezone(EASTERN).date()
            != self.approved_at.astimezone(EASTERN).date()
        ):
            raise ValueError("exact dated, reviewed, paper-only owner authority required")
        value_history(self.reviewed_history)
        return self


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _get(connection: Any, key: str) -> str | None:
    return cast(
        str | None,
        connection.scalar(select(controls.c.control_value).where(controls.c.control_key == key)),
    )


def _pointer(session: ScheduledPaperSession, published_at: datetime) -> str:
    return json.dumps(
        {
            "session_id": str(session.session_id),
            "sha256": session.identity,
            "published_at": _utc(published_at).isoformat(),
        },
        sort_keys=True,
    )


def _pinned(connection: Any, session: ScheduledPaperSession) -> bool:
    for key, (value, at) in session.control_versions.items():
        row = (
            connection.execute(select(controls).where(controls.c.control_key == key))
            .mappings()
            .one_or_none()
        )
        if value is None:
            if row is not None:
                return False
        elif row is None or row["control_value"] != value or _utc(row["updated_at"]) != stamp(at):
            return False
    return True


def _lease(connection: Any, owner: str | None, now: datetime) -> bool:
    row = (
        connection.execute(
            select(worker_locks)
            .where(worker_locks.c.lock_name == "tradeagent-event-worker")
            .with_for_update()
        )
        .mappings()
        .one_or_none()
    )
    return bool(
        row
        and (owner is None or row["owner_id"] == owner)
        and timedelta(0) <= now - _utc(row["acquired_at"]) <= timedelta(seconds=90)
    )


def publish_authority(store: EventStore, session: ScheduledPaperSession, *, now: datetime) -> str:
    """Called tonight after human review; never edits prior authority, pauses or budgets."""
    if (
        not session.approved_at <= now < session.not_before
        or now.astimezone(EASTERN).date() != session.approved_at.astimezone(EASTERN).date()
        or not timedelta(0) <= now - session.nightly_acceptance_at <= timedelta(minutes=15)
        or not timedelta(0)
        <= now - stamp(session.reviewed_history["observed_at"])
        <= timedelta(minutes=15)
    ):
        raise ValueError("publish tonight with actual owner approval and fresh nightly reviews")
    with store.database.begin() as connection:
        if not _lease(connection, None, now) or not _pinned(connection, session):
            raise ValueError("current host lease or pinned controls changed")
        host = (
            connection.execute(
                select(heartbeats, worker_locks.c.owner_id)
                .join(worker_locks, worker_locks.c.lock_name == heartbeats.c.service_name)
                .where(heartbeats.c.service_name == "tradeagent-event-worker")
            )
            .mappings()
            .one_or_none()
        )
        if (
            host is None
            or host["instance_id"] != host["owner_id"]
            or not timedelta(0) <= now - _utc(host["observed_at"]) <= timedelta(seconds=90)
            or any(
                host["details"].get(key) != value
                for key, value in {
                    "cohort_id": session.worker_cohort_id,
                    "config_hash": session.worker_config_hash,
                    "code_sha": session.code_sha,
                    "mode": "experimental-paper",
                    "entry_policy": "scheduled-operator",
                }.items()
            )
        ):
            raise ValueError("publication requires the actual current scheduled host heartbeat")
        frozen = (
            connection.execute(
                select(event_cohorts).where(event_cohorts.c.cohort_id == session.worker_cohort_id)
            )
            .mappings()
            .one()
        )
        settings = frozen["manifest"]["settings"]
        if (
            frozen["config_hash"] != session.worker_config_hash
            or frozen["manifest"]["code_sha"] != session.code_sha
            or settings["entry_policy"] != "scheduled-operator"
            or settings["mode"] != "experimental-paper"
            or settings["practice_start_date"] != str(session.session_date)
            or settings["news_account_digest"] != session.account_digest
            or settings["symbols"] != "AAPL"
        ):
            raise ValueError("authority requires the exact frozen scheduled paper host")
        pointer = f"scheduled-paper-host:{session.worker_cohort_id}"
        if _get(connection, session.key) is not None:
            published_at = connection.scalar(
                select(controls.c.updated_at).where(controls.c.control_key == session.key)
            )
            if published_at is None:
                raise ValueError("scheduled publication record disappeared")
            if _get(connection, session.key) == session.model_dump_json() and _get(
                connection, pointer
            ) == _pointer(session, published_at):
                return session.identity
            raise ValueError("immutable scheduled authority already exists")
        if any(
            _get(connection, key) is not None
            for key in (
                pointer,
                "operator-paper-active",
                f"operator-paper-request:{session.worker_cohort_id}",
                f"{session.operator_cohort_id}:operator-scope",
                f"{session.operator_cohort_id}:operator-terminal",
            )
        ):
            raise ValueError(
                "existing schedule, operator authority or terminal cannot be overwritten"
            )
        _write_control(connection, session.key, session.model_dump_json(), now)
        _write_control(connection, pointer, _pointer(session, now), now)
        store.audit(
            "scheduled_paper_approved",
            session.model_dump(mode="json"),
            now,
            session.key,
            connection,
        )
    return session.identity


def load_session(repo: ProductionRepository, cohort: str) -> ScheduledPaperSession | None:
    identity = repo.get_control(f"scheduled-paper-host:{cohort}")
    if identity is None:
        return None
    pointer = json.loads(identity)
    key = f"scheduled-paper:{UUID(pointer['session_id'])}"
    raw = repo.get_control(key)
    if raw is None:
        raise ValueError("scheduled authority pointer has no immutable authority")
    with repo._database.begin() as connection:
        published_at = connection.scalar(
            select(controls.c.updated_at).where(controls.c.control_key == key)
        )
    session = ScheduledPaperSession.model_validate_json(raw)
    if published_at is None:
        raise ValueError("scheduled publication record disappeared")
    if identity != _pointer(session, published_at):
        raise ValueError("scheduled authority content or publication version changed")
    return session


def delegation_conditions(repo: ProductionRepository, request: OperatorPaperRequest) -> list[Any]:
    key = f"scheduled-paper:{request.scheduled_session_id}"
    raw = repo.get_control(key)
    if raw is None or sha256(raw.encode()).hexdigest() != request.scheduled_session_sha256:
        return [literal(False)]
    try:
        session = ScheduledPaperSession.model_validate_json(raw)
    except ValueError:
        return [literal(False)]
    if (
        request.request_id != session.request_id
        or request.approved_at != session.approved_at
        or request.not_before != session.not_before
        or request.entry_deadline != session.entry_deadline
        or request.control_versions != session.control_versions
        or request.worker_config_hash != session.worker_config_hash
        or request.worker_cohort_id != session.worker_cohort_id
        or request.code_sha != session.code_sha
        or request.account_digest != session.account_digest
    ):
        return [literal(False)]
    proof = repo.get_control(f"{key}:readiness-certificate")
    if proof is None or sha256(proof.encode()).hexdigest() != request.acceptance_sha256:
        return [literal(False)]
    conditions: list[Any] = [
        exists(
            select(controls.c.control_key).where(
                controls.c.control_key == f"{key}:readiness-certificate",
                controls.c.control_value == proof,
            )
        ),
        exists(
            select(controls.c.control_key).where(
                controls.c.control_key == key,
                controls.c.control_value == raw,
                controls.c.updated_at == stamp(str(request.scheduled_authority_updated_at)),
            )
        ),
        exists(
            select(controls.c.control_key).where(
                controls.c.control_key == f"scheduled-paper-host:{session.worker_cohort_id}",
                controls.c.control_value
                == _pointer(session, stamp(str(request.scheduled_authority_updated_at))),
            )
        ),
        exists(
            select(controls.c.control_key).where(
                controls.c.control_key == f"{key}:delegation",
                controls.c.control_value == request.model_dump_json(),
            )
        ),
    ]
    conditions.extend(
        ~exists(select(controls.c.control_key).where(controls.c.control_key == f"{key}:{suffix}"))
        for suffix in ("terminal", "revoked")
    )
    return conditions


def delegation_valid(repo: ProductionRepository, request: OperatorPaperRequest) -> bool:
    conditions = delegation_conditions(repo, request)
    with repo._database.begin() as connection:
        return bool(connection.scalar(select(literal(True)).where(*conditions)))


def validate_history_extension(
    session: ScheduledPaperSession, history: dict[str, Any], now: datetime
) -> dict[str, Any]:
    if history.get("account_digest") != session.account_digest or not timedelta(0) <= now - stamp(
        history["observed_at"]
    ) <= timedelta(seconds=60):
        raise ValueError("current complete history for the approved account required")
    baseline = session.reviewed_history
    for field in ("orders", "activities"):
        current = {item["id"]: item for item in history[field]}
        if any(current.get(item["id"]) != item for item in baseline[field]):
            raise ValueError("reviewed broker history was removed or mutated")
    previous_activities = {item["id"] for item in baseline["activities"]}
    if any(
        item["id"] not in previous_activities
        and item["activity_type"] not in {"FILL", "CFEE", "FEE"}
        for item in history["activities"]
    ):
        raise ValueError("new external credits or unreviewed activity are forbidden")
    observed = stamp(history["observed_at"])
    for order in history["orders"]:
        for key in (
            "created_at",
            "submitted_at",
            "updated_at",
            "filled_at",
            "canceled_at",
            "expired_at",
        ):
            if order.get(key) and stamp(order[key]) > observed:
                raise ValueError("future broker order history")
    return value_history(history)


def process_rss_mib() -> float:
    # Production is Linux. Missing OS telemetry is a blocker, never a passing default.
    if os.name != "posix":
        raise ValueError("local process RSS telemetry unavailable")
    from importlib import import_module

    resource = import_module("resource")
    return float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) / 1024


def prearm_baseline(
    store: EventStore,
    broker: Any,
    *,
    reviewed_code_sha: str,
    expected_account_digest: str,
    reviewed_history_sha256: str,
    persist: bool = False,
) -> dict[str, Any]:
    """Same-image preflight: broker GETs only; never arms or invokes an OMS."""
    from tradeagent.event_doctor import code_identity
    from tradeagent.experimental_policy import reject_live_environment
    from tradeagent.paper_account_history import preview_baseline

    reject_live_environment()
    actual_code = code_identity()
    if (
        len(reviewed_code_sha) != 40
        or actual_code != reviewed_code_sha
        or broker.broker_host != "https://paper-api.alpaca.markets"
    ):
        raise ValueError("the exact reviewed code on the paper broker is required")
    with store.database.begin() as connection:
        if (
            connection.scalar(
                select(controls.c.control_key)
                .where(
                    (controls.c.control_key == "operator-paper-active")
                    | controls.c.control_key.startswith("operator-paper-request:", autoescape=True)
                )
                .limit(1)
            )
            is not None
        ):
            raise ValueError("active operator commands forbid pre-arm baseline import")

    def confirm_flat_account() -> None:
        account = broker.account()
        if (
            sha256(account.id.encode()).hexdigest() != expected_account_digest
            or account.status != "ACTIVE"
            or account.trading_blocked
            or account.account_blocked
            or broker.positions()
            or broker.open_orders()
        ):
            raise ValueError("the pinned active flat paper account without open orders is required")

    confirm_flat_account()
    history = broker.account_history()
    if (
        history.get("account_digest") != expected_account_digest
        or history_identity(history) != reviewed_history_sha256
    ):
        raise ValueError("current broker history differs from the exact reviewed snapshot")
    confirm_flat_account()
    now = datetime.now(UTC)
    preview = preview_baseline(store, history, now)
    result = {
        "kind": "reviewed_same_image_paper_baseline_preflight",
        "code_sha": actual_code,
        "broker_host": broker.broker_host,
        "account_digest": expected_account_digest,
        "flat_verified_at": now.isoformat(),
        "preview": preview,
        "persisted": False,
        "broker_orders_submitted": 0,
        "schedule_authority_created": False,
        "kill_and_pause_controls_written": False,
        "market_acceptance": False,
    }
    if persist:
        report = save_baseline(store, history, now)
        result["persisted"] = True
        result["baseline_identity"] = report["identity"]
        result["after_import_preview"] = preview_baseline(store, history, datetime.now(UTC))
        store.audit("paper_baseline_prearm_import", result, now, expected_account_digest)
    return result


def readiness_account_risk(
    runtime: Any, session: ScheduledPaperSession, now: datetime
) -> dict[str, Any]:
    from tradeagent.event_account_risk import account_risk

    cached = getattr(runtime, "_scheduled_readiness_history", None)
    if (
        cached is None
        or cached[0] != session.identity
        or not timedelta(0) <= now - stamp(cached[1]["observed_at"]) < timedelta(seconds=45)
    ):
        history = runtime.broker.account_history()
        now = datetime.now(UTC)
        validate_history_extension(session, history, now)
        baseline = save_baseline(runtime.store, history, now)
        cached = (session.identity, history, now, baseline)
        runtime._scheduled_readiness_history = cached
    risk = account_risk(runtime.store, runtime.settings, {}, now)
    raw_budget = runtime.repo.get_control(
        session_control_key(session.account_digest, session.session_date)
    )
    budget = json.loads(raw_budget) if raw_budget else {}
    reserved = (
        budget.get("scheduled_request_id") == str(session.request_id)
        and budget.get("scheduled_entry_reserved") is True
    )
    entries = budget.get("total_entries_reserved", 0)
    return {
        "history_identity": history_identity(cached[1]),
        "history_observed_at": cached[1]["observed_at"],
        "history_checked_at": cached[2].isoformat(),
        "history_max_age_seconds": 60,
        "baseline_identity": cached[3]["identity"],
        "account_day_entries_including_reservations": entries,
        "reservation_owned_by_this_schedule": reserved,
        "account_day_capacity": entries - int(reserved) < session.maximum_account_day_buys,
        "economic_risk": risk,
    }


def collect_readiness(
    runtime: Any,
    session: ScheduledPaperSession,
    now: datetime,
    since: datetime,
    *,
    include_physical: bool = True,
) -> dict[str, Any]:
    from tradeagent.market_progress import market_window_counts

    if include_physical and not timedelta(0) <= datetime.now(UTC) - since <= timedelta(minutes=25):
        raise ValueError("physical progress queries require a bounded current window")
    context = runtime.context
    market = runtime.market_states.get("AAPL")
    account = runtime.broker.account()
    plan = runtime.session_plan
    asset = runtime.broker.asset("AAPL")
    broker_clock = runtime.broker.clock()
    flat = not runtime.broker.positions() and not runtime.broker.open_orders()
    risk = (
        readiness_account_risk(runtime, session, datetime.now(UTC))
        if flat and sha256(account.id.encode()).hexdigest() == session.account_digest
        else None
    )
    physical = (
        market_window_counts(runtime.store.database, since=since, until=datetime.now(UTC))
        if include_physical
        else None
    )
    recorder = read_shadow_recorder_snapshot(
        runtime.repo,
        batch_since=datetime.now(UTC) - timedelta(seconds=10),
        clock=lambda: datetime.now(UTC),
    )
    hb, lease, batch = recorder.heartbeat, recorder.lease, recorder.batch
    details = hb["details"] if hb else {}
    quote_scan_at = datetime.now(UTC)
    with runtime.store.database.begin() as connection:
        recorder_quote = (
            connection.execute(
                select(
                    market_quotes.c.symbol,
                    market_quotes.c.event_at,
                    market_quotes.c.received_at,
                    market_quotes.c.processed_at,
                )
                .where(
                    market_quotes.c.symbol.in_(RECORDER_SYMBOLS),
                    market_quotes.c.event_at >= quote_scan_at - timedelta(seconds=10),
                    market_quotes.c.event_at <= quote_scan_at,
                )
                .order_by(market_quotes.c.event_at.desc())
                .limit(1)
            )
            .mappings()
            .one_or_none()
        )
    quote = runtime._refresh_quote("AAPL")
    stream = runtime.order_stream.health_snapshot() if runtime.order_stream else {}
    now = datetime.now(UTC)
    rss = process_rss_mib()
    checks = {
        "regular_session": bool(
            plan
            and plan.session_date == session.session_date
            and plan.session_open <= now < plan.session_close
            and broker_clock.is_open
            and timedelta(0) <= now - broker_clock.timestamp <= timedelta(seconds=60)
        ),
        "paper_account": runtime.broker.broker_host == "https://paper-api.alpaca.markets"
        and sha256(account.id.encode()).hexdigest() == session.account_digest
        and account.status == "ACTIVE"
        and not account.trading_blocked
        and not account.account_blocked,
        "flat_account": flat,
        "account_risk": bool(
            risk
            and risk["economic_risk"].get("state") == "valued"
            and risk["economic_risk"].get("blocked") is False
            and risk["account_day_capacity"]
            and timedelta(0) <= now - stamp(risk["history_observed_at"]) <= timedelta(seconds=60)
        ),
        "source": runtime._sources_fresh(now)
        and not runtime.source.last_errors
        and runtime.source.last_poll_stats.get("coverage_complete") is True,
        "official_context": bool(
            context
            and not context.errors
            and timedelta(0) <= now - context.observed_at <= timedelta(seconds=90)
            and not context.blocking_reasons(now=now)
            and context.halted_for("AAPL", now=now) is False
            and not any(
                now - timedelta(minutes=30) <= at <= now + timedelta(minutes=31)
                for at in context.scheduled_macro_events
            )
        ),
        "quote": bool(
            quote.ask >= quote.bid > 0
            and timedelta(0) <= now - quote.quote_at <= timedelta(seconds=5)
            and (quote.ask - quote.bid) / ((quote.ask + quote.bid) / 2) * 10000 <= 10
        ),
        "stream": all(
            stream.get(key) is True
            for key in ("running", "connected", "authenticated", "subscribed")
        )
        and stream.get("gap_started_at") is None
        and stream.get("dropped_updates") == 0
        and stream.get("invalid_messages") == 0,
        "recorder": bool(
            hb
            and lease
            and batch
            and hb["instance_id"] == lease["owner_id"]
            and timedelta(0) <= now - _utc(hb["observed_at"]) <= timedelta(seconds=60)
            and timedelta(0) <= now - _utc(lease["acquired_at"]) <= timedelta(seconds=120)
            and details.get("healthy") is True
            and details.get("state") == "healthy"
            and details.get("persistence_error") is None
            and details.get("last_market_commit_at") is not None
            and timedelta(0)
            <= now - stamp(details["last_market_commit_at"])
            <= timedelta(seconds=10)
            and all(
                details.get(key, 0) == 0
                for key in ("dropped_events", "notice_overflow", "decision_errors")
            )
            and timedelta(0)
            <= now - stamp(batch["payload"]["last_event_at"])
            <= timedelta(seconds=10)
            and plan is not None
            and stamp(batch["payload"]["last_event_at"]) >= plan.session_open
        ),
        "recorder_quote": bool(
            recorder_quote
            and timedelta(0) <= now - _utc(recorder_quote["event_at"]) <= timedelta(seconds=10)
            and _utc(recorder_quote["event_at"])
            <= _utc(recorder_quote["received_at"])
            <= _utc(recorder_quote["processed_at"])
            <= now
        ),
        "process_memory": 0 < rss <= session.maximum_process_rss_mib,
        "asset_and_liquidity": bool(
            asset.symbol == "AAPL"
            and asset.status == "active"
            and asset.tradable
            and asset.fractionable
            and asset.asset_class == "us_equity"
            and asset.exchange in {"NYSE", "NASDAQ", "AMEX", "ARCA"}
            and market
            and market.median_daily_dollar_volume is not None
            and market.median_daily_dollar_volume >= 50_000_000
            and market.completed_daily_sessions is not None
            and market.completed_daily_sessions >= runtime.policy.minimum_liquidity_sessions
        ),
    }
    with runtime.store.database.begin() as connection:
        checks["lease"] = _lease(connection, runtime.instance_id, now)
    result: dict[str, Any] = json.loads(
        json.dumps(
            {
                "observed_at": now.isoformat(),
                "worker_owner": runtime.instance_id,
                "worker_code_sha": runtime.code_sha,
                "worker_config_hash": runtime.config_hash,
                "checks": checks,
                "process_peak_rss_mib": rss,
                "market_count_scope": {
                    "since_exchange_at": since.isoformat(),
                    "until_exchange_at": now.isoformat(),
                    "symbols": list(RECORDER_SYMBOLS),
                }
                if include_physical
                else None,
                "market": physical,
                "physical_snapshot_collected": include_physical,
                "physical_progress_basis": (
                    "All five quote/trade/bar series assessed at bounded >=10-minute endpoints; "
                    "no per-minute print or bar freshness claim."
                ),
                "recorder_batch_id": str(batch["event_id"]) if batch else None,
                "recorder_owner": hb["instance_id"] if hb else None,
                "recorder_gap_count": details.get("gaps"),
                "recorder_last_exchange_at": batch["payload"]["last_event_at"] if batch else None,
                "recorder_reported_commit_at": details.get("last_market_commit_at"),
                "latest_durable_recorder_quote": dict(recorder_quote) if recorder_quote else None,
                "stream": stream,
                "source_poll": runtime.source.last_poll_stats,
                "account_risk_evidence": risk,
                "infrastructure_capacity_basis": session.infrastructure_capacity_basis,
                "nightly_acceptance_sha256": session.nightly_acceptance_sha256,
                "nightly_infrastructure_evidence": {
                    "kind": "reviewed_afterhours_deployment_and_resource_attestation",
                    "observed_at": session.nightly_acceptance_at.isoformat(),
                    "sha256": session.nightly_acceptance_sha256,
                    "scope": (
                        "reviewed nightly deployment/resource capacity; not morning cloud metrics"
                    ),
                    "morning_render_or_postgres_memory_observed": False,
                },
            },
            default=lambda value: (
                _utc(value).isoformat() if hasattr(value, "tzinfo") else str(value)
            ),
        )
    )
    return result


def _physical_anchor(runtime: Any, since: datetime) -> dict[str, Any]:
    from tradeagent.market_progress import market_window_counts

    rows = market_window_counts(runtime.store.database, since=since, until=datetime.now(UTC))
    observed_at = datetime.now(UTC)
    result: dict[str, Any] = json.loads(
        json.dumps(
            {
                "observed_at": observed_at.isoformat(),
                "market_count_scope": {
                    "since_exchange_at": since.isoformat(),
                    "until_exchange_at": observed_at.isoformat(),
                    "symbols": list(RECORDER_SYMBOLS),
                },
                "market": rows,
            },
            default=lambda value: (
                _utc(value).isoformat() if hasattr(value, "tzinfo") else str(value)
            ),
        )
    )
    return result


def _physical_endpoint(sample: dict[str, Any]) -> dict[str, Any]:
    return {key: sample[key] for key in ("observed_at", "market_count_scope", "market")}


def _observation_identity(runtime: Any, sample: dict[str, Any]) -> dict[str, Any]:
    stream = sample.get("stream", {})
    return {
        "worker_owner": runtime.instance_id,
        "worker_code_sha": runtime.code_sha,
        "worker_config_hash": runtime.config_hash,
        "recorder_owner": sample.get("recorder_owner"),
        "recorder_gap_count": sample.get("recorder_gap_count"),
        "stream_connections": stream.get("connections"),
        "stream_gaps": stream.get("gap_count"),
        "stream_disconnects": stream.get("disconnects"),
    }


def physical_coverage_valid(windows: list[dict[str, Any]], now: datetime) -> bool:
    from tradeagent.market_progress import physical_progress_failures

    if not windows:
        return False
    try:
        covered = timedelta(0)
        previous_end = None
        for window in windows:
            first, last = window["first"], window["last"]
            start = stamp(first["market_count_scope"]["since_exchange_at"])
            end = stamp(last["market_count_scope"]["until_exchange_at"])
            if (
                physical_progress_failures(first, last)
                or end - stamp(first["market_count_scope"]["until_exchange_at"])
                < timedelta(seconds=600)
                or (previous_end is not None and start != previous_end)
                or end > now
            ):
                return False
            covered += end - start
            previous_end = end
        return bool(
            previous_end
            and covered >= timedelta(seconds=1800)
            and timedelta(0) <= now - previous_end <= timedelta(seconds=90)
        )
    except (KeyError, TypeError, ValueError):
        return False


def _entry_observation_valid(runtime: Any, now: datetime) -> bool:
    market = runtime.market_states.get("AAPL")
    plan = runtime.session_plan
    if not (
        market
        and market.completed_bar
        and plan
        and plan.session_open + timedelta(minutes=5) <= market.completed_bar.timestamp <= now
        and market.pre_event_volatility_bps is not None
    ):
        return False
    receipt = runtime.first_bar_receipts.get(("AAPL", market.completed_bar.timestamp))
    return bool(
        receipt
        and receipt <= now
        and now >= receipt + timedelta(seconds=runtime.policy.processing_latency_seconds)
    )


def owned_execution_pending(runtime: Any) -> bool:
    """Entry-free selector after recovery; command pointers are not ownership evidence."""
    from tradeagent.event_orders import FINAL

    query = (
        select(event_order_links.c.cohort_id)
        .join(orders, orders.c.client_order_id == event_order_links.c.client_order_id)
        .join(event_cohorts, event_cohorts.c.cohort_id == event_order_links.c.cohort_id)
        .where(
            or_(
                event_order_links.c.cohort_id == runtime.settings.cohort_id,
                event_order_links.c.payload["account_digest"].as_string()
                == runtime.settings.news_account_digest,
                event_cohorts.c.manifest["settings"]["entry_policy"].as_string()
                == "operator-calibration",
            )
        )
        .group_by(event_order_links.c.cohort_id, orders.c.symbol)
        .having(
            or_(
                func.sum(case((orders.c.status.not_in(FINAL), 1), else_=0)) > 0,
                func.sum(
                    case(
                        (orders.c.side == "buy", orders.c.filled_quantity),
                        else_=-orders.c.filled_quantity,
                    )
                )
                != 0,
            )
        )
        .limit(1)
    )
    with runtime.store.database.begin() as connection:
        return connection.scalar(query) is not None


def _notice(
    runtime: Any, session: ScheduledPaperSession, state: str, detail: dict[str, Any], now: datetime
) -> None:
    RoundTripNotificationRepository(runtime.store.database).enqueue_status(
        uuid5(NAMESPACE_URL, f"{PROTOCOL}:{session.session_id}:{state}"),
        {
            "subject": f"[TradeAgent PAPER scheduled] {state}",
            "text": "One dated AAPL paper equipment round trip; no news strategy or edge claim.\n"
            f"Approved: {session.approved_at.isoformat()}\n"
            f"Entry window: {session.not_before.isoformat()} "
            f"to {session.entry_deadline.isoformat()} exclusive.\n"
            "Global strategy kill remains active; only the exact pinned operator exception "
            "is authorized.\n" + json.dumps(detail, sort_keys=True, default=str),
            "scheduled_session_id": str(session.session_id),
            "qualification_eligible": False,
        },
        created_at=now,
    )


def _terminal(
    runtime: Any, session: ScheduledPaperSession, reason: str, now: datetime
) -> dict[str, Any]:
    state = {"state": "MISSED", "reason": reason, "actual_filled_round_trip": False}
    key = f"{session.key}:terminal"
    if runtime.repo.get_control(key) is None:
        runtime.repo.set_control(key, json.dumps(state))
        runtime.store.audit("scheduled_paper_terminal", state, now, session.key)
    _notice(runtime, session, "MISSED", state, now)
    return state


def _issue(
    runtime: Any, session: ScheduledPaperSession, proof: dict[str, Any], now: datetime
) -> OperatorPaperRequest:
    history = runtime.broker.account_history()
    now = datetime.now(UTC)
    validate_history_extension(session, history, now)
    if not physical_coverage_valid(proof.get("physical_progress_windows", []), now):
        raise ValueError("complete current aggregate physical readiness proof required")
    if not session.not_before <= now < session.entry_deadline:
        raise ValueError("entry deadline reached during readiness/history collection")
    baseline = save_baseline(runtime.store, history, now)
    with runtime.store.database.begin() as connection:
        if not _lease(connection, runtime.instance_id, now) or not _pinned(connection, session):
            raise ValueError("lease or reviewed controls changed during issuance")
        record = (
            connection.execute(
                select(controls).where(controls.c.control_key == session.key).with_for_update()
            )
            .mappings()
            .one()
        )
        if record["control_value"] != session.model_dump_json() or _get(
            connection, f"scheduled-paper-host:{session.worker_cohort_id}"
        ) != _pointer(session, record["updated_at"]):
            raise ValueError("immutable scheduled authority changed")
        if any(
            _get(connection, f"{session.key}:{suffix}") is not None
            for suffix in ("terminal", "revoked")
        ):
            raise ValueError("schedule terminal or revoked")
        prior = _get(connection, f"{session.key}:delegation")
        if prior is not None:
            return OperatorPaperRequest.model_validate_json(prior)
        if any(
            _get(connection, key) is not None
            for key in (
                "operator-paper-active",
                f"operator-paper-request:{session.worker_cohort_id}",
                f"{session.operator_cohort_id}:operator-terminal",
                f"{session.operator_cohort_id}:operator-scope",
            )
        ):
            raise ValueError("another operator request or terminal already exists")
        budget = locked_session_budget(
            connection, session.account_digest, session.session_date, now
        )
        if (
            budget["total_entries_reserved"] >= 2
            or budget["equipment_client_order_id"] is not None
            or budget.get("scheduled_request_id")
        ):
            raise ValueError("account-day or equipment budget already consumed")
        proof = {
            **proof,
            "issued_at": now.isoformat(),
            "history_sha256": history_identity(history),
            "scheduled_session_sha256": session.identity,
            "approved_at": session.approved_at.isoformat(),
            "owner_confirmation_observed_tomorrow": False,
        }
        proof_json = json.dumps(proof, sort_keys=True)
        request = OperatorPaperRequest(
            request_id=session.request_id,
            action=session.action,
            worker_cohort_id=session.worker_cohort_id,
            worker_config_hash=session.worker_config_hash,
            code_sha=session.code_sha,
            account_digest=session.account_digest,
            session_date=session.session_date,
            approved_at=session.approved_at,
            entry_deadline=session.entry_deadline,
            acceptance_sha256=sha256(proof_json.encode()).hexdigest(),
            history_sha256=history_identity(history),
            owner_authority=session.owner_authority,
            checks=dict.fromkeys(
                (
                    "reviewed_release",
                    "market_acceptance",
                    "account_history_complete",
                    "operator_confirmation",
                    "normal_entries_paused",
                ),
                True,
            ),
            control_versions=session.control_versions,
            scheduled_session_id=session.session_id,
            scheduled_session_sha256=session.identity,
            scheduled_authority_updated_at=_utc(record["updated_at"]).isoformat(),
            issued_at=now,
            not_before=session.not_before,
        )
        budget["total_entries_reserved"] += 1
        budget["scheduled_request_id"] = str(request.request_id)
        budget["scheduled_entry_reserved"] = True
        save_session_budget(connection, budget, now)
        for key in (
            f"{session.key}:delegation",
            f"operator-paper-request:{session.worker_cohort_id}",
            "operator-paper-active",
            f"{session.operator_cohort_id}:operator-scope",
        ):
            _write_control(connection, key, request.model_dump_json(), now)
        _write_control(connection, f"{session.key}:readiness-certificate", proof_json, now)
        runtime.store.audit(
            "scheduled_paper_readiness_certificate", proof, now, session.key, connection
        )
        runtime.store.audit(
            "scheduled_paper_delegated",
            request.model_dump(mode="json"),
            now,
            session.key,
            connection,
        )
    runtime._scheduled_history = (request.request_id, history, now, baseline)
    runtime._scheduled_readiness_history = (session.identity, history, now, baseline)
    return request


def step(
    runtime: Any, *, now: datetime | None = None, collect: bool = True
) -> dict[str, Any] | None:
    """Called after regular collection, never instead of owned exposure recovery."""
    from tradeagent.market_progress import physical_progress_failures

    now = now or datetime.now(UTC)
    if runtime.settings.entry_policy != "scheduled-operator":
        return None
    try:
        session = load_session(runtime.repo, runtime.settings.cohort_id)
    except (ValueError, KeyError, TypeError) as error:
        pointer = runtime.repo.get_control(f"scheduled-paper-host:{runtime.settings.cohort_id}")
        identity = sha256((pointer or "").encode()).hexdigest()
        state = {"state": "invalid_schedule_requires_review", "reason": str(error)[:500]}
        RoundTripNotificationRepository(runtime.store.database).enqueue_status(
            uuid5(NAMESPACE_URL, f"{PROTOCOL}:invalid:{identity}"),
            {
                "subject": "[TradeAgent PAPER scheduled] Invalid authority; no entry",
                "text": json.dumps(state) + "\nOwned exits remain supervised; no flat claim.",
            },
            created_at=now,
        )
        return state
    if session is None:
        return {"state": "not_authorized", "ordinary_entries_enabled": False}
    terminal = runtime.repo.get_control(f"{session.key}:terminal")
    if terminal:
        _notice(runtime, session, "MISSED", json.loads(terminal), now)
        return dict(json.loads(terminal))
    delegation = runtime.repo.get_control(f"{session.key}:delegation")
    if delegation:
        request = OperatorPaperRequest.model_validate_json(delegation)
        finished = runtime.repo.get_control(f"{request.cohort_id}:operator-terminal")
        if finished is not None:
            return {
                "state": "operator_finished",
                "result": json.loads(finished),
                "request_id": str(request.request_id),
                "repeated": False,
            }
        if collect and not runtime.store.linked_orders(request.cohort_id):
            if not request.in_window(now):
                return _terminal(
                    runtime, session, "issued readiness certificate expired unsent", now
                )
            try:
                current = collect_readiness(
                    runtime, session, now, now - timedelta(minutes=1), include_physical=False
                )
                proof_raw = runtime.repo.get_control(f"{session.key}:readiness-certificate")
                proof = json.loads(proof_raw) if proof_raw else {}
                if (
                    not all(current["checks"].values())
                    or not delegation_valid(runtime.repo, request)
                    or not physical_coverage_valid(
                        proof.get("physical_progress_windows", []),
                        stamp(current["observed_at"]),
                    )
                    or _observation_identity(runtime, current) != proof.get("observation_identity")
                ):
                    return _terminal(
                        runtime, session, "readiness changed before delegated entry", now
                    )
            except (ValueError, KeyError, TypeError, httpx.HTTPError) as error:
                return _terminal(
                    runtime, session, "readiness unavailable: " + str(error)[:200], now
                )
        return {"state": "delegated", "request_id": str(session.request_id), "repeated": False}
    if (
        session.code_sha != runtime.code_sha
        or session.worker_config_hash != runtime.config_hash
        or session.worker_cohort_id != runtime.settings.cohort_id
        or session.account_digest != runtime.settings.news_account_digest
        or session.session_date != runtime.settings.practice_start_date
        or runtime.settings.mode != "experimental-paper"
        or control_versions(runtime.repo, session.worker_cohort_id, session.operator_cohort_id)
        != session.control_versions
        or runtime.repo.get_control(f"{session.key}:revoked") is not None
        or runtime.repo.get_control(f"{session.operator_cohort_id}:operator-terminal") is not None
    ):
        return _terminal(
            runtime, session, "approved identity, control version or revocation changed", now
        )
    if now >= session.entry_deadline:
        progress = runtime.repo.get_control(f"{session.key}:progress")
        prior = json.loads(progress) if progress else {}
        failures = list(prior.get("failures", []))
        if prior.get("entry_blocker"):
            failures.append(prior["entry_blocker"])
        return _terminal(
            runtime, session, "entry deadline elapsed; last readiness: " + str(failures)[:500], now
        )
    _notice(
        runtime,
        session,
        "scheduled_ready",
        {"authority": session.identity, "market_readiness": "not_yet_certified"},
        now,
    )
    opening = datetime.combine(session.session_date, time(9, 30), EASTERN)
    if now < opening:
        return {
            "state": "scheduled_preopen",
            "market_readiness": False,
            "global_strategy_kill": "active",
            "operator_exception": "scheduled",
        }
    if not collect:
        return {"state": "awaiting_fresh_collection"}
    key = f"{session.key}:progress"
    raw = runtime.repo.get_control(key)
    progress = json.loads(raw) if raw else {}
    if not progress or not timedelta(0) <= now - stamp(progress["last_at"]) <= timedelta(
        seconds=90
    ):
        progress = {"started_at": None, "anchor": None, "physical_proofs": []}
    anchor = progress.get("anchor")
    since = stamp(anchor["market_count_scope"]["since_exchange_at"]) if anchor else now
    windows = progress.get("physical_proofs", [])
    same_window = bool(
        anchor
        and windows
        and windows[-1]["first"]["market_count_scope"]["since_exchange_at"]
        == anchor["market_count_scope"]["since_exchange_at"]
    )
    endpoint_age = (
        now - stamp(windows[-1]["last"]["market_count_scope"]["until_exchange_at"])
        if windows
        else timedelta.max
    )
    physical_due = bool(
        anchor
        and now - stamp(anchor["market_count_scope"]["until_exchange_at"])
        >= timedelta(seconds=session.physical_progress_window_seconds)
        and (
            not same_window
            or endpoint_age >= timedelta(seconds=600)
            or (now >= session.not_before and endpoint_age >= timedelta(seconds=60))
        )
    )
    try:
        sample = collect_readiness(runtime, session, now, since, include_physical=physical_due)
        now = stamp(sample["observed_at"])
        if now >= session.entry_deadline:
            return _terminal(runtime, session, "entry deadline elapsed during collection", now)
        previous_observation = progress.get("last_at")
        if previous_observation is not None and not timedelta(0) <= (
            now - stamp(previous_observation)
        ) <= timedelta(seconds=session.maximum_observation_gap_seconds):
            sample["continuity_reset"] = {
                "reason": "completed_observation_gap",
                "previous_observation_at": previous_observation,
                "gap_seconds": (now - stamp(previous_observation)).total_seconds(),
            }
            progress = {"started_at": None, "anchor": None, "physical_proofs": []}
            anchor = None
        failures = [name for name, passed in sample["checks"].items() if passed is not True]
        observation_identity = _observation_identity(runtime, sample)
        if progress.get("observation_identity") != observation_identity:
            progress = {"started_at": None, "anchor": None, "physical_proofs": []}
            anchor = None
        windows = progress.get("physical_proofs", [])
        if not failures and anchor and physical_due:
            failures.extend(physical_progress_failures(anchor, sample))
            if not failures:
                proof_window = {
                    "first": _physical_endpoint(anchor),
                    "last": _physical_endpoint(sample),
                }
                if (
                    windows
                    and windows[-1]["first"]["market_count_scope"]["since_exchange_at"]
                    == anchor["market_count_scope"]["since_exchange_at"]
                ):
                    windows[-1] = proof_window
                else:
                    windows.append(proof_window)
                # Keep the last window's fixed initial endpoint to refresh its proof
                # through the entry window without requiring prints in every new minute.
                if not physical_coverage_valid(windows, now) or now - stamp(
                    anchor["market_count_scope"]["since_exchange_at"]
                ) >= timedelta(seconds=session.maximum_physical_progress_window_seconds):
                    anchor = _physical_anchor(runtime, now)
        if failures:
            progress = {
                "started_at": None,
                "anchor": None,
                "physical_proofs": [],
                "failures": failures,
            }
        else:
            progress["started_at"] = progress.get("started_at") or now.isoformat()
            progress["anchor"] = anchor or _physical_anchor(runtime, now)
            progress["physical_proofs"] = windows
            progress["failures"] = []
        progress["observation_identity"] = observation_identity
        progress["last_at"] = now.isoformat()
        runtime.repo.set_control(key, json.dumps(progress))
        runtime.store.audit("scheduled_paper_readiness_observation", sample, now, session.key)
        started = stamp(progress["started_at"]) if progress.get("started_at") else None
        if (
            started
            and now - started >= timedelta(seconds=1800)
            and now >= session.not_before
            and physical_coverage_valid(progress["physical_proofs"], now)
        ):
            if not _entry_observation_valid(runtime, now):
                progress["entry_blocker"] = "completed_regular_observation_or_receipt_latency"
                runtime.repo.set_control(key, json.dumps(progress))
                return {
                    "state": "warming_up",
                    "reason": progress["entry_blocker"],
                    "continuous_since": progress["started_at"],
                    "completed_physical_windows": len(progress["physical_proofs"]),
                    "failures": [],
                }
            request = _issue(
                runtime,
                session,
                {
                    **sample,
                    "continuous_since": started.isoformat(),
                    "continuous_seconds": (now - started).total_seconds(),
                    "observation_identity": observation_identity,
                    "physical_progress_windows": progress["physical_proofs"],
                    "physical_progress_contract": {
                        "minimum_endpoint_interval_seconds": (
                            session.physical_progress_window_seconds
                        ),
                        "series": "all five recorder symbols: quotes, trades and bars",
                        "sparse_minute_prints_are_not_a_critical_failure": True,
                        "latest_completed_proof_max_age_seconds": 90,
                        "critical_recorder_event_and_commit_max_age_seconds": 10,
                    },
                },
                now,
            )
            _notice(
                runtime,
                session,
                "market_ready",
                {
                    "request_id": str(request.request_id),
                    "issued_at": request.issued_at.isoformat() if request.issued_at else None,
                    "owner_approved_at": session.approved_at.isoformat(),
                },
                now,
            )
            return {"state": "delegated", "request_id": str(request.request_id)}
        return {
            "state": "warming_up",
            "continuous_since": progress.get("started_at"),
            "failures": progress["failures"],
            "completed_physical_windows": len(progress["physical_proofs"]),
        }
    except (ValueError, KeyError, TypeError, httpx.HTTPError) as error:
        runtime.repo.set_control(
            key,
            json.dumps(
                {
                    "started_at": None,
                    "anchor": None,
                    "physical_proofs": [],
                    "last_at": now.isoformat(),
                    "failures": [str(error)[:500]],
                }
            ),
        )
        return {"state": "readiness_blocked", "reason": str(error)[:500]}
