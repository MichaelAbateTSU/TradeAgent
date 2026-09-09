from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.engine import Connection

from tradeagent.config import AppConfig
from tradeagent.event_demo import _write_control
from tradeagent.event_session import equipment_identity
from tradeagent.event_store import EventStore, event_cohorts, event_decisions
from tradeagent.experimental_policy import ExperimentalSettings, OperationalCertificate
from tradeagent.intraday import NyseSessionCalendar
from tradeagent.persistence import ProductionRepository, controls

PROTOCOL = "news-paper-local-protection-v1"
STOP_FRACTION = Decimal("0.005")
TARGET_FRACTION = Decimal("0.01")


def scope(settings: ExperimentalSettings, config_hash: str, code_sha: str) -> dict[str, Any]:
    if settings.entry_policy != "news-paper" or settings.practice_start_date is None:
        raise ValueError("explicit dated news-paper policy required")
    if settings.news_account_digest is None:
        raise ValueError("news-paper account pin required")
    return {
        "protocol": PROTOCOL,
        "cohort_id": settings.cohort_id,
        "config_hash": config_hash,
        "code_sha": code_sha,
        "account_digest": settings.news_account_digest,
        "session_date": settings.practice_start_date.isoformat(),
        "equipment_test_id": equipment_identity(
            settings.news_account_digest, settings.practice_start_date
        ),
        "symbols": settings.symbols.split(","),
        "maximum_entries": 2,
        "maximum_news_entries": 1,
        "maximum_notional": str(settings.max_entry_notional),
        "maximum_positions_including_pending": 1,
        "protection": protection(Decimal(100)),
        "qualification_eligible": False,
    }


def _state(connection: Connection, cohort: str) -> dict[str, tuple[Any, Any]]:
    keys = [
        "kill_switch",
        *(f"{cohort}:{s}" for s in ("pause", "news-terminal", "news-authorization", "certificate")),
    ]
    return {
        row.control_key: (row.control_value, row.updated_at)
        for row in connection.execute(
            select(controls).where(controls.c.control_key.in_(keys)).with_for_update()
        )
    }


def control_state(store: EventStore, cohort: str) -> dict[str, tuple[Any, Any]]:
    with store.database.begin() as connection:
        return _state(connection, cohort)


def activate(
    store: EventStore,
    settings: ExperimentalSettings,
    proof: OperationalCertificate,
    acceptance_sha256: str,
    expected: dict[str, tuple[Any, Any]],
    now: datetime,
) -> None:
    bounds = (
        NyseSessionCalendar(AppConfig().intraday).session_bounds(settings.practice_start_date)
        if settings.practice_start_date
        else None
    )
    if not (
        bounds
        and bounds[0] + timedelta(minutes=35) <= now < bounds[0] + timedelta(minutes=60)
        and proof.issued_at <= now < proof.expires_at
    ):
        raise ValueError("news activation requires the post-acceptance morning interval")
    authority = {
        **scope(settings, proof.config_hash, proof.code_sha),
        "issued_at": now.isoformat(),
        "acceptance_sha256": acceptance_sha256,
        "incident_acceptance_passed": True,
        "authority": "explicit user-authorized bounded equipment then genuine news paper session",
    }
    with store.database.begin() as connection:
        cohort = (
            connection.execute(
                select(event_cohorts)
                .where(event_cohorts.c.cohort_id == settings.cohort_id)
                .with_for_update()
            )
            .mappings()
            .one()
        )
        current = _state(connection, settings.cohort_id)
        if (
            not proof.permits_paper
            or proof.account_digest != settings.news_account_digest
            or proof.cohort_id != settings.cohort_id
            or cohort["config_hash"] != proof.config_hash
            or not re.fullmatch(r"[0-9a-f]{64}", acceptance_sha256)
            or current != expected
            or current.get("kill_switch", (None,))[0] != "active"
            or current.get(f"{settings.cohort_id}:pause", (None,))[0] != "OPERATOR_PAUSE"
            or any(
                f"{settings.cohort_id}:{s}" in current
                for s in ("news-terminal", "news-authorization", "certificate")
            )
        ):
            raise ValueError("news activation refused: changed controls or invalid/consumed scope")
        for key, value in {
            f"{settings.cohort_id}:certificate": proof.model_dump_json(),
            f"{settings.cohort_id}:news-authorization": json.dumps(authority, sort_keys=True),
            "v20:mechanics_attestation": proof.code_sha,
            f"{settings.cohort_id}:pause": "",
            "kill_switch": "inactive",
        }.items():
            _write_control(connection, key, value, now)
        store.audit("news_authorization", authority, now, settings.cohort_id, connection)


def authorized(
    repo: ProductionRepository,
    settings: ExperimentalSettings,
    config_hash: str,
    code_sha: str,
    digest: str,
    now: datetime,
    calendar: NyseSessionCalendar,
) -> bool:
    raw = repo.get_control(f"{settings.cohort_id}:news-authorization")
    if not raw or repo.get_control(f"{settings.cohort_id}:news-terminal"):
        return False
    try:
        value = json.loads(raw)
        bounds = (
            calendar.session_bounds(settings.practice_start_date)
            if settings.practice_start_date
            else None
        )
        issued = datetime.fromisoformat(value["issued_at"])
        return bool(
            all(value.get(k) == v for k, v in scope(settings, config_hash, code_sha).items())
            and digest == settings.news_account_digest
            and re.fullmatch(r"[0-9a-f]{64}", value["acceptance_sha256"])
            and value["incident_acceptance_passed"] is True
            and bounds
            and bounds[0] + timedelta(minutes=35) <= issued < bounds[0] + timedelta(minutes=60)
            and issued <= now < bounds[1]
        )
    except (KeyError, TypeError, ValueError):
        return False


def terminate(store: EventStore, cohort: str, reason: str, now: datetime) -> None:
    with store.database.begin() as connection:
        connection.execute(
            select(event_cohorts).where(event_cohorts.c.cohort_id == cohort).with_for_update()
        ).one()
        current = _state(connection, cohort)
        if f"{cohort}:news-terminal" not in current:
            value = {"reason": reason, "observed_at": now.isoformat(), "entries_disabled": True}
            _write_control(connection, f"{cohort}:news-terminal", json.dumps(value), now)
            store.audit("news_terminal", value, now, cohort, connection)
        if not current.get(f"{cohort}:pause", (None,))[0]:
            _write_control(connection, f"{cohort}:pause", "NEWS_SESSION_TERMINAL", now)
        if current.get("kill_switch", (None,))[0] != "active":
            _write_control(connection, "kill_switch", "active", now)


def protection(price: Decimal) -> dict[str, Any]:
    if not price.is_finite() or price <= 0:
        raise ValueError("positive actual/planned price required")
    return {
        "policy_version": PROTOCOL,
        "execution": "local_trigger_then_owned_quantity_market_exit",
        "broker_native": False,
        "reference_price": str(price),
        "stop_fraction": str(STOP_FRACTION),
        "target_fraction": str(TARGET_FRACTION),
        "stop_price": str(price * (1 - STOP_FRACTION)),
        "take_profit_price": str(price * (1 + TARGET_FRACTION)),
        "limits": (
            "Requires running worker/fresh quote; gaps, outages and slippage can exceed trigger."
        ),
    }


def valid_ticket(store: EventStore, cohort: str, ticket: dict[str, Any] | None) -> bool:
    if not ticket:
        return False
    with store.database.begin() as connection:
        saved = connection.scalar(
            select(event_decisions.c.payload).where(
                event_decisions.c.cohort_id == cohort,
                event_decisions.c.decision_id == ticket.get("stored_decision_id"),
            )
        )
    extraction = ticket.get("extraction") or {}
    decision = ticket.get("decision") or {}
    return bool(
        saved
        and saved.get("extraction") == extraction
        and extraction.get("facts")
        and extraction.get("facts") == ticket.get("facts")
        and extraction.get("evidence_ids") == ticket.get("evidence_ids")
        and decision.get("action") == "eligible"
        and decision.get("hypothesis") in {"H1", "H2"}
        and not decision.get("position_review_required")
        and ticket.get("probability_of_profit") is None
        and ticket.get("expected_net_return_bps") is None
    )
