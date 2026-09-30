"""Finite immutable acceptance checks and a GET-only paper safety watcher."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any
from uuid import NAMESPACE_URL, uuid5

import httpx
from sqlalchemy import func, select

from tradeagent.alpaca_paper import AlpacaPaperSettings
from tradeagent.persistence import Database, ProductionRepository, events, worker_locks
from tradeagent.scalping_store import (
    canonical,
    insert_once,
    scalping_cycles,
    scalping_order_links,
    utc,
)
from tradeagent.shadow_dataset import (
    DATASET_ID,
    ShadowDatasetProtocol,
    dataset_status,
    shadow_source_links,
)

CHECK_EVENT = "shadow_dataset_acceptance_check"
STOP_KEY = f"shadow-dataset:{DATASET_ID}:stop"
SCHEDULE = {
    "readiness_final": datetime(2026, 9, 30, 23, 50, tzinfo=UTC),
    "startup_smoke": datetime(2026, 10, 1, 0, 25, tzinfo=UTC),
    "day1": datetime(2026, 10, 2, 0, 20, tzinfo=UTC),
}


class PaperReadOnlyMonitor:
    """Only GET on a fixed paper host. There is no POST/PATCH/DELETE method."""

    def __init__(self, settings: AlpacaPaperSettings, client: httpx.Client):
        self.client = client
        self.headers = {
            "APCA-API-KEY-ID": settings.key_id.get_secret_value(),
            "APCA-API-SECRET-KEY": settings.secret_key.get_secret_value(),
        }

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        if path not in {
            "/v2/account",
            "/v2/account/configurations",
            "/v2/positions",
            "/v2/orders",
            "/v2/account/activities",
        }:
            raise ValueError("paper monitor endpoint is not allowlisted")
        response = self.client.get(
            "https://paper-api.alpaca.markets" + path,
            params=params,
            headers=self.headers,
        )
        response.raise_for_status()
        return response.json()

    def snapshot(self, protocol: ShadowDatasetProtocol) -> dict[str, Any]:
        account = self._get("/v2/account")
        digest = sha256(str(account["id"]).encode()).hexdigest()
        if digest != protocol.account_digest:
            raise ValueError("read-only monitor account fingerprint mismatch")
        positions = self._get("/v2/positions")
        open_orders = self._get("/v2/orders", {"status": "open", "limit": 100})
        recent = self._get(
            "/v2/orders",
            {
                "status": "all",
                "after": protocol.frozen_at.isoformat(),
                "limit": 100,
                "direction": "desc",
            },
        )
        return {
            "paper_host": "https://paper-api.alpaca.markets",
            "account_digest": digest,
            "account_status": account["status"],
            "positions": len(positions),
            "open_orders": len(open_orders),
            "broker_order_records_since_freeze": len(recent),
            "broker_order_page_saturated": len(recent) >= 100,
            "verified_at": datetime.now(UTC).isoformat(),
            "read_methods": ["GET"],
            "order_submission_calls": 0,
        }

    def fee_evidence(self, protocol: ShadowDatasetProtocol) -> dict[str, Any]:
        account = self._get("/v2/account")
        if sha256(str(account["id"]).encode()).hexdigest() != protocol.account_digest:
            raise ValueError("fee verification account mismatch")
        unavailable = []
        try:
            configuration = self._get("/v2/account/configurations")
        except httpx.HTTPError as error:
            configuration = {}
            unavailable.append(
                {"source": "account/configurations", "error_type": type(error).__name__}
            )
        try:
            activities = self._get(
                "/v2/account/activities",
                {
                    "activity_types": "CFEE,FEE",
                    "after": "2026-09-01T00:00:00Z",
                    "page_size": 10,
                    "direction": "desc",
                },
            )
        except httpx.HTTPError as error:
            activities = None
            unavailable.append({"source": "account/activities", "error_type": type(error).__name__})
        fields = {
            key: value
            for key, value in account.items()
            if "fee" in key.lower() or "tier" in key.lower() or "commission" in key.lower()
        }
        config_fields = {
            key: value
            for key, value in configuration.items()
            if "fee" in key.lower() or "tier" in key.lower() or "commission" in key.lower()
        }
        return {
            "actual_crypto_fee_tier": None,
            "reported_account_crypto_tier": account.get("crypto_tier"),
            "reported_tier_field_source": "https://paper-api.alpaca.markets/v2/account",
            "published_schedule_source": "https://docs.alpaca.markets/us/docs/crypto-fees",
            "published_tier_1_scenario": {
                "maker_bps": 15,
                "taker_bps": 25,
                "volume_basis": "0-100000 USD executed crypto over 30 days",
            },
            "verified": False,
            "verification_time": datetime.now(UTC).isoformat(),
            "account_digest": protocol.account_digest,
            "source_endpoints": [
                "paper:/v2/account",
                "paper:/v2/account/configurations",
                "paper:/v2/account/activities?activity_types=CFEE,FEE",
            ],
            "explicit_account_fee_fields": fields,
            "explicit_configuration_fee_fields": config_fields,
            "fee_activities_returned": len(activities) if isinstance(activities, list) else None,
            "unavailable_fee_sources": unavailable,
            "effective_basis": "Account-specific rolling-volume tier and fee role not established",
            "reason": (
                "The account's reported crypto_tier is preserved separately; the published "
                "account schema does not establish effective fee-role/rate/rounding basis. "
                "Fee-sensitive outputs remain scenarios pending independent confirmation."
            ),
            "frozen_assumptions_changed": False,
            "fee_sensitive_outputs_are_scenarios": True,
        }


def checkpoint_id(kind: str, protocol_hash: str) -> str:
    return str(uuid5(NAMESPACE_URL, f"{DATASET_ID}:{protocol_hash}:acceptance:{kind}"))


def local_safety(database: Database, protocol: ShadowDatasetProtocol) -> dict[str, Any]:
    repo = ProductionRepository(database)
    heartbeat = repo.latest_heartbeat("tradeagent-event-worker")
    details = heartbeat[2] if heartbeat else {}
    with database.begin() as connection:
        attempts = int(
            connection.scalar(
                select(func.count())
                .select_from(
                    scalping_order_links.join(
                        scalping_cycles,
                        scalping_cycles.c.cycle_id == scalping_order_links.c.cycle_id,
                    )
                )
                .where(
                    scalping_cycles.c.account_digest == protocol.account_digest,
                    scalping_order_links.c.created_at >= protocol.frozen_at,
                    scalping_order_links.c.dispatch_state.not_in(("intent", "expired_unsent")),
                )
            )
            or 0
        )
    renewed = (
        details.get("economic_entries_enabled") is True
        or details.get("ordinary_entries_enabled") is True
        or details.get("trading_authorization") not in (None, "expired")
    )
    return {
        "local_order_attempts_since_freeze": attempts,
        "reported_order_attempts": details.get("orders_submitted"),
        "trading_authorization_renewed": renewed,
        "reported_trading_authorization": details.get("trading_authorization"),
        "reported_model_state": details.get("model_state"),
        "entry_policy": details.get("entry_policy"),
        "feed_state": (details.get("feed") or {}).get("state"),
        "feed_subscribed": (details.get("feed") or {}).get("subscribed"),
        "heartbeat_at": utc(heartbeat[1]).isoformat() if heartbeat else None,
    }


def signal_stop(database: Database, reason: str, now: datetime) -> None:
    repo = ProductionRepository(database)
    if repo.get_control(STOP_KEY) is None:
        repo.set_control(
            STOP_KEY,
            canonical(
                {
                    "reason": reason,
                    "observed_at": now.isoformat(),
                    "automatic_rearm": False,
                    "protocol_changed": False,
                }
            ),
        )
        repo.append_event(
            "shadow_dataset_safety_stop",
            {
                "dataset_id": DATASET_ID,
                "reason": reason,
                "orders_submitted_by_monitor": 0,
                "automatic_rearm": False,
            },
            occurred_at=now,
            trace_id=DATASET_ID,
        )


def readiness_snapshot(
    database: Database,
    protocol: ShadowDatasetProtocol,
    broker: PaperReadOnlyMonitor,
    *,
    now: datetime,
) -> dict[str, Any]:
    repo = ProductionRepository(database)
    heartbeat = repo.latest_heartbeat("tradeagent-event-worker")
    details = heartbeat[2] if heartbeat else {}
    with database.begin() as connection:
        lease = (
            connection.execute(
                select(worker_locks).where(worker_locks.c.lock_name == "tradeagent-event-worker")
            )
            .mappings()
            .one_or_none()
        )
        raw_at = connection.scalar(
            select(func.max(shadow_source_links.c.recorded_at)).where(
                shadow_source_links.c.dataset_id == DATASET_ID
            )
        )
        schema = connection.exec_driver_sql("select version_num from alembic_version").scalar()
    broker_state = broker.snapshot(protocol)
    safety = local_safety(database, protocol)
    owner_matches = bool(lease and heartbeat and lease["owner_id"] == heartbeat[0])
    feed = details.get("feed") or {}
    completed_at = datetime.now(UTC)
    clean = (
        owner_matches
        and feed.get("subscribed") is True
        and details.get("trading_authorization") == "expired"
        and details.get("model_state") == "no_support"
        and not safety["local_order_attempts_since_freeze"]
        and safety["reported_order_attempts"] == 0
        and not broker_state["positions"]
        and not broker_state["open_orders"]
        and not broker_state["broker_order_records_since_freeze"]
        and raw_at is not None
        and timedelta(0) <= completed_at - utc(raw_at) <= timedelta(seconds=30)
        and schema == "0015_shadow_research_dataset"
    )
    return {
        "snapshot_at": now.isoformat(),
        "snapshot_completed_at": completed_at.isoformat(),
        "protocol_hash": protocol.identity,
        "deployed_release": details.get("code_sha"),
        "lease_owner": lease["owner_id"] if lease else None,
        "lease_matches_heartbeat": owner_matches,
        "readiness_clean": clean,
        "readiness_state": "verified_clean" if clean else "unverified_or_transitioning",
        "heartbeat_at": utc(heartbeat[1]).isoformat() if heartbeat else None,
        "feed": details.get("feed"),
        "latest_persisted_raw_batch": utc(raw_at).isoformat() if raw_at else None,
        "migration_level": schema,
        "trading_authorization": details.get("trading_authorization"),
        "model_state": details.get("model_state"),
        "order_evidence": safety,
        "broker_state": broker_state,
        "protocol_modified": False,
    }


def _decision(report: dict[str, Any], safety: dict[str, Any], broker: dict[str, Any]) -> str:
    if (
        safety["local_order_attempts_since_freeze"]
        or safety["reported_order_attempts"] not in (None, 0)
        or safety["trading_authorization_renewed"]
        or broker["positions"]
        or broker["open_orders"]
        or broker["broker_order_records_since_freeze"]
    ):
        return "stop_on_order_exposure_or_authorization"
    primary = [row for row in report.get("coverage", []) if row["horizon_seconds"] == 60]
    if not primary or any(not row["matured_evaluations"] for row in primary):
        return "insufficient_elapsed_mature_data"
    if any(
        row["overdue_unwritten_beyond_10s_monitor_grace"]
        for row in report.get("coverage", [])
        if row["matured_evaluations"]
    ):
        return "pause_on_label_persistence_defect"
    if any(report["missing_evaluation_slots"].values()):
        return "pause_on_evaluation_grid_defect"
    if report["quality"].get("counts", {}).get("timestamp_reversals", 0):
        return "pause_on_timestamp_integrity_defect"
    if safety["feed_subscribed"] is not True:
        return "pause_on_feed_subscription_defect"
    if all(row["coverage"] >= 0.95 for row in primary):
        return "continue_unchanged"
    if all(
        all(
            reason
            in {
                "DECISION_QUOTE_STALE_OR_MISSING",
                "HORIZON_QUOTE_STALE_OR_MISSING",
                "INSUFFICIENT_DISPLAYED_LONG_SIZE",
                "LOCAL_CONTINUITY_CHANGED",
            }
            for reason in row["missing_reasons"]
        )
        for row in primary
    ):
        return "retain_legitimate_missingness_no_synthesis"
    return "pause_on_unexplained_observation_defect"


class AcceptanceScheduler:
    def __init__(
        self,
        database: Database,
        protocol: ShadowDatasetProtocol,
        broker: PaperReadOnlyMonitor,
        *,
        sleeper: Callable[[float], None],
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ):
        self.database, self.protocol, self.broker, self.sleeper = (
            database,
            protocol,
            broker,
            sleeper,
        )
        self.clock = clock

    def _exists(self, kind: str) -> bool:
        with self.database.begin() as connection:
            return bool(
                connection.scalar(
                    select(events.c.event_id).where(
                        events.c.event_id == checkpoint_id(kind, self.protocol.identity)
                    )
                )
            )

    def _archive(self, kind: str, report: dict[str, Any], now: datetime) -> None:
        with self.database.begin() as connection:
            insert_once(
                connection,
                events,
                {
                    "event_id": checkpoint_id(kind, self.protocol.identity),
                    "event_type": CHECK_EVENT,
                    "trace_id": DATASET_ID,
                    "occurred_at": now,
                    "recorded_at": now,
                    "payload": json.loads(
                        canonical(
                            {
                                "kind": kind,
                                "protocol_hash": self.protocol.identity,
                                **report,
                            }
                        )
                    ),
                },
            )

    def run_due(self, now: datetime) -> list[str]:
        captured = []
        if not self._exists("readiness_initial"):
            self._archive(
                "readiness_initial",
                readiness_snapshot(self.database, self.protocol, self.broker, now=now),
                now,
            )
            captured.append("readiness_initial")
        if not self._exists("fee_verification"):
            self._archive("fee_verification", self.broker.fee_evidence(self.protocol), now)
            captured.append("fee_verification")
        for kind, due in SCHEDULE.items():
            if now < due or self._exists(kind):
                continue
            if kind == "readiness_final":
                report = readiness_snapshot(self.database, self.protocol, self.broker, now=now)
                report["missed_prestart_check"] = now >= self.protocol.start
                if not report["readiness_clean"]:
                    signal_stop(self.database, "FINAL_READINESS_UNVERIFIED", now)
            else:
                limit = (
                    self.protocol.start + timedelta(minutes=30)
                    if kind == "startup_smoke"
                    else due + timedelta(hours=2)
                )
                if now >= limit:
                    report = {
                        "state": "missed_bounded_checkpoint",
                        "observed_at": now.isoformat(),
                        "future_checks_not_backdated": True,
                    }
                else:
                    period = self.protocol.start.date() if kind == "day1" else None
                    before = dataset_status(self.database, now=now, detailed=True, period=period)
                    if kind == "startup_smoke":
                        self.sleeper(15)
                    checked = self.clock() if kind == "startup_smoke" else now
                    after = dataset_status(self.database, now=checked, detailed=True, period=period)
                    broker_state = self.broker.snapshot(self.protocol)
                    safety = local_safety(self.database, self.protocol)
                    decision = _decision(after, safety, broker_state)
                    if kind == "startup_smoke" and decision.startswith(("continue_", "retain_")):
                        advanced = all(
                            after["evaluations_by_symbol"].get(symbol, 0)
                            > before["evaluations_by_symbol"].get(symbol, 0)
                            for symbol in self.protocol.symbols
                        )
                        if not advanced:
                            decision = "pause_on_stalled_evaluation_worker"
                        elif after["source_manifest_root"] == before["source_manifest_root"]:
                            decision = "pause_on_stalled_raw_manifest"
                    report = {
                        "observed_at": checked.isoformat(),
                        "before": before,
                        "quality": after,
                        "broker_state": broker_state,
                        "order_evidence": safety,
                        "decision": decision,
                        "evaluations_advanced": {
                            symbol: after["evaluations_by_symbol"].get(symbol, 0)
                            > before["evaluations_by_symbol"].get(symbol, 0)
                            for symbol in self.protocol.symbols
                        }
                        if kind == "startup_smoke"
                        else None,
                        "manifest_advanced": after["source_manifest_root"]
                        != before["source_manifest_root"],
                        "does_not_claim_future_day_or_profitability": True,
                    }
                    if decision.startswith(("stop_", "pause_")):
                        signal_stop(self.database, decision, checked)
            self._archive(kind, report, now)
            captured.append(kind)
        return captured
