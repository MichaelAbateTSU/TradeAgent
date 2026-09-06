from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any
from uuid import uuid4

from sqlalchemy import JSON, Column, DateTime, ForeignKey, String, Table, insert, select, update
from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError

from tradeagent.config import IntradayConfig
from tradeagent.intraday import NyseSessionCalendar
from tradeagent.persistence import Database, events, metadata, orders

event_cohorts = Table(
    "event_cohorts",
    metadata,
    Column("cohort_id", String(64), primary_key=True),
    Column("config_hash", String(64), nullable=False),
    Column("manifest", JSON, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("mode", String(32), nullable=False),
)
event_evidence = Table(
    "event_evidence",
    metadata,
    Column("evidence_id", String(64), primary_key=True),
    Column("received_at", DateTime(timezone=True), nullable=False),
    Column("payload", JSON, nullable=False),
)
event_decisions = Table(
    "event_decisions",
    metadata,
    Column("decision_id", String(64), primary_key=True),
    Column("cohort_id", String(64), ForeignKey("event_cohorts.cohort_id"), nullable=False),
    Column("evidence_id", String(64), ForeignKey("event_evidence.evidence_id"), nullable=False),
    Column("decided_at", DateTime(timezone=True), nullable=False),
    Column("payload", JSON, nullable=False),
)
event_order_links = Table(
    "event_order_links",
    metadata,
    Column("client_order_id", String(48), ForeignKey("orders.client_order_id"), primary_key=True),
    Column("cohort_id", String(64), ForeignKey("event_cohorts.cohort_id"), nullable=False),
    Column("cluster_key", String(64), nullable=False),
    Column("payload", JSON, nullable=False),
)
event_cluster_claims = Table(
    "event_cluster_claims",
    metadata,
    Column("claim_key", String(64), primary_key=True),
    Column("cohort_id", String(64), ForeignKey("event_cohorts.cohort_id"), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)


class EventStore:
    def __init__(self, database: Database):
        self.database = database

    def freeze(
        self, cohort_id: str, config_hash: str, manifest: dict[str, Any], mode: str, now: datetime
    ) -> None:
        with self.database.begin() as connection:
            existing = (
                connection.execute(
                    select(event_cohorts).where(event_cohorts.c.cohort_id == cohort_id)
                )
                .mappings()
                .one_or_none()
            )
            if existing:
                if existing["config_hash"] != config_hash:
                    raise ValueError(
                        "cohort immutable: config/model/policy changed; new cohort required"
                    )
                return
            connection.execute(
                insert(event_cohorts).values(
                    cohort_id=cohort_id,
                    config_hash=config_hash,
                    manifest=manifest,
                    mode=mode,
                    created_at=now,
                )
            )

    def evidence(self, evidence_id: str, payload: dict[str, Any], now: datetime) -> bool:
        try:
            with self.database.begin() as connection:
                connection.execute(
                    insert(event_evidence).values(
                        evidence_id=evidence_id,
                        received_at=now,
                        payload=payload,
                    )
                )
            return True
        except IntegrityError:
            with self.database.begin() as connection:
                existing = connection.scalar(
                    select(event_evidence.c.evidence_id).where(
                        event_evidence.c.evidence_id == evidence_id
                    )
                )
                if existing is None:
                    raise
            return False

    def decision(
        self, cohort_id: str, evidence_id: str, payload: dict[str, Any], now: datetime
    ) -> str:
        # First decisions are immutable; later observations belong in the audit timeline.
        decision_id = sha256(f"{cohort_id}:{evidence_id}".encode()).hexdigest()
        with self.database.begin() as connection:
            existing = connection.scalar(
                select(event_decisions.c.decision_id).where(
                    event_decisions.c.decision_id == decision_id
                )
            )
            if existing is None:
                connection.execute(
                    insert(event_decisions).values(
                        decision_id=decision_id,
                        cohort_id=cohort_id,
                        evidence_id=evidence_id,
                        payload=payload,
                        decided_at=now,
                    )
                )
        return decision_id

    def was_decided(self, cohort_id: str, evidence_id: str) -> bool:
        with self.database.begin() as connection:
            return (
                connection.scalar(
                    select(event_decisions.c.decision_id).where(
                        event_decisions.c.cohort_id == cohort_id,
                        event_decisions.c.evidence_id == evidence_id,
                    )
                )
                is not None
            )

    def pending_evidence(self, cohort_id: str, limit: int = 500) -> list[dict[str, Any]]:
        with self.database.begin() as connection:
            processed = select(event_decisions.c.evidence_id).where(
                event_decisions.c.cohort_id == cohort_id
            )
            return [
                dict(row)
                for row in connection.execute(
                    select(event_evidence)
                    .where(event_evidence.c.evidence_id.not_in(processed))
                    .order_by(event_evidence.c.received_at)
                    .limit(limit)
                ).mappings()
            ]

    def audit(
        self,
        kind: str,
        payload: dict[str, Any],
        now: datetime,
        trace_id: str,
        connection: Connection | None = None,
    ) -> None:
        statement = insert(events).values(
            event_id=str(uuid4()),
            occurred_at=now,
            recorded_at=datetime.now(UTC),
            event_type=f"event_{kind}",
            trace_id=trace_id,
            payload=payload,
        )
        if connection is not None:
            connection.execute(statement)
        else:
            with self.database.begin() as local:
                local.execute(statement)

    def linked_orders(self, cohort_id: str) -> list[dict[str, Any]]:
        with self.database.begin() as connection:
            return [
                dict(row)
                for row in connection.execute(
                    select(orders, event_order_links.c.payload.label("link"))
                    .join(
                        event_order_links,
                        orders.c.client_order_id == event_order_links.c.client_order_id,
                    )
                    .where(event_order_links.c.cohort_id == cohort_id)
                    .order_by(orders.c.created_at, orders.c.order_id)
                ).mappings()
            ]

    def update_link(self, client_id: str, payload: dict[str, Any]) -> None:
        with self.database.begin() as connection:
            connection.execute(
                update(event_order_links)
                .where(event_order_links.c.client_order_id == client_id)
                .values(payload=payload)
            )

    def report(self, cohort_id: str, limit: int = 100) -> dict[str, Any]:
        with self.database.begin() as connection:
            cohort = (
                connection.execute(
                    select(event_cohorts).where(event_cohorts.c.cohort_id == cohort_id)
                )
                .mappings()
                .one_or_none()
            )
            decisions = list(
                connection.execute(
                    select(event_decisions)
                    .where(event_decisions.c.cohort_id == cohort_id)
                    .order_by(event_decisions.c.decided_at.desc())
                ).mappings()
            )
            recent_events = [
                dict(row)
                for row in connection.execute(
                    select(events)
                    .where(events.c.trace_id.like(f"{cohort_id}%"))
                    .order_by(events.c.recorded_at.desc())
                    .limit(limit)
                ).mappings()
            ]
        reasons: Counter[str] = Counter()
        for decision in decisions:
            for reason in decision["payload"].get("reasons", []):
                reasons[str(reason)] += 1
        calendar = NyseSessionCalendar(IntradayConfig())
        evaluated_days = {
            _aware(row["decided_at"]).date()
            for row in decisions
            if row["payload"].get("action") == "eligible"
            and calendar.gate(_aware(row["decided_at"])).can_enter
            and row["payload"].get("mode") != "offline_replay"
        }
        return {
            "cohort": dict(cohort) if cohort else None,
            "decisions": [dict(row) for row in decisions[:limit]],
            "decision_count": len(decisions),
            "leading_no_trade_reasons": dict(reasons),
            "orders": self.linked_orders(cohort_id),
            "timeline": recent_events,
            "performance_label": "experimental; edge unproven",
            "qualified": False,
            "usable_forward_trading_sessions": len(evaluated_days),
            "independent_event_clusters": len(
                {
                    row["payload"].get("event_cluster_id")
                    for row in decisions
                    if row["payload"].get("event_cluster_id")
                    and row["payload"].get("issuer_id")
                    and row["payload"].get("hypothesis") in {"H1", "H2"}
                }
            ),
            "abstention_count": sum(row["payload"].get("action") == "abstain" for row in decisions),
        }


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
