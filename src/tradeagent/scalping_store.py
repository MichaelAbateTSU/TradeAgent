"""Isolated v30 projections with shared durable orders and transactional audit metadata."""

from __future__ import annotations

import json
import zlib
from collections.abc import Sequence
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any
from uuid import NAMESPACE_URL, uuid4, uuid5

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    Numeric,
    String,
    Table,
    UniqueConstraint,
    func,
    select,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Connection

from tradeagent.persistence import Database, append_reporting_metadata, events, metadata, orders
from tradeagent.scalping_config import ScalpingConfig

scalping_runs = Table(
    "scalping_runs",
    metadata,
    Column("run_id", String(64), primary_key=True),
    Column("cohort_id", String(64), nullable=False),
    Column("account_digest", String(64), nullable=False),
    Column("code_sha", String(64), nullable=False),
    Column("config_hash", String(64), nullable=False),
    Column("config", JSON, nullable=False),
    Column("approved_at", DateTime(timezone=True), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)
Index("ix_scalping_runs_account", scalping_runs.c.account_digest, scalping_runs.c.created_at)

scalping_cycles = Table(
    "scalping_cycles",
    metadata,
    Column("cycle_id", String(36), primary_key=True),
    Column("run_id", String(64), ForeignKey("scalping_runs.run_id"), nullable=False),
    Column("account_digest", String(64), nullable=False),
    Column("symbol", String(32), nullable=False),
    Column("decision_id", String(128), nullable=False),
    Column("state", String(40), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    Column("opened_at", DateTime(timezone=True)),
    Column("exit_due_at", DateTime(timezone=True)),
    Column("closed_at", DateTime(timezone=True)),
    Column("owned_quantity", Numeric(38, 18), nullable=False),
    Column("entry_quantity", Numeric(38, 18), nullable=False),
    Column("entry_value", Numeric(38, 18)),
    Column("exit_quantity", Numeric(38, 18), nullable=False),
    Column("exit_value", Numeric(38, 18)),
    Column("actual_cash_fees", Numeric(38, 18), nullable=False),
    Column("actual_base_fees", Numeric(38, 18), nullable=False),
    Column("gross_cash_flow", Numeric(38, 18)),
    Column("actual_net_pnl", Numeric(38, 18)),
    Column("modeled_net_pnl", Numeric(38, 18)),
    Column("fees_pending", Boolean, nullable=False),
    Column("payload", JSON, nullable=False),
    UniqueConstraint(
        "account_digest", "symbol", "decision_id", name="uq_scalping_account_decision"
    ),
)
Index("ix_scalping_cycles_account_state", scalping_cycles.c.account_digest, scalping_cycles.c.state)
Index(
    "ix_scalping_cycles_account_closed",
    scalping_cycles.c.account_digest,
    scalping_cycles.c.closed_at,
)

scalping_order_links = Table(
    "scalping_order_links",
    metadata,
    Column("client_order_id", String(48), ForeignKey("orders.client_order_id"), primary_key=True),
    Column("cycle_id", String(36), ForeignKey("scalping_cycles.cycle_id"), nullable=False),
    Column("run_id", String(64), ForeignKey("scalping_runs.run_id"), nullable=False),
    Column("decision_key", String(64), unique=True),
    Column("dispatch_state", String(32), nullable=False),
    Column("intent", JSON, nullable=False),
    Column("broker", JSON),
    Column("submission_error", JSON),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("expires_at", DateTime(timezone=True)),
    Column("submission_started_at", DateTime(timezone=True)),
    Column("cancel_requested_at", DateTime(timezone=True)),
    Column("last_cancel_attempt_at", DateTime(timezone=True)),
    Column("first_positive_fill_at", DateTime(timezone=True)),
    Column("last_reconciled_at", DateTime(timezone=True)),
)
Index("ix_scalping_order_links_cycle", scalping_order_links.c.cycle_id)

scalping_activities = Table(
    "scalping_activities",
    metadata,
    Column("activity_key", String(64), primary_key=True),
    Column("account_digest", String(64), nullable=False),
    Column("activity_id", String(128), nullable=False),
    Column("kind", String(32), nullable=False),
    Column("symbol", String(32)),
    Column("broker_order_id", String(128)),
    Column("side", String(8)),
    Column("occurred_at", DateTime(timezone=True), nullable=False),
    Column("time_precision", String(16), nullable=False),
    Column("effective_date", String(10), nullable=False),
    Column("cycle_id", String(36), ForeignKey("scalping_cycles.cycle_id")),
    Column("attributed_order_id", String(128)),
    Column("attribution_basis", String(64)),
    Column("quantity", Numeric(38, 18), nullable=False),
    Column("net_amount", Numeric(38, 18), nullable=False),
    Column("payload", JSON, nullable=False),
    Column("payload_hash", String(64), nullable=False),
    Column("received_at", DateTime(timezone=True), nullable=False),
)
Index(
    "ix_scalping_activities_account_time",
    scalping_activities.c.account_digest,
    scalping_activities.c.occurred_at,
)
Index("ix_scalping_activities_cycle", scalping_activities.c.cycle_id)
Index("ix_scalping_activities_order", scalping_activities.c.broker_order_id)
Index(
    "ix_scalping_activities_attribution",
    scalping_activities.c.account_digest,
    scalping_activities.c.attribution_basis,
)

scalping_market_batches = Table(
    "scalping_market_batches",
    metadata,
    Column("batch_id", String(64), primary_key=True),
    Column("recorded_at", DateTime(timezone=True), nullable=False),
    Column("event_count", Integer, nullable=False),
    Column("first_exchange_ns", Numeric(30, 0)),
    Column("last_exchange_ns", Numeric(30, 0)),
    Column("encoding", String(32), nullable=False),
    Column("raw", LargeBinary, nullable=False),
)
Index("ix_scalping_market_batches_time", scalping_market_batches.c.recorded_at)

SCALPING_TABLES = (
    scalping_runs,
    scalping_cycles,
    scalping_order_links,
    scalping_activities,
    scalping_market_batches,
)


def utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def json_value(value: Any) -> Any:
    return json.loads(
        json.dumps(
            value,
            default=lambda item: utc(item).isoformat() if isinstance(item, datetime) else str(item),
        )
    )


def canonical(value: Any) -> str:
    return json.dumps(json_value(value), sort_keys=True, separators=(",", ":"))


def insert_once(connection: Connection, table: Table, values: dict[str, Any]) -> bool:
    statement = (
        pg_insert(table) if connection.dialect.name == "postgresql" else sqlite_insert(table)
    )
    keys = [column.name for column in table.primary_key]
    result = connection.execute(
        statement.values(**values).on_conflict_do_nothing(index_elements=keys)
    )
    return bool(result.rowcount)


class ScalpStore:
    def __init__(self, database: Database):
        self.database = database

    def audit(
        self,
        kind: str,
        payload: dict[str, Any],
        *,
        at: datetime,
        connection: Connection | None = None,
        identity: str | None = None,
    ) -> None:
        if connection is None:
            with self.database.begin() as local:
                self.audit(kind, payload, at=at, connection=local, identity=identity)
            return
        event_type = "scalp_" + kind
        event_id = str(uuid5(NAMESPACE_URL, identity)) if identity else str(uuid4())
        body = json_value(payload)
        if insert_once(
            connection,
            events,
            {
                "event_id": event_id,
                "occurred_at": at,
                "recorded_at": at,
                "event_type": event_type,
                "trace_id": str(payload.get("cycle_id") or payload.get("run_id") or event_id),
                "payload": body,
            },
        ):
            append_reporting_metadata(connection, event_id, event_type, body)

    def freeze_run(self, config: ScalpingConfig, code_sha: str, *, at: datetime) -> str:
        manifest = {"config": config.model_dump(mode="json"), "code_sha": code_sha}
        run_id = sha256(canonical(manifest).encode()).hexdigest()
        with self.database.begin() as connection:
            added = insert_once(
                connection,
                scalping_runs,
                {
                    "run_id": run_id,
                    "cohort_id": config.cohort_id,
                    "account_digest": config.account_digest,
                    "code_sha": code_sha,
                    "config_hash": config.identity,
                    "config": manifest["config"],
                    "approved_at": config.approved_at,
                    "created_at": at,
                },
            )
            row = (
                connection.execute(select(scalping_runs).where(scalping_runs.c.run_id == run_id))
                .mappings()
                .one()
            )
            if row["config"] != manifest["config"] or row["code_sha"] != code_sha:
                raise ValueError("immutable scalp run changed")
            if added:
                self.audit(
                    "run_frozen", {"run_id": run_id, **manifest}, at=at, connection=connection
                )
        return run_id

    def persist_market_batch(self, events: Sequence[dict[str, Any]], *, at: datetime) -> None:
        if not events:
            return
        raw = canonical(list(events)).encode()
        identity = sha256(raw).hexdigest()
        times = [int(item["exchange_time_ns"]) for item in events if item.get("exchange_time_ns")]
        with self.database.begin() as connection:
            if insert_once(
                connection,
                scalping_market_batches,
                {
                    "batch_id": identity,
                    "recorded_at": at,
                    "event_count": len(events),
                    "first_exchange_ns": min(times) if times else None,
                    "last_exchange_ns": max(times) if times else None,
                    "encoding": "zlib-json-v1",
                    "raw": zlib.compress(raw),
                },
            ):
                self.audit(
                    "market_batch",
                    {
                        "batch_id": identity,
                        "event_count": len(events),
                        "encoding": "zlib-json-v1",
                    },
                    at=at,
                    connection=connection,
                    identity="scalp-market:" + identity,
                )

    def cycle_orders(self, cycle_id: str) -> list[dict[str, Any]]:
        with self.database.begin() as connection:
            return [
                dict(row)
                for row in connection.execute(
                    select(
                        orders,
                        scalping_order_links.c.intent,
                        scalping_order_links.c.broker,
                        scalping_order_links.c.submission_error,
                        scalping_order_links.c.dispatch_state,
                        scalping_order_links.c.expires_at,
                        scalping_order_links.c.submission_started_at,
                        scalping_order_links.c.cancel_requested_at,
                        scalping_order_links.c.last_cancel_attempt_at,
                        scalping_order_links.c.first_positive_fill_at,
                        scalping_order_links.c.last_reconciled_at,
                    )
                    .join(
                        scalping_order_links,
                        scalping_order_links.c.client_order_id == orders.c.client_order_id,
                    )
                    .where(scalping_order_links.c.cycle_id == cycle_id)
                    .order_by(orders.c.created_at, orders.c.client_order_id)
                ).mappings()
            ]

    def summary(self, *, account_digest: str, since: datetime | None = None) -> dict[str, Any]:
        condition = scalping_cycles.c.account_digest == account_digest
        closed_condition = condition & scalping_cycles.c.closed_at.is_not(None)
        if since is not None:
            condition &= scalping_cycles.c.updated_at >= since
            closed_condition &= scalping_cycles.c.closed_at >= since
        with self.database.begin() as connection:
            counts = {
                str(row[0]): int(row[1])
                for row in connection.execute(
                    select(scalping_cycles.c.state, func.count())
                    .where(condition)
                    .group_by(scalping_cycles.c.state)
                ).all()
            }
            aggregate = dict(
                connection.execute(
                    select(
                        func.sum(scalping_cycles.c.gross_cash_flow).label("gross_cash_flow"),
                        func.sum(scalping_cycles.c.actual_cash_fees).label("actual_cash_fees"),
                        func.sum(scalping_cycles.c.actual_net_pnl).label("confirmed_net_pnl"),
                        func.sum(scalping_cycles.c.modeled_net_pnl).label("modeled_net_pnl"),
                        func.count()
                        .filter(scalping_cycles.c.fees_pending.is_(True))
                        .label("fees_pending_cycles"),
                        func.count()
                        .filter(scalping_cycles.c.state == "closed_owned_flat")
                        .label("completed_round_trips"),
                        func.avg(
                            scalping_cycles.c.payload["timing"][
                                "actual_entry_fill_latency_seconds"
                            ].as_float()
                        ).label("mean_actual_entry_fill_latency_seconds"),
                    ).where(closed_condition)
                )
                .mappings()
                .one()
            )
            recent = [
                dict(row)
                for row in connection.execute(
                    select(scalping_cycles)
                    .where(condition)
                    .order_by(scalping_cycles.c.updated_at.desc())
                    .limit(20)
                ).mappings()
            ]
            unassigned = dict(
                connection.execute(
                    select(
                        func.count().label("unattributed_account_fee_records"),
                        func.sum(-scalping_activities.c.net_amount).label(
                            "unattributed_account_cash_fees"
                        ),
                    ).where(
                        scalping_activities.c.account_digest == account_digest,
                        scalping_activities.c.kind.in_(("CFEE", "FEE")),
                        (scalping_activities.c.attribution_basis.is_(None))
                        | (scalping_activities.c.attribution_basis != "broker_order_id"),
                        scalping_activities.c.payload["status"].as_string() == "executed",
                        scalping_activities.c.payload["currency"].as_string() == "USD",
                        scalping_activities.c.net_amount <= 0,
                        *(
                            [scalping_activities.c.received_at >= since]
                            if since is not None
                            else []
                        ),
                    )
                )
                .mappings()
                .one()
            )
        result: dict[str, Any] = json_value(
            {
                "profile": "v30-paper-unrestricted",
                "account_digest": account_digest,
                "since": since,
                "cycle_counts": counts,
                **aggregate,
                **unassigned,
                "recent_cycles": recent,
                "period_totals_basis": (
                    "cycles closed in the period; recent_cycles includes fee revisions"
                ),
                "unattributed_account_fees_are_not_confirmed_strategy_costs": True,
                "actual_profitability_established": False,
                "fees_pending_are_estimates_not_actual_costs": True,
            }
        )
        return result
