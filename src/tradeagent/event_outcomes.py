"""Prospective quote-path diagnostics. Never substituted for actual brokerage fills."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from typing import Any

from sqlalchemy import JSON, Column, DateTime, String, Table, insert, select

from tradeagent.event_market import EventMarketState
from tradeagent.event_store import EventStore, event_decisions
from tradeagent.persistence import metadata

event_outcomes = Table(
    "event_outcomes",
    metadata,
    Column("outcome_id", String(64), primary_key=True),
    Column("cohort_id", String(64), nullable=False),
    Column("decision_id", String(64), nullable=False),
    Column("observed_at", DateTime(timezone=True), nullable=False),
    Column("payload", JSON, nullable=False),
)


def record_quote_paths(
    store: EventStore, cohort_id: str, states: dict[str, EventMarketState], now: datetime
) -> int:
    inserted = 0
    with store.database.begin() as connection:
        rows = list(
            connection.execute(
                select(event_decisions).where(
                    event_decisions.c.cohort_id == cohort_id,
                    event_decisions.c.decided_at >= now - timedelta(hours=8),
                )
            ).mappings()
        )
        for row in rows:
            decision = row["payload"]
            quote = decision.get("quote_snapshot")
            if not quote or decision.get("symbol") not in states:
                continue
            decided_at = datetime.fromisoformat(decision["decided_at"].replace("Z", "+00:00"))
            reference_at = datetime.fromisoformat(quote["timestamp"].replace("Z", "+00:00"))
            if not timedelta(0) <= decided_at - reference_at <= timedelta(seconds=5):
                continue
            state = states[decision["symbol"]]
            if (
                state.observed_at > now
                or state.quote_at > state.observed_at
                or not timedelta(0) <= now - state.quote_at <= timedelta(seconds=5)
            ):
                continue
            for minutes in (1, 5, 15, 60):
                target = decided_at + timedelta(minutes=minutes)
                if not target <= state.quote_at <= target + timedelta(seconds=60):
                    continue
                identity = sha256(f"{row['decision_id']}:{minutes}".encode()).hexdigest()
                if connection.scalar(
                    select(event_outcomes.c.outcome_id).where(
                        event_outcomes.c.outcome_id == identity
                    )
                ):
                    continue
                entry_ask = Decimal(quote["ask"])
                gross = (state.bid / entry_ask - 1) * 10000
                connection.execute(
                    insert(event_outcomes).values(
                        outcome_id=identity,
                        cohort_id=cohort_id,
                        decision_id=row["decision_id"],
                        observed_at=now,
                        payload={
                            "kind": "quote_path_diagnostic_not_executed_trade",
                            "horizon_minutes": minutes,
                            "entry_quote": quote,
                            "exit_quote": state.model_dump(mode="json"),
                            "spread_crossed_return_bps": str(gross),
                            "omitted_cost_stress_bps": {
                                "base": str(gross - 1),
                                "1.5x": str(gross - Decimal("1.5")),
                                "2x": str(gross - 2),
                                "3x": str(gross - 3),
                            },
                            "cash_control_bps": "0",
                            "policy_action": decision["action"],
                            "expected_net_return_bps": None,
                            "not_independent_trade_sample": True,
                        },
                    )
                )
                inserted += 1
    return inserted


def outcome_summary(store: EventStore, cohort_id: str) -> dict[str, Any]:
    with store.database.begin() as connection:
        rows = list(
            connection.execute(
                select(event_outcomes.c.payload).where(event_outcomes.c.cohort_id == cohort_id)
            ).scalars()
        )
    return {
        "available_quote_paths": len(rows),
        "hypothetical_not_broker_performance": True,
        "horizons": [1, 5, 15, 60],
        "latest": rows[-20:],
        "dsr": None,
        "pbo": None,
        "statistical_status": "insufficient prospective evidence",
    }
