"""Paper-only prospective execution-validation probes, isolated from strategy results."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from typing import Any, Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import func, select

from tradeagent.persistence import Database
from tradeagent.scalping_config import ScalpQuote, crypto_symbol
from tradeagent.scalping_execution import CLOSED, ScalpOrderEngine
from tradeagent.scalping_store import scalping_cycles, scalping_order_links, utc

CLASSIFICATION = "execution_validation_probe"
PROBE_COHORT_ID = "execution-validation-probe-20260916-r1"
AUTHORIZATION_CUTOFF = datetime(2026, 9, 21, 19, 22, 58, 996000, tzinfo=UTC)


class ExecutionValidationProbePolicy(BaseModel):
    """The immutable, deliberately small paper label-collection contract."""

    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    policy_id: str = "execution-validation-probe-v1"
    cohort_id: str = PROBE_COHORT_ID
    mode: str = "paper"
    symbols: tuple[str, ...] = ("BTC/USD", "ETH/USD")
    max_order_notional_usd: Decimal = Field(default=Decimal("10"), gt=0, le=Decimal("25"))
    daily_submitted_cap: int = Field(default=4, ge=1, le=20)
    daily_filled_cycle_cap: int = Field(default=2, ge=1, le=10)
    schedule_interval_seconds: int = Field(default=900, ge=60)
    entry_ttl_seconds: int = Field(default=3, ge=1, le=15)
    exit_after_seconds: int = Field(default=15, ge=1, le=120)
    authorization_cutoff: AwareDatetime = AUTHORIZATION_CUTOFF

    @model_validator(mode="after")
    def paper_contract(self) -> Self:
        if self.mode != "paper" or self.symbols != ("BTC/USD", "ETH/USD"):
            raise ValueError("probes are paper-only and restricted to BTC/USD and ETH/USD")
        if len(self.cohort_id) > 64:
            raise ValueError("probe cohort identity is too long")
        if self.authorization_cutoff != AUTHORIZATION_CUTOFF:
            raise ValueError("probe authorization cutoff is immutable")
        if tuple(crypto_symbol(symbol) for symbol in self.symbols) != self.symbols:
            raise ValueError("probe symbols must be canonical")
        return self

    @property
    def identity(self) -> str:
        return sha256(self.model_dump_json().encode()).hexdigest()

    def description(self) -> dict[str, Any]:
        return {
            "policy_id": self.policy_id,
            "policy_hash": self.identity,
            "cohort_id": self.cohort_id,
            "paper_only": True,
            "classification": CLASSIFICATION,
            "symbols": list(self.symbols),
            "max_order_notional_usd": str(self.max_order_notional_usd),
            "daily_submitted_cap": self.daily_submitted_cap,
            "daily_filled_cycle_cap": self.daily_filled_cycle_cap,
            "one_global_outstanding_order_or_owned_exposure": True,
            "entry": "passive BUY at current bid",
            "entry_ttl_seconds": self.entry_ttl_seconds,
            "owned_exit": "broker market sell only after ownership reconciliation",
            "authorization_cutoff": self.authorization_cutoff.isoformat(),
            "new_probes_at_or_after_cutoff": False,
            "strategy_profitability_or_promotion": False,
        }


class ExecutionValidationProbeCohort:
    """Schedule one bounded probe through the existing hardened order engine."""

    def __init__(self, engine: ScalpOrderEngine, policy: ExecutionValidationProbePolicy):
        self.engine, self.policy = engine, policy

    def _today(self, now: datetime) -> date:
        return utc(now).date()

    def _counts(self, now: datetime) -> dict[str, Any]:
        start = datetime.combine(self._today(now), datetime.min.time(), tzinfo=UTC)
        with self.engine.database.begin() as connection:
            scope = (
                scalping_cycles.c.account_digest == self.engine.config.account_digest,
                scalping_cycles.c.payload["classification"].as_string() == CLASSIFICATION,
                scalping_cycles.c.created_at >= start,
            )
            submitted = int(connection.scalar(select(func.count()).where(*scope)) or 0)
            filled = int(
                connection.scalar(
                    select(func.count()).where(*scope, scalping_cycles.c.entry_quantity > 0)
                )
                or 0
            )
            outstanding = int(
                connection.scalar(
                    select(func.count())
                    .select_from(scalping_cycles)
                    .where(
                        scalping_cycles.c.account_digest == self.engine.config.account_digest,
                        scalping_cycles.c.payload["classification"].as_string() == CLASSIFICATION,
                        scalping_cycles.c.state.not_in(CLOSED),
                    )
                )
                or 0
            )
            last = connection.scalar(
                select(func.max(scalping_cycles.c.created_at)).where(
                    scalping_cycles.c.account_digest == self.engine.config.account_digest,
                    scalping_cycles.c.payload["classification"].as_string() == CLASSIFICATION,
                )
            )
        return {
            "submitted": submitted,
            "filled": filled,
            "outstanding": outstanding,
            "last_submitted_at": utc(last).isoformat() if last else None,
        }

    def step(self, quotes: dict[str, ScalpQuote], *, now: datetime) -> dict[str, Any]:
        """Reconcile first; schedule at most one current passive probe when eligible."""
        self.engine.reconcile(now=now)
        counts = self._counts(now)
        eligible = (
            now < self.policy.authorization_cutoff
            and counts["submitted"] < self.policy.daily_submitted_cap
            and counts["filled"] < self.policy.daily_filled_cycle_cap
            and counts["outstanding"] == 0
            and (
                counts["last_submitted_at"] is None
                or now - datetime.fromisoformat(str(counts["last_submitted_at"]))
                >= timedelta(seconds=self.policy.schedule_interval_seconds)
            )
        )
        submitted = None
        if eligible:
            for symbol in self.policy.symbols:
                quote = quotes.get(symbol)
                if quote is not None:
                    submitted = self.engine.submit_execution_validation_probe(
                        policy=self.policy, quote=quote, now=now
                    )
                    if submitted:
                        break
        return {
            **self.status(now=now),
            "scheduled_client_order_id": submitted,
            "eligible_for_new_probe": eligible,
        }

    def status(self, *, now: datetime) -> dict[str, Any]:
        counts = self._counts(now)
        with self.engine.database.begin() as connection:
            resolved = int(
                connection.scalar(
                    select(func.count())
                    .select_from(scalping_cycles)
                    .where(
                        scalping_cycles.c.account_digest == self.engine.config.account_digest,
                        scalping_cycles.c.payload["classification"].as_string() == CLASSIFICATION,
                        scalping_cycles.c.state.in_(CLOSED),
                    )
                )
                or 0
            )
            open_orders = int(
                connection.scalar(
                    select(func.count())
                    .select_from(scalping_order_links)
                    .join(
                        scalping_cycles,
                        scalping_cycles.c.cycle_id == scalping_order_links.c.cycle_id,
                    )
                    .where(
                        scalping_cycles.c.account_digest == self.engine.config.account_digest,
                        scalping_cycles.c.payload["classification"].as_string() == CLASSIFICATION,
                        scalping_order_links.c.dispatch_state.not_in(
                            ("expired_unsent", "broker_rejected")
                        ),
                    )
                )
                or 0
            )
        return {
            "as_of": now.isoformat(),
            "policy": self.policy.description(),
            "active_cohort": self.policy.cohort_id,
            "counts": {**counts, "resolved_labels": resolved, "tracked_order_records": open_orders},
            "limits": self.policy.description(),
            "outstanding_owned_probe_exposure_or_orders": counts["outstanding"],
            "deadline": self.policy.authorization_cutoff.isoformat(),
            "new_probe_eligibility": now < self.policy.authorization_cutoff,
            "actual_broker_labels_only": True,
            "strategy_profitability_validated": False,
        }


def held_out_probe_report(
    database: Database,
    account_digest: str,
    policy: ExecutionValidationProbePolicy,
    *,
    now: datetime,
) -> dict[str, Any]:
    """Read actual broker-resolved labels chronologically; never calibrate a model."""
    with database.begin() as connection:
        rows = list(
            connection.execute(
                select(scalping_cycles.c.closed_at, scalping_cycles.c.state)
                .where(
                    scalping_cycles.c.account_digest == account_digest,
                    scalping_cycles.c.payload["classification"].as_string() == CLASSIFICATION,
                    scalping_cycles.c.state.in_(CLOSED),
                    scalping_cycles.c.entry_quantity > 0,
                    scalping_cycles.c.exit_quantity > 0,
                )
                .order_by(scalping_cycles.c.closed_at)
            ).mappings()
        )
    resolved = len(rows)
    split = resolved // 2
    return {
        "as_of": now.isoformat(),
        "policy_id": policy.policy_id,
        "cohort_id": policy.cohort_id,
        "evidence": "actual broker-resolved probe fills and exits only",
        "eligible": resolved >= 2,
        "reason": None if resolved >= 2 else "INSUFFICIENT_ACTUAL_RESOLVED_PROBE_LABELS",
        "chronological_train_labels": split,
        "chronological_held_out_labels": resolved - split,
        "profitability_claim": False,
        "model_validity_claim": False,
    }
