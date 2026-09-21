"""Bounded paper execution acceptance and signal-driven evidence collection."""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from typing import Any, Literal, Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import func, select

from tradeagent.scalping_config import ScalpQuote, ScalpSignal, crypto_symbol
from tradeagent.scalping_execution import ScalpOrderEngine
from tradeagent.scalping_store import scalping_cycles, utc

ACCEPTANCE_CLASSIFICATION = "execution_acceptance_test"
EXPERIMENTAL_CLASSIFICATION = "experimental_signal_scalp"
EXPERIMENT_CUTOFF = datetime(2026, 9, 28, 14, 40, 7, tzinfo=UTC)
ACCEPTANCE_COHORT_ID = "execution-acceptance-20260921-r5"
EXPERIMENTAL_COHORT_ID = "signal-scalp-experiment-20260921-r1"


class PaperExperimentPolicy(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    policy_id: str
    cohort_id: str
    classification: str
    decision_prefix: str
    strategy_id: str
    entry_style: Literal["marketable_limit"] = "marketable_limit"
    maximum_price_cap_bps: Decimal = Field(default=Decimal("2"), ge=0, le=Decimal("10"))
    mode: Literal["paper"] = "paper"
    symbols: tuple[str, ...] = ("BTC/USD", "ETH/USD")
    broker_minimum_order_notional_usd: Decimal = Field(
        default=Decimal("10"), gt=0, le=Decimal("10")
    )
    exit_notional_buffer_bps: Decimal = Field(default=Decimal("50"), ge=0, le=Decimal("100"))
    max_order_notional_usd: Decimal = Field(
        default=Decimal("10.25"), gt=0, le=Decimal("10.25")
    )
    max_quote_age_seconds: float = Field(default=2, gt=0, le=5)
    dispatch_quote_max_age_seconds: float = Field(default=2, gt=0, le=5)
    daily_submitted_cap: int = Field(ge=1, le=20)
    daily_filled_cycle_cap: int = Field(ge=1, le=20)
    schedule_interval_seconds: int = Field(ge=1, le=3600)
    entry_ttl_seconds: int = Field(ge=1, le=15)
    exit_after_seconds: int = Field(ge=1, le=120)
    authorization_cutoff: AwareDatetime = EXPERIMENT_CUTOFF

    @model_validator(mode="after")
    def bounded_paper_contract(self) -> Self:
        if tuple(crypto_symbol(symbol) for symbol in self.symbols) != self.symbols:
            raise ValueError("paper experiment symbols must be canonical")
        if self.authorization_cutoff != EXPERIMENT_CUTOFF:
            raise ValueError("paper experiment cutoff is immutable")
        if len(self.cohort_id) > 64:
            raise ValueError("paper experiment cohort identity is too long")
        if self.broker_minimum_order_notional_usd >= self.max_order_notional_usd:
            raise ValueError("paper experiment maximum must exceed the broker minimum")
        return self

    @property
    def identity(self) -> str:
        return sha256(self.model_dump_json().encode()).hexdigest()


class ExecutionAcceptancePolicy(PaperExperimentPolicy):
    policy_id: Literal["execution-acceptance-v5"] = "execution-acceptance-v5"
    cohort_id: Literal["execution-acceptance-20260921-r5"] = (
        "execution-acceptance-20260921-r5"
    )
    classification: Literal["execution_acceptance_test"] = "execution_acceptance_test"
    decision_prefix: Literal["accept"] = "accept"
    strategy_id: Literal["execution-acceptance-v5"] = "execution-acceptance-v5"
    symbols: tuple[str, ...] = ("BTC/USD",)
    max_quote_age_seconds: float = 1
    daily_submitted_cap: int = 3
    daily_filled_cycle_cap: int = 1
    schedule_interval_seconds: int = 5
    entry_ttl_seconds: int = 5
    exit_after_seconds: int = 5


class ExperimentalScalpPolicy(PaperExperimentPolicy):
    policy_id: Literal["experimental-signal-scalp-v1"] = "experimental-signal-scalp-v1"
    cohort_id: Literal["signal-scalp-experiment-20260921-r1"] = (
        "signal-scalp-experiment-20260921-r1"
    )
    classification: Literal["experimental_signal_scalp"] = "experimental_signal_scalp"
    decision_prefix: Literal["experiment"] = "experiment"
    strategy_id: Literal["experimental-signal-scalp-v1"] = "experimental-signal-scalp-v1"
    daily_submitted_cap: int = 12
    daily_filled_cycle_cap: int = 6
    schedule_interval_seconds: int = 60
    entry_ttl_seconds: int = 5
    exit_after_seconds: int = 5
    daily_loss_limit_usd: Decimal = Field(default=Decimal("5"), gt=0, le=Decimal("25"))
    momentum_score_threshold: float = Field(default=0.25, gt=0)
    reversion_score_threshold: float = Field(default=0.65, gt=0)
    minimum_training_round_trips: int = Field(default=20, ge=10)
    minimum_held_out_round_trips: int = Field(default=10, ge=10)


def _scope(engine: ScalpOrderEngine, classification: str) -> tuple[Any, ...]:
    return (
        scalping_cycles.c.account_digest == engine.config.account_digest,
        scalping_cycles.c.payload["classification"].as_string() == classification,
    )


def _counts(
    engine: ScalpOrderEngine, policy: PaperExperimentPolicy, now: datetime
) -> dict[str, int]:
    start = datetime.combine(utc(now).date(), datetime.min.time(), tzinfo=UTC)
    with engine.database.begin() as connection:
        scope = (
            *_scope(engine, policy.classification),
            scalping_cycles.c.payload["probe_policy"]["cohort_id"].as_string()
            == policy.cohort_id,
            scalping_cycles.c.created_at >= start,
        )
        return {
            "submitted": int(connection.scalar(select(func.count()).where(*scope)) or 0),
            "filled": int(
                connection.scalar(
                    select(func.count()).where(*scope, scalping_cycles.c.entry_quantity > 0)
                )
                or 0
            ),
            "completed": int(
                connection.scalar(
                    select(func.count()).where(
                        *_scope(engine, policy.classification),
                        scalping_cycles.c.payload["probe_policy"][
                            "cohort_id"
                        ].as_string()
                        == policy.cohort_id,
                        scalping_cycles.c.state == "closed_owned_flat",
                    )
                )
                or 0
            ),
        }


def _audit_candidate(
    engine: ScalpOrderEngine,
    policy: PaperExperimentPolicy,
    *,
    now: datetime,
    symbol: str | None,
    checks: list[dict[str, Any]],
) -> None:
    engine.store.audit(
        "paper_experiment_candidate",
        {
            "cohort_id": policy.cohort_id,
            "classification": policy.classification,
            "account_digest": engine.config.account_digest,
            "symbol": symbol,
            "eligible": all(check["passed"] for check in checks),
            "checks": checks,
            "model_validity_claim": False,
            "profitability_claim": False,
        },
        at=now,
    )


class ExecutionAcceptanceCohort:
    def __init__(self, engine: ScalpOrderEngine, policy: ExecutionAcceptancePolicy):
        self.engine, self.policy = engine, policy
        self._last_slot: int | None = None

    def step(self, quotes: dict[str, ScalpQuote], *, now: datetime) -> dict[str, Any]:
        now = utc(now)
        slot = int(now.timestamp()) // self.policy.schedule_interval_seconds
        counts = _counts(self.engine, self.policy, now)
        if self._last_slot == slot:
            return {"state": "waiting", **counts}
        self._last_slot = slot
        quote = quotes.get(self.policy.symbols[0])
        checks = [
            {
                "name": "authorization_cutoff",
                "passed": now < self.policy.authorization_cutoff,
                "actual": now.isoformat(),
                "threshold": self.policy.authorization_cutoff.isoformat(),
            },
            {
                "name": "completed_round_trip",
                "passed": counts["completed"] == 0,
                "actual": counts["completed"],
                "threshold": 0,
            },
            {
                "name": "daily_submission_cap",
                "passed": counts["submitted"] < self.policy.daily_submitted_cap,
                "actual": counts["submitted"],
                "threshold": self.policy.daily_submitted_cap,
            },
            {
                "name": "fresh_quote",
                "passed": (
                    quote is not None
                    and self.engine._quote_valid(
                        quote,
                        now,
                        maximum_age_seconds=self.policy.max_quote_age_seconds,
                    )
                ),
                "actual": (
                    None
                    if quote is None
                    else max(
                        (now - quote.exchange_at).total_seconds(),
                        (now - quote.received_at).total_seconds(),
                    )
                ),
                "threshold": self.policy.max_quote_age_seconds,
            },
        ]
        _audit_candidate(
            self.engine,
            self.policy,
            now=now,
            symbol=quote.symbol if quote is not None else None,
            checks=checks,
        )
        if not all(check["passed"] for check in checks) or quote is None:
            state = "complete" if counts["completed"] else "blocked"
            return {"state": state, **counts, "checks": checks}
        client_id = self.engine.submit_paper_experiment(policy=self.policy, quote=quote, now=now)
        return {
            "state": "submitted" if client_id else "blocked",
            **_counts(self.engine, self.policy, now),
            "client_order_id": client_id,
            "checks": checks,
        }


class ExperimentalScalpCohort:
    def __init__(self, engine: ScalpOrderEngine, policy: ExperimentalScalpPolicy):
        self.engine, self.policy = engine, policy
        self._last_slot: int | None = None

    def _daily_net_pnl(self, now: datetime) -> Decimal:
        start = datetime.combine(utc(now).date(), datetime.min.time(), tzinfo=UTC)
        with self.engine.database.begin() as connection:
            value = connection.scalar(
                select(func.coalesce(func.sum(scalping_cycles.c.modeled_net_pnl), 0)).where(
                    *_scope(self.engine, self.policy.classification),
                    scalping_cycles.c.created_at >= start,
                    scalping_cycles.c.state == "closed_owned_flat",
                )
            )
        return Decimal(str(value or 0))

    def _acceptance_complete(self) -> bool:
        with self.engine.database.begin() as connection:
            return bool(
                connection.scalar(
                    select(func.count()).where(
                        *_scope(self.engine, ACCEPTANCE_CLASSIFICATION),
                        scalping_cycles.c.state == "closed_owned_flat",
                        scalping_cycles.c.owned_quantity == 0,
                    )
                )
                or 0
            )

    def step(
        self,
        signals: tuple[ScalpSignal, ...],
        quotes: dict[str, ScalpQuote],
        *,
        now: datetime,
    ) -> dict[str, Any]:
        now = utc(now)
        slot = int(now.timestamp()) // self.policy.schedule_interval_seconds
        counts = _counts(self.engine, self.policy, now)
        daily_net = self._daily_net_pnl(now)
        if self._last_slot == slot:
            return {"state": "waiting", **counts, "daily_net_pnl_usd": str(daily_net)}
        self._last_slot = slot
        candidates = [
            signal
            for signal in signals
            if signal.symbol in self.policy.symbols
            and signal.family in {"momentum", "reversion"}
            and signal.quote.symbol == signal.symbol
        ]
        candidate = max(candidates, key=lambda item: item.score, default=None)
        quote = quotes.get(candidate.symbol) if candidate is not None else None
        threshold = (
            self.policy.momentum_score_threshold
            if candidate is not None and candidate.family == "momentum"
            else self.policy.reversion_score_threshold
        )
        quote_age = (
            None
            if quote is None
            else max(
                (now - quote.exchange_at).total_seconds(),
                (now - quote.received_at).total_seconds(),
            )
        )
        checks = [
            {
                "name": "execution_acceptance_complete",
                "passed": self._acceptance_complete(),
                "actual": self._acceptance_complete(),
                "threshold": True,
            },
            {
                "name": "authorization_cutoff",
                "passed": now < self.policy.authorization_cutoff,
                "actual": now.isoformat(),
                "threshold": self.policy.authorization_cutoff.isoformat(),
            },
            {
                "name": "daily_submission_cap",
                "passed": counts["submitted"] < self.policy.daily_submitted_cap,
                "actual": counts["submitted"],
                "threshold": self.policy.daily_submitted_cap,
            },
            {
                "name": "daily_filled_cycle_cap",
                "passed": counts["filled"] < self.policy.daily_filled_cycle_cap,
                "actual": counts["filled"],
                "threshold": self.policy.daily_filled_cycle_cap,
            },
            {
                "name": "daily_loss_limit",
                "passed": daily_net > -self.policy.daily_loss_limit_usd,
                "actual": str(daily_net),
                "threshold": str(-self.policy.daily_loss_limit_usd),
            },
            {
                "name": "signal_family",
                "passed": candidate is not None,
                "actual": candidate.family if candidate is not None else None,
                "threshold": "momentum_or_reversion",
            },
            {
                "name": "signal_score",
                "passed": candidate is not None and candidate.score >= threshold,
                "actual": candidate.score if candidate is not None else None,
                "threshold": threshold,
            },
            {
                "name": "fresh_quote",
                "passed": (
                    quote_age is not None
                    and 0 <= quote_age <= self.policy.max_quote_age_seconds
                ),
                "actual": quote_age,
                "threshold": self.policy.max_quote_age_seconds,
            },
        ]
        _audit_candidate(
            self.engine,
            self.policy,
            now=now,
            symbol=candidate.symbol if candidate is not None else None,
            checks=checks,
        )
        if not all(check["passed"] for check in checks) or candidate is None or quote is None:
            reasons = Counter(
                check["name"] for check in checks if not bool(check["passed"])
            )
            return {
                "state": "blocked",
                **counts,
                "daily_net_pnl_usd": str(daily_net),
                "block_reasons": dict(sorted(reasons.items())),
                "checks": checks,
            }
        client_id = self.engine.submit_paper_experiment(
            policy=self.policy,
            quote=quote,
            now=now,
            signal=candidate,
        )
        return {
            "state": "submitted" if client_id else "blocked",
            **_counts(self.engine, self.policy, now),
            "daily_net_pnl_usd": str(daily_net),
            "client_order_id": client_id,
            "checks": checks,
        }


def experiment_policy_report() -> dict[str, Any]:
    acceptance = ExecutionAcceptancePolicy()
    experimental = ExperimentalScalpPolicy()
    return {
        "schema": "paper-scalping-experiment-policy-v1",
        "acceptance": acceptance.model_dump(mode="json"),
        "experimental": experimental.model_dump(mode="json"),
        "evidence_requirement": {
            "independent_observation": "one broker-confirmed entry and completed exit",
            "minimum_training_round_trips": experimental.minimum_training_round_trips,
            "minimum_held_out_round_trips": experimental.minimum_held_out_round_trips,
            "multiple_order_status_updates_are_not_independent": True,
        },
        "paper_fill_limitations": [
            "no_queue_position",
            "no_latency_slippage",
            "no_market_impact",
            "marketable_acceptance_is_not_passive_execution_evidence",
        ],
        "qualified_strategy_gate_preserved": True,
    }
