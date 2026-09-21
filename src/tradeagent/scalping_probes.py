"""Paper-only prospective execution-validation probes, isolated from strategy results."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from typing import Any, Literal, Self

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    model_validator,
)
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


class ProbeOrderObservationV2(BaseModel):
    """A broker-order fact retained as part of a probe observation."""

    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    client_order_id: str
    broker_order_id: str | None = None
    side: Literal["buy", "sell"]
    requested_quantity: Decimal | None = Field(default=None, ge=0)
    limit_price: Decimal | None = Field(default=None, gt=0)
    dispatch_state: str
    broker_status: str | None = None
    broker_created_at: AwareDatetime | None = None
    broker_submitted_at: AwareDatetime | None = None
    broker_filled_at: AwareDatetime | None = None
    broker_canceled_at: AwareDatetime | None = None
    filled_quantity: Decimal | None = Field(default=None, ge=0)
    filled_average_price: Decimal | None = Field(default=None, gt=0)
    first_positive_fill_at: AwareDatetime | None = None
    last_reconciled_at: AwareDatetime | None = None


class ProbeObservationV2(BaseModel):
    """Typed execution evidence for passive probes, not a strategy training row."""

    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    schema_version: Literal["probe-observation-v2"] = "probe-observation-v2"
    evidence_environment: Literal["broker_paper"] = "broker_paper"
    account_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    run_id: str
    cohort_id: str
    policy_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    cycle_id: str
    purpose: Literal["passive_execution_validation_probe"] = (
        "passive_execution_validation_probe"
    )
    symbol: str
    action: Literal["PASSIVE_BUY"] = "PASSIVE_BUY"
    decision_at: AwareDatetime
    dispatch_started_at: AwareDatetime | None = None
    quote_event_at: AwareDatetime | None = None
    quote_received_at: AwareDatetime | None = None
    client_order_ids: tuple[str, ...]
    broker_order_ids: tuple[str, ...]
    requested_quantity: Decimal | None = Field(default=None, ge=0)
    limit_price: Decimal | None = Field(default=None, gt=0)
    terminal_state: str | None = None
    entry_quantity: Decimal = Field(ge=0)
    exit_quantity: Decimal = Field(ge=0)
    orders: tuple[ProbeOrderObservationV2, ...]
    resolved_at: AwareDatetime | None = None
    information_available_at: AwareDatetime
    exclusion_reasons: tuple[str, ...] = ()

    @model_validator(mode="after")
    def consistent_identity(self) -> Self:
        if self.resolved_at is not None and self.resolved_at < self.decision_at:
            raise ValueError("a probe cannot resolve before its decision")
        if self.information_available_at < self.decision_at:
            raise ValueError("probe information cannot predate its decision")
        if self.symbol not in {"BTC/USD", "ETH/USD"}:
            raise ValueError("probe evidence must use the restricted crypto universe")
        return self


def _probe_scope(
    account_digest: str,
    policy: ExecutionValidationProbePolicy,
    *,
    as_of: datetime | None = None,
) -> tuple[Any, ...]:
    scope: tuple[Any, ...] = (
        scalping_cycles.c.account_digest == account_digest,
        scalping_cycles.c.payload["classification"].as_string() == CLASSIFICATION,
        scalping_cycles.c.payload["probe_policy"]["cohort_id"].as_string()
        == policy.cohort_id,
        scalping_cycles.c.created_at < policy.authorization_cutoff,
    )
    if as_of is not None:
        scope += (scalping_cycles.c.created_at <= as_of,)
    return scope


def _aware_timestamp(value: Any) -> Any:
    return utc(value) if isinstance(value, datetime) else value


def probe_observations(
    database: Database,
    account_digest: str,
    policy: ExecutionValidationProbePolicy,
    *,
    now: datetime,
) -> tuple[tuple[ProbeObservationV2, ...], dict[str, int]]:
    """Return versioned broker-paper records known by ``now`` with explicit exclusions."""

    now = utc(now)
    with database.begin() as connection:
        cycles = [
            dict(row)
            for row in connection.execute(
                select(scalping_cycles)
                .where(*_probe_scope(account_digest, policy, as_of=now))
                .order_by(scalping_cycles.c.created_at, scalping_cycles.c.cycle_id)
            )
            .mappings()
            .all()
        ]
        cycle_ids = [str(row["cycle_id"]) for row in cycles]
        orders = (
            [
                dict(row)
                for row in connection.execute(
                    select(scalping_order_links)
                    .where(scalping_order_links.c.cycle_id.in_(cycle_ids))
                    .order_by(
                        scalping_order_links.c.created_at,
                        scalping_order_links.c.client_order_id,
                    )
                )
                .mappings()
                .all()
            ]
            if cycle_ids
            else []
        )
    orders_by_cycle: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for order in orders:
        orders_by_cycle[str(order["cycle_id"])].append(order)

    observations: list[ProbeObservationV2] = []
    exclusions: Counter[str] = Counter()
    for cycle in cycles:
        payload = cycle["payload"]
        if not isinstance(payload, Mapping):
            exclusions["INVALID_PROBE_PAYLOAD"] += 1
            continue
        raw_policy = payload.get("probe_policy")
        if not isinstance(raw_policy, Mapping):
            exclusions["MISSING_STORED_POLICY"] += 1
            continue
        try:
            stored_policy = ExecutionValidationProbePolicy.model_validate(raw_policy)
        except ValidationError:
            exclusions["INVALID_STORED_POLICY"] += 1
            continue
        if stored_policy.identity != policy.identity:
            exclusions["POLICY_HASH_MISMATCH"] += 1
            continue
        cycle_orders = orders_by_cycle.get(str(cycle["cycle_id"]), [])
        serialized_orders = []
        try:
            for order in cycle_orders:
                intent = order["intent"] if isinstance(order["intent"], Mapping) else {}
                broker = order["broker"] if isinstance(order["broker"], Mapping) else {}
                raw_request = intent.get("request")
                request = raw_request if isinstance(raw_request, Mapping) else {}
                side = str(request.get("side", "")).removeprefix("Side.").lower()
                if side not in {"buy", "sell"}:
                    raise ValueError("probe order side is invalid")
                order_side: Literal["buy", "sell"] = "buy" if side == "buy" else "sell"
                serialized_orders.append(
                    ProbeOrderObservationV2(
                        client_order_id=str(order["client_order_id"]),
                        broker_order_id=(
                            str(broker["id"]) if broker.get("id") is not None else None
                        ),
                        side=order_side,
                        requested_quantity=request.get("quantity"),
                        limit_price=intent.get("limit_price"),
                        dispatch_state=str(order["dispatch_state"]),
                        broker_status=(
                            str(broker["status"]) if broker.get("status") is not None else None
                        ),
                        broker_created_at=_aware_timestamp(broker.get("created_at")),
                        broker_submitted_at=_aware_timestamp(broker.get("submitted_at")),
                        broker_filled_at=_aware_timestamp(broker.get("filled_at")),
                        broker_canceled_at=_aware_timestamp(broker.get("canceled_at")),
                        filled_quantity=broker.get("filled_quantity"),
                        filled_average_price=broker.get("filled_average_price"),
                        first_positive_fill_at=_aware_timestamp(
                            order["first_positive_fill_at"]
                        ),
                        last_reconciled_at=_aware_timestamp(order["last_reconciled_at"]),
                    )
                )
            quote = payload.get("entry_quote")
            raw_quote = quote if isinstance(quote, Mapping) else {}
            entry = next((order for order in serialized_orders if order.side == "buy"), None)
            entry_order = None
            for order in cycle_orders:
                intent = order["intent"]
                if not isinstance(intent, Mapping):
                    continue
                entry_request = intent.get("request")
                if isinstance(entry_request, Mapping) and entry_request.get("side") == "buy":
                    entry_order = order
                    break
            closed_at = cycle["closed_at"]
            resolved_at = (
                utc(closed_at)
                if cycle["state"] in CLOSED and closed_at is not None and utc(closed_at) <= now
                else None
            )
            exclusion_reasons = (
                ()
                if resolved_at is not None
                else ("RESOLUTION_NOT_AVAILABLE_AS_OF_CUTOFF",)
            )
            observations.append(
                ProbeObservationV2(
                    account_digest=account_digest,
                    run_id=str(cycle["run_id"]),
                    cohort_id=policy.cohort_id,
                    policy_hash=policy.identity,
                    cycle_id=str(cycle["cycle_id"]),
                    symbol=str(cycle["symbol"]),
                    decision_at=utc(cycle["created_at"]),
                    dispatch_started_at=(
                        utc(entry_order["submission_started_at"])
                        if entry_order is not None
                        and entry_order["submission_started_at"] is not None
                        else None
                    ),
                    quote_event_at=raw_quote.get("exchange_at"),
                    quote_received_at=raw_quote.get("received_at"),
                    client_order_ids=tuple(order.client_order_id for order in serialized_orders),
                    broker_order_ids=tuple(
                        order.broker_order_id
                        for order in serialized_orders
                        if order.broker_order_id is not None
                    ),
                    requested_quantity=entry.requested_quantity if entry is not None else None,
                    limit_price=entry.limit_price if entry is not None else None,
                    terminal_state=str(cycle["state"]) if resolved_at is not None else None,
                    entry_quantity=cycle["entry_quantity"],
                    exit_quantity=cycle["exit_quantity"],
                    orders=tuple(serialized_orders),
                    resolved_at=resolved_at,
                    information_available_at=resolved_at or now,
                    exclusion_reasons=exclusion_reasons,
                )
            )
        except ValidationError as error:
            fields = {
                str(detail["loc"][0]) if detail.get("loc") else "unknown"
                for detail in error.errors(include_url=False)
            }
            for field in fields:
                exclusions[f"INVALID_PROBE_OBSERVATION:{field}"] += 1
        except (TypeError, ValueError):
            exclusions["INVALID_PROBE_OBSERVATION"] += 1
    return tuple(observations), dict(sorted(exclusions.items()))


class ExecutionValidationProbeCohort:
    """Schedule one bounded probe through the existing hardened order engine."""

    def __init__(self, engine: ScalpOrderEngine, policy: ExecutionValidationProbePolicy):
        self.engine, self.policy = engine, policy
        self._last_schedule_evaluation_at: datetime | None = None

    def _scope(self, *, before_cutoff: bool = False) -> tuple[Any, ...]:
        scope: tuple[Any, ...] = (
            scalping_cycles.c.account_digest == self.engine.config.account_digest,
            scalping_cycles.c.payload["classification"].as_string() == CLASSIFICATION,
            scalping_cycles.c.payload["probe_policy"]["cohort_id"].as_string()
            == self.policy.cohort_id,
        )
        if before_cutoff:
            scope += (scalping_cycles.c.created_at < self.policy.authorization_cutoff,)
        return scope

    def _today(self, now: datetime) -> date:
        return utc(now).date()

    def _counts(self, now: datetime) -> dict[str, Any]:
        start = datetime.combine(self._today(now), datetime.min.time(), tzinfo=UTC)
        with self.engine.database.begin() as connection:
            scope = (
                *self._scope(before_cutoff=True),
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
                        *self._scope(),
                        scalping_cycles.c.state.not_in(CLOSED),
                    )
                )
                or 0
            )
            last = connection.scalar(
                select(func.max(scalping_cycles.c.created_at)).where(
                    *self._scope(before_cutoff=True),
                )
            )
            by_symbol = {
                str(row["symbol"]): int(row["count"])
                for row in connection.execute(
                    select(
                        scalping_cycles.c.symbol,
                        func.count().label("count"),
                    )
                    .where(*scope)
                    .group_by(scalping_cycles.c.symbol)
                ).mappings()
            }
        return {
            "submitted": submitted,
            "filled": filled,
            "outstanding": outstanding,
            "last_submitted_at": utc(last).isoformat() if last else None,
            "submitted_by_symbol": by_symbol,
        }

    def _schedule_due(self, counts: dict[str, Any], now: datetime) -> bool:
        last_submitted = counts["last_submitted_at"]
        reference = (
            datetime.fromisoformat(str(last_submitted))
            if last_submitted is not None
            else self._last_schedule_evaluation_at
        )
        return reference is None or now - reference >= timedelta(
            seconds=self.policy.schedule_interval_seconds
        )

    def step(self, quotes: dict[str, ScalpQuote], *, now: datetime) -> dict[str, Any]:
        """Reconcile exposure promptly and idle probes only at their schedule boundary."""
        now = utc(now)
        counts = self._counts(now)
        schedule_due = self._schedule_due(counts, now)
        if not schedule_due and counts["outstanding"] == 0:
            return {
                **self.status(now=now),
                "scheduled_client_order_id": None,
                "eligible_for_new_probe": False,
                "broker_reconciliation": "deferred_until_schedule_boundary",
            }

        self.engine.reconcile(now=now)
        if schedule_due:
            self._last_schedule_evaluation_at = now
        counts = self._counts(now)
        eligible = (
            schedule_due
            and now < self.policy.authorization_cutoff
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
            available = [
                symbol for symbol in self.policy.symbols if quotes.get(symbol) is not None
            ]
            for symbol in sorted(
                available,
                key=lambda item: (
                    int(counts["submitted_by_symbol"].get(item, 0)),
                    self.policy.symbols.index(item),
                ),
            ):
                quote = quotes.get(symbol)
                if quote is None:
                    continue
                submitted = self.engine.submit_execution_validation_probe(
                    policy=self.policy, quote=quote, now=now
                )
                if submitted:
                    break
        return {
            **self.status(now=now),
            "scheduled_client_order_id": submitted,
            "eligible_for_new_probe": eligible,
            "broker_reconciliation": "performed",
        }

    def status(self, *, now: datetime) -> dict[str, Any]:
        counts = self._counts(now)
        with self.engine.database.begin() as connection:
            resolved = int(
                connection.scalar(
                    select(func.count())
                    .select_from(scalping_cycles)
                    .where(
                        *self._scope(before_cutoff=True),
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
                        *self._scope(),
                        scalping_order_links.c.dispatch_state.not_in(
                            ("expired_unsent", "broker_rejected")
                        ),
                    )
                )
                or 0
            )
            out_of_window_records = int(
                connection.scalar(
                    select(func.count())
                    .select_from(scalping_cycles)
                    .where(
                        *self._scope(),
                        scalping_cycles.c.created_at >= self.policy.authorization_cutoff,
                    )
                )
                or 0
            )
        return {
            "as_of": now.isoformat(),
            "policy": self.policy.description(),
            "active_cohort": self.policy.cohort_id,
            "counts": {
                **counts,
                "resolved_labels": resolved,
                "tracked_order_records": open_orders,
                "out_of_window_records_excluded": out_of_window_records,
            },
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
    observations, exclusions = probe_observations(database, account_digest, policy, now=now)
    resolved = [
        observation
        for observation in observations
        if observation.resolved_at is not None
        and observation.entry_quantity > 0
        and observation.exit_quantity > 0
    ]
    resolved.sort(key=lambda observation: observation.resolved_at or observation.decision_at)
    resolved_count = len(resolved)
    split = resolved_count // 2
    return {
        "as_of": now.isoformat(),
        "policy_id": policy.policy_id,
        "cohort_id": policy.cohort_id,
        "policy_hash": policy.identity,
        "evidence": "actual broker-resolved probe fills and exits only",
        "eligible": resolved_count >= 2,
        "execution_round_trip_evidence_present": resolved_count > 0,
        "reason": (
            None if resolved_count >= 2 else "INSUFFICIENT_ACTUAL_RESOLVED_PROBE_LABELS"
        ),
        "required_labels": 2,
        "qualifying_labels": resolved_count,
        "cohort_and_cutoff_scoped": True,
        "cohort_policy_and_as_of_scoped": True,
        "chronological_train_labels": split,
        "chronological_held_out_labels": resolved_count - split,
        "observation_schema": "probe-observation-v2",
        "observation_count": len(observations),
        "observation_exclusion_reasons": exclusions,
        "profitability_claim": False,
        "model_validity_claim": False,
    }
