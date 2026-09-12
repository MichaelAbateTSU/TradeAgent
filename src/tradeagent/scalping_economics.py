"""Runtime-independent, prospective economics for short-horizon crypto actions.

This is a deliberately small empirical model, not a directional classifier. Each
symbol/family/action is fitted once, before a chronological, embargoed holdout.
Overlapping outcome horizons are thinned within each fold. Feature support comes
only from fitting data; held-out observations never refit the prediction.
The same fit-only latency/cost gates are replayed on the holdout. Raw action
outcomes and executable-policy outcomes are reported separately; validation is
an action-level veto, not an opportunity to retune the runtime edge buffer.

Returns are conditional on an observed fill, in bps of executed notional. An
unfilled attempt has zero cash P&L, not its hypothetical directional return.
Costs embedded in a return are disclosed but never deducted again. Unknown,
non-embedded costs require an independently evidenced residual allowance.

The reported uncertainty is a standard error, not a probability of profit.
Studentized empirical bounds and Wilson fill bounds are simultaneous across the
predeclared action groups. Their usual independent-observation/stationarity
assumptions are limitations, not a claim of distribution-free coverage. Neither
positive fixtures nor selected-entry evidence establish full-market profitability.
"""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from fractions import Fraction
from functools import lru_cache
from hashlib import sha256
from math import atan, ceil, cos, isclose, isfinite, pi, sin, sqrt
from statistics import NormalDist, fmean, stdev
from types import MappingProxyType
from typing import Annotated, Literal, Self

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_serializer,
    field_validator,
    model_validator,
)

EconomicAction = Literal[
    "NO_TRADE", "PASSIVE_BUY", "AGGRESSIVE_BUY", "PASSIVE_SELL", "AGGRESSIVE_SELL"
]
TradeAction = Literal["PASSIVE_BUY", "AGGRESSIVE_BUY", "PASSIVE_SELL", "AGGRESSIVE_SELL"]
EconomicFamily = Literal["momentum", "reversion"]
FeatureValue = float | int | str | None
Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
Nonnegative = Annotated[float, Field(ge=0)]
Positive = Annotated[float, Field(gt=0)]
Probability = Annotated[float, Field(ge=0, le=1)]
CostName = Literal["fees", "spread", "slippage", "impact", "adverse_selection"]
ModelStatus = Literal["validated", "no_support", "failed_validation"]
ACTIONS: tuple[EconomicAction, ...] = (
    "NO_TRADE",
    "PASSIVE_BUY",
    "AGGRESSIVE_BUY",
    "PASSIVE_SELL",
    "AGGRESSIVE_SELL",
)
COST_NAMES: tuple[CostName, ...] = ("fees", "spread", "slippage", "impact", "adverse_selection")
ECONOMIC_FEATURE_NAMES: tuple[str, ...] = (
    "l1_imbalance",
    "l5_imbalance",
    "normalized_ofi_1s",
    "normalized_ofi_5s",
    "taker_delta_1s",
    "taker_delta_5s",
    "known_taker_fraction_5s",
    "microprice_displacement_bps",
    "spread_bps",
    "bid_depth_l5",
    "ask_depth_l5",
    "bid_slope_bps_per_unit",
    "ask_slope_bps_per_unit",
    "bid_additions_1s",
    "ask_additions_1s",
    "bid_additions_5s",
    "ask_additions_5s",
    "bid_removals_proxy_5s",
    "ask_removals_proxy_5s",
    "bid_depletion_fraction_5s",
    "ask_depletion_fraction_5s",
    "trade_intensity_5s",
    "event_intensity_5s",
    "trade_volume_5s",
    "trade_count_5s",
    "native_taker_trade_count_5s",
    "return_1s_bps",
    "return_5s_bps",
    "volatility_1s_bps",
    "volatility_5s_bps",
)


def _digest(value: object) -> str:
    return sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def _aware(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("an aware datetime is required; numeric timestamp units are not inferred")
    return value.astimezone(UTC)


def _number(value: float | int, name: str, *, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (float, int)):
        raise ValueError(f"{name} must be a finite number, not a coerced string or boolean")
    try:
        result = float(value)
    except OverflowError as exc:
        raise ValueError(f"{name} must be finite") from exc
    if not isfinite(result) or result < 0 or (positive and result == 0):
        raise ValueError(f"{name} must be finite and {'positive' if positive else 'nonnegative'}")
    return result


def _features(value: Mapping[str, FeatureValue]) -> Mapping[str, FeatureValue]:
    if not isinstance(value, Mapping):
        raise ValueError("features must be a mapping")
    for key, item in value.items():
        if not isinstance(key, str) or not key.strip():
            raise ValueError("feature names must be nonempty strings")
        if item is None or isinstance(item, str):
            continue
        if isinstance(item, bool) or not isinstance(item, (float, int)):
            raise ValueError(f"feature {key} must be a finite number, string, or None")
        try:
            finite = isfinite(item)
        except OverflowError as exc:
            raise ValueError(f"feature {key} must be finite") from exc
        if not finite:
            raise ValueError(f"feature {key} must be finite")
    return MappingProxyType(dict(sorted(value.items())))


def project_economic_features(snapshot: Mapping[str, object]) -> Mapping[str, FeatureValue]:
    """Project the same recorded predecision microstructure inputs in live and replay.

    IDs, clocks, readiness flags, policy metadata, cumulative session counters,
    heuristic scores, and forward labels are not market-state predictors here.
    Missing keys stay missing; observed nulls stay null. This projection does not
    establish snapshot causality or outcome completeness: the adapter must retain
    timestamps and every original candidate, including incomplete observations.
    """
    if not isinstance(snapshot, Mapping):
        raise ValueError("a predecision feature snapshot must be a mapping")
    selected: dict[str, FeatureValue] = {}
    for name in ECONOMIC_FEATURE_NAMES:
        if name not in snapshot:
            continue
        value = snapshot[name]
        if value is None:
            selected[name] = None
        elif isinstance(value, bool) or not isinstance(value, (float, int)):
            raise ValueError(f"economic feature {name} must be numeric or explicitly unobserved")
        else:
            selected[name] = value
    return _features(selected)


class _Frozen(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        strict=True,
        extra="forbid",
        allow_inf_nan=False,
        revalidate_instances="always",
    )


class CostEstimate(_Frozen):
    bps: Nonnegative | None = None
    included_in_return: bool = False
    kind: Literal["unknown", "observed", "estimated", "conservative_allowance", "embedded"]
    provenance: str = Field(min_length=1)
    validation_sha256: Digest | None = None

    @model_validator(mode="after")
    def coherent_estimate(self) -> Self:
        if not self.provenance.strip():
            raise ValueError("cost provenance must not be blank")
        if self.kind == "unknown":
            if self.bps is not None or self.included_in_return:
                raise ValueError("unknown cost is not a priced or embedded zero")
        elif self.kind == "embedded":
            if not self.included_in_return:
                raise ValueError("embedded costs must be marked included_in_return")
        elif self.bps is None:
            raise ValueError("priced cost estimates require finite bps")
        return self


def _unknown_cost() -> CostEstimate:
    return CostEstimate(kind="unknown", provenance="not independently observed or estimated")


class EconomicCosts(_Frozen):
    """Use aggregate fees or both fee legs, never both representations.

    Separate legs are required when inventory-withheld entry fees are already
    in the return but cash exit fees are not. All bps use the same executed
    entry-notional denominator as the sample's return. A partial quantity-
    difference principal does not certify that the full entry fee is embedded;
    use an unmixed raw-price basis rather than declaring the whole budget paid.
    """

    fees: CostEstimate = Field(default_factory=_unknown_cost)
    entry_fee: CostEstimate | None = None
    exit_fee: CostEstimate | None = None
    spread: CostEstimate = Field(default_factory=_unknown_cost)
    slippage: CostEstimate = Field(default_factory=_unknown_cost)
    impact: CostEstimate = Field(default_factory=_unknown_cost)
    adverse_selection: CostEstimate = Field(default_factory=_unknown_cost)
    residual: CostEstimate | None = None
    residual_covers: tuple[CostName, ...] = ()

    @model_validator(mode="after")
    def residual_is_not_a_second_charge(self) -> Self:
        if (self.entry_fee is None) != (self.exit_fee is None):
            raise ValueError("per-leg fee accounting requires both entry_fee and exit_fee")
        if self.entry_fee is not None and self.fees.kind != "unknown":
            raise ValueError("aggregate fees and per-leg fees cannot both be charged")
        if len(set(self.residual_covers)) != len(self.residual_covers):
            raise ValueError("duplicate residual cost coverage")
        if "fees" in self.residual_covers:
            raise ValueError(
                "fees require their own explicit schedule, not aggregate residual coverage"
            )
        if self.residual is not None and (
            self.residual.bps is None
            or self.residual.included_in_return
            or self.residual.kind not in {"estimated", "conservative_allowance"}
            or self.residual.validation_sha256 is None
        ):
            raise ValueError("a residual needs a priced, separately validated additional allowance")
        if self.residual_covers and (
            self.residual is None or self.residual.bps is None or self.residual.bps <= 0
        ):
            raise ValueError("unknown cost coverage requires a positive evidenced residual")
        for name in self.residual_covers:
            component = getattr(self, name)
            if component.bps is not None or component.included_in_return:
                raise ValueError("a residual cannot cover an already priced or embedded cost")
        return self

    def unpriced_components(self) -> tuple[str, ...]:
        return tuple(
            name
            for name, component in self.active_components()
            if not component.included_in_return
            and component.bps is None
            and name not in self.residual_covers
        )

    def fee_components(self) -> tuple[tuple[str, CostEstimate], ...]:
        if self.entry_fee is not None and self.exit_fee is not None:
            return (("entry_fee", self.entry_fee), ("exit_fee", self.exit_fee))
        return (("fees", self.fees),)

    def active_components(self) -> tuple[tuple[str, CostEstimate], ...]:
        return (
            *self.fee_components(),
            *((name, self.component(name)) for name in COST_NAMES if name != "fees"),
        )

    @property
    def total_fee_bps(self) -> float | None:
        components = self.fee_components()
        if any(component.bps is None for _, component in components):
            return None
        return sum(component.bps for _, component in components if component.bps is not None)

    def component(self, name: CostName) -> CostEstimate:
        return {
            "fees": self.fees,
            "spread": self.spread,
            "slippage": self.slippage,
            "impact": self.impact,
            "adverse_selection": self.adverse_selection,
        }[name]

    def additional_bps(self) -> float:
        if self.unpriced_components():
            raise ValueError("unpriced costs cannot be treated as zero")
        values = [
            component.bps
            for _, component in self.active_components()
            if not component.included_in_return and component.bps is not None
        ]
        if self.residual is not None and self.residual.bps is not None:
            values.append(self.residual.bps)
        return sum(values)


class EconomicSample(_Frozen):
    """An observed policy outcome, not a quote-only counterfactual execution.

    For ``fill_to_mid``, spread/slippage/impact describe the still-unembedded
    exit leg; the entry leg is already in the fill-price return. ``close_long``
    declares incremental liquidation economics, never a short or a rebooking
    of the position's historical round-trip P&L. ``entry_fee_net_*`` means the
    returned/sold quantity has already lost the entry base-currency fee.
    ``notional_usd`` is requested notional; ``filled_fraction`` is gross filled
    notional divided by requested notional, before inventory fee withholding.
    """

    sample_id: str = Field(min_length=1)
    account_digest: Digest
    source_sha256: Digest
    symbol: str = Field(pattern=r"^[A-Z0-9]+/USD$")
    family: EconomicFamily
    action: TradeAction
    decision_at: AwareDatetime
    feature_observed_at: AwareDatetime
    label_matured_at: AwareDatetime
    return_end_at: AwareDatetime | None = None
    horizon_seconds: Positive
    cancellation_horizon_seconds: Positive
    time_unit: Literal["seconds"] = "seconds"
    return_unit: Literal["bps"] = "bps"
    features: Mapping[str, FeatureValue]
    quote_age_seconds: Nonnegative
    decision_latency_seconds: Nonnegative
    spread_bps: Nonnegative
    notional_usd: Positive
    filled: bool
    filled_fraction: Probability
    fill_delay_seconds: Nonnegative | None = None
    gross_return_bps: float | None = None
    directional_return_bps: float | None = None
    return_basis: Literal[
        "mid_to_mid",
        "fill_to_mid",
        "fill_to_fill",
        "net_fill_to_fill",
        "entry_fee_net_fill_to_mid",
        "entry_fee_net_fill_to_fill",
    ]
    costs: EconomicCosts
    execution_evidence: Literal["observed", "validated_simulation", "unsupported_counterfactual"]
    execution_validation_sha256: Digest | None = None
    position_effect: Literal["open_long", "close_long"] = "open_long"

    @field_validator("decision_at", "feature_observed_at", "label_matured_at", "return_end_at")
    @classmethod
    def utc_times(cls, value: datetime | None) -> datetime | None:
        return None if value is None else _aware(value)

    @field_validator("features", mode="before")
    @classmethod
    def accept_immutable_mapping(cls, value: object) -> object:
        return dict(value) if isinstance(value, Mapping) else value

    @field_validator("features")
    @classmethod
    def freeze_features(cls, value: Mapping[str, FeatureValue]) -> Mapping[str, FeatureValue]:
        return _features(value)

    @field_serializer("features")
    def serialize_features(self, value: Mapping[str, FeatureValue]) -> dict[str, FeatureValue]:
        return dict(value)

    @model_validator(mode="after")
    def causal_outcome(self) -> Self:
        if not self.sample_id.strip():
            raise ValueError("sample identity must not be blank")
        if self.feature_observed_at > self.decision_at:
            raise ValueError("future features leak information into the decision")
        maturity = self.decision_at + timedelta(
            seconds=max(self.horizon_seconds, self.cancellation_horizon_seconds)
        )
        if self.label_matured_at < maturity:
            raise ValueError(
                "label must mature after the full prediction and cancellation horizons"
            )
        if self.filled:
            if (
                self.filled_fraction <= 0
                or self.fill_delay_seconds is None
                or self.gross_return_bps is None
                or self.return_end_at is None
            ):
                raise ValueError(
                    "filled samples need an observed fraction, delay, payoff and end time"
                )
            if (
                self.decision_at + timedelta(seconds=self.fill_delay_seconds)
                > self.label_matured_at
            ):
                raise ValueError("fill cannot occur after its label matured")
            if not (
                self.decision_at + timedelta(seconds=self.fill_delay_seconds)
                <= self.return_end_at
                <= self.decision_at + timedelta(seconds=self.horizon_seconds)
            ):
                raise ValueError(
                    "payoff must follow the fill and end within the prediction horizon"
                )
            if not self.costs.adverse_selection.included_in_return:
                raise ValueError(
                    "observed fill-conditioned returns already include adverse selection"
                )
        elif (
            self.filled_fraction != 0
            or self.fill_delay_seconds is not None
            or self.gross_return_bps is not None
            or self.return_end_at is not None
        ):
            raise ValueError("unfilled returns belong in directional_return_bps, not cash payoff")
        if (
            self.return_basis in {"fill_to_fill", "net_fill_to_fill", "entry_fee_net_fill_to_fill"}
            and self.filled
            and any(
                not getattr(self.costs, name).included_in_return
                for name in ("spread", "slippage", "impact")
            )
        ):
            raise ValueError("executed fill prices already include spread, slippage, and impact")
        if (
            self.return_basis in {"mid_to_mid", "fill_to_mid", "entry_fee_net_fill_to_mid"}
            and self.filled
            and any(
                getattr(self.costs, name).included_in_return
                for name in ("spread", "slippage", "impact")
            )
        ):
            raise ValueError("mid-ended returns need explicit unembedded execution costs")
        if self.filled:
            if self.return_basis.startswith("entry_fee_net_"):
                if (
                    self.position_effect != "open_long"
                    or self.costs.entry_fee is None
                    or self.costs.exit_fee is None
                    or not self.costs.entry_fee.included_in_return
                    or self.costs.exit_fee.included_in_return
                ):
                    raise ValueError(
                        "entry-fee-net returns require an embedded entry_fee and separate exit_fee"
                    )
            elif any(
                component.included_in_return != (self.return_basis == "net_fill_to_fill")
                for _, component in self.costs.fee_components()
            ):
                raise ValueError("fee inclusion must agree with the declared return basis")
            if self.position_effect == "close_long" and (
                self.costs.entry_fee is not None and self.costs.entry_fee.bps != 0
            ):
                raise ValueError("an owned exit must not rebook its historical entry fee")
        if self.execution_evidence == "validated_simulation" and (
            self.execution_validation_sha256 is None
        ):
            raise ValueError("simulated execution requires independent execution validation")
        if self.action.endswith("SELL") != (self.position_effect == "close_long"):
            raise ValueError("sell samples must be owned long exits, never short entries")
        return self


class NumericSupport(_Frozen):
    minimum: float
    maximum: float

    @model_validator(mode="after")
    def ordered(self) -> Self:
        if self.minimum > self.maximum:
            raise ValueError("support minimum exceeds maximum")
        return self

    def contains(self, value: float) -> bool:
        return self.minimum <= value <= self.maximum


class FeatureSupport(_Frozen):
    name: str
    numeric: NumericSupport | None
    categories: tuple[str, ...]
    allows_none: bool

    def contains(self, value: FeatureValue) -> bool:
        if value is None:
            return self.allows_none
        if isinstance(value, str):
            return value in self.categories
        return self.numeric is not None and self.numeric.contains(float(value))


class ObservationWindow(_Frozen):
    first_decision_at: AwareDatetime
    last_decision_at: AwareDatetime
    last_label_matured_at: AwareDatetime

    @model_validator(mode="after")
    def causal(self) -> Self:
        if not self.first_decision_at <= self.last_decision_at <= self.last_label_matured_at:
            raise ValueError("observation windows must be causal")
        return self


class EmpiricalEstimate(_Frozen):
    attempts: int = Field(ge=2)
    filled_attempts: int = Field(ge=2)
    fill_probability: Probability
    fill_probability_horizon: Probability
    fill_probability_ttl: Probability
    fill_probability_lower: Probability
    fill_probability_upper: Probability
    conditional_gross_return_bps: float
    conditional_net_return_bps: float
    conditional_standard_error_bps: Nonnegative
    expected_net_edge_bps: float
    standard_error_bps: Nonnegative
    net_lower_bound_bps: float
    mean_filled_fraction: Probability

    @model_validator(mode="after")
    def reconcile(self) -> Self:
        if self.filled_attempts > self.attempts:
            raise ValueError("filled attempts exceed all attempts")
        if self.fill_probability != self.filled_attempts / self.attempts:
            raise ValueError("fill probability must be the empirical observed frequency")
        if not self.fill_probability_lower <= self.fill_probability <= self.fill_probability_upper:
            raise ValueError("fill probability lies outside its uncertainty interval")
        if self.net_lower_bound_bps > self.expected_net_edge_bps:
            raise ValueError("net lower bound cannot exceed the empirical point estimate")
        if self.mean_filled_fraction <= 0:
            raise ValueError("a filled fraction must be positive")
        return self


class PolicyValidation(_Frozen):
    decision_count: int = Field(ge=2)
    selected_attempts: int = Field(ge=2)
    evaluated_trades: int = Field(ge=2)
    expected_net_edge_bps: float
    standard_error_bps: Nonnegative
    net_lower_bound_bps: float
    basis: Literal["frozen_fit_policy_including_no_trade_zeros"] = (
        "frozen_fit_policy_including_no_trade_zeros"
    )

    @model_validator(mode="after")
    def reconcile(self) -> Self:
        if not self.evaluated_trades <= self.selected_attempts <= self.decision_count:
            raise ValueError("policy validation counts do not reconcile")
        if self.net_lower_bound_bps > self.expected_net_edge_bps:
            raise ValueError("policy lower bound exceeds its point estimate")
        return self


class ActionCalibration(_Frozen):
    symbol: str
    family: EconomicFamily
    action: TradeAction
    status: ModelStatus
    reason_codes: tuple[str, ...]
    input_count: int = Field(ge=0)
    fit_count: int = Field(ge=0)
    validation_count: int = Field(ge=0)
    purged_count: int = Field(ge=0)
    fit_sample_ids: tuple[str, ...]
    validation_sample_ids: tuple[str, ...]
    fit_window: ObservationWindow | None
    validation_window: ObservationWindow | None
    fit: EmpiricalEstimate | None
    validation: EmpiricalEstimate | None
    policy_validation: PolicyValidation | None = None
    policy_selected_sample_ids: tuple[str, ...] = ()
    costs: EconomicCosts | None
    feature_support: tuple[FeatureSupport, ...]
    quote_age_support: NumericSupport | None
    latency_support: NumericSupport | None
    spread_support: NumericSupport | None
    notional_support: NumericSupport | None
    spread_cost_ratio: Nonnegative | None
    impact_cost_ratio: Nonnegative | None
    filled_directional_return_bps: float | None
    unfilled_directional_return_bps: float | None
    adverse_selection_gap_bps: float | None
    execution_evidence: tuple[str, ...]
    execution_validation_hashes: tuple[Digest, ...]

    @model_validator(mode="after")
    def reconcile(self) -> Self:
        if self.input_count != self.fit_count + self.validation_count + self.purged_count:
            raise ValueError("action observation counts do not reconcile")
        if (
            len(self.fit_sample_ids) != self.fit_count
            or len(self.validation_sample_ids) != self.validation_count
        ):
            raise ValueError("action counts must match retained identities")
        if self.fit is not None and self.fit.attempts != self.fit_count:
            raise ValueError("fit estimate count disagrees with retained observations")
        if self.validation is not None and self.validation.attempts != self.validation_count:
            raise ValueError("validation estimate count disagrees with retained observations")
        if not set(self.policy_selected_sample_ids).issubset(self.validation_sample_ids):
            raise ValueError("policy validation must use only held-out observations")
        if self.policy_validation is not None and (
            self.policy_validation.decision_count != self.validation_count
            or self.policy_validation.selected_attempts != len(self.policy_selected_sample_ids)
        ):
            raise ValueError("policy validation identities disagree with its counts")
        return self


class EconomicProvenance(_Frozen):
    account_digest: Digest
    source_sha256: Digest
    samples_sha256: Digest
    costs_sha256: Digest
    universe: tuple[str, ...]
    maker_fee_bps: Nonnegative
    taker_fee_bps: Nonnegative
    entry_fee_policy: Literal["entry_liquidity_unconfirmed_use_worst_configured_fee"]
    horizon_seconds: Positive
    cancellation_horizon_seconds: Positive
    validation_fraction: Annotated[float, Field(gt=0, lt=1)]
    embargo_seconds: Nonnegative
    error_rate: Annotated[float, Field(gt=0, lt=0.5)]
    simultaneous_comparisons: int = Field(ge=1)
    minimum_fit_samples: int = Field(ge=2)
    minimum_validation_samples: int = Field(ge=2)
    minimum_filled_samples: int = Field(ge=2)
    evidence_scope: Literal["observed_policy_support_only"] = "observed_policy_support_only"
    time_unit: Literal["seconds"] = "seconds"
    return_unit: Literal["bps"] = "bps"
    uncertainty_method: Literal["studentized_empirical_and_wilson"] = (
        "studentized_empirical_and_wilson"
    )
    latency_method: Literal["remaining_horizon_haircut_with_observed_latency_support"] = (
        "remaining_horizon_haircut_with_observed_latency_support"
    )
    cost_repricing_method: Literal["fit_maximum_observed_cost_exposure_ratio"] = (
        "fit_maximum_observed_cost_exposure_ratio"
    )

    @model_validator(mode="after")
    def representable_uncertainty(self) -> Self:
        alpha = self.error_rate / self.simultaneous_comparisons
        if 1 - alpha / 2 == 1:
            raise ValueError("simultaneous error rate is below floating-point resolution")
        return self


class EconomicModel(_Frozen):
    schema_version: Literal["scalping-economics-v1"] = "scalping-economics-v1"
    algorithm: Literal["purged_empirical_action_groups_v1"] = "purged_empirical_action_groups_v1"
    model_id: Digest = ""
    provenance: EconomicProvenance
    calibrated_at: AwareDatetime
    valid_until: AwareDatetime
    validation_start_at: AwareDatetime | None
    training_label_cutoff_at: AwareDatetime | None
    status: ModelStatus
    reason_codes: tuple[str, ...]
    input_count: int = Field(ge=0)
    fit_count: int = Field(ge=0)
    validation_count: int = Field(ge=0)
    purged_count: int = Field(ge=0)
    evaluated_trades: int = Field(ge=0)
    observed_validation_fills: int = Field(ge=0)
    validation_net_lower_bound_bps: float | None
    observed_validation_net_lower_bound_bps: float | None
    cells: tuple[ActionCalibration, ...]

    @field_validator(
        "calibrated_at", "valid_until", "validation_start_at", "training_label_cutoff_at"
    )
    @classmethod
    def utc_times(cls, value: datetime | None) -> datetime | None:
        return None if value is None else _aware(value)

    @model_validator(mode="after")
    def verify_artifact(self) -> Self:
        if self.valid_until <= self.calibrated_at:
            raise ValueError("model validity must follow calibration")
        if self.provenance.cancellation_horizon_seconds > self.provenance.horizon_seconds:
            raise ValueError("cancellation horizon cannot outlast the prediction")
        if self.input_count != self.fit_count + self.validation_count + self.purged_count:
            raise ValueError("model observation counts do not reconcile")
        identities = [(cell.symbol, cell.family, cell.action) for cell in self.cells]
        if len(set(identities)) != len(identities):
            raise ValueError("duplicate action calibration")
        fit_ids = {identity for cell in self.cells for identity in cell.fit_sample_ids}
        validation_ids = {
            identity for cell in self.cells for identity in cell.validation_sample_ids
        }
        if fit_ids & validation_ids:
            raise ValueError("fit and validation identities overlap")
        for cell in self.cells:
            if cell.fit_window is not None and (
                self.training_label_cutoff_at is None
                or cell.fit_window.last_label_matured_at >= self.training_label_cutoff_at
            ):
                raise ValueError("training labels cross the embargoed decision cutoff")
            if cell.validation_window is not None and (
                self.validation_start_at is None
                or cell.validation_window.first_decision_at < self.validation_start_at
                or cell.validation_window.last_label_matured_at > self.calibrated_at
            ):
                raise ValueError("invalid validation window")
            if cell.status == "validated" and (
                cell.fit is None
                or cell.validation is None
                or cell.fit.net_lower_bound_bps <= 0
                or cell.validation.net_lower_bound_bps <= 0
                or cell.costs is None
                or bool(cell.costs.unpriced_components())
                or cell.policy_validation is None
                or cell.policy_validation.net_lower_bound_bps <= 0
            ):
                raise ValueError(
                    "a validated action needs positive priced fit and holdout evidence"
                )
            if (
                cell.status == "validated"
                and cell.costs is not None
                and cell.fit is not None
                and _fee_floor_reasons(
                    cell.action,
                    cell.costs,
                    self.provenance.maker_fee_bps,
                    self.provenance.taker_fee_bps,
                    fraction=cell.fit.mean_filled_fraction,
                )
            ):
                raise ValueError("validated fees are below the configured ledger fee reserve")
            if (
                cell.status == "validated"
                and cell.costs is not None
                and cell.fit is not None
                and not isclose(
                    cell.fit.conditional_net_return_bps,
                    cell.fit.conditional_gross_return_bps - cell.costs.additional_bps(),
                    rel_tol=1e-12,
                    abs_tol=1e-12,
                )
            ):
                raise ValueError("fit payoff does not reconcile with its cost estimates")
        if (self.status == "validated") != any(c.status == "validated" for c in self.cells):
            raise ValueError("model validation status does not agree with its actions")
        identity = _digest(self.model_dump(mode="json", exclude={"model_id"}))
        if self.model_id and self.model_id != identity:
            raise ValueError("economic model identity does not match its immutable contents")
        object.__setattr__(self, "model_id", identity)
        return self

    def canonical_json(self) -> str:
        return json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":"), allow_nan=False
        )


class ActionValue(_Frozen):
    action: EconomicAction
    available: bool
    reason_codes: tuple[str, ...]
    expected_net_edge_bps: float | None = None
    expected_net_pnl_usd: float | None = None
    conservative_net_edge_bps: float | None = None
    conditional_gross_return_bps: float | None = None
    conditional_net_return_bps: float | None = None
    fill_probability: Probability | None = None
    fill_probability_horizon: Probability | None = None
    fill_probability_ttl: Probability | None = None
    fill_probability_lower: Probability | None = None
    uncertainty: Nonnegative | None = None
    required_buffer_bps: Nonnegative | None = None
    latency_haircut_bps: Nonnegative | None = None
    spread_repricing_bps: Nonnegative | None = None
    impact_repricing_bps: Nonnegative | None = None
    realized_favorable_move_bps: Nonnegative | None = None
    price_move_haircut_bps: Nonnegative | None = None
    viable_latency_seconds: Nonnegative | None = None
    costs: EconomicCosts | None = None
    confidence: None = None
    uncertainty_unit: Literal["standard_error_bps"] = "standard_error_bps"

    @model_validator(mode="after")
    def consistent_availability(self) -> Self:
        numerical = (
            self.expected_net_edge_bps,
            self.expected_net_pnl_usd,
            self.conservative_net_edge_bps,
            self.conditional_gross_return_bps,
            self.conditional_net_return_bps,
            self.fill_probability,
            self.fill_probability_horizon,
            self.fill_probability_ttl,
            self.fill_probability_lower,
            self.uncertainty,
            self.required_buffer_bps,
            self.latency_haircut_bps,
            self.viable_latency_seconds,
            self.spread_repricing_bps,
            self.impact_repricing_bps,
            self.realized_favorable_move_bps,
            self.price_move_haircut_bps,
        )
        if not self.available:
            if (
                not self.reason_codes
                or any(value is not None for value in numerical)
                or self.costs is not None
            ):
                raise ValueError("unavailable actions need reasons and null valuations")
        elif any(value is None for value in numerical[:3]):
            raise ValueError("available actions need explicit economic values")
        elif self.action != "NO_TRADE":
            if (
                any(value is None for value in numerical)
                or self.costs is None
                or self.fill_probability is None
                or self.fill_probability <= 0
            ):
                raise ValueError(
                    "an available trade needs empirically known costs, fill, and uncertainty"
                )
            if (
                self.conservative_net_edge_bps is not None
                and self.expected_net_edge_bps is not None
                and self.conservative_net_edge_bps > self.expected_net_edge_bps
            ):
                raise ValueError("a conservative value cannot exceed its point estimate")
            if (
                self.conditional_net_return_bps is not None
                and self.conditional_gross_return_bps is not None
                and self.spread_repricing_bps is not None
                and self.impact_repricing_bps is not None
                and not isclose(
                    self.conditional_net_return_bps,
                    self.conditional_gross_return_bps
                    - self.costs.additional_bps()
                    - self.spread_repricing_bps
                    - self.impact_repricing_bps,
                    rel_tol=1e-12,
                    abs_tol=1e-12,
                )
            ):
                raise ValueError("action payoff does not reconcile with its cost estimates")
        if self.action == "NO_TRADE" and (
            not self.available
            or self.expected_net_edge_bps != 0
            or self.expected_net_pnl_usd != 0
            or self.conservative_net_edge_bps != 0
            or self.uncertainty != 0
            or self.fill_probability is not None
            or self.fill_probability_horizon is not None
            or self.fill_probability_ttl is not None
            or self.costs is not None
        ):
            raise ValueError("NO_TRADE is the explicit zero-risk cash-flow baseline")
        return self


class EconomicDecision(_Frozen):
    decision_id: Digest = ""
    selected_action: EconomicAction
    symbol: str
    family: str
    evaluated_at: AwareDatetime
    valid_until: AwareDatetime | None
    prediction_expires_at: AwareDatetime | None
    reference_mid_price: Positive | None
    current_mid_price: Positive | None
    account_digest: Digest | None
    model_id: Digest | None
    provenance: EconomicProvenance | None
    features_sha256: Digest
    quote_age_seconds: Nonnegative
    decision_latency_seconds: Nonnegative
    spread_bps: Nonnegative
    notional_usd: Positive
    owned_inventory_notional_usd: Nonnegative
    prediction_horizon_seconds: Positive | None
    cancellation_horizon_seconds: Positive | None
    expected_net_edge_bps: float
    expected_net_pnl_usd: float
    conservative_net_edge_bps: float
    uncertainty: Nonnegative | None
    required_buffer_bps: Nonnegative | None
    fill_probability: Probability | None
    fill_probability_horizon: Probability | None
    fill_probability_ttl: Probability | None
    viable_latency_seconds: Nonnegative | None
    profitability_validated: bool
    reason_codes: tuple[str, ...]
    action_values: tuple[ActionValue, ...]
    confidence: None = None
    uncertainty_unit: Literal["standard_error_bps"] = "standard_error_bps"

    @model_validator(mode="after")
    def consistent_selection(self) -> Self:
        if tuple(value.action for value in self.action_values) != ACTIONS:
            raise ValueError("a decision must disclose each action exactly once in canonical order")
        chosen = next(value for value in self.action_values if value.action == self.selected_action)
        if not chosen.available:
            raise ValueError("an unavailable action cannot be selected")
        for name in (
            "expected_net_edge_bps",
            "expected_net_pnl_usd",
            "conservative_net_edge_bps",
            "uncertainty",
            "required_buffer_bps",
            "fill_probability",
            "fill_probability_horizon",
            "fill_probability_ttl",
            "viable_latency_seconds",
        ):
            if getattr(self, name) != getattr(chosen, name):
                raise ValueError(f"selected {name} disagrees with its action value")
        if any(
            value.available
            and value.conservative_net_edge_bps is not None
            and value.conservative_net_edge_bps > self.conservative_net_edge_bps
            for value in self.action_values
        ):
            raise ValueError("selected action does not maximize conservative economic value")
        if not isclose(
            self.expected_net_pnl_usd,
            self.notional_usd * self.expected_net_edge_bps / 10_000,
            rel_tol=1e-12,
            abs_tol=1e-12,
        ):
            raise ValueError("expected P&L must use the bound requested notional")
        trading = self.selected_action != "NO_TRADE"
        if self.profitability_validated != trading:
            raise ValueError("only a selected, validated positive action can authorize a trade")
        if trading and (
            self.model_id is None
            or self.provenance is None
            or self.account_digest != self.provenance.account_digest
            or self.conservative_net_edge_bps <= 0
            or self.expected_net_edge_bps <= 0
            or self.valid_until is None
            or self.valid_until <= self.evaluated_at
            or self.prediction_expires_at is None
            or self.prediction_expires_at <= self.evaluated_at
            or self.reference_mid_price is None
            or self.current_mid_price is None
        ):
            raise ValueError("a trade requires current, account-bound, positive model evidence")
        if (
            trading
            and self.provenance is not None
            and (
                self.prediction_horizon_seconds != self.provenance.horizon_seconds
                or self.cancellation_horizon_seconds != self.provenance.cancellation_horizon_seconds
                or (
                    self.prediction_expires_at is not None
                    and self.prediction_expires_at
                    > self.evaluated_at + timedelta(seconds=self.provenance.horizon_seconds)
                )
                or (
                    self.prediction_expires_at is not None
                    and self.valid_until is not None
                    and self.valid_until
                    > self.prediction_expires_at
                    - timedelta(seconds=self.provenance.cancellation_horizon_seconds)
                )
            )
        ):
            raise ValueError("entry validity must reserve the original prediction's calibrated TTL")
        if self.selected_action.endswith("SELL") and (
            self.owned_inventory_notional_usd < self.notional_usd
        ):
            raise ValueError("sells may only close sufficient owned inventory")
        if self.selected_action.endswith("BUY") and self.owned_inventory_notional_usd != 0:
            raise ValueError("new entries require flat owned inventory")
        identity = _digest(self.model_dump(mode="json", exclude={"decision_id"}))
        if self.decision_id and self.decision_id != identity:
            raise ValueError("economic decision identity does not match its contents")
        object.__setattr__(self, "decision_id", identity)
        return self


def _range(values: Sequence[float]) -> NumericSupport:
    return NumericSupport(minimum=min(values), maximum=max(values))


def _window(rows: Sequence[EconomicSample]) -> ObservationWindow | None:
    if not rows:
        return None
    return ObservationWindow(
        first_decision_at=min(row.decision_at for row in rows),
        last_decision_at=max(row.decision_at for row in rows),
        last_label_matured_at=max(row.label_matured_at for row in rows),
    )


def _nonoverlapping(rows: Sequence[EconomicSample]) -> list[EconomicSample]:
    retained: list[EconomicSample] = []
    next_at: datetime | None = None
    for row in rows:
        if next_at is None or row.decision_at >= next_at:
            retained.append(row)
            next_at = row.decision_at + timedelta(
                seconds=max(row.horizon_seconds, row.cancellation_horizon_seconds)
            )
    return retained


def _feature_support(rows: Sequence[EconomicSample]) -> tuple[FeatureSupport, ...]:
    names = sorted({name for row in rows for name in row.features})
    result: list[FeatureSupport] = []
    for name in names:
        values = [row.features[name] for row in rows if name in row.features]
        numbers = [float(value) for value in values if isinstance(value, (float, int))]
        result.append(
            FeatureSupport(
                name=name,
                numeric=_range(numbers) if numbers else None,
                categories=tuple(sorted({value for value in values if isinstance(value, str)})),
                allows_none=None in values,
            )
        )
    return tuple(result)


def _state_reasons(
    support: tuple[FeatureSupport, ...], features: Mapping[str, FeatureValue]
) -> tuple[str, ...]:
    return tuple(
        f"MISSING_FEATURE:{item.name}" if item.name not in features else f"OOD_FEATURE:{item.name}"
        for item in support
        if item.name not in features or not item.contains(features[item.name])
    )


@lru_cache(maxsize=512)
def _student_critical(error_rate: float, degrees: int) -> float:
    """Invert the integer-df Student CDF using its cosine-integral recurrence."""

    def cdf(value: float) -> float:
        angle = atan(value / sqrt(degrees))
        power = degrees - 1
        numerator = angle if power % 2 == 0 else sin(angle)
        denominator = pi / 2 if power % 2 == 0 else 1.0
        for exponent in range(2 if power % 2 == 0 else 3, power + 1, 2):
            numerator = (
                sin(angle) * cos(angle) ** (exponent - 1) / exponent
                + (exponent - 1) * numerator / exponent
            )
            denominator *= (exponent - 1) / exponent
        return 0.5 + numerator / (2 * denominator)

    low, high = 0.0, 1.0
    while cdf(high) < 1 - error_rate:
        high *= 2
    for _ in range(64):
        middle = (low + high) / 2
        if cdf(middle) < 1 - error_rate:
            low = middle
        else:
            high = middle
    return high


def _wilson(successes: int, count: int, error_rate: float) -> tuple[float, float]:
    z = NormalDist().inv_cdf(1 - error_rate / 2)
    probability = successes / count
    denominator = 1 + z * z / count
    center = (probability + z * z / (2 * count)) / denominator
    radius = (
        z
        * sqrt(probability * (1 - probability) / count + z * z / (4 * count * count))
        / denominator
    )
    return (
        max(0.0, min(probability, center - radius)),
        min(1.0, max(probability, center + radius)),
    )


def _fee_floors(
    action: TradeAction, costs: EconomicCosts, maker: float, taker: float
) -> dict[str, float]:
    entry = max(maker, taker) if action.endswith("BUY") else 0.0
    return (
        {"entry_fee": entry, "exit_fee": taker}
        if costs.entry_fee is not None
        else {"fees": entry + taker}
    )


def _priced_costs(row: EconomicSample, maker: float, taker: float) -> EconomicCosts:
    values = row.costs.model_dump()
    floors = _fee_floors(row.action, row.costs, maker, taker)
    for name, component in row.costs.fee_components():
        if component.bps is None and not component.included_in_return:
            if name == "entry_fee":
                provenance = (
                    "entry_liquidity_unconfirmed_use_worst_configured_fee"
                    if row.action.endswith("BUY")
                    else "owned_exit_has_no_new_entry_fee"
                )
            elif name == "exit_fee":
                provenance = "cash_exit_uses_configured_taker_fee"
            else:
                provenance = (
                    "entry_liquidity_unconfirmed_use_worst_configured_fee; "
                    "cash_exit_uses_configured_taker_fee; owned_exit_has_no_new_entry_fee"
                )
            values[name] = CostEstimate(
                bps=floors[name],
                kind="estimated",
                provenance=provenance,
            )
    return EconomicCosts.model_validate(values)


def _fee_floor_reasons(
    action: TradeAction, costs: EconomicCosts, maker: float, taker: float, *, fraction: float = 1.0
) -> list[str]:
    reasons: list[str] = []
    floors = _fee_floors(action, costs, maker, taker)
    for name, component in costs.fee_components():
        floor = floors[name] * fraction
        if component.bps is None:
            reasons.append("UNPRICED_EMBEDDED_FEES")
        elif component.bps < floor and not isclose(
            component.bps, floor, rel_tol=1e-12, abs_tol=1e-12
        ):
            reasons.append("FEES_BELOW_CONFIGURED_SCENARIO")
    return reasons


def _cost_reasons(
    row: EconomicSample, costs: EconomicCosts, maker: float, taker: float
) -> list[str]:
    reasons = [f"UNPRICED_{name.upper()}" for name in costs.unpriced_components()]
    reasons.extend(_fee_floor_reasons(row.action, costs, maker, taker))
    return reasons


def _estimate(
    rows: Sequence[EconomicSample], costs: Mapping[str, EconomicCosts], alpha: float
) -> EmpiricalEstimate:
    filled = [row for row in rows if row.filled]
    gross = [
        row.filled_fraction * row.gross_return_bps
        for row in filled
        if row.gross_return_bps is not None
    ]
    conditional = [
        row.filled_fraction * (row.gross_return_bps - costs[row.sample_id].additional_bps())
        for row in filled
        if row.gross_return_bps is not None
    ]
    attempts = [*conditional, *([0.0] * (len(rows) - len(filled)))]
    conditional_se = stdev(conditional) / sqrt(len(conditional))
    attempt_se = stdev(attempts) / sqrt(len(attempts))
    probability = len(filled) / len(rows)
    probability_low, probability_high = _wilson(len(filled), len(rows), alpha)
    conditional_low = fmean(conditional) - (
        _student_critical(alpha, len(conditional) - 1) * conditional_se
    )
    product_lower = conditional_low * (
        probability_low if conditional_low >= 0 else probability_high
    )
    return EmpiricalEstimate(
        attempts=len(rows),
        filled_attempts=len(filled),
        fill_probability=probability,
        fill_probability_horizon=sum(
            row.fill_delay_seconds is not None and row.fill_delay_seconds <= row.horizon_seconds
            for row in rows
        )
        / len(rows),
        fill_probability_ttl=sum(
            row.fill_delay_seconds is not None
            and row.fill_delay_seconds <= row.cancellation_horizon_seconds
            for row in rows
        )
        / len(rows),
        fill_probability_lower=probability_low,
        fill_probability_upper=probability_high,
        conditional_gross_return_bps=fmean(gross),
        conditional_net_return_bps=fmean(conditional),
        conditional_standard_error_bps=conditional_se,
        expected_net_edge_bps=fmean(attempts),
        standard_error_bps=attempt_se,
        net_lower_bound_bps=min(
            fmean(attempts) - _student_critical(alpha, len(attempts) - 1) * attempt_se,
            product_lower,
        ),
        mean_filled_fraction=fmean(row.filled_fraction for row in filled),
    )


def _mean_costs(
    rows: Sequence[EconomicSample], costs: Mapping[str, EconomicCosts]
) -> EconomicCosts:
    filled = [row for row in rows if row.filled]
    first = costs[filled[0].sample_id]
    components: dict[str, object] = {}
    for name in (*(name for name, _ in first.active_components()), "residual"):
        estimates = [getattr(costs[row.sample_id], name) for row in filled]
        if all(estimate is None for estimate in estimates):
            components[name] = None
            continue
        known = all(estimate is not None and estimate.bps is not None for estimate in estimates)
        included = estimates[0].included_in_return
        bps = (
            fmean(
                row.filled_fraction * estimate.bps
                for row, estimate in zip(filled, estimates, strict=True)
            )
            if known
            else None
        )
        provenance = (
            "fit-only conditional estimate; "
            + (
                "entry_liquidity_unconfirmed_use_worst_configured_fee; "
                if name in {"fees", "entry_fee", "exit_fee"}
                else ""
            )
            + "sources:"
            + _digest([estimate.model_dump(mode="json") for estimate in estimates])
        )
        if name == "entry_fee" and all(
            estimate.provenance == "entry_liquidity_unconfirmed_use_worst_configured_fee"
            for estimate in estimates
        ):
            provenance = "entry_liquidity_unconfirmed_use_worst_configured_fee"
        components[name] = CostEstimate(
            bps=bps,
            included_in_return=included,
            kind=(
                "estimated"
                if known and name in {"fees", "entry_fee", "exit_fee"}
                else "embedded"
                if included
                else "conservative_allowance"
                if name == "residual"
                else "estimated"
                if known
                else "unknown"
            ),
            provenance=provenance,
            validation_sha256=(
                _digest([estimate.validation_sha256 for estimate in estimates])
                if name == "residual"
                else None
            ),
        )
    return EconomicCosts.model_validate(
        {
            **components,
            "residual_covers": first.residual_covers,
        }
    )


def _cost_basis(costs: EconomicCosts) -> tuple[object, ...]:
    return (
        *((name, component.included_in_return) for name, component in costs.active_components()),
        costs.residual is not None,
        costs.residual_covers,
    )


def _cost_ratio(
    rows: Sequence[EconomicSample],
    costs: Mapping[str, EconomicCosts],
    name: Literal["spread", "impact"],
) -> float | None:
    ratios: list[float] = []
    for row in rows:
        if not row.filled:
            continue
        component = costs[row.sample_id].component(name)
        denominator = row.spread_bps if name == "spread" else row.notional_usd
        if component.bps is None or (denominator == 0 and component.bps != 0):
            return None
        ratios.append(row.filled_fraction * component.bps / denominator if denominator else 0.0)
    return max(ratios) if ratios else None


def _policy_estimate(
    rows: Sequence[EconomicSample],
    selected: Sequence[EconomicSample],
    costs: Mapping[str, EconomicCosts],
    alpha: float,
) -> PolicyValidation:
    selected_estimate = _estimate(selected, costs, alpha)
    identities = {row.sample_id for row in selected}
    outcomes = [
        row.filled_fraction * (row.gross_return_bps - costs[row.sample_id].additional_bps())
        if row.sample_id in identities and row.filled and row.gross_return_bps is not None
        else 0.0
        for row in rows
    ]
    standard_error = stdev(outcomes) / sqrt(len(outcomes))
    selection_lower, selection_upper = _wilson(len(selected), len(rows), alpha)
    selection_bound = selected_estimate.net_lower_bound_bps * (
        selection_lower if selected_estimate.net_lower_bound_bps >= 0 else selection_upper
    )
    expected = fmean(outcomes)
    return PolicyValidation(
        decision_count=len(rows),
        selected_attempts=len(selected),
        evaluated_trades=selected_estimate.filled_attempts,
        expected_net_edge_bps=expected,
        standard_error_bps=standard_error,
        net_lower_bound_bps=min(
            expected - _student_critical(alpha, len(rows) - 1) * standard_error,
            selection_bound,
        ),
    )


def _calibrate_cell(
    rows: Sequence[EconomicSample],
    split: datetime,
    cutoff: datetime,
    provenance: EconomicProvenance,
) -> ActionCalibration:
    fit = _nonoverlapping(
        [row for row in rows if row.decision_at < split and row.label_matured_at < cutoff]
    )
    validation = _nonoverlapping([row for row in rows if row.decision_at >= split])
    support = _feature_support(fit)
    reasons: list[str] = []
    if len(fit) < provenance.minimum_fit_samples:
        reasons.append("INSUFFICIENT_FIT_SUPPORT")
    if len(validation) < provenance.minimum_validation_samples:
        reasons.append("INSUFFICIENT_VALIDATION_SUPPORT")
    if (
        sum(row.filled for row in fit) < provenance.minimum_filled_samples
        or sum(row.filled for row in validation) < provenance.minimum_filled_samples
    ):
        reasons.append("UNKNOWN_FILL_CONDITIONAL_UNCERTAINTY")
    if not any(item.numeric is not None or item.categories for item in support):
        reasons.append("NO_OBSERVED_STATE_FEATURES")
    if any(_state_reasons(support, row.features) for row in fit):
        reasons.append("INCONSISTENT_FIT_FEATURE_SCHEMA")
    if any(row.execution_evidence == "unsupported_counterfactual" for row in (*fit, *validation)):
        reasons.append("UNSUPPORTED_EXECUTION_EVIDENCE")
    if any(
        row.filled
        and row.fill_delay_seconds is not None
        and row.fill_delay_seconds > min(row.horizon_seconds, row.cancellation_horizon_seconds)
        for row in (*fit, *validation)
    ):
        reasons.append("FILL_OUTSIDE_PREDICTION_OR_CANCELLATION_BUDGET")
    if any(
        max(row.quote_age_seconds, row.decision_latency_seconds) + row.cancellation_horizon_seconds
        >= row.horizon_seconds
        for row in (*fit, *validation)
    ):
        reasons.append("OBSERVATION_OUTSIDE_LATENCY_BUDGET")
    costs = {
        row.sample_id: _priced_costs(row, provenance.maker_fee_bps, provenance.taker_fee_bps)
        for row in (*fit, *validation)
        if row.filled
    }
    for row in (*fit, *validation):
        if row.filled:
            reasons.extend(
                _cost_reasons(
                    row, costs[row.sample_id], provenance.maker_fee_bps, provenance.taker_fee_bps
                )
            )
    if (
        len(
            {
                (row.return_basis, _cost_basis(costs[row.sample_id]))
                for row in (*fit, *validation)
                if row.filled
            }
        )
        > 1
    ):
        reasons.append("INCONSISTENT_COST_OR_RETURN_BASIS")
    quote_support = _range([row.quote_age_seconds for row in fit]) if fit else None
    latency_support = _range([row.decision_latency_seconds for row in fit]) if fit else None
    spread_support = _range([row.spread_bps for row in fit]) if fit else None
    notional_support = _range([row.notional_usd for row in fit]) if fit else None
    validation_ood = any(
        _state_reasons(support, row.features)
        or any(
            bounds is None or not bounds.contains(value)
            for bounds, value in (
                (quote_support, row.quote_age_seconds),
                (latency_support, row.decision_latency_seconds),
                (spread_support, row.spread_bps),
                (notional_support, row.notional_usd),
            )
        )
        for row in validation
    )
    fit_estimate = validation_estimate = mean_costs = None
    status: ModelStatus = "no_support"
    if not reasons:
        alpha = provenance.error_rate / provenance.simultaneous_comparisons
        fit_estimate = _estimate(fit, costs, alpha)
        validation_estimate = _estimate(validation, costs, alpha)
        mean_costs = _mean_costs(fit, costs)
        if fit_estimate.net_lower_bound_bps <= 0:
            reasons.append("NONPOSITIVE_FIT_NET_LOWER_BOUND")
        if validation_estimate.net_lower_bound_bps <= 0:
            reasons.append("NONPOSITIVE_HELDOUT_NET_LOWER_BOUND")
        if validation_ood:
            reasons.append("VALIDATION_OOD")
        status = "failed_validation" if reasons else "no_support"
    filled_directional = [
        row.directional_return_bps
        for row in fit
        if row.filled and row.directional_return_bps is not None
    ]
    unfilled_directional = [
        row.directional_return_bps
        for row in fit
        if not row.filled and row.directional_return_bps is not None
    ]
    filled_mean = fmean(filled_directional) if filled_directional else None
    unfilled_mean = fmean(unfilled_directional) if unfilled_directional else None
    cell = ActionCalibration(
        symbol=rows[0].symbol,
        family=rows[0].family,
        action=rows[0].action,
        status=status,
        reason_codes=tuple(sorted(set(reasons))) or ("POLICY_VALIDATION_PENDING",),
        input_count=len(rows),
        fit_count=len(fit),
        validation_count=len(validation),
        purged_count=len(rows) - len(fit) - len(validation),
        fit_sample_ids=tuple(row.sample_id for row in fit),
        validation_sample_ids=tuple(row.sample_id for row in validation),
        fit_window=_window(fit),
        validation_window=_window(validation),
        fit=fit_estimate,
        validation=validation_estimate,
        costs=mean_costs,
        feature_support=support,
        quote_age_support=quote_support,
        latency_support=latency_support,
        spread_support=spread_support,
        notional_support=notional_support,
        spread_cost_ratio=_cost_ratio(fit, costs, "spread"),
        impact_cost_ratio=_cost_ratio(fit, costs, "impact"),
        filled_directional_return_bps=filled_mean,
        unfilled_directional_return_bps=unfilled_mean,
        adverse_selection_gap_bps=(
            unfilled_mean - filled_mean
            if filled_mean is not None and unfilled_mean is not None
            else None
        ),
        execution_evidence=tuple(sorted({row.execution_evidence for row in (*fit, *validation)})),
        execution_validation_hashes=tuple(
            sorted(
                {
                    row.execution_validation_sha256
                    for row in (*fit, *validation)
                    if row.execution_validation_sha256 is not None
                }
            )
        ),
    )
    if fit_estimate is None or validation_estimate is None or mean_costs is None:
        return cell
    selected: list[EconomicSample] = []
    for row in validation:
        value = _score_fit(
            cell,
            provenance,
            features=row.features,
            quote_age=row.quote_age_seconds,
            latency=row.decision_latency_seconds,
            spread=row.spread_bps,
            notional=row.notional_usd,
        )
        if (
            value.available
            and value.conservative_net_edge_bps is not None
            and value.conservative_net_edge_bps > 0
        ):
            selected.append(row)
    policy_validation = None
    if (
        len(selected) < provenance.minimum_validation_samples
        or sum(row.filled for row in selected) < provenance.minimum_filled_samples
    ):
        status = "failed_validation" if reasons else "no_support"
        reasons.append("UNKNOWN_HELDOUT_POLICY_UNCERTAINTY")
    else:
        policy_validation = _policy_estimate(
            validation, selected, costs, provenance.error_rate / provenance.simultaneous_comparisons
        )
        if policy_validation.net_lower_bound_bps <= 0:
            reasons.append("NONPOSITIVE_HELDOUT_POLICY_NET_LOWER_BOUND")
        status = "failed_validation" if reasons else "validated"
    return ActionCalibration.model_validate(
        {
            **cell.model_dump(),
            "status": status,
            "reason_codes": tuple(sorted(set(reasons))) or ("POSITIVE_CHRONOLOGICAL_NET_EVIDENCE",),
            "policy_validation": policy_validation,
            "policy_selected_sample_ids": tuple(row.sample_id for row in selected),
        }
    )


def calibrate_model(
    samples: Sequence[EconomicSample],
    *,
    account_digest: str,
    source_sha256: str,
    maker_fee_bps: float,
    taker_fee_bps: float,
    horizon_seconds: float,
    cancellation_horizon_seconds: float,
    calibrated_at: datetime,
    valid_until: datetime,
    validation_fraction: float = 0.3,
    embargo_seconds: float | None = None,
    error_rate: float = 0.05,
    minimum_fit_samples: int = 4,
    minimum_validation_samples: int = 4,
    minimum_filled_samples: int = 2,
) -> EconomicModel:
    """Fit once, gate on held-out net economics, and return even a rejected artifact.

    ``calibrated_at`` is the information cutoff, not an inferred wall clock.
    Every label must already be mature. ``valid_until`` is an explicit operational
    expiry, not a promised stationary lifetime. The split uses distinct decision
    timestamps so simultaneous observations never straddle the boundary.
    Order style never establishes maker liquidity. Opening fees reserve the
    greater configured rate; cash exits reserve the configured taker rate.
    Known amounts below that prospective budget are rejected, not silently
    relabeled actual fees. Partial fee inclusion requires separate fee legs.
    """
    calibrated_at, valid_until = _aware(calibrated_at), _aware(valid_until)
    maker_fee_bps = _number(maker_fee_bps, "maker_fee_bps")
    taker_fee_bps = _number(taker_fee_bps, "taker_fee_bps")
    horizon_seconds = _number(horizon_seconds, "horizon_seconds", positive=True)
    cancellation_horizon_seconds = _number(
        cancellation_horizon_seconds, "cancellation_horizon_seconds", positive=True
    )
    rows = sorted(
        (EconomicSample.model_validate(sample) for sample in samples),
        key=lambda row: (row.decision_at, row.sample_id),
    )
    if len({row.sample_id for row in rows}) != len(rows):
        raise ValueError("duplicate sample IDs are not independent observations")
    if len({(r.decision_at, r.symbol, r.family, r.action) for r in rows}) != len(rows):
        raise ValueError("duplicate decision/action observations cannot inflate support")
    for row in rows:
        if row.account_digest != account_digest or row.source_sha256 != source_sha256:
            raise ValueError("sample account or source does not match calibration provenance")
        if row.label_matured_at > calibrated_at or row.decision_at >= calibrated_at:
            raise ValueError("future or immature labels cannot be used at the training cutoff")
        if (
            row.horizon_seconds != horizon_seconds
            or row.cancellation_horizon_seconds != cancellation_horizon_seconds
        ):
            raise ValueError("sample horizons or time units do not match the model")
    grouped: dict[tuple[str, EconomicFamily, TradeAction], list[EconomicSample]] = defaultdict(list)
    for row in rows:
        grouped[(row.symbol, row.family, row.action)].append(row)
    provenance = EconomicProvenance(
        account_digest=account_digest,
        source_sha256=source_sha256,
        samples_sha256=_digest([row.model_dump(mode="json") for row in rows]),
        costs_sha256=_digest(
            {
                "maker_fee_bps": maker_fee_bps,
                "taker_fee_bps": taker_fee_bps,
                "entry_fee_policy": "entry_liquidity_unconfirmed_use_worst_configured_fee",
                "sample_costs": [row.costs.model_dump(mode="json") for row in rows],
            }
        ),
        universe=tuple(sorted({row.symbol for row in rows})),
        maker_fee_bps=maker_fee_bps,
        taker_fee_bps=taker_fee_bps,
        entry_fee_policy="entry_liquidity_unconfirmed_use_worst_configured_fee",
        horizon_seconds=horizon_seconds,
        cancellation_horizon_seconds=cancellation_horizon_seconds,
        validation_fraction=validation_fraction,
        embargo_seconds=(
            max(horizon_seconds, cancellation_horizon_seconds)
            if embargo_seconds is None
            else embargo_seconds
        ),
        error_rate=error_rate,
        simultaneous_comparisons=max(1, len(grouped)) * 12,
        minimum_fit_samples=minimum_fit_samples,
        minimum_validation_samples=minimum_validation_samples,
        minimum_filled_samples=minimum_filled_samples,
    )
    times = sorted({row.decision_at for row in rows})
    validation_decisions = ceil(len(times) * Fraction(str(provenance.validation_fraction)))
    split = times[min(len(times) - 1, max(1, len(times) - validation_decisions))] if times else None
    cutoff = split - timedelta(seconds=provenance.embargo_seconds) if split else None
    cells = (
        tuple(_calibrate_cell(grouped[key], split, cutoff, provenance) for key in sorted(grouped))
        if split is not None and cutoff is not None
        else ()
    )
    status: ModelStatus = (
        "validated"
        if any(cell.status == "validated" for cell in cells)
        else "failed_validation"
        if any(cell.status == "failed_validation" for cell in cells)
        else "no_support"
    )
    bounds = [
        cell.policy_validation.net_lower_bound_bps
        for cell in cells
        if cell.policy_validation is not None
    ]
    observed_bounds = [
        cell.validation.net_lower_bound_bps for cell in cells if cell.validation is not None
    ]
    return EconomicModel(
        provenance=provenance,
        calibrated_at=calibrated_at,
        valid_until=valid_until,
        validation_start_at=split,
        training_label_cutoff_at=cutoff,
        status=status,
        reason_codes=(
            ("POSITIVE_CHRONOLOGICAL_NET_EVIDENCE",)
            if status == "validated"
            else tuple(sorted({reason for cell in cells for reason in cell.reason_codes}))
            or ("NO_SAMPLES",)
        ),
        input_count=len(rows),
        fit_count=sum(cell.fit_count for cell in cells),
        validation_count=sum(cell.validation_count for cell in cells),
        purged_count=sum(cell.purged_count for cell in cells),
        evaluated_trades=sum(
            cell.policy_validation.evaluated_trades
            for cell in cells
            if cell.policy_validation is not None
        ),
        observed_validation_fills=sum(
            cell.validation.filled_attempts for cell in cells if cell.validation is not None
        ),
        validation_net_lower_bound_bps=min(bounds) if bounds else None,
        observed_validation_net_lower_bound_bps=min(observed_bounds) if observed_bounds else None,
        cells=cells,
    )


def _unavailable(action: EconomicAction, *reasons: str) -> ActionValue:
    return ActionValue(action=action, available=False, reason_codes=tuple(sorted(set(reasons))))


def _value(
    cell: ActionCalibration,
    provenance: EconomicProvenance,
    *,
    features: Mapping[str, FeatureValue],
    quote_age: float,
    latency: float,
    spread: float,
    notional: float,
) -> ActionValue:
    if (
        cell.status != "validated"
        or cell.fit is None
        or cell.validation is None
        or cell.costs is None
        or cell.policy_validation is None
    ):
        return _unavailable(cell.action, *cell.reason_codes)
    return _score_fit(
        cell,
        provenance,
        features=features,
        quote_age=quote_age,
        latency=latency,
        spread=spread,
        notional=notional,
    )


def _score_fit(
    cell: ActionCalibration,
    provenance: EconomicProvenance,
    *,
    features: Mapping[str, FeatureValue],
    quote_age: float,
    latency: float,
    spread: float,
    notional: float,
) -> ActionValue:
    if cell.fit is None or cell.costs is None:
        raise ValueError("a frozen prospective score requires fit-only economic estimates")
    reasons = list(_state_reasons(cell.feature_support, features))
    for bounds, value, reason in (
        (cell.quote_age_support, quote_age, "OOD_QUOTE_AGE"),
        (cell.latency_support, latency, "OOD_DECISION_LATENCY"),
        (cell.spread_support, spread, "OOD_SPREAD"),
        (cell.notional_support, notional, "OOD_NOTIONAL"),
    ):
        if bounds is None or not bounds.contains(value):
            reasons.append(reason)
    elapsed = max(quote_age, latency)
    if elapsed >= provenance.horizon_seconds:
        reasons.append("LATENCY_EXCEEDS_PREDICTION_HORIZON")
    if elapsed + provenance.cancellation_horizon_seconds >= provenance.horizon_seconds:
        reasons.append("INSUFFICIENT_HORIZON_FOR_CALIBRATED_TTL")
    repricing: dict[str, float] = {}
    for name, current, bounds, ratio in (
        ("spread", spread, cell.spread_support, cell.spread_cost_ratio),
        ("impact", notional, cell.notional_support, cell.impact_cost_ratio),
    ):
        previous = cell.costs.spread if name == "spread" else cell.costs.impact
        if ratio is None:
            if bounds is None or current > bounds.minimum:
                reasons.append(f"UNPRICED_{name.upper()}_CHANGE")
            repricing[name] = 0.0
        elif previous.bps is None:
            raise ValueError("cost exposure ratio is missing its priced reference")
        else:
            repricing[name] = max(0.0, ratio * current - previous.bps)
    if reasons:
        return _unavailable(cell.action, *reasons)
    fit = cell.fit
    haircut = max(0.0, fit.conditional_gross_return_bps) * elapsed / provenance.horizon_seconds
    cost_adjustment = sum(repricing.values())
    conditional = fit.conditional_net_return_bps - haircut - cost_adjustment
    expected = fit.fill_probability * conditional
    conservative = fit.net_lower_bound_bps - (
        fit.fill_probability_upper * (haircut + cost_adjustment)
    )
    if cell.latency_support is None:
        raise ValueError("validated calibration is missing latency support")
    return ActionValue(
        action=cell.action,
        available=True,
        reason_codes=(
            ("POSITIVE_CONSERVATIVE_ACTION_VALUE",)
            if conservative > 0
            else ("UNCERTAINTY_OR_LATENCY_ERASES_NET_EDGE",)
        ),
        expected_net_edge_bps=expected,
        expected_net_pnl_usd=notional * expected / 10_000,
        conservative_net_edge_bps=conservative,
        conditional_gross_return_bps=fit.conditional_gross_return_bps - haircut,
        conditional_net_return_bps=conditional,
        fill_probability=fit.fill_probability,
        fill_probability_horizon=fit.fill_probability_horizon,
        fill_probability_ttl=fit.fill_probability_ttl,
        fill_probability_lower=fit.fill_probability_lower,
        uncertainty=fit.standard_error_bps,
        required_buffer_bps=max(0.0, expected - conservative),
        latency_haircut_bps=haircut,
        spread_repricing_bps=repricing["spread"],
        impact_repricing_bps=repricing["impact"],
        realized_favorable_move_bps=0.0,
        price_move_haircut_bps=0.0,
        viable_latency_seconds=min(
            cell.latency_support.maximum,
            provenance.horizon_seconds - provenance.cancellation_horizon_seconds,
        ),
        costs=cell.costs,
    )


def evaluate_actions(
    model: EconomicModel | None,
    *,
    symbol: str,
    family: str,
    features: Mapping[str, FeatureValue],
    now: datetime,
    quote_age_seconds: float,
    decision_latency_seconds: float,
    spread_bps: float,
    notional_usd: float,
    reference_mid_price: float | None = None,
    account_digest: str | None = None,
    owned_inventory_notional_usd: float = 0,
    maker_fee_bps: float | None = None,
    taker_fee_bps: float | None = None,
) -> EconomicDecision:
    """Use exactly the same evaluator in live and replay; no I/O or wall clock.

    Quote age and decision latency are both measured *at now*. They overlap, so
    the aging fence uses their maximum, not their sum. The TTL begins at order
    submission; a shorter, unvalidated TTL is never substituted to rescue a late
    signal. Buy payoffs include a round trip; sell payoffs are incremental owned
    exits and must not re-charge sunk entry fees. The zero baseline is incremental
    cash flow, not a claim that holding existing inventory has no market risk.
    Mandatory risk/time exits remain the caller's responsibility.
    ``reference_mid_price`` binds the quote underlying the original prediction.
    Missing price context cannot authorize an order.
    """
    now, features = _aware(now), _features(features)
    quote_age = _number(quote_age_seconds, "quote_age_seconds")
    latency = _number(decision_latency_seconds, "decision_latency_seconds")
    spread = _number(spread_bps, "spread_bps")
    notional = _number(notional_usd, "notional_usd", positive=True)
    owned = _number(owned_inventory_notional_usd, "owned_inventory_notional_usd")
    reference_mid = (
        _number(reference_mid_price, "reference_mid_price", positive=True)
        if reference_mid_price is not None
        else None
    )
    if maker_fee_bps is not None:
        maker_fee_bps = _number(maker_fee_bps, "maker_fee_bps")
    if taker_fee_bps is not None:
        taker_fee_bps = _number(taker_fee_bps, "taker_fee_bps")
    reasons: list[str] = []
    if reference_mid is None:
        reasons.append("MISSING_DECISION_PRICE_CONTEXT")
    provenance = None
    if model is None:
        reasons.append("MISSING_MODEL")
    else:
        model = EconomicModel.model_validate(model)
        provenance = model.provenance
        if account_digest is None:
            reasons.append("ACCOUNT_NOT_PROVIDED")
        elif account_digest != provenance.account_digest:
            reasons.append("ACCOUNT_MISMATCH")
        if now < model.calibrated_at:
            reasons.append("FUTURE_MODEL")
        elif now >= model.valid_until:
            reasons.append("STALE_MODEL")
        if symbol not in provenance.universe:
            reasons.append("UNSUPPORTED_SYMBOL")
        if family not in {"momentum", "reversion"}:
            reasons.append("UNSUPPORTED_FAMILY")
        if (maker_fee_bps is not None and maker_fee_bps != provenance.maker_fee_bps) or (
            taker_fee_bps is not None and taker_fee_bps != provenance.taker_fee_bps
        ):
            reasons.append("FEE_SCHEDULE_MISMATCH")
        if max(quote_age, latency) >= provenance.horizon_seconds:
            reasons.append("LATENCY_EXCEEDS_PREDICTION_HORIZON")
    baseline = ActionValue(
        action="NO_TRADE",
        available=True,
        reason_codes=("ZERO_RISK_BASELINE",),
        expected_net_edge_bps=0.0,
        expected_net_pnl_usd=0.0,
        conservative_net_edge_bps=0.0,
        uncertainty=0.0,
        required_buffer_bps=0.0,
    )
    values = [baseline]
    for action in ACTIONS[1:]:
        if action.endswith("SELL") and owned < notional:
            value = _unavailable(action, "SELL_REQUIRES_SUFFICIENT_OWNED_INVENTORY")
        elif action.endswith("BUY") and owned > 0:
            value = _unavailable(action, "ENTRY_REQUIRES_FLAT_INVENTORY")
        elif reasons or model is None or provenance is None:
            value = _unavailable(action, *(reasons or ["MISSING_MODEL"]))
        else:
            cell = next(
                (
                    cell
                    for cell in model.cells
                    if (cell.symbol, cell.family, cell.action) == (symbol, family, action)
                ),
                None,
            )
            value = (
                _unavailable(action, "UNSUPPORTED_ACTION")
                if cell is None
                else _value(
                    cell,
                    provenance,
                    features=features,
                    quote_age=quote_age,
                    latency=latency,
                    spread=spread,
                    notional=notional,
                )
            )
        values.append(value)
    selected = max(
        (value for value in values if value.available),
        key=lambda value: (
            value.conservative_net_edge_bps
            if value.conservative_net_edge_bps is not None
            else float("-inf")
        ),
    )
    decision_reasons: tuple[str, ...]
    if selected.action != "NO_TRADE":
        decision_reasons = ("POSITIVE_CONSERVATIVE_ACTION_VALUE",)
    else:
        decision_reasons = tuple(
            sorted(
                {
                    *reasons,
                    "NO_POSITIVE_CONSERVATIVE_ACTION",
                    *(reason for value in values[1:] for reason in value.reason_codes),
                }
            )
        )
    if (
        selected.expected_net_edge_bps is None
        or selected.expected_net_pnl_usd is None
        or selected.conservative_net_edge_bps is None
    ):
        raise ValueError("selected action has no economic valuation")
    prediction_expires = (
        now + timedelta(seconds=model.provenance.horizon_seconds - max(quote_age, latency))
        if model is not None
        else None
    )
    expires = (
        min(
            model.valid_until,
            prediction_expires - timedelta(seconds=model.provenance.cancellation_horizon_seconds),
        )
        if model is not None and prediction_expires is not None
        else None
    )
    return EconomicDecision(
        selected_action=selected.action,
        symbol=symbol,
        family=family,
        evaluated_at=now,
        valid_until=expires,
        prediction_expires_at=prediction_expires,
        reference_mid_price=reference_mid,
        current_mid_price=reference_mid,
        account_digest=account_digest,
        model_id=model.model_id if model is not None else None,
        provenance=provenance,
        features_sha256=_digest(dict(features)),
        quote_age_seconds=quote_age,
        decision_latency_seconds=latency,
        spread_bps=spread,
        notional_usd=notional,
        owned_inventory_notional_usd=owned,
        prediction_horizon_seconds=provenance.horizon_seconds if provenance is not None else None,
        cancellation_horizon_seconds=(
            provenance.cancellation_horizon_seconds if provenance is not None else None
        ),
        expected_net_edge_bps=selected.expected_net_edge_bps,
        expected_net_pnl_usd=selected.expected_net_pnl_usd,
        conservative_net_edge_bps=selected.conservative_net_edge_bps,
        uncertainty=selected.uncertainty,
        required_buffer_bps=selected.required_buffer_bps,
        fill_probability=selected.fill_probability,
        fill_probability_horizon=selected.fill_probability_horizon,
        fill_probability_ttl=selected.fill_probability_ttl,
        viable_latency_seconds=selected.viable_latency_seconds,
        profitability_validated=selected.action != "NO_TRADE",
        reason_codes=decision_reasons,
        action_values=tuple(values),
    )


def _consume_realized_move(
    value: ActionValue, fit: EmpiricalEstimate, move_bps: float, notional: float
) -> ActionValue:
    if (
        value.conditional_gross_return_bps is None
        or value.conditional_net_return_bps is None
        or value.conservative_net_edge_bps is None
    ):
        raise ValueError("remaining price edge requires a priced conditional payoff")
    gross = value.conditional_gross_return_bps
    remainder = gross - fit.mean_filled_fraction * move_bps
    # Rebasing must never make a negative remainder look less costly.
    remaining_gross = min(remainder, remainder / (1 + move_bps / 10_000))
    haircut = max(0.0, gross - remaining_gross)
    conditional = value.conditional_net_return_bps - haircut
    expected = fit.fill_probability * conditional
    conservative = value.conservative_net_edge_bps - fit.fill_probability_upper * haircut
    reasons = value.reason_codes
    if conservative <= 0 and haircut > 0:
        reasons = ("REALIZED_PRICE_MOVE_CONSUMES_EDGE",)
    return ActionValue.model_validate(
        {
            **value.model_dump(),
            "reason_codes": reasons,
            "conditional_gross_return_bps": remaining_gross,
            "conditional_net_return_bps": conditional,
            "expected_net_edge_bps": expected,
            "expected_net_pnl_usd": notional * expected / 10_000,
            "conservative_net_edge_bps": conservative,
            "required_buffer_bps": max(0.0, expected - conservative),
            "realized_favorable_move_bps": move_bps,
            "price_move_haircut_bps": haircut,
        }
    )


def revalidate_for_dispatch(
    decision: EconomicDecision,
    *,
    model: EconomicModel | None,
    now: datetime,
    account_digest: str,
    features: Mapping[str, FeatureValue],
    quote_age_seconds: float,
    decision_latency_seconds: float,
    spread_bps: float,
    notional_usd: float,
    current_mid_price: float | None = None,
    owned_inventory_notional_usd: float = 0,
    maker_fee_bps: float | None = None,
    taker_fee_bps: float | None = None,
) -> EconomicDecision:
    """Final economic admission, not authorization for mandatory owned-position exits.

    Pass the original immutable feature values and a fresh quote's mid, spread,
    and age. The feature fingerprint must match. Measured latency cannot reset
    either the original feature age or prediction expiry. Favorable mid movement
    consumes the old forecast, with remaining bps rebased to the current mid;
    adverse movement cannot manufacture extra edge from stale features.

    The returned decision retains the original reference mid and prediction
    expiry. Repeated calls always start from the frozen fit, so elapsed latency,
    consumed movement, fees, and spread are not charged twice. The returned
    quote_age_seconds includes the aged original feature quote, not only the
    fresh dispatch quote's age.
    """
    decision, now = EconomicDecision.model_validate(decision), _aware(now)
    features = _features(features)
    current_mid = (
        _number(current_mid_price, "current_mid_price", positive=True)
        if current_mid_price is not None
        else None
    )
    notional = _number(notional_usd, "notional_usd", positive=True)
    reasons: list[str] = []
    if not decision.profitability_validated or decision.selected_action == "NO_TRADE":
        reasons.append("ORIGINAL_DECISION_NOT_EXECUTABLE")
    if model is None or decision.model_id != model.model_id:
        reasons.append("MODEL_CHANGED_OR_MISSING")
    if model is not None and decision.evaluated_at < model.calibrated_at:
        reasons.append("MODEL_NOT_AVAILABLE_AT_ORIGINAL_DECISION")
    if account_digest != decision.account_digest:
        reasons.append("ACCOUNT_CHANGED")
    if now < decision.evaluated_at:
        reasons.append("FUTURE_DECISION")
    if decision.valid_until is None or now >= decision.valid_until:
        reasons.append("DECISION_EXPIRED")
    if decision.prediction_expires_at is None or now >= decision.prediction_expires_at:
        reasons.append("PREDICTION_EXPIRED")
    if _digest(dict(features)) != decision.features_sha256:
        reasons.append("DECISION_FEATURES_CHANGED")
    if decision.reference_mid_price is None:
        reasons.append("MISSING_DECISION_PRICE_CONTEXT")
    if current_mid is None:
        reasons.append("MISSING_DISPATCH_PRICE_CONTEXT")
    if notional != decision.notional_usd:
        reasons.append("NOTIONAL_CHANGED")
    elapsed = max(0.0, (now - decision.evaluated_at).total_seconds())
    minimum_latency = decision.decision_latency_seconds + elapsed
    latency = max(_number(decision_latency_seconds, "decision_latency_seconds"), minimum_latency)
    fresh_quote_age = _number(quote_age_seconds, "quote_age_seconds")
    if now - timedelta(seconds=fresh_quote_age) < (
        decision.evaluated_at - timedelta(seconds=decision.quote_age_seconds)
    ):
        reasons.append("DISPATCH_QUOTE_PREDATES_REFERENCE")
    quote_age = max(
        fresh_quote_age,
        decision.quote_age_seconds + elapsed,
    )
    move_bps = 0.0
    if current_mid is not None and decision.reference_mid_price is not None:
        if decision.selected_action.endswith("BUY"):
            if current_mid < max(
                decision.reference_mid_price,
                decision.current_mid_price or decision.reference_mid_price,
            ):
                reasons.append("ADVERSE_PRICE_CONTEXT_DRIFT")
            else:
                move_bps = _number(
                    (current_mid / decision.reference_mid_price - 1) * 10_000,
                    "realized_favorable_move_bps",
                )
        elif current_mid != decision.reference_mid_price:
            reasons.append("PRICE_CONTEXT_CHANGED")
    current = evaluate_actions(
        model,
        symbol=decision.symbol,
        family=decision.family,
        features=features,
        now=now,
        quote_age_seconds=quote_age,
        decision_latency_seconds=latency,
        spread_bps=spread_bps,
        notional_usd=notional,
        reference_mid_price=decision.reference_mid_price,
        account_digest=account_digest,
        owned_inventory_notional_usd=owned_inventory_notional_usd,
        maker_fee_bps=maker_fee_bps,
        taker_fee_bps=taker_fee_bps,
    )
    values = list(current.action_values)
    if not reasons and model is not None and move_bps > 0:
        for index, value in enumerate(values):
            if not value.available or not value.action.endswith("BUY"):
                continue
            cell = next(
                (
                    cell
                    for cell in model.cells
                    if (cell.symbol, cell.family, cell.action)
                    == (decision.symbol, decision.family, value.action)
                ),
                None,
            )
            if cell is None or cell.fit is None:
                raise ValueError("available action is missing its fitted execution evidence")
            values[index] = _consume_realized_move(value, cell.fit, move_bps, notional)
    selected = max(
        (value for value in values if value.available),
        key=lambda value: (
            value.conservative_net_edge_bps
            if value.conservative_net_edge_bps is not None
            else float("-inf")
        ),
    )
    hard_rejection = bool(reasons)
    if selected.action != "NO_TRADE" and selected.action != decision.selected_action:
        reasons.append("DISPATCH_ACTION_CHANGED")
        hard_rejection = True
    if selected.action == "NO_TRADE" or selected.action != decision.selected_action:
        reasons.extend(
            (
                "FINAL_ACTION_NOT_VALIDATED",
                *(reason for value in values[1:] for reason in value.reason_codes),
            )
        )
    if hard_rejection:
        values = [
            values[0],
            *(_unavailable(action, *reasons) for action in ACTIONS[1:]),
        ]
        selected = values[0]
    expires = (
        min(decision.valid_until, current.valid_until)
        if decision.valid_until is not None and current.valid_until is not None
        else None
    )
    payload = current.model_dump()
    payload.pop("decision_id")
    payload.update(
        {
            "selected_action": selected.action,
            "profitability_validated": selected.action != "NO_TRADE",
            "reason_codes": tuple(sorted(set(reasons)))
            or ("DISPATCH_VALIDATED_ORIGINAL_PREDICTION",),
            "action_values": tuple(values),
            "valid_until": expires,
            "prediction_expires_at": decision.prediction_expires_at,
            "reference_mid_price": decision.reference_mid_price,
            "current_mid_price": current_mid,
            "features_sha256": decision.features_sha256,
        }
    )
    for name in (
        "expected_net_edge_bps",
        "expected_net_pnl_usd",
        "conservative_net_edge_bps",
        "uncertainty",
        "required_buffer_bps",
        "fill_probability",
        "fill_probability_horizon",
        "fill_probability_ttl",
        "viable_latency_seconds",
    ):
        payload[name] = getattr(selected, name)
    return EconomicDecision.model_validate(payload)


def execution_rejection_reasons(
    decision: EconomicDecision,
    *,
    model: EconomicModel | None,
    now: datetime,
    account_digest: str,
    features: Mapping[str, FeatureValue],
    quote_age_seconds: float,
    decision_latency_seconds: float,
    spread_bps: float,
    notional_usd: float,
    current_mid_price: float | None = None,
    owned_inventory_notional_usd: float = 0,
    maker_fee_bps: float | None = None,
    taker_fee_bps: float | None = None,
) -> tuple[str, ...]:
    """Return final-dispatch rejection reasons without discarding the original prediction."""
    result = revalidate_for_dispatch(
        decision,
        model=model,
        now=now,
        account_digest=account_digest,
        features=features,
        quote_age_seconds=quote_age_seconds,
        decision_latency_seconds=decision_latency_seconds,
        spread_bps=spread_bps,
        notional_usd=notional_usd,
        current_mid_price=current_mid_price,
        owned_inventory_notional_usd=owned_inventory_notional_usd,
        maker_fee_bps=maker_fee_bps,
        taker_fee_bps=taker_fee_bps,
    )
    return () if result.profitability_validated else result.reason_codes


def decision_is_executable(
    decision: EconomicDecision,
    *,
    model: EconomicModel | None,
    now: datetime,
    account_digest: str,
    features: Mapping[str, FeatureValue],
    quote_age_seconds: float,
    decision_latency_seconds: float,
    spread_bps: float,
    notional_usd: float,
    current_mid_price: float | None = None,
    owned_inventory_notional_usd: float = 0,
    maker_fee_bps: float | None = None,
    taker_fee_bps: float | None = None,
) -> bool:
    """Boolean convenience; use execution_rejection_reasons for durable rejection telemetry."""
    return not execution_rejection_reasons(
        decision,
        model=model,
        now=now,
        account_digest=account_digest,
        features=features,
        quote_age_seconds=quote_age_seconds,
        decision_latency_seconds=decision_latency_seconds,
        spread_bps=spread_bps,
        notional_usd=notional_usd,
        current_mid_price=current_mid_price,
        owned_inventory_notional_usd=owned_inventory_notional_usd,
        maker_fee_bps=maker_fee_bps,
        taker_fee_bps=taker_fee_bps,
    )
