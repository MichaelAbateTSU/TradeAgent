from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta, timezone
from itertools import pairwise
from math import isfinite

import pytest
from pydantic import ValidationError

from tradeagent.scalping_economics import (
    ACTIONS,
    ECONOMIC_FEATURE_NAMES,
    ActionValue,
    CostEstimate,
    EconomicCosts,
    EconomicDecision,
    EconomicModel,
    EconomicSample,
    FeatureValue,
    _student_critical,
    _wilson,
    calibrate_model,
    decision_is_executable,
    evaluate_actions,
    execution_rejection_reasons,
    project_economic_features,
    revalidate_for_dispatch,
)

START = datetime(2026, 9, 11, 12, tzinfo=UTC)
CALIBRATED = START + timedelta(hours=1)
NOW = CALIBRATED + timedelta(seconds=1)
ACCOUNT = "a" * 64
SOURCE = "b" * 64
VALIDATION = "c" * 64
FEATURES: Mapping[str, FeatureValue] = {
    "ofi": 0.6,
    "regime": "continuation",
    "unobserved_aggressor": None,
}


def test_shared_feature_projection_excludes_identifiers_clocks_flags_and_outcome_labels() -> None:
    snapshot: dict[str, object] = {
        **dict.fromkeys(ECONOMIC_FEATURE_NAMES, 0.5),
        "as_of_ns": 1_700_000_000_000_000_000,
        "source_event_id": "event-1",
        "book_exchange_at_ns": 1_700_000_000_000_000_000,
        "book_received_at_ns": 1_700_000_000_000_000_010,
        "ready": True,
        "aggregate_change_proxies": True,
        "exact_queue_position_available": False,
        "session_trade_count": 1000,
        "session_observed_volume": 5000,
        "momentum_score": 0.9,
        "reversion_score": 0.8,
        "financial_qualification_gate": "disabled",
        "label_return_5s_bps": 100.0,
        "gross_return_bps": 80.0,
        "fill_delay_seconds": 0.25,
    }
    projected = project_economic_features(snapshot)
    assert set(projected) == set(ECONOMIC_FEATURE_NAMES)
    assert projected["return_5s_bps"] == 0.5
    snapshot["normalized_ofi_5s"] = 99.0
    assert projected["normalized_ofi_5s"] == 0.5
    with pytest.raises(TypeError):
        projected["normalized_ofi_5s"] = 99.0


def test_feature_projection_preserves_missing_and_unobserved_inputs_without_imputation() -> None:
    projected = project_economic_features(
        {
            "normalized_ofi_5s": -0.5,
            "taker_delta_5s": None,
            "trade_count_5s": 0,
        }
    )
    assert dict(projected) == {
        "normalized_ofi_5s": -0.5,
        "taker_delta_5s": None,
        "trade_count_5s": 0,
    }
    assert "return_1s_bps" not in projected
    assert not project_economic_features({"ready": True, "source_event_id": "only-metadata"})


@pytest.mark.parametrize("value", [True, "0.5", float("nan"), float("inf"), {"value": 0.5}])
def test_feature_projection_does_not_coerce_invalid_predictors(value: object) -> None:
    with pytest.raises(ValueError):
        project_economic_features({"normalized_ofi_5s": value})


def estimate(bps: float, *, included: bool = False) -> CostEstimate:
    return CostEstimate(
        bps=bps,
        included_in_return=included,
        kind="observed",
        provenance="frozen test fixture; not actual trading evidence",
    )


def costs(**overrides: object) -> EconomicCosts:
    return EconomicCosts.model_validate(
        {
            "fees": estimate(50.0),
            "spread": estimate(2.0),
            "slippage": estimate(1.0),
            "impact": estimate(1.0),
            "adverse_selection": CostEstimate(
                bps=9.0,
                included_in_return=True,
                kind="embedded",
                provenance="measured fill-conditioned return already contains selection",
            ),
            **overrides,
        }
    )


def per_leg_costs(
    entry_fee: CostEstimate,
    exit_fee: CostEstimate,
    *,
    full_execution_costs_embedded: bool = True,
) -> EconomicCosts:
    values: dict[str, object] = {
        "fees": CostEstimate(
            kind="unknown", provenance="aggregate unused; both fee legs disclosed"
        ),
        "entry_fee": entry_fee,
        "exit_fee": exit_fee,
    }
    if full_execution_costs_embedded:
        embedded = CostEstimate(
            kind="embedded",
            included_in_return=True,
            provenance="both executed prices already reflect this execution cost",
        )
        values.update(spread=embedded, slippage=embedded, impact=embedded)
    return costs(**values)


def sample(index: int, **overrides: object) -> EconomicSample:
    at = START + timedelta(seconds=index * 10)
    return EconomicSample.model_validate(
        {
            "sample_id": f"fixture-{index}",
            "account_digest": ACCOUNT,
            "source_sha256": SOURCE,
            "symbol": "BTC/USD",
            "family": "momentum",
            "action": "PASSIVE_BUY",
            "decision_at": at,
            "feature_observed_at": at,
            "label_matured_at": at + timedelta(seconds=5.5),
            "return_end_at": at + timedelta(seconds=5),
            "horizon_seconds": 5.0,
            "cancellation_horizon_seconds": 3.0,
            "features": FEATURES,
            "quote_age_seconds": 0.0,
            "decision_latency_seconds": 0.0,
            "spread_bps": 2.0,
            "notional_usd": 100.0,
            "filled": True,
            "filled_fraction": 1.0,
            "fill_delay_seconds": 0.25,
            "gross_return_bps": 80.0,
            "directional_return_bps": 90.0,
            "return_basis": "mid_to_mid",
            "costs": costs(),
            "execution_evidence": "observed",
            **overrides,
        }
    )


def rows(count: int = 48, **overrides: object) -> tuple[EconomicSample, ...]:
    return tuple(sample(index, **overrides) for index in range(count))


def refit_sample(row: EconomicSample, **overrides: object) -> EconomicSample:
    return EconomicSample.model_validate({**row.model_dump(), **overrides})


def fit(observations: Sequence[EconomicSample] | None = None, **overrides: object) -> EconomicModel:
    parameters = {
        "account_digest": ACCOUNT,
        "source_sha256": SOURCE,
        "maker_fee_bps": 15.0,
        "taker_fee_bps": 25.0,
        "horizon_seconds": 5.0,
        "cancellation_horizon_seconds": 3.0,
        "calibrated_at": CALIBRATED,
        "valid_until": CALIBRATED + timedelta(days=1),
        **overrides,
    }
    return calibrate_model(rows() if observations is None else observations, **parameters)


def decide(model: EconomicModel | None, **overrides: object) -> EconomicDecision:
    parameters = {
        "symbol": "BTC/USD",
        "family": "momentum",
        "features": FEATURES,
        "now": NOW,
        "quote_age_seconds": 0.0,
        "decision_latency_seconds": 0.0,
        "spread_bps": 2.0,
        "notional_usd": 100.0,
        "account_digest": ACCOUNT,
        "reference_mid_price": 100.0,
        **overrides,
    }
    return evaluate_actions(model, **parameters)


def action(decision: EconomicDecision, name: str) -> ActionValue:
    return next(value for value in decision.action_values if value.action == name)


def fence_arguments(model: EconomicModel, /, **overrides: object) -> dict[str, object]:
    return {
        "model": model,
        "now": NOW,
        "account_digest": ACCOUNT,
        "features": FEATURES,
        "quote_age_seconds": 0.0,
        "decision_latency_seconds": 0.0,
        "spread_bps": 2.0,
        "notional_usd": 100.0,
        "current_mid_price": 100.0,
        **overrides,
    }


def test_frozen_positive_fixture_approves_only_after_actual_chronological_validation() -> None:
    model = fit()
    result = decide(model)
    assert model.status == "validated"
    assert model.evaluated_trades == 15
    assert model.fit_count == 32 and model.validation_count == 15 and model.purged_count == 1
    assert model.validation_net_lower_bound_bps is not None
    assert model.validation_net_lower_bound_bps > 0
    assert result.selected_action == "PASSIVE_BUY"
    assert result.profitability_validated
    assert result.expected_net_edge_bps == pytest.approx(26)
    assert result.expected_net_pnl_usd == pytest.approx(0.26)
    assert 0 < result.conservative_net_edge_bps < result.expected_net_edge_bps
    assert result.required_buffer_bps == pytest.approx(
        result.expected_net_edge_bps - result.conservative_net_edge_bps
    )
    assert result.fill_probability == result.fill_probability_horizon == result.fill_probability_ttl
    assert result.fill_probability == 1
    assert result.confidence is None
    assert result.uncertainty == 0
    assert result.uncertainty_unit == "standard_error_bps"
    assert result.prediction_horizon_seconds == 5
    assert result.cancellation_horizon_seconds == 3
    assert tuple(value.action for value in result.action_values) == ACTIONS
    assert result.valid_until == NOW + timedelta(seconds=2)
    assert result.prediction_expires_at == NOW + timedelta(seconds=5)
    assert result.reference_mid_price == result.current_mid_price == 100


def test_one_second_horizon_is_preserved_without_a_minimum_holding_period() -> None:
    observations = tuple(
        refit_sample(
            row,
            horizon_seconds=1.0,
            cancellation_horizon_seconds=0.3,
            fill_delay_seconds=0.1,
            return_end_at=row.decision_at + timedelta(seconds=1),
            label_matured_at=row.decision_at + timedelta(seconds=1.1),
        )
        for row in rows()
    )
    model = fit(observations, horizon_seconds=1.0, cancellation_horizon_seconds=0.3)
    decision = decide(model)
    assert decision.profitability_validated
    assert decision.prediction_horizon_seconds == 1
    assert decision.cancellation_horizon_seconds == 0.3
    assert decision.valid_until == NOW + timedelta(seconds=0.7)


def test_supported_fill_probability_is_observed_attempt_frequency_not_a_bullish_score() -> None:
    observations = tuple(
        sample(index)
        if index % 3 != 0
        else sample(
            index,
            filled=False,
            filled_fraction=0.0,
            fill_delay_seconds=None,
            gross_return_bps=None,
            return_end_at=None,
            directional_return_bps=500.0,
        )
        for index in range(180)
    )
    model = fit(observations)
    cell = model.cells[0]
    assert cell.fit is not None
    actual = {row.sample_id: row for row in observations if row.sample_id in cell.fit_sample_ids}
    probability = sum(row.filled for row in actual.values()) / len(actual)
    result = decide(model, features={**FEATURES, "bullish_score": 1.0})
    assert result.profitability_validated
    assert 0 < probability < 1
    assert result.fill_probability == probability
    assert result.fill_probability_horizon == result.fill_probability_ttl == probability
    assert result.expected_net_edge_bps == pytest.approx(probability * 26)
    assert result.confidence is None


def test_model_identity_and_serialization_are_deterministic_and_include_provenance() -> None:
    observations = rows()
    first = fit(observations)
    reordered = fit(tuple(reversed(observations)))
    assert first == reordered
    assert first == fit(observations, maker_fee_bps=15, taker_fee_bps=25)
    assert first.canonical_json() == reordered.canonical_json()
    assert len(first.model_id) == 64
    restored = EconomicModel.model_validate_json(first.canonical_json())
    assert restored == first
    assert EconomicModel.model_validate_json(first.model_dump_json()) == first
    assert first.provenance.account_digest == ACCOUNT
    assert first.provenance.source_sha256 == SOURCE
    assert first.provenance.universe == ("BTC/USD",)
    assert len(first.provenance.samples_sha256) == len(first.provenance.costs_sha256) == 64
    assert first.provenance.evidence_scope == "observed_policy_support_only"
    changed = fit([refit_sample(row, gross_return_bps=81.0) for row in observations])
    assert first.model_id != changed.model_id
    assert first.provenance.samples_sha256 != changed.provenance.samples_sha256
    assert first.provenance.costs_sha256 == changed.provenance.costs_sha256
    assert decide(first) == decide(restored)
    decision = decide(first)
    assert EconomicDecision.model_validate_json(decision.model_dump_json()) == decision
    assert decide(first, features=dict(reversed(tuple(FEATURES.items())))) == decision


def test_model_and_decision_cannot_silently_accept_tampered_identities() -> None:
    model = fit()
    payload = json.loads(model.canonical_json())
    payload["provenance"]["taker_fee_bps"] = 0.0
    with pytest.raises(ValidationError, match="identity"):
        EconomicModel.model_validate_json(json.dumps(payload))
    with pytest.raises(ValidationError, match="identity"):
        decide(model.model_copy(update={"valid_until": CALIBRATED + timedelta(days=2)}))
    decision = decide(model)
    payload = json.loads(decision.model_dump_json())
    payload["spread_bps"] = 4.0
    with pytest.raises(ValidationError, match="identity"):
        EconomicDecision.model_validate_json(json.dumps(payload))


def test_features_and_nested_models_really_are_immutable() -> None:
    mutable: dict[str, FeatureValue] = {"ofi": 0.6}
    row = sample(0, features=mutable)
    mutable["ofi"] = 99.0
    assert row.features["ofi"] == 0.6
    with pytest.raises(TypeError):
        row.features["ofi"] = 1.0
    with pytest.raises(ValidationError, match="frozen"):
        row.gross_return_bps = 999.0
    with pytest.raises(ValidationError, match="frozen"):
        row.costs.fees.bps = 0.0
    with pytest.raises(ValidationError, match="frozen"):
        fit().status = "validated"


def test_fees_spread_slippage_and_impact_overpower_directional_alpha() -> None:
    model = fit(rows(gross_return_bps=20.0, directional_return_bps=100.0))
    assert model.status == "failed_validation"
    cell = model.cells[0]
    assert cell.fit is not None and cell.validation is not None
    assert cell.fit.expected_net_edge_bps == -34
    assert cell.validation.net_lower_bound_bps < 0
    result = decide(model, features={**FEATURES, "bullish_score": 1.0})
    assert result.selected_action == "NO_TRADE"
    assert result.expected_net_edge_bps == result.expected_net_pnl_usd == 0
    assert not result.profitability_validated
    unavailable = action(result, "PASSIVE_BUY")
    assert not unavailable.available and unavailable.expected_net_edge_bps is None
    assert "NONPOSITIVE_HELDOUT_NET_LOWER_BOUND" in unavailable.reason_codes


def test_unfilled_bullish_returns_never_rescue_adversely_selected_passive_fills() -> None:
    observations = tuple(
        sample(index, gross_return_bps=-3.0, directional_return_bps=-2.0)
        if index % 3 == 0
        else sample(
            index,
            filled=False,
            filled_fraction=0.0,
            fill_delay_seconds=None,
            gross_return_bps=None,
            return_end_at=None,
            directional_return_bps=250.0,
        )
        for index in range(90)
    )
    model = fit(observations)
    result = decide(model, features={**FEATURES, "directional_probability": 0.999})
    cell = model.cells[0]
    assert cell.fit is not None
    assert cell.filled_directional_return_bps == -2
    assert cell.unfilled_directional_return_bps == 250
    assert cell.adverse_selection_gap_bps == 252
    assert cell.fit.conditional_net_return_bps == -57
    assert cell.fit.expected_net_edge_bps == pytest.approx(
        cell.fit.fill_probability * cell.fit.conditional_net_return_bps
    )
    assert cell.fit.expected_net_edge_bps < 0
    assert result.selected_action == "NO_TRADE"
    assert action(result, "AGGRESSIVE_BUY").expected_net_edge_bps is None


@pytest.mark.parametrize(
    ("passive_gross", "aggressive_gross", "selected"),
    [(60.0, 110.0, "AGGRESSIVE_BUY"), (95.0, 90.0, "PASSIVE_BUY")],
)
def test_action_optimization_uses_separately_supported_execution(
    passive_gross: float, aggressive_gross: float, selected: str
) -> None:
    observations = (
        *rows(gross_return_bps=passive_gross),
        *(
            sample(
                index,
                sample_id=f"aggressive-{index}",
                action="AGGRESSIVE_BUY",
                gross_return_bps=aggressive_gross,
                costs=costs(fees=estimate(50.0)),
            )
            for index in range(48)
        ),
    )
    model = fit(observations)
    decision = decide(model)
    assert model.status == "validated"
    assert decision.selected_action == selected
    for name in ("PASSIVE_BUY", "AGGRESSIVE_BUY"):
        value = action(decision, name)
        assert value.available and value.conservative_net_edge_bps is not None
        assert value.conservative_net_edge_bps > 0
    assert model.provenance.simultaneous_comparisons == 24


def test_passive_only_history_never_validates_aggressive_counterfactual_fills() -> None:
    observed = rows()
    hypotheticals = tuple(
        sample(
            index,
            sample_id=f"hypothetical-{index}",
            action="AGGRESSIVE_BUY",
            gross_return_bps=1000.0,
            costs=costs(fees=estimate(50.0)),
            execution_evidence="unsupported_counterfactual",
        )
        for index in range(48)
    )
    decision = decide(fit((*observed, *hypotheticals)))
    assert decision.selected_action == "PASSIVE_BUY"
    aggressive = action(decision, "AGGRESSIVE_BUY")
    assert not aggressive.available
    assert "UNSUPPORTED_EXECUTION_EVIDENCE" in aggressive.reason_codes
    assert aggressive.fill_probability is None


def test_simulated_execution_requires_independent_evidence_and_preserves_its_hash() -> None:
    with pytest.raises(ValidationError, match="independent execution"):
        sample(0, execution_evidence="validated_simulation")
    model = fit(
        rows(
            execution_evidence="validated_simulation",
            execution_validation_sha256=VALIDATION,
        )
    )
    assert model.status == "validated"
    assert model.cells[0].execution_validation_hashes == (VALIDATION,)
    assert model.cells[0].execution_evidence == ("validated_simulation",)


def test_no_short_sell_entry_and_only_sufficient_owned_exit_is_available() -> None:
    with pytest.raises(ValidationError, match="owned long exits"):
        sample(0, action="PASSIVE_SELL")
    model = fit(
        rows(
            action="PASSIVE_SELL",
            position_effect="close_long",
            costs=costs(fees=estimate(25.0)),
        )
    )
    for owned in (0.0, 50.0):
        decision = decide(model, owned_inventory_notional_usd=owned)
        assert decision.selected_action == "NO_TRADE"
        assert not action(decision, "PASSIVE_SELL").available
        assert "SELL_REQUIRES_SUFFICIENT_OWNED_INVENTORY" in (
            action(decision, "PASSIVE_SELL").reason_codes
        )
    owned_exit = decide(model, owned_inventory_notional_usd=100.0)
    assert owned_exit.selected_action == "PASSIVE_SELL"
    assert owned_exit.expected_net_edge_bps == 51
    assert not action(owned_exit, "PASSIVE_BUY").available
    assert not decision_is_executable(
        owned_exit, **fence_arguments(model, owned_inventory_notional_usd=50.0)
    )


def test_momentum_reversion_and_symbol_estimates_are_never_pooled() -> None:
    observations = (
        *rows(),
        *(
            sample(
                index,
                sample_id=f"reversion-{index}",
                family="reversion",
                gross_return_bps=-40.0,
            )
            for index in range(48)
        ),
        *(
            sample(
                index,
                sample_id=f"ether-{index}",
                symbol="ETH/USD",
                gross_return_bps=-90.0,
            )
            for index in range(48)
        ),
    )
    model = fit(observations)
    assert decide(model).selected_action == "PASSIVE_BUY"
    assert decide(model, family="reversion").selected_action == "NO_TRADE"
    assert decide(model, symbol="ETH/USD").selected_action == "NO_TRADE"
    assert model.provenance.universe == ("BTC/USD", "ETH/USD")


def test_unknown_model_or_fill_conditional_uncertainty_never_invents_confidence() -> None:
    no_fills = fit(
        rows(
            filled=False,
            filled_fraction=0.0,
            fill_delay_seconds=None,
            return_end_at=None,
            gross_return_bps=None,
            directional_return_bps=900.0,
        )
    )
    empty = fit(())
    for model in (None, no_fills, empty, fit(rows(count=3))):
        result = decide(model)
        assert result.selected_action == "NO_TRADE"
        assert result.confidence is None
        assert result.fill_probability is None
        for value in result.action_values[1:]:
            assert value.expected_net_edge_bps is None
            assert value.fill_probability is None
            assert value.confidence is None
    assert no_fills.status == empty.status == "no_support"
    assert "UNKNOWN_FILL_CONDITIONAL_UNCERTAINTY" in no_fills.reason_codes
    assert empty.reason_codes == ("NO_SAMPLES",)


def test_chronological_split_purges_labels_and_never_refits_from_heldout_returns() -> None:
    good = fit()
    split = good.validation_start_at
    assert split is not None
    changed = fit(
        tuple(
            refit_sample(row, gross_return_bps=-800.0) if row.decision_at >= split else row
            for row in rows()
        )
    )
    assert changed.cells[0].fit == good.cells[0].fit
    assert changed.cells[0].feature_support == good.cells[0].feature_support
    assert changed.status == "failed_validation"
    assert changed.cells[0].validation is not None
    assert changed.cells[0].validation.expected_net_edge_bps == -854
    assert set(good.cells[0].fit_sample_ids).isdisjoint(good.cells[0].validation_sample_ids)
    assert good.cells[0].fit_window is not None
    assert good.training_label_cutoff_at is not None
    assert good.cells[0].fit_window.last_label_matured_at < good.training_label_cutoff_at
    assert good.training_label_cutoff_at == split - timedelta(seconds=5)
    assert decide(changed).selected_action == "NO_TRADE"


@pytest.mark.parametrize(
    ("count", "fraction", "validation_count"),
    [(180, 0.3, 54), (100, 0.07, 7), (100, 0.29, 29), (48, 0.3, 15)],
)
def test_split_does_not_round_binary_float_noise_into_an_extra_observation(
    count: int,
    fraction: float,
    validation_count: int,
) -> None:
    model = fit(rows(count=count), validation_fraction=fraction)
    assert model.validation_count == validation_count
    assert model.validation_start_at == START + timedelta(seconds=(count - validation_count) * 10)


def test_positive_raw_holdout_does_not_hide_a_losing_executable_policy_subset() -> None:
    initial = tuple(
        sample(
            index,
            gross_return_bps=55.0,
            quote_age_seconds=float(index % 2),
            decision_latency_seconds=float(index % 2),
        )
        for index in range(180)
    )
    split = fit(initial).validation_start_at
    assert split is not None
    observations = tuple(
        refit_sample(row, gross_return_bps=53.0 if index % 2 == 0 else 64.0)
        if row.decision_at >= split
        else row
        for index, row in enumerate(initial)
    )
    model = fit(observations)
    cell = model.cells[0]
    assert cell.validation is not None
    assert cell.validation.net_lower_bound_bps > 0
    assert cell.policy_validation is not None
    assert cell.policy_validation.net_lower_bound_bps < 0
    assert cell.policy_validation.expected_net_edge_bps == -0.5
    assert cell.policy_validation.selected_attempts == 27
    assert model.evaluated_trades == 27 and model.observed_validation_fills == 54
    assert model.status == "failed_validation"
    assert "NONPOSITIVE_HELDOUT_POLICY_NET_LOWER_BOUND" in cell.reason_codes
    assert decide(model).selected_action == "NO_TRADE"


def test_holdout_outcomes_can_veto_but_never_retune_the_frozen_live_buffer() -> None:
    original = fit()
    split = original.validation_start_at
    assert split is not None
    improved = fit(
        tuple(
            refit_sample(row, gross_return_bps=800.0) if row.decision_at >= split else row
            for row in rows()
        )
    )
    assert original.status == improved.status == "validated"
    first, second = decide(original), decide(improved)
    assert first.expected_net_edge_bps == second.expected_net_edge_bps
    assert first.conservative_net_edge_bps == second.conservative_net_edge_bps
    assert first.required_buffer_bps == second.required_buffer_bps
    assert first.uncertainty == second.uncertainty


def test_overlapping_prediction_windows_are_not_independent_samples() -> None:
    observations = tuple(
        sample(
            index,
            decision_at=START + timedelta(seconds=index),
            feature_observed_at=START + timedelta(seconds=index),
            label_matured_at=START + timedelta(seconds=index + 5.5),
            return_end_at=START + timedelta(seconds=index + 5),
        )
        for index in range(80)
    )
    model = fit(observations)
    assert model.purged_count > model.fit_count + model.validation_count
    indexed = {row.sample_id: row for row in observations}
    for identities in (model.cells[0].fit_sample_ids, model.cells[0].validation_sample_ids):
        retained = [indexed[identity] for identity in identities]
        for earlier, later in pairwise(retained):
            assert earlier.decision_at + timedelta(seconds=5) <= later.decision_at
    assert model.input_count == model.fit_count + model.validation_count + model.purged_count


def test_validation_ood_is_reported_not_learned_or_silently_dropped() -> None:
    original = fit()
    split = original.validation_start_at
    assert split is not None
    model = fit(
        tuple(
            refit_sample(row, features={**FEATURES, "ofi": 1000.0})
            if row.decision_at >= split
            else row
            for row in rows()
        )
    )
    assert model.status == "failed_validation"
    assert model.cells[0].feature_support == original.cells[0].feature_support
    assert model.cells[0].validation_count == original.cells[0].validation_count
    assert "VALIDATION_OOD" in model.cells[0].reason_codes


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"symbol": "DOGE/USD"}, "UNSUPPORTED_SYMBOL"),
        ({"family": "bullish"}, "UNSUPPORTED_FAMILY"),
        ({"features": {**FEATURES, "ofi": 100.0}}, "OOD_FEATURE:ofi"),
        ({"features": {**FEATURES, "regime": "unknown"}}, "OOD_FEATURE:regime"),
        ({"features": {**FEATURES, "ofi": None}}, "OOD_FEATURE:ofi"),
        ({"features": {"ofi": 0.6}}, "MISSING_FEATURE:regime"),
        ({"quote_age_seconds": 0.01}, "OOD_QUOTE_AGE"),
        ({"decision_latency_seconds": 0.01}, "OOD_DECISION_LATENCY"),
        ({"spread_bps": 3.0}, "OOD_SPREAD"),
        ({"notional_usd": 200.0}, "OOD_NOTIONAL"),
        ({"account_digest": "d" * 64}, "ACCOUNT_MISMATCH"),
        ({"account_digest": None}, "ACCOUNT_NOT_PROVIDED"),
        ({"now": CALIBRATED - timedelta(microseconds=1)}, "FUTURE_MODEL"),
        ({"now": CALIBRATED + timedelta(days=1)}, "STALE_MODEL"),
        ({"maker_fee_bps": 16.0}, "FEE_SCHEDULE_MISMATCH"),
        ({"taker_fee_bps": 26.0}, "FEE_SCHEDULE_MISMATCH"),
    ],
)
def test_unsupported_or_incompatible_current_state_fails_closed(
    overrides: dict[str, object], reason: str
) -> None:
    result = decide(fit(), **overrides)
    assert result.selected_action == "NO_TRADE"
    assert reason in result.reason_codes
    assert action(result, "PASSIVE_BUY").expected_net_edge_bps is None


def test_known_numeric_and_categorical_support_does_not_require_unsupported_optional_data() -> None:
    model = fit()
    assert decide(
        model, features={**FEATURES, "extraneous_bullish_score": 1.0}
    ).selected_action == ("PASSIVE_BUY")
    unknown = fit(rows(features={"nothing_observed": None}))
    assert unknown.status == "no_support"
    assert "NO_OBSERVED_STATE_FEATURES" in unknown.reason_codes


@pytest.mark.parametrize(
    "overrides",
    [
        {"account_digest": "d" * 64},
        {"source_sha256": "d" * 64},
        {"horizon_seconds": 5000.0},
        {"cancellation_horizon_seconds": 3000.0},
        {"calibrated_at": START + timedelta(seconds=100)},
    ],
)
def test_wrong_account_source_horizon_and_immature_training_are_invalid(
    overrides: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        fit(**overrides)


def test_duplicate_ids_and_duplicate_decisions_cannot_inflate_calibration() -> None:
    observations = rows()
    with pytest.raises(ValueError, match="duplicate sample IDs"):
        fit((*observations, observations[0]))
    with pytest.raises(ValueError, match="duplicate decision/action"):
        fit((*observations, refit_sample(observations[0], sample_id="renamed-duplicate")))


def test_embargo_can_be_explicit_but_cannot_admit_an_unmatured_training_label() -> None:
    model = fit(embargo_seconds=0.0)
    assert model.training_label_cutoff_at == model.validation_start_at
    assert model.cells[0].fit_window is not None
    assert model.training_label_cutoff_at is not None
    assert model.cells[0].fit_window.last_label_matured_at < model.training_label_cutoff_at
    larger = fit(embargo_seconds=40.0)
    assert larger.fit_count < model.fit_count
    assert larger.validation_count == model.validation_count


def test_latency_haircut_erases_positive_point_edge_without_discounting_costs() -> None:
    observations = tuple(
        sample(
            index,
            gross_return_bps=55.0,
            quote_age_seconds=float(index % 2),
            decision_latency_seconds=float(index % 2),
        )
        for index in range(48)
    )
    model = fit(observations)
    assert model.status == "validated"
    assert decide(model).selected_action == "PASSIVE_BUY"
    result = decide(model, quote_age_seconds=0.5, decision_latency_seconds=0.5)
    value = action(result, "PASSIVE_BUY")
    assert value.available
    assert value.latency_haircut_bps == 5.5
    assert value.expected_net_edge_bps == -4.5
    assert value.costs is not None and value.costs.additional_bps() == 54
    assert result.selected_action == "NO_TRADE"
    assert "UNCERTAINTY_OR_LATENCY_ERASES_NET_EDGE" in result.reason_codes
    assert decide(model, decision_latency_seconds=5.0).selected_action == "NO_TRADE"
    assert (
        "LATENCY_EXCEEDS_PREDICTION_HORIZON"
        in decide(model, decision_latency_seconds=5.0).reason_codes
    )
    assert (
        "INSUFFICIENT_HORIZON_FOR_CALIBRATED_TTL"
        in decide(model, decision_latency_seconds=2.0).reason_codes
    )


def test_current_wider_spread_is_repriced_even_inside_empirical_support() -> None:
    observations = tuple(
        sample(
            index,
            spread_bps=2.0 if index % 2 == 0 else 10.0,
            gross_return_bps=56.0 if index % 2 == 0 else 64.0,
            costs=costs(spread=estimate(2.0 if index % 2 == 0 else 10.0)),
        )
        for index in range(48)
    )
    model = fit(observations)
    assert model.status == "validated"
    assert decide(model).selected_action == "PASSIVE_BUY"
    wide = decide(model, spread_bps=10.0)
    value = action(wide, "PASSIVE_BUY")
    assert value.available
    assert value.spread_repricing_bps == 4
    assert value.expected_net_edge_bps == -2
    assert wide.selected_action == "NO_TRADE"


def test_current_larger_size_reprices_impact_without_extrapolating_notional_support() -> None:
    observations = tuple(
        sample(
            index,
            notional_usd=100.0 if index % 2 == 0 else 200.0,
            gross_return_bps=59.0 if index % 2 == 0 else 63.0,
            costs=costs(impact=estimate(4.0 if index % 2 == 0 else 8.0)),
        )
        for index in range(48)
    )
    model = fit(observations)
    assert model.status == "validated"
    assert decide(model).selected_action == "PASSIVE_BUY"
    larger = decide(model, notional_usd=200.0)
    value = action(larger, "PASSIVE_BUY")
    assert value.impact_repricing_bps == 2
    assert value.expected_net_edge_bps == 0
    assert larger.selected_action == "NO_TRADE"
    assert "OOD_NOTIONAL" in decide(model, notional_usd=201.0).reason_codes


def test_unknown_embedded_cost_is_not_zero_when_current_execution_conditions_change() -> None:
    embedded = CostEstimate(
        included_in_return=True,
        kind="embedded",
        provenance="executed price component unknown",
    )
    observations = tuple(
        sample(
            index,
            spread_bps=2.0 if index % 2 == 0 else 4.0,
            return_basis="fill_to_fill",
            costs=costs(spread=embedded, slippage=embedded, impact=embedded),
        )
        for index in range(48)
    )
    model = fit(observations)
    assert decide(model).selected_action == "PASSIVE_BUY"
    changed = decide(model, spread_bps=4.0)
    assert changed.selected_action == "NO_TRADE"
    assert "UNPRICED_SPREAD_CHANGE" in changed.reason_codes


def test_positive_mean_with_large_uncertainty_does_not_validate() -> None:
    model = fit(
        tuple(
            sample(index, gross_return_bps=55.0 + (20.0 if index % 2 == 0 else -20.0))
            for index in range(48)
        )
    )
    cell = model.cells[0]
    assert cell.fit is not None
    assert cell.fit.expected_net_edge_bps > 0
    assert cell.fit.standard_error_bps > 0
    assert cell.fit.net_lower_bound_bps < 0
    assert model.status == "failed_validation"
    assert decide(model).selected_action == "NO_TRADE"


def test_filled_fraction_scales_payoffs_and_costs_once_in_requested_notional_units() -> None:
    model = fit(rows(filled_fraction=0.25))
    result = decide(model)
    assert result.selected_action == "PASSIVE_BUY"
    assert result.expected_net_edge_bps == 6.5
    assert result.expected_net_pnl_usd == 0.065
    value = action(result, "PASSIVE_BUY")
    assert value.conditional_gross_return_bps == 20
    assert value.conditional_net_return_bps == 6.5
    assert value.costs is not None and value.costs.additional_bps() == 13.5


@pytest.mark.parametrize("basis", ["fill_to_fill", "net_fill_to_fill"])
def test_executed_prices_and_fill_conditioned_selection_are_not_double_charged(basis: str) -> None:
    embedded = CostEstimate(
        bps=None,
        included_in_return=True,
        kind="embedded",
        provenance="execution prices contain this cost; isolated actual component is unknown",
    )
    model = fit(
        rows(
            return_basis=basis,
            costs=costs(
                fees=estimate(50.0, included=basis == "net_fill_to_fill"),
                spread=embedded,
                slippage=embedded,
                impact=embedded,
            ),
        )
    )
    result = decide(model)
    assert result.expected_net_edge_bps == (80 if basis == "net_fill_to_fill" else 30)
    value = action(result, "PASSIVE_BUY")
    assert value.costs is not None
    assert value.costs.slippage.bps is None and value.costs.slippage.included_in_return
    assert value.costs.impact.bps is None and value.costs.impact.included_in_return
    assert value.costs.adverse_selection.bps == 9
    assert value.costs.adverse_selection.included_in_return


def test_unknown_nonembedded_costs_are_not_silently_zero() -> None:
    unknown = CostEstimate(kind="unknown", provenance="actual slippage is unobserved")
    model = fit(rows(costs=costs(slippage=unknown, impact=unknown)))
    result = decide(model)
    assert model.status == "no_support"
    assert result.selected_action == "NO_TRADE"
    assert "UNPRICED_SLIPPAGE" in result.reason_codes
    assert "UNPRICED_IMPACT" in result.reason_codes
    with pytest.raises(ValueError, match="unpriced"):
        costs(slippage=unknown).additional_bps()


def test_independently_validated_residual_covers_unknown_costs_exactly_once() -> None:
    unknown = CostEstimate(kind="unknown", provenance="isolated actual component unavailable")
    allowance = CostEstimate(
        bps=10.0,
        kind="conservative_allowance",
        provenance="independent execution residual study, frozen synthetic fixture",
        validation_sha256=VALIDATION,
    )
    value = costs(
        slippage=unknown,
        impact=unknown,
        residual=allowance,
        residual_covers=("slippage", "impact"),
    )
    assert value.additional_bps() == 62
    result = decide(fit(rows(costs=value)))
    assert result.expected_net_edge_bps == 18
    assert result.profitability_validated
    modeled = action(result, "PASSIVE_BUY").costs
    assert modeled is not None and modeled.slippage.bps is None
    assert modeled.residual is not None and modeled.residual.bps == 10
    assert modeled.residual.validation_sha256 is not None


def test_missing_fees_use_an_explicit_estimated_schedule_not_an_actual_zero() -> None:
    unknown = CostEstimate(kind="unknown", provenance="broker fee was not reported")
    result = decide(fit(rows(costs=costs(fees=unknown))))
    value = action(result, "PASSIVE_BUY")
    assert value.costs is not None
    assert value.costs.fees.bps == 50
    assert value.costs.fees.kind == "estimated"
    assert result.expected_net_edge_bps == 26
    underpriced = fit(rows(costs=costs(fees=estimate(0.0))))
    assert underpriced.status == "no_support"
    assert "FEES_BELOW_CONFIGURED_SCENARIO" in underpriced.reason_codes


@pytest.mark.parametrize("style", ["PASSIVE_BUY", "AGGRESSIVE_BUY"])
def test_native_limit_order_and_execution_proof_do_not_establish_discounted_maker_fees(
    style: str,
) -> None:
    claims: Mapping[str, FeatureValue] = {
        **FEATURES,
        "order_type": "limit",
        "at_bid": 1,
        "claimed_liquidity_role": "maker",
    }
    underpriced = CostEstimate(
        bps=40.0,
        kind="estimated",
        provenance="native limit order was placed at the bid",
        validation_sha256=VALIDATION,
    )
    model = fit(
        rows(
            action=style,
            features=claims,
            costs=costs(fees=underpriced),
            execution_evidence="validated_simulation",
            execution_validation_sha256=VALIDATION,
        )
    )
    assert model.status == "no_support"
    assert "FEES_BELOW_CONFIGURED_SCENARIO" in model.reason_codes
    assert decide(model, features=claims).selected_action == "NO_TRADE"


@pytest.mark.parametrize("style", ["PASSIVE_BUY", "AGGRESSIVE_BUY"])
@pytest.mark.parametrize(
    ("maker", "taker", "total"),
    [(15.0, 25.0, 50.0), (35.0, 25.0, 60.0)],
)
def test_unconfirmed_entry_matches_worst_configured_base_fee_for_every_order_style(
    style: str,
    maker: float,
    taker: float,
    total: float,
) -> None:
    unknown = CostEstimate(kind="unknown", provenance="no attributable liquidity-role evidence")
    model = fit(
        rows(action=style, costs=costs(fees=unknown)),
        maker_fee_bps=maker,
        taker_fee_bps=taker,
    )
    result = decide(model)
    value = action(result, style)
    assert value.costs is not None
    assert value.costs.total_fee_bps == total
    assert value.costs.fees.bps == total
    assert value.costs.fees.kind == "estimated"
    assert "entry_liquidity_unconfirmed_use_worst_configured_fee" in value.costs.fees.provenance
    assert model.provenance.entry_fee_policy == (
        "entry_liquidity_unconfirmed_use_worst_configured_fee"
    )
    assert result.expected_net_edge_bps == 80 - total - 4


def test_move_that_only_cleared_the_old_forty_bps_fee_estimate_is_rejected() -> None:
    unknown = CostEstimate(kind="unknown", provenance="no attributable liquidity-role evidence")
    model = fit(rows(gross_return_bps=49.0, costs=costs(fees=unknown)))
    cell = model.cells[0]
    assert cell.fit is not None and cell.costs is not None
    assert cell.costs.total_fee_bps == 50
    assert cell.fit.expected_net_edge_bps == -5
    assert model.status == "failed_validation"
    assert decide(model).selected_action == "NO_TRADE"


def test_inventory_withheld_base_fee_is_not_subtracted_twice_from_actual_cash_flow() -> None:
    bought_quantity, buy_price, sell_price = 1.0, 100.0, 101.0
    base_fee_quantity = bought_quantity * 25 / 10_000
    sold_quantity = bought_quantity - base_fee_quantity
    entry_value = bought_quantity * buy_price
    sell_value = sold_quantity * sell_price
    exit_cash_fee = sell_value * 25 / 10_000
    entry_fee_net_return = (sell_value - entry_value) / entry_value * 10_000
    exit_fee_bps = exit_cash_fee / entry_value * 10_000
    value = per_leg_costs(
        CostEstimate(
            bps=25.0,
            kind="estimated",
            included_in_return=True,
            provenance="configured base fee withheld from the already-valued sold quantity",
        ),
        CostEstimate(
            bps=exit_fee_bps,
            kind="estimated",
            provenance="sell cash value times configured taker fee / entry notional",
        ),
    )
    model = fit(
        rows(
            gross_return_bps=entry_fee_net_return,
            return_basis="entry_fee_net_fill_to_fill",
            costs=value,
        )
    )
    result = decide(model)
    assert result.profitability_validated
    assert result.expected_net_pnl_usd == pytest.approx(sell_value - entry_value - exit_cash_fee)
    assert result.expected_net_edge_bps == pytest.approx(49.563125)
    modeled = action(result, "PASSIVE_BUY").costs
    assert modeled is not None and modeled.entry_fee is not None and modeled.exit_fee is not None
    assert modeled.entry_fee.included_in_return
    assert not modeled.exit_fee.included_in_return
    assert modeled.additional_bps() == pytest.approx(exit_fee_bps)
    assert modeled.total_fee_bps == pytest.approx(25 + exit_fee_bps)
    assert modeled.fees.bps is None
    assert EconomicModel.model_validate_json(model.canonical_json()) == model
    assert decision_is_executable(result, **fence_arguments(model))


def test_flat_price_base_fee_net_return_pays_only_the_remaining_conservative_cash_fee() -> None:
    unknown_cash_fee = CostEstimate(kind="unknown", provenance="cash exit fee not yet realized")
    value = per_leg_costs(
        CostEstimate(
            bps=25.0,
            kind="estimated",
            included_in_return=True,
            provenance="entry inventory reserve already reduces sellable quantity",
        ),
        unknown_cash_fee,
    )
    model = fit(
        rows(
            gross_return_bps=-25.0,
            return_basis="entry_fee_net_fill_to_fill",
            costs=value,
        )
    )
    cell = model.cells[0]
    assert cell.fit is not None and cell.costs is not None and cell.costs.exit_fee is not None
    assert cell.costs.total_fee_bps == 50
    assert cell.costs.additional_bps() == 25
    assert cell.costs.exit_fee.kind == "estimated"
    assert cell.fit.expected_net_edge_bps == -50
    assert cell.fit.expected_net_edge_bps != -75
    assert decide(model).selected_action == "NO_TRADE"


def test_partial_entry_fee_net_markout_also_prices_remaining_exit_execution_costs() -> None:
    model = fit(
        rows(
            gross_return_bps=55.0,
            return_basis="entry_fee_net_fill_to_mid",
            costs=per_leg_costs(
                estimate(25.0, included=True),
                estimate(25.0),
                full_execution_costs_embedded=False,
            ),
        )
    )
    result = decide(model)
    assert result.profitability_validated
    assert result.expected_net_edge_bps == 26
    value = action(result, "PASSIVE_BUY")
    assert value.costs is not None
    assert value.costs.total_fee_bps == 50
    assert value.costs.additional_bps() == 29


def test_per_leg_fee_amounts_scale_once_with_partial_fills() -> None:
    model = fit(
        rows(
            filled_fraction=0.25,
            gross_return_bps=55.0,
            return_basis="entry_fee_net_fill_to_fill",
            costs=per_leg_costs(estimate(25.0, included=True), estimate(25.0)),
        )
    )
    result = decide(model)
    assert result.profitability_validated
    assert result.expected_net_edge_bps == 7.5
    value = action(result, "PASSIVE_BUY")
    assert value.costs is not None and value.costs.entry_fee is not None
    assert value.costs.total_fee_bps == 12.5
    assert value.costs.entry_fee.bps == 6.25
    assert value.costs.additional_bps() == 6.25


def test_unknown_embedded_entry_fee_amount_is_not_assumed_to_satisfy_the_fee_reserve() -> None:
    value = per_leg_costs(
        CostEstimate(kind="embedded", included_in_return=True, provenance="amount is unobserved"),
        estimate(25.0),
    )
    assert value.total_fee_bps is None
    model = fit(rows(return_basis="entry_fee_net_fill_to_fill", costs=value))
    assert model.status == "no_support"
    assert "UNPRICED_EMBEDDED_FEES" in model.reason_codes
    assert decide(model).selected_action == "NO_TRADE"


@pytest.mark.parametrize("included", [False, True])
def test_entry_fee_estimate_keeps_exact_unconfirmed_liquidity_provenance(included: bool) -> None:
    marker = "entry_liquidity_unconfirmed_use_worst_configured_fee"
    model = fit(
        rows(
            gross_return_bps=55.0 if included else 80.0,
            return_basis="entry_fee_net_fill_to_fill" if included else "fill_to_fill",
            costs=per_leg_costs(
                CostEstimate(
                    bps=25.0,
                    kind="estimated",
                    included_in_return=included,
                    provenance=marker,
                ),
                CostEstimate(bps=25.0, kind="estimated", provenance="configured cash fee budget"),
            ),
        )
    )
    result = decide(model)
    value = action(result, "PASSIVE_BUY")
    assert result.profitability_validated
    assert value.costs is not None and value.costs.entry_fee is not None
    assert value.costs.entry_fee.kind == "estimated"
    assert value.costs.entry_fee.provenance == marker
    assert value.costs.entry_fee.included_in_return is included
    assert result.expected_net_edge_bps == 30


def test_missing_fee_legs_get_explicit_estimates_not_observed_fee_postings() -> None:
    unknown = CostEstimate(kind="unknown", provenance="no FEE or CFEE posting")
    model = fit(
        rows(
            return_basis="fill_to_fill",
            costs=per_leg_costs(unknown, unknown),
        )
    )
    value = action(decide(model), "PASSIVE_BUY")
    assert value.costs is not None
    assert value.costs.entry_fee is not None and value.costs.exit_fee is not None
    assert value.costs.entry_fee.kind == value.costs.exit_fee.kind == "estimated"
    assert value.costs.entry_fee.provenance == (
        "entry_liquidity_unconfirmed_use_worst_configured_fee"
    )
    assert value.costs.total_fee_bps == 50


@pytest.mark.parametrize("known_fraction", [None, 0.0, 1.0])
def test_market_taker_side_coverage_never_proves_our_order_was_maker(
    known_fraction: float | None,
) -> None:
    state = {**FEATURES, "known_taker_fraction_5s": known_fraction}
    model = fit(rows(features=state, costs=costs(fees=estimate(40.0))))
    assert model.status == "no_support"
    assert "FEES_BELOW_CONFIGURED_SCENARIO" in model.reason_codes
    assert decide(model, features=state).selected_action == "NO_TRADE"


def test_partial_quantity_principal_cannot_satisfy_the_full_embedded_base_fee_budget() -> None:
    value = per_leg_costs(
        CostEstimate(
            bps=18.0,
            kind="embedded",
            included_in_return=True,
            provenance="observed quantity-difference principal; not an actual fee posting",
        ),
        CostEstimate(bps=25.0, kind="estimated", provenance="configured cash fee budget"),
    )
    model = fit(
        rows(
            return_basis="entry_fee_net_fill_to_fill",
            gross_return_bps=62.0,
            costs=value,
        )
    )
    assert model.status == "no_support"
    assert "FEES_BELOW_CONFIGURED_SCENARIO" in model.reason_codes
    assert decide(model).selected_action == "NO_TRADE"


def test_fee_legs_and_return_basis_must_unambiguously_describe_the_same_cash_flow() -> None:
    with pytest.raises(ValidationError, match="both entry_fee and exit_fee"):
        costs(entry_fee=estimate(25.0))
    with pytest.raises(ValidationError, match="cannot both be charged"):
        costs(entry_fee=estimate(25.0), exit_fee=estimate(25.0))
    partial = per_leg_costs(estimate(25.0, included=True), estimate(25.0))
    for basis in ("fill_to_fill", "net_fill_to_fill"):
        with pytest.raises(ValidationError, match="fee inclusion"):
            sample(0, return_basis=basis, costs=partial)
    with pytest.raises(ValidationError, match="entry-fee-net"):
        sample(
            0,
            return_basis="entry_fee_net_fill_to_fill",
            costs=per_leg_costs(estimate(25.0), estimate(25.0)),
        )
    with pytest.raises(ValidationError, match="historical entry fee"):
        sample(
            0,
            action="PASSIVE_SELL",
            position_effect="close_long",
            return_basis="fill_to_fill",
            costs=per_leg_costs(estimate(25.0), estimate(25.0)),
        )


def test_fully_fee_net_per_leg_return_does_not_pay_either_fee_again() -> None:
    value = per_leg_costs(estimate(25.0, included=True), estimate(25.0, included=True))
    result = decide(
        fit(
            rows(
                gross_return_bps=30.0,
                return_basis="net_fill_to_fill",
                costs=value,
            )
        )
    )
    assert result.expected_net_edge_bps == 30
    assert value.total_fee_bps == 50
    assert value.additional_bps() == 0


def test_obsolete_fee_policy_and_underpriced_artifact_cannot_authorize_orders() -> None:
    model = fit()
    old = json.loads(model.canonical_json())
    old["provenance"].pop("entry_fee_policy")
    old.pop("model_id")
    with pytest.raises(ValidationError, match="entry_fee_policy"):
        EconomicModel.model_validate_json(json.dumps(old))
    underpriced = json.loads(model.canonical_json())
    underpriced["cells"][0]["costs"]["fees"]["bps"] = 40.0
    underpriced.pop("model_id")
    with pytest.raises(ValidationError, match="ledger fee reserve"):
        EconomicModel.model_validate_json(json.dumps(underpriced))


def test_fee_metadata_cannot_be_patched_without_refitting_its_underpriced_payoff() -> None:
    payload = json.loads(fit().canonical_json())
    payload["cells"][0]["fit"]["conditional_net_return_bps"] += 10
    payload["cells"][0]["fit"]["expected_net_edge_bps"] += 10
    payload.pop("model_id")
    with pytest.raises(ValidationError, match="fit payoff does not reconcile"):
        EconomicModel.model_validate_json(json.dumps(payload))
    priced = action(decide(fit()), "PASSIVE_BUY")
    assert priced.conditional_net_return_bps is not None
    with pytest.raises(ValidationError, match="action payoff does not reconcile"):
        ActionValue.model_validate(
            {
                **priced.model_dump(),
                "conditional_net_return_bps": priced.conditional_net_return_bps + 10,
            }
        )


@pytest.mark.parametrize(
    "payload",
    [
        {"bps": 0.0, "kind": "unknown", "provenance": "unknown is not zero"},
        {"bps": None, "kind": "estimated", "provenance": "missing"},
        {"bps": 1.0, "kind": "embedded", "included_in_return": False, "provenance": "bad"},
        {"bps": -1.0, "kind": "observed", "provenance": "negative"},
        {"bps": float("nan"), "kind": "estimated", "provenance": "not finite"},
        {"bps": float("inf"), "kind": "estimated", "provenance": "not finite"},
        {"bps": "1", "kind": "estimated", "provenance": "not numeric"},
        {"bps": True, "kind": "estimated", "provenance": "not numeric"},
        {"bps": 1.0, "kind": "estimated", "provenance": " "},
    ],
)
def test_invalid_cost_payloads_fail_explicitly(payload: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        CostEstimate.model_validate(payload)


def test_residual_cannot_cover_priced_costs_or_hide_missing_independent_validation() -> None:
    with pytest.raises(ValidationError, match="already priced"):
        costs(
            residual=CostEstimate(
                bps=3.0,
                kind="conservative_allowance",
                provenance="test",
                validation_sha256=VALIDATION,
            ),
            residual_covers=("slippage",),
        )
    with pytest.raises(ValidationError, match="separately validated"):
        costs(residual=CostEstimate(bps=3.0, kind="estimated", provenance="unsupported"))
    with pytest.raises(ValidationError, match="positive evidenced"):
        costs(residual_covers=("impact",))
    with pytest.raises(ValidationError, match="explicit schedule"):
        costs(residual_covers=("fees",))


@pytest.mark.parametrize(
    "overrides",
    [
        {"decision_at": START.replace(tzinfo=None)},
        {"decision_at": 1_789_000_000_000},
        {"decision_at": START.isoformat()},
        {"feature_observed_at": START + timedelta(microseconds=1)},
        {"label_matured_at": START + timedelta(seconds=4.999)},
        {"return_end_at": START + timedelta(seconds=11)},
        {"return_end_at": START + timedelta(seconds=0.1)},
        {"return_end_at": None},
        {"time_unit": "milliseconds"},
        {"return_unit": "percent"},
        {"gross_return_bps": float("nan")},
        {"gross_return_bps": float("inf")},
        {"gross_return_bps": "80"},
        {"gross_return_bps": True},
        {"filled": 1},
        {"filled_fraction": 0.0},
        {"filled_fraction": 1.01},
        {"fill_delay_seconds": -1.0},
        {"fill_delay_seconds": 250_000_000.0},
        {"horizon_seconds": 0.0},
        {"notional_usd": 0.0},
        {"quote_age_seconds": -0.1},
        {"features": {"ofi": float("nan")}},
        {"features": {"ofi": float("inf")}},
        {"features": {"ofi": True}},
        {"features": {"": 1.0}},
        {"action": "SHORT"},
        {"family": "negative_momentum"},
        {"account_digest": "wrong"},
        {"sample_id": " "},
        {"unexpected": 1},
    ],
)
def test_strict_finite_causal_sample_payloads(overrides: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        sample(0, **overrides)


def test_inconsistent_fill_and_return_cost_semantics_are_rejected() -> None:
    with pytest.raises(ValidationError, match="unfilled returns"):
        sample(0, filled=False, filled_fraction=0.0)
    with pytest.raises(ValidationError, match="already include adverse"):
        sample(0, costs=costs(adverse_selection=estimate(9.0)))
    with pytest.raises(ValidationError, match="already include spread"):
        sample(0, return_basis="fill_to_fill")
    with pytest.raises(ValidationError, match="unembedded execution"):
        sample(0, costs=costs(spread=estimate(2.0, included=True)))
    with pytest.raises(ValidationError, match="fee inclusion"):
        sample(0, costs=costs(fees=estimate(40.0, included=True)))


def test_late_fill_after_cancel_budget_cannot_be_relabelled_an_unfilled_zero() -> None:
    model = fit(rows(fill_delay_seconds=4.0))
    assert model.status == "no_support"
    assert "FILL_OUTSIDE_PREDICTION_OR_CANCELLATION_BUDGET" in model.reason_codes
    assert decide(model).selected_action == "NO_TRADE"


@pytest.mark.parametrize(
    "overrides",
    [
        {"quote_age_seconds": float("nan")},
        {"decision_latency_seconds": float("inf")},
        {"decision_latency_seconds": -1.0},
        {"spread_bps": "2"},
        {"spread_bps": True},
        {"notional_usd": float("inf")},
        {"notional_usd": 0.0},
        {"owned_inventory_notional_usd": -1.0},
        {"features": {"ofi": float("nan")}},
        {"features": {"ofi": True}},
        {"now": NOW.replace(tzinfo=None)},
        {"maker_fee_bps": "15"},
        {"taker_fee_bps": float("nan")},
    ],
)
def test_invalid_evaluation_inputs_raise_instead_of_success_shaped_fallback(
    overrides: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        decide(fit(), **overrides)


@pytest.mark.parametrize(
    "overrides",
    [
        {"validation_fraction": 0.0},
        {"validation_fraction": 1.0},
        {"error_rate": 0.0},
        {"error_rate": 1e-100},
        {"embargo_seconds": -1.0},
        {"minimum_fit_samples": 1},
        {"minimum_validation_samples": True},
        {"minimum_filled_samples": 1},
        {"maker_fee_bps": float("nan")},
        {"valid_until": CALIBRATED},
    ],
)
def test_invalid_calibration_policy_is_explicit(overrides: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        fit(**overrides)


def test_utc_normalization_and_json_round_trip_do_not_infer_timestamp_units() -> None:
    offset = timezone(timedelta(hours=-4))
    original = sample(0)
    adjusted = sample(
        0,
        decision_at=START.astimezone(offset),
        feature_observed_at=START.astimezone(offset),
        return_end_at=(START + timedelta(seconds=5)).astimezone(offset),
        label_matured_at=(START + timedelta(seconds=5.5)).astimezone(offset),
    )
    assert adjusted == original
    assert adjusted.model_dump_json() == original.model_dump_json()
    assert EconomicSample.model_validate_json(original.model_dump_json()) == original
    payload = json.loads(original.model_dump_json())
    payload["decision_at"] = 1_789_000_000_000
    with pytest.raises(ValidationError):
        EconomicSample.model_validate_json(json.dumps(payload))


def test_final_fence_reprices_current_state_and_preserves_action_identity() -> None:
    model = fit()
    decision = decide(model)
    assert decision_is_executable(decision, **fence_arguments(model))
    assert execution_rejection_reasons(decision, **fence_arguments(model)) == ()
    for overrides in (
        {"spread_bps": 10.0},
        {"features": {**FEATURES, "ofi": -100.0}},
        {"account_digest": "d" * 64},
        {"notional_usd": 101.0},
        {"now": NOW + timedelta(seconds=2)},
        {"now": NOW - timedelta(microseconds=1)},
        {"maker_fee_bps": 18.0},
        {"model": fit(rows(gross_return_bps=81.0))},
        {"model": None},
        {"owned_inventory_notional_usd": 100.0},
    ):
        assert not decision_is_executable(decision, **fence_arguments(model, **overrides))
    assert not decision_is_executable(decide(None), **fence_arguments(model))


def test_final_fence_cannot_reset_elapsed_decision_latency_with_a_fresh_quote() -> None:
    model = fit(
        tuple(
            sample(
                index,
                gross_return_bps=55.0,
                quote_age_seconds=float(index % 2),
                decision_latency_seconds=float(index % 2),
            )
            for index in range(48)
        )
    )
    decision = decide(model)
    assert decision.profitability_validated
    reasons = execution_rejection_reasons(
        decision,
        **fence_arguments(
            model,
            now=NOW + timedelta(seconds=0.5),
            quote_age_seconds=0.0,
            decision_latency_seconds=0.0,
        ),
    )
    assert "FINAL_ACTION_NOT_VALIDATED" in reasons
    assert "UNCERTAINTY_OR_LATENCY_ERASES_NET_EDGE" in reasons


def test_dispatch_preserves_original_prediction_and_feature_age_with_a_fresh_quote() -> None:
    model = fit(
        tuple(
            sample(
                index,
                quote_age_seconds=float(index % 2),
                decision_latency_seconds=float(index % 2),
            )
            for index in range(48)
        )
    )
    original = decide(model, quote_age_seconds=0.75, decision_latency_seconds=0.25)
    assert original.profitability_validated
    final = revalidate_for_dispatch(
        original,
        **fence_arguments(model, now=NOW + timedelta(seconds=0.1)),
    )
    assert final.profitability_validated
    assert final.selected_action == original.selected_action
    assert final.model_id == original.model_id
    assert final.account_digest == original.account_digest
    assert final.features_sha256 == original.features_sha256
    assert final.prediction_expires_at == original.prediction_expires_at
    assert final.valid_until == original.valid_until
    assert final.quote_age_seconds == pytest.approx(0.85)
    assert final.decision_latency_seconds == pytest.approx(0.35)
    assert final.prediction_expires_at == NOW + timedelta(seconds=4.25)


@pytest.mark.parametrize("delay", [2.0, 3.0, 6.0])
def test_get_requests_cannot_reset_entry_or_prediction_expiry_at_final_post(delay: float) -> None:
    model = fit()
    original = decide(model)
    final_time = NOW + timedelta(seconds=delay)
    reset_prediction = decide(model, now=final_time)
    assert reset_prediction.profitability_validated
    final = revalidate_for_dispatch(
        original,
        **fence_arguments(model, now=final_time),
    )
    assert not final.profitability_validated
    assert final.selected_action == "NO_TRADE"
    assert final.prediction_expires_at == original.prediction_expires_at
    assert final.valid_until == original.valid_until
    assert final.decision_latency_seconds >= delay
    assert "DECISION_EXPIRED" in final.reason_codes
    if delay >= 5:
        assert "PREDICTION_EXPIRED" in final.reason_codes


def test_dispatch_cannot_replace_original_features_even_when_new_values_are_in_support() -> None:
    model = fit()
    original = decide(model)
    changed = {**FEATURES, "new_bullish_score": 1.0}
    assert decide(model, features=changed).profitability_validated
    final = revalidate_for_dispatch(original, **fence_arguments(model, features=changed))
    assert final.selected_action == "NO_TRADE"
    assert "DECISION_FEATURES_CHANGED" in final.reason_codes
    unchanged = revalidate_for_dispatch(
        original,
        **fence_arguments(model, features=dict(reversed(tuple(FEATURES.items())))),
    )
    assert unchanged.profitability_validated


def test_dispatch_consumes_realized_favorable_mid_move_before_fees_without_double_spread() -> None:
    model = fit()
    original = decide(model)
    final = revalidate_for_dispatch(original, **fence_arguments(model, current_mid_price=100.1))
    assert final.profitability_validated
    value = action(final, "PASSIVE_BUY")
    original_value = action(original, "PASSIVE_BUY")
    remaining_gross = (80.0 - 10.0) / 1.001
    assert value.realized_favorable_move_bps == pytest.approx(10)
    assert value.price_move_haircut_bps == pytest.approx(80 - remaining_gross)
    assert value.conditional_gross_return_bps == pytest.approx(remaining_gross)
    assert final.expected_net_edge_bps == pytest.approx(remaining_gross - 54)
    assert final.expected_net_edge_bps < original.expected_net_edge_bps
    assert final.conservative_net_edge_bps < original.conservative_net_edge_bps
    assert value.costs == original_value.costs
    assert value.costs is not None and value.costs.total_fee_bps == 50
    assert value.spread_repricing_bps == 0
    assert final.reference_mid_price == 100
    assert final.current_mid_price == 100.1
    assert final.prediction_expires_at == original.prediction_expires_at


def test_dispatch_holds_when_the_move_was_already_realized_before_submission() -> None:
    model = fit()
    original = decide(model)
    assert original.expected_net_edge_bps == 26
    final = revalidate_for_dispatch(original, **fence_arguments(model, current_mid_price=100.4))
    assert final.selected_action == "NO_TRADE"
    assert not final.profitability_validated
    assert "REALIZED_PRICE_MOVE_CONSUMES_EDGE" in final.reason_codes
    value = action(final, "PASSIVE_BUY")
    assert value.available
    assert value.conditional_gross_return_bps == pytest.approx(40 / 1.004)
    assert value.expected_net_edge_bps is not None and value.expected_net_edge_bps < 0
    assert not decision_is_executable(
        original,
        **fence_arguments(model, current_mid_price=100.4),
    )


def test_adverse_price_context_does_not_manufacture_extra_edge_for_stale_features() -> None:
    model = fit()
    result = revalidate_for_dispatch(
        decide(model),
        **fence_arguments(model, current_mid_price=99.99),
    )
    assert result.selected_action == "NO_TRADE"
    assert "ADVERSE_PRICE_CONTEXT_DRIFT" in result.reason_codes
    assert action(result, "PASSIVE_BUY").expected_net_edge_bps is None


def test_reversal_after_a_prior_dispatch_check_cannot_restore_consumed_edge() -> None:
    model = fit()
    first = revalidate_for_dispatch(
        decide(model),
        **fence_arguments(model, current_mid_price=100.1),
    )
    assert first.profitability_validated
    reversed_price = revalidate_for_dispatch(
        first,
        **fence_arguments(model, current_mid_price=100.05),
    )
    assert reversed_price.selected_action == "NO_TRADE"
    assert "ADVERSE_PRICE_CONTEXT_DRIFT" in reversed_price.reason_codes


def test_dispatch_quote_must_not_predate_the_original_reference_even_within_age_support() -> None:
    model = fit(
        tuple(
            sample(
                index,
                quote_age_seconds=float(index % 2),
                decision_latency_seconds=float(index % 2),
            )
            for index in range(48)
        )
    )
    original = decide(model)
    result = revalidate_for_dispatch(
        original,
        **fence_arguments(model, quote_age_seconds=0.1),
    )
    assert result.selected_action == "NO_TRADE"
    assert "DISPATCH_QUOTE_PREDATES_REFERENCE" in result.reason_codes


def test_prediction_expiry_cannot_claim_more_than_the_models_original_horizon() -> None:
    payload = decide(fit()).model_dump()
    payload.pop("decision_id")
    payload["prediction_expires_at"] = NOW + timedelta(seconds=6)
    with pytest.raises(ValidationError, match="original prediction"):
        EconomicDecision.model_validate(payload)


def test_mid_drift_haircut_uses_expected_gross_fill_fraction_once() -> None:
    model = fit(rows(filled_fraction=0.25))
    result = revalidate_for_dispatch(
        decide(model),
        **fence_arguments(model, current_mid_price=100.1),
    )
    value = action(result, "PASSIVE_BUY")
    remaining_gross = (20.0 - 0.25 * 10.0) / 1.001
    assert result.profitability_validated
    assert value.conditional_gross_return_bps == pytest.approx(remaining_gross)
    assert result.expected_net_edge_bps == pytest.approx(remaining_gross - 13.5)


def test_repeated_dispatch_validation_neither_renews_horizon_nor_double_consumes_move() -> None:
    model = fit(
        tuple(
            sample(
                index,
                quote_age_seconds=float(index % 2),
                decision_latency_seconds=float(index % 2),
            )
            for index in range(48)
        )
    )
    original = decide(model)
    first = revalidate_for_dispatch(
        original,
        **fence_arguments(
            model,
            now=NOW + timedelta(seconds=0.1),
            current_mid_price=100.05,
        ),
    )
    assert first.profitability_validated
    arguments = fence_arguments(
        model,
        now=NOW + timedelta(seconds=0.2),
        current_mid_price=100.1,
    )
    chained = revalidate_for_dispatch(first, **arguments)
    direct = revalidate_for_dispatch(original, **arguments)
    assert chained.profitability_validated
    assert chained == direct
    assert chained.prediction_expires_at == original.prediction_expires_at
    assert chained.valid_until == original.valid_until
    assert EconomicDecision.model_validate_json(chained.model_dump_json()) == chained


def test_spread_only_quote_change_is_not_a_realized_mid_move_or_a_second_spread_charge() -> None:
    model = fit(
        tuple(
            sample(
                index,
                spread_bps=2.0 if index % 2 == 0 else 4.0,
                gross_return_bps=80.0 if index % 2 == 0 else 82.0,
                costs=costs(spread=estimate(2.0 if index % 2 == 0 else 4.0)),
            )
            for index in range(48)
        )
    )
    original = decide(model)
    arguments = fence_arguments(model, spread_bps=4.0, current_mid_price=100.0)
    first = revalidate_for_dispatch(original, **arguments)
    again = revalidate_for_dispatch(first, **arguments)
    assert first.profitability_validated
    assert first == again
    value = action(first, "PASSIVE_BUY")
    assert value.realized_favorable_move_bps == value.price_move_haircut_bps == 0
    assert value.spread_repricing_bps == 1
    assert first.expected_net_edge_bps == 25


def test_dispatch_never_switches_to_another_action_after_repricing() -> None:
    observations = (
        *rows(gross_return_bps=160.0, filled_fraction=0.5),
        *(
            sample(
                index,
                sample_id=f"aggressive-{index}",
                action="AGGRESSIVE_BUY",
                gross_return_bps=110.0,
            )
            for index in range(48)
        ),
    )
    model = fit(observations)
    original = decide(model)
    assert original.selected_action == "AGGRESSIVE_BUY"
    result = revalidate_for_dispatch(
        original,
        **fence_arguments(model, current_mid_price=100.1),
    )
    assert result.selected_action == "NO_TRADE"
    assert "DISPATCH_ACTION_CHANGED" in result.reason_codes


def test_missing_price_context_cannot_authorize_an_order() -> None:
    model = fit()
    absent_initial_price = decide(model, reference_mid_price=None)
    assert absent_initial_price.selected_action == "NO_TRADE"
    assert "MISSING_DECISION_PRICE_CONTEXT" in absent_initial_price.reason_codes
    result = revalidate_for_dispatch(
        decide(model),
        **fence_arguments(model, current_mid_price=None),
    )
    assert result.selected_action == "NO_TRADE"
    assert "MISSING_DISPATCH_PRICE_CONTEXT" in result.reason_codes


@pytest.mark.parametrize("price", [0.0, -1.0, float("nan"), float("inf"), True, "100"])
def test_invalid_reference_or_dispatch_price_is_an_explicit_error(price: object) -> None:
    model = fit()
    with pytest.raises(ValueError):
        decide(model, reference_mid_price=price)
    with pytest.raises(ValueError):
        revalidate_for_dispatch(decide(model), **fence_arguments(model, current_mid_price=price))


def test_nonfinite_derived_price_drift_is_not_an_available_budget() -> None:
    model = fit()
    original = decide(model, reference_mid_price=1e-300)
    with pytest.raises(ValueError, match="finite"):
        revalidate_for_dispatch(
            original,
            **fence_arguments(model, current_mid_price=1e300),
        )


@pytest.mark.parametrize(
    ("degrees", "expected"),
    [(1, 6.313751515), (9, 1.833112933), (30, 1.697260887), (100, 1.660234326)],
)
def test_studentized_uncertainty_matches_reference_small_sample_quantiles(
    degrees: int, expected: float
) -> None:
    critical = _student_critical(0.05, degrees)
    assert isfinite(critical)
    assert critical == pytest.approx(expected, rel=1e-8)
    assert _student_critical(0.01, degrees) > critical


def test_wilson_intervals_contain_exact_empirical_frequency_at_small_sample_boundaries() -> None:
    for count in range(1, 230):
        for filled in (0, count // 2, count):
            lower, upper = _wilson(filled, count, 0.05 / 6)
            assert 0 <= lower <= filled / count <= upper <= 1


def test_unavailable_and_baseline_payloads_cannot_smuggle_numerical_authorization() -> None:
    with pytest.raises(ValidationError, match="null valuations"):
        ActionValue(
            action="AGGRESSIVE_BUY",
            available=False,
            reason_codes=("NO_EVIDENCE",),
            expected_net_edge_bps=100.0,
        )
    with pytest.raises(ValidationError, match="zero-risk"):
        ActionValue(
            action="NO_TRADE",
            available=True,
            reason_codes=("BAD_BASELINE",),
            expected_net_edge_bps=1.0,
            expected_net_pnl_usd=1.0,
            conservative_net_edge_bps=1.0,
            uncertainty=0.0,
        )
    with pytest.raises(ValidationError, match="empirically known"):
        ActionValue(
            action="PASSIVE_BUY",
            available=True,
            reason_codes=("FABRICATED_CONFIDENCE",),
            expected_net_edge_bps=100.0,
            expected_net_pnl_usd=1.0,
            conservative_net_edge_bps=90.0,
        )
    with pytest.raises(ValidationError):
        ActionValue(
            action="PASSIVE_BUY",
            available=False,
            reason_codes=("UNKNOWN",),
            confidence=0.8,
        )
