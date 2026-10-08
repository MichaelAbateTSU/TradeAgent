"""Offline synthetic evidence only; no orders, model fits, clients or production IO."""

from __future__ import annotations

import json
from decimal import ROUND_DOWN, Decimal, Inexact, localcontext
from fractions import Fraction
from pathlib import Path

import pytest
from pydantic import ValidationError

import tradeagent.scalping_profitability_readiness as readiness
from tradeagent.scalping_cost_math import (
    long_cash_return_bps,
    long_quote_funded_cash_return_bps,
    retained_base_quantity,
)
from tradeagent.scalping_profitability_readiness import (
    NS,
    SLOTS,
    AccountEligibility,
    AdditionalFriction,
    ApprovedEvidence,
    CostScenario,
    ExecutionRelationship,
    FeeSchedule,
    FrozenContract,
    ReadinessEnvelope,
    Role,
    SourceQuality,
    TrustManifest,
    bounded_json,
    closeout_example,
    cost_hurdle,
    evaluate_readiness,
)


def scenario(**changes: object) -> CostScenario:
    data: dict[str, object] = {
        "scenario_id": "predeclared-cost-fixture",
        "venue": "kraken",
        "provenance": "account_evidence",
        "basis": "raw_ask_to_bid",
        "entry_liquidity": "unconfirmed_worst_rate",
        "entry_fee_bps": "80",
        "exit_fee_bps": "80",
        "entry_charge": "base_inventory",
        "exit_charge": "cash",
        "residual_nonembedded_bps": "2",
        "residual_basis": "all_in_entry_capital",
        "target_net_bps": "0",
        "observed_basis_return_bps": "200",
    }
    return CostScenario.model_validate_json(json.dumps({**data, **changes}))


def contract() -> FrozenContract:
    start = 1_800_000_000 * NS  # Synthetic fixed clock, not an actual freeze.
    return FrozenContract(
        protocol_sha256="a" * 64,
        frozen_at_ns=start - 60 * NS,
        start_ns=start,
        end_ns=start + 72 * 3600 * NS,
        symbols=("BTC/USD", "ETH/USD"),
        slots_per_symbol=25920,
        cadence_seconds=10,
        horizon_seconds=60,
        settlement_seconds=2,
        provider_freshness_seconds=2,
        receipt_freshness_seconds=2,
        best_side_notional_usd="10.25",
    )


def source(**changes: object) -> SourceQuality:
    fixed = contract()
    data = {
        "kind": "source_quality",
        "authority_id": "fixture-source-auditor",
        "source_id": "qualified-fixture-not-a-market",
        "source_venue": "kraken",
        "source_kind": "native_checksum_book",
        "contract": fixed.model_dump(mode="json"),
        "report_as_of_ns": fixed.end_ns + 75 * NS,
        "state": "completed",
        "symbols": [
            {
                "symbol": symbol,
                "matured_slots": SLOTS,
                "complete_labels": SLOTS,
                "rejected_labels": 0,
                "unwritten_mature_labels": 0,
                "observed_evaluations": SLOTS,
            }
            for symbol in ("BTC/USD", "ETH/USD")
        ],
        "raw_frames_verified": 100000,
        "final_quality_frames": 100000,
        "checksum_failures": 0,
        "clock_failures": 0,
        "continuity_breaks": 0,
        "raw_sequence_failures": 0,
        "journal_root": "b" * 64,
        "original_artifact_sha256": "c" * 64,
    }
    return SourceQuality.model_validate_json(json.dumps({**data, **changes}))


def qualified(
    quality: SourceQuality | None = None,
    *,
    cost: CostScenario | None = None,
    change_proof: str | None = None,
    changes: dict[str, object] | None = None,
) -> tuple[ReadinessEnvelope, TrustManifest]:
    quality = quality or source()
    cost = cost or scenario()
    account_digest = "d" * 64
    fixed = quality.contract
    execution = ExecutionRelationship(
        kind="execution_relationship",
        authority_id="fixture-execution-auditor",
        source_quality_sha256=quality.identity,
        source_venue="kraken",
        execution_venue="kraken",
        account_digest=account_digest,
        symbols=("BTC/USD", "ETH/USD"),
        methodology="independent_same_venue_quote_fill_validation",
        quote_fill_pairs=100,
        venue_identifier_mismatches=0,
        causal_violations=0,
        quantity_or_price_discrepancies=0,
        supporting_records_sha256="e" * 64,
        validated_return_basis=cost.basis,
        fee_embedding_records_sha256=None,
        embedded_entry_charge=cost.entry_charge if cost.basis != "raw_ask_to_bid" else None,
        embedded_exit_charge=cost.exit_charge if cost.basis == "cash_net_ask_to_bid" else None,
        verified_entry_liquidity="unconfirmed",
        verified_at_ns=fixed.end_ns + 75 * NS,
    )
    account = AccountEligibility(
        kind="account_eligibility",
        authority_id="fixture-account-provider",
        venue="kraken",
        account_digest=account_digest,
        eligibility="eligible",
        methodology="account_specific_provider_and_jurisdiction_evidence",
        effective_from_ns=fixed.start_ns,
        effective_until_ns=fixed.end_ns + 3600 * NS,
    )
    fees = FeeSchedule(
        kind="fee_schedule",
        authority_id="fixture-fee-provider",
        venue="kraken",
        account_digest=account_digest,
        scope="account_specific",
        maker_bps=Decimal(40),
        taker_bps=Decimal(80),
        entry_charge=cost.entry_charge,
        entry_charge_evidence_sha256="9" * 64,
        entry_charge_evidence_kind="confirmed_fill_fee_records",
        entry_charge_symbols=("BTC/USD", "ETH/USD"),
        exit_charge=cost.exit_charge,
        exit_charge_evidence_sha256="8" * 64,
        exit_charge_evidence_kind="confirmed_fill_fee_records",
        exit_charge_symbols=("BTC/USD", "ETH/USD"),
        effective_from_ns=fixed.start_ns,
        effective_until_ns=fixed.end_ns + 3600 * NS,
    )
    friction = AdditionalFriction(
        kind="additional_friction",
        authority_id="fixture-friction-auditor",
        execution_venue="kraken",
        account_digest=account_digest,
        source_quality_sha256=quality.identity,
        basis=cost.basis,
        residual_bps=Decimal(2),
        residual_basis=cost.residual_basis,
        covers=("slippage", "impact", "adverse_selection"),
        methodology="independently_validated_nonembedded_allowance",
        supporting_records_sha256="f" * 64,
    )
    payloads = [
        row.model_dump(mode="json") for row in (quality, execution, account, fees, friction)
    ]
    if change_proof is not None:
        for payload in payloads:
            if payload["kind"] == change_proof:
                payload.update(changes or {})
    envelope = ReadinessEnvelope.model_validate_json(
        json.dumps(
            {
                "schema_version": "profitability-readiness-input-v1",
                "evidence_mode": "hypothetical_fixture",
                "assessed_at_ns": fixed.end_ns + 75 * NS,
                "predeclared_at_ns": fixed.frozen_at_ns,
                "execution_venue": "kraken",
                "account_digest": account_digest,
                "sources": [
                    {
                        "source_id": quality.source_id,
                        "source_quality_sha256": quality.identity,
                        "cost_scenario_id": cost.scenario_id,
                        "execution_relationship_sha256": execution.identity,
                        "account_eligibility_sha256": account.identity,
                        "fee_schedule_sha256": fees.identity,
                        "additional_friction_sha256": friction.identity,
                    }
                ],
                "evidence": payloads,
                "cost_scenarios": [cost.model_dump(mode="json")],
            }
        )
    )
    # Changing a proof also updates the linked digest, so negative tests isolate
    # its substantive scope rather than only testing broken content hashes.
    binding = envelope.sources[0].model_dump(mode="json")
    by_kind: dict[str, readiness.Evidence] = {row.kind: row for row in envelope.evidence}
    for field, kind in (
        ("execution_relationship_sha256", "execution_relationship"),
        ("account_eligibility_sha256", "account_eligibility"),
        ("fee_schedule_sha256", "fee_schedule"),
        ("additional_friction_sha256", "additional_friction"),
    ):
        binding[field] = by_kind[kind].identity
    envelope = ReadinessEnvelope.model_validate_json(
        json.dumps(
            {
                **envelope.model_dump(mode="json"),
                "sources": [binding],
            }
        )
    )
    roles: dict[str, Role] = {
        "source_quality": "source_auditor",
        "execution_relationship": "execution_auditor",
        "account_eligibility": "account_provider",
        "fee_schedule": "fee_provider",
        "additional_friction": "friction_auditor",
    }
    manifest = TrustManifest(
        schema_version="profitability-evidence-approval-v1",
        mode="hypothetical_fixture",
        approved_at_ns=envelope.assessed_at_ns,
        predeclared_plan_sha256=envelope.plan_identity,
        entries=tuple(
            ApprovedEvidence(
                artifact_sha256=row.identity,
                authority_id=row.authority_id,
                role=roles[row.kind],
            )
            for row in envelope.evidence
        ),
    )
    return envelope, manifest


def report(envelope: ReadinessEnvelope, manifest: TrustManifest) -> readiness.ReadinessReport:
    return evaluate_readiness(envelope, manifest, trusted_manifest_sha256=manifest.identity)


def test_qualified_fixture_is_not_profitability_model_or_trading_permission() -> None:
    envelope, manifest = qualified()
    result = report(envelope, manifest)
    assert result.decision == "QUALIFIED_FIXTURE_ONLY"
    assert result.offline_research_ready and result.trust_root_matched
    assert not result.model_ready and not result.trading_allowed and not result.profitability_claim
    assert result.cost_frontiers[0].discovery_only
    assert result.cost_frontiers[0].observed_target_satisfied_exactly


@pytest.mark.parametrize("state", ["failed_incomplete", "collecting"])
def test_failed_or_live_source_stays_no_go_even_with_good_fixture_counts(state: str) -> None:
    envelope, manifest = qualified(source(state=state))
    result = report(envelope, manifest)
    assert result.decision == "NO_GO"
    assert "SOURCE_NOT_COMPLETED_NO_RESTART" in result.sources[0].reasons
    assert not result.model_ready and not result.trading_allowed


def test_wrong_venue_with_99_percent_and_approved_evidence_is_rejected() -> None:
    rows = source().model_dump(mode="json")["symbols"]
    assert isinstance(rows, list)
    for row in rows:
        assert isinstance(row, dict)
        row.update(complete_labels=25661, rejected_labels=259)
    envelope, manifest = qualified(source(source_venue="alpaca_paper", symbols=rows))
    result = report(envelope, manifest)
    assert result.decision == "NO_GO"
    assert "SOURCE_EXECUTION_VENUE_MISMATCH" in result.sources[0].reasons


def test_quality_is_per_symbol_not_pooled_and_all_missingness_remains() -> None:
    rows = source().model_dump(mode="json")["symbols"]
    assert isinstance(rows, list) and isinstance(rows[1], dict)
    rows[1].update(complete_labels=24364, rejected_labels=1556)
    envelope, manifest = qualified(source(symbols=rows))
    result = report(envelope, manifest)
    assert result.decision == "NO_GO"
    assert "ETH/USD:FULL_MATURE_QUALITY_BELOW_95" in result.sources[0].reasons
    assert result.sources[0].coverage[1].matured_at_assessment == SLOTS
    rows[1].update(rejected_labels=0, unwritten_mature_labels=1556)
    envelope, manifest = qualified(source(symbols=rows))
    assert report(envelope, manifest).decision == "NO_GO"
    assert report(envelope, manifest).sources[0].coverage[1].unwritten_mature_labels == 1556


def test_maturity_denominator_cannot_be_reported_as_observed_or_partial_slice() -> None:
    fixed = contract()
    with pytest.raises(ValidationError, match="maturity denominator"):
        source(report_as_of_ns=fixed.start_ns + 62 * NS)
    assert fixed.matured(fixed.start_ns + 62 * NS - 1) == 0
    assert fixed.matured(fixed.start_ns + 62 * NS) == 1
    assert fixed.matured(fixed.end_ns + 52 * NS) == SLOTS
    partial = source(
        report_as_of_ns=fixed.start_ns + 72 * NS,
        state="collecting",
        symbols=[
            {
                "symbol": symbol,
                "matured_slots": 2,
                "complete_labels": 2,
                "rejected_labels": 0,
                "unwritten_mature_labels": 0,
                "observed_evaluations": 2,
            }
            for symbol in ("BTC/USD", "ETH/USD")
        ],
    )
    envelope, manifest = qualified(partial)
    result = report(envelope, manifest)
    assert result.decision == "NO_GO"
    assert result.sources[0].coverage[0].reported_matured_slots == 2
    assert result.sources[0].coverage[0].matured_at_assessment == SLOTS
    assert Decimal(result.sources[0].coverage[0].full_mature_primary_percent) < 1


@pytest.mark.parametrize(
    ("kind", "changes", "reason"),
    [
        ("account_eligibility", {"eligibility": "unknown"}, "VENUE_ACCOUNT_ELIGIBILITY_UNPROVEN"),
        ("account_eligibility", {"account_digest": "0" * 64}, "VENUE_ACCOUNT_ELIGIBILITY_UNPROVEN"),
        ("fee_schedule", {"scope": "public_tier"}, "ACCOUNT_SPECIFIC_FEES_UNPROVEN"),
        ("fee_schedule", {"scope": "configured_model"}, "ACCOUNT_SPECIFIC_FEES_UNPROVEN"),
        ("fee_schedule", {"maker_bps": None}, "ACCOUNT_SPECIFIC_FEES_UNPROVEN"),
        ("fee_schedule", {"scope": "unknown"}, "ACCOUNT_SPECIFIC_FEES_UNPROVEN"),
        ("fee_schedule", {"entry_charge": None}, "ENTRY_FEE_TREATMENT_UNPROVEN"),
        ("fee_schedule", {"entry_charge": "unknown"}, "ENTRY_FEE_TREATMENT_UNPROVEN"),
        (
            "fee_schedule",
            {"entry_charge_evidence_sha256": None},
            "ENTRY_FEE_TREATMENT_UNPROVEN",
        ),
        ("fee_schedule", {"entry_charge_symbols": None}, "ENTRY_FEE_TREATMENT_UNPROVEN"),
        (
            "fee_schedule",
            {"entry_charge_evidence_kind": "preference_only"},
            "ENTRY_FEE_TREATMENT_UNPROVEN",
        ),
        ("fee_schedule", {"exit_charge": None}, "EXIT_FEE_TREATMENT_UNPROVEN"),
        ("fee_schedule", {"exit_charge": "unknown"}, "EXIT_FEE_TREATMENT_UNPROVEN"),
        (
            "fee_schedule",
            {"exit_charge_evidence_sha256": None},
            "EXIT_FEE_TREATMENT_UNPROVEN",
        ),
        ("fee_schedule", {"exit_charge_symbols": None}, "EXIT_FEE_TREATMENT_UNPROVEN"),
        (
            "fee_schedule",
            {"exit_charge_evidence_kind": "preference_only"},
            "EXIT_FEE_TREATMENT_UNPROVEN",
        ),
        (
            "fee_schedule",
            {"exit_charge": "base_inventory"},
            "EXIT_BASE_FEE_TREATMENT_UNSUPPORTED",
        ),
        (
            "fee_schedule",
            {"entry_charge": "quote_added"},
            "SCENARIO_ENTRY_FEE_TREATMENT_DIFFERS_FROM_ACCOUNT_EVIDENCE",
        ),
        ("fee_schedule", {"entry_charge": "cash"}, None),
        (
            "execution_relationship",
            {"quote_fill_pairs": 0},
            "SOURCE_EXECUTION_RELATIONSHIP_UNPROVEN",
        ),
        (
            "execution_relationship",
            {"execution_venue": "alpaca_paper"},
            "SOURCE_EXECUTION_RELATIONSHIP_UNPROVEN",
        ),
        (
            "execution_relationship",
            {"source_quality_sha256": "0" * 64},
            "SOURCE_EXECUTION_RELATIONSHIP_UNPROVEN",
        ),
        (
            "execution_relationship",
            {"causal_violations": 1},
            "SOURCE_EXECUTION_RELATIONSHIP_UNPROVEN",
        ),
        ("additional_friction", {"residual_bps": "0"}, "NONEMBEDDED_FRICTION_UNPROVEN"),
        ("additional_friction", {"residual_basis": None}, "NONEMBEDDED_FRICTION_UNPROVEN"),
        ("additional_friction", {"residual_basis": "unknown"}, "NONEMBEDDED_FRICTION_UNPROVEN"),
        (
            "additional_friction",
            {"residual_basis": "entry_trade_notional"},
            "NONEMBEDDED_FRICTION_UNPROVEN",
        ),
    ],
)
def test_unknown_or_mismatching_evidence_fails_closed(
    kind: str,
    changes: dict[str, object],
    reason: str | None,
) -> None:
    if reason is None:
        with pytest.raises(ValidationError):
            qualified(change_proof=kind, changes=changes)
        return
    envelope, manifest = qualified(change_proof=kind, changes=changes)
    result = report(envelope, manifest)
    assert result.decision == "NO_GO" and reason in result.sources[0].reasons


def test_unapproved_self_attestation_and_tampered_trust_root_cannot_qualify() -> None:
    envelope, manifest = qualified()
    assert (
        evaluate_readiness(
            envelope,
            manifest,
            trusted_manifest_sha256="0" * 64,
        ).decision
        == "NO_GO"
    )
    empty = manifest.model_copy(update={"entries": ()})
    assert report(envelope, empty).decision == "NO_GO"
    changed = manifest.model_copy(
        update={
            "entries": (
                manifest.entries[0].model_copy(update={"authority_id": "not-the-auditor"}),
                *manifest.entries[1:],
            )
        }
    )
    assert report(envelope, changed).decision == "NO_GO"


def test_predeclared_plan_unknown_late_or_modified_cannot_qualify() -> None:
    envelope, manifest = qualified()
    assert (
        report(envelope.model_copy(update={"predeclared_at_ns": None}), manifest).decision
        == "NO_GO"
    )
    late = envelope.model_copy(update={"predeclared_at_ns": contract().frozen_at_ns + 1})
    late_manifest = manifest.model_copy(update={"predeclared_plan_sha256": late.plan_identity})
    assert report(late, late_manifest).decision == "NO_GO"
    changed = envelope.model_copy(update={"cost_scenarios": (scenario(entry_fee_bps="40"),)})
    assert report(changed, manifest).decision == "NO_GO"


def test_exact_95_percent_each_symbol_is_not_shifted_or_rounded() -> None:
    rows = source().model_dump(mode="json")["symbols"]
    assert isinstance(rows, list)
    for row in rows:
        assert isinstance(row, dict)
        row.update(complete_labels=24624, rejected_labels=1296)
    envelope, manifest = qualified(source(symbols=rows))
    assert report(envelope, manifest).offline_research_ready
    assert isinstance(rows[1], dict)
    rows[1].update(complete_labels=24623, rejected_labels=1297)
    envelope, manifest = qualified(source(symbols=rows))
    assert report(envelope, manifest).decision == "NO_GO"


def test_embedded_fee_basis_requires_matching_execution_evidence_and_records() -> None:
    envelope, manifest = qualified(cost=scenario(basis="entry_fee_net_ask_to_bid"))
    assert report(envelope, manifest).decision == "NO_GO"
    envelope, manifest = qualified(
        cost=scenario(basis="entry_fee_net_ask_to_bid"),
        change_proof="execution_relationship",
        changes={"fee_embedding_records_sha256": "1" * 64},
    )
    assert report(envelope, manifest).offline_research_ready
    envelope, manifest = qualified(
        cost=scenario(basis="cash_net_ask_to_bid"),
        change_proof="execution_relationship",
        changes={
            "validated_return_basis": "raw_ask_to_bid",
            "fee_embedding_records_sha256": "1" * 64,
        },
    )
    assert report(envelope, manifest).decision == "NO_GO"


def test_maker_fill_evidence_not_passive_order_assumption() -> None:
    cost = scenario(entry_liquidity="maker_verified", entry_fee_bps="40")
    envelope, manifest = qualified(cost=cost)
    assert report(envelope, manifest).decision == "NO_GO"
    envelope, manifest = qualified(
        cost=cost,
        change_proof="execution_relationship",
        changes={"verified_entry_liquidity": "maker"},
    )
    assert report(envelope, manifest).offline_research_ready
    assert cost_hurdle(cost).passive_fill_guaranteed is False


@pytest.mark.parametrize(
    "field",
    [
        "checksum_failures",
        "clock_failures",
        "continuity_breaks",
        "raw_sequence_failures",
    ],
)
def test_integrity_failures_never_become_cost_readiness(field: str) -> None:
    envelope, manifest = qualified(source(**{field: 1}))
    assert report(envelope, manifest).decision == "NO_GO"


def test_account_evidence_expiry_and_counter_uncertainty_fail_closed() -> None:
    envelope, manifest = qualified(
        change_proof="account_eligibility",
        changes={"effective_until_ns": contract().end_ns},
    )
    assert report(envelope, manifest).decision == "NO_GO"
    for count in (None, 99999):
        envelope, manifest = qualified(source(final_quality_frames=count))
        assert report(envelope, manifest).decision == "NO_GO"


@pytest.mark.parametrize(
    ("entry", "exit_fee"), [("0", "0"), ("25", "25"), ("40", "80"), ("80", "80")]
)
def test_exact_multiplicative_hurdles_and_no_additive_fee_shortcut(
    entry: str, exit_fee: str
) -> None:
    value = scenario(entry_fee_bps=entry, exit_fee_bps=exit_fee, residual_nonembedded_bps="0")
    frontier = cost_hurdle(value)
    factor = (1 - Fraction(Decimal(entry)) / 10000) * (1 - Fraction(Decimal(exit_fee)) / 10000)
    expected = (1 / factor - 1) * 10000
    assert frontier.required_raw_ask_to_bid_return_bps is not None
    shown = Fraction(Decimal(frontier.required_raw_ask_to_bid_return_bps))
    assert shown >= expected and shown - expected < Fraction(1, 10**55)
    if entry != "0":
        assert shown > Decimal(entry) + Decimal(exit_fee)
    assert frontier.spread_charged_again is False
    assert frontier.passive_fill_guaranteed is False


def test_entry_inventory_and_cash_fee_net_bases_are_not_double_charged() -> None:
    raw = scenario(observed_basis_return_bps="0", residual_nonembedded_bps="0")
    entry_net = scenario(
        basis="entry_fee_net_ask_to_bid",
        observed_basis_return_bps="-80",
        residual_nonembedded_bps="0",
    )
    cash_net = scenario(
        basis="cash_net_ask_to_bid",
        observed_basis_return_bps="-159.36",
        residual_nonembedded_bps="0",
    )
    results = [cost_hurdle(row) for row in (raw, entry_net, cash_net)]
    for row in results:
        assert row.net_at_observed_basis_return_bps is not None
        assert Decimal(row.net_at_observed_basis_return_bps) == Decimal("-159.36")
    assert len({row.required_raw_ask_to_bid_return_bps for row in results}) == 1
    assert retained_base_quantity(Decimal("0.011"), Decimal(80)) == Decimal("0.010912")
    with pytest.raises(ValidationError):
        scenario(entry_charge="cash")
    with pytest.raises(ValidationError):
        scenario(fees_bps="160")  # Aggregate additive fees cannot silently replace both legs.


@pytest.mark.parametrize("charge", ["base_inventory", "quote_added"])
def test_entry_treatment_has_distinct_exact_hurdles_and_net_returns(charge: str) -> None:
    cost = scenario(
        entry_charge=charge,
        entry_fee_bps="1000",
        exit_fee_bps="2000",
        residual_nonembedded_bps="0",
        observed_basis_return_bps="1000",
    )
    result = cost_hurdle(cost)
    factor = Fraction(9, 10) * Fraction(4, 5) if charge == "base_inventory" else Fraction(8, 11)
    expected = (1 / factor - 1) * 10000
    assert result.entry_charge == charge and result.required_raw_ask_to_bid_return_bps is not None
    shown = Fraction(Decimal(result.required_raw_ask_to_bid_return_bps))
    assert shown >= expected and shown - expected < Fraction(1, 10**55)
    assert result.net_at_observed_basis_return_bps is not None
    assert Decimal(result.net_at_observed_basis_return_bps) == Decimal(
        "-2080" if charge == "base_inventory" else "-2000",
    )
    other = cost_hurdle(
        scenario(
            entry_charge="quote_added" if charge == "base_inventory" else "base_inventory",
            entry_fee_bps="1000",
            exit_fee_bps="2000",
            residual_nonembedded_bps="0",
        )
    )
    assert result.required_raw_ask_to_bid_return_bps != other.required_raw_ask_to_bid_return_bps
    assert not result.spread_charged_again
    assert long_quote_funded_cash_return_bps(
        Decimal("1.1"),
        Decimal(1000),
        Decimal(2000),
    ) == Decimal(-2000)
    assert long_cash_return_bps(
        Decimal("1.1"),
        Decimal(1000),
        Decimal(2000),
    ) == Decimal(-2080)


@pytest.mark.parametrize("charge", ["base_inventory", "quote_added"])
def test_raw_entry_net_cash_net_apply_each_declared_fee_once(charge: str) -> None:
    expected_net = "-2080" if charge == "base_inventory" else "-2000"
    rows = [
        scenario(
            entry_charge=charge,
            entry_fee_bps="1000",
            exit_fee_bps="2000",
            residual_nonembedded_bps="0",
            basis=basis,
            observed_basis_return_bps=observed,
        )
        for basis, observed in (
            ("raw_ask_to_bid", "1000"),
            ("entry_fee_net_ask_to_bid", "-100" if charge == "base_inventory" else "0"),
            ("cash_net_ask_to_bid", expected_net),
        )
    ]
    results = [cost_hurdle(row) for row in rows]
    assert all(
        result.net_at_observed_basis_return_bps is not None
        and Decimal(result.net_at_observed_basis_return_bps) == Decimal(expected_net)
        for result in results
    )
    assert len({result.required_raw_ask_to_bid_return_bps for result in results}) == 1
    assert all(result.observed_target_satisfied_exactly is False for result in results)


def test_quote_fee_target_uses_exact_cash_outlay_denominator_at_boundary() -> None:
    cost = scenario(
        entry_charge="quote_added",
        entry_fee_bps="1000",
        exit_fee_bps="2000",
        residual_nonembedded_bps="0",
        observed_basis_return_bps="3750",
    )
    result = cost_hurdle(cost)
    assert result.required_raw_ask_to_bid_return_bps == "3750"
    assert result.net_at_observed_basis_return_bps is not None
    assert Decimal(result.net_at_observed_basis_return_bps) == 0
    assert result.observed_target_satisfied_exactly is True
    below = scenario(
        entry_charge="quote_added",
        entry_fee_bps="1000",
        exit_fee_bps="2000",
        residual_nonembedded_bps="0",
        observed_basis_return_bps="3749.99999999999999999999999999",
    )
    assert cost_hurdle(below).observed_target_satisfied_exactly is False
    assert result.discovery_only and not result.profitability_claim


@pytest.mark.parametrize("fee", ["40", "80"])
@pytest.mark.parametrize("charge", ["base_inventory", "quote_added"])
def test_parent_verified_fee_only_frontiers_keep_distinct_capital_conventions(
    fee: str,
    charge: str,
) -> None:
    value = Fraction(Decimal(fee)) / 10000
    ratio = 1 / (1 - value) ** 2 if charge == "base_inventory" else (1 + value) / (1 - value)
    result = cost_hurdle(
        scenario(
            entry_charge=charge,
            entry_fee_bps=fee,
            exit_fee_bps=fee,
            residual_nonembedded_bps="0",
        )
    )
    assert result.required_raw_ask_to_bid_return_bps is not None
    displayed = Fraction(Decimal(result.required_raw_ask_to_bid_return_bps))
    expected = (ratio - 1) * 10000
    assert displayed >= expected and displayed - expected < Fraction(1, 10**55)
    assert result.net_return_basis == "all_in_entry_capital"


@pytest.mark.parametrize(
    "basis",
    [
        "raw_ask_to_bid",
        "entry_fee_net_ask_to_bid",
        "cash_net_ask_to_bid",
    ],
)
def test_trade_notional_friction_is_normalized_once_to_quote_added_capital(basis: str) -> None:
    cost = scenario(
        entry_charge="quote_added",
        entry_fee_bps="1000",
        exit_fee_bps="0",
        residual_nonembedded_bps="110",
        residual_basis="entry_trade_notional",
        basis=basis,
        observed_basis_return_bps="1000" if basis == "raw_ask_to_bid" else "0",
    )
    trade_notional = cost_hurdle(cost)
    all_in = cost_hurdle(
        scenario(
            entry_charge="quote_added",
            entry_fee_bps="1000",
            exit_fee_bps="0",
            residual_nonembedded_bps="100",
            residual_basis="all_in_entry_capital",
            basis=basis,
            observed_basis_return_bps="1000" if basis == "raw_ask_to_bid" else "0",
        )
    )
    assert (
        trade_notional.required_raw_ask_to_bid_return_bps
        == all_in.required_raw_ask_to_bid_return_bps
        == "1110"
    )
    assert trade_notional.net_at_observed_basis_return_bps is not None
    assert Decimal(trade_notional.net_at_observed_basis_return_bps) == Decimal("-100")
    assert (
        trade_notional.net_at_observed_basis_return_bps == all_in.net_at_observed_basis_return_bps
    )
    wrong_denominator = cost_hurdle(
        scenario(
            entry_charge="quote_added",
            entry_fee_bps="1000",
            exit_fee_bps="0",
            residual_nonembedded_bps="110",
            residual_basis="all_in_entry_capital",
        )
    )
    assert wrong_denominator.required_raw_ask_to_bid_return_bps == "1121"


@pytest.mark.parametrize("basis", [None, "unknown"])
def test_unknown_friction_units_block_numeric_frontier_and_account_readiness(basis: object) -> None:
    cost = scenario(residual_basis=basis)
    result = cost_hurdle(cost)
    assert result.status == "unknown_nonembedded_friction"
    assert result.required_basis_return_bps is None
    assert result.net_at_observed_basis_return_bps is None
    assert result.observed_target_satisfied_exactly is None
    assert result.fees_only_raw_floor_bps is not None
    envelope, manifest = qualified(cost=cost)
    assessment = report(envelope, manifest)
    assert assessment.decision == "NO_GO"
    assert "NONEMBEDDED_FRICTION_UNPROVEN" in assessment.sources[0].reasons


def test_base_withheld_cash_outlay_has_no_quote_fee_capital_addition_for_friction() -> None:
    results = [
        cost_hurdle(scenario(entry_charge="base_inventory", residual_basis=basis))
        for basis in ("all_in_entry_capital", "entry_trade_notional")
    ]
    assert (
        results[0].required_raw_ask_to_bid_return_bps
        == results[1].required_raw_ask_to_bid_return_bps
    )
    assert (
        results[0].net_at_observed_basis_return_bps == results[1].net_at_observed_basis_return_bps
    )


@pytest.mark.parametrize("charge", [None, "unknown"])
def test_unknown_entry_currency_has_no_frontier_or_readiness(charge: object) -> None:
    cost = scenario(entry_charge=charge)
    result = cost_hurdle(cost)
    assert result.status == "unknown_entry_fee_treatment"
    assert result.required_basis_return_bps is None
    assert result.required_raw_ask_to_bid_return_bps is None
    assert result.fees_only_raw_floor_bps is None
    assert result.net_at_observed_basis_return_bps is None
    assert result.observed_target_satisfied_exactly is None
    envelope, manifest = qualified(cost=cost)
    assessment = report(envelope, manifest)
    assert assessment.decision == "NO_GO"
    assert "ENTRY_FEE_TREATMENT_UNPROVEN" in assessment.sources[0].reasons


@pytest.mark.parametrize("embedded", [None, "unknown", "base_inventory"])
def test_fee_net_currency_must_match_approved_embedding_records(embedded: object) -> None:
    envelope, manifest = qualified(
        cost=scenario(entry_charge="quote_added", basis="entry_fee_net_ask_to_bid"),
        change_proof="execution_relationship",
        changes={
            "fee_embedding_records_sha256": "1" * 64,
            "embedded_entry_charge": embedded,
        },
    )
    assessment = report(envelope, manifest)
    assert assessment.decision == "NO_GO"
    assert (
        "DECLARED_RETURN_BASIS_OR_EMBEDDED_FEE_PROOF_UNSUPPORTED" in assessment.sources[0].reasons
    )
    envelope, manifest = qualified(
        cost=scenario(entry_charge="quote_added", basis="entry_fee_net_ask_to_bid"),
        change_proof="execution_relationship",
        changes={"fee_embedding_records_sha256": "1" * 64},
    )
    assert report(envelope, manifest).decision == "QUALIFIED_FIXTURE_ONLY"


def test_explicit_quote_fee_fixture_does_not_approve_public_tiers_or_unproved_currency() -> None:
    cost = scenario(entry_charge="quote_added")
    envelope, manifest = qualified(cost=cost)
    assert report(envelope, manifest).decision == "QUALIFIED_FIXTURE_ONLY"
    cases: tuple[dict[str, object], ...] = (
        {"scope": "public_tier"},
        {"entry_charge_evidence_sha256": None},
        {"taker_bps": None},
    )
    for changes in cases:
        envelope, manifest = qualified(cost=cost, change_proof="fee_schedule", changes=changes)
        assert report(envelope, manifest).decision == "NO_GO"


def test_omitted_treatment_proof_is_not_legacy_account_approval() -> None:
    envelope, manifest = qualified()
    fee = next(row for row in envelope.evidence if isinstance(row, FeeSchedule))
    old = fee.model_dump(mode="json")
    del old["entry_charge_evidence_sha256"]
    del old["entry_charge_symbols"]
    parsed = FeeSchedule.model_validate_json(json.dumps(old))
    assert parsed.entry_charge_evidence_sha256 is None and parsed.entry_charge_symbols is None
    envelope, manifest = qualified(
        change_proof="fee_schedule",
        changes={"entry_charge_evidence_sha256": None, "entry_charge_symbols": None},
    )
    assert report(envelope, manifest).decision == "NO_GO"


@pytest.mark.parametrize("exit_charge", [None, "unknown", "base_inventory"])
def test_unknown_or_base_fee_sell_is_not_silently_quote_cash(exit_charge: object) -> None:
    cost = scenario(exit_charge=exit_charge, exit_preference="base")
    frontier = cost_hurdle(cost)
    assert frontier.exit_charge == exit_charge and frontier.exit_preference == "base"
    assert frontier.status == (
        "unsupported_exit_base_fee_treatment"
        if exit_charge == "base_inventory"
        else "unknown_exit_fee_treatment"
    )
    assert frontier.required_raw_ask_to_bid_return_bps is None
    assert frontier.required_basis_return_bps is None
    assert frontier.fees_only_raw_floor_bps is None
    assert frontier.net_at_observed_basis_return_bps is None
    assert frontier.observed_target_satisfied_exactly is None
    envelope, manifest = qualified(cost=cost)
    assert report(envelope, manifest).decision == "NO_GO"


def test_preferences_do_not_establish_confirmed_fee_currency() -> None:
    cost = scenario(entry_preference="quote", exit_preference="base")
    envelope, manifest = qualified(
        cost=cost,
        change_proof="fee_schedule",
        changes={
            "entry_preference": "quote",
            "exit_preference": "base",
            "entry_charge_evidence_kind": "preference_only",
            "exit_charge_evidence_kind": "preference_only",
        },
    )
    result = report(envelope, manifest)
    assert result.decision == "NO_GO"
    assert "ENTRY_FEE_TREATMENT_UNPROVEN" in result.sources[0].reasons
    assert "EXIT_FEE_TREATMENT_UNPROVEN" in result.sources[0].reasons
    envelope, manifest = qualified(
        cost=cost,
        change_proof="fee_schedule",
        changes={"entry_preference": "quote", "exit_preference": "base"},
    )
    # Preferences are not guarantees; approved actual settlement records control.
    assert report(envelope, manifest).decision == "QUALIFIED_FIXTURE_ONLY"


@pytest.mark.parametrize("embedded_exit", [None, "unknown", "base_inventory"])
def test_cash_net_sell_leg_requires_matching_confirmed_embedding(
    embedded_exit: object,
) -> None:
    envelope, manifest = qualified(
        cost=scenario(basis="cash_net_ask_to_bid"),
        change_proof="execution_relationship",
        changes={
            "fee_embedding_records_sha256": "1" * 64,
            "embedded_exit_charge": embedded_exit,
        },
    )
    result = report(envelope, manifest)
    assert result.decision == "NO_GO"
    assert "DECLARED_RETURN_BASIS_OR_EMBEDDED_FEE_PROOF_UNSUPPORTED" in result.sources[0].reasons


def test_unknown_fees_and_friction_have_no_success_shaped_zero_defaults() -> None:
    for field in ("entry_fee_bps", "exit_fee_bps"):
        diagnostic = cost_hurdle(scenario(**{field: None}))
        assert diagnostic.status == "unknown_fees"
        assert diagnostic.required_basis_return_bps is None
    diagnostic = cost_hurdle(scenario(residual_nonembedded_bps=None))
    assert diagnostic.status == "unknown_nonembedded_friction"
    assert diagnostic.required_raw_ask_to_bid_return_bps is None
    assert diagnostic.fees_only_raw_floor_bps is not None


@pytest.mark.parametrize("charge", ["base_inventory", "quote_added"])
def test_hurdle_target_residual_and_exact_comparison_do_not_depend_on_decimal_context(
    charge: str,
) -> None:
    cost = scenario(
        entry_charge=charge,
        entry_fee_bps="0",
        exit_fee_bps="0",
        target_net_bps="2",
        residual_nonembedded_bps="3",
        observed_basis_return_bps="5",
    )
    baseline = cost_hurdle(cost)
    assert baseline.required_basis_return_bps == "5"
    assert baseline.observed_target_satisfied_exactly
    assert (
        cost_hurdle(
            scenario(
                entry_charge=charge,
                entry_fee_bps="0",
                exit_fee_bps="0",
                target_net_bps="2",
                residual_nonembedded_bps="3",
                observed_basis_return_bps="4.99999999999999999999999999",
            )
        ).observed_target_satisfied_exactly
        is False
    )
    with localcontext() as context:
        context.prec = 6
        context.rounding = ROUND_DOWN
        context.traps[Inexact] = True
        assert cost_hurdle(cost) == baseline
        recurring = scenario(entry_charge=charge)
        assert cost_hurdle(recurring).required_raw_ask_to_bid_return_bps is not None


def test_shared_math_extraction_preserves_existing_research_operation_order() -> None:
    for ratio in (Decimal(1), Decimal("1.0025"), Decimal("0.99999999999999999999")):
        for entry, exit_fee in ((Decimal(25), Decimal(25)), (Decimal(40), Decimal(80))):
            assert (
                long_cash_return_bps(ratio, entry, exit_fee)
                == (ratio * (1 - entry / 10000) * (1 - exit_fee / 10000) - 1) * 10000
            )
            quantity = Decimal("10.25") / Decimal("9999.123456")
            assert retained_base_quantity(quantity, entry) == quantity * (1 - entry / 10000)


@pytest.mark.parametrize("bad", [True, 80, 80.0, "NaN", "Infinity", "1e10000000"])
def test_fee_units_reject_coercion_and_unbounded_scalars(bad: object) -> None:
    with pytest.raises(ValidationError):
        scenario(entry_fee_bps=bad)


def test_small_decimal_scalars_round_trip_without_exponent_coercion() -> None:
    tiny = scenario(entry_fee_bps="0.000000000000000000000000000001")
    assert cost_hurdle(tiny).status == "hypothetical_full_cost_frontier"
    assert "E-" not in tiny.model_dump_json()


def test_real_committed_failed_closeout_is_no_go_with_full_missingness() -> None:
    base = Path(__file__).parents[1] / "docs" / "experiments" / "2026-10-book-confirmation-72h-v2"
    result = closeout_example(
        (base / "frozen-protocol.json").read_bytes(),
        (base / "final-closeout-20261007" / "closeout-verdict.json").read_bytes(),
    )
    assert result.decision == "NO_GO" and not result.offline_research_ready
    assert not result.model_ready and not result.trading_allowed
    btc, eth = result.sources[0].coverage
    assert btc.complete_labels == 4616 and eth.complete_labels == 4689
    assert btc.matured_at_assessment == eth.matured_at_assessment == 25920
    assert btc.unwritten_mature_labels == eth.unwritten_mature_labels == 20526
    assert Decimal(btc.full_mature_primary_percent) < 18
    assert Decimal(eth.full_mature_primary_percent) < 19
    assert all(row.discovery_only for row in result.cost_frontiers)
    assert all(row.required_basis_return_bps is None for row in result.cost_frontiers)
    assert tuple(row.entry_charge for row in result.cost_frontiers) == (
        "quote_added",
        "quote_added",
        "base_inventory",
    )
    assert tuple(row.exit_charge for row in result.cost_frontiers) == ("unknown", "unknown", "cash")
    for row in result.cost_frontiers[:2]:
        assert row.entry_preference == "quote" and row.exit_preference == "base"
        assert row.status == "unknown_exit_fee_treatment"
        assert row.fees_only_raw_floor_bps is None


def test_deterministic_json_and_input_output_budgets() -> None:
    envelope, manifest = qualified()
    first = report(envelope, manifest)
    assert first.model_dump_json() == report(envelope, manifest).model_dump_json()
    assert len(first.model_dump_json().encode()) < readiness.OUTPUT_LIMIT
    with pytest.raises(ValueError, match="byte budget"):
        bounded_json(b" " * (readiness.INPUT_LIMIT + 1))
    with pytest.raises(ValueError, match="duplicate"):
        bounded_json(b'{"a":1,"a":2}')
    with pytest.raises(ValueError, match="nonfinite"):
        bounded_json(b'{"a":NaN}')


def test_read_only_cli_closeout_and_schema(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    base = Path(__file__).parents[1] / "docs" / "experiments" / "2026-10-book-confirmation-72h-v2"
    monkeypatch.setattr(
        "sys.argv",
        [
            "readiness",
            "closeout",
            str(base / "final-closeout-20261007" / "closeout-verdict.json"),
            "--protocol",
            str(base / "frozen-protocol.json"),
        ],
    )
    readiness.main()
    result = json.loads(capsys.readouterr().out)
    assert result["decision"] == "NO_GO" and result["trading_allowed"] is False
    monkeypatch.setattr("sys.argv", ["readiness", "schema"])
    readiness.main()
    schema = json.loads(capsys.readouterr().out)
    assert "input" in schema and "trust_manifest" in schema and "output" in schema
