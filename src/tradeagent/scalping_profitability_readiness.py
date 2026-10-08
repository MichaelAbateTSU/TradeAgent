"""Offline evidence readiness and cost-frontier diagnostics, never model promotion.

Read-only CLI:
  python -m tradeagent.scalping_profitability_readiness schema
  python -m tradeagent.scalping_profitability_readiness evaluate INPUT.json \
      --trust-manifest APPROVED.json --trust-sha256 EXTERNALLY_APPROVED_HASH
  python -m tradeagent.scalping_profitability_readiness closeout VERDICT.json \
      --protocol FROZEN_PROTOCOL.json

Trust is an explicit caller boundary: the separately approved manifest's hash and
authority bindings are required. Content hashes prove identity, not truth of a
provider, jurisdiction or fill-validation claim. Never approve a manifest merely
because this module can hash it. Fixture approval is labeled separately.
The trust hash is ``TrustManifest.identity`` (canonical validated JSON), not the
pretty-printed file's byte hash. Its plan pin must equal ``envelope.plan_identity``;
the plan fixes sources/protocols/cost assumptions before the source freeze, not
future observed returns. Missing retrospective plan proof stays NO_GO.

Every source must independently satisfy the unchanged two-symbol full frozen
grid/95% gate, closed integrity, same-venue execution validation, account
eligibility, actual account fee schedule and additional friction evidence.
Public/configured rates can produce discovery-only hurdles, never cost readiness.
Even qualified evidence allows only considering offline research; model_ready,
trading_allowed and profitability_claim remain false. No fitting, optimization,
broker client, persistence, artifact replacement, or actual fee defaults.
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import UTC, datetime
from decimal import ROUND_CEILING, ROUND_HALF_EVEN, Context, Decimal, localcontext
from fractions import Fraction
from hashlib import sha256
from pathlib import Path
from typing import Annotated, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    TypeAdapter,
    field_serializer,
    field_validator,
    model_validator,
)

from tradeagent.scalping_cost_math import long_cash_return_bps

NS = 1_000_000_000
SLOTS: Literal[25920] = 25920
INPUT_LIMIT = 1024 * 1024
OUTPUT_LIMIT = 256 * 1024
SYMBOLS = ("BTC/USD", "ETH/USD")
Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
Name = Annotated[str, Field(min_length=1, max_length=128, pattern=r"^\S(?:.*\S)?$")]
Clock = Annotated[int, Field(ge=0, le=2**63 - 1)]
Count = Annotated[int, Field(ge=0, le=SLOTS)]
Venue = Literal["kraken", "alpaca_paper"]
Symbol = Literal["BTC/USD", "ETH/USD"]
ReturnBasis = Literal["raw_ask_to_bid", "entry_fee_net_ask_to_bid", "cash_net_ask_to_bid"]
Role = Literal[
    "source_auditor", "execution_auditor", "account_provider", "fee_provider", "friction_auditor"
]
JSON_OBJECT = TypeAdapter(dict[str, JsonValue])


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


class Frozen(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        strict=True,
        extra="forbid",
        allow_inf_nan=False,
        revalidate_instances="always",
    )

    @property
    def identity(self) -> str:
        return sha256(canonical(self.model_dump(mode="json"))).hexdigest()

    @field_serializer("*", when_used="json")
    def ordinary_decimals(self, value: object) -> object:
        return format(value, "f") if isinstance(value, Decimal) else value


def decimal_scalar(value: object) -> Decimal:
    if isinstance(value, str):
        if len(value) > 48 or re.fullmatch(r"-?[0-9]+(?:\.[0-9]+)?", value) is None:
            raise ValueError("financial scalars require bounded ordinary decimal strings")
        value = Decimal(value)
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError("financial scalars must be Decimal/string, never float/bool/integer")
    exponent = value.as_tuple().exponent
    if not isinstance(exponent, int) or len(value.as_tuple().digits) > 40 or abs(exponent) > 40:
        raise ValueError("financial scalar precision budget exceeded")
    return value


class FrozenContract(Frozen):
    protocol_sha256: Digest
    frozen_at_ns: Clock
    start_ns: Clock
    end_ns: Clock
    symbols: tuple[Literal["BTC/USD"], Literal["ETH/USD"]]
    slots_per_symbol: Literal[25920]
    cadence_seconds: Literal[10]
    horizon_seconds: Literal[60]
    settlement_seconds: Literal[2]
    provider_freshness_seconds: Literal[2]
    receipt_freshness_seconds: Literal[2]
    best_side_notional_usd: Literal["10.25"]

    @model_validator(mode="after")
    def fixed(self) -> Self:
        if (
            self.end_ns - self.start_ns != 72 * 3600 * NS
            or self.frozen_at_ns >= self.start_ns
            or self.start_ns % (10 * NS)
        ):
            raise ValueError("one frozen, aligned 72h contract is required")
        return self

    def matured(self, at_ns: int) -> int:
        return min(SLOTS, max(0, (at_ns - self.start_ns - 62 * NS) // (10 * NS) + 1))


class SymbolCounts(Frozen):
    symbol: Symbol
    matured_slots: Count
    complete_labels: Count
    rejected_labels: Count
    unwritten_mature_labels: Count
    observed_evaluations: Count

    @model_validator(mode="after")
    def partition(self) -> Self:
        if (
            self.complete_labels + self.rejected_labels + self.unwritten_mature_labels
            != self.matured_slots
            or self.complete_labels + self.rejected_labels > self.observed_evaluations
        ):
            raise ValueError("mature denominator includes every rejected/unwritten outcome")
        return self


class SourceQuality(Frozen):
    kind: Literal["source_quality"]
    authority_id: Name
    source_id: Name
    source_venue: Venue
    source_kind: Literal["native_checksum_book", "native_quote", "paper_quote"]
    contract: FrozenContract
    report_as_of_ns: Clock
    state: Literal["completed", "failed_incomplete", "collecting"]
    symbols: tuple[SymbolCounts, SymbolCounts]
    raw_frames_verified: Annotated[int, Field(ge=0, le=100_000_000)] | None
    final_quality_frames: Annotated[int, Field(ge=0, le=100_000_000)] | None
    checksum_failures: Annotated[int, Field(ge=0, le=100_000_000)]
    clock_failures: Annotated[int, Field(ge=0, le=100_000_000)]
    continuity_breaks: Annotated[int, Field(ge=0, le=100_000_000)]
    raw_sequence_failures: Annotated[int, Field(ge=0, le=100_000_000)]
    journal_root: Digest
    original_artifact_sha256: Digest

    @model_validator(mode="after")
    def universe_and_maturity(self) -> Self:
        if tuple(row.symbol for row in self.symbols) != SYMBOLS:
            raise ValueError("both ordered symbols are required independently")
        if any(
            row.matured_slots != self.contract.matured(self.report_as_of_ns) for row in self.symbols
        ):
            raise ValueError("reported maturity denominator differs from the frozen clock")
        return self


class ExecutionRelationship(Frozen):
    kind: Literal["execution_relationship"]
    authority_id: Name
    source_quality_sha256: Digest
    source_venue: Venue
    execution_venue: Venue
    account_digest: Digest
    symbols: tuple[Literal["BTC/USD"], Literal["ETH/USD"]]
    methodology: Literal["independent_same_venue_quote_fill_validation"]
    quote_fill_pairs: Annotated[int, Field(ge=0, le=100_000_000)]
    venue_identifier_mismatches: Annotated[int, Field(ge=0, le=100_000_000)]
    causal_violations: Annotated[int, Field(ge=0, le=100_000_000)]
    quantity_or_price_discrepancies: Annotated[int, Field(ge=0, le=100_000_000)]
    supporting_records_sha256: Digest
    validated_return_basis: ReturnBasis
    fee_embedding_records_sha256: Digest | None
    verified_entry_liquidity: Literal["maker", "taker", "unconfirmed"]
    verified_at_ns: Clock


class AccountEligibility(Frozen):
    kind: Literal["account_eligibility"]
    authority_id: Name
    venue: Venue
    account_digest: Digest
    eligibility: Literal["eligible", "ineligible", "unknown"]
    methodology: Literal["account_specific_provider_and_jurisdiction_evidence"]
    effective_from_ns: Clock
    effective_until_ns: Clock


class FeeSchedule(Frozen):
    kind: Literal["fee_schedule"]
    authority_id: Name
    venue: Venue
    account_digest: Digest | None
    scope: Literal["account_specific", "public_tier", "configured_model", "unknown"]
    maker_bps: Annotated[Decimal, Field(ge=0, lt=10000)] | None
    taker_bps: Annotated[Decimal, Field(ge=0, lt=10000)] | None
    entry_charge: Literal["base_inventory"]
    exit_charge: Literal["cash"]
    effective_from_ns: Clock
    effective_until_ns: Clock

    @field_validator("maker_bps", "taker_bps", mode="before")
    @classmethod
    def scalar(cls, value: object) -> Decimal | None:
        return None if value is None else decimal_scalar(value)


class AdditionalFriction(Frozen):
    kind: Literal["additional_friction"]
    authority_id: Name
    execution_venue: Venue
    account_digest: Digest
    source_quality_sha256: Digest
    basis: ReturnBasis
    residual_bps: Annotated[Decimal, Field(ge=0, le=10000)]
    covers: tuple[Literal["slippage"], Literal["impact"], Literal["adverse_selection"]]
    methodology: Literal["independently_validated_nonembedded_allowance"]
    supporting_records_sha256: Digest

    @field_validator("residual_bps", mode="before")
    @classmethod
    def scalar(cls, value: object) -> Decimal:
        return decimal_scalar(value)


Evidence = Annotated[
    SourceQuality | ExecutionRelationship | AccountEligibility | FeeSchedule | AdditionalFriction,
    Field(discriminator="kind"),
]


class ApprovedEvidence(Frozen):
    artifact_sha256: Digest
    authority_id: Name
    role: Role


class TrustManifest(Frozen):
    schema_version: Literal["profitability-evidence-approval-v1"]
    mode: Literal["independently_approved", "hypothetical_fixture"]
    approved_at_ns: Clock
    predeclared_plan_sha256: Digest | None
    entries: Annotated[tuple[ApprovedEvidence, ...], Field(max_length=64)]

    @model_validator(mode="after")
    def unique(self) -> Self:
        if len({entry.artifact_sha256 for entry in self.entries}) != len(self.entries):
            raise ValueError("duplicate approved artifact")
        return self


class CostScenario(Frozen):
    scenario_id: Name
    venue: Venue
    provenance: Literal["public_tier_scenario", "configured_model_scenario", "account_evidence"]
    basis: ReturnBasis
    entry_liquidity: Literal["maker_verified", "taker", "unconfirmed_worst_rate"]
    entry_fee_bps: Annotated[Decimal, Field(ge=0, lt=10000)] | None
    exit_fee_bps: Annotated[Decimal, Field(ge=0, lt=10000)] | None
    entry_charge: Literal["base_inventory"]
    exit_charge: Literal["cash"]
    residual_nonembedded_bps: Annotated[Decimal, Field(ge=0, le=10000)] | None
    target_net_bps: Annotated[Decimal, Field(ge=0, le=10000)]
    observed_basis_return_bps: Annotated[Decimal, Field(gt=-10000, le=100000)] | None

    @field_validator(
        "entry_fee_bps",
        "exit_fee_bps",
        "residual_nonembedded_bps",
        "target_net_bps",
        "observed_basis_return_bps",
        mode="before",
    )
    @classmethod
    def scalar(cls, value: object) -> Decimal | None:
        return None if value is None else decimal_scalar(value)


class SourceBinding(Frozen):
    source_id: Name
    source_quality_sha256: Digest
    cost_scenario_id: Name
    execution_relationship_sha256: Digest | None
    account_eligibility_sha256: Digest | None
    fee_schedule_sha256: Digest | None
    additional_friction_sha256: Digest | None


class ReadinessEnvelope(Frozen):
    schema_version: Literal["profitability-readiness-input-v1"]
    evidence_mode: Literal["independently_approved", "hypothetical_fixture"]
    assessed_at_ns: Clock
    predeclared_at_ns: Clock | None
    execution_venue: Venue
    account_digest: Digest | None
    sources: Annotated[tuple[SourceBinding, ...], Field(min_length=1, max_length=8)]
    evidence: Annotated[tuple[Evidence, ...], Field(min_length=1, max_length=40)]
    cost_scenarios: Annotated[tuple[CostScenario, ...], Field(min_length=1, max_length=8)]

    @model_validator(mode="after")
    def distinct(self) -> Self:
        for names in (
            [row.source_id for row in self.sources],
            [row.identity for row in self.evidence],
            [row.scenario_id for row in self.cost_scenarios],
        ):
            if len(set(names)) != len(names):
                raise ValueError("duplicate source/evidence/scenario")
        return self

    @property
    def plan_identity(self) -> str:
        qualities = {row.identity: row for row in self.evidence if isinstance(row, SourceQuality)}
        plan = {
            "execution_venue": self.execution_venue,
            "account_digest": self.account_digest,
            "predeclared_at_ns": self.predeclared_at_ns,
            "sources": [
                {
                    "source_id": row.source_id,
                    "protocol_sha256": (
                        qualities[row.source_quality_sha256].contract.protocol_sha256
                        if row.source_quality_sha256 in qualities
                        else None
                    ),
                    "cost_scenario_id": row.cost_scenario_id,
                }
                for row in self.sources
            ],
            "cost_scenarios": [
                row.model_dump(mode="json", exclude={"observed_basis_return_bps"})
                for row in self.cost_scenarios
            ],
        }
        return sha256(canonical(plan)).hexdigest()


class HurdleDiagnostic(Frozen):
    scenario_id: Name
    discovery_only: Literal[True] = True
    profitability_claim: Literal[False] = False
    status: Literal[
        "hypothetical_full_cost_frontier", "unknown_nonembedded_friction", "unknown_fees"
    ]
    required_basis_return_bps: str | None
    required_raw_ask_to_bid_return_bps: str | None
    fees_only_raw_floor_bps: str | None
    net_at_observed_basis_return_bps: str | None
    observed_target_satisfied_exactly: bool | None
    spread_charged_again: Literal[False] = False
    passive_fill_guaranteed: Literal[False] = False


def _rounded_fraction(value: Fraction, *, upward: bool = False) -> str:
    with localcontext(
        Context(
            prec=64,
            rounding=ROUND_CEILING if upward else ROUND_HALF_EVEN,
        )
    ):
        return str(Decimal(value.numerator) / Decimal(value.denominator))


def cost_hurdle(scenario: CostScenario) -> HurdleDiagnostic:
    scenario = CostScenario.model_validate_json(scenario.model_dump_json())
    if scenario.entry_fee_bps is None or scenario.exit_fee_bps is None:
        return HurdleDiagnostic(
            scenario_id=scenario.scenario_id,
            status="unknown_fees",
            required_basis_return_bps=None,
            required_raw_ask_to_bid_return_bps=None,
            fees_only_raw_floor_bps=None,
            net_at_observed_basis_return_bps=None,
            observed_target_satisfied_exactly=None,
        )
    entry = Fraction(scenario.entry_fee_bps) / 10000
    exit_fee = Fraction(scenario.exit_fee_bps) / 10000
    raw_factor = (1 - entry) * (1 - exit_fee)
    fee_floor = (1 / raw_factor - 1) * 10000
    basis_factor = (
        raw_factor
        if scenario.basis == "raw_ask_to_bid"
        else 1 - exit_fee
        if scenario.basis == "entry_fee_net_ask_to_bid"
        else Fraction(1)
    )
    residual = scenario.residual_nonembedded_bps
    if residual is None:
        return HurdleDiagnostic(
            scenario_id=scenario.scenario_id,
            status="unknown_nonembedded_friction",
            required_basis_return_bps=None,
            required_raw_ask_to_bid_return_bps=None,
            fees_only_raw_floor_bps=_rounded_fraction(fee_floor, upward=True),
            net_at_observed_basis_return_bps=None,
            observed_target_satisfied_exactly=None,
        )
    target_plus_cost = Fraction(scenario.target_net_bps) + Fraction(residual)
    required = 1 + target_plus_cost / 10000
    observed = scenario.observed_basis_return_bps
    exact_net = (
        ((1 + Fraction(observed) / 10000) * basis_factor - 1) * 10000 - Fraction(residual)
        if observed is not None
        else None
    )
    # Reuse the research arithmetic while retaining exact rational comparison at
    # a hurdle: a rounded display must never turn a below-target input into a pass.
    net_display: str | None = None
    if observed is not None:
        with localcontext(Context(prec=64, rounding=ROUND_HALF_EVEN)):
            applied_entry = (
                scenario.entry_fee_bps if scenario.basis == "raw_ask_to_bid" else Decimal(0)
            )
            applied_exit = (
                scenario.exit_fee_bps if scenario.basis != "cash_net_ask_to_bid" else Decimal(0)
            )
            net_display = str(
                long_cash_return_bps(
                    1 + observed / 10000,
                    applied_entry,
                    applied_exit,
                )
                - residual
            )
    return HurdleDiagnostic(
        scenario_id=scenario.scenario_id,
        status="hypothetical_full_cost_frontier",
        required_basis_return_bps=_rounded_fraction(
            (required / basis_factor - 1) * 10000, upward=True
        ),
        required_raw_ask_to_bid_return_bps=_rounded_fraction(
            (required / raw_factor - 1) * 10000, upward=True
        ),
        fees_only_raw_floor_bps=_rounded_fraction(fee_floor, upward=True),
        net_at_observed_basis_return_bps=net_display,
        observed_target_satisfied_exactly=(
            exact_net >= Fraction(scenario.target_net_bps) if exact_net is not None else None
        ),
    )


class Coverage(Frozen):
    symbol: Symbol
    frozen_scheduled_slots: Literal[25920] = SLOTS
    matured_at_assessment: Count
    reported_matured_slots: Count
    complete_labels: Count
    rejected_labels: Count
    unwritten_mature_labels: Count
    full_mature_primary_percent: str
    observed_written_slice_percent_not_primary: str | None


class SourceDecision(Frozen):
    source_id: Name
    offline_research_prerequisites_met: bool
    reasons: tuple[str, ...]
    coverage: tuple[Coverage, ...]
    evidence_hashes_checked: tuple[str, ...]


class ReadinessReport(Frozen):
    schema_version: Literal["profitability-readiness-report-v1"] = (
        "profitability-readiness-report-v1"
    )
    decision: Literal["NO_GO", "OFFLINE_RESEARCH_PREREQUISITES_MET", "QUALIFIED_FIXTURE_ONLY"]
    input_sha256: Digest
    trust_manifest_sha256: Digest
    trust_root_matched: bool
    offline_research_ready: bool
    model_ready: Literal[False] = False
    trading_allowed: Literal[False] = False
    profitability_claim: Literal[False] = False
    sources: tuple[SourceDecision, ...]
    cost_frontiers: tuple[HurdleDiagnostic, ...]
    limitations: tuple[str, ...] = (
        "Hashes/approved manifests establish identity and declared trust, not external truth.",
        "This gate is not training, economic validation, executable orders or promotion.",
        "Public/configured fees do not establish account-specific rates or eligibility.",
        "Ask-to-bid returns embed spread; passive orders do not establish maker fills.",
    )


def evaluate_readiness(
    envelope: ReadinessEnvelope,
    manifest: TrustManifest,
    *,
    trusted_manifest_sha256: str,
) -> ReadinessReport:
    envelope = ReadinessEnvelope.model_validate_json(envelope.model_dump_json())
    manifest = TrustManifest.model_validate_json(manifest.model_dump_json())
    if len(envelope.model_dump_json().encode()) > INPUT_LIMIT:
        raise ValueError("readiness input budget exceeded")
    root_matched = (
        manifest.identity == trusted_manifest_sha256
        and manifest.mode == envelope.evidence_mode
        and manifest.approved_at_ns <= envelope.assessed_at_ns
    )
    artifacts = {row.identity: row for row in envelope.evidence}
    approvals = {row.artifact_sha256: row for row in manifest.entries}
    scenarios = {row.scenario_id: row for row in envelope.cost_scenarios}
    decisions: list[SourceDecision] = []
    for binding in envelope.sources:
        reasons: list[str] = []
        checked: list[str] = []
        if (
            envelope.predeclared_at_ns is None
            or manifest.predeclared_plan_sha256 != envelope.plan_identity
        ):
            reasons.append("PREDECLARED_SOURCE_COST_PLAN_UNPROVEN")

        def proof(
            digest: str | None,
            role: Role,
            *,
            failures: list[str] = reasons,
            hashes: list[str] = checked,
        ) -> Evidence | None:
            item = artifacts.get(digest) if digest is not None else None
            approved = approvals.get(digest) if digest is not None else None
            if (
                not root_matched
                or item is None
                or approved is None
                or approved.role != role
                or approved.authority_id != item.authority_id
            ):
                failures.append("UNAPPROVED_OR_MISSING_" + role.upper())
                return None
            hashes.append(item.identity)
            return item

        source_item = artifacts.get(binding.source_quality_sha256)
        approved_source = proof(binding.source_quality_sha256, "source_auditor")
        coverage: list[Coverage] = []
        if not isinstance(source_item, SourceQuality) or source_item.source_id != binding.source_id:
            reasons.append("SOURCE_REPORT_OR_BINDING_MISMATCH")
        else:
            if (
                envelope.predeclared_at_ns is not None
                and envelope.predeclared_at_ns > source_item.contract.frozen_at_ns
            ):
                reasons.append("SOURCE_COST_PLAN_NOT_PROSPECTIVE")
            if not isinstance(approved_source, SourceQuality):
                reasons.append("SOURCE_PROVENANCE_NOT_APPROVED")
            matured = source_item.contract.matured(envelope.assessed_at_ns)
            if source_item.report_as_of_ns > envelope.assessed_at_ns:
                reasons.append("FUTURE_SOURCE_REPORT")
            if source_item.state != "completed":
                reasons.append("SOURCE_NOT_COMPLETED_NO_RESTART")
            if source_item.source_venue != envelope.execution_venue:
                reasons.append("SOURCE_EXECUTION_VENUE_MISMATCH")
            if (
                not source_item.raw_frames_verified
                or source_item.final_quality_frames != source_item.raw_frames_verified
                or any(
                    (
                        source_item.checksum_failures,
                        source_item.clock_failures,
                        source_item.continuity_breaks,
                        source_item.raw_sequence_failures,
                    )
                )
            ):
                reasons.append("SOURCE_INTEGRITY_OR_CLOSED_COUNTERS_UNPROVEN")
            if matured != SLOTS:
                reasons.append("FULL_FROZEN_WINDOW_NOT_MATURE")
            for row in source_item.symbols:
                if row.matured_slots != matured:
                    reasons.append(row.symbol + ":STALE_OR_PARTIAL_MATURITY_DENOMINATOR")
                if row.unwritten_mature_labels or row.observed_evaluations != SLOTS:
                    reasons.append(row.symbol + ":INCOMPLETE_SCHEDULE_OR_LABELS")
                if not matured or row.complete_labels * 100 < matured * 95:
                    reasons.append(row.symbol + ":FULL_MATURE_QUALITY_BELOW_95")
                written = row.complete_labels + row.rejected_labels
                coverage.append(
                    Coverage(
                        symbol=row.symbol,
                        matured_at_assessment=matured,
                        reported_matured_slots=row.matured_slots,
                        complete_labels=row.complete_labels,
                        rejected_labels=row.rejected_labels,
                        unwritten_mature_labels=row.unwritten_mature_labels,
                        full_mature_primary_percent=(
                            _rounded_fraction(Fraction(row.complete_labels * 100, matured))
                            if matured
                            else "0"
                        ),
                        observed_written_slice_percent_not_primary=(
                            _rounded_fraction(Fraction(row.complete_labels * 100, written))
                            if written
                            else None
                        ),
                    )
                )
        scenario = scenarios.get(binding.cost_scenario_id)
        execution = proof(binding.execution_relationship_sha256, "execution_auditor")
        account = proof(binding.account_eligibility_sha256, "account_provider")
        fees = proof(binding.fee_schedule_sha256, "fee_provider")
        friction = proof(binding.additional_friction_sha256, "friction_auditor")
        if envelope.account_digest is None:
            reasons.append("VENUE_ACCOUNT_UNKNOWN")
        if not isinstance(execution, ExecutionRelationship) or (
            execution.source_quality_sha256 != binding.source_quality_sha256
            or execution.source_venue != envelope.execution_venue
            or execution.execution_venue != envelope.execution_venue
            or execution.account_digest != envelope.account_digest
            or execution.quote_fill_pairs == 0
            or execution.verified_at_ns > envelope.assessed_at_ns
            or any(
                (
                    execution.causal_violations,
                    execution.venue_identifier_mismatches,
                    execution.quantity_or_price_discrepancies,
                )
            )
        ):
            reasons.append("SOURCE_EXECUTION_RELATIONSHIP_UNPROVEN")
        if not isinstance(source_item, SourceQuality):
            reasons.append("ACCOUNT_COST_WINDOW_UNAVAILABLE")
        else:
            contract = source_item.contract
            if not isinstance(account, AccountEligibility) or (
                account.venue != envelope.execution_venue
                or account.account_digest != envelope.account_digest
                or account.eligibility != "eligible"
                or account.effective_from_ns > contract.start_ns
                or account.effective_until_ns < max(contract.end_ns, envelope.assessed_at_ns)
            ):
                reasons.append("VENUE_ACCOUNT_ELIGIBILITY_UNPROVEN")
            if not isinstance(fees, FeeSchedule) or (
                fees.venue != envelope.execution_venue
                or fees.account_digest != envelope.account_digest
                or fees.scope != "account_specific"
                or fees.maker_bps is None
                or fees.taker_bps is None
                or fees.effective_from_ns > contract.start_ns
                or fees.effective_until_ns < max(contract.end_ns, envelope.assessed_at_ns)
            ):
                reasons.append("ACCOUNT_SPECIFIC_FEES_UNPROVEN")
        if (
            scenario is None
            or scenario.provenance != "account_evidence"
            or (scenario.venue != envelope.execution_venue)
        ):
            reasons.append("COST_SCENARIO_NOT_ACCOUNT_EVIDENCE")
        else:
            if (
                isinstance(fees, FeeSchedule)
                and fees.maker_bps is not None
                and fees.taker_bps is not None
            ):
                expected_entry = (
                    fees.maker_bps
                    if scenario.entry_liquidity == "maker_verified"
                    else fees.taker_bps
                    if scenario.entry_liquidity == "taker"
                    else max(fees.maker_bps, fees.taker_bps)
                )
                if (
                    scenario.entry_fee_bps != expected_entry
                    or scenario.exit_fee_bps != fees.taker_bps
                ):
                    reasons.append("SCENARIO_FEE_LEGS_DIFFER_FROM_ACCOUNT_EVIDENCE")
            if scenario.entry_liquidity == "maker_verified" and (
                not isinstance(execution, ExecutionRelationship)
                or execution.verified_entry_liquidity != "maker"
            ):
                reasons.append("PASSIVE_ORDER_DOES_NOT_PROVE_MAKER_FILL")
            if not isinstance(execution, ExecutionRelationship) or (
                execution.validated_return_basis != scenario.basis
                or (
                    scenario.basis != "raw_ask_to_bid"
                    and execution.fee_embedding_records_sha256 is None
                )
            ):
                reasons.append("DECLARED_RETURN_BASIS_OR_EMBEDDED_FEE_PROOF_UNSUPPORTED")
            if (
                scenario.residual_nonembedded_bps is None
                or not isinstance(friction, AdditionalFriction)
                or (
                    friction.execution_venue != envelope.execution_venue
                    or friction.account_digest != envelope.account_digest
                    or friction.source_quality_sha256 != binding.source_quality_sha256
                    or friction.basis != scenario.basis
                    or friction.residual_bps != scenario.residual_nonembedded_bps
                )
            ):
                reasons.append("NONEMBEDDED_FRICTION_UNPROVEN")
        decisions.append(
            SourceDecision(
                source_id=binding.source_id,
                offline_research_prerequisites_met=not reasons,
                reasons=tuple(dict.fromkeys(reasons)),
                coverage=tuple(coverage),
                evidence_hashes_checked=tuple(checked),
            )
        )
    ready = all(row.offline_research_prerequisites_met for row in decisions)
    return ReadinessReport(
        decision=(
            "QUALIFIED_FIXTURE_ONLY"
            if ready and envelope.evidence_mode == "hypothetical_fixture"
            else "OFFLINE_RESEARCH_PREREQUISITES_MET"
            if ready
            else "NO_GO"
        ),
        input_sha256=envelope.identity,
        trust_manifest_sha256=manifest.identity,
        trust_root_matched=root_matched,
        offline_research_ready=ready,
        sources=tuple(decisions),
        cost_frontiers=tuple(cost_hurdle(row) for row in envelope.cost_scenarios),
    )


def _object(pairs: list[tuple[str, JsonValue]]) -> dict[str, JsonValue]:
    result: dict[str, JsonValue] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _nonfinite(value: str) -> JsonValue:
    raise ValueError("nonfinite JSON is not evidence")


def bounded_json(data: bytes) -> dict[str, JsonValue]:
    if len(data) > INPUT_LIMIT:
        raise ValueError("input byte budget exceeded")
    return JSON_OBJECT.validate_python(
        json.loads(
            data,
            object_pairs_hook=_object,
            parse_constant=_nonfinite,
        )
    )


def _read(path: Path) -> bytes:
    with path.open("rb") as stream:
        value = stream.read(INPUT_LIMIT + 1)
    bounded_json(value)
    return value


def _ns(value: JsonValue) -> int:
    if not isinstance(value, str):
        raise ValueError("archive requires an explicit aware UTC timestamp")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("archive timestamp is naive")
    delta = parsed.astimezone(UTC) - datetime(1970, 1, 1, tzinfo=UTC)
    return (delta.days * 86400 + delta.seconds) * NS + delta.microseconds * 1000


def closeout_example(protocol_bytes: bytes, verdict_bytes: bytes) -> ReadinessReport:
    """Map the committed failed archive without approving or rewriting its evidence."""
    protocol = bounded_json(protocol_bytes)
    verdict = bounded_json(verdict_bytes)
    protocol_hash = sha256(canonical(protocol)).hexdigest()
    if (
        verdict.get("protocol_hash") != protocol_hash
        or protocol_hash != "e05f3c559cfc3841c8492500aeebdecbcb2812e0b2f4bc743c549ee9ed560f3a"
    ):
        raise ValueError("archive protocol identity mismatch")
    expected_contract: dict[str, JsonValue] = {
        "symbols": ["BTC/USD", "ETH/USD"],
        "duration_hours": 72,
        "slots_per_symbol": SLOTS,
        "evaluation_seconds": 10,
        "horizon_seconds": 60,
        "settle_seconds": 2,
        "freshness_seconds": 2,
        "notional_usd": "10.25",
        "minimum_coverage": "0.95",
        "depth": 10,
    }
    if any(protocol.get(key) != value for key, value in expected_contract.items()):
        raise ValueError("archive observation contract cannot be relabeled")
    if verdict.get("decision") != "NO_GO" or not str(verdict.get("state", "")).startswith(
        "failed_"
    ):
        raise ValueError("this blocked example requires the original failed NO_GO closeout")
    contract = FrozenContract(
        protocol_sha256=protocol_hash,
        frozen_at_ns=_ns(protocol.get("frozen_at")),
        start_ns=_ns(protocol.get("start")),
        end_ns=_ns(protocol.get("end")),
        symbols=("BTC/USD", "ETH/USD"),
        slots_per_symbol=25920,
        cadence_seconds=10,
        horizon_seconds=60,
        settlement_seconds=2,
        provider_freshness_seconds=2,
        receipt_freshness_seconds=2,
        best_side_notional_usd="10.25",
    )
    raw_symbols = verdict.get("per_symbol")
    if not isinstance(raw_symbols, dict):
        raise ValueError("closeout per-symbol counters missing")
    rows: list[SymbolCounts] = []
    for symbol in SYMBOLS:
        body = raw_symbols.get(symbol)
        if not isinstance(body, dict):
            raise ValueError("both closeout symbol counters required")
        absent = body.get("absent_evaluations_after_coordinator_stall")
        if not isinstance(absent, int) or isinstance(absent, bool):
            raise ValueError("absent evaluation count missing")
        rows.append(
            SymbolCounts.model_validate_json(
                canonical(
                    {
                        "symbol": symbol,
                        "matured_slots": body.get("scheduled_and_mature"),
                        "complete_labels": body.get("complete_original_labels"),
                        "rejected_labels": body.get("written_rejected_labels"),
                        "unwritten_mature_labels": body.get("unwritten_mature_outcomes"),
                        "observed_evaluations": SLOTS - absent,
                    }
                )
            )
        )
    source = SourceQuality(
        kind="source_quality",
        authority_id="unapproved-committed-closeout",
        source_id="frozen-e05",
        source_venue="kraken",
        source_kind="native_checksum_book",
        contract=contract,
        report_as_of_ns=_ns(verdict.get("status_as_of")),
        state="failed_incomplete",
        symbols=(rows[0], rows[1]),
        raw_frames_verified=None,
        final_quality_frames=None,
        checksum_failures=0,
        clock_failures=0,
        continuity_breaks=0,
        raw_sequence_failures=0,
        journal_root="54782507ee32d1d6a4c4b80561aa9d045975c96f6ddc0f8bf5aec3d3a8f03ab2",
        original_artifact_sha256=sha256(verdict_bytes).hexdigest(),
    )

    def scenario(
        name: str,
        venue: Venue,
        fee: str,
        provenance: Literal[
            "public_tier_scenario",
            "configured_model_scenario",
        ],
    ) -> CostScenario:
        return CostScenario(
            scenario_id=name,
            venue=venue,
            provenance=provenance,
            basis="raw_ask_to_bid",
            entry_liquidity="unconfirmed_worst_rate",
            entry_fee_bps=Decimal(fee),
            exit_fee_bps=Decimal(fee),
            entry_charge="base_inventory",
            exit_charge="cash",
            residual_nonembedded_bps=None,
            target_net_bps=Decimal(0),
            observed_basis_return_bps=None,
        )

    scenarios = (
        scenario("kraken-public-maker-not-account-tier", "kraken", "40", "public_tier_scenario"),
        scenario("kraken-public-taker-not-account-tier", "kraken", "80", "public_tier_scenario"),
        scenario(
            "alpaca-configured-not-actual-fees", "alpaca_paper", "25", "configured_model_scenario"
        ),
    )
    envelope = ReadinessEnvelope(
        schema_version="profitability-readiness-input-v1",
        evidence_mode="independently_approved",
        assessed_at_ns=source.report_as_of_ns,
        predeclared_at_ns=None,
        execution_venue="kraken",
        account_digest=None,
        sources=(
            SourceBinding(
                source_id=source.source_id,
                source_quality_sha256=source.identity,
                cost_scenario_id=scenarios[1].scenario_id,
                execution_relationship_sha256=None,
                account_eligibility_sha256=None,
                fee_schedule_sha256=None,
                additional_friction_sha256=None,
            ),
        ),
        evidence=(source,),
        cost_scenarios=scenarios,
    )
    manifest = TrustManifest(
        schema_version="profitability-evidence-approval-v1",
        mode="independently_approved",
        approved_at_ns=source.report_as_of_ns,
        predeclared_plan_sha256=None,
        entries=(),
    )
    return evaluate_readiness(envelope, manifest, trusted_manifest_sha256="0" * 64)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("schema")
    evaluate = commands.add_parser("evaluate")
    evaluate.add_argument("input", type=Path)
    evaluate.add_argument("--trust-manifest", type=Path, required=True)
    evaluate.add_argument("--trust-sha256", required=True)
    closeout = commands.add_parser("closeout")
    closeout.add_argument("verdict", type=Path)
    closeout.add_argument("--protocol", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "schema":
            result: object = {
                "input": ReadinessEnvelope.model_json_schema(),
                "trust_manifest": TrustManifest.model_json_schema(),
                "output": ReadinessReport.model_json_schema(),
            }
        elif args.command == "closeout":
            result = closeout_example(_read(args.protocol), _read(args.verdict)).model_dump(
                mode="json"
            )
        else:
            envelope = ReadinessEnvelope.model_validate_json(_read(args.input))
            manifest = TrustManifest.model_validate_json(_read(args.trust_manifest))
            result = evaluate_readiness(
                envelope,
                manifest,
                trusted_manifest_sha256=args.trust_sha256,
            ).model_dump(mode="json")
        rendered = canonical(result)
        if len(rendered) > OUTPUT_LIMIT:
            raise ValueError("output byte budget exceeded")
    except (ValueError, OSError) as error:
        print(
            canonical(
                {
                    "decision": "NO_GO",
                    "model_ready": False,
                    "trading_allowed": False,
                    "profitability_claim": False,
                    "error_type": type(error).__name__,
                    "reason": "INVALID_OR_UNAVAILABLE_EVIDENCE",
                }
            ).decode()
        )
        raise SystemExit(2) from None
    print(rendered.decode())


if __name__ == "__main__":
    main()
