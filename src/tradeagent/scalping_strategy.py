from __future__ import annotations

import math
from datetime import UTC, datetime
from hashlib import sha256
from typing import Literal, cast

from tradeagent.scalping_config import ScalpingConfig, ScalpInventory, ScalpSignal
from tradeagent.scalping_economics import (
    EconomicDecision,
    EconomicModel,
    evaluate_actions,
    project_economic_features,
)
from tradeagent.scalping_market import BookFeatures, datetime_ns


class ScalpStrategy:
    """Two deterministic, uncalibrated hypotheses; neither estimates a profitable edge."""

    def __init__(self, config: ScalpingConfig, economic_model: EconomicModel | None = None) -> None:
        self.config = config
        self.economic_model = economic_model

    def _economics(
        self,
        features: BookFeatures,
        *,
        family: Literal["momentum", "reversion"],
        now: datetime,
        decision_latency_seconds: float,
    ) -> EconomicDecision:
        quote_age = (
            now.astimezone(UTC) - features.quote.exchange_at.astimezone(UTC)
        ).total_seconds()
        mid = (features.quote.bid + features.quote.ask) / 2
        return evaluate_actions(
            self.economic_model,
            symbol=features.symbol,
            family=family,
            features=project_economic_features(features.signal_features()),
            now=now,
            quote_age_seconds=max(0.0, quote_age),
            decision_latency_seconds=max(0.0, decision_latency_seconds),
            spread_bps=features.spread_bps,
            notional_usd=float(self.config.order_notional_usd),
            reference_mid_price=float(mid),
            account_digest=self.config.account_digest,
            maker_fee_bps=float(self.config.maker_fee_bps),
            taker_fee_bps=float(self.config.taker_fee_bps),
        )

    @staticmethod
    def _momentum(features: BookFeatures) -> float:
        scale = max(features.spread_bps, features.volatility_5s_bps or 0.0, 0.01)
        flow = (
            features.taker_delta_5s / features.trade_volume_5s
            if features.taker_delta_5s is not None and features.trade_volume_5s > 0
            else 0.0
        )
        return (
            0.20 * features.l1_imbalance
            + 0.20 * features.l5_imbalance
            + 0.25 * math.tanh(3 * features.normalized_ofi_5s)
            + 0.15 * math.tanh((features.return_5s_bps or 0.0) / scale)
            + 0.10 * math.tanh(4 * features.microprice_displacement_bps / scale)
            + 0.10 * flow
        )

    @staticmethod
    def _reversion(features: BookFeatures) -> tuple[bool, float]:
        five, one = features.return_5s_bps, features.return_1s_bps
        if five is None or one is None:
            return False, 0.0
        scale = max(features.spread_bps, features.volatility_5s_bps or 0.0, 0.01)
        shock = -five / scale
        recovery = (
            five < 0
            and shock >= 0.5
            and features.bid_depletion_fraction_5s >= 0.10
            and one >= 0
            and features.normalized_ofi_1s > 0
            and features.bid_additions_1s > 0
            and features.l1_imbalance > 0
            and features.microprice_displacement_bps > 0
        )
        score = (
            0.25 * math.tanh(max(0.0, shock))
            + 0.25 * min(1.0, features.bid_depletion_fraction_5s)
            + 0.20 * math.tanh(features.bid_additions_1s / features.bid_depth_l5)
            + 0.20 * max(0.0, math.tanh(3 * features.normalized_ofi_1s))
            + 0.10 * max(0.0, features.l1_imbalance)
        )
        return recovery, score

    def decide(
        self,
        features: BookFeatures,
        *,
        inventory: ScalpInventory | None,
        now: datetime,
        decision_latency_seconds: float = 0,
    ) -> ScalpSignal:
        now_ns = datetime_ns(now)
        if features.symbol not in self.config.symbols:
            raise ValueError("feature symbol is outside the configured crypto universe")
        if features.as_of_ns > now_ns or features.quote.received_at > now:
            raise ValueError("future features cannot inform a decision")
        if inventory is not None and (
            inventory.symbol != features.symbol or inventory.opened_at > now
        ):
            raise ValueError("inventory must match the symbol and already exist")
        momentum = self._momentum(features)
        reversion_candidate, reversion = self._reversion(features)
        positive_momentum = (
            features.return_5s_bps is not None
            and features.return_5s_bps > 0
            and features.normalized_ofi_5s > 0
            and features.microprice_displacement_bps > 0
            and momentum >= self.config.momentum_threshold
        )
        positive_reversion = reversion_candidate and reversion >= self.config.reversion_threshold
        candidates: list[Literal["momentum", "reversion"]] = []
        if positive_momentum:
            candidates.append("momentum")
        if positive_reversion:
            candidates.append("reversion")
        candidate_families = tuple(candidates)
        family: Literal["momentum", "reversion", "none"] = (
            candidate_families[0] if candidate_families else "none"
        )
        score = reversion if family == "reversion" else momentum
        action: Literal["buy", "sell", "hold"] = "hold"
        held = inventory is not None and inventory.quantity > 0
        positive = features.ready and family != "none"
        economic_policy = self.config.decision_policy == "action-value-v1"
        economic: EconomicDecision | None = None
        reasons: tuple[str, ...]
        if (
            held
            and inventory is not None
            and (
                (
                    economic_policy
                    and inventory.exit_due_at is not None
                    and now >= inventory.exit_due_at
                )
                or (
                    not economic_policy
                    and self.config.exit_after_seconds is not None
                    and (now - inventory.opened_at).total_seconds()
                    >= self.config.exit_after_seconds
                )
            )
        ):
            action, reasons = "sell", ("strategy_time_exit",)
        elif economic_policy and held and inventory is not None and inventory.exit_due_at is None:
            action, reasons = "sell", ("missing_entry_horizon",)
        elif not features.ready:
            family, reasons = "none", ("insufficient_actual_feature_history",)
        elif held:
            if positive and (
                not economic_policy or (inventory is not None and inventory.family == family)
            ):
                reasons = ("positive_alpha_maintained",)
            else:
                action, reasons = (
                    "sell",
                    (
                        ("original_regime_invalidated",)
                        if economic_policy and inventory is not None and inventory.family != family
                        else ("entry_signal_invalidated",)
                    ),
                )
        elif positive:
            if economic_policy:
                decisions = [
                    self._economics(
                        features,
                        family=candidate,
                        now=now,
                        decision_latency_seconds=decision_latency_seconds,
                    )
                    for candidate in candidate_families
                ]
                economic = max(
                    decisions,
                    key=lambda item: (
                        item.conservative_net_edge_bps,
                        item.family == "momentum",
                    ),
                )
                family = cast(Literal["momentum", "reversion"], economic.family)
                score = reversion if family == "reversion" else momentum
                if economic.selected_action in {"PASSIVE_BUY", "AGGRESSIVE_BUY"}:
                    action = "buy"
                    reasons = (
                        "positive_action_value",
                        economic.selected_action.lower(),
                    )
                else:
                    reasons = ("economic_no_trade", *economic.reason_codes)
            else:
                action = "buy"
                reasons = (
                    ("positive_momentum", "advancing_price_and_order_flow")
                    if family == "momentum"
                    else (
                        "liquidity_shock_reversion",
                        "bid_depletion_then_replenishment_and_recovery",
                    )
                )
        else:
            reasons = (
                ("negative_alpha_no_crypto_short",) if momentum < 0 else ("no_positive_alpha",)
            )
        fee_bps = (
            max(self.config.maker_fee_bps, self.config.taker_fee_bps)
            if economic_policy
            else self.config.maker_fee_bps
            if self.config.entry_style == "passive"
            else self.config.taker_fee_bps
        ) + self.config.taker_fee_bps
        diagnostics = features.signal_features()
        diagnostics.update(
            momentum_score=momentum,
            reversion_score=reversion,
            score_kind="heuristic_not_probability",
            financial_qualification_gate="prospective_action_economics"
            if economic_policy
            else "disabled",
            cost_basis="entry fee + taker exit fee + conservative full spread",
        )
        identity = "|".join(
            (
                self.config.identity,
                features.symbol,
                str(now_ns),
                features.source_event_id,
                action,
                str(inventory.quantity) if inventory is not None else "flat",
            )
        )
        return ScalpSignal(
            decision_id="v30-" + sha256(identity.encode()).hexdigest()[:48],
            symbol=features.symbol,
            observed_at=now,
            action=action,
            family=family,
            score=score,
            reasons=reasons,
            quote=features.quote,
            features=diagnostics,
            estimated_round_trip_cost_bps=float(fee_bps) + features.spread_bps,
            economics=economic,
            expected_net_edge_bps=economic.expected_net_edge_bps if economic else None,
            profitability_validated=economic.profitability_validated if economic else False,
        )
