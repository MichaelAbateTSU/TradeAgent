from __future__ import annotations

import math
from datetime import datetime
from hashlib import sha256
from typing import Literal

from tradeagent.scalping_config import ScalpingConfig, ScalpInventory, ScalpSignal
from tradeagent.scalping_market import BookFeatures, datetime_ns


class ScalpStrategy:
    """Two deterministic, uncalibrated hypotheses; neither estimates a profitable edge."""

    def __init__(self, config: ScalpingConfig) -> None:
        self.config = config

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
        self, features: BookFeatures, *, inventory: ScalpInventory | None, now: datetime
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
        family: Literal["momentum", "reversion", "none"] = (
            "momentum" if positive_momentum else "reversion" if positive_reversion else "none"
        )
        score = reversion if family == "reversion" else momentum
        action: Literal["buy", "sell", "hold"] = "hold"
        held = inventory is not None and inventory.quantity > 0
        positive = features.ready and family != "none"
        reasons: tuple[str, ...]
        if (
            held
            and inventory is not None
            and self.config.exit_after_seconds is not None
            and (now - inventory.opened_at).total_seconds() >= self.config.exit_after_seconds
        ):
            action, reasons = "sell", ("strategy_time_exit",)
        elif not features.ready:
            family, reasons = "none", ("insufficient_actual_feature_history",)
        elif held:
            if positive:
                reasons = ("positive_alpha_maintained",)
            else:
                action, reasons = "sell", ("entry_signal_invalidated",)
        elif positive:
            action = "buy"
            reasons = (
                ("positive_momentum", "advancing_price_and_order_flow")
                if family == "momentum"
                else ("liquidity_shock_reversion", "bid_depletion_then_replenishment_and_recovery")
            )
        else:
            reasons = (
                ("negative_alpha_no_crypto_short",) if momentum < 0 else ("no_positive_alpha",)
            )
        fee_bps = (
            self.config.maker_fee_bps
            if self.config.entry_style == "passive"
            else self.config.taker_fee_bps
        ) + self.config.taker_fee_bps
        diagnostics = features.signal_features()
        diagnostics.update(
            momentum_score=momentum,
            reversion_score=reversion,
            score_kind="heuristic_not_probability",
            financial_qualification_gate="disabled",
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
            expected_net_edge_bps=None,
            profitability_validated=False,
        )
