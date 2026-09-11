from datetime import timedelta
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError
from test_scalping_market import NOW, Tape, config, rising

from tradeagent.scalping_config import ScalpInventory
from tradeagent.scalping_market import BookFeatureEngine, BookFeatures
from tradeagent.scalping_strategy import ScalpStrategy


def features(**overrides: Any) -> BookFeatures:
    engine = BookFeatureEngine(config())
    events = rising(Tape())
    for event in events:
        engine.on_event(event)
    result = engine.features("BTC/USD", events[-1].received_at)
    assert result is not None
    return BookFeatures.model_validate({**result.model_dump(), **overrides})


def inventory(value: BookFeatures, *, seconds: float = 1, **overrides: Any) -> ScalpInventory:
    return ScalpInventory.model_validate(
        {
            "symbol": value.symbol,
            "quantity": Decimal("0.5"),
            "opening_vwap": 100,
            "opened_at": value.as_of - timedelta(seconds=seconds),
            **overrides,
        }
    )


def test_positive_momentum_is_transparent_and_not_gated_by_fees_or_old_approval() -> None:
    value = features()
    strategy = ScalpStrategy(config(maker_fee_bps=1000, taker_fee_bps=2000))
    signal = strategy.decide(value, inventory=None, now=value.as_of)
    assert signal.action == "buy" and signal.family == "momentum"
    assert 0.25 <= signal.score <= 1
    assert signal.estimated_round_trip_cost_bps > 3000
    assert signal.expected_net_edge_bps is None and not signal.profitability_validated
    assert signal.features["score_kind"] == "heuristic_not_probability"
    assert signal.features["financial_qualification_gate"] == "disabled"
    assert signal == strategy.decide(value, inventory=None, now=value.as_of)
    later = strategy.decide(value, inventory=None, now=value.as_of + timedelta(seconds=1))
    assert later.decision_id != signal.decision_id


def test_liquidity_shock_reversion_is_replenishment_recovery_not_inverse_momentum() -> None:
    value = features(
        return_5s_bps=-20,
        return_1s_bps=1,
        volatility_5s_bps=10,
        spread_bps=2,
        normalized_ofi_5s=-0.5,
        normalized_ofi_1s=0.5,
        l1_imbalance=0.8,
        l5_imbalance=0.2,
        microprice_displacement_bps=0.5,
        bid_depletion_fraction_5s=1.0,
        bid_additions_1s=10,
        bid_depth_l5=10,
    )
    strategy = ScalpStrategy(config())
    signal = strategy.decide(value, inventory=None, now=value.as_of)
    assert signal.action == "buy" and signal.family == "reversion"
    assert signal.features["momentum_score"] < 0.25
    assert "bid_depletion_then_replenishment_and_recovery" in signal.reasons
    for missing_repair in (
        {"bid_additions_1s": 0},
        {"bid_depletion_fraction_5s": 0},
        {"normalized_ofi_1s": 0},
        {"return_1s_bps": -1},
    ):
        damaged = BookFeatures.model_validate({**value.model_dump(), **missing_repair})
        absent = strategy.decide(damaged, inventory=None, now=value.as_of)
        assert absent.action == "hold" and absent.family == "none"


def test_negative_alpha_does_not_sell_without_owned_crypto() -> None:
    value = features(
        return_5s_bps=-5,
        return_1s_bps=-1,
        l1_imbalance=-0.8,
        l5_imbalance=-0.8,
        normalized_ofi_5s=-1,
        microprice_displacement_bps=-1,
        bid_additions_1s=0,
    )
    strategy = ScalpStrategy(config())
    flat = strategy.decide(value, inventory=None, now=value.as_of)
    assert flat.action == "hold" and flat.score < 0
    assert flat.reasons == ("negative_alpha_no_crypto_short",)
    held = strategy.decide(value, inventory=inventory(value), now=value.as_of)
    assert held.action == "sell" and held.reasons == ("entry_signal_invalidated",)


def test_existing_positive_alpha_holds_inventory_and_fifteen_second_strategy_exit_wins() -> None:
    value, strategy = features(), ScalpStrategy(config())
    held = strategy.decide(value, inventory=inventory(value, seconds=14.99), now=value.as_of)
    assert held.action == "hold" and held.reasons == ("positive_alpha_maintained",)
    exit_signal = strategy.decide(value, inventory=inventory(value, seconds=15), now=value.as_of)
    assert exit_signal.action == "sell" and exit_signal.reasons == ("strategy_time_exit",)
    no_timer = ScalpStrategy(config(exit_after_seconds=None))
    assert no_timer.decide(
        value, inventory=inventory(value, seconds=9999), now=value.as_of
    ).action == ("hold")


def test_sparse_history_holds_entries_but_does_not_suppress_time_exit() -> None:
    value, strategy = features(ready=False, history_seconds=1), ScalpStrategy(config())
    signal = strategy.decide(value, inventory=None, now=value.as_of)
    assert signal.action == "hold" and signal.reasons == ("insufficient_actual_feature_history",)
    assert (
        strategy.decide(value, inventory=inventory(value, seconds=15), now=value.as_of).action
        == "sell"
    )


def test_neutral_data_never_generates_quota_or_random_trades() -> None:
    value = features(
        l1_imbalance=0,
        l5_imbalance=0,
        ofi_5s=0,
        normalized_ofi_5s=0,
        microprice_displacement_bps=0,
        return_5s_bps=0,
        return_1s_bps=0,
    )
    strategy = ScalpStrategy(config())
    for offset in range(30):
        signal = strategy.decide(value, inventory=None, now=value.as_of + timedelta(seconds=offset))
        assert signal.action == "hold" and signal.family == "none"


def test_strategy_rejects_future_cross_symbol_and_nonfinite_input() -> None:
    value, strategy = features(), ScalpStrategy(config())
    with pytest.raises(ValueError, match="future"):
        strategy.decide(value, inventory=None, now=NOW)
    with pytest.raises(ValueError, match="inventory"):
        strategy.decide(value, inventory=inventory(value, symbol="ETH/USD"), now=value.as_of)
    with pytest.raises(ValueError, match="inventory"):
        strategy.decide(value, inventory=inventory(value, seconds=-1), now=value.as_of)
    with pytest.raises(ValidationError):
        features(normalized_ofi_5s=float("nan"))


def test_current_book_only_alpha_needs_neither_trades_quotes_vwap_nor_native_side() -> None:
    tape, engine = Tape(), BookFeatureEngine(config())
    assert engine.on_event(
        tape.book(
            -67,
            received_seconds=0.01,
            bid="99",
            ask="99.01",
            bid_size="5",
            ask_size="0.1",
            snapshot=True,
        )
    )
    for event in rising(tape, 1, 6):
        assert engine.on_event(event)
    value = engine.features("BTC/USD", NOW + timedelta(seconds=6.01))
    assert value is not None and value.ready
    assert value.trade_count_5s == value.native_taker_trade_count_5s == 0
    assert value.taker_delta_1s is None and value.taker_delta_5s is None
    assert value.session_vwap is None and value.session_poc is None and value.atr_1m is None
    assert value.quote.exchange_time_ns == value.as_of_ns - 10_000_000
    signal = ScalpStrategy(config()).decide(value, inventory=None, now=value.as_of)
    assert signal.action == "buy" and signal.family == "momentum"
    assert signal.features["trade_count_5s"] == 0
    assert signal.features["taker_delta_5s"] is None
    assert signal.expected_net_edge_bps is None and not signal.profitability_validated


def test_liquidity_reversion_accepts_explicitly_unobserved_trade_inputs() -> None:
    value = features(
        return_5s_bps=-20,
        return_1s_bps=1,
        volatility_5s_bps=10,
        spread_bps=2,
        normalized_ofi_5s=-0.5,
        normalized_ofi_1s=0.5,
        l1_imbalance=0.8,
        l5_imbalance=0.2,
        microprice_displacement_bps=0.5,
        bid_depletion_fraction_5s=1.0,
        bid_additions_1s=10,
        bid_depth_l5=10,
        trade_count_5s=0,
        native_taker_trade_count_5s=0,
        trade_volume_5s=0,
        taker_delta_1s=None,
        taker_delta_5s=None,
        session_vwap=None,
        session_poc=None,
        known_taker_fraction_5s=None,
    )
    signal = ScalpStrategy(config()).decide(value, inventory=None, now=value.as_of)
    assert signal.action == "buy" and signal.family == "reversion"
    assert signal.features["taker_delta_5s"] is None and signal.features["session_vwap"] is None
