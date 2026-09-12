from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from tradeagent.cli import _parser
from tradeagent.scalping_cli import configuration
from tradeagent.scalping_config import ScalpingConfig, ScalpQuote, ScalpSignal

NOW = datetime(2026, 9, 11, 6, 18, 28, tzinfo=UTC)


def config(**overrides: object) -> ScalpingConfig:
    return ScalpingConfig.model_validate(
        {
            "cohort_id": "v30-test",
            "account_digest": "a" * 64,
            "approved_at": NOW,
            **overrides,
        }
    )


def quote(**overrides: object) -> ScalpQuote:
    return ScalpQuote.model_validate(
        {
            "symbol": "BTC/USD",
            "exchange_at": NOW,
            "exchange_time_ns": int(NOW.timestamp()) * 1_000_000_000,
            "received_at": NOW + timedelta(milliseconds=20),
            "bid": "100",
            "ask": "100.01",
            "bid_size": "2",
            "ask_size": "1",
            **overrides,
        }
    )


def test_paper_profile_has_no_financial_or_approval_gates() -> None:
    settings = config(order_notional_usd="1000000")
    policy = settings.policy_description()
    assert settings.order_notional_usd == Decimal("1000000")
    assert settings.mode == "paper"
    assert policy["paper_only"] is True
    assert policy["daily_approval_required"] is False
    assert policy["qualification_required"] is False
    assert policy["shadow_required"] is False
    assert policy["news_risk_veto"] is False
    assert policy["macro_veto"] is False
    for name in (
        "paper_daily_loss_limit",
        "paper_drawdown_limit",
        "paper_exposure_limit",
        "paper_daily_trade_limit",
        "catastrophic_stop",
    ):
        assert policy[name] is None


@pytest.mark.parametrize(
    "overrides",
    [
        {"mode": "live"},
        {"mode": "shadow"},
        {"profile": "news-paper"},
        {"symbols": []},
        {"symbols": ["BTC/USD", "btc/usd"]},
        {"symbols": ["MES"]},
        {"order_notional_usd": "0"},
        {"order_notional_usd": "NaN"},
        {"decision_interval_seconds": float("inf")},
        {"approved_at": NOW.replace(tzinfo=None)},
        {"daily_loss_limit": 1},
    ],
)
def test_invalid_operating_contract_is_explicit(overrides: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        config(**overrides)


def test_configuration_identity_is_stable_and_versioned() -> None:
    first = config(symbols=["btc/usd", "eth/usd"])
    assert first.symbols == ("BTC/USD", "ETH/USD")
    assert ScalpingConfig.model_validate_json(first.model_dump_json()).identity == first.identity
    assert config(order_notional_usd="101").identity != first.identity


@pytest.mark.parametrize(
    "overrides",
    [
        {"ask": "99"},
        {"bid": "0"},
        {"bid_size": "-1"},
        {"exchange_at": NOW + timedelta(seconds=1)},
        {"bid": "NaN"},
    ],
)
def test_quotes_require_real_numerical_and_causal_input(overrides: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        quote(**overrides)


def test_signal_does_not_invent_profitability_or_future_information() -> None:
    values = {
        "decision_id": "decision-1",
        "symbol": "BTC/USD",
        "observed_at": NOW + timedelta(milliseconds=30),
        "action": "buy",
        "family": "momentum",
        "score": 0.5,
        "reasons": ["book_pressure"],
        "quote": quote(),
        "features": {"ofi_5s": 1.0, "aggressor_delta": None},
        "estimated_round_trip_cost_bps": 40,
    }
    signal = ScalpSignal.model_validate(values)
    assert signal.expected_net_edge_bps is None and signal.profitability_validated is False
    with pytest.raises(ValidationError):
        ScalpSignal.model_validate({**values, "profitability_validated": True})
    with pytest.raises(ValidationError):
        ScalpSignal.model_validate({**values, "observed_at": NOW})
    with pytest.raises(ValidationError):
        ScalpSignal.model_validate({**values, "symbol": "ETH/USD"})


def test_cli_selects_standing_crypto_profile_without_dated_entry_window() -> None:
    args = _parser().parse_args(
        [
            "scalp-run",
            "--cohort-id",
            "v30-test",
            "--account-digest",
            "a" * 64,
            "--approved-at",
            NOW.isoformat(),
            "--confirm-paper-unrestricted",
        ]
    )
    settings = configuration(args)
    assert settings.profile == "v30-paper-unrestricted"
    assert settings.order_notional_usd == 100
    assert settings.decision_policy == "action-value-v1"
    assert settings.maximum_quote_age_seconds == 1
    assert settings.exit_after_seconds == settings.feature_horizon_seconds == 5
    assert settings.policy_description()["prospective_economic_gate"] is True
    assert settings.catastrophic_stop_bps == 100
    assert "entry_deadline" not in ScalpingConfig.model_fields
    args.confirm_paper_unrestricted = False
    with pytest.raises(ValueError, match="explicit selection"):
        configuration(args)


def test_economic_model_requires_a_hash_and_exit_grace_is_horizon_bounded():
    with pytest.raises(ValidationError, match="immutable hash"):
        config(economic_model_path="unreviewed-model.json")
    with pytest.raises(ValidationError, match="catastrophic stop"):
        config(decision_policy="action-value-v1")
    with pytest.raises(ValidationError, match="prediction horizon"):
        config(
            decision_policy="action-value-v1",
            catastrophic_stop_bps="100",
            latency_grace_seconds=300,
        )
