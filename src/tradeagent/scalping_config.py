from __future__ import annotations

from decimal import Decimal
from hashlib import sha256
from typing import Any, Literal, Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from tradeagent.scalping_economics import EconomicDecision


def crypto_symbol(value: str) -> str:
    symbol = value.strip().upper()
    base, separator, quote = symbol.partition("/")
    if not separator or not base.isalnum() or quote != "USD":
        raise ValueError("a USD crypto pair such as BTC/USD is required")
    return symbol


class ScalpingConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="SCALP_", frozen=True, extra="forbid", allow_inf_nan=False
    )

    profile: Literal["v30-paper-unrestricted"] = "v30-paper-unrestricted"
    mode: Literal["paper"] = "paper"
    cohort_id: str = Field(min_length=1, max_length=64)
    account_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    approved_at: AwareDatetime
    symbols: tuple[str, ...] = ("BTC/USD", "ETH/USD")
    order_notional_usd: Decimal = Field(default=Decimal("100"), gt=0)
    decision_interval_seconds: float = Field(default=1, gt=0)
    feature_horizon_seconds: float = Field(default=5, gt=0)
    exit_after_seconds: float | None = Field(default=15, gt=0)
    entry_order_ttl_seconds: float = Field(default=3, gt=0)
    entry_style: Literal["passive", "marketable"] = "passive"
    momentum_threshold: float = Field(default=0.25, gt=0)
    reversion_threshold: float = Field(default=0.65, gt=0)
    maker_fee_bps: Decimal = Field(default=Decimal("15"), ge=0)
    taker_fee_bps: Decimal = Field(default=Decimal("25"), ge=0)
    reconcile_interval_seconds: float = Field(default=15, gt=0)
    heartbeat_interval_seconds: float = Field(default=5, gt=0)
    email_digest_seconds: int = Field(default=1800, gt=0)
    event_queue_capacity: int = Field(default=5000, gt=0)
    raw_batch_size: int = Field(default=500, gt=0)
    decision_policy: Literal["legacy-v30", "action-value-v1"] = "legacy-v30"
    economic_model_path: str | None = None
    economic_model_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    latency_grace_seconds: float = Field(default=0, ge=0)
    catastrophic_stop_bps: Decimal | None = Field(default=None, gt=0, lt=10000)

    @model_validator(mode="after")
    def validate_economic_policy(self) -> Self:
        if (self.economic_model_path is None) != (self.economic_model_sha256 is None):
            raise ValueError("an economic artifact requires both its path and immutable hash")
        if self.decision_policy == "action-value-v1":
            if self.catastrophic_stop_bps is None:
                raise ValueError("the action-value policy requires an explicit catastrophic stop")
            if self.latency_grace_seconds > self.feature_horizon_seconds:
                raise ValueError("exit grace cannot outlast the prediction horizon")
        return self

    @property
    def maximum_quote_age_seconds(self) -> float:
        return (
            min(self.feature_horizon_seconds, self.decision_interval_seconds)
            if self.decision_policy == "action-value-v1"
            else self.feature_horizon_seconds
        )

    @property
    def prediction_lifetime_seconds(self) -> float:
        return self.feature_horizon_seconds + self.latency_grace_seconds

    @field_validator("symbols")
    @classmethod
    def validate_symbols(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(crypto_symbol(symbol) for symbol in value)
        if not normalized or len(set(normalized)) != len(normalized):
            raise ValueError("a nonempty, unique crypto universe is required")
        return normalized

    @property
    def identity(self) -> str:
        return sha256(self.model_dump_json().encode()).hexdigest()

    def policy_description(self) -> dict[str, Any]:
        return {
            "profile": self.profile,
            "paper_only": True,
            "autonomous": True,
            "daily_approval_required": False,
            "qualification_required": False,
            "shadow_required": False,
            "entry_window": "24/7 crypto",
            "paper_daily_loss_limit": None,
            "paper_drawdown_limit": None,
            "paper_exposure_limit": None,
            "paper_daily_trade_limit": None,
            "news_risk_veto": False,
            "macro_veto": False,
            "automatic_loss_kill": False,
            "catastrophic_stop": (
                {"bps": str(self.catastrophic_stop_bps), "basis": "local owned-position hard stop"}
                if self.decision_policy == "action-value-v1"
                else None
            ),
            "decision_policy": self.decision_policy,
            "prospective_economic_gate": self.decision_policy == "action-value-v1",
            "uncalibrated_or_unprofitable_action": "NO_TRADE",
            "economic_model_sha256": self.economic_model_sha256,
            "order_notional_is_strategy_parameter_not_risk_ceiling": True,
            "technical_requirements": [
                "paper endpoint and pinned account",
                "current valid market state",
                "singleton ownership and durable client-order identities",
                "broker-supported orders and confirmed owned exits",
                "reconciliation and automatic recovery",
            ],
            "profitability_validated": False,
        }


class ScalpQuote(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    symbol: str
    exchange_at: AwareDatetime
    exchange_time_ns: int = Field(gt=0)
    received_at: AwareDatetime
    bid: Decimal = Field(gt=0)
    ask: Decimal = Field(gt=0)
    bid_size: Decimal = Field(ge=0)
    ask_size: Decimal = Field(ge=0)
    venue: Literal["alpaca-crypto-us"] = "alpaca-crypto-us"

    @model_validator(mode="after")
    def validate_quote(self) -> Self:
        crypto_symbol(self.symbol)
        if self.ask < self.bid or self.exchange_at > self.received_at:
            raise ValueError("crossed or future-dated quote is not executable market evidence")
        return self


class ScalpSignal(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    decision_id: str = Field(min_length=1, max_length=128)
    symbol: str
    observed_at: AwareDatetime
    action: Literal["buy", "sell", "hold"]
    family: Literal["momentum", "reversion", "none"]
    score: float
    reasons: tuple[str, ...]
    quote: ScalpQuote
    features: dict[str, float | int | str | None]
    estimated_round_trip_cost_bps: float = Field(ge=0)
    expected_net_edge_bps: float | None = None
    economics: EconomicDecision | None = None
    timing: dict[str, int | float | str | None] = Field(default_factory=dict)
    profitability_validated: bool = False

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        if self.symbol != self.quote.symbol or self.quote.received_at > self.observed_at:
            raise ValueError("signal must use its own symbol's already-received quote")
        if self.economics is not None and (
            self.economics.symbol != self.symbol
            or self.economics.family != self.family
            or self.expected_net_edge_bps != self.economics.expected_net_edge_bps
            or self.profitability_validated != self.economics.profitability_validated
        ):
            raise ValueError("signal economics must match its symbol, family and net edge")
        if self.economics is None and self.profitability_validated:
            raise ValueError("profitability cannot be validated without an economic decision")
        return self


class ScalpInventory(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    symbol: str
    quantity: Decimal = Field(ge=0)
    opened_at: AwareDatetime
    opening_vwap: Decimal = Field(gt=0)
    family: Literal["momentum", "reversion", "none"] = "none"
    exit_due_at: AwareDatetime | None = None
