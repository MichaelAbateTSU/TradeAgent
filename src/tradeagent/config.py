from __future__ import annotations

from decimal import Decimal
from hashlib import sha256
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class RiskLimits(BaseModel):
    model_config = ConfigDict(frozen=True)

    max_gross_exposure: Decimal = Field(default=Decimal("0.50"), gt=0, le=1)
    max_position_exposure: Decimal = Field(default=Decimal("0.05"), gt=0, le=1)
    max_order_exposure: Decimal = Field(default=Decimal("0.02"), gt=0, le=1)
    max_positions: int = Field(default=10, gt=0)
    max_daily_loss: Decimal = Field(default=Decimal("0.01"), gt=0, lt=1)
    max_drawdown: Decimal = Field(default=Decimal("0.015"), gt=0, lt=1)
    max_orders_per_hour: int = Field(default=20, gt=0)
    max_data_age_seconds: int = Field(default=120, gt=0)
    max_volume_participation: Decimal = Field(default=Decimal("0.01"), gt=0, le=1)
    allow_shorting: bool = False
    allow_leverage: bool = False

    @model_validator(mode="after")
    def validate_exposure_hierarchy(self) -> RiskLimits:
        if self.max_order_exposure > self.max_position_exposure:
            raise ValueError("max_order_exposure cannot exceed max_position_exposure")
        if self.max_position_exposure > self.max_gross_exposure:
            raise ValueError("max_position_exposure cannot exceed max_gross_exposure")
        return self


class BrokerConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    starting_cash: Decimal = Field(default=Decimal("100000"), gt=0)
    slippage_bps: Decimal = Field(default=Decimal("2"), ge=0, le=100)
    spread_bps: Decimal = Field(default=Decimal("1"), ge=0, le=100)
    commission_bps: Decimal = Field(default=Decimal("1"), ge=0, le=100)
    max_volume_participation: Decimal = Field(default=Decimal("0.01"), gt=0, le=1)


class StrategyConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    strategy_id: str = "sma-crossover-v1"
    fast_window: int = Field(default=20, ge=2)
    slow_window: int = Field(default=50, ge=3)
    volatility_window: int = Field(default=20, ge=2)
    target_annual_volatility: Decimal = Field(default=Decimal("0.10"), gt=0, le=1)
    mean_reversion_window: int = Field(default=20, ge=3)
    mean_reversion_entry_z: Decimal = Field(default=Decimal("-1.5"), lt=0)
    mean_reversion_exit_z: Decimal = Field(default=Decimal("-0.25"), le=0)
    execution_delay_bars: int = Field(default=1, ge=0, le=10)
    target_weight: Decimal = Field(default=Decimal("0.02"), gt=0, le=1)

    @model_validator(mode="after")
    def validate_windows(self) -> StrategyConfig:
        if self.fast_window >= self.slow_window:
            raise ValueError("fast_window must be less than slow_window")
        if self.mean_reversion_entry_z >= self.mean_reversion_exit_z:
            raise ValueError("mean_reversion_entry_z must be below mean_reversion_exit_z")
        return self


class AppConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="TRADEAGENT_",
        env_nested_delimiter="__",
        env_file=".env",
        env_file_encoding="utf-8",
        frozen=True,
        extra="ignore",
    )

    mode: str = Field(default="paper", pattern="^paper$")
    trading_enabled: bool = True
    database_path: Path = Path("data/tradeagent.db")
    risk: RiskLimits = RiskLimits()
    broker: BrokerConfig = BrokerConfig()
    strategy: StrategyConfig = StrategyConfig()

    @model_validator(mode="after")
    def strategy_fits_risk(self) -> AppConfig:
        if self.strategy.target_weight > self.risk.max_order_exposure:
            raise ValueError("strategy target_weight cannot exceed max_order_exposure")
        return self


def config_fingerprint(config: AppConfig) -> str:
    return sha256(config.model_dump_json().encode()).hexdigest()
