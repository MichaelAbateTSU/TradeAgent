from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True)


class Side(StrEnum):
    BUY = "buy"
    SELL = "sell"


class OrderType(StrEnum):
    MARKET = "market"


class OrderStatus(StrEnum):
    FILLED = "filled"
    REJECTED = "rejected"


class MarketBar(FrozenModel):
    symbol: str = Field(min_length=1, max_length=32)
    timestamp: datetime
    open: Decimal = Field(gt=0)
    high: Decimal = Field(gt=0)
    low: Decimal = Field(gt=0)
    close: Decimal = Field(gt=0)
    volume: Decimal = Field(ge=0)

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return value.strip().upper()

    @field_validator("timestamp")
    @classmethod
    def require_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_range(self) -> MarketBar:
        if self.high < max(self.open, self.close) or self.low > min(self.open, self.close):
            raise ValueError("OHLC values are inconsistent")
        if self.low > self.high:
            raise ValueError("low cannot exceed high")
        return self


class TradeIntent(FrozenModel):
    strategy_id: str = Field(min_length=1)
    symbol: str = Field(min_length=1, max_length=32)
    target_weight: Decimal = Field(ge=0, le=1)
    generated_at: datetime
    rationale: str = Field(min_length=1, max_length=500)

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return value.strip().upper()


class OrderRequest(FrozenModel):
    client_order_id: str = Field(min_length=1, max_length=128)
    decision_id: str = Field(min_length=1, max_length=64)
    strategy_id: str = Field(min_length=1)
    symbol: str = Field(min_length=1, max_length=32)
    side: Side
    quantity: Decimal = Field(gt=0)
    order_type: OrderType = OrderType.MARKET
    submitted_at: datetime

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return value.strip().upper()


class Fill(FrozenModel):
    fill_id: str
    client_order_id: str
    symbol: str
    side: Side
    quantity: Decimal = Field(gt=0)
    price: Decimal = Field(gt=0)
    commission: Decimal = Field(ge=0)
    filled_at: datetime

    @property
    def notional(self) -> Decimal:
        return self.quantity * self.price


class Position(FrozenModel):
    symbol: str
    quantity: Decimal
    average_price: Decimal = Field(ge=0)
    market_price: Decimal = Field(gt=0)
    realized_pnl: Decimal

    @property
    def market_value(self) -> Decimal:
        return self.quantity * self.market_price

    @property
    def unrealized_pnl(self) -> Decimal:
        return self.quantity * (self.market_price - self.average_price)


class AccountSnapshot(FrozenModel):
    as_of: datetime
    cash: Decimal
    equity: Decimal = Field(gt=0)
    day_start_equity: Decimal = Field(gt=0)
    high_watermark: Decimal = Field(gt=0)
    positions: tuple[Position, ...] = ()

    def position_for(self, symbol: str) -> Position | None:
        normalized = symbol.strip().upper()
        return next(
            (position for position in self.positions if position.symbol == normalized),
            None,
        )

    @property
    def gross_exposure(self) -> Decimal:
        return sum((abs(position.market_value) for position in self.positions), Decimal(0))

    @property
    def daily_return(self) -> Decimal:
        return (self.equity / self.day_start_equity) - Decimal(1)

    @property
    def drawdown(self) -> Decimal:
        return (self.equity / self.high_watermark) - Decimal(1)


class RiskDecision(FrozenModel):
    approved: bool
    codes: tuple[str, ...] = ()
    message: str
    projected_gross_exposure: Decimal = Decimal(0)


class EngineStep(FrozenModel):
    timestamp: datetime
    symbol: str
    intent: TradeIntent | None = None
    order: OrderRequest | None = None
    risk: RiskDecision | None = None
    fill: Fill | None = None
    equity: Decimal


class BacktestReport(FrozenModel):
    mode: str = "paper"
    symbol: str
    started_at: datetime
    ended_at: datetime
    starting_equity: Decimal
    ending_equity: Decimal
    total_return: Decimal
    max_drawdown: Decimal
    orders: int
    fills: int
    rejected_orders: int
    final_positions: tuple[Position, ...]


JsonObject = dict[str, Any]
