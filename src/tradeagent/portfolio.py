from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime
from decimal import ROUND_DOWN, Decimal
from hashlib import sha256
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from tradeagent.broker import PaperBroker
from tradeagent.config import AppConfig, config_fingerprint
from tradeagent.domain import MarketBar, OrderRequest, Position, Side
from tradeagent.ledger import SQLiteLedger
from tradeagent.metrics import performance_metrics
from tradeagent.risk import RiskEngine
from tradeagent.universe import UniverseFrame


class PortfolioIntent(BaseModel):
    model_config = ConfigDict(frozen=True)

    strategy_id: str = Field(min_length=1)
    timestamp: datetime
    target_weights: dict[str, Decimal]
    rationale: str = Field(min_length=1, max_length=1_000)

    @field_validator("target_weights")
    @classmethod
    def normalize_weights(cls, weights: Mapping[str, Decimal]) -> dict[str, Decimal]:
        return {symbol.strip().upper(): Decimal(weight) for symbol, weight in weights.items()}

    @model_validator(mode="after")
    def validate_weights(self) -> PortfolioIntent:
        if any(weight < 0 or weight > 1 for weight in self.target_weights.values()):
            raise ValueError("portfolio target weights must be between zero and one")
        if sum(self.target_weights.values(), Decimal(0)) > Decimal(1):
            raise ValueError("portfolio target weights cannot exceed one")
        return self


class PortfolioStrategy(Protocol):
    @property
    def strategy_id(self) -> str: ...

    def on_frame(self, frame: UniverseFrame) -> PortfolioIntent | None: ...


class PortfolioBacktestReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    mode: str = "paper"
    strategy_id: str
    symbols: tuple[str, ...]
    started_at: datetime
    ended_at: datetime
    starting_equity: Decimal
    ending_equity: Decimal
    total_return: Decimal
    annualized_return: Decimal
    annualized_volatility: Decimal
    sharpe_ratio: Decimal | None
    sortino_ratio: Decimal | None
    calmar_ratio: Decimal | None
    max_drawdown: Decimal
    turnover: Decimal
    orders: int
    fills: int
    rejected_orders: int
    final_positions: tuple[Position, ...]


class PortfolioEngine:
    """Synchronized multi-asset target engine using the existing hard-risk gateway."""

    def __init__(
        self,
        config: AppConfig,
        strategy: PortfolioStrategy,
        broker: PaperBroker,
        risk: RiskEngine,
        ledger: SQLiteLedger,
    ) -> None:
        self._config = config
        self._strategy = strategy
        self._broker = broker
        self._risk = risk
        self._ledger = ledger
        self._orders = 0
        self._rejections = 0
        self._config_fingerprint = config_fingerprint(config)

    def run(self, frames: Iterable[UniverseFrame]) -> PortfolioBacktestReport:
        first_frame: UniverseFrame | None = None
        last_frame: UniverseFrame | None = None
        equities: list[Decimal] = []
        starting_equity: Decimal | None = None
        starting_fill_count = self._broker.fill_count
        starting_notional = sum((fill.notional for fill in self._broker.fills), Decimal(0))
        symbols: tuple[str, ...] = ()

        for frame in frames:
            if first_frame is None:
                first_frame = frame
                symbols = tuple(sorted(bar.symbol for bar in frame.bars))
                starting_equity = self._broker.account(frame.timestamp).equity
                equities.append(starting_equity)
            elif tuple(sorted(bar.symbol for bar in frame.bars)) != symbols:
                raise ValueError("portfolio frame symbols changed during the run")
            last_frame = frame

            for bar in frame.bars:
                self._broker.mark(bar)
            intent = self._strategy.on_frame(frame)
            if intent is not None:
                unknown = set(intent.target_weights) - set(symbols)
                if unknown:
                    raise ValueError(
                        f"intent contains symbols outside the universe: {sorted(unknown)}"
                    )
                self._execute_targets(frame, intent)
            self._ledger.append(
                "portfolio_progress",
                {
                    "timestamp": frame.timestamp.isoformat(),
                    "strategy_id": self._strategy.strategy_id,
                    "config_fingerprint": self._config_fingerprint,
                },
                occurred_at=frame.timestamp,
                trace_id=self._frame_trace_id(frame),
            )
            equities.append(self._broker.account(frame.timestamp).equity)

        if first_frame is None or last_frame is None or starting_equity is None:
            raise ValueError("at least one universe frame is required")
        final_account = self._broker.account(last_frame.timestamp)
        self._ledger.append(
            "broker_checkpoint",
            self._broker.export_state(),
            occurred_at=last_frame.timestamp,
            trace_id=self._frame_trace_id(last_frame),
        )
        traded_notional = (
            sum((fill.notional for fill in self._broker.fills), Decimal(0)) - starting_notional
        )
        periods_per_year = (
            252 * (390 // self._config.intraday.primary_bar_minutes)
            if self._config.intraday.enabled
            else 252
        )
        metrics = performance_metrics(
            equities,
            traded_notional,
            periods_per_year=periods_per_year,
        )
        return PortfolioBacktestReport(
            strategy_id=self._strategy.strategy_id,
            symbols=symbols,
            started_at=first_frame.timestamp,
            ended_at=last_frame.timestamp,
            starting_equity=starting_equity,
            ending_equity=final_account.equity,
            total_return=metrics["total_return"],
            annualized_return=metrics["annualized_return"],
            annualized_volatility=metrics["annualized_volatility"],
            sharpe_ratio=metrics["sharpe_ratio"],
            sortino_ratio=metrics["sortino_ratio"],
            calmar_ratio=metrics["calmar_ratio"],
            max_drawdown=metrics["max_drawdown"],
            turnover=metrics["turnover"],
            orders=self._orders,
            fills=self._broker.fill_count - starting_fill_count,
            rejected_orders=self._rejections,
            final_positions=final_account.positions,
        )

    def _execute_targets(
        self,
        frame: UniverseFrame,
        intent: PortfolioIntent,
    ) -> None:
        self._ledger.append(
            "portfolio_intent",
            intent,
            occurred_at=frame.timestamp,
            trace_id=self._frame_trace_id(frame),
        )
        for bar in sorted(frame.bars, key=lambda item: item.symbol):
            account = self._broker.account(frame.timestamp)
            target_weight = intent.target_weights.get(bar.symbol, Decimal(0))
            target_notional = account.equity * target_weight
            if self._config.intraday.enabled and target_weight > 0:
                target_notional = min(
                    target_notional,
                    self._config.intraday.maximum_order_notional,
                )
                if target_notional < self._config.intraday.minimum_order_notional:
                    target_notional = Decimal(0)
                target_quantity = (target_notional / bar.close).quantize(
                    Decimal("0.000001"),
                    rounding=ROUND_DOWN,
                )
            else:
                target_quantity = (target_notional / bar.close).to_integral_value(
                    rounding=ROUND_DOWN
                )
            current = account.position_for(bar.symbol)
            current_quantity = current.quantity if current is not None else Decimal(0)
            difference = target_quantity - current_quantity
            if difference == 0:
                continue
            if (
                self._config.intraday.enabled
                and target_quantity > 0
                and abs(difference) * bar.close < self._config.intraday.minimum_order_notional
            ):
                continue
            side = Side.BUY if difference > 0 else Side.SELL
            decision_id = self._decision_id(intent, bar)
            strategy_token = sha256(intent.strategy_id.encode()).hexdigest()[:10]
            order = OrderRequest(
                client_order_id=f"ta-{strategy_token}:{decision_id}:1",
                decision_id=decision_id,
                strategy_id=intent.strategy_id,
                symbol=bar.symbol,
                side=side,
                quantity=abs(difference),
                submitted_at=frame.timestamp,
            )
            self._orders += 1
            decision = self._risk.evaluate(
                order,
                bar,
                account,
                observed_at=frame.timestamp,
                trading_enabled=self._config.trading_enabled,
            )
            self._ledger.append(
                "risk_decision",
                decision,
                occurred_at=frame.timestamp,
                trace_id=decision_id,
            )
            if not decision.approved:
                self._rejections += 1
                continue
            self._ledger.append(
                "order_submitted",
                order,
                occurred_at=frame.timestamp,
                trace_id=decision_id,
            )
            fill = self._broker.submit(order, bar)
            self._ledger.append(
                "order_filled",
                fill,
                occurred_at=frame.timestamp,
                trace_id=decision_id,
            )

    @staticmethod
    def _decision_id(intent: PortfolioIntent, bar: MarketBar) -> str:
        raw = (
            f"{intent.strategy_id}|{bar.symbol}|{intent.timestamp}|"
            f"{intent.target_weights.get(bar.symbol, Decimal(0))}"
        )
        return sha256(raw.encode()).hexdigest()[:24]

    @staticmethod
    def _frame_trace_id(frame: UniverseFrame) -> str:
        raw = f"{frame.timestamp.isoformat()}|{','.join(bar.symbol for bar in frame.bars)}"
        return sha256(raw.encode()).hexdigest()[:24]
