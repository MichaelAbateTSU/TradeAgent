from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from decimal import ROUND_DOWN, Decimal
from hashlib import sha256

from tradeagent.broker import PaperBroker
from tradeagent.config import AppConfig, config_fingerprint
from tradeagent.domain import (
    BacktestReport,
    EngineStep,
    MarketBar,
    OrderRequest,
    Side,
    TradeIntent,
)
from tradeagent.ledger import SQLiteLedger
from tradeagent.metrics import performance_metrics
from tradeagent.risk import RiskEngine
from tradeagent.strategy import Strategy


class TradingEngine:
    def __init__(
        self,
        config: AppConfig,
        strategy: Strategy,
        broker: PaperBroker,
        risk: RiskEngine,
        ledger: SQLiteLedger,
    ) -> None:
        self._config = config
        self._strategy = strategy
        self._broker = broker
        self._risk = risk
        self._ledger = ledger
        self._config_fingerprint = config_fingerprint(config)
        self._orders = 0
        self._rejections = 0

    def process_bar(self, bar: MarketBar, *, observed_at: datetime | None = None) -> EngineStep:
        observation_time = observed_at or bar.timestamp
        self._broker.mark(bar)
        intent = self._strategy.on_bar(bar)
        account = self._broker.account(bar.timestamp)
        if intent is None:
            step = EngineStep(
                timestamp=bar.timestamp,
                symbol=bar.symbol,
                equity=account.equity,
            )
            self._record_progress(bar, self._bar_trace_id(bar))
            return step

        trace_id = self._decision_id(intent)
        self._ledger.append(
            "trade_intent",
            intent,
            occurred_at=bar.timestamp,
            trace_id=trace_id,
        )
        order = self._plan_order(intent, account.equity, bar)
        if order is None:
            step = EngineStep(
                timestamp=bar.timestamp,
                symbol=bar.symbol,
                intent=intent,
                equity=account.equity,
            )
            self._record_progress(bar, trace_id)
            return step

        self._orders += 1
        decision = self._risk.evaluate(
            order,
            bar,
            account,
            observed_at=observation_time,
            trading_enabled=self._config.trading_enabled,
        )
        self._ledger.append(
            "risk_decision",
            decision,
            occurred_at=bar.timestamp,
            trace_id=trace_id,
        )
        if not decision.approved:
            self._rejections += 1
            step = EngineStep(
                timestamp=bar.timestamp,
                symbol=bar.symbol,
                intent=intent,
                order=order,
                risk=decision,
                equity=account.equity,
            )
            self._record_progress(bar, trace_id)
            return step

        self._ledger.append(
            "order_submitted",
            order,
            occurred_at=bar.timestamp,
            trace_id=trace_id,
        )
        fill = self._broker.submit(order, bar)
        self._ledger.append(
            "order_filled",
            fill,
            occurred_at=bar.timestamp,
            trace_id=trace_id,
        )
        updated_account = self._broker.account(bar.timestamp)
        self._ledger.append(
            "broker_checkpoint",
            self._broker.export_state(),
            occurred_at=bar.timestamp,
            trace_id=trace_id,
        )
        step = EngineStep(
            timestamp=bar.timestamp,
            symbol=bar.symbol,
            intent=intent,
            order=order,
            risk=decision,
            fill=fill,
            equity=updated_account.equity,
        )
        self._record_progress(bar, trace_id)
        return step

    def run(self, bars: Iterable[MarketBar]) -> BacktestReport:
        first_bar: MarketBar | None = None
        last_bar: MarketBar | None = None
        equities: list[Decimal] = []
        starting_equity: Decimal | None = None
        starting_traded_notional = sum((fill.notional for fill in self._broker.fills), Decimal(0))
        for bar in bars:
            if first_bar is None:
                first_bar = bar
                starting_equity = self._broker.account(bar.timestamp).equity
                equities.append(starting_equity)
            last_bar = bar
            step = self.process_bar(bar)
            equities.append(step.equity)

        if first_bar is None or last_bar is None or starting_equity is None:
            raise ValueError("at least one market bar is required")
        final_account = self._broker.account(last_bar.timestamp)
        self._ledger.append(
            "broker_checkpoint",
            self._broker.export_state(),
            occurred_at=last_bar.timestamp,
            trace_id=self._bar_trace_id(last_bar),
        )
        traded_notional = (
            sum((fill.notional for fill in self._broker.fills), Decimal(0))
            - starting_traded_notional
        )
        metrics = performance_metrics(equities, traded_notional)
        return BacktestReport(
            symbol=first_bar.symbol,
            started_at=first_bar.timestamp,
            ended_at=last_bar.timestamp,
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
            fills=self._broker.fill_count,
            rejected_orders=self._rejections,
            final_positions=final_account.positions,
        )

    def _record_progress(self, bar: MarketBar, trace_id: str) -> None:
        self._ledger.append(
            "engine_progress",
            {
                "symbol": bar.symbol,
                "timestamp": bar.timestamp.isoformat(),
                "config_fingerprint": self._config_fingerprint,
            },
            occurred_at=bar.timestamp,
            trace_id=trace_id,
        )

    def _plan_order(
        self, intent: TradeIntent, equity: Decimal, bar: MarketBar
    ) -> OrderRequest | None:
        current = self._broker.account(bar.timestamp).position_for(intent.symbol)
        current_quantity = current.quantity if current is not None else Decimal(0)
        target_notional = equity * intent.target_weight
        target_quantity = (target_notional / bar.close).to_integral_value(rounding=ROUND_DOWN)
        difference = target_quantity - current_quantity
        if difference == 0:
            return None
        side = Side.BUY if difference > 0 else Side.SELL
        decision_id = self._decision_id(intent)
        strategy_token = sha256(intent.strategy_id.encode()).hexdigest()[:10]
        client_order_id = f"ta-{strategy_token}:{decision_id}:1"
        return OrderRequest(
            client_order_id=client_order_id,
            decision_id=decision_id,
            strategy_id=intent.strategy_id,
            symbol=intent.symbol,
            side=side,
            quantity=abs(difference),
            submitted_at=bar.timestamp,
        )

    @staticmethod
    def _decision_id(intent: TradeIntent) -> str:
        raw = (
            f"{intent.strategy_id}|{intent.symbol}|{intent.generated_at.isoformat()}|"
            f"{intent.target_weight}"
        )
        return sha256(raw.encode()).hexdigest()[:24]

    @staticmethod
    def _bar_trace_id(bar: MarketBar) -> str:
        raw = f"{bar.symbol}|{bar.timestamp.isoformat()}|{bar.close}"
        return sha256(raw.encode()).hexdigest()[:24]
