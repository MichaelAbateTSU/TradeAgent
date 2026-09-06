from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from tradeagent.alpaca_stream import MarketQuote, MarketTrade
from tradeagent.config import AppConfig
from tradeagent.domain import MarketBar
from tradeagent.intraday import IntradayDataGapError
from tradeagent.intraday_strategy import RegimeFilteredMomentumStrategy
from tradeagent.news import NewsContextService
from tradeagent.persistence import ProductionRepository
from tradeagent.runtime import ShadowAuditProcessor
from tradeagent.universe import UniverseFrame


class SynchronizedFiveMinuteBuilder:
    def __init__(self, symbols: tuple[str, ...]) -> None:
        self._symbols = tuple(sorted(symbols))
        self._current: dict[str, tuple[datetime, list[MarketBar]]] = {}
        self._completed: dict[datetime, dict[str, MarketBar]] = {}

    def add(self, bar: MarketBar) -> UniverseFrame | None:
        if bar.symbol not in self._symbols:
            raise ValueError(f"unexpected shadow symbol {bar.symbol}")
        bucket = bar.timestamp.replace(
            minute=bar.timestamp.minute - bar.timestamp.minute % 5,
            second=0,
            microsecond=0,
        )
        current = self._current.get(bar.symbol)
        if current is None:
            self._current[bar.symbol] = (bucket, [bar])
            return None
        current_bucket, bars = current
        if bucket == current_bucket:
            if bars and bar.timestamp <= bars[-1].timestamp:
                raise IntradayDataGapError("stream bars must be unique and chronological")
            bars.append(bar)
            return None
        completed = self._complete(current_bucket, bars)
        self._current[bar.symbol] = (bucket, [bar])
        by_symbol = self._completed.setdefault(completed.timestamp, {})
        by_symbol[bar.symbol] = completed
        if set(by_symbol) != set(self._symbols):
            return None
        frame = UniverseFrame(
            timestamp=completed.timestamp,
            bars=tuple(by_symbol[symbol] for symbol in self._symbols),
        )
        del self._completed[completed.timestamp]
        return frame

    @staticmethod
    def _complete(bucket: datetime, bars: list[MarketBar]) -> MarketBar:
        expected = [bucket + timedelta(minutes=index) for index in range(5)]
        if [bar.timestamp for bar in bars] != expected:
            raise IntradayDataGapError(f"incomplete five-minute bucket at {bucket}")
        return MarketBar(
            symbol=bars[0].symbol,
            timestamp=bucket + timedelta(minutes=5),
            open=bars[0].open,
            high=max(bar.high for bar in bars),
            low=min(bar.low for bar in bars),
            close=bars[-1].close,
            volume=sum((bar.volume for bar in bars), Decimal(0)),
        )


@dataclass
class _ShadowState:
    nav: Decimal
    closes: dict[str, Decimal]
    targets: dict[str, Decimal]
    pending_turnover: Decimal = Decimal(0)


class LiveShadowDecisionProcessor:
    def __init__(
        self,
        config: AppConfig,
        repository: ProductionRepository,
        strategy: RegimeFilteredMomentumStrategy,
        *,
        symbols: tuple[str, ...],
        news_context: NewsContextService | None = None,
    ) -> None:
        self._config = config
        self._repository = repository
        self._strategy = strategy
        self._audit = ShadowAuditProcessor(repository)
        self._builder = SynchronizedFiveMinuteBuilder(symbols)
        self._news_context = news_context
        self._state = _ShadowState(
            nav=config.broker.starting_cash,
            closes={},
            targets={symbol: Decimal(0) for symbol in symbols},
        )

    async def on_quote(self, quote: MarketQuote, *, can_enter: bool) -> None:
        self._strategy.on_quote(quote)
        await self._audit.on_quote(quote, can_enter=can_enter)

    async def on_bar(self, bar: MarketBar, *, can_enter: bool) -> None:
        await self._audit.on_bar(bar, can_enter=can_enter)
        frame = self._builder.add(bar)
        if frame is None:
            return
        self._record_prior_outcome(frame)
        intent = self._strategy.on_frame(frame)
        next_targets = dict(intent.target_weights)
        news = {}
        if self._news_context is not None:
            for symbol, target in tuple(next_targets.items()):
                context = self._news_context.context(symbol, frame.timestamp)
                news[symbol] = context.model_dump(mode="json")
                if target > 0 and not context.permits_entry:
                    next_targets[symbol] = Decimal(0)
        turnover = sum(
            (
                abs(next_targets.get(symbol, Decimal(0)) - self._state.targets[symbol])
                for symbol in self._state.targets
            ),
            Decimal(0),
        )
        self._state.pending_turnover = turnover
        self._state.targets = next_targets
        self._state.closes = {bar.symbol: bar.close for bar in frame.bars}
        self._repository.append_event(
            "shadow_decision",
            {
                "strategy_id": intent.strategy_id,
                "targets": {key: str(value) for key, value in next_targets.items()},
                "rationale": intent.rationale,
                "news_context": news,
                "shadow_nav": str(self._state.nav),
                "execution_enabled": can_enter,
                "signal_at": frame.timestamp.isoformat(),
                "simulated_submission_at": (
                    frame.timestamp + timedelta(minutes=self._config.intraday.primary_bar_minutes)
                ).isoformat(),
                "cost_model_status": "provisional",
                "cost_model_feed": "iex-realtime-plus-estimated-slippage",
            },
            occurred_at=frame.timestamp,
            trace_id=f"shadow-decision:{frame.timestamp.isoformat()}",
        )

    async def on_trade(self, trade: MarketTrade, *, can_enter: bool) -> None:
        await self._audit.on_trade(trade, can_enter=can_enter)

    def _record_prior_outcome(self, frame: UniverseFrame) -> None:
        if not self._state.closes:
            return
        gross_return = sum(
            (
                self._state.targets.get(bar.symbol, Decimal(0))
                * (bar.close / self._state.closes[bar.symbol] - Decimal(1))
                for bar in frame.bars
            ),
            Decimal(0),
        )
        cost_bps = (
            self._config.broker.slippage_bps
            + self._config.broker.spread_bps / Decimal(2)
            + self._config.broker.commission_bps
        )
        cost = self._state.nav * self._state.pending_turnover * cost_bps / Decimal(10_000)
        pnl = self._state.nav * gross_return - cost
        self._state.nav += pnl
        self._repository.append_event(
            "shadow_outcome",
            {
                "gross_return": str(gross_return),
                "modeled_cost": str(cost),
                "pnl": str(pnl),
                "shadow_nav": str(self._state.nav),
            },
            occurred_at=frame.timestamp,
            trace_id=f"shadow-outcome:{frame.timestamp.isoformat()}",
        )
