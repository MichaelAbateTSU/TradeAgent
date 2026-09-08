from __future__ import annotations

from collections.abc import Callable, Sequence
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from uuid import uuid4

from tradeagent.alpaca_stream import MarketQuote, MarketTrade, ReceivedStreamEvent
from tradeagent.config import AppConfig
from tradeagent.domain import MarketBar
from tradeagent.intraday_strategy import RegimeFilteredMomentumStrategy
from tradeagent.news import NewsContextService
from tradeagent.persistence import ProductionRepository
from tradeagent.runtime import ShadowAuditProcessor
from tradeagent.shadow_recorder import ShadowDecisionBatchResult, raw_already_recorded
from tradeagent.universe import UniverseFrame


class SynchronizedFiveMinuteBuilder:
    def __init__(
        self,
        symbols: tuple[str, ...],
        *,
        on_unusable: Callable[[dict[str, object]], None] | None = None,
    ) -> None:
        self._symbols = tuple(sorted(set(symbols)))
        if not self._symbols:
            raise ValueError("a synchronized frame requires at least one symbol")
        self._on_unusable = on_unusable
        self._bucket: datetime | None = None
        self._bars: dict[str, dict[datetime, MarketBar]] = {}
        self._emitted = False
        self._invalid = False
        self.unusable_frames = 0
        self.duplicate_minutes = 0
        self.last_unusable: dict[str, object] | None = None

    def add(self, bar: MarketBar) -> UniverseFrame | None:
        if bar.symbol not in self._symbols:
            raise ValueError(f"unexpected shadow symbol {bar.symbol}")
        bucket = bar.timestamp.replace(
            minute=bar.timestamp.minute - bar.timestamp.minute % 5,
            second=0,
            microsecond=0,
        )
        if self._bucket is not None and bucket < self._bucket:
            self._unusable(bucket, "late_bar_after_bucket_closed", bar.timestamp)
            return None
        if self._bucket is not None and bucket > self._bucket:
            if not self._emitted:
                self._unusable(self._bucket, "incomplete_five_minute_bucket", bar.timestamp)
            if bucket > self._bucket + timedelta(minutes=5):
                self._unusable(
                    self._bucket + timedelta(minutes=5),
                    "missing_five_minute_buckets",
                    bar.timestamp,
                    missing_until=bucket.isoformat(),
                    missing_bucket_count=int((bucket - self._bucket).total_seconds() // 300) - 1,
                )
            self._bars = {}
            self._emitted = False
            self._invalid = False
        self._bucket = bucket
        by_minute = self._bars.setdefault(bar.symbol, {})
        prior = by_minute.get(bar.timestamp)
        if prior is not None:
            if prior == bar:
                self.duplicate_minutes += 1
            else:
                self._invalid = True
                self._unusable(
                    bucket,
                    "conflicting_minute_bar",
                    bar.timestamp,
                    incoming_bar=bar.model_dump(mode="json"),
                    previous_bar=prior.model_dump(mode="json"),
                )
            return None
        if bar.timestamp.second != 0 or bar.timestamp.microsecond != 0:
            self._invalid = True
            self._unusable(bucket, "unaligned_minute_bar", bar.timestamp)
            return None
        by_minute[bar.timestamp] = bar
        expected = tuple(bucket + timedelta(minutes=index) for index in range(5))
        if (
            self._emitted
            or self._invalid
            or any(set(self._bars.get(symbol, {})) != set(expected) for symbol in self._symbols)
        ):
            return None
        # Emit only actual, complete five-minute evidence. A missing symbol/minute is
        # never zero-filled, carried forward, or waited on without a bounded watermark.
        frame = UniverseFrame(
            timestamp=bucket + timedelta(minutes=5),
            bars=tuple(
                self._complete(bucket, [self._bars[symbol][minute] for minute in expected])
                for symbol in self._symbols
            ),
        )
        self._emitted = True
        return frame

    @property
    def pending_minutes(self) -> int:
        return sum(len(minutes) for minutes in self._bars.values())

    def finish(self, *, observed_at: datetime) -> None:
        if self._bucket is not None and not self._emitted:
            self._unusable(
                self._bucket,
                "incomplete_bucket_at_stop",
                self._bucket,
                stopped_at=observed_at.isoformat(),
            )
        self._bucket = None
        self._bars = {}
        self._emitted = False
        self._invalid = False

    def _unusable(
        self,
        bucket: datetime,
        reason: str,
        discovered_event_at: datetime,
        **details: object,
    ) -> None:
        self.unusable_frames += 1
        expected = tuple(bucket + timedelta(minutes=index) for index in range(5))
        issue: dict[str, object] = {
            "bucket_start": bucket.isoformat(),
            "frame_at": (bucket + timedelta(minutes=5)).isoformat(),
            "reason": reason,
            "discovered_event_at": discovered_event_at.isoformat(),
            "missing_minutes": {
                symbol: [
                    minute.isoformat()
                    for minute in expected
                    if minute not in self._bars.get(symbol, {})
                ]
                for symbol in self._symbols
            },
            "usable": False,
            **details,
        }
        self.last_unusable = issue
        if self._on_unusable is not None:
            self._on_unusable(issue)

    @staticmethod
    def _complete(bucket: datetime, bars: list[MarketBar]) -> MarketBar:
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


@dataclass
class _ShadowSignal:
    symbol: str
    signal_at: datetime
    reference_price: Decimal
    elapsed_frames: int = 0


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
        self._strategy_template = deepcopy(strategy)
        self._audit = ShadowAuditProcessor(repository)
        self._builder = SynchronizedFiveMinuteBuilder(symbols, on_unusable=self._on_unusable_frame)
        self._news_context = news_context
        self._signals: list[_ShadowSignal] = []
        self._evidence_epoch = str(uuid4())
        self._evidence_complete = True
        self._last_frame_at: datetime | None = None
        self._latest_quote_at: dict[str, datetime] = {}
        self._ignored_quotes = 0
        self._batch_quote_rejections: list[dict[str, object]] | None = None
        self._state = _ShadowState(
            nav=config.broker.starting_cash,
            closes={},
            targets={symbol: Decimal(0) for symbol in symbols},
        )

    async def on_quote(self, quote: MarketQuote, *, can_enter: bool) -> None:
        await self._audit.on_quote(quote, can_enter=can_enter)
        prior_at = self._latest_quote_at.get(quote.symbol)
        if prior_at is not None and quote.timestamp < prior_at:
            self._ignored_quotes += 1
            payload: dict[str, object] = {
                "symbol": quote.symbol,
                "event_at": quote.timestamp.isoformat(),
                "latest_accepted_event_at": prior_at.isoformat(),
                "reason": "out_of_order_quote",
                "raw_data_preserved": True,
                "execution_enabled": False,
            }
            if self._batch_quote_rejections is not None:
                self._batch_quote_rejections.append(payload)
                return
            self._repository.append_event(
                "shadow_quote_unusable",
                payload,
                occurred_at=quote.timestamp,
                trace_id=f"shadow-quote-unusable:{uuid4()}",
            )
            return
        self._strategy.on_quote(quote)
        self._latest_quote_at[quote.symbol] = quote.timestamp

    async def on_bar(self, bar: MarketBar, *, can_enter: bool) -> None:
        await self._audit.on_bar(bar, can_enter=can_enter)
        frame = self._builder.add(bar)
        if frame is None:
            return
        self._last_frame_at = frame.timestamp
        self._record_signal_decay(frame)
        self._record_prior_outcome(frame)
        intent = self._strategy.on_frame(frame)
        next_targets = dict(intent.target_weights)
        prior_targets = dict(self._state.targets)
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
        for bar in frame.bars:
            if (
                next_targets.get(bar.symbol, Decimal(0)) > 0
                and prior_targets.get(bar.symbol, Decimal(0)) == 0
            ):
                self._signals.append(
                    _ShadowSignal(
                        symbol=bar.symbol,
                        signal_at=frame.timestamp,
                        reference_price=bar.close,
                    )
                )
        expected_round_trip_cost_bps = Decimal(2) * (
            self._config.broker.slippage_bps
            + self._config.broker.spread_bps / Decimal(2)
            + self._config.broker.commission_bps
        )
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
                "frame_usable": True,
                "evidence_epoch": self._evidence_epoch,
                "cumulative_evidence_complete": self._evidence_complete,
                "cost_model_feed": "iex-realtime-plus-estimated-slippage",
                "expected_round_trip_cost_bps": str(expected_round_trip_cost_bps),
                "expected_return_floor_bps": str(self._config.intraday.minimum_expected_edge_bps),
            },
            occurred_at=frame.timestamp,
            trace_id=f"shadow-decision:{frame.timestamp.isoformat()}",
        )

    async def on_trade(self, trade: MarketTrade, *, can_enter: bool) -> None:
        await self._audit.on_trade(trade, can_enter=can_enter)

    async def on_recorded_batch(
        self, receipts: Sequence[ReceivedStreamEvent]
    ) -> ShadowDecisionBatchResult:
        """Derive from already-committed, worker-freshness-gated raw evidence only."""
        result = ShadowDecisionBatchResult()
        token = raw_already_recorded.set(True)
        self._batch_quote_rejections = []
        try:
            for receipt in receipts:
                event = receipt.event
                try:
                    if isinstance(event, MarketBar):
                        await self.on_bar(event, can_enter=False)
                        result.bars += 1
                    elif isinstance(event, MarketQuote):
                        await self.on_quote(event, can_enter=False)
                        result.quotes += 1
                    else:
                        await self.on_trade(event, can_enter=False)
                        result.trades += 1
                except Exception as exc:
                    result.errors.append(type(exc).__name__)
        finally:
            rejections = self._batch_quote_rejections
            self._batch_quote_rejections = None
            raw_already_recorded.reset(token)
        if rejections:
            try:
                self._repository.append_event(
                    "shadow_quote_unusable",
                    {
                        "count": len(rejections),
                        "quotes": rejections,
                        "raw_data_preserved": True,
                        "execution_enabled": False,
                    },
                    occurred_at=receipts[-1].received_at,
                    trace_id=f"shadow-quotes-unusable:{uuid4()}",
                )
            except Exception as exc:
                result.errors.append(type(exc).__name__)
        return result

    def finish_recorded_batches(self, *, observed_at: datetime) -> None:
        self._builder.finish(observed_at=observed_at)

    def derived_health(self, *, observed_at: datetime) -> dict[str, object]:
        age = (
            (observed_at - self._last_frame_at).total_seconds()
            if self._last_frame_at is not None
            else None
        )
        healthy = age is not None and 0 <= age <= (
            self._config.intraday.primary_bar_minutes * 60
            + self._config.intraday.bar_max_age_seconds
        )
        return {
            "state": (
                "ready" if healthy else "stale" if age is not None else "awaiting_complete_frame"
            ),
            "healthy": healthy,
            "frame_age_seconds": age,
            "last_usable_frame_at": (
                self._last_frame_at.isoformat() if self._last_frame_at is not None else None
            ),
            "unusable_frame_events": self._builder.unusable_frames,
            "last_unusable_frame": self._builder.last_unusable,
            "pending_minutes": self._builder.pending_minutes,
            "duplicate_minutes": self._builder.duplicate_minutes,
            "ignored_out_of_order_quotes": self._ignored_quotes,
            "evidence_epoch": self._evidence_epoch,
            "cumulative_evidence_complete": self._evidence_complete,
        }

    def _on_unusable_frame(self, issue: dict[str, object]) -> None:
        # Momentum windows, simulated outcomes, and frame-count decay must not bridge
        # an unobserved interval and pretend it was contiguous five-minute evidence.
        self._strategy = deepcopy(self._strategy_template)
        self._signals.clear()
        self._state.closes.clear()
        self._state.targets = {symbol: Decimal(0) for symbol in self._state.targets}
        self._state.pending_turnover = Decimal(0)
        self._latest_quote_at.clear()
        self._last_frame_at = None
        self._evidence_complete = False
        self._evidence_epoch = str(uuid4())
        self._repository.append_event(
            "shadow_frame_unusable",
            {
                **issue,
                "raw_data_preserved": True,
                "execution_enabled": False,
                "next_evidence_epoch": self._evidence_epoch,
            },
            occurred_at=datetime.fromisoformat(str(issue["discovered_event_at"])),
            trace_id=f"shadow-frame-unusable:{uuid4()}",
        )

    def _record_signal_decay(self, frame: UniverseFrame) -> None:
        remaining: list[_ShadowSignal] = []
        for signal in self._signals:
            signal.elapsed_frames += 1
            if signal.elapsed_frames in {1, 3, 6}:
                current = frame.bar_for(signal.symbol).close
                self._repository.append_event(
                    "shadow_signal_decay",
                    {
                        "symbol": signal.symbol,
                        "signal_at": signal.signal_at.isoformat(),
                        "observed_at": frame.timestamp.isoformat(),
                        "horizon_frames": signal.elapsed_frames,
                        "evidence_epoch": self._evidence_epoch,
                        "gross_return_bps": str(
                            (current / signal.reference_price - 1) * Decimal(10_000)
                        ),
                    },
                    occurred_at=frame.timestamp,
                    trace_id=(
                        f"shadow-decay:{signal.symbol}:{signal.signal_at.isoformat()}:"
                        f"{signal.elapsed_frames}"
                    ),
                )
            if signal.elapsed_frames < 6:
                remaining.append(signal)
        self._signals = remaining

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
                "evidence_epoch": self._evidence_epoch,
                "cumulative_evidence_complete": self._evidence_complete,
            },
            occurred_at=frame.timestamp,
            trace_id=f"shadow-outcome:{frame.timestamp.isoformat()}",
        )
