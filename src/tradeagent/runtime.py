from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Protocol

from pydantic import BaseModel, ConfigDict

from tradeagent.alpaca_paper import (
    AlpacaPaperAccount,
    AlpacaPaperOrder,
    AlpacaPaperPosition,
)
from tradeagent.alpaca_stream import AlpacaMarketStream, MarketQuote, MarketTrade
from tradeagent.domain import MarketBar
from tradeagent.persistence import ProductionRepository
from tradeagent.scheduler import ReconciliationScheduler
from tradeagent.worker import AutonomousPaperWorker


class RuntimeReconciliationStatus(BaseModel):
    model_config = ConfigDict(frozen=True)

    healthy: bool
    position_count: int
    open_order_count: int
    mismatches: tuple[str, ...]


class RuntimePaperClient(Protocol):
    def account(self) -> AlpacaPaperAccount: ...

    def positions(self) -> tuple[AlpacaPaperPosition, ...]: ...

    def open_orders(self) -> tuple[AlpacaPaperOrder, ...]: ...


class ProductionPaperReconciler:
    def __init__(
        self,
        client: RuntimePaperClient,
        repository: ProductionRepository,
    ) -> None:
        self._client = client
        self._repository = repository

    def reconcile(self, *, observed_at: datetime) -> RuntimeReconciliationStatus:
        account = self._client.account()
        positions = self._client.positions()
        open_orders = self._client.open_orders()
        mismatches: list[str] = []
        if account.account_blocked or account.trading_blocked:
            mismatches.append("BROKER_TRADING_BLOCKED")
        client_order_ids: set[str] = set()
        for order in open_orders:
            if order.client_order_id in client_order_ids:
                mismatches.append(f"DUPLICATE_BROKER_CLIENT_ORDER_ID:{order.client_order_id}")
            client_order_ids.add(order.client_order_id)
        status = RuntimeReconciliationStatus(
            healthy=not mismatches,
            position_count=len(positions),
            open_order_count=len(open_orders),
            mismatches=tuple(mismatches),
        )
        self._repository.append_event(
            "broker_reconciliation",
            status.model_dump(mode="json"),
            occurred_at=observed_at,
            trace_id=f"reconcile:{observed_at.isoformat()}",
        )
        if not status.healthy:
            self._repository.set_control("kill_switch", "active")
        return status


class ShadowAuditProcessor:
    def __init__(self, repository: ProductionRepository) -> None:
        self._repository = repository

    async def on_bar(self, bar: MarketBar, *, can_enter: bool) -> None:
        received_at = datetime.now(UTC)
        self._repository.store_market_bar(
            symbol=bar.symbol,
            timeframe="1Min",
            event_at=bar.timestamp,
            received_at=received_at,
            open_price=bar.open,
            high_price=bar.high,
            low_price=bar.low,
            close_price=bar.close,
            volume=bar.volume,
        )
        self._repository.append_event(
            "shadow_market_bar",
            {
                "bar": bar.model_dump(mode="json"),
                "can_enter": can_enter,
            },
            occurred_at=bar.timestamp,
            trace_id=f"bar:{bar.symbol}:{bar.timestamp.isoformat()}",
        )

    async def on_quote(self, quote: MarketQuote, *, can_enter: bool) -> None:
        received_at = datetime.now(UTC)
        self._repository.store_market_quote(
            symbol=quote.symbol,
            event_at=quote.timestamp,
            received_at=received_at,
            bid_price=quote.bid_price,
            ask_price=quote.ask_price,
            bid_size=quote.bid_size,
            ask_size=quote.ask_size,
            feed_source=quote.feed_source,
            bid_exchange=quote.bid_exchange,
            ask_exchange=quote.ask_exchange,
        )
        self._repository.append_event(
            "shadow_market_quote",
            {
                "quote": quote.model_dump(mode="json"),
                "can_enter": can_enter,
            },
            occurred_at=quote.timestamp,
            trace_id=f"quote:{quote.symbol}:{quote.timestamp.isoformat()}",
        )

    async def on_trade(self, trade: MarketTrade, *, can_enter: bool) -> None:
        received_at = datetime.now(UTC)
        self._repository.store_market_trade(
            provider_trade_id=str(trade.trade_id),
            symbol=trade.symbol,
            event_at=trade.timestamp,
            received_at=received_at,
            price=trade.price,
            size=trade.size,
            exchange=trade.exchange,
            conditions=trade.conditions,
            tape=trade.tape,
            feed_source=trade.feed_source,
        )
        self._repository.append_event(
            "shadow_market_trade",
            {
                "trade": trade.model_dump(mode="json"),
                "can_enter": can_enter,
            },
            occurred_at=trade.timestamp,
            trace_id=f"trade:{trade.symbol}:{trade.trade_id}",
        )


async def run_shadow_runtime(
    stream: AlpacaMarketStream,
    worker: AutonomousPaperWorker,
    scheduler: ReconciliationScheduler,
    *,
    symbols: tuple[str, ...],
) -> None:
    stop_event = asyncio.Event()
    async with asyncio.TaskGroup() as tasks:
        tasks.create_task(worker.run(stream.events(symbols), stop_event=stop_event))
        tasks.create_task(scheduler.run(stop_event))
