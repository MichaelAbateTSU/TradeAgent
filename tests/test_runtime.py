from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from tradeagent.alpaca_paper import (
    AlpacaPaperAccount,
    AlpacaPaperOrder,
    AlpacaPaperPosition,
)
from tradeagent.alpaca_stream import MarketQuote
from tradeagent.data import synthetic_bars
from tradeagent.persistence import Database, ProductionRepository
from tradeagent.runtime import ProductionPaperReconciler, ShadowAuditProcessor


class FakePaperClient:
    def account(self) -> AlpacaPaperAccount:
        return AlpacaPaperAccount(
            id="account-1",
            status="ACTIVE",
            currency="USD",
            cash=Decimal("100000"),
            portfolio_value=Decimal("100000"),
            buying_power=Decimal("100000"),
            trading_blocked=False,
            transfers_blocked=False,
            account_blocked=False,
        )

    def positions(self) -> tuple[AlpacaPaperPosition, ...]:
        return ()

    def open_orders(self) -> tuple[AlpacaPaperOrder, ...]:
        return ()


def test_production_reconciler_and_shadow_audit(tmp_path: Path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'runtime.db'}")
    database.initialize()
    repository = ProductionRepository(database)
    reconciler = ProductionPaperReconciler(FakePaperClient(), repository)
    now = datetime(2026, 9, 4, 15, tzinfo=UTC)

    status = reconciler.reconcile(observed_at=now)
    processor = ShadowAuditProcessor(repository)
    bar = next(synthetic_bars(count=1)).model_copy(update={"timestamp": now})
    quote = MarketQuote(
        symbol="SPY",
        timestamp=now,
        bid_price=Decimal("99.9"),
        ask_price=Decimal("100.1"),
        bid_size=Decimal("10"),
        ask_size=Decimal("10"),
    )
    asyncio.run(processor.on_bar(bar, can_enter=False))
    asyncio.run(processor.on_quote(quote, can_enter=False))

    assert status.healthy
    assert status.position_count == 0
    assert status.open_order_count == 0
    assert repository.event_count() == 3
    database.dispose()
