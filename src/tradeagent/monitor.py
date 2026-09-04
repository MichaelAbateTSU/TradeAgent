from __future__ import annotations

import time
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from typing import Protocol

from pydantic import BaseModel, ConfigDict

from tradeagent.alpaca_paper import (
    AlpacaOrderStatus,
    AlpacaPaperAccount,
    AlpacaPaperOrder,
    AlpacaPaperPosition,
)
from tradeagent.domain import OrderRequest, Side
from tradeagent.ledger import SQLiteLedger

TERMINAL_STATUSES = {
    AlpacaOrderStatus.FILLED,
    AlpacaOrderStatus.CANCELED,
    AlpacaOrderStatus.EXPIRED,
    AlpacaOrderStatus.REJECTED,
}


class TakeProfitGateway(Protocol):
    def account(self) -> AlpacaPaperAccount: ...

    def positions(self) -> tuple[AlpacaPaperPosition, ...]: ...

    def submit_market_order(self, order: OrderRequest) -> AlpacaPaperOrder: ...

    def order_by_client_id(self, client_order_id: str) -> AlpacaPaperOrder: ...


class TakeProfitResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    status: str
    symbol: str
    quantity: Decimal = Decimal(0)
    observed_profit: Decimal = Decimal(0)
    broker_order: AlpacaPaperOrder | None = None


def monitor_take_profit(
    gateway: TakeProfitGateway,
    ledger: SQLiteLedger,
    *,
    symbol: str,
    minimum_profit: Decimal,
    poll_seconds: float,
    on_sample: Callable[[AlpacaPaperPosition], None] | None = None,
    sleeper: Callable[[float], None] = time.sleep,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    max_polls: int | None = None,
) -> TakeProfitResult:
    normalized_symbol = symbol.replace("/", "").upper()
    polls = 0
    while True:
        position = next(
            (
                candidate
                for candidate in gateway.positions()
                if candidate.symbol.replace("/", "").upper() == normalized_symbol
            ),
            None,
        )
        if position is None:
            return TakeProfitResult(status="no_position", symbol=symbol)
        if on_sample is not None:
            on_sample(position)
        if position.unrealized_pnl > minimum_profit:
            account = gateway.account()
            if account.account_blocked or account.trading_blocked:
                raise RuntimeError("Alpaca paper account is blocked")
            now = clock()
            client_order_id = f"take-profit-{now:%Y%m%dT%H%M%S%fZ}"
            decision_id = sha256(client_order_id.encode()).hexdigest()[:24]
            order = OrderRequest(
                client_order_id=client_order_id,
                decision_id=decision_id,
                strategy_id="manual-paper-take-profit-v1",
                symbol=symbol,
                side=Side.SELL,
                quantity=position.quantity,
                submitted_at=now,
            )
            broker_order = gateway.submit_market_order(order)
            ledger.append(
                "manual_take_profit_triggered",
                {
                    "symbol": symbol,
                    "quantity": str(position.quantity),
                    "observed_profit": str(position.unrealized_pnl),
                    "minimum_profit": str(minimum_profit),
                    "paper_only": True,
                },
                occurred_at=now,
                trace_id=decision_id,
            )
            ledger.append(
                "broker_order_state",
                broker_order,
                occurred_at=now,
                trace_id=decision_id,
            )
            for _ in range(120):
                if broker_order.status in TERMINAL_STATUSES:
                    break
                sleeper(1)
                broker_order = gateway.order_by_client_id(client_order_id)
            ledger.append(
                "broker_order_state",
                broker_order,
                occurred_at=clock(),
                trace_id=decision_id,
            )
            return TakeProfitResult(
                status=broker_order.status.value,
                symbol=symbol,
                quantity=position.quantity,
                observed_profit=position.unrealized_pnl,
                broker_order=broker_order,
            )

        polls += 1
        if max_polls is not None and polls >= max_polls:
            return TakeProfitResult(
                status="monitoring",
                symbol=symbol,
                quantity=position.quantity,
                observed_profit=position.unrealized_pnl,
            )
        sleeper(poll_seconds)
