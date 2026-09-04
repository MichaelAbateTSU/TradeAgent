from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal
from hashlib import sha256

from tradeagent.config import BrokerConfig
from tradeagent.domain import (
    AccountSnapshot,
    Fill,
    MarketBar,
    OrderRequest,
    PaperBrokerState,
    PaperPositionState,
    Position,
    Side,
)

MONEY_QUANTUM = Decimal("0.0001")
PRICE_QUANTUM = Decimal("0.000001")


def _money(value: Decimal) -> Decimal:
    return value.quantize(MONEY_QUANTUM, rounding=ROUND_HALF_UP)


@dataclass
class _PositionState:
    quantity: Decimal = Decimal(0)
    average_price: Decimal = Decimal(0)
    realized_pnl: Decimal = Decimal(0)


class PaperBroker:
    """Deterministic fake-money broker with costs, slippage, and idempotent orders."""

    def __init__(self, config: BrokerConfig) -> None:
        self._config = config
        self._cash = config.starting_cash
        self._positions: dict[str, _PositionState] = {}
        self._marks: dict[str, Decimal] = {}
        self._fills: dict[str, Fill] = {}
        self._session_date: date | None = None
        self._day_start_equity = config.starting_cash
        self._high_watermark = config.starting_cash

    @property
    def fill_count(self) -> int:
        return len(self._fills)

    @property
    def fills(self) -> tuple[Fill, ...]:
        return tuple(self._fills.values())

    @classmethod
    def from_state(cls, config: BrokerConfig, state: PaperBrokerState) -> PaperBroker:
        broker = cls(config)
        broker._cash = state.cash
        broker._positions = {
            position.symbol: _PositionState(
                quantity=position.quantity,
                average_price=position.average_price,
                realized_pnl=position.realized_pnl,
            )
            for position in state.positions
        }
        broker._marks = dict(state.marks)
        broker._fills = {fill.client_order_id: fill for fill in state.fills}
        broker._session_date = state.session_date
        broker._day_start_equity = state.day_start_equity
        broker._high_watermark = state.high_watermark
        return broker

    def export_state(self) -> PaperBrokerState:
        return PaperBrokerState(
            cash=self._cash,
            positions=tuple(
                PaperPositionState(
                    symbol=symbol,
                    quantity=state.quantity,
                    average_price=state.average_price,
                    realized_pnl=state.realized_pnl,
                )
                for symbol, state in sorted(self._positions.items())
            ),
            marks=dict(self._marks),
            fills=self.fills,
            session_date=self._session_date,
            day_start_equity=self._day_start_equity,
            high_watermark=self._high_watermark,
        )

    def mark(self, bar: MarketBar) -> None:
        prior_equity = self._calculate_equity()
        if self._session_date != bar.timestamp.date():
            self._session_date = bar.timestamp.date()
            self._day_start_equity = prior_equity
        self._marks[bar.symbol] = bar.close
        equity = self._calculate_equity()
        self._high_watermark = max(self._high_watermark, equity)

    def submit(self, order: OrderRequest, bar: MarketBar) -> Fill:
        existing = self._fills.get(order.client_order_id)
        if existing is not None:
            return existing
        if order.symbol != bar.symbol:
            raise ValueError("order symbol does not match execution bar")

        slip = self._config.slippage_bps / Decimal(10_000)
        multiplier = Decimal(1) + slip if order.side is Side.BUY else Decimal(1) - slip
        fill_price = (bar.close * multiplier).quantize(PRICE_QUANTUM, rounding=ROUND_HALF_UP)
        notional = fill_price * order.quantity
        commission = _money(notional * self._config.commission_bps / Decimal(10_000))
        position = self._positions.setdefault(order.symbol, _PositionState())

        if order.side is Side.BUY:
            required_cash = notional + commission
            if required_cash > self._cash:
                raise ValueError("paper broker rejected order for insufficient cash")
            new_quantity = position.quantity + order.quantity
            position.average_price = (
                (position.quantity * position.average_price) + notional
            ) / new_quantity
            position.quantity = new_quantity
            self._cash -= required_cash
        else:
            if order.quantity > position.quantity:
                raise ValueError("paper broker rejected order that would create a short")
            position.realized_pnl += (
                (fill_price - position.average_price) * order.quantity
            ) - commission
            position.quantity -= order.quantity
            self._cash += notional - commission
            if position.quantity == 0:
                position.average_price = Decimal(0)

        fill_key = f"{order.client_order_id}:{fill_price}:{order.quantity}"
        fill = Fill(
            fill_id=sha256(fill_key.encode()).hexdigest()[:24],
            client_order_id=order.client_order_id,
            symbol=order.symbol,
            side=order.side,
            quantity=order.quantity,
            price=fill_price,
            commission=commission,
            filled_at=bar.timestamp,
        )
        self._fills[order.client_order_id] = fill
        self.mark(bar)
        return fill

    def account(self, as_of: datetime) -> AccountSnapshot:
        positions = tuple(
            Position(
                symbol=symbol,
                quantity=state.quantity,
                average_price=state.average_price,
                market_price=self._marks[symbol],
                realized_pnl=state.realized_pnl,
            )
            for symbol, state in sorted(self._positions.items())
            if state.quantity != 0 and symbol in self._marks
        )
        equity = self._cash + sum((position.market_value for position in positions), Decimal(0))
        self._high_watermark = max(self._high_watermark, equity)
        return AccountSnapshot(
            as_of=as_of,
            cash=_money(self._cash),
            equity=_money(equity),
            day_start_equity=_money(self._day_start_equity),
            high_watermark=_money(self._high_watermark),
            positions=positions,
        )

    def _calculate_equity(self) -> Decimal:
        market_value = sum(
            (
                state.quantity * self._marks.get(symbol, state.average_price)
                for symbol, state in self._positions.items()
            ),
            Decimal(0),
        )
        return self._cash + market_value
