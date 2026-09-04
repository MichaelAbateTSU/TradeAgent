from __future__ import annotations

from collections import deque
from datetime import datetime, timedelta
from decimal import Decimal

from tradeagent.config import RiskLimits
from tradeagent.domain import AccountSnapshot, MarketBar, OrderRequest, RiskDecision, Side


class RiskEngine:
    """Independent, deterministic pre-trade risk gateway."""

    def __init__(self, limits: RiskLimits) -> None:
        self._limits = limits
        self._order_times: deque[datetime] = deque()
        self._kill_switch_active = False

    @property
    def kill_switch_active(self) -> bool:
        return self._kill_switch_active

    def activate_kill_switch(self) -> None:
        self._kill_switch_active = True

    def reset_kill_switch(self) -> None:
        self._kill_switch_active = False

    def evaluate(
        self,
        order: OrderRequest,
        bar: MarketBar,
        account: AccountSnapshot,
        *,
        observed_at: datetime,
        trading_enabled: bool,
    ) -> RiskDecision:
        codes: list[str] = []
        current = account.position_for(order.symbol)
        current_quantity = current.quantity if current is not None else Decimal(0)
        signed_quantity = order.quantity if order.side is Side.BUY else -order.quantity
        projected_quantity = current_quantity + signed_quantity
        risk_reducing = abs(projected_quantity) < abs(current_quantity)

        data_age = observed_at - bar.timestamp
        if data_age < timedelta(0) or data_age > timedelta(
            seconds=self._limits.max_data_age_seconds
        ):
            codes.append("STALE_DATA")
        if (not trading_enabled or self._kill_switch_active) and not risk_reducing:
            codes.append("TRADING_DISABLED")
        if projected_quantity < 0 and not self._limits.allow_shorting:
            codes.append("SHORTING_DISABLED")

        equity = account.equity
        order_notional = order.quantity * bar.close
        projected_symbol_value = abs(projected_quantity * bar.close)
        current_symbol_value = abs(current_quantity * bar.close)
        projected_gross = account.gross_exposure - current_symbol_value + projected_symbol_value
        projected_gross_ratio = projected_gross / equity

        if not risk_reducing:
            if order_notional / equity > self._limits.max_order_exposure:
                codes.append("MAX_ORDER_EXPOSURE")
            if projected_symbol_value / equity > self._limits.max_position_exposure:
                codes.append("MAX_POSITION_EXPOSURE")
            if projected_gross_ratio > self._limits.max_gross_exposure:
                codes.append("MAX_GROSS_EXPOSURE")
            projected_symbols = {position.symbol for position in account.positions}
            if projected_quantity > 0:
                projected_symbols.add(order.symbol)
            if len(projected_symbols) > self._limits.max_positions:
                codes.append("MAX_POSITIONS")
            if account.daily_return <= -self._limits.max_daily_loss:
                codes.append("MAX_DAILY_LOSS")
            if account.drawdown <= -self._limits.max_drawdown:
                codes.append("MAX_DRAWDOWN")
            if (
                not self._limits.allow_leverage
                and order.side is Side.BUY
                and order_notional > account.cash
            ):
                codes.append("LEVERAGE_DISABLED")

        cutoff = observed_at - timedelta(hours=1)
        while self._order_times and self._order_times[0] <= cutoff:
            self._order_times.popleft()
        if len(self._order_times) >= self._limits.max_orders_per_hour and not risk_reducing:
            codes.append("ORDER_RATE_LIMIT")

        approved = not codes
        if approved:
            self._order_times.append(observed_at)
        message = "approved" if approved else f"rejected: {', '.join(codes)}"
        return RiskDecision(
            approved=approved,
            codes=tuple(codes),
            message=message,
            projected_gross_exposure=projected_gross_ratio,
        )
