from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from typing import Protocol

from pydantic import BaseModel, ConfigDict

from tradeagent.alpaca_paper import (
    AlpacaOrderStatus,
    AlpacaPaperAccount,
    AlpacaPaperOrder,
    AlpacaPaperPosition,
)
from tradeagent.domain import AccountSnapshot, MarketBar, OrderRequest, RiskDecision, Side
from tradeagent.ledger import SQLiteLedger
from tradeagent.risk import RiskEngine

TERMINAL_ORDER_STATUSES = {
    AlpacaOrderStatus.FILLED,
    AlpacaOrderStatus.DONE_FOR_DAY,
    AlpacaOrderStatus.CANCELED,
    AlpacaOrderStatus.EXPIRED,
    AlpacaOrderStatus.REPLACED,
    AlpacaOrderStatus.STOPPED,
    AlpacaOrderStatus.REJECTED,
    AlpacaOrderStatus.SUSPENDED,
    AlpacaOrderStatus.CALCULATED,
}


class AlpacaPaperGateway(Protocol):
    def account(self) -> AlpacaPaperAccount: ...

    def positions(self) -> tuple[AlpacaPaperPosition, ...]: ...

    def submit_market_order(self, order: OrderRequest) -> AlpacaPaperOrder: ...

    def find_order_by_client_id(self, client_order_id: str) -> AlpacaPaperOrder | None: ...

    def open_orders(self) -> tuple[AlpacaPaperOrder, ...]: ...


class QualificationGate(Protocol):
    def is_strategy_qualified(self, strategy_id: str) -> bool: ...


class OmsSubmission(BaseModel):
    model_config = ConfigDict(frozen=True)

    risk: RiskDecision
    broker_order: AlpacaPaperOrder | None
    recovered_existing_order: bool


class ReconciliationResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    healthy: bool
    account: AlpacaPaperAccount
    positions: tuple[AlpacaPaperPosition, ...]
    open_orders: tuple[AlpacaPaperOrder, ...]
    mismatches: tuple[str, ...]


class PaperOrderManager:
    """Risk gate and reconciliation boundary for the external fake-money broker."""

    def __init__(
        self,
        gateway: AlpacaPaperGateway,
        risk: RiskEngine,
        ledger: SQLiteLedger,
        qualification_gate: QualificationGate | None = None,
    ) -> None:
        self._gateway = gateway
        self._risk = risk
        self._ledger = ledger
        self._qualification_gate = qualification_gate

    def submit(
        self,
        order: OrderRequest,
        bar: MarketBar,
        account: AccountSnapshot,
        *,
        observed_at: datetime,
    ) -> OmsSubmission:
        kill_switch_active = self._ledger.get_control("kill_switch", default="inactive") == "active"
        if kill_switch_active:
            self._risk.activate_kill_switch()
        else:
            self._risk.reset_kill_switch()
        decision = self._risk.evaluate(
            order,
            bar,
            account,
            observed_at=observed_at,
            trading_enabled=True,
        )
        current = account.position_for(order.symbol)
        current_quantity = current.quantity if current is not None else Decimal(0)
        signed_quantity = order.quantity if order.side is Side.BUY else -order.quantity
        risk_reducing = abs(current_quantity + signed_quantity) < abs(current_quantity)
        qualified = (
            self._qualification_gate is not None
            and self._qualification_gate.is_strategy_qualified(order.strategy_id)
        )
        if decision.approved and not qualified and not risk_reducing:
            decision = RiskDecision(
                approved=False,
                codes=("STRATEGY_NOT_QUALIFIED",),
                message="rejected: STRATEGY_NOT_QUALIFIED",
                projected_gross_exposure=decision.projected_gross_exposure,
            )
        self._ledger.append(
            "risk_decision",
            decision,
            occurred_at=observed_at,
            trace_id=order.decision_id,
        )
        if not decision.approved:
            return OmsSubmission(
                risk=decision,
                broker_order=None,
                recovered_existing_order=False,
            )

        existing = self._gateway.find_order_by_client_id(order.client_order_id)
        broker_order = existing or self._gateway.submit_market_order(order)
        self._record_order_state(
            broker_order,
            observed_at=observed_at,
            trace_id=order.decision_id,
        )
        return OmsSubmission(
            risk=decision,
            broker_order=broker_order,
            recovered_existing_order=existing is not None,
        )

    def reconcile(self, *, observed_at: datetime) -> ReconciliationResult:
        account = self._gateway.account()
        positions = self._gateway.positions()
        open_orders = self._gateway.open_orders()
        mismatches: list[str] = []
        if account.account_blocked or account.trading_blocked:
            mismatches.append("BROKER_TRADING_BLOCKED")

        broker_by_client_id: dict[str, AlpacaPaperOrder] = {}
        for order in open_orders:
            if order.client_order_id in broker_by_client_id:
                mismatches.append(f"DUPLICATE_BROKER_CLIENT_ORDER_ID:{order.client_order_id}")
            broker_by_client_id[order.client_order_id] = order
            self._record_order_state(
                order,
                observed_at=observed_at,
                trace_id=f"reconcile:{order.client_order_id}",
            )

        local_states = self._latest_local_order_states()
        for client_order_id, local_order in local_states.items():
            if local_order.status in TERMINAL_ORDER_STATUSES:
                continue
            broker_order = broker_by_client_id.get(client_order_id)
            if broker_order is None:
                broker_order = self._gateway.find_order_by_client_id(client_order_id)
            if broker_order is None:
                mismatches.append(f"LOCAL_ORDER_MISSING_AT_BROKER:{client_order_id}")
                continue
            self._record_order_state(
                broker_order,
                observed_at=observed_at,
                trace_id=f"reconcile:{client_order_id}",
            )

        result = ReconciliationResult(
            healthy=not mismatches,
            account=account,
            positions=positions,
            open_orders=open_orders,
            mismatches=tuple(mismatches),
        )
        self._ledger.append(
            "broker_reconciliation",
            result,
            occurred_at=observed_at,
            trace_id=f"reconcile:{observed_at.isoformat()}",
        )
        if mismatches:
            self._ledger.set_control(
                "kill_switch",
                "active",
                occurred_at=observed_at,
                trace_id="reconcile:kill-switch:active",
            )
        return result

    def _latest_local_order_states(self) -> Mapping[str, AlpacaPaperOrder]:
        latest: dict[str, AlpacaPaperOrder] = {}
        for event in self._ledger.events(limit=10_000):
            if event["event_type"] != "broker_order_state":
                continue
            order = AlpacaPaperOrder.model_validate(event["payload"])
            latest.setdefault(order.client_order_id, order)
        return latest

    def _record_order_state(
        self,
        order: AlpacaPaperOrder,
        *,
        observed_at: datetime,
        trace_id: str,
    ) -> None:
        self._ledger.append(
            "broker_order_state",
            order,
            occurred_at=observed_at,
            trace_id=trace_id,
        )
