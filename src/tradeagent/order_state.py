from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import insert, select, update

from tradeagent.alpaca_paper import AlpacaOrderStatus, AlpacaPaperOrder
from tradeagent.domain import OrderRequest
from tradeagent.persistence import Database, events, orders


class OrderLifecycleState(StrEnum):
    CREATED = "created"
    RISK_REJECTED = "risk_rejected"
    APPROVED = "approved"
    SUBMITTING = "submitting"
    ACKNOWLEDGED = "acknowledged"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCEL_PENDING = "cancel_pending"
    CANCELED = "canceled"
    REJECTED = "rejected"
    EXPIRED = "expired"
    RECONCILIATION_REQUIRED = "reconciliation_required"


TERMINAL_STATES = {
    OrderLifecycleState.RISK_REJECTED,
    OrderLifecycleState.FILLED,
    OrderLifecycleState.CANCELED,
    OrderLifecycleState.REJECTED,
    OrderLifecycleState.EXPIRED,
}

ALLOWED_TRANSITIONS: dict[OrderLifecycleState, set[OrderLifecycleState]] = {
    OrderLifecycleState.CREATED: {
        OrderLifecycleState.RISK_REJECTED,
        OrderLifecycleState.APPROVED,
    },
    OrderLifecycleState.APPROVED: {
        OrderLifecycleState.SUBMITTING,
        OrderLifecycleState.RECONCILIATION_REQUIRED,
    },
    OrderLifecycleState.SUBMITTING: {
        OrderLifecycleState.ACKNOWLEDGED,
        OrderLifecycleState.REJECTED,
        OrderLifecycleState.RECONCILIATION_REQUIRED,
    },
    OrderLifecycleState.ACKNOWLEDGED: {
        OrderLifecycleState.PARTIALLY_FILLED,
        OrderLifecycleState.FILLED,
        OrderLifecycleState.CANCEL_PENDING,
        OrderLifecycleState.REJECTED,
        OrderLifecycleState.EXPIRED,
        OrderLifecycleState.RECONCILIATION_REQUIRED,
    },
    OrderLifecycleState.PARTIALLY_FILLED: {
        OrderLifecycleState.PARTIALLY_FILLED,
        OrderLifecycleState.FILLED,
        OrderLifecycleState.CANCEL_PENDING,
        OrderLifecycleState.CANCELED,
        OrderLifecycleState.RECONCILIATION_REQUIRED,
    },
    OrderLifecycleState.CANCEL_PENDING: {
        OrderLifecycleState.PARTIALLY_FILLED,
        OrderLifecycleState.FILLED,
        OrderLifecycleState.CANCELED,
        OrderLifecycleState.RECONCILIATION_REQUIRED,
    },
    OrderLifecycleState.RECONCILIATION_REQUIRED: {
        OrderLifecycleState.ACKNOWLEDGED,
        OrderLifecycleState.PARTIALLY_FILLED,
        OrderLifecycleState.FILLED,
        OrderLifecycleState.CANCELED,
        OrderLifecycleState.REJECTED,
        OrderLifecycleState.EXPIRED,
    },
}


class TrackedOrder(BaseModel):
    model_config = ConfigDict(frozen=True)

    order_id: UUID
    client_order_id: str
    broker_order_id: str | None = None
    strategy_version: str
    symbol: str
    side: str
    quantity: Decimal = Field(gt=0)
    filled_quantity: Decimal = Field(ge=0)
    state: OrderLifecycleState
    created_at: datetime
    updated_at: datetime


class OrderTransition(BaseModel):
    model_config = ConfigDict(frozen=True)

    order_id: UUID
    from_state: OrderLifecycleState
    to_state: OrderLifecycleState
    event_at: datetime
    reason: str = Field(min_length=1)
    filled_quantity: Decimal = Field(ge=0)
    broker_order_id: str | None = None


def lifecycle_state_from_alpaca(order: AlpacaPaperOrder) -> OrderLifecycleState:
    mapping = {
        AlpacaOrderStatus.NEW: OrderLifecycleState.ACKNOWLEDGED,
        AlpacaOrderStatus.ACCEPTED: OrderLifecycleState.ACKNOWLEDGED,
        AlpacaOrderStatus.PENDING_NEW: OrderLifecycleState.SUBMITTING,
        AlpacaOrderStatus.PARTIALLY_FILLED: OrderLifecycleState.PARTIALLY_FILLED,
        AlpacaOrderStatus.FILLED: OrderLifecycleState.FILLED,
        AlpacaOrderStatus.PENDING_CANCEL: OrderLifecycleState.CANCEL_PENDING,
        AlpacaOrderStatus.CANCELED: OrderLifecycleState.CANCELED,
        AlpacaOrderStatus.EXPIRED: OrderLifecycleState.EXPIRED,
        AlpacaOrderStatus.REJECTED: OrderLifecycleState.REJECTED,
    }
    return mapping.get(order.status, OrderLifecycleState.RECONCILIATION_REQUIRED)


class OrderStateMachine:
    def __init__(self, database: Database) -> None:
        self._database = database

    def create(self, request: OrderRequest) -> TrackedOrder:
        order = TrackedOrder(
            order_id=uuid4(),
            client_order_id=request.client_order_id,
            strategy_version=request.strategy_id,
            symbol=request.symbol,
            side=request.side.value,
            quantity=request.quantity,
            filled_quantity=Decimal(0),
            state=OrderLifecycleState.CREATED,
            created_at=request.submitted_at,
            updated_at=request.submitted_at,
        )
        with self._database.begin() as connection:
            connection.execute(
                insert(orders).values(
                    order_id=str(order.order_id),
                    client_order_id=order.client_order_id,
                    broker_order_id=None,
                    strategy_version=order.strategy_version,
                    symbol=order.symbol,
                    side=order.side,
                    quantity=order.quantity,
                    filled_quantity=order.filled_quantity,
                    status=order.state.value,
                    created_at=order.created_at,
                    updated_at=order.updated_at,
                )
            )
            self._append_transition_event(
                connection,
                OrderTransition(
                    order_id=order.order_id,
                    from_state=OrderLifecycleState.CREATED,
                    to_state=OrderLifecycleState.CREATED,
                    event_at=order.created_at,
                    reason="order created",
                    filled_quantity=Decimal(0),
                ),
                order.client_order_id,
            )
        return order

    def get(self, order_id: UUID) -> TrackedOrder:
        with self._database.begin() as connection:
            row = (
                connection.execute(select(orders).where(orders.c.order_id == str(order_id)))
                .mappings()
                .one_or_none()
            )
        if row is None:
            raise KeyError(f"order {order_id} does not exist")
        return self._tracked_order(dict(row))

    def transition(
        self,
        order_id: UUID,
        to_state: OrderLifecycleState,
        *,
        reason: str,
        event_at: datetime | None = None,
        filled_quantity: Decimal | None = None,
        broker_order_id: str | None = None,
    ) -> TrackedOrder:
        timestamp = event_at or datetime.now(UTC)
        with self._database.begin() as connection:
            row = (
                connection.execute(select(orders).where(orders.c.order_id == str(order_id)))
                .mappings()
                .one_or_none()
            )
            if row is None:
                raise KeyError(f"order {order_id} does not exist")
            current = self._tracked_order(dict(row))
            allowed = ALLOWED_TRANSITIONS.get(current.state, set())
            if to_state not in allowed:
                raise ValueError(
                    f"invalid order transition {current.state.value} -> {to_state.value}"
                )
            next_filled = (
                current.quantity
                if to_state is OrderLifecycleState.FILLED
                else filled_quantity
                if filled_quantity is not None
                else current.filled_quantity
            )
            if next_filled < current.filled_quantity:
                raise ValueError("filled quantity cannot decrease")
            if next_filled > current.quantity:
                raise ValueError("filled quantity cannot exceed order quantity")
            if (
                to_state is OrderLifecycleState.PARTIALLY_FILLED
                and not Decimal(0) < next_filled < current.quantity
            ):
                raise ValueError("partial fill quantity must be between zero and order quantity")
            resolved_broker_id = broker_order_id or current.broker_order_id
            transition = OrderTransition(
                order_id=order_id,
                from_state=current.state,
                to_state=to_state,
                event_at=timestamp,
                reason=reason,
                filled_quantity=next_filled,
                broker_order_id=resolved_broker_id,
            )
            connection.execute(
                update(orders)
                .where(orders.c.order_id == str(order_id))
                .values(
                    broker_order_id=resolved_broker_id,
                    filled_quantity=next_filled,
                    status=to_state.value,
                    updated_at=timestamp,
                )
            )
            self._append_transition_event(
                connection,
                transition,
                current.client_order_id,
            )
        return self.get(order_id)

    @staticmethod
    def _tracked_order(row: dict[str, object]) -> TrackedOrder:
        return TrackedOrder(
            order_id=UUID(str(row["order_id"])),
            client_order_id=str(row["client_order_id"]),
            broker_order_id=(
                str(row["broker_order_id"]) if row["broker_order_id"] is not None else None
            ),
            strategy_version=str(row["strategy_version"]),
            symbol=str(row["symbol"]),
            side=str(row["side"]),
            quantity=Decimal(str(row["quantity"])),
            filled_quantity=Decimal(str(row["filled_quantity"])),
            state=OrderLifecycleState(str(row["status"])),
            created_at=row["created_at"],  # type: ignore[arg-type]
            updated_at=row["updated_at"],  # type: ignore[arg-type]
        )

    @staticmethod
    def _append_transition_event(
        connection: object,
        transition: OrderTransition,
        trace_id: str,
    ) -> None:
        connection.execute(  # type: ignore[attr-defined]
            insert(events).values(
                event_id=str(uuid4()),
                occurred_at=transition.event_at,
                recorded_at=datetime.now(UTC),
                event_type="order_transition",
                trace_id=trace_id,
                payload=transition.model_dump(mode="json"),
            )
        )
