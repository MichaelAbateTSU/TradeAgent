from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest
from conftest import make_order

from tradeagent.alpaca_paper import AlpacaOrderStatus
from tradeagent.order_state import (
    OrderLifecycleState,
    OrderStateMachine,
    lifecycle_state_from_alpaca,
)
from tradeagent.persistence import Database, ProductionRepository


def _machine(tmp_path: Path) -> tuple[Database, OrderStateMachine]:
    database = Database(f"sqlite:///{tmp_path / 'orders.db'}")
    database.initialize()
    return database, OrderStateMachine(database)


def test_complete_partial_fill_and_cancel_lifecycle(tmp_path: Path, timestamp: datetime) -> None:
    database, machine = _machine(tmp_path)
    order = machine.create(make_order(timestamp))

    order = machine.transition(
        order.order_id,
        OrderLifecycleState.APPROVED,
        reason="risk approved",
    )
    order = machine.transition(
        order.order_id,
        OrderLifecycleState.SUBMITTING,
        reason="request started",
    )
    order = machine.transition(
        order.order_id,
        OrderLifecycleState.ACKNOWLEDGED,
        reason="broker acknowledged",
        broker_order_id="broker-1",
    )
    order = machine.transition(
        order.order_id,
        OrderLifecycleState.PARTIALLY_FILLED,
        reason="first fill",
        filled_quantity=Decimal("4"),
    )
    order = machine.transition(
        order.order_id,
        OrderLifecycleState.CANCEL_PENDING,
        reason="cancel requested",
    )
    order = machine.transition(
        order.order_id,
        OrderLifecycleState.CANCELED,
        reason="broker canceled remainder",
    )

    assert order.state is OrderLifecycleState.CANCELED
    assert order.filled_quantity == Decimal("4")
    assert order.broker_order_id == "broker-1"
    assert ProductionRepository(database).event_count() == 7
    database.dispose()


def test_reconciliation_recovers_lost_acknowledgement(tmp_path: Path, timestamp: datetime) -> None:
    database, machine = _machine(tmp_path)
    order = machine.create(make_order(timestamp))
    order = machine.transition(
        order.order_id,
        OrderLifecycleState.APPROVED,
        reason="risk approved",
    )
    order = machine.transition(
        order.order_id,
        OrderLifecycleState.RECONCILIATION_REQUIRED,
        reason="submit response lost",
    )
    order = machine.transition(
        order.order_id,
        OrderLifecycleState.FILLED,
        reason="broker truth recovered",
        broker_order_id="broker-2",
    )

    assert order.state is OrderLifecycleState.FILLED
    assert order.filled_quantity == order.quantity
    database.dispose()


def test_invalid_transitions_and_fill_quantities_fail_closed(
    tmp_path: Path, timestamp: datetime
) -> None:
    database, machine = _machine(tmp_path)
    order = machine.create(make_order(timestamp))

    with pytest.raises(ValueError, match="invalid order transition"):
        machine.transition(
            order.order_id,
            OrderLifecycleState.FILLED,
            reason="illegal shortcut",
        )
    order = machine.transition(
        order.order_id,
        OrderLifecycleState.APPROVED,
        reason="risk approved",
    )
    order = machine.transition(
        order.order_id,
        OrderLifecycleState.SUBMITTING,
        reason="submitting",
    )
    order = machine.transition(
        order.order_id,
        OrderLifecycleState.ACKNOWLEDGED,
        reason="acknowledged",
    )
    with pytest.raises(ValueError, match="between zero"):
        machine.transition(
            order.order_id,
            OrderLifecycleState.PARTIALLY_FILLED,
            reason="invalid fill",
            filled_quantity=order.quantity,
        )
    with pytest.raises(KeyError, match="does not exist"):
        machine.get(order.order_id.__class__("00000000-0000-0000-0000-000000000000"))
    database.dispose()


def test_alpaca_status_mapping_defaults_to_reconciliation() -> None:
    class StatusOnly:
        status = AlpacaOrderStatus.FILLED

    assert (
        lifecycle_state_from_alpaca(StatusOnly())  # type: ignore[arg-type]
        is OrderLifecycleState.FILLED
    )
    StatusOnly.status = AlpacaOrderStatus.HELD
    assert (
        lifecycle_state_from_alpaca(StatusOnly())  # type: ignore[arg-type]
        is OrderLifecycleState.RECONCILIATION_REQUIRED
    )
