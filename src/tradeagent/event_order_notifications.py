from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Connection

from tradeagent.persistence import notification_outbox


def enqueue_lifecycle(
    connection: Connection,
    *,
    cohort: str,
    client_id: str,
    symbol: str,
    side: str,
    status: str,
    link: dict[str, Any],
    now: datetime,
) -> None:
    broker = link.get("broker") or {}
    rejection = link.get("rejection")
    filled = Decimal(str(broker.get("filled_quantity", 0)))
    if status in {
        "accepted",
        "submitted",
        "new",
        "pending_new",
        "accepted_for_bidding",
        "acknowledged",
        "submitting",
    }:
        stage = "accepted"
    elif status in {"partially_filled", "filled", "canceled", "expired", "rejected"}:
        stage = status
    else:
        return
    if not broker and not rejection:
        return
    # Accepted states are one notification; partial fills use cumulative quantity, not delivery ID.
    identity = f"paper-lifecycle:{cohort}:{client_id}:{stage}:{filled.normalize()}"
    notification_id = str(uuid5(NAMESPACE_URL, identity))
    payload = {
        "cohort_id": cohort,
        "client_order_id": client_id,
        "broker_order_id": broker.get("id"),
        "symbol": symbol,
        "side": side,
        "stage": stage,
        "broker_status": broker.get("status"),
        "filled_quantity": str(filled),
        "filled_average_price": broker.get("filled_average_price"),
        "broker_updated_at": broker.get("updated_at"),
        "filled_at": broker.get("filled_at"),
        "rejection": rejection,
        "trade_classification": link.get("trade_classification"),
        "decision_ticket": link.get("decision_ticket"),
        "protection": link.get("protection"),
        "exit_decision": link.get("exit_decision"),
        "paper_only": True,
        "qualification_eligible": False,
        "is_exit": side == "sell",
        "subject": f"[PAPER {'EXIT' if side == 'sell' else 'ENTRY'} {stage.upper()}] {symbol}",
    }
    payload["text"] = (
        "Actual paper order lifecycle observation, not a live-money trade or profitability proof.\n"
        "Local protective triggers are not broker-native stops and do not guarantee fill prices.\n"
        + json.dumps(payload, sort_keys=True, indent=2)
    )
    insert = pg_insert if connection.dialect.name == "postgresql" else sqlite_insert
    connection.execute(
        insert(notification_outbox)
        .values(
            notification_id=notification_id,
            cycle_id=None,
            notification_type="paper_order_lifecycle",
            payload=payload,
            status="pending",
            attempts=0,
            created_at=now,
        )
        .on_conflict_do_nothing(index_elements=[notification_outbox.c.notification_id])
    )
