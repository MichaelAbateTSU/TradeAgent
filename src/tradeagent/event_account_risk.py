from __future__ import annotations

import json
from datetime import UTC, datetime, time, timedelta
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from tradeagent.event_demo import _write_control
from tradeagent.event_performance import (
    RESIDUAL_RATE,
    SELL_MINIMUM,
    SELL_SHARE_RATE,
    SELL_VALUE_RATE,
    allocation_ledgers,
)
from tradeagent.event_store import EventStore, event_order_links
from tradeagent.experimental_policy import ExperimentalSettings
from tradeagent.paper_account_history import before as history_before
from tradeagent.persistence import controls, orders


def account_risk(
    store: EventStore, settings: ExperimentalSettings, marks: dict[str, Decimal], now: datetime
) -> dict[str, Any]:
    """Account-wide economic P&L; new cohorts cannot reset historical loss budgets."""
    digest = settings.news_account_digest
    if digest is None:
        raise ValueError("pinned paper account required")
    local_date = now.astimezone(ZoneInfo("America/New_York")).date()
    monday = local_date - timedelta(days=local_date.weekday())
    key = "paper-risk:" + digest
    with store.database.begin() as connection:
        insert = pg_insert if connection.dialect.name == "postgresql" else sqlite_insert
        connection.execute(
            insert(controls)
            .values(control_key=key, control_value="{}", updated_at=now)
            .on_conflict_do_nothing(index_elements=[controls.c.control_key])
        )
        raw = connection.execute(
            select(controls.c.control_value).where(controls.c.control_key == key).with_for_update()
        ).scalar_one()
        saved = json.loads(raw)
        baseline_raw = connection.scalar(
            select(controls.c.control_value).where(
                controls.c.control_key == "paper-baseline:" + digest
            )
        )
        baseline = json.loads(baseline_raw) if baseline_raw else None
        if settings.entry_policy == "operator-calibration" and baseline is None:
            return {
                "state": "unvalued",
                "blocked": True,
                "reason": "complete broker baseline required",
            }
        capital = Decimal(saved.get("capital", str(settings.virtual_equity)))
        rows = [
            dict(row)
            for row in connection.execute(
                select(orders, event_order_links.c.payload.label("link"))
                .join(
                    event_order_links,
                    orders.c.client_order_id == event_order_links.c.client_order_id,
                )
                .where(event_order_links.c.payload["account_digest"].as_string() == digest)
                .order_by(orders.c.created_at, orders.c.order_id)
            ).mappings()
        ]
        if baseline is not None:
            rows = [
                row for row in rows if row["broker_order_id"] not in baseline["broker_order_ids"]
            ]

        def equity(
            selected: list[dict[str, Any]], prices: dict[str, Decimal], offset: Decimal = Decimal(0)
        ) -> Decimal:
            report = allocation_ledgers(
                selected, prices, capital, session_date=local_date, purpose="iex-practice"
            )
            if report["state"] != "valued":
                raise ValueError("account risk history/position cannot be valued")
            return Decimal(report["economic_paper_equity"]) + offset

        def before(boundary: datetime) -> Decimal:
            selected = []
            for row in rows:
                created = row["created_at"]
                created = created.replace(tzinfo=UTC) if created.tzinfo is None else created
                if created >= boundary:
                    continue
                broker = row["link"].get("broker") or {}
                if not Decimal(str(broker.get("filled_quantity", 0))):
                    continue
                at = broker.get("filled_at")
                if not at:
                    raise ValueError("filled account history lacks an exact fill timestamp")
                filled_at = datetime.fromisoformat(at)
                if filled_at.tzinfo is None:
                    raise ValueError("naive broker fill timestamp")
                if filled_at < boundary:
                    selected.append(row)
            return equity(
                selected, {}, history_before(baseline, boundary) if baseline else Decimal(0)
            )

        try:
            current = equity(
                rows, marks, Decimal(baseline["economic_pnl"]) if baseline else Decimal(0)
            )
            day_start = before(datetime.combine(local_date, time(), ZoneInfo("America/New_York")))
            week_start = before(datetime.combine(monday, time(), ZoneInfo("America/New_York")))
        except (ValueError, ArithmeticError) as error:
            return {"state": "unvalued", "blocked": True, "reason": str(error)}
        day_limit = min(
            Decimal(saved.get("daily_limit", "50")),
            settings.virtual_equity * settings.daily_loss_fraction,
        )
        week_limit = min(
            Decimal(saved.get("weekly_limit", "50")),
            settings.virtual_equity * settings.weekly_loss_fraction,
        )
        drawdown_limit = min(
            Decimal(saved.get("drawdown_limit", "150")),
            settings.virtual_equity * settings.drawdown_fraction,
        )
        historical_peak = capital + Decimal(baseline["peak_pnl"]) if baseline else capital
        cash = Decimal(baseline["economic_pnl"]) if baseline else Decimal(0)
        inventory: dict[str, Decimal] = {}
        for row in rows:
            broker = row["link"].get("broker") or {}
            quantity = Decimal(str(broker.get("filled_quantity", 0)))
            if not quantity:
                continue
            price = Decimal(str(broker["filled_average_price"]))
            signed = quantity if row["side"] == "buy" else -quantity
            inventory[row["symbol"]] = inventory.get(row["symbol"], Decimal(0)) + signed
            cash -= signed * price + quantity * price * RESIDUAL_RATE
            if row["side"] == "sell":
                cash -= max(
                    SELL_MINIMUM, quantity * price * SELL_VALUE_RATE + quantity * SELL_SHARE_RATE
                )
            if not any(inventory.values()):
                historical_peak = max(historical_peak, capital + cash)
        peak = max(
            Decimal(saved.get("peak", str(capital))),
            historical_peak,
            day_start,
            week_start,
            current,
        )
        blocked = (
            current <= day_start - day_limit
            or current <= week_start - week_limit
            or current <= peak - drawdown_limit
        )
        state = {
            "account_digest": digest,
            "capital": str(capital),
            "peak": str(peak),
            "daily_limit": str(day_limit),
            "weekly_limit": str(week_limit),
            "drawdown_limit": str(drawdown_limit),
            "observed_at": now.astimezone(UTC).isoformat(),
        }
        _write_control(connection, key, json.dumps(state, sort_keys=True), now)
        report = {
            **state,
            "state": "valued",
            "blocked": blocked,
            "economic_equity": str(current),
            "day_start_equity": str(day_start),
            "week_start_equity": str(week_start),
            "session_date": str(local_date),
            "week_start": str(monday),
            "scope": "verified complete broker baseline plus subsequent OMS fills"
            if baseline
            else "retained OMS-linked fills only; external history is not established",
            "broker_baseline_identity": baseline["identity"] if baseline else None,
            "fees": "existing conservative economic cost reserves, not invented actual fees",
        }
        store.audit("account_risk", report, now, settings.cohort_id, connection)
        return report
