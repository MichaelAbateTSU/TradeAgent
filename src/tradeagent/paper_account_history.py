"""Validated flat-account history bridge for operator and normal OMS risk accounting."""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import UTC, date, datetime
from decimal import ROUND_UP, Decimal
from hashlib import sha256
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
)
from tradeagent.event_session import locked_session_budget, save_session_budget
from tradeagent.event_store import EventStore, event_order_links
from tradeagent.persistence import controls, orders

EASTERN = ZoneInfo("America/New_York")
FINAL = {"filled", "canceled", "expired", "rejected"}
CRYPTO_TAKER_FEE_RESERVE = Decimal("0.0025")


def stamp(value: Any) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("history timestamp must identify its timezone")
    return parsed.astimezone(UTC)


def number(value: Any) -> Decimal:
    try:
        result = Decimal(str(value))
    except ArithmeticError as error:
        raise ValueError("invalid history amount") from error
    if not result.is_finite():
        raise ValueError("history amounts must be finite")
    return result


def history_identity(history: dict[str, Any]) -> str:
    return sha256(
        json.dumps(
            {
                **{key: history[key] for key in ("account_digest", "orders", "activities")},
                "account_cash": history.get("account_cash"),
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


def value_history(history: dict[str, Any]) -> dict[str, Any]:
    """Cash fees affect cash; asset fees affect inventory, not cash a second time."""
    if history.get("complete") is not True or history.get("broker_host") != (
        "https://paper-api.alpaca.markets"
    ):
        raise ValueError("complete paper account history required")
    observed = stamp(history["observed_at"])
    by_id = {row["id"]: row for row in history["orders"]}
    if len(by_id) != len(history["orders"]) or len(
        {row["client_order_id"] for row in by_id.values()}
    ) != len(by_id):
        raise ValueError("duplicate broker order history")
    totals: dict[str, Decimal] = defaultdict(Decimal)
    values: dict[str, Decimal] = defaultdict(Decimal)
    timeline: list[dict[str, Any]] = []
    activity_ids: set[str] = set()
    known_symbols = {row["symbol"].replace("/", "") for row in by_id.values()}
    if any(
        row["symbol"] not in {"AAPL", "MSFT", "NVDA", "BTC/USD"}
        and number(row.get("filled_quantity", row.get("filled_qty"))) != 0
        for row in by_id.values()
    ):
        raise ValueError("unvalued historical instrument; explicit valuation review required")
    funding = Decimal(0)
    funding_ids: list[str] = []
    crypto: dict[tuple[str, str], dict[str, Any]] = {}
    coin_fees: dict[str, Decimal] = defaultdict(Decimal)
    dollar_fees: dict[str, Decimal] = defaultdict(Decimal)
    for row in history["activities"]:
        identity = row["id"]
        if identity in activity_ids:
            raise ValueError("duplicate broker activity")
        activity_ids.add(identity)
        kind = row["activity_type"]
        at = stamp(row.get("transaction_time") or row.get("created_at"))
        if at > observed:
            raise ValueError("future broker activity")
        day = str(at.astimezone(EASTERN).date())
        if kind == "FILL":
            order = by_id.get(row["order_id"])
            if order is None or (row["symbol"], row["side"]) != (order["symbol"], order["side"]):
                raise ValueError("fill ownership/side mismatch")
            quantity, price = number(row["qty"]), number(row["price"])
            if quantity <= 0 or price <= 0 or row["side"] not in {"buy", "sell"}:
                raise ValueError("invalid fill")
            totals[order["id"]] += quantity
            values[order["id"]] += quantity * price
            signed = quantity if row["side"] == "buy" else -quantity
            reserve = quantity * price * RESIDUAL_RATE
            if row["side"] == "sell" and row["symbol"] != "BTC/USD":
                reserve += max(
                    SELL_MINIMUM, quantity * price * SELL_VALUE_RATE + quantity * SELL_SHARE_RATE
                )
            if row["symbol"] == "BTC/USD":
                group = crypto.setdefault(
                    (day, row["side"]),
                    {
                        "quantity": Decimal(0),
                        "value": Decimal(0),
                        "max_price": Decimal(0),
                        "at": at,
                    },
                )
                group["quantity"] += quantity
                group["value"] += quantity * price
                group["max_price"] = max(group["max_price"], price)
                group["at"] = max(group["at"], at)
            timeline.append(
                {
                    "at": at.isoformat(),
                    "id": identity,
                    "symbol": row["symbol"].replace("/", ""),
                    "quantity": str(signed),
                    "cash": str(-signed * price),
                    "reserve": str(reserve),
                }
            )
        elif kind in {"CFEE", "FEE"}:
            cash = number(row["net_amount"])
            quantity = number(row.get("qty", 0))
            symbol = row.get("symbol", "").replace("/", "")
            if (
                row.get("status") != "executed"
                or row.get("currency") != "USD"
                or cash > 0
                or quantity > 0
                or (quantity and (cash or symbol not in known_symbols))
                or (not quantity and symbol)
                or (kind == "FEE" and quantity)
            ):
                raise ValueError("unsupported or ambiguous fee")
            if kind == "CFEE":
                fee_day = str(date.fromisoformat(str(row.get("date", day))))
                if fee_day > str(observed.astimezone(EASTERN).date()):
                    raise ValueError("future fee effective date")
                if quantity:
                    if symbol != "BTCUSD":
                        raise ValueError("unvalued crypto fee asset")
                    coin_fees[fee_day] -= quantity
                else:
                    dollar_fees[fee_day] -= cash
            timeline.append(
                {
                    "at": at.isoformat(),
                    "id": identity,
                    "symbol": symbol,
                    "quantity": str(quantity),
                    "cash": str(cash),
                    "reserve": "0",
                }
            )
        elif kind == "JNLC":
            amount = number(row["net_amount"])
            if (
                funding_ids
                or amount <= 0
                or row.get("currency") != "USD"
                or row.get("status") != "executed"
                or row.get("symbol")
                or row.get("qty")
                or any(stamp(order["created_at"]) <= at for order in by_id.values())
            ):
                raise ValueError("only an identified initial funding journal is supported")
            funding += amount
            funding_ids.append(identity)
            timeline.append(
                {
                    "at": at.isoformat(),
                    "id": identity,
                    "symbol": "",
                    "quantity": "0",
                    "cash": "0",
                    "reserve": "0",
                    "external_cash_flow": str(amount),
                }
            )
        else:
            raise ValueError(f"unvalued broker activity type: {kind}")
    buys: dict[str, list[str]] = defaultdict(list)
    for order in by_id.values():
        quantity = number(order.get("filled_quantity", order.get("filled_qty")))
        average = order.get("filled_average_price", order.get("filled_avg_price"))
        created = stamp(order["created_at"])
        if (
            order["status"] not in FINAL
            or created > observed
            or order["side"] not in {"buy", "sell"}
            or quantity < 0
            or totals[order["id"]] != quantity
            or (
                quantity
                and (
                    average is None
                    or abs(values[order["id"]] - quantity * number(average)) > Decimal("0.00000001")
                )
            )
        ):
            raise ValueError("unresolved or incompletely valued broker order")
        if order["side"] == "buy":
            buys[str(created.astimezone(EASTERN).date())].append(order["client_order_id"])
    fee_reserves = []
    for (day, side), group in crypto.items():
        amount = (
            max(Decimal(0), group["quantity"] * CRYPTO_TAKER_FEE_RESERVE - coin_fees[day])
            * group["max_price"]
            if side == "buy"
            else max(Decimal(0), group["value"] * CRYPTO_TAKER_FEE_RESERVE - dollar_fees[day])
        ).quantize(Decimal("0.01"), rounding=ROUND_UP)
        if amount:
            fee_reserves.append(
                {
                    "at": group["at"].isoformat(),
                    "id": f"estimated-crypto-fee:{day}:{side}",
                    "symbol": "",
                    "quantity": "0",
                    "cash": "0",
                    "reserve": str(amount),
                    "basis": "unposted fee allowance at 0.25%, rounded up; not an actual fee",
                }
            )
    known_cash = sum((number(row["cash"]) for row in timeline), Decimal(0))
    cash_deficit = Decimal(0)
    if history.get("account_cash") is not None:
        if not funding_ids:
            raise ValueError("cash reconciliation requires the initial funding journal")
        discrepancy = funding + known_cash - number(history["account_cash"])
        if discrepancy < -Decimal("0.01"):
            raise ValueError("unexplained positive account cash flow requires review")
        cash_deficit = max(Decimal(0), discrepancy)
        unexplained = max(
            Decimal(0),
            cash_deficit - sum((number(row["reserve"]) for row in fee_reserves), Decimal(0)),
        )
        if unexplained:
            fee_reserves.append(
                {
                    "at": observed.isoformat(),
                    "id": "unattributed-cash-deficit-reserve",
                    "symbol": "",
                    "quantity": "0",
                    "cash": "0",
                    "reserve": str(unexplained),
                    "basis": "unexplained cash-deficit reserve; not an assigned fee or P&L",
                }
            )
    timeline.sort(key=lambda row: (row["at"], row["id"]))
    inventory: dict[str, Decimal] = defaultdict(Decimal)
    cash = reserve = peak = Decimal(0)
    for row in sorted([*timeline, *fee_reserves], key=lambda row: (row["at"], row["id"])):
        inventory[row["symbol"]] += number(row["quantity"])
        cash += number(row["cash"])
        reserve += number(row["reserve"])
        if any(value < 0 for value in inventory.values()):
            raise ValueError("history contains short or unowned inventory")
        if not any(inventory.values()):
            peak = max(peak, cash - reserve)
    if any(inventory.values()):
        raise ValueError("history does not establish a flat baseline")
    return {
        "identity": history_identity(history),
        "account_digest": history["account_digest"],
        "observed_at": observed.isoformat(),
        "broker_order_ids": sorted(by_id),
        "buy_client_ids": dict(buys),
        "timeline": timeline,
        "fee_reserves": fee_reserves,
        "external_funding": str(funding),
        "account_cash": history.get("account_cash"),
        "unexplained_cash_deficit": str(cash_deficit),
        "cash_pnl_basis": "fill cash flows and posted cash fees only; excludes funding",
        "economic_pnl_basis": "known cash P&L less conservative reserves; not actual net P&L",
        "cash_pnl": str(cash),
        "economic_pnl": str(cash - reserve),
        "peak_pnl": str(peak),
    }


def before(history: dict[str, Any], boundary: datetime) -> Decimal:
    inventory: dict[str, Decimal] = defaultdict(Decimal)
    result = Decimal(0)
    for row in sorted(
        [*history["timeline"], *history.get("fee_reserves", [])],
        key=lambda row: (row["at"], row["id"]),
    ):
        if stamp(row["at"]) >= boundary:
            break
        inventory[row["symbol"]] += number(row["quantity"])
        result += number(row["cash"]) - number(row["reserve"])
    if any(inventory.values()):
        raise ValueError("historical boundary exposure lacks a verified mark")
    return result


def save_baseline(store: EventStore, history: dict[str, Any], now: datetime) -> dict[str, Any]:
    report = value_history(history)
    if not 0 <= (now - stamp(report["observed_at"])).total_seconds() <= 60:
        raise ValueError("fresh history baseline required")
    digest = report["account_digest"]
    key = "paper-baseline:" + digest
    with store.database.begin() as connection:
        insert = pg_insert if connection.dialect.name == "postgresql" else sqlite_insert
        connection.execute(
            insert(controls)
            .values(control_key=key, control_value="{}", updated_at=now)
            .on_conflict_do_nothing(index_elements=[controls.c.control_key])
        )
        old = connection.scalar(
            select(controls.c.control_value).where(controls.c.control_key == key).with_for_update()
        )
        old = None if old == "{}" else old
        if old and stamp(json.loads(old)["observed_at"]) > stamp(report["observed_at"]):
            raise ValueError("cannot replace a newer account baseline")
        if old:
            previous_report = json.loads(old)
            timeline = {row["id"]: row for row in report["timeline"]}
            if not set(previous_report["broker_order_ids"]) <= set(
                report["broker_order_ids"]
            ) or any(timeline.get(row["id"]) != row for row in previous_report["timeline"]):
                raise ValueError("baseline cannot discard or rewrite previously valued history")
        linked = list(
            connection.execute(
                select(orders.c.client_order_id, orders.c.status, orders.c.broker_order_id)
                .join(
                    event_order_links,
                    orders.c.client_order_id == event_order_links.c.client_order_id,
                )
                .where(event_order_links.c.payload["account_digest"].as_string() == digest)
            ).mappings()
        )
        if any(
            row["status"] not in {"filled", "canceled", "expired", "rejected"} for row in linked
        ):
            raise ValueError("unresolved local history blocks baseline")
        if any(
            row["broker_order_id"] and row["broker_order_id"] not in report["broker_order_ids"]
            for row in linked
        ):
            raise ValueError("broker snapshot omits retained OMS history")
        local_ids = {row["client_order_id"] for row in linked}
        for day, identities in report["buy_client_ids"].items():
            budget = locked_session_budget(
                connection, digest, datetime.fromisoformat(day).date(), now
            )
            previous = set(budget.get("external_buy_client_ids", []))
            external = set(identities) - local_ids
            if not previous <= external:
                raise ValueError("history cannot remove previously counted BUY attempts")
            budget["total_entries_reserved"] += len(external - previous)
            budget["external_buy_client_ids"] = sorted(external)
            save_session_budget(connection, budget, now)
        _write_control(connection, key, json.dumps(report, sort_keys=True), now)
        store.audit("account_history_baseline", report, now, digest, connection)
    return report
