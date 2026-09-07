from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from tradeagent.event_reporting import is_calibration, performance_labels, reporting_purpose


def allocation_ledgers(
    rows: list[dict[str, Any]],
    marks: dict[str, Decimal],
    virtual_equity: Decimal,
    *,
    session_date: date,
    purpose: str = "research",
) -> dict[str, Any]:
    purpose = reporting_purpose({"purpose": purpose}, *(row["link"] for row in rows))
    cash = virtual_equity
    quantities: dict[str, Decimal] = {}
    calibration_positions: set[str] = set()
    modeled_omissions = Decimal(0)
    gross_notional = Decimal(0)
    fees = Decimal(0)
    closed = 0
    calibration_closed = 0
    for row in rows:
        broker = row["link"].get("broker")
        if not broker:
            continue
        quantity = Decimal(str(broker["filled_quantity"]))
        if not quantity:
            continue
        price = Decimal(str(broker["filled_average_price"]))
        symbol = row["symbol"]
        if is_calibration(row["link"]):
            calibration_positions.add(symbol)
        signed = quantity if row["side"] == "buy" else -quantity
        cash -= signed * price
        quantities[symbol] = quantities.get(symbol, Decimal(0)) + signed
        gross_notional += quantity * price
        # Separately labeled conservative paper omissions, not a rewrite of reported broker fills.
        modeled_omissions += quantity * price * Decimal("0.00005")
        if row["side"] == "sell":
            fees += max(
                Decimal("0.01"),
                quantity * price * Decimal("0.00002060") + quantity * Decimal("0.000195"),
            )
            if quantities[symbol] == 0:
                closed += 1
                if symbol in calibration_positions:
                    calibration_closed += 1
                    calibration_positions.remove(symbol)
    missing_marks = [symbol for symbol, q in quantities.items() if q and symbol not in marks]
    if missing_marks:
        return performance_labels(
            {
                "state": "UNVALUED_POSITION",
                "missing_marks": missing_marks,
                "broker_paper_pnl": None,
                "economic_paper_pnl": None,
                "closed_round_trips": closed,
                "calibration_round_trips": calibration_closed,
                "qualifying_closed_round_trips": closed - calibration_closed,
            },
            purpose,
        )
    equity = cash + sum((q * marks[s] for s, q in quantities.items() if q), Decimal(0))
    economic_equity = equity - fees - modeled_omissions
    result = {
        "state": "valued",
        "session_date": session_date.isoformat(),
        "virtual_equity_anchor": str(virtual_equity),
        "broker_paper_equity": str(equity),
        "economic_paper_equity": str(economic_equity),
        "broker_paper_pnl": str(equity - virtual_equity),
        "economic_paper_pnl": str(economic_equity - virtual_equity),
        "omitted_slippage_assumption": str(modeled_omissions),
        "regulatory_fee_reserve": str(fees),
        "fee_status": "current-cost conservative reserve; pending broker activity reconciliation",
        "closed_round_trips": closed,
        "calibration_round_trips": calibration_closed,
        "qualifying_closed_round_trips": closed - calibration_closed,
        "turnover": str(gross_notional / virtual_equity),
        "positions": {s: str(q) for s, q in quantities.items() if q},
        "cash_baseline_pnl": "0",
        "passive_benchmark_pnl": None,
        "structured_only_baseline": "recorded prospective decisions; insufficient outcomes",
        "text_only_baseline": "not evaluated; no configured inference provider",
        "with_without_overlay": "prospective ablation; insufficient outcomes",
        "matched_and_shuffled_controls": "diagnostic only; insufficient independent events",
        "inference_cost_usd": "0",
        "fixed_service_cost_usd": None,
        "net_product_economics": None,
        "qualification": "unproven",
    }
    return performance_labels(result, purpose)
