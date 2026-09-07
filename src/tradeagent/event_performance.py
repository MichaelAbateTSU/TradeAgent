from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from tradeagent.event_reporting import performance_labels, reporting_purpose, trade_classification

COST_MODEL_VERSION = "v20-paper-residual-20260906-v1"
RESIDUAL_RATE = Decimal("0.00005")
SELL_VALUE_RATE = Decimal("0.00002060")
SELL_SHARE_RATE = Decimal("0.000195")
SELL_MINIMUM = Decimal("0.01")
STRESS_MULTIPLIERS = {"1.5x": Decimal("1.5"), "2x": Decimal(2), "3x": Decimal(3)}


def cost_assumptions() -> dict[str, Any]:
    return {
        "version": COST_MODEL_VERSION,
        "baseline": "actual cumulative broker-paper fill VWAP, not a midpoint or quote-path return",
        "residual_slippage_rate_each_side": str(RESIDUAL_RATE),
        "sell_value_fee_rate": str(SELL_VALUE_RATE),
        "sell_per_share_fee": str(SELL_SHARE_RATE),
        "minimum_fee_per_filled_sell_order": str(SELL_MINIMUM),
        "additional_spread_deduction": "0",
        "base": {
            "price_baseline": "broker-confirmed cumulative fill VWAP",
            "residual_execution_rate_each_side": str(RESIDUAL_RATE),
            "sell_value_fee_rate": str(SELL_VALUE_RATE),
            "sell_per_share_fee": str(SELL_SHARE_RATE),
            "minimum_fee_per_filled_sell_order_usd": str(SELL_MINIMUM),
            "additional_spread_deduction": "0",
        },
        "stress_scenarios": {
            name: {
                "residual_cost_multiplier": str(multiplier),
                "regulatory_fee_multiplier": "1",
                "additional_spread_deduction": "0",
                "separate_from_base": True,
            }
            for name, multiplier in STRESS_MULTIPLIERS.items()
        },
        "benchmarks": {
            "cash": {
                "declaration": "zero-interest cash baseline",
                "capital_basis": "same allocated strategy capital",
                "timing_basis": "same reported evaluation interval",
                "pnl": "0",
            },
            "passive": {
                "declaration": "only an instrument and allocation predeclared in the cohort",
                "instrument": None,
                "capital_basis": "same allocated strategy capital",
                "timing_basis": "same reported evaluation interval",
                "pnl": None,
                "status": "unknown without a declared instrument and comparable observations",
                "retrospective_selection_permitted": False,
            },
        },
        "partial_fill_method": (
            "Cumulative filled quantity times broker VWAP once per client order ID; "
            "unfilled remainder incurs no modeled execution cost. Fee floor applies per sell order."
        ),
        "stress_method": (
            "Existing 1.5x/2x/3x scenarios multiply residual execution friction only; "
            "the same fee reserve is retained. Stress results never replace the base estimate."
        ),
        "limitations": [
            "Reserves are assumptions, not observed commissions or reconciled broker fees.",
            "Residual friction approximates omitted latency, queue position and market impact.",
            "No second spread deduction: the execution-price baseline already includes the fills.",
        ],
        "reference": "https://docs.alpaca.markets/us/docs/paper-trading",
    }


paper_cost_assumptions = cost_assumptions


def _ratio(value: Decimal | None, denominator: Decimal | None) -> str | None:
    return str(value / denominator) if value is not None and denominator else None


def _fill_price(value: Any) -> Decimal | None:
    if value is None or str(value).strip().lower() in {"", "none", "null"}:
        return None
    try:
        price = Decimal(str(value))
    except (ArithmeticError, ValueError):
        return None
    return price if price.is_finite() and price > 0 else None


def _confirmed_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unique: dict[str, dict[str, Any]] = {}
    for index, row in enumerate(rows):
        identity = str(row.get("client_order_id", f"unidentified:{index}"))
        old = unique.get(identity)
        quantity = Decimal(str((row["link"].get("broker") or {}).get("filled_quantity", 0)))
        old_quantity = (
            Decimal(str((old["link"].get("broker") or {}).get("filled_quantity", 0)))
            if old
            else Decimal(-1)
        )
        if quantity >= old_quantity:
            unique[identity] = row
    return list(unique.values())


def _economics(
    rows: list[dict[str, Any]], marks: dict[str, Decimal], capital: Decimal | None
) -> dict[str, Any]:
    cash_flow = entry_notional = exit_notional = fees = omissions = Decimal(0)
    quantities: dict[str, Decimal] = {}
    execution_costs = []
    unvalued_fills = []
    entry_price_missing = exit_price_missing = False
    for row in rows:
        broker = row["link"].get("broker")
        if not broker:
            continue
        quantity = Decimal(str(broker["filled_quantity"]))
        if not quantity:
            continue
        price = _fill_price(broker.get("filled_average_price"))
        entry = row["side"] == "buy"
        signed = quantity if entry else -quantity
        symbol = row["symbol"]
        quantities[symbol] = quantities.get(symbol, Decimal(0)) + signed
        if price is None:
            entry_price_missing |= entry
            exit_price_missing |= not entry
            unvalued_fills.append(
                {
                    "client_order_id": row.get("client_order_id"),
                    "symbol": symbol,
                    "side": row["side"],
                    "filled_quantity": str(quantity),
                    "reason": "missing_or_invalid_broker_filled_average_price",
                }
            )
        notional = quantity * price if price is not None else None
        residual = notional * RESIDUAL_RATE if notional is not None else None
        fee = (
            Decimal(0)
            if entry
            else max(SELL_MINIMUM, notional * SELL_VALUE_RATE + quantity * SELL_SHARE_RATE)
            if notional is not None
            else None
        )
        if price is not None and notional is not None and residual is not None and fee is not None:
            cash_flow -= signed * price
            omissions += residual
            fees += fee
            if entry:
                entry_notional += notional
            else:
                exit_notional += notional
        requested = row.get("quantity", broker.get("quantity"))
        execution_costs.append(
            {
                "client_order_id": row.get("client_order_id"),
                "broker_order_id": broker.get("order_id", row.get("broker_order_id")),
                "side": row["side"],
                "filled_quantity": str(quantity),
                "filled_average_price": str(price) if price is not None else None,
                "broker_status": broker.get("status"),
                "broker_fill_at": broker.get("filled_at"),
                "broker_confirmed_at": broker.get("observed_at", broker.get("updated_at")),
                "last_local_update_at": (str(row["updated_at"]) if row.get("updated_at") else None),
                "filled_notional": str(notional) if notional is not None else None,
                "partially_filled": (
                    quantity < Decimal(str(requested)) if requested is not None else None
                ),
                "residual_execution_cost": str(residual) if residual is not None else None,
                "regulatory_fee_reserve": str(fee) if fee is not None else None,
                "total_additional_cost": (
                    str(residual + fee) if residual is not None and fee is not None else None
                ),
                "valuation_state": "unvalued_fill" if price is None else "valued",
                "cost_model_version": COST_MODEL_VERSION,
            }
        )
    missing = [
        symbol for symbol, quantity in quantities.items() if quantity and symbol not in marks
    ]
    pnl = (
        None
        if missing or unvalued_fills
        else cash_flow + sum((q * marks[s] for s, q in quantities.items() if q), Decimal(0))
    )
    economic = pnl - fees - omissions if pnl is not None else None
    deployed = None if entry_price_missing else entry_notional
    exit_value = None if exit_price_missing else exit_notional
    total_fees = None if exit_price_missing else str(fees)
    return {
        "state": "UNVALUED_POSITION" if missing or unvalued_fills else "valued",
        "missing_marks": missing,
        "unvalued_fills": unvalued_fills,
        "fill_prices_complete": not unvalued_fills,
        "positions": {symbol: str(q) for symbol, q in quantities.items() if q},
        "broker_paper_pnl": str(pnl) if pnl is not None else None,
        "economic_paper_pnl": str(economic) if economic is not None else None,
        "actual_deployed_notional": str(deployed) if deployed is not None else None,
        "exit_filled_notional": str(exit_value) if exit_value is not None else None,
        "strategy_capital_denominator": str(capital) if capital is not None else None,
        "deployed_notional_denominator_method": (
            "sum of actual filled entry notionals; not the $25 cap"
        ),
        "broker_return_on_deployed_notional": _ratio(pnl, deployed),
        "economic_return_on_deployed_notional": _ratio(economic, deployed),
        "broker_return_on_strategy_capital": _ratio(pnl, capital),
        "economic_return_on_strategy_capital": _ratio(economic, capital),
        "omitted_slippage_assumption": None if unvalued_fills else str(omissions),
        "regulatory_fee_reserve": total_fees,
        "execution_costs": execution_costs,
        "stress_scenarios": {
            name: {
                "residual_cost_multiplier": str(multiplier),
                "additional_execution_cost": (
                    None if unvalued_fills else str(omissions * multiplier)
                ),
                "regulatory_fee_reserve": total_fees,
                "net_pnl": str(pnl - fees - omissions * multiplier) if pnl is not None else None,
                "return_on_deployed_notional": _ratio(
                    pnl - fees - omissions * multiplier if pnl is not None else None,
                    deployed,
                ),
                "return_on_strategy_capital": _ratio(
                    pnl - fees - omissions * multiplier if pnl is not None else None, capital
                ),
            }
            for name, multiplier in STRESS_MULTIPLIERS.items()
        },
        "cost_assumptions": cost_assumptions(),
        "cash_baseline_pnl": "0",
        "cash_benchmark": {
            "method": "zero-interest cash over the same observation interval",
            "capital": str(capital) if capital is not None else None,
            "pnl": "0",
        },
        "passive_benchmark_pnl": None,
        "passive_benchmark_status": "unknown: comparable predeclared benchmark observations absent",
        "sharpe": None,
        "statistical_status": (
            "experimental observations only; no meaningful Sharpe or qualified edge"
        ),
    }


def trade_economics(
    rows: list[dict[str, Any]],
    marks: dict[str, Decimal],
    capital: Decimal | None,
    *,
    purpose: str = "research",
) -> list[dict[str, Any]]:
    """Reconstruct long-only rounds without dropping losing or unresolved rounds."""
    rounds: list[list[dict[str, Any]]] = []
    active: dict[tuple[str, str], tuple[list[dict[str, Any]], Decimal]] = {}
    for row in _confirmed_rows(rows):
        broker = row["link"].get("broker")
        if not broker or not Decimal(str(broker["filled_quantity"])):
            continue
        key = (row["symbol"], trade_classification(row["link"]))
        if key not in active:
            group: list[dict[str, Any]] = []
            rounds.append(group)
            active[key] = group, Decimal(0)
        group, quantity = active[key]
        group.append(row)
        filled = Decimal(str(broker["filled_quantity"]))
        quantity += filled if row["side"] == "buy" else -filled
        if quantity == 0:
            del active[key]
        else:
            active[key] = group, quantity
    result = []
    for index, group in enumerate(rounds):
        classification = trade_classification(group[0]["link"])
        economics = _economics(group, marks, capital)
        closed = not economics["positions"] and group[0]["side"] == "buy"
        thesis = next(
            (
                row["link"]["thesis_outcome"]
                for row in reversed(group)
                if isinstance(row["link"].get("thesis_outcome"), dict)
                and isinstance(row["link"]["thesis_outcome"].get("invalidated"), bool)
                and row["link"]["thesis_outcome"].get("evidence")
                and row["link"]["thesis_outcome"].get("observed_at")
            ),
            None,
        )
        result.append(
            {
                **performance_labels(economics, purpose),
                "trade_id": group[0].get("client_order_id", f"unidentified-round:{index}"),
                "trade_classification": classification,
                "symbol": group[0]["symbol"],
                "state": "closed" if closed else "open_or_unresolved",
                "valuation_state": economics["state"],
                "closed_round_trips": int(closed),
                "qualifying_closed_round_trips": (
                    int(closed)
                    if purpose == "research"
                    and classification == "NEWS_STRATEGY"
                    and economics["state"] == "valued"
                    else 0
                ),
                "qualification_eligible": (
                    purpose == "research" and classification == "NEWS_STRATEGY"
                ),
                "evidence_use": (
                    "operational_equipment_test_only"
                    if classification == "EQUIPMENT_TEST"
                    else "operational_practice_only"
                    if purpose == "iex-practice"
                    else "experimental_research"
                ),
                "entry_client_order_id": group[0].get("client_order_id"),
                "order_ids": [row.get("client_order_id") for row in group],
                "entry_ticket": group[0]["link"].get("decision_ticket"),
                "entry_decision": group[0]["link"],
                "exit_decisions": [row["link"] for row in group if row["side"] == "sell"],
                "entry_at": str(group[0].get("created_at")) if group[0].get("created_at") else None,
                "exit_at": (
                    str(group[-1].get("updated_at"))
                    if closed and group[-1].get("updated_at")
                    else None
                ),
                "thesis_invalidated": thesis["invalidated"] if thesis else None,
                "thesis_outcome_evidence": thesis,
                "thesis_outcome_status": (
                    "timestamped thesis evidence recorded"
                    if thesis
                    else "unknown unless supported by timestamped thesis evidence"
                ),
            }
        )
    return result


def allocation_ledgers(
    rows: list[dict[str, Any]],
    marks: dict[str, Decimal],
    virtual_equity: Decimal | None,
    *,
    session_date: date,
    purpose: str = "research",
) -> dict[str, Any]:
    purpose = reporting_purpose({"purpose": purpose}, *(row["link"] for row in rows))
    rows = _confirmed_rows(rows)
    aggregate = _economics(rows, marks, virtual_equity)
    rounds = trade_economics(rows, marks, virtual_equity, purpose=purpose)
    ledgers = {}
    for classification in ("EQUIPMENT_TEST", "NEWS_STRATEGY", "UNCLASSIFIED"):
        classified = [row for row in rows if trade_classification(row["link"]) == classification]
        trades = [trade for trade in rounds if trade["trade_classification"] == classification]
        ledger = performance_labels(_economics(classified, marks, virtual_equity), purpose)
        ledger.update(
            trade_classification=classification,
            qualification_eligible=purpose == "research" and classification == "NEWS_STRATEGY",
            closed_round_trips=sum(trade["closed_round_trips"] for trade in trades),
            qualifying_closed_round_trips=sum(
                trade["qualifying_closed_round_trips"] for trade in trades
            ),
            trades=trades,
        )
        if classification == "EQUIPMENT_TEST":
            ledger["evidence_use"] = "operational_equipment_test_only"
        ledgers[classification] = ledger
    broker_pnl = aggregate["broker_paper_pnl"]
    economic_pnl = aggregate["economic_paper_pnl"]
    return performance_labels(
        {
            **aggregate,
            "session_date": session_date.isoformat(),
            "virtual_equity_anchor": str(virtual_equity) if virtual_equity is not None else None,
            "broker_paper_equity": (
                str(virtual_equity + Decimal(broker_pnl))
                if broker_pnl is not None and virtual_equity is not None
                else None
            ),
            "economic_paper_equity": (
                str(virtual_equity + Decimal(economic_pnl))
                if economic_pnl is not None and virtual_equity is not None
                else None
            ),
            "accounting_scope": "all broker-paper fills; not NEWS_STRATEGY performance",
            "separate_ledgers": ledgers,
            "news_strategy": ledgers["NEWS_STRATEGY"],
            "equipment_test": ledgers["EQUIPMENT_TEST"],
            "closed_round_trips": sum(trade["closed_round_trips"] for trade in rounds),
            "calibration_round_trips": ledgers["EQUIPMENT_TEST"]["closed_round_trips"],
            "qualifying_closed_round_trips": (
                ledgers["NEWS_STRATEGY"]["qualifying_closed_round_trips"]
            ),
            "turnover": _ratio(
                Decimal(aggregate["actual_deployed_notional"])
                + Decimal(aggregate["exit_filled_notional"])
                if aggregate["actual_deployed_notional"] is not None
                and aggregate["exit_filled_notional"] is not None
                else None,
                virtual_equity,
            ),
            "fee_status": (
                "current-cost conservative reserve; pending broker activity reconciliation"
            ),
            "structured_only_baseline": "recorded prospective decisions; insufficient outcomes",
            "text_only_baseline": "not evaluated; no configured inference provider",
            "with_without_overlay": "prospective ablation; insufficient outcomes",
            "matched_and_shuffled_controls": "diagnostic only; insufficient independent events",
            "inference_cost_usd": "0",
            "fixed_service_cost_usd": None,
            "net_product_economics": None,
        },
        purpose,
    )
