from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from datetime import date, datetime
from decimal import ROUND_CEILING, ROUND_DOWN, Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from tradeagent.execution_calibration import _regular_session_quote
from tradeagent.lower_execution_evidence import PointInTimeSnapshot
from tradeagent.metrics import performance_metrics
from tradeagent.portfolio import PortfolioIntent, PortfolioStrategy
from tradeagent.universe import UniverseFrame

ExecutionStyle = Literal["market", "decision_marketable_limit"]


class RegulatoryFeeSchedule(BaseModel):
    model_config = ConfigDict(frozen=True)

    sec_dollars_per_million_sold: Decimal = Decimal("20.60")
    finra_taf_per_share_sold: Decimal = Decimal("0.000195")
    finra_taf_maximum_per_trade: Decimal = Decimal("9.79")
    cat_per_share: Decimal = Decimal("0.000003")
    evidence_status: str = "unverified_current_cost_counterfactual_not_historical_charges"


DEFAULT_FEE_SCHEDULE = RegulatoryFeeSchedule()


class ExecutionEvidenceUnavailableError(ValueError):
    """A legacy result must not masquerade as validated economic accounting."""


def _require_execution_provenance() -> None:
    raise ExecutionEvidenceUnavailableError(
        "observed_execution_unavailable: legacy daily frames/snapshots do not establish raw "
        "price/share basis, causal first eligible arrival, historical quote-size normalization, "
        "corporate-action ledger, or effective-dated account/charge-date fee rules. "
        "Preserve v0.10 artifacts as superseded for these reasons; do not rerun or qualify them."
    )


class ObservedExecutionReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    strategy_id: str
    execution_style: ExecutionStyle
    cost_multiplier: Decimal = Field(ge=1)
    started_at: datetime
    ended_at: datetime
    starting_equity: Decimal
    ending_equity: Decimal
    total_return: Decimal
    annualized_return: Decimal
    annualized_volatility: Decimal
    sharpe_ratio: Decimal | None
    sortino_ratio: Decimal | None
    max_drawdown: Decimal
    turnover: Decimal
    average_gross_exposure: Decimal
    period_dates: tuple[date, ...]
    period_returns: tuple[Decimal, ...]
    orders: int
    full_fills: int
    partial_fills: int
    missed_fills: int
    spread_cost: Decimal
    delay_cost: Decimal
    slippage_cost: Decimal
    regulatory_fees: Decimal
    flattening_cost: Decimal
    open_positions: dict[str, Decimal]
    pnl_by_symbol: dict[str, Decimal]


def simulate_observed_execution(
    frames: Sequence[UniverseFrame],
    strategy: PortfolioStrategy,
    snapshots: dict[tuple[str, datetime], PointInTimeSnapshot],
    *,
    execution_style: ExecutionStyle,
    cost_multiplier: Decimal = Decimal(1),
    starting_equity: Decimal = Decimal("100000"),
    fee_schedule: RegulatoryFeeSchedule = DEFAULT_FEE_SCHEDULE,
) -> ObservedExecutionReport:
    if not frames:
        raise ValueError("observed execution requires daily frames")
    if cost_multiplier < 1:
        raise ValueError("cost multiplier cannot be below one")
    _require_execution_provenance()
    cash = starting_equity
    positions: defaultdict[str, Decimal] = defaultdict(Decimal)
    prior_closes: dict[str, Decimal] = {}
    equities = [starting_equity]
    exposures: list[Decimal] = []
    dates: list[date] = []
    pending: PortfolioIntent | None = None
    orders = full_fills = partial_fills = missed_fills = 0
    traded_notional = Decimal(0)
    spread_cost = delay_cost = slippage_cost = fees = flattening_cost = Decimal(0)
    pnl_by_symbol: defaultdict[str, Decimal] = defaultdict(Decimal)

    for frame in frames:
        closes = {bar.symbol: bar.close for bar in frame.bars}
        for symbol, quantity in positions.items():
            if symbol in prior_closes:
                pnl_by_symbol[symbol] += quantity * (closes[symbol] - prior_closes[symbol])
        pretrade_equity = cash + sum(
            (quantity * closes[symbol] for symbol, quantity in positions.items()),
            Decimal(0),
        )
        daily_fees: defaultdict[str, Decimal] = defaultdict(Decimal)
        if pending is not None:
            for symbol in sorted(closes):
                target_weight = pending.target_weights.get(symbol, Decimal(0))
                target_quantity = (
                    pretrade_equity * target_weight / closes[symbol]
                ).to_integral_value(rounding=ROUND_DOWN)
                difference = target_quantity - positions[symbol]
                if difference == 0:
                    continue
                orders += 1
                fill = _fill(
                    symbol,
                    frame.timestamp,
                    pending.timestamp,
                    abs(difference),
                    buy=difference > 0,
                    snapshots=snapshots,
                    execution_style=execution_style,
                    cost_multiplier=cost_multiplier,
                    fee_schedule=fee_schedule,
                )
                spread_cost += fill.spread_cost
                delay_cost += fill.delay_cost
                slippage_cost += fill.slippage_cost
                for fee_name, value in fill.raw_fees.items():
                    daily_fees[fee_name] += value
                if fill.quantity == abs(difference):
                    full_fills += 1
                elif fill.quantity > 0:
                    partial_fills += 1
                else:
                    missed_fills += 1
                signed_quantity = fill.quantity if difference > 0 else -fill.quantity
                positions[symbol] += signed_quantity
                notional = fill.quantity * fill.price
                traded_notional += notional
                cash += -notional if difference > 0 else notional
                execution_cost = fill.spread_cost + fill.slippage_cost
                pnl_by_symbol[symbol] -= execution_cost
        charged_fees = sum(
            (_round_up_cent(value) for value in daily_fees.values() if value > 0),
            Decimal(0),
        )
        fees += charged_fees
        cash -= charged_fees
        if charged_fees:
            pnl_by_symbol["REGULATORY_FEES"] -= charged_fees
        equity = cash + sum(
            (quantity * closes[symbol] for symbol, quantity in positions.items()),
            Decimal(0),
        )
        equities.append(equity)
        exposures.append(
            sum(
                (abs(quantity * closes[symbol]) for symbol, quantity in positions.items()),
                Decimal(0),
            )
            / equity
            if equity > 0
            else Decimal(0)
        )
        dates.append(frame.timestamp.date())
        pending = strategy.on_frame(frame)
        prior_closes = closes

    last = frames[-1]
    final_fees: defaultdict[str, Decimal] = defaultdict(Decimal)
    for symbol, quantity in tuple(positions.items()):
        if quantity <= 0:
            continue
        orders += 1
        fill = _fill(
            symbol,
            last.timestamp,
            last.timestamp,
            quantity,
            buy=False,
            snapshots=snapshots,
            execution_style="market",
            cost_multiplier=cost_multiplier,
            fee_schedule=fee_schedule,
        )
        if fill.quantity == quantity:
            full_fills += 1
        elif fill.quantity > 0:
            partial_fills += 1
        else:
            missed_fills += 1
        proceeds = fill.quantity * fill.price
        cash += proceeds
        positions[symbol] -= fill.quantity
        traded_notional += proceeds
        spread_cost += fill.spread_cost
        delay_cost += fill.delay_cost
        slippage_cost += fill.slippage_cost
        flattening_cost += fill.spread_cost + fill.slippage_cost
        for fee_name, value in fill.raw_fees.items():
            final_fees[fee_name] += value
    final_fee_charge = sum(
        (_round_up_cent(value) for value in final_fees.values() if value > 0),
        Decimal(0),
    )
    fees += final_fee_charge
    flattening_cost += final_fee_charge
    cash -= final_fee_charge
    final_equity = cash + sum(
        (quantity * last.bar_for(symbol).close for symbol, quantity in positions.items()),
        Decimal(0),
    )
    equities[-1] = final_equity
    exposures[-1] = (
        sum(
            (abs(quantity * last.bar_for(symbol).close) for symbol, quantity in positions.items()),
            Decimal(0),
        )
        / final_equity
        if final_equity > 0
        else Decimal(0)
    )
    metrics = performance_metrics(equities, traded_notional, periods_per_year=252)
    return ObservedExecutionReport(
        strategy_id=strategy.strategy_id,
        execution_style=execution_style,
        cost_multiplier=cost_multiplier,
        started_at=frames[0].timestamp,
        ended_at=frames[-1].timestamp,
        starting_equity=starting_equity,
        ending_equity=final_equity,
        total_return=metrics["total_return"],
        annualized_return=metrics["annualized_return"],
        annualized_volatility=metrics["annualized_volatility"],
        sharpe_ratio=metrics["sharpe_ratio"],
        sortino_ratio=metrics["sortino_ratio"],
        max_drawdown=metrics["max_drawdown"],
        turnover=metrics["turnover"],
        average_gross_exposure=sum(exposures, Decimal(0)) / len(exposures),
        period_dates=tuple(dates),
        period_returns=tuple(
            equities[index] / equities[index - 1] - Decimal(1) for index in range(1, len(equities))
        ),
        orders=orders,
        full_fills=full_fills,
        partial_fills=partial_fills,
        missed_fills=missed_fills,
        spread_cost=spread_cost,
        delay_cost=delay_cost,
        slippage_cost=slippage_cost,
        regulatory_fees=fees,
        flattening_cost=flattening_cost,
        open_positions={
            symbol: quantity for symbol, quantity in positions.items() if quantity != 0
        },
        pnl_by_symbol=dict(pnl_by_symbol),
    )


class _Fill(BaseModel):
    model_config = ConfigDict(frozen=True)

    quantity: Decimal
    price: Decimal
    spread_cost: Decimal
    delay_cost: Decimal
    slippage_cost: Decimal
    raw_fees: dict[str, Decimal]
    status: str = "hypothetical_current_cost_counterfactual"


def _fill(
    symbol: str,
    submitted_at: datetime,
    signal_at: datetime,
    quantity: Decimal,
    *,
    buy: bool,
    snapshots: dict[tuple[str, datetime], PointInTimeSnapshot],
    execution_style: ExecutionStyle,
    cost_multiplier: Decimal,
    fee_schedule: RegulatoryFeeSchedule,
    quote_size_units: Literal["shares", "unknown"] = "unknown",
) -> _Fill:
    if quantity <= 0 or cost_multiplier < 1:
        raise ValueError("positive quantity and cost multiplier at least one required")
    submission = snapshots.get((symbol, submitted_at))
    signal = snapshots.get((symbol, signal_at))
    if (
        submission is None
        or submission.quote_before is None
        or submission.quote_after is None
        or signal is None
        or signal.quote_before is None
        or quote_size_units != "shares"
        or submitted_at < signal_at
        or not _regular_session_quote(submitted_at, submission.quote_after)
        or signal.quote_before.timestamp > signal_at
        or (signal_at - signal.quote_before.timestamp).total_seconds() > 60
    ):
        return _Fill(
            quantity=Decimal(0),
            price=Decimal(0),
            spread_cost=Decimal(0),
            delay_cost=Decimal(0),
            slippage_cost=Decimal(0),
            raw_fees={},
            status="unavailable",
        )
    after = submission.quote_after
    signal_quote = signal.quote_before
    mid = (after.bid_price + after.ask_price) / Decimal(2)
    signal_mid = (signal_quote.bid_price + signal_quote.ask_price) / Decimal(2)
    opposite = after.ask_price if buy else after.bid_price
    displayed = after.ask_size if buy else after.bid_size
    crossing = opposite - mid if buy else mid - opposite
    # Arrival already includes the quote movement; only a distinct residual is stressed.
    slippage = mid * Decimal("0.00005")
    stressed_crossing = crossing * cost_multiplier
    stressed_slippage = slippage * cost_multiplier
    price = (
        mid + stressed_crossing + stressed_slippage
        if buy
        else mid - stressed_crossing - stressed_slippage
    )
    if execution_style == "decision_marketable_limit":
        limit = signal_quote.ask_price if buy else signal_quote.bid_price
        marketable = price <= limit if buy else price >= limit
        if not marketable:
            displayed = Decimal(0)
    filled_quantity = min(quantity, displayed)
    notional = filled_quantity * price
    raw_fees = {
        "cat": filled_quantity * fee_schedule.cat_per_share * cost_multiplier,
    }
    if not buy:
        raw_fees["sec"] = (
            notional
            * fee_schedule.sec_dollars_per_million_sold
            / Decimal(1_000_000)
            * cost_multiplier
        )
        raw_fees["taf"] = (
            min(
                filled_quantity * fee_schedule.finra_taf_per_share_sold,
                fee_schedule.finra_taf_maximum_per_trade,
            )
            * cost_multiplier
        )
    return _Fill(
        quantity=filled_quantity,
        price=price,
        spread_cost=filled_quantity * stressed_crossing,
        delay_cost=filled_quantity * ((mid - signal_mid) if buy else (signal_mid - mid)),
        slippage_cost=filled_quantity * stressed_slippage,
        raw_fees=raw_fees,
    )


def _round_up_cent(value: Decimal) -> Decimal:
    return (value / Decimal("0.01")).to_integral_value(rounding=ROUND_CEILING) * Decimal("0.01")
