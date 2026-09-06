from __future__ import annotations

import json
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from math import sqrt
from pathlib import Path
from statistics import mean, stdev
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict

from tradeagent.alpaca import HistoricalQuote
from tradeagent.diagnostics import StrategyDiagnostics, TradeDiagnostic
from tradeagent.domain import Side
from tradeagent.execution_evidence import EvidenceAnchor
from tradeagent.squeeze_external import FrozenSqueezeExternalReport


class SimulatedExecution(BaseModel):
    model_config = ConfigDict(frozen=True)

    side: Side
    submitted_at: datetime
    fill_price: Decimal | None
    filled_quantity: Decimal
    status: str
    displayed_size: Decimal


class ObservedTradeCalibration(BaseModel):
    model_config = ConfigDict(frozen=True)

    symbol: str
    timeframe: str
    entry_at: datetime
    exit_at: datetime
    quantity: Decimal
    market_entry: SimulatedExecution
    market_exit: SimulatedExecution
    marketable_limit_entry: SimulatedExecution
    marketable_limit_exit: SimulatedExecution
    gross_edge: Decimal | None
    spread_cost: Decimal | None
    delay_cost: Decimal | None
    slippage: Decimal
    fees: Decimal
    flattening_cost: Decimal | None
    market_net_edge: Decimal | None
    marketable_limit_net_edge: Decimal | None


class ObservedCellCalibration(BaseModel):
    model_config = ConfigDict(frozen=True)

    symbol: str
    timeframe: str
    trades: int
    quote_complete_trades: int
    market_filled_round_trips: int
    marketable_limit_filled_round_trips: int
    marketable_limit_missed_round_trips: int
    estimated_cost: Decimal
    observed_market_cost: Decimal
    observed_gross_edge: Decimal
    observed_market_net_edge: Decimal
    observed_marketable_limit_net_edge: Decimal
    observed_market_date_clustered_sharpe: Decimal | None
    trade_details: tuple[ObservedTradeCalibration, ...]


class ExecutionCalibrationReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    generated_at: datetime
    evidence_feed: str
    cost_model_status: str
    market_order_assumption: str
    marketable_limit_assumption: str
    cells: tuple[ObservedCellCalibration, ...]
    total_trades: int
    quote_complete_trades: int
    marketable_limit_missed_round_trips: int
    estimated_cost: Decimal
    observed_market_cost: Decimal


def calibrate_squeeze_execution(
    report: FrozenSqueezeExternalReport,
    evidence_path: Path,
    *,
    generated_at: datetime,
) -> ExecutionCalibrationReport:
    quotes = _load_quotes(evidence_path)
    cells: list[ObservedCellCalibration] = []
    for result in report.results:
        cells.append(
            _calibrate_diagnostics(
                result.diagnostics,
                result.timeframe,
                quotes,
                symbol=result.symbol,
            )
        )
    return ExecutionCalibrationReport(
        generated_at=generated_at,
        evidence_feed="alpaca-sip",
        cost_model_status=(
            "observed top-of-book for covered trades; "
            "depth beyond displayed size remains provisional"
        ),
        market_order_assumption=(
            "Fill at first SIP quote at/after submission, capped by displayed opposite-side size."
        ),
        marketable_limit_assumption=(
            "Limit fixed to the decision-time opposite quote; fill only when still marketable "
            "at submission and displayed size covers the entire order."
        ),
        cells=tuple(cells),
        total_trades=sum(cell.trades for cell in cells),
        quote_complete_trades=sum(cell.quote_complete_trades for cell in cells),
        marketable_limit_missed_round_trips=sum(
            cell.marketable_limit_missed_round_trips for cell in cells
        ),
        estimated_cost=sum((cell.estimated_cost for cell in cells), Decimal(0)),
        observed_market_cost=sum((cell.observed_market_cost for cell in cells), Decimal(0)),
    )


def calibrate_strategy_execution(
    diagnostics: StrategyDiagnostics,
    timeframe: str,
    evidence_path: Path,
) -> ObservedCellCalibration:
    symbols = {trade.symbol for trade in diagnostics.trades}
    symbol = next(iter(symbols)) if len(symbols) == 1 else "MULTI"
    return _calibrate_diagnostics(
        diagnostics,
        timeframe,
        _load_quotes(evidence_path),
        symbol=symbol,
    )


def _calibrate_diagnostics(
    diagnostics: StrategyDiagnostics,
    timeframe: str,
    quotes: dict[tuple[str, str, str, datetime], tuple[HistoricalQuote, ...]],
    *,
    symbol: str,
) -> ObservedCellCalibration:
    details = tuple(
        _calibrate_trade(
            trade,
            timeframe,
            quotes,
        )
        for trade in diagnostics.trades
    )
    observed_gross = sum(
        (trade.gross_edge or Decimal(0) for trade in details),
        Decimal(0),
    )
    market_net = sum(
        (trade.market_net_edge or Decimal(0) for trade in details),
        Decimal(0),
    )
    limit_net = sum(
        (trade.marketable_limit_net_edge or Decimal(0) for trade in details),
        Decimal(0),
    )
    complete = [
        trade
        for trade in details
        if trade.gross_edge is not None and trade.market_net_edge is not None
    ]
    return ObservedCellCalibration(
        symbol=symbol,
        timeframe=timeframe,
        trades=len(details),
        quote_complete_trades=len(complete),
        market_filled_round_trips=sum(trade.market_net_edge is not None for trade in details),
        marketable_limit_filled_round_trips=sum(
            trade.marketable_limit_net_edge is not None for trade in details
        ),
        marketable_limit_missed_round_trips=sum(
            trade.marketable_limit_net_edge is None for trade in details
        ),
        estimated_cost=diagnostics.execution_cost,
        observed_market_cost=observed_gross - market_net,
        observed_gross_edge=observed_gross,
        observed_market_net_edge=market_net,
        observed_marketable_limit_net_edge=limit_net,
        observed_market_date_clustered_sharpe=_date_clustered_sharpe(complete),
        trade_details=details,
    )


def _calibrate_trade(
    trade: TradeDiagnostic,
    timeframe: str,
    quotes: dict[tuple[str, str, str, datetime], tuple[HistoricalQuote, ...]],
) -> ObservedTradeCalibration:
    interval_minutes = {"5Min": 5, "30Min": 30, "1Hour": 60}[timeframe]
    entry_signal_at = trade.entry_at - timedelta(minutes=interval_minutes)
    exit_signal_at = trade.exit_at - timedelta(minutes=interval_minutes)
    entry_signal_quote = _decision_quote(
        quotes.get((trade.symbol, timeframe, "entry_signal", entry_signal_at), ()),
        entry_signal_at,
    )
    exit_signal_quote = _decision_quote(
        quotes.get((trade.symbol, timeframe, "exit_signal", exit_signal_at), ()),
        exit_signal_at,
    )
    entry_submission_quote = _submission_quote(
        quotes.get((trade.symbol, timeframe, "entry_submission", trade.entry_at), ()),
        trade.entry_at,
    )
    exit_submission_quote = _submission_quote(
        quotes.get((trade.symbol, timeframe, "exit_submission", trade.exit_at), ()),
        trade.exit_at,
    )
    market_entry = _market_execution(
        Side.BUY,
        trade.quantity,
        trade.entry_at,
        entry_submission_quote,
    )
    market_exit = _market_execution(
        Side.SELL,
        trade.quantity,
        trade.exit_at,
        exit_submission_quote,
    )
    limit_entry = _marketable_limit_execution(
        Side.BUY,
        trade.quantity,
        trade.entry_at,
        entry_signal_quote,
        entry_submission_quote,
    )
    limit_exit = _marketable_limit_execution(
        Side.SELL,
        trade.quantity,
        trade.exit_at,
        exit_signal_quote,
        exit_submission_quote,
    )
    gross_edge: Decimal | None = None
    spread_cost: Decimal | None = None
    delay_cost: Decimal | None = None
    flattening_cost: Decimal | None = None
    if (
        entry_signal_quote is not None
        and exit_signal_quote is not None
        and entry_submission_quote is not None
        and exit_submission_quote is not None
    ):
        entry_signal_mid = _mid(entry_signal_quote)
        exit_signal_mid = _mid(exit_signal_quote)
        entry_submission_mid = _mid(entry_submission_quote)
        exit_submission_mid = _mid(exit_submission_quote)
        gross_edge = (exit_submission_mid - entry_submission_mid) * trade.quantity
        spread_cost = (
            entry_submission_quote.ask_price
            - entry_submission_mid
            + exit_submission_mid
            - exit_submission_quote.bid_price
        ) * trade.quantity
        delay_cost = (
            entry_submission_mid - entry_signal_mid + exit_signal_mid - exit_submission_mid
        ) * trade.quantity
        flattening_cost = (
            (exit_submission_mid - exit_submission_quote.bid_price) * trade.quantity
            if trade.exit_at.astimezone(ZoneInfo("America/New_York")).time() >= time(15, 50)
            else Decimal(0)
        )
    market_net = _round_trip_net(market_entry, market_exit)
    limit_net = _round_trip_net(limit_entry, limit_exit)
    return ObservedTradeCalibration(
        symbol=trade.symbol,
        timeframe=timeframe,
        entry_at=trade.entry_at,
        exit_at=trade.exit_at,
        quantity=trade.quantity,
        market_entry=market_entry,
        market_exit=market_exit,
        marketable_limit_entry=limit_entry,
        marketable_limit_exit=limit_exit,
        gross_edge=gross_edge,
        spread_cost=spread_cost,
        delay_cost=delay_cost,
        slippage=Decimal(0),
        fees=Decimal(0),
        flattening_cost=flattening_cost,
        market_net_edge=market_net,
        marketable_limit_net_edge=limit_net,
    )


def _market_execution(
    side: Side,
    quantity: Decimal,
    submitted_at: datetime,
    quote: HistoricalQuote | None,
) -> SimulatedExecution:
    if quote is None:
        return SimulatedExecution(
            side=side,
            submitted_at=submitted_at,
            fill_price=None,
            filled_quantity=Decimal(0),
            status="missing_quote",
            displayed_size=Decimal(0),
        )
    price = quote.ask_price if side is Side.BUY else quote.bid_price
    size = quote.ask_size if side is Side.BUY else quote.bid_size
    filled = min(quantity, size)
    return SimulatedExecution(
        side=side,
        submitted_at=submitted_at,
        fill_price=price if filled > 0 else None,
        filled_quantity=filled,
        status=("filled" if filled == quantity else "partial" if filled > 0 else "missed"),
        displayed_size=size,
    )


def _marketable_limit_execution(
    side: Side,
    quantity: Decimal,
    submitted_at: datetime,
    decision_quote: HistoricalQuote | None,
    submission_quote: HistoricalQuote | None,
) -> SimulatedExecution:
    if decision_quote is None or submission_quote is None:
        return SimulatedExecution(
            side=side,
            submitted_at=submitted_at,
            fill_price=None,
            filled_quantity=Decimal(0),
            status="missing_quote",
            displayed_size=Decimal(0),
        )
    limit = decision_quote.ask_price if side is Side.BUY else decision_quote.bid_price
    current = submission_quote.ask_price if side is Side.BUY else submission_quote.bid_price
    size = submission_quote.ask_size if side is Side.BUY else submission_quote.bid_size
    still_marketable = current <= limit if side is Side.BUY else current >= limit
    filled = min(quantity, size) if still_marketable else Decimal(0)
    return SimulatedExecution(
        side=side,
        submitted_at=submitted_at,
        fill_price=current if filled > 0 else None,
        filled_quantity=filled,
        status=("filled" if filled == quantity else "partial" if filled > 0 else "missed"),
        displayed_size=size,
    )


def _round_trip_net(
    entry: SimulatedExecution,
    exit: SimulatedExecution,
) -> Decimal | None:
    if (
        entry.status != "filled"
        or exit.status != "filled"
        or entry.fill_price is None
        or exit.fill_price is None
    ):
        return None
    return (exit.fill_price - entry.fill_price) * entry.filled_quantity


def _load_quotes(
    path: Path,
) -> dict[tuple[str, str, str, datetime], tuple[HistoricalQuote, ...]]:
    collected: defaultdict[
        tuple[str, str, str, datetime],
        list[HistoricalQuote],
    ] = defaultdict(list)
    with path.open(encoding="utf-8") as source:
        for line in source:
            payload = json.loads(line)
            if payload["record_type"] != "quote":
                continue
            anchor = EvidenceAnchor.model_validate(payload["anchor"])
            key = (
                anchor.symbol,
                anchor.timeframe,
                anchor.anchor_type,
                anchor.timestamp,
            )
            collected[key].append(HistoricalQuote.model_validate(payload["record"]))
    return {
        key: tuple(sorted(values, key=lambda quote: quote.timestamp))
        for key, values in collected.items()
    }


def _decision_quote(
    quotes: tuple[HistoricalQuote, ...],
    timestamp: datetime,
) -> HistoricalQuote | None:
    before = [quote for quote in quotes if quote.timestamp <= timestamp]
    return before[-1] if before else (quotes[0] if quotes else None)


def _submission_quote(
    quotes: tuple[HistoricalQuote, ...],
    timestamp: datetime,
) -> HistoricalQuote | None:
    return next(
        (quote for quote in quotes if quote.timestamp >= timestamp),
        quotes[-1] if quotes else None,
    )


def _mid(quote: HistoricalQuote | None) -> Decimal:
    if quote is None:
        raise ValueError("quote is required")
    return (quote.bid_price + quote.ask_price) / Decimal(2)


def _date_clustered_sharpe(
    trades: list[ObservedTradeCalibration],
) -> Decimal | None:
    by_date: defaultdict[date, Decimal] = defaultdict(Decimal)
    for trade in trades:
        if (
            trade.market_net_edge is not None
            and trade.market_entry.fill_price is not None
            and trade.quantity > 0
        ):
            entry_notional = trade.market_entry.fill_price * trade.quantity
            by_date[trade.exit_at.date()] += trade.market_net_edge / entry_notional
    values = [float(value) for value in by_date.values()]
    if len(values) < 2 or stdev(values) == 0:
        return None
    return Decimal(str(mean(values) / stdev(values) * sqrt(252)))
