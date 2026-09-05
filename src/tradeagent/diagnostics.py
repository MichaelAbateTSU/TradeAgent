from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from statistics import median

from pydantic import BaseModel, ConfigDict

from tradeagent.broker import PaperBroker
from tradeagent.config import AppConfig
from tradeagent.domain import Fill, MarketBar, Side
from tradeagent.ledger import SQLiteLedger
from tradeagent.portfolio import PortfolioEngine, PortfolioStrategy
from tradeagent.portfolio_strategy import DelayedPortfolioStrategy
from tradeagent.risk import RiskEngine
from tradeagent.universe import UniverseFrame


class TradeDiagnostic(BaseModel):
    model_config = ConfigDict(frozen=True)

    symbol: str
    entry_at: datetime
    exit_at: datetime
    quantity: Decimal
    gross_pnl: Decimal
    net_pnl: Decimal
    execution_cost: Decimal
    mfe: Decimal
    mae: Decimal
    holding_frames: int


class StrategyDiagnostics(BaseModel):
    model_config = ConfigDict(frozen=True)

    strategy_id: str
    trades: tuple[TradeDiagnostic, ...]
    gross_pnl: Decimal
    net_pnl: Decimal
    execution_cost: Decimal
    expectancy: Decimal
    median_trade: Decimal
    win_rate: Decimal
    average_win: Decimal | None
    average_loss: Decimal | None
    profit_factor: Decimal | None


def diagnose_strategy(
    frames: Sequence[UniverseFrame],
    config: AppConfig,
    strategy: PortfolioStrategy,
    *,
    delay_frames: int = 1,
) -> StrategyDiagnostics:
    broker = PaperBroker(config.broker)
    with SQLiteLedger(":memory:") as ledger:
        PortfolioEngine(
            config,
            DelayedPortfolioStrategy(
                strategy,
                delay_frames,
                intraday=config.intraday if config.intraday.enabled else None,
            ),
            broker,
            RiskEngine(config.risk),
            ledger,
        ).run(frames)
    bars = {(bar.symbol, frame.timestamp): bar for frame in frames for bar in frame.bars}
    trades = _pair_fills(broker.fills, frames, bars)
    net_values = [trade.net_pnl for trade in trades]
    gross_pnl = sum((trade.gross_pnl for trade in trades), Decimal(0))
    net_pnl = sum(net_values, Decimal(0))
    cost = sum((trade.execution_cost for trade in trades), Decimal(0))
    wins = [value for value in net_values if value > 0]
    losses = [value for value in net_values if value < 0]
    return StrategyDiagnostics(
        strategy_id=strategy.strategy_id,
        trades=tuple(trades),
        gross_pnl=gross_pnl,
        net_pnl=net_pnl,
        execution_cost=cost,
        expectancy=net_pnl / len(trades) if trades else Decimal(0),
        median_trade=Decimal(str(median(net_values))) if net_values else Decimal(0),
        win_rate=Decimal(len(wins)) / len(trades) if trades else Decimal(0),
        average_win=sum(wins, Decimal(0)) / len(wins) if wins else None,
        average_loss=sum(losses, Decimal(0)) / len(losses) if losses else None,
        profit_factor=(
            sum(wins, Decimal(0)) / abs(sum(losses, Decimal(0))) if wins and losses else None
        ),
    )


def _pair_fills(
    fills: Sequence[Fill],
    frames: Sequence[UniverseFrame],
    bars: dict[tuple[str, datetime], MarketBar],
) -> list[TradeDiagnostic]:
    entries: dict[str, Fill] = {}
    frame_indices = {frame.timestamp: index for index, frame in enumerate(frames)}
    trades: list[TradeDiagnostic] = []
    for fill in fills:
        if fill.side is Side.BUY:
            entries[fill.symbol] = fill
            continue
        entry = entries.pop(fill.symbol, None)
        if entry is None:
            continue
        entry_bar = bars[(fill.symbol, entry.filled_at)]
        exit_bar = bars[(fill.symbol, fill.filled_at)]
        entry_index = frame_indices[entry.filled_at]
        exit_index = frame_indices[fill.filled_at]
        path = [frame.bar_for(fill.symbol) for frame in frames[entry_index : exit_index + 1]]
        quantity = min(entry.quantity, fill.quantity)
        reference_entry = entry_bar.close
        reference_exit = exit_bar.close
        gross = (reference_exit - reference_entry) * quantity
        net = (fill.price - entry.price) * quantity - entry.commission - fill.commission
        trades.append(
            TradeDiagnostic(
                symbol=fill.symbol,
                entry_at=entry.filled_at,
                exit_at=fill.filled_at,
                quantity=quantity,
                gross_pnl=gross,
                net_pnl=net,
                execution_cost=gross - net,
                mfe=max(
                    ((bar.high - reference_entry) * quantity for bar in path),
                    default=Decimal(0),
                ),
                mae=min(
                    ((bar.low - reference_entry) * quantity for bar in path),
                    default=Decimal(0),
                ),
                holding_frames=exit_index - entry_index,
            )
        )
    return trades
