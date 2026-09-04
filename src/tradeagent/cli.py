from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from tradeagent.broker import PaperBroker
from tradeagent.config import AppConfig, BrokerConfig, StrategyConfig
from tradeagent.data import read_bars, synthetic_bars
from tradeagent.engine import TradingEngine
from tradeagent.ledger import SQLiteLedger
from tradeagent.risk import RiskEngine
from tradeagent.strategy import SmaCrossoverStrategy


def _json(value: Any) -> str:
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    return json.dumps(value, indent=2, sort_keys=True, default=str)


def _build_engine(config: AppConfig, ledger: SQLiteLedger) -> TradingEngine:
    return TradingEngine(
        config=config,
        strategy=SmaCrossoverStrategy(config.strategy),
        broker=PaperBroker(config.broker),
        risk=RiskEngine(config.risk),
        ledger=ledger,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="tradeagent",
        description="Safety-first autonomous paper-trading agent (fake money only).",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    backtest = subparsers.add_parser("backtest", help="run an offline synthetic backtest")
    backtest.add_argument("--symbol", default="SPY")
    backtest.add_argument("--bars", type=int, default=500)
    backtest.add_argument("--seed", type=int, default=7)
    backtest.add_argument("--cash", type=str, default="100000")
    backtest.add_argument("--fast-window", type=int, default=20)
    backtest.add_argument("--slow-window", type=int, default=50)

    paper = subparsers.add_parser("paper", help="run a persistent fake-money simulation")
    source = paper.add_mutually_exclusive_group()
    source.add_argument("--csv", type=Path)
    source.add_argument("--synthetic-bars", type=int, default=500)
    paper.add_argument("--symbol", default="SPY")
    paper.add_argument("--seed", type=int, default=7)
    paper.add_argument("--database", type=Path, default=Path("data/tradeagent.db"))

    status = subparsers.add_parser("status", help="inspect the local audit ledger")
    status.add_argument("--database", type=Path, default=Path("data/tradeagent.db"))
    status.add_argument("--limit", type=int, default=10)
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    args = _parser().parse_args(argv)
    if args.command == "status":
        with SQLiteLedger(args.database) as ledger:
            status = {
                "event_count": ledger.event_count(),
                "events": list(ledger.events(limit=args.limit)),
            }
            print(_json(status))
        return

    if args.command == "backtest":
        config = AppConfig(
            broker=BrokerConfig(starting_cash=args.cash),
            strategy=StrategyConfig(
                fast_window=args.fast_window,
                slow_window=args.slow_window,
            ),
        )
        with SQLiteLedger(":memory:") as ledger:
            report = _build_engine(config, ledger).run(
                synthetic_bars(symbol=args.symbol, count=args.bars, seed=args.seed)
            )
        print(_json(report))
        return

    config = AppConfig(database_path=args.database)
    bars = (
        read_bars(args.csv, symbol=args.symbol)
        if args.csv
        else synthetic_bars(symbol=args.symbol, count=args.synthetic_bars, seed=args.seed)
    )
    with SQLiteLedger(config.database_path) as ledger:
        report = _build_engine(config, ledger).run(bars)
        print(_json(report))


if __name__ == "__main__":
    main()
