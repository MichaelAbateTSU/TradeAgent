from __future__ import annotations

import argparse
import json
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from tradeagent.alpaca import AlpacaDataClient, AlpacaDataSettings
from tradeagent.broker import PaperBroker
from tradeagent.config import AppConfig, BrokerConfig, StrategyConfig
from tradeagent.data import read_bars, synthetic_bars, write_bars
from tradeagent.domain import PaperBrokerState
from tradeagent.engine import TradingEngine
from tradeagent.ledger import SQLiteLedger
from tradeagent.research import (
    ExperimentRegistry,
    WalkForwardConfig,
    evaluate_research_suite,
)
from tradeagent.risk import RiskEngine
from tradeagent.strategy import (
    ConstantWeightStrategy,
    MeanReversionStrategy,
    SmaCrossoverStrategy,
    Strategy,
    VolatilityTargetTrendStrategy,
)


def _json(value: Any) -> str:
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json")
    return json.dumps(
        value,
        indent=2,
        sort_keys=True,
        default=lambda item: (
            item.model_dump(mode="json") if isinstance(item, BaseModel) else str(item)
        ),
    )


def _build_engine(
    config: AppConfig,
    ledger: SQLiteLedger,
    *,
    strategy: Strategy | None = None,
    broker: PaperBroker | None = None,
    risk: RiskEngine | None = None,
) -> TradingEngine:
    return TradingEngine(
        config=config,
        strategy=strategy or SmaCrossoverStrategy(config.strategy),
        broker=broker or PaperBroker(config.broker),
        risk=risk or RiskEngine(config.risk),
        ledger=ledger,
    )


def _strategy_factory(name: str, config: AppConfig) -> Callable[[], Strategy]:
    if name == "cash":
        return lambda: ConstantWeightStrategy("cash-v1", target_weight=Decimal(0))
    if name == "buy-and-hold":
        return lambda: ConstantWeightStrategy(
            "buy-and-hold-v1", target_weight=config.strategy.target_weight
        )
    if name == "volatility-trend":
        return lambda: VolatilityTargetTrendStrategy(config.strategy)
    if name == "mean-reversion":
        return lambda: MeanReversionStrategy(config.strategy)
    return lambda: SmaCrossoverStrategy(config.strategy)


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
    backtest.add_argument("--csv", type=Path)

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

    kill_switch = subparsers.add_parser(
        "kill-switch", help="inspect or change the durable paper kill switch"
    )
    kill_switch.add_argument("action", choices=["status", "activate", "reset"])
    kill_switch.add_argument("--database", type=Path, default=Path("data/tradeagent.db"))
    kill_switch.add_argument(
        "--confirm-reconciled",
        action="store_true",
        help="required to reset after account and data reconciliation",
    )

    serve = subparsers.add_parser("serve", help="serve the localhost-only read-only paper console")
    serve.add_argument(
        "--host",
        choices=["127.0.0.1", "localhost", "0.0.0.0"],
        default="127.0.0.1",
    )
    serve.add_argument("--port", type=int, default=8000)
    serve.add_argument("--database", type=Path, default=Path("data/tradeagent.db"))
    serve.add_argument("--experiments", type=Path, default=Path("data/experiments.db"))

    evaluate = subparsers.add_parser(
        "evaluate", help="run cost-stressed rolling walk-forward research"
    )
    evaluate.add_argument(
        "--strategy",
        choices=[
            "sma",
            "volatility-trend",
            "mean-reversion",
            "buy-and-hold",
            "cash",
        ],
        default="sma",
    )
    evaluate.add_argument("--symbol", default="SPY")
    evaluate.add_argument("--bars", type=int, default=1000)
    evaluate.add_argument("--seed", type=int, default=7)
    evaluate.add_argument("--fast-window", type=int, default=20)
    evaluate.add_argument("--slow-window", type=int, default=50)
    evaluate.add_argument("--training-bars", type=int, default=252)
    evaluate.add_argument("--testing-bars", type=int, default=63)
    evaluate.add_argument("--step-bars", type=int, default=63)
    evaluate.add_argument("--embargo-bars", type=int, default=5)
    evaluate.add_argument("--csv", type=Path)
    evaluate.add_argument("--database", type=Path, default=Path("data/experiments.db"))

    download = subparsers.add_parser(
        "download-alpaca", help="download adjusted historical bars to canonical CSV"
    )
    download.add_argument("--symbol", required=True)
    download.add_argument("--start", required=True, help="ISO date or timestamp")
    download.add_argument("--end", required=True, help="ISO date or timestamp")
    download.add_argument("--timeframe", choices=["1Day", "1Hour", "5Min"], default="1Day")
    download.add_argument("--output", type=Path, required=True)
    download.add_argument("--overwrite", action="store_true")
    return parser


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


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

    if args.command == "kill-switch":
        with SQLiteLedger(args.database) as ledger:
            current = ledger.get_control("kill_switch", default="inactive")
            if args.action == "status":
                print(_json({"kill_switch": current}))
                return
            if args.action == "reset" and not args.confirm_reconciled:
                raise ValueError(
                    "reset requires --confirm-reconciled after account and data checks"
                )
            value = "active" if args.action == "activate" else "inactive"
            ledger.set_control(
                "kill_switch",
                value,
                occurred_at=datetime.now(UTC),
                trace_id=f"operator:kill-switch:{value}",
            )
            print(_json({"kill_switch": value}))
        return

    if args.command == "serve":
        import uvicorn

        from tradeagent.api import create_app

        app = create_app(
            ledger_path=args.database,
            experiments_path=args.experiments,
        )
        uvicorn.run(app, host=args.host, port=args.port)
        return

    if args.command == "download-alpaca":
        settings = AlpacaDataSettings.model_validate({})
        with AlpacaDataClient(settings) as client:
            bars = client.bars(
                args.symbol,
                start=_parse_utc(args.start),
                end=_parse_utc(args.end),
                timeframe=args.timeframe,
            )
            count = write_bars(args.output, bars, overwrite=args.overwrite)
        print(
            _json(
                {
                    "provider": "alpaca",
                    "feed": settings.feed,
                    "symbol": args.symbol.upper(),
                    "rows": count,
                    "output": str(args.output),
                }
            )
        )
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
            backtest_bars = (
                read_bars(args.csv, symbol=args.symbol)
                if args.csv
                else synthetic_bars(
                    symbol=args.symbol,
                    count=args.bars,
                    seed=args.seed,
                )
            )
            backtest_report = _build_engine(config, ledger).run(backtest_bars)
        print(_json(backtest_report))
        return

    if args.command == "evaluate":
        config = AppConfig(
            strategy=StrategyConfig(
                fast_window=args.fast_window,
                slow_window=args.slow_window,
            )
        )
        evaluation_bars = list(
            read_bars(args.csv, symbol=args.symbol)
            if args.csv
            else synthetic_bars(
                symbol=args.symbol,
                count=args.bars,
                seed=args.seed,
            )
        )
        walk_forward = WalkForwardConfig(
            training_bars=args.training_bars,
            testing_bars=args.testing_bars,
            step_bars=args.step_bars,
            embargo_bars=args.embargo_bars,
            warmup_bars=min(args.slow_window, args.training_bars),
        )
        research_report = evaluate_research_suite(
            evaluation_bars,
            config,
            walk_forward,
            _strategy_factory(args.strategy, config),
            random_seed=args.seed,
        )
        with ExperimentRegistry(args.database) as registry:
            experiment_id = registry.record(research_report)
        print(_json({"experiment_id": experiment_id, "report": research_report}))
        return

    config = AppConfig(database_path=args.database)
    all_bars = list(
        read_bars(args.csv, symbol=args.symbol)
        if args.csv
        else synthetic_bars(
            symbol=args.symbol,
            count=args.synthetic_bars,
            seed=args.seed,
        )
    )
    if not all_bars:
        raise ValueError("paper simulation requires at least one market bar")
    with SQLiteLedger(config.database_path) as ledger:
        progress = ledger.latest_event("engine_progress")
        checkpoint = ledger.latest_event("broker_checkpoint")
        broker = (
            PaperBroker.from_state(
                config.broker,
                PaperBrokerState.model_validate(checkpoint["payload"]),
            )
            if checkpoint is not None
            else PaperBroker(config.broker)
        )
        strategy = SmaCrossoverStrategy(config.strategy)
        risk = RiskEngine(config.risk)
        if ledger.get_control("kill_switch", default="inactive") == "active":
            risk.activate_kill_switch()
        last_timestamp = (
            datetime.fromisoformat(str(progress["payload"]["timestamp"]))
            if progress is not None
            else None
        )
        processed = (
            [bar for bar in all_bars if bar.timestamp <= last_timestamp]
            if last_timestamp is not None
            else []
        )
        checkpoint_time = (
            datetime.fromisoformat(str(checkpoint["occurred_at"]))
            if checkpoint is not None
            else None
        )
        for historical_bar in processed:
            strategy.on_bar(historical_bar)
            if checkpoint_time is None or historical_bar.timestamp > checkpoint_time:
                broker.mark(historical_bar)
        pending = (
            [bar for bar in all_bars if bar.timestamp > last_timestamp]
            if last_timestamp is not None
            else all_bars
        )
        if not pending:
            as_of = last_timestamp or all_bars[-1].timestamp
            print(
                _json(
                    {
                        "mode": "paper",
                        "status": "up_to_date",
                        "as_of": as_of,
                        "account": broker.account(as_of),
                    }
                )
            )
            return
        paper_report = _build_engine(
            config,
            ledger,
            strategy=strategy,
            broker=broker,
            risk=risk,
        ).run(pending)
        print(_json(paper_report))


if __name__ == "__main__":
    main()
