from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from tradeagent.advanced_strategy import (
    DonchianAtrBreakoutStrategy,
    NoiseAreaMomentumStrategy,
    VolatilitySqueezeBreakoutStrategy,
)
from tradeagent.alpaca import AlpacaDataClient, AlpacaDataSettings
from tradeagent.alpaca_news import AlpacaNewsClient
from tradeagent.alpaca_paper import AlpacaPaperClient, AlpacaPaperSettings
from tradeagent.alpaca_stream import AlpacaMarketStream, AlpacaStreamSettings
from tradeagent.broker import PaperBroker
from tradeagent.candle_strategy import CandlePattern, CandleTrackingStrategy
from tradeagent.config import AppConfig, BrokerConfig, StrategyConfig, config_fingerprint
from tradeagent.data import read_bars, synthetic_bars, write_bars
from tradeagent.data_quality import analyze_dataset
from tradeagent.diagnostics import diagnose_strategy
from tradeagent.domain import PaperBrokerState
from tradeagent.economic_ml import (
    build_economic_events,
    evaluate_economic_ml,
)
from tradeagent.economic_ml import (
    report_hash as economic_ml_report_hash,
)
from tradeagent.engine import TradingEngine
from tradeagent.execution_calibration import calibrate_squeeze_execution
from tradeagent.execution_evidence import (
    collect_execution_evidence,
    squeeze_evidence_anchors,
    write_evidence_manifest,
)
from tradeagent.holdout import (
    HOLDOUT_AUTHORIZATION,
    development_frames,
    load_holdout_manifest,
    open_holdout_once,
    seal_holdout,
)
from tradeagent.intraday import NyseSessionCalendar, regular_session_frames
from tradeagent.intraday_strategy import (
    IntradayEqualWeightBenchmark,
    IntradayStrategyConfig,
    OpeningRangeBreakoutStrategy,
    RegimeFilteredMomentumStrategy,
    SessionVwapMeanReversionStrategy,
)
from tradeagent.ledger import SQLiteLedger
from tradeagent.live_shadow import LiveShadowDecisionProcessor
from tradeagent.lower_turnover_research import (
    evaluate_lower_turnover_families,
)
from tradeagent.lower_turnover_research import (
    report_hash as lower_turnover_report_hash,
)
from tradeagent.meta_label import (
    TrendPullbackCandidateStrategy,
    build_meta_label_events,
    evaluate_meta_labels,
)
from tradeagent.monitor import monitor_take_profit
from tradeagent.news import NewsBlackoutPolicy, NewsContextService, NewsRepository
from tradeagent.news_worker import NewsWorker, NewsWorkerSettings
from tradeagent.notifications import (
    EmailSettings,
    NotificationDispatcher,
    ResendEmailProvider,
    RoundTripNotificationRepository,
)
from tradeagent.notifier import NotifierService
from tradeagent.oms import PaperOrderManager
from tradeagent.persistence import Database, ProductionRepository
from tradeagent.portfolio import PortfolioStrategy
from tradeagent.portfolio_research import evaluate_portfolio_suite
from tradeagent.portfolio_strategy import (
    CrossSectionalMomentumStrategy,
    EqualWeightPortfolioStrategy,
    PortfolioStrategyConfig,
)
from tradeagent.research import (
    ExperimentRegistry,
    WalkForwardConfig,
    evaluate_research_suite,
)
from tradeagent.research_dataset import (
    V09_ETF_UNIVERSE,
    V09_TIMEFRAMES,
    build_v09_bar_dataset,
    write_dataset_manifest,
)
from tradeagent.risk import RiskEngine
from tradeagent.runtime import (
    ProductionPaperReconciler,
    run_shadow_runtime,
)
from tradeagent.scheduler import ReconciliationScheduler
from tradeagent.squeeze_external import (
    FrozenSqueezeExternalReport,
    evaluate_frozen_squeeze_matrix,
)
from tradeagent.squeeze_external import (
    report_hash as squeeze_report_hash,
)
from tradeagent.strategy import (
    ConstantWeightStrategy,
    DelayedStrategy,
    MeanReversionStrategy,
    SmaCrossoverStrategy,
    Strategy,
    VolatilityTargetTrendStrategy,
)
from tradeagent.universe import align_universe, load_universe, symbol_filename
from tradeagent.worker import AutonomousPaperWorker, WorkerMode


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
    backtest.add_argument("--execution-delay-bars", type=int, default=1)
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
    serve.add_argument("--production", action="store_true")

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
    download.add_argument(
        "--timeframe",
        choices=["1Day", "1Hour", "30Min", "5Min", "1Min"],
        default="1Day",
    )
    download.add_argument("--output", type=Path, required=True)
    download.add_argument("--overwrite", action="store_true")
    download_universe = subparsers.add_parser(
        "download-universe",
        help="download a comma-separated Alpaca universe to canonical CSV files",
    )
    download_universe.add_argument("--symbols", default="SPY,QQQ,IWM,TLT,GLD")
    download_universe.add_argument("--start", required=True)
    download_universe.add_argument("--end", required=True)
    download_universe.add_argument(
        "--timeframe",
        choices=["1Day", "1Hour", "30Min", "5Min", "1Min"],
        default="1Day",
    )
    download_universe.add_argument("--output-directory", type=Path, default=Path("data/universe"))
    download_universe.add_argument("--overwrite", action="store_true")
    v09_dataset = subparsers.add_parser(
        "build-v09-dataset",
        help="download and hash the predefined v0.9 SIP ETF bar dataset",
    )
    v09_dataset.add_argument("--start", default="2020-01-01T00:00:00Z")
    v09_dataset.add_argument("--end", default="2025-01-01T00:00:00Z")
    v09_dataset.add_argument(
        "--output-directory",
        type=Path,
        default=Path("data/v09/alpaca-sip-20200101-20250101"),
    )
    v09_dataset.add_argument(
        "--manifest",
        type=Path,
        default=Path("research/datasets/v0.9.0-alpaca-sip.json"),
    )

    subparsers.add_parser(
        "alpaca-paper-status",
        help="verify the Alpaca paper account and list positions without trading",
    )
    reconcile = subparsers.add_parser(
        "alpaca-paper-reconcile",
        help="record and verify broker-authoritative paper state without trading",
    )
    reconcile.add_argument("--database", type=Path, default=Path("data/tradeagent.db"))
    take_profit = subparsers.add_parser(
        "alpaca-paper-take-profit",
        help="monitor and close a paper position after it becomes profitable",
    )
    take_profit.add_argument("--symbol", default="BTC/USD")
    take_profit.add_argument("--minimum-profit", type=Decimal, default=Decimal(0))
    take_profit.add_argument("--poll-seconds", type=float, default=15)
    take_profit.add_argument("--database", type=Path, default=Path("data/alpaca-paper.db"))
    take_profit.add_argument("--confirm-paper", action="store_true")
    portfolio_evaluate = subparsers.add_parser(
        "portfolio-evaluate",
        help="qualify cross-sectional momentum on an aligned local universe",
    )
    portfolio_evaluate.add_argument("--symbols", default="SPY,QQQ,IWM,TLT,GLD")
    portfolio_evaluate.add_argument(
        "--universe-directory", type=Path, default=Path("data/universe")
    )
    portfolio_evaluate.add_argument("--lookback-frames", type=int, default=63)
    portfolio_evaluate.add_argument("--top-n", type=int, default=2)
    portfolio_evaluate.add_argument("--gross-target", type=Decimal, default=Decimal("0.04"))
    portfolio_evaluate.add_argument("--training-frames", type=int, default=252)
    portfolio_evaluate.add_argument("--testing-frames", type=int, default=63)
    portfolio_evaluate.add_argument("--step-frames", type=int, default=63)
    portfolio_evaluate.add_argument("--embargo-frames", type=int, default=5)
    portfolio_evaluate.add_argument("--seed", type=int, default=7)
    portfolio_evaluate.add_argument("--database", type=Path, default=Path("data/experiments.db"))
    notifier = subparsers.add_parser(
        "notifier",
        help="deliver exactly-once round-trip emails from the production outbox",
    )
    notifier.add_argument("--once", action="store_true")
    notifier.add_argument("--poll-seconds", type=float, default=5)
    notifier.add_argument("--instance-id")
    worker = subparsers.add_parser(
        "worker-shadow",
        help="run the always-on paper data and reconciliation worker without orders",
    )
    worker.add_argument("--symbols", default="SPY,QQQ,IWM,TLT,GLD")
    worker.add_argument("--instance-id")
    news_worker = subparsers.add_parser(
        "news-worker",
        help="poll and persist Alpaca news with a durable watermark",
    )
    news_worker.add_argument("--instance-id")
    intraday_evaluate = subparsers.add_parser(
        "intraday-evaluate",
        help="qualify intraday strategies on aligned 5-minute bars",
    )
    intraday_evaluate.add_argument(
        "--strategy",
        choices=[
            "opening-range",
            "vwap",
            "regime-momentum",
            "heikin-ashi",
            "engulfing",
            "inside-breakout",
            "noise-area",
            "donchian-atr",
            "volatility-squeeze",
        ],
        required=True,
    )
    intraday_evaluate.add_argument("--symbols", default="SPY,QQQ")
    intraday_evaluate.add_argument("--universe-directory", type=Path, default=Path("data/intraday"))
    intraday_evaluate.add_argument("--training-frames", type=int, default=1_560)
    intraday_evaluate.add_argument("--testing-frames", type=int, default=390)
    intraday_evaluate.add_argument("--step-frames", type=int, default=390)
    intraday_evaluate.add_argument("--embargo-frames", type=int, default=78)
    intraday_evaluate.add_argument("--warmup-frames", type=int, default=1_560)
    intraday_evaluate.add_argument("--maximum-frames", type=int, default=10_000)
    intraday_evaluate.add_argument("--bar-minutes", type=int, choices=[1, 5], default=5)
    intraday_evaluate.add_argument("--holdout-manifest", type=Path)
    intraday_evaluate.add_argument("--minimum-closed-trades", type=int, default=200)
    intraday_evaluate.add_argument("--seed", type=int, default=7)
    intraday_evaluate.add_argument("--database", type=Path, default=Path("data/experiments.db"))
    seal = subparsers.add_parser(
        "seal-intraday-holdout",
        help="seal the terminal fraction of a regular-session intraday panel",
    )
    seal.add_argument("--symbols", default="SPY,QQQ")
    seal.add_argument("--universe-directory", type=Path, default=Path("data/intraday"))
    seal.add_argument("--manifest", type=Path, default=Path("data/intraday-holdout.json"))
    seal.add_argument("--fraction", type=float, default=0.20)
    seal.add_argument("--maximum-frames", type=int, default=10_000)
    open_holdout = subparsers.add_parser(
        "open-intraday-holdout",
        help="open a sealed terminal holdout once and write an audit marker",
    )
    open_holdout.add_argument("--symbols", default="SPY,QQQ")
    open_holdout.add_argument("--universe-directory", type=Path, default=Path("data/intraday"))
    open_holdout.add_argument("--manifest", type=Path, default=Path("data/intraday-holdout.json"))
    open_holdout.add_argument(
        "--audit", type=Path, default=Path("data/intraday-holdout-opened.json")
    )
    open_holdout.add_argument("--maximum-frames", type=int, default=10_000)
    open_holdout.add_argument("--authorization", required=True)
    quality = subparsers.add_parser(
        "data-quality",
        help="report aligned bar quality and explicit vendor limitations",
    )
    quality.add_argument("--symbols", default="SPY,QQQ")
    quality.add_argument("--universe-directory", type=Path, default=Path("data/intraday"))
    diagnostics = subparsers.add_parser(
        "intraday-diagnostics",
        help="report gross/net trade attribution and MFE/MAE",
    )
    diagnostics.add_argument(
        "--strategy",
        choices=["noise-area", "donchian-atr", "volatility-squeeze"],
        required=True,
    )
    diagnostics.add_argument("--symbols", default="SPY,QQQ")
    diagnostics.add_argument("--universe-directory", type=Path, default=Path("data/intraday"))
    diagnostics.add_argument(
        "--holdout-manifest", type=Path, default=Path("data/intraday-holdout.json")
    )
    diagnostics.add_argument("--maximum-frames", type=int, default=10_000)
    meta = subparsers.add_parser(
        "meta-label-evaluate",
        help="evaluate a calibrated filter on deterministic trend-pullback events",
    )
    meta.add_argument("--symbols", default="SPY,QQQ")
    meta.add_argument("--universe-directory", type=Path, default=Path("data/intraday"))
    meta.add_argument("--holdout-manifest", type=Path, default=Path("data/intraday-holdout.json"))
    meta.add_argument("--maximum-frames", type=int, default=10_000)
    meta.add_argument("--threshold", type=Decimal, default=Decimal("0.65"))
    lower_turnover = subparsers.add_parser(
        "lower-turnover-research",
        help="evaluate the 30 predefined v0.9 daily ETF configurations",
    )
    lower_turnover.add_argument(
        "--universe-directory",
        type=Path,
        default=Path("data/v09/alpaca-sip-20200101-20250101/1day"),
    )
    lower_turnover.add_argument("--symbols", default=",".join(V09_ETF_UNIVERSE))
    lower_turnover.add_argument("--database", type=Path, default=Path("data/experiments.db"))
    lower_turnover.add_argument(
        "--output",
        type=Path,
        default=Path("research/results/v0.9.0-lower-turnover.json"),
    )
    squeeze_external = subparsers.add_parser(
        "squeeze-external",
        help="run the frozen v0.8 squeeze across the predefined v0.9 matrix",
    )
    squeeze_external.add_argument(
        "--source-directory",
        type=Path,
        default=Path("data/v09/alpaca-sip-20200101-20250101/30min"),
    )
    squeeze_external.add_argument("--symbols", default=",".join(V09_ETF_UNIVERSE))
    squeeze_external.add_argument(
        "--output",
        type=Path,
        default=Path("research/results/v0.9.0-frozen-squeeze-external.json"),
    )
    economic_ml = subparsers.add_parser(
        "economic-ml-research",
        help="evaluate net-return regression after event eligibility checks",
    )
    economic_ml.add_argument(
        "--universe-directory",
        type=Path,
        default=Path("data/v09/alpaca-sip-20200101-20250101/1day"),
    )
    economic_ml.add_argument("--symbols", default=",".join(V09_ETF_UNIVERSE))
    economic_ml.add_argument(
        "--output",
        type=Path,
        default=Path("research/results/v0.9.0-economic-ml.json"),
    )
    execution_evidence = subparsers.add_parser(
        "collect-squeeze-evidence",
        help="archive SIP quotes and trades around every frozen squeeze signal and fill",
    )
    execution_evidence.add_argument(
        "--report",
        type=Path,
        default=Path("research/results/v0.9.0-frozen-squeeze-external.json"),
    )
    execution_evidence.add_argument(
        "--records",
        type=Path,
        default=Path("data/v09/execution-evidence/squeeze-quotes-trades.jsonl"),
    )
    execution_evidence.add_argument(
        "--manifest",
        type=Path,
        default=Path("research/datasets/v0.9.0-squeeze-execution-evidence.json"),
    )
    execution_calibration = subparsers.add_parser(
        "calibrate-squeeze-execution",
        help="compare estimated costs with SIP market and marketable-limit fills",
    )
    execution_calibration.add_argument(
        "--report",
        type=Path,
        default=Path("research/results/v0.9.0-frozen-squeeze-external.json"),
    )
    execution_calibration.add_argument(
        "--evidence",
        type=Path,
        default=Path("data/v09/execution-evidence/squeeze-quotes-trades.jsonl"),
    )
    execution_calibration.add_argument(
        "--output",
        type=Path,
        default=Path("research/results/v0.9.0-execution-calibration.json"),
    )
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
            production_database_url=(
                AppConfig().database_url.get_secret_value() if args.production else None
            ),
        )
        uvicorn.run(app, host=args.host, port=args.port)
        return

    if args.command == "download-alpaca":
        data_settings = AlpacaDataSettings.model_validate({})
        with AlpacaDataClient(data_settings) as data_client:
            bars = data_client.bars(
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
                    "feed": data_settings.feed,
                    "symbol": args.symbol.upper(),
                    "rows": count,
                    "output": str(args.output),
                }
            )
        )
        return

    if args.command == "download-universe":
        symbols = tuple(
            symbol.strip().upper() for symbol in args.symbols.split(",") if symbol.strip()
        )
        if len(symbols) < 2:
            raise ValueError("universe download requires at least two symbols")
        data_settings = AlpacaDataSettings.model_validate({})
        downloaded: dict[str, int] = {}
        with AlpacaDataClient(data_settings) as data_client:
            for symbol in symbols:
                bars = data_client.bars(
                    symbol,
                    start=_parse_utc(args.start),
                    end=_parse_utc(args.end),
                    timeframe=args.timeframe,
                )
                downloaded[symbol] = write_bars(
                    args.output_directory / symbol_filename(symbol),
                    bars,
                    overwrite=args.overwrite,
                )
        print(
            _json(
                {
                    "provider": "alpaca",
                    "feed": data_settings.feed,
                    "symbols": symbols,
                    "rows": downloaded,
                    "output_directory": str(args.output_directory),
                }
            )
        )
        return

    if args.command == "build-v09-dataset":
        v09_settings = AlpacaDataSettings.model_validate({})
        with AlpacaDataClient(v09_settings) as data_client:
            entitlement = data_client.probe_historical_sip(
                "SPY",
                start=_parse_utc(args.start),
                end=min(
                    _parse_utc(args.end),
                    _parse_utc(args.start) + timedelta(minutes=1),
                ),
            )
            if not entitlement.historical_quotes:
                raise RuntimeError(
                    "SIP entitlement unavailable; no historical evidence was fabricated: "
                    f"{entitlement.reason}"
                )
            v09_manifest = build_v09_bar_dataset(
                data_client,
                args.output_directory,
                start=_parse_utc(args.start),
                end=_parse_utc(args.end),
                symbols=V09_ETF_UNIVERSE,
                timeframes=V09_TIMEFRAMES,
                on_file=lambda record: print(
                    _json({"downloaded": record}),
                    flush=True,
                ),
            )
        write_dataset_manifest(args.manifest, v09_manifest)
        print(_json({"manifest": v09_manifest, "output": str(args.manifest)}))
        return

    if args.command == "alpaca-paper-status":
        paper_settings = AlpacaPaperSettings.model_validate({})
        with AlpacaPaperClient(paper_settings) as paper_client:
            account = paper_client.account()
            positions = paper_client.positions()
        print(
            _json(
                {
                    "mode": "paper",
                    "account": account,
                    "positions": positions,
                    "live_trading_available": False,
                }
            )
        )
        return

    if args.command == "alpaca-paper-reconcile":
        paper_settings = AlpacaPaperSettings.model_validate({})
        config = AppConfig(database_path=args.database)
        with (
            AlpacaPaperClient(paper_settings) as paper_client,
            SQLiteLedger(args.database) as ledger,
        ):
            result = PaperOrderManager(
                paper_client,
                RiskEngine(config.risk),
                ledger,
            ).reconcile(observed_at=datetime.now(UTC))
        print(_json(result))
        return

    if args.command == "alpaca-paper-take-profit":
        if not args.confirm_paper:
            raise ValueError("take-profit monitoring requires --confirm-paper")
        paper_settings = AlpacaPaperSettings.model_validate({})
        with (
            AlpacaPaperClient(paper_settings) as paper_client,
            SQLiteLedger(args.database) as ledger,
        ):
            monitor_result = monitor_take_profit(
                paper_client,
                ledger,
                symbol=args.symbol,
                minimum_profit=args.minimum_profit,
                poll_seconds=args.poll_seconds,
                on_sample=lambda position: print(
                    _json(
                        {
                            "status": "monitoring",
                            "symbol": position.symbol,
                            "quantity": position.quantity,
                            "unrealized_pnl": position.unrealized_pnl,
                        }
                    ),
                    flush=True,
                ),
            )
            reconciliation = PaperOrderManager(
                paper_client,
                RiskEngine(AppConfig(database_path=args.database).risk),
                ledger,
            ).reconcile(observed_at=datetime.now(UTC))
        print(_json({"result": monitor_result, "reconciliation": reconciliation}))
        return

    if args.command == "portfolio-evaluate":
        symbols = tuple(
            symbol.strip().upper() for symbol in args.symbols.split(",") if symbol.strip()
        )
        dataset = load_universe(args.universe_directory, symbols)
        strategy_config = PortfolioStrategyConfig(
            lookback_frames=args.lookback_frames,
            top_n=args.top_n,
            gross_target=args.gross_target,
        )
        walk_forward = WalkForwardConfig(
            training_bars=args.training_frames,
            testing_bars=args.testing_frames,
            step_bars=args.step_frames,
            embargo_bars=args.embargo_frames,
            warmup_bars=min(args.lookback_frames + 1, args.training_frames),
        )
        config = AppConfig()
        report = evaluate_portfolio_suite(
            dataset.frames,
            dataset.manifest,
            config,
            walk_forward,
            strategy_config,
            lambda: CrossSectionalMomentumStrategy(strategy_config),
            lambda: EqualWeightPortfolioStrategy(strategy_config.gross_target),
            random_seed=args.seed,
        )
        with ExperimentRegistry(args.database) as registry:
            experiment_id = registry.record_model(
                report,
                dataset_hash=report.dataset.dataset_hash,
                config_hash_value=report.config_hash,
                git_sha=report.git_sha,
                random_seed=report.random_seed,
                strategy_id=report.scenarios[0].strategy_id,
                qualified=report.qualified,
            )
        print(_json({"experiment_id": experiment_id, "report": report}))
        return

    if args.command == "notifier":
        config = AppConfig()
        email_settings = EmailSettings.model_validate({})
        with (
            Database(config.database_url.get_secret_value()) as database,
            ResendEmailProvider(email_settings) as provider,
        ):
            notification_repository = RoundTripNotificationRepository(database)
            service = NotifierService(
                NotificationDispatcher(notification_repository, provider),
                ProductionRepository(database),
                instance_id=(
                    args.instance_id or os.getenv("RENDER_INSTANCE_ID") or socket.gethostname()
                ),
                poll_seconds=args.poll_seconds,
            )
            if args.once:
                dispatched = service.run_once()
                print(_json({"dispatched": dispatched}))
            else:
                asyncio.run(service.run())
        return

    if args.command == "worker-shadow":
        symbols = tuple(
            symbol.strip().upper() for symbol in args.symbols.split(",") if symbol.strip()
        )
        if not symbols:
            raise ValueError("shadow worker requires at least one symbol")
        config = AppConfig()
        stream_settings = AlpacaStreamSettings.model_validate({})
        paper_settings = AlpacaPaperSettings.model_validate({})
        with (
            Database(config.database_url.get_secret_value()) as database,
            AlpacaPaperClient(paper_settings) as paper_client,
        ):
            repository = ProductionRepository(database)
            reconciler = ProductionPaperReconciler(paper_client, repository)
            news_repository = NewsRepository(database)

            def latest_news_feed_at() -> datetime | None:
                heartbeat = repository.latest_heartbeat("tradeagent-news-worker")
                return heartbeat[1] if heartbeat else None

            worker = AutonomousPaperWorker(
                config,
                repository,
                reconciler,
                LiveShadowDecisionProcessor(
                    config,
                    repository,
                    RegimeFilteredMomentumStrategy(
                        IntradayStrategyConfig(),
                        config.intraday.model_copy(update={"enabled": True}),
                    ),
                    symbols=symbols,
                    news_context=NewsContextService(
                        news_repository,
                        NewsBlackoutPolicy(),
                        latest_feed_at=latest_news_feed_at,
                    ),
                ),
                mode=WorkerMode.SHADOW,
                instance_id=(
                    args.instance_id or os.getenv("RENDER_INSTANCE_ID") or socket.gethostname()
                ),
                strategy_authorized=lambda: False,
            )
            scheduler = ReconciliationScheduler(
                repository,
                reconciler,
                interval_seconds=config.intraday.reconciliation_interval_seconds,
                instance_id=f"{args.instance_id or socket.gethostname()}-reconciler",
            )
            asyncio.run(
                run_shadow_runtime(
                    AlpacaMarketStream(stream_settings),
                    worker,
                    scheduler,
                    symbols=symbols,
                )
            )
        return

    if args.command == "news-worker":
        config = AppConfig()
        settings = NewsWorkerSettings.model_validate({})
        alpaca_settings = AlpacaDataSettings.model_validate({})
        with (
            Database(config.database_url.get_secret_value()) as database,
            AlpacaNewsClient(alpaca_settings) as news_source,
        ):
            instance_id = (
                args.instance_id or os.getenv("RENDER_INSTANCE_ID") or socket.gethostname()
            )
            news_service = NewsWorker(
                settings,
                news_source,
                NewsRepository(database),
                ProductionRepository(database),
                instance_id=instance_id,
            )
            asyncio.run(news_service.run())
        return

    if args.command == "lower-turnover-research":
        symbols = tuple(
            symbol.strip().upper() for symbol in args.symbols.split(",") if symbol.strip()
        )
        lower_report = evaluate_lower_turnover_families(
            args.universe_directory,
            symbols,
            args.database,
            generated_at=datetime.now(UTC),
        )
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(lower_report.model_dump_json(indent=2) + "\n", encoding="utf-8")
        print(
            _json(
                {
                    "output": str(args.output),
                    "sha256": lower_turnover_report_hash(lower_report),
                    "experiment_count": lower_report.experiment_count,
                    "qualified_strategy_ids": lower_report.qualified_strategy_ids,
                }
            )
        )
        return

    if args.command == "squeeze-external":
        symbols = tuple(
            symbol.strip().upper() for symbol in args.symbols.split(",") if symbol.strip()
        )
        squeeze_report = evaluate_frozen_squeeze_matrix(
            args.source_directory,
            symbols,
            generated_at=datetime.now(UTC),
        )
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(squeeze_report.model_dump_json(indent=2) + "\n", encoding="utf-8")
        print(
            _json(
                {
                    "output": str(args.output),
                    "sha256": squeeze_report_hash(squeeze_report),
                    "matrix_size": len(squeeze_report.results),
                    "family_decision": squeeze_report.family_decision,
                }
            )
        )
        return

    if args.command == "economic-ml-research":
        symbols = tuple(
            symbol.strip().upper() for symbol in args.symbols.split(",") if symbol.strip()
        )
        ml_dataset = load_universe(args.universe_directory, symbols)
        economic_events = build_economic_events(ml_dataset.frames)
        economic_report = evaluate_economic_ml(economic_events)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(economic_report.model_dump_json(indent=2) + "\n", encoding="utf-8")
        print(
            _json(
                {
                    "output": str(args.output),
                    "sha256": economic_ml_report_hash(economic_report),
                    "total_candidate_events": economic_report.total_candidate_events,
                    "positive_net_edge_events": economic_report.positive_net_edge_events,
                    "eligible": economic_report.eligible,
                    "qualified": economic_report.qualified,
                    "qualification_reasons": economic_report.qualification_reasons,
                }
            )
        )
        return

    if args.command == "collect-squeeze-evidence":
        squeeze_report = FrozenSqueezeExternalReport.model_validate_json(
            args.report.read_text(encoding="utf-8")
        )
        anchors = squeeze_evidence_anchors(squeeze_report)
        evidence_settings = AlpacaDataSettings.model_validate({})
        with AlpacaDataClient(evidence_settings) as data_client:
            evidence_manifest = collect_execution_evidence(
                data_client,
                anchors,
                args.records,
            )
        write_evidence_manifest(args.manifest, evidence_manifest)
        print(
            _json(
                {
                    "manifest": str(args.manifest),
                    "records": str(args.records),
                    "anchor_count": evidence_manifest.anchor_count,
                    "quote_coverage_ratio": evidence_manifest.quote_coverage_ratio,
                    "quote_records": evidence_manifest.quote_records,
                    "trade_records": evidence_manifest.trade_records,
                }
            )
        )
        return

    if args.command == "calibrate-squeeze-execution":
        squeeze_report = FrozenSqueezeExternalReport.model_validate_json(
            args.report.read_text(encoding="utf-8")
        )
        calibration = calibrate_squeeze_execution(
            squeeze_report,
            args.evidence,
            generated_at=datetime.now(UTC),
        )
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(calibration.model_dump_json(indent=2) + "\n", encoding="utf-8")
        print(
            _json(
                {
                    "output": str(args.output),
                    "total_trades": calibration.total_trades,
                    "quote_complete_trades": calibration.quote_complete_trades,
                    "estimated_cost": calibration.estimated_cost,
                    "observed_market_cost": calibration.observed_market_cost,
                    "marketable_limit_missed_round_trips": (
                        calibration.marketable_limit_missed_round_trips
                    ),
                }
            )
        )
        return

    if args.command == "intraday-evaluate":
        symbols = tuple(
            symbol.strip().upper() for symbol in args.symbols.split(",") if symbol.strip()
        )
        source_dataset = load_universe(args.universe_directory, symbols)
        intraday_config = AppConfig().intraday.model_copy(
            update={
                "enabled": True,
                "primary_bar_minutes": args.bar_minutes,
            }
        )
        calendar = NyseSessionCalendar(intraday_config)
        frames = regular_session_frames(source_dataset.frames, calendar)
        if args.maximum_frames > 0:
            frames = frames[-args.maximum_frames :]
        if args.holdout_manifest is not None:
            frames = development_frames(
                frames,
                load_holdout_manifest(args.holdout_manifest),
            )
        bars_by_symbol = {symbol: [frame.bar_for(symbol) for frame in frames] for symbol in symbols}
        dataset = align_universe(bars_by_symbol)
        intraday_strategy_config = IntradayStrategyConfig()
        app_config = AppConfig(intraday=intraday_config)
        walk_forward = WalkForwardConfig(
            training_bars=args.training_frames,
            testing_bars=args.testing_frames,
            step_bars=args.step_frames,
            embargo_bars=args.embargo_frames,
            warmup_bars=args.warmup_frames,
        )

        def intraday_strategy_factory() -> (
            OpeningRangeBreakoutStrategy
            | SessionVwapMeanReversionStrategy
            | RegimeFilteredMomentumStrategy
            | CandleTrackingStrategy
            | NoiseAreaMomentumStrategy
            | DonchianAtrBreakoutStrategy
            | VolatilitySqueezeBreakoutStrategy
        ):
            if args.strategy == "opening-range":
                return OpeningRangeBreakoutStrategy(
                    intraday_strategy_config,
                    intraday_config,
                )
            if args.strategy == "vwap":
                return SessionVwapMeanReversionStrategy(
                    intraday_strategy_config,
                    intraday_config,
                )
            if args.strategy == "regime-momentum":
                return RegimeFilteredMomentumStrategy(
                    intraday_strategy_config,
                    intraday_config,
                )
            if args.strategy in {pattern.value for pattern in CandlePattern}:
                return CandleTrackingStrategy(
                    CandlePattern(args.strategy),
                    intraday_config,
                    target_weight=intraday_strategy_config.target_weight,
                )
            if args.strategy == "noise-area":
                return NoiseAreaMomentumStrategy(
                    intraday_config,
                    target_weight=intraday_strategy_config.target_weight,
                )
            if args.strategy == "donchian-atr":
                return DonchianAtrBreakoutStrategy(
                    intraday_config,
                    target_weight=intraday_strategy_config.target_weight,
                )
            return VolatilitySqueezeBreakoutStrategy(
                intraday_config,
                target_weight=intraday_strategy_config.target_weight,
            )

        gross_target = intraday_strategy_config.target_weight * intraday_strategy_config.top_n
        report = evaluate_portfolio_suite(
            dataset.frames,
            dataset.manifest,
            app_config,
            walk_forward,
            intraday_strategy_config,
            intraday_strategy_factory,
            lambda: IntradayEqualWeightBenchmark(
                gross_target,
                intraday_config,
            ),
            random_seed=args.seed,
            minimum_closed_trades=args.minimum_closed_trades,
            minimum_deflated_sharpe_probability=Decimal("0.95"),
            maximum_backtest_overfitting_probability=Decimal("0.20"),
            number_of_trials=24,
        )
        with ExperimentRegistry(args.database) as registry:
            experiment_id = registry.record_model(
                report,
                dataset_hash=report.dataset.dataset_hash,
                config_hash_value=report.config_hash,
                git_sha=report.git_sha,
                random_seed=report.random_seed,
                strategy_id=report.scenarios[0].strategy_id,
                qualified=report.qualified,
            )
        print(_json({"experiment_id": experiment_id, "report": report}))
        return

    if args.command in {"seal-intraday-holdout", "open-intraday-holdout"}:
        symbols = tuple(
            symbol.strip().upper() for symbol in args.symbols.split(",") if symbol.strip()
        )
        source = load_universe(args.universe_directory, symbols)
        calendar = NyseSessionCalendar(AppConfig().intraday)
        frames = regular_session_frames(source.frames, calendar)
        if args.maximum_frames > 0:
            frames = frames[-args.maximum_frames :]
        if args.command == "seal-intraday-holdout":
            manifest = seal_holdout(
                frames,
                args.manifest,
                holdout_fraction=args.fraction,
            )
            print(_json(manifest))
            return
        manifest = load_holdout_manifest(args.manifest)
        holdout = open_holdout_once(
            frames,
            manifest,
            args.audit,
            authorization=args.authorization,
        )
        print(
            _json(
                {
                    "opened": True,
                    "frames": len(holdout),
                    "holdout_hash": manifest.holdout_hash,
                    "authorization_matched": (args.authorization == HOLDOUT_AUTHORIZATION),
                }
            )
        )
        return

    if args.command == "data-quality":
        symbols = tuple(
            symbol.strip().upper() for symbol in args.symbols.split(",") if symbol.strip()
        )
        dataset = load_universe(args.universe_directory, symbols)
        calendar = NyseSessionCalendar(AppConfig().intraday)
        frames = regular_session_frames(dataset.frames, calendar)
        quality_report = analyze_dataset(
            align_universe(
                {symbol: [frame.bar_for(symbol) for frame in frames] for symbol in symbols}
            )
        )
        print(_json(quality_report))
        return

    if args.command == "intraday-diagnostics":
        symbols = tuple(
            symbol.strip().upper() for symbol in args.symbols.split(",") if symbol.strip()
        )
        source = load_universe(args.universe_directory, symbols)
        intraday = AppConfig().intraday.model_copy(update={"enabled": True})
        frames = regular_session_frames(
            source.frames,
            NyseSessionCalendar(intraday),
        )
        if args.maximum_frames > 0:
            frames = frames[-args.maximum_frames :]
        frames = development_frames(
            frames,
            load_holdout_manifest(args.holdout_manifest),
        )
        dataset = align_universe(
            {symbol: [frame.bar_for(symbol) for frame in frames] for symbol in symbols}
        )
        diagnostic_strategy: PortfolioStrategy
        if args.strategy == "noise-area":
            diagnostic_strategy = NoiseAreaMomentumStrategy(intraday)
        elif args.strategy == "donchian-atr":
            diagnostic_strategy = DonchianAtrBreakoutStrategy(intraday)
        else:
            diagnostic_strategy = VolatilitySqueezeBreakoutStrategy(intraday)
        diagnostic_report = diagnose_strategy(
            dataset.frames,
            AppConfig(intraday=intraday),
            diagnostic_strategy,
        )
        print(_json(diagnostic_report))
        return

    if args.command == "meta-label-evaluate":
        symbols = tuple(
            symbol.strip().upper() for symbol in args.symbols.split(",") if symbol.strip()
        )
        source = load_universe(args.universe_directory, symbols)
        intraday = AppConfig().intraday.model_copy(update={"enabled": True})
        frames = regular_session_frames(
            source.frames,
            NyseSessionCalendar(intraday),
        )
        if args.maximum_frames > 0:
            frames = frames[-args.maximum_frames :]
        frames = development_frames(
            frames,
            load_holdout_manifest(args.holdout_manifest),
        )
        config = AppConfig(intraday=intraday)
        events = build_meta_label_events(
            frames,
            TrendPullbackCandidateStrategy(config),
            config,
        )
        try:
            meta_report = evaluate_meta_labels(
                events,
                threshold=args.threshold,
            )
            print(_json(meta_report))
        except ValueError as error:
            print(
                _json(
                    {
                        "qualified_filter": False,
                        "event_count": len(events),
                        "reason": str(error),
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
                execution_delay_bars=args.execution_delay_bars,
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
            strategy = DelayedStrategy(
                SmaCrossoverStrategy(config.strategy),
                config.strategy.execution_delay_bars,
            )
            backtest_report = _build_engine(
                config,
                ledger,
                strategy=strategy,
            ).run(backtest_bars)
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
        if progress is not None and progress["payload"].get(
            "config_fingerprint"
        ) != config_fingerprint(config):
            raise ValueError("existing paper run configuration differs; use a separate database")
        strategy = DelayedStrategy(
            SmaCrossoverStrategy(config.strategy),
            config.strategy.execution_delay_bars,
        )
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
