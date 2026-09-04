from __future__ import annotations

from decimal import Decimal

from tradeagent.broker import PaperBroker
from tradeagent.config import AppConfig, StrategyConfig
from tradeagent.data import synthetic_bars
from tradeagent.engine import TradingEngine
from tradeagent.ledger import SQLiteLedger
from tradeagent.risk import RiskEngine
from tradeagent.strategy import SmaCrossoverStrategy


def test_end_to_end_paper_backtest_is_deterministic() -> None:
    config = AppConfig(
        strategy=StrategyConfig(fast_window=5, slow_window=20, target_weight=Decimal("0.02"))
    )

    def execute() -> tuple[object, int]:
        with SQLiteLedger(":memory:") as ledger:
            engine = TradingEngine(
                config=config,
                strategy=SmaCrossoverStrategy(config.strategy),
                broker=PaperBroker(config.broker),
                risk=RiskEngine(config.risk),
                ledger=ledger,
            )
            report = engine.run(synthetic_bars(count=250, seed=17))
            progress = ledger.latest_event("engine_progress")
            assert progress is not None
            assert len(progress["payload"]["config_fingerprint"]) == 64
            return report, ledger.event_count()

    first, first_event_count = execute()
    second, second_event_count = execute()

    assert first == second
    assert first_event_count == second_event_count
    assert first.mode == "paper"  # type: ignore[attr-defined]
    assert first.fills > 0  # type: ignore[attr-defined]
    assert first.rejected_orders == 0  # type: ignore[attr-defined]
    assert first_event_count >= first.fills * 3  # type: ignore[attr-defined]


def test_engine_rejects_empty_dataset() -> None:
    config = AppConfig()
    with SQLiteLedger(":memory:") as ledger:
        engine = TradingEngine(
            config=config,
            strategy=SmaCrossoverStrategy(config.strategy),
            broker=PaperBroker(config.broker),
            risk=RiskEngine(config.risk),
            ledger=ledger,
        )

        try:
            engine.run([])
        except ValueError as error:
            assert "at least one" in str(error)
        else:
            raise AssertionError("empty datasets must fail")


def test_engine_client_order_ids_fit_alpaca_limit() -> None:
    config = AppConfig(
        strategy=StrategyConfig(
            strategy_id="an-intentionally-long-versioned-strategy-identifier",
            fast_window=2,
            slow_window=3,
        )
    )
    with SQLiteLedger(":memory:") as ledger:
        engine = TradingEngine(
            config=config,
            strategy=SmaCrossoverStrategy(config.strategy),
            broker=PaperBroker(config.broker),
            risk=RiskEngine(config.risk),
            ledger=ledger,
        )
        engine.run(synthetic_bars(count=20, seed=2))
        submitted = [
            event for event in ledger.events(limit=100) if event["event_type"] == "order_submitted"
        ]

    assert submitted
    assert all(len(event["payload"]["client_order_id"]) <= 48 for event in submitted)
