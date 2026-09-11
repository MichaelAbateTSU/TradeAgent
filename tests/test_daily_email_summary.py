import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import insert

from tradeagent.daily_email_summary import build_daily_email_summary, compact_legacy_trades
from tradeagent.email_schedule import DailyStatusSettings
from tradeagent.persistence import Database, ProductionRepository, controls
from tradeagent.scalping_config import ScalpingConfig
from tradeagent.scalping_store import ScalpStore, scalping_cycles

NOW = datetime(2026, 9, 11, 22, tzinfo=UTC)
ACCOUNT = "a" * 64


def add_cycle(
    database, run_id, at, *, symbol="BTC/USD", value="-2", pending=True, state="closed_owned_flat"
):
    cycle_id = str(uuid4())
    with database.begin() as connection:
        connection.execute(
            insert(scalping_cycles).values(
                cycle_id=cycle_id,
                run_id=run_id,
                account_digest=ACCOUNT,
                symbol=symbol,
                decision_id=cycle_id,
                state=state,
                created_at=at - timedelta(seconds=10),
                updated_at=at,
                opened_at=at - timedelta(seconds=5),
                closed_at=at,
                owned_quantity=0,
                entry_quantity=0 if state == "no_fill" else 1,
                entry_value=100,
                exit_quantity=0 if state == "no_fill" else 1,
                exit_value=100 + Decimal(value),
                actual_cash_fees=0,
                actual_base_fees=0,
                gross_cash_flow=Decimal(value),
                actual_net_pnl=None if pending else Decimal(value),
                modeled_net_pnl=Decimal(value),
                fees_pending=pending,
                payload={"signal": {"family": "momentum"}},
            )
        )
    return cycle_id


@pytest.fixture
def setup(tmp_path):
    with Database(f"sqlite:///{tmp_path / 'summary.db'}") as database:
        database.initialize()
        config = ScalpingConfig(
            cohort_id="new-run", account_digest=ACCOUNT, approved_at=NOW - timedelta(days=2)
        )
        store = ScalpStore(database)
        old_run = store.freeze_run(
            config.model_copy(update={"cohort_id": "old-run"}), "b" * 40, at=NOW - timedelta(days=1)
        )
        new_run = store.freeze_run(config, "c" * 40, at=NOW - timedelta(hours=2))
        repository = ProductionRepository(database)
        repository.acquire_worker_lock("tradeagent-event-worker", "owner", observed_at=NOW)
        repository.heartbeat(
            "tradeagent-event-worker",
            "owner",
            {
                "entry_policy": config.profile,
                "cohort_id": config.cohort_id,
                "code_sha": "c" * 40,
                "config_hash": config.identity,
            },
            observed_at=NOW,
        )
        snapshot = {
            "state": "running",
            "profile": config.profile,
            "mode": "paper",
            "cohort_id": config.cohort_id,
            "owner_id": "owner",
            "code_sha": "c" * 40,
            "config_hash": config.identity,
            "execution": {"account_digest": ACCOUNT, "inventory": {}, "unresolved_order_count": 0},
            "operator_stop": False,
        }
        with database.begin() as connection:
            connection.execute(
                insert(controls).values(
                    control_key="scalping:new-run:status",
                    control_value=json.dumps(snapshot),
                    updated_at=NOW,
                )
            )
        yield database, old_run, new_run


def test_five_plain_paragraphs_cover_all_runs_and_overnight_without_future_trades(setup):
    database, old_run, new_run = setup
    ids = [
        add_cycle(database, old_run, NOW - timedelta(hours=23), value="1"),
        add_cycle(database, new_run, NOW - timedelta(hours=1), symbol="ETH/USD", value="-3"),
        add_cycle(
            database, new_run, NOW - timedelta(minutes=5), state="no_fill", value="0", pending=False
        ),
        add_cycle(database, old_run, NOW - timedelta(days=2), value="999"),
        add_cycle(database, new_run, NOW + timedelta(seconds=1), value="999"),
    ]
    result = build_daily_email_summary(database, NOW, DailyStatusSettings(_env_file=None))
    paragraphs = result["text"].split("\n\n")
    assert len(paragraphs) == 5 and all("\n" not in paragraph for paragraph in paragraphs)
    assert 250 <= len(result["text"].split()) <= 650
    assert result["completed_round_trips"] == 2
    assert result["fees_pending_cycles"] == 2
    assert "a loss of $2.00" in paragraphs[2]
    assert "exact final net result is not confirmed" in paragraphs[2]
    assert "Bitcoin" in paragraphs[1] and "Ether" in paragraphs[1]
    assert "1 entry attempt ended without a fill" in paragraphs[1]
    assert "overnight activity" in paragraphs[0]
    assert "999" not in result["text"]
    assert all(identity not in result["text"] for identity in ids)
    assert "{" not in result["text"] and "VWAP" not in result["text"]
    assert result["financial_result_final"] is False


@pytest.mark.parametrize(
    "value,label", [("4.25", "profit"), ("-4.25", "loss"), ("0", "break-even")]
)
def test_confirmed_profit_loss_and_flat_are_described_honestly(setup, value, label):
    database, _, run = setup
    add_cycle(database, run, NOW - timedelta(minutes=1), value=value, pending=False)
    result = build_daily_email_summary(database, NOW, DailyStatusSettings(_env_file=None))
    assert len(result["text"].split("\n\n")) == 5
    assert label in result["text"].split("\n\n")[2]
    assert result["financial_result_final"] is True


def test_no_trades_still_produces_five_useful_paragraphs(setup):
    database, _, _ = setup
    result = build_daily_email_summary(database, NOW, DailyStatusSettings(_env_file=None))
    assert len(result["text"].split("\n\n")) == 5
    assert "No completed buy-and-sell trades" in result["text"]
    assert "no realized profit or loss" in result["text"]
    assert "no meaningful win rate" in result["text"]


def test_stale_status_does_not_claim_current_flatness(setup):
    database, _, run = setup
    add_cycle(database, run, NOW - timedelta(minutes=1), value="-1")
    result = build_daily_email_summary(
        database, NOW + timedelta(minutes=5), DailyStatusSettings(_env_file=None)
    )
    assert "status is unavailable or stale" in result["text"]
    assert "cannot confirm" in result["text"]
    assert len(result["text"].split("\n\n")) == 5


def test_missing_modeled_value_does_not_turn_a_partial_total_into_daily_pnl(setup):
    from sqlalchemy import update

    database, _, run = setup
    identity = add_cycle(database, run, NOW - timedelta(minutes=1))
    with database.begin() as connection:
        connection.execute(
            update(scalping_cycles)
            .where(scalping_cycles.c.cycle_id == identity)
            .values(modeled_net_pnl=None)
        )
    result = build_daily_email_summary(database, NOW, DailyStatusSettings(_env_file=None))
    assert "complete profit or loss figure is not available" in result["text"]
    assert len(result["text"].split("\n\n")) == 5


def test_mixed_final_and_estimated_results_are_complete_not_a_partial_sum(setup):
    from sqlalchemy import update

    database, _, run = setup
    confirmed = add_cycle(database, run, NOW - timedelta(minutes=2), value="3", pending=False)
    add_cycle(database, run, NOW - timedelta(minutes=1), value="-5", pending=True)
    with database.begin() as connection:
        connection.execute(
            update(scalping_cycles)
            .where(scalping_cycles.c.cycle_id == confirmed)
            .values(modeled_net_pnl=None)
        )
    result = build_daily_email_summary(database, NOW, DailyStatusSettings(_env_file=None))
    assert "a loss of $2.00" in result["text"]
    assert result["financial_result_final"] is False


def test_legacy_trade_projection_includes_prior_versions_without_duplicate_views():
    trade = {
        "trade_id": "original-entry",
        "state": "closed",
        "symbol": "AAPL",
        "entry_at": (NOW - timedelta(hours=1)).isoformat(),
        "exit_at": (NOW - timedelta(minutes=1)).isoformat(),
        "economic_paper_pnl": "-.12",
    }
    prior = {**trade, "trade_id": "prior-entry", "symbol": "MSFT"}
    report = {
        "all_execution_economics": {"separate_ledgers": {"NEWS_STRATEGY": {"trades": [trade]}}},
        "news_strategy": {"trades": [trade]},
        "prior_cohort_activity": {
            "versions": [
                {
                    "all_execution_economics": {
                        "separate_ledgers": {"NEWS_STRATEGY": {"trades": [prior, trade]}}
                    }
                }
            ]
        },
    }
    result = compact_legacy_trades(report)
    assert [item["symbol"] for item in result] == ["AAPL", "MSFT"]
    assert all("trade_id" not in item for item in result)


def test_legacy_summary_filters_reporting_window_and_keeps_unknown_values(tmp_path):
    with Database(f"sqlite:///{tmp_path / 'legacy.db'}") as database:
        database.initialize()
        legacy = [
            {
                "symbol": "AAPL",
                "opened_at": (NOW - timedelta(hours=2)).isoformat(),
                "closed_at": (NOW - timedelta(hours=1)).isoformat(),
                "modeled_net_pnl": "-2",
            },
            {
                "symbol": "MSFT",
                "opened_at": (NOW - timedelta(days=2)).isoformat(),
                "closed_at": (NOW - timedelta(days=1, seconds=1)).isoformat(),
                "modeled_net_pnl": "999",
            },
        ]
        result = build_daily_email_summary(
            database, NOW, DailyStatusSettings(_env_file=None), legacy_trades=legacy
        )
        assert result["completed_round_trips"] == 1
        assert "a loss of $2.00" in result["text"]
        assert "999" not in result["text"] and "MSFT" not in result["text"]
        legacy[0]["modeled_net_pnl"] = None
        unpriced = build_daily_email_summary(
            database, NOW, DailyStatusSettings(_env_file=None), legacy_trades=legacy
        )
        assert unpriced["completed_round_trips"] == 1
        assert "complete profit or loss figure is not available" in unpriced["text"]
