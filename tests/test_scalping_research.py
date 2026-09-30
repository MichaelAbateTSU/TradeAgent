import json
from datetime import timedelta
from decimal import Decimal

import pytest
from test_scalping_market import NOW, Tape, config

from tradeagent.cli import main
from tradeagent.persistence import Database
from tradeagent.scalping_config import ScalpQuote
from tradeagent.scalping_research import (
    HORIZONS,
    ResearchPolicy,
    ResearchSignal,
    ResearchTick,
    _QuoteReplay,
    _ranks,
    analyze_signals,
    hypothesis_two,
    regimes,
    research_database,
    summarize,
)
from tradeagent.scalping_store import ScalpStore


def quote(seconds=0, bid="100", ask="100.01"):
    at = NOW + timedelta(seconds=seconds)
    return ScalpQuote(
        symbol="BTC/USD",
        exchange_at=at,
        received_at=at,
        exchange_time_ns=int(at.timestamp() * 1_000_000_000),
        bid=bid,
        ask=ask,
        bid_size="10",
        ask_size="10",
    )


def signal(seconds=0, identity="s1", **updates):
    return ResearchSignal(
        event_id=identity,
        run_id="run",
        decision_id=identity,
        symbol="BTC/USD",
        family="momentum",
        selected_action="hold",
        decision_at=NOW + timedelta(seconds=seconds),
        score=0.3,
        quote=quote(seconds),
        features={"volatility_5s_bps": 6, "return_15s_bps": 8, "trade_count_5s": 0},
    ).model_copy(update=updates)


def tick(seconds, bid="100", ask="100.01", continuity="one"):
    return ResearchTick(
        event_id=f"q-{seconds}",
        symbol="BTC/USD",
        received_at=NOW + timedelta(seconds=seconds),
        continuity_id=continuity,
        quote=quote(seconds, bid, ask),
    )


def analyze(signals, ticks, **policy):
    return analyze_signals(
        signals,
        ticks,
        ResearchPolicy(**policy),
        split_at=NOW + timedelta(days=1),
        information_cutoff=NOW + timedelta(days=2),
    )


def test_all_requested_horizons_have_exact_costs_and_no_double_spread():
    result = analyze(
        [signal()],
        [tick(0.3), *(tick(h - 0.01, "101", "101.01") for h in HORIZONS)],
    )
    assert len(result["rows"]) == 7
    assert all(row["complete"] for row in result["rows"])
    expected_gross = (Decimal("101") / Decimal("100.01") - 1) * 10000
    expected_net = (Decimal("101") / Decimal("100.01") * Decimal("0.9975") ** 2 - 1) * 10000
    for row in result["rows"]:
        assert row["gross_executable_return_bps"] == pytest.approx(float(expected_gross))
        assert row["net_executable_return_bps"] == pytest.approx(float(expected_net))
    assert result["promotion_allowed"] is False
    assert result["finite_trial_budget"] == 7
    assert result["horizon_results"][0]["ratio_is_not_portfolio_sharpe"] is True


def test_quote_after_horizon_cannot_fill_missing_deadline():
    result = analyze([signal()], [tick(0.3), tick(5.01, "110", "110.01")])
    five = next(row for row in result["rows"] if row["horizon_seconds"] == 5)
    assert not five["complete"]
    assert "EXIT_QUOTE_MISSING_OR_STALE" in five["missing_reasons"]
    assert five["net_executable_return_bps"] is None


def test_future_mutation_does_not_change_earlier_markout():
    prefix = [tick(0.3), tick(0.99, "100.5", "100.51")]
    first = analyze([signal()], [*prefix, tick(5.01)])
    changed = analyze([signal()], [*prefix, tick(5.01, "500", "500.01")])
    assert first["rows"][0] == changed["rows"][0]


def test_continuity_gap_censors_position_instead_of_claiming_flatness():
    result = analyze([signal()], [tick(0.3), tick(0.99, continuity="reconnected")])
    assert "LOCAL_CONTINUITY_CHANGED" in result["rows"][0]["missing_reasons"]
    assert result["rows"][0]["net_executable_return_bps"] is None


def test_arrival_cap_and_displayed_size_are_not_assumed_fills():
    result = analyze([signal()], [tick(0.3, "101", "101.01"), tick(0.99)])
    assert "ARRIVAL_ASK_EXCEEDS_ORIGINAL_CAP" in result["rows"][0]["missing_reasons"]
    assert result["rows"][0]["mid_forward_return_bps"] is not None
    poor_size = tick(0.3).model_copy(
        update={"quote": quote(0.3).model_copy(update={"ask_size": Decimal("0.0001")})}
    )
    result = analyze([signal()], [poor_size, tick(0.99)])
    assert "INSUFFICIENT_OBSERVED_ENTRY_SIZE" in result["rows"][0]["missing_reasons"]


def test_sell_signal_does_not_become_an_unavailable_short_trade():
    result = analyze([signal(selected_action="sell")], [tick(0.3), tick(0.99)])
    assert all("NOT_A_LONG_ENTRY_HYPOTHESIS" in row["missing_reasons"] for row in result["rows"])


def test_cutoff_censors_even_fresh_quotes_without_backdating():
    result = analyze_signals(
        [signal()],
        [tick(0.3), tick(0.99), tick(4.99)],
        ResearchPolicy(),
        split_at=NOW + timedelta(seconds=1),
        information_cutoff=NOW + timedelta(seconds=2),
    )
    five = result["rows"][1]
    assert "HORIZON_AFTER_INFORMATION_CUTOFF" in five["missing_reasons"]
    assert five["net_executable_return_bps"] is None


def test_budget_and_time_reversal_are_explicit_failures_not_truncation():
    with pytest.raises(ValueError, match="signal budget"):
        analyze([signal(), signal(identity="s2")], [], maximum_signals=1)
    with pytest.raises(ValueError, match="tape budget"):
        analyze([signal()], [tick(0.3), tick(0.99)], maximum_tape_events=1)
    with pytest.raises(ValueError, match="reversed"):
        analyze([signal()], [tick(0.99), tick(0.3)])
    with pytest.raises(ValueError, match="duplicate"):
        analyze([signal(), signal()], [])


def test_missing_features_remain_unknown_and_ties_have_mid_ranks():
    assert regimes(signal(features={}))["volatility"] == "unknown"
    assert regimes(signal())["volume_regime"] == "no_observed_trades"
    assert _ranks([2, 1, 2]) == [1.5, 0, 1.5]
    assert hypothesis_two(signal(features={})) is False
    assert (
        hypothesis_two(
            signal(
                features={
                    "volatility_5s_bps": 5,
                    "return_15s_bps": 6,
                    "normalized_ofi_5s": 0.2,
                }
            )
        )
        is True
    )


def test_top_decile_uses_discovery_only_and_cannot_create_a_winning_rule():
    result = analyze([signal()], [tick(0.3), tick(0.99)])
    rows = result["rows"]
    later = dict(
        rows[0], event_id="later", partition="later_diagnostic", net_executable_return_bps=1000
    )
    report = summarize([*rows, later])
    top = report["discovery_top_decile"][0]
    assert "later" not in top["event_ids"]
    assert top["positive_count"] == 0
    assert top["hindsight_only_not_a_rule_or_holdout"] is True


def test_split_embargo_censors_opposing_label_horizons():
    result = analyze_signals(
        [signal()],
        [tick(0.3), tick(0.99)],
        ResearchPolicy(),
        split_at=NOW + timedelta(seconds=500),
        information_cutoff=NOW + timedelta(seconds=1000),
    )
    assert all(row["partition"] == "embargo" for row in result["rows"])


def test_empty_population_reports_unknown_metrics():
    result = analyze([], [])
    assert result["signal_count"] == 0
    assert result["horizon_results"] == []
    assert result["promotion_allowed"] is False


def test_offline_cli_writes_reproducible_artifacts_without_database(tmp_path, capsys):
    signals = tmp_path / "signals.jsonl"
    quotes = tmp_path / "quotes.jsonl"
    signals.write_text(signal().model_dump_json() + "\n", encoding="utf-8")
    quotes.write_text(
        "\n".join(t.model_dump_json() for t in [tick(0.3), tick(0.99)]) + "\n",
        encoding="utf-8",
    )
    main(
        [
            "scalp-signal-research",
            "--signals-jsonl",
            str(signals),
            "--quotes-jsonl",
            str(quotes),
            "--start",
            NOW.isoformat(),
            "--end",
            (NOW + timedelta(days=2)).isoformat(),
            "--split-at",
            (NOW + timedelta(days=1)).isoformat(),
            "--at",
            (NOW + timedelta(days=2)).isoformat(),
            "--output-dir",
            str(tmp_path / "research"),
        ]
    )
    result = json.loads(capsys.readouterr().out)
    assert result["manifest"]["broker_called"] is False
    assert result["manifest"]["database_modified"] is False
    assert (tmp_path / "research" / "markouts.jsonl").is_file()
    assert "rows" not in result
    with pytest.raises(FileExistsError):
        from tradeagent.scalping_research import write_research

        write_research({"rows": []}, tmp_path / "research")


def test_database_reader_is_read_only_and_preserves_tape_hashes(tmp_path):
    tape = Tape()
    market = [
        tape.book(0, ask="100.01", snapshot=True),
        tape.book(0.3, ask="100.01"),
        tape.book(0.99, bid="101", ask="101.01"),
        tape.book(1.01, bid="101", ask="101.01"),
    ]
    with Database(f"sqlite:///{tmp_path / 'research.db'}") as database:
        database.initialize()
        store = ScalpStore(database)
        frozen = config()
        run = store.freeze_run(frozen, "a" * 40, at=NOW)
        s = signal(0.02).model_dump(mode="json")
        store.audit(
            "candidate_decision",
            {
                "run_id": run,
                "account_digest": frozen.account_digest,
                "signal": {
                    "decision_id": s["decision_id"],
                    "symbol": s["symbol"],
                    "family": s["family"],
                    "action": s["selected_action"],
                    "score": s["score"],
                    "quote": s["quote"],
                    "features": s["features"],
                },
            },
            at=NOW + timedelta(seconds=0.02),
        )
        store.persist_market_batch([m.model_dump(mode="json") for m in market], at=NOW)
        from sqlalchemy import func, select

        from tradeagent.persistence import events

        with database.begin() as connection:
            before = connection.scalar(select(func.count()).select_from(events))
        result = research_database(
            database,
            cohort_id=frozen.cohort_id,
            start=NOW,
            end=NOW + timedelta(days=2),
            split_at=NOW + timedelta(days=1),
            information_cutoff=NOW + timedelta(days=2),
            output_dir=tmp_path / "db-research",
            policy=ResearchPolicy(),
        )
        assert result["signal_count"] == 1
        assert result["manifest"]["market_batch_count"] == 1
        with database.begin() as connection:
            assert before == connection.scalar(select(func.count()).select_from(events))


def test_quote_only_replay_uses_real_native_l1_without_inventing_l2():
    tape = Tape()
    reader = _QuoteReplay(("BTC/USD",), 1000)
    q = tape.event("q", 0.3, bp="100", ap="100.01", bs="10", **{"as": "10"})
    observed, source = reader.on_event(q)
    assert source == "native_quote"
    assert observed is not None and observed.ask == Decimal("100.01")
    assert reader.books["BTC/USD"].valid is False
    stale = tape.book(0.4, ask="100.01", snapshot=True, received_seconds=3)
    observed, source = reader.on_event(stale)
    assert observed is not None and source == "native_quote"
    assert observed.received_at == q.received_at
    assert observed.exchange_at == q.exchange_at
    assert reader.last_event["BTC/USD"] == q.event_id
    current = tape.event("q", 3.2, bp="101", ap="101.01", bs="10", **{"as": "10"})
    observed, source = reader.on_event(current)
    assert observed is not None and observed.bid == Decimal("101")
    assert source == "native_quote"
    assert reader.books["BTC/USD"].awaiting_current_update


def test_quote_only_book_replay_matches_existing_engine_for_valid_tape():
    from tradeagent.scalping_market import BookFeatureEngine

    tape = Tape()
    market = [
        tape.book(0, ask="100.01", snapshot=True),
        tape.book(0.1, bid="100.1", ask="100.11"),
        tape.event("q", 0.2, bp="100.2", ap="100.21", bs="10", **{"as": "10"}),
        tape.book(0.3, bid="100.3", ask="100.31"),
    ]
    reader = _QuoteReplay(("BTC/USD",), 1000)
    engine = BookFeatureEngine(config(), stale_after_seconds=1)
    for event in market:
        engine.on_event(event)
        observed, _ = reader.on_event(event)
        assert observed == engine.quote("BTC/USD")
