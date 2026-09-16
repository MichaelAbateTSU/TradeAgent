from datetime import timedelta
from decimal import Decimal

import pytest
from test_scalping_market import NOW, Tape
from test_scalping_shadow import action_config, market_quote, quote, signal, synthetic_outcome

from tradeagent.persistence import Database
from tradeagent.scalping_policy import calibrate_from_shadow_report
from tradeagent.scalping_shadow import (
    ShadowActionEvaluator,
    ShadowPolicy,
    _with_bbo,
    candidate_from_signal,
    outcome_sample,
    shadow_report,
    simulate_candidates,
    simulate_database_candidates,
    validate_passive_simulation,
)
from tradeagent.scalping_store import ScalpStore


def candidate(**kwargs):
    return candidate_from_signal(
        signal(),
        run_id="run",
        account_digest="a" * 64,
        config=action_config(),
        shadow_send_at=NOW,
        **kwargs,
    )


def observe(evaluator, event, **kwargs):
    current = market_quote(event, **kwargs)
    return evaluator.on_market(event, quote=current, bid_levels=(), ask_levels=())


def test_arrival_uses_prior_quote_even_when_next_event_changes_touch():
    evaluator = ShadowActionEvaluator()
    evaluator.add(candidate())
    tape = Tape()
    observe(evaluator, tape.event("q", 0.2, bp="100", bs="1", ap="100.01", **{"as": "10"}))
    observe(
        evaluator,
        tape.event("q", 0.8, bp="101", bs="1", ap="101.01", **{"as": "10"}),
        bid="101",
        ask="101.01",
    )
    observe(evaluator, tape.event("q", 4.9, bp="100", bs="1", ap="100.01", **{"as": "10"}))
    rows = evaluator.flush(NOW + timedelta(seconds=5))
    aggressive = next(row for row in rows if row.action == "AGGRESSIVE_BUY")
    assert aggressive.complete and aggressive.filled
    assert aggressive.fill_delay_seconds == pytest.approx(0.35)
    assert aggressive.entry_value / aggressive.entry_quantity == Decimal("100.01")
    assert not evaluator._source_ids


def test_late_prearrival_trade_cannot_fill_passive_order():
    evaluator = ShadowActionEvaluator()
    evaluator.add(candidate())
    tape = Tape()
    event = tape.event("q", 0.2, bp="100", bs="1", ap="100.01", **{"as": "10"})
    observe(evaluator, event)
    delayed = tape.trade(0.3, received_seconds=0.5, quantity="100", side="S")
    evaluator.on_market(delayed, quote=market_quote(event), bid_levels=(), ask_levels=())
    observe(evaluator, tape.event("q", 4.9, bp="100", bs="1", ap="100.01", **{"as": "10"}))
    passive = evaluator.flush(NOW + timedelta(seconds=5))[0]
    assert passive.complete and not passive.filled


def test_event_after_horizon_cannot_supply_missing_horizon_price():
    evaluator = ShadowActionEvaluator()
    evaluator.add(candidate())
    tape = Tape()
    observe(evaluator, tape.event("q", 0.2, bp="100", bs="1", ap="100.01", **{"as": "10"}))
    rows = observe(
        evaluator,
        tape.event("q", 5.01, bp="110", bs="1", ap="110.01", **{"as": "10"}),
        bid="110",
        ask="110.01",
    )
    assert all(not row.complete and row.horizon_mid is None for row in rows)
    assert all("horizon_quote_too_old" in row.missing_reasons for row in rows)


def test_new_bbo_discards_obsolete_better_levels():
    from tradeagent.scalping_market import BookLevel

    levels = [BookLevel(price=price, quantity=1) for price in (99, 102, 100, 98)]
    assert [row.price for row in _with_bbo(quote(NOW), levels, side="bid")] == [100, 99, 98]
    assert [row.price for row in _with_bbo(quote(NOW), levels, side="ask")] == [
        Decimal("100.01"),
        102,
    ]


def test_replay_snapshot_preserves_depth_clock_and_drops_untimed_depth():
    from tradeagent.scalping_market import BookFeatureEngine, BookLevel, datetime_ns

    current = quote(NOW + timedelta(seconds=6))
    levels = (BookLevel(price=99, quantity=100),)
    source = f"{Tape().connection}:1"
    for timed in (False, True):
        engine = BookFeatureEngine(action_config())
        assert engine.restore_replay_snapshot(
            current,
            bids=(BookLevel(price=100, quantity=1), *levels),
            asks=(BookLevel(price=101, quantity=1),),
            source_event_id=source,
            observed_at=current.received_at,
            book_exchange_at_ns=datetime_ns(NOW) if timed else None,
            book_received_at_ns=datetime_ns(NOW) if timed else None,
        )
        if timed:
            assert not engine.fresh_depth(
                "BTC/USD", side="bid", at_ns=datetime_ns(current.received_at), maximum_age_ms=250
            )
        else:
            assert len(engine.levels("BTC/USD", side="bid")) == 1


def test_depth_expiring_between_event_and_horizon_cannot_supply_exit():
    from tradeagent.scalping_market import BookLevel, datetime_ns

    evaluator = ShadowActionEvaluator()
    evaluator.add(candidate())
    tape = Tape()
    observe(evaluator, tape.event("q", 0.2, bp="100", bs="1", ap="100.01", **{"as": "10"}))
    event = tape.event("q", 4.9, bp="100", bs="0.01", ap="100.01", **{"as": "10"})
    evaluator.on_market(
        event,
        quote=market_quote(event, size="0.01"),
        bid_levels=(BookLevel(price=99, quantity=100),),
        ask_levels=(),
        depth_at_ns=datetime_ns(NOW + timedelta(seconds=3.95)),
    )
    aggressive = evaluator.flush(NOW + timedelta(seconds=5))[1]
    assert aggressive.filled and not aggressive.complete
    assert "insufficient_observed_exit_depth" in aggressive.missing_reasons


def test_database_replay_keeps_l2_horizon_depth_and_matches_direct_replay(tmp_path):
    tape = Tape()
    snapshot = tape.book(0, ask="100.01", snapshot=True)
    ready = tape.book(0.2, ask="100.01")
    fill = tape.trade(0.5, price="100", quantity="100", side="S")
    horizon = tape.event(
        "o",
        4.9,
        b=[{"p": "100", "s": "0.1"}, {"p": "99.99", "s": "10"}],
        a=[{"p": "100.01", "s": "10"}],
        r=False,
    )
    end = tape.book(5.1, ask="100.01")
    frozen = candidate().model_copy(
        update={
            "signal": signal(NOW + timedelta(milliseconds=20)).model_copy(
                update={
                    "features": {"source_event_id": snapshot.event_id, "normalized_ofi_5s": 0.5}
                }
            ),
            "shadow_send_at": NOW + timedelta(milliseconds=20),
            "shadow_send_at_ns": snapshot.received_at_ns + 10_000_000,
        }
    )
    market = (snapshot, ready, fill, horizon, end)
    direct = simulate_candidates((frozen,), market, action_config())
    with Database(f"sqlite:///{tmp_path / 'tape.db'}") as database:
        database.initialize()
        ScalpStore(database).persist_market_batch(
            [row.model_dump(mode="json") for row in market], at=end.received_at
        )
        replay = simulate_database_candidates(database, (frozen,), action_config())
    assert replay == direct
    assert len(replay) == 2
    assert all(row.complete and row.filled for row in replay)
    assert all(row.exit_quantity == row.entry_quantity for row in replay)


def test_nondefault_cancellation_horizon_survives_sample_conversion():
    row = synthetic_outcome(0).model_copy(update={"cancellation_horizon_seconds": 2.0})
    sample = outcome_sample(
        row,
        source_sha256="a" * 64,
        execution_validation_sha256=None,
        maker_fee_bps=15,
        taker_fee_bps=25,
    )
    assert sample.cancellation_horizon_seconds == 2


def test_old_policy_is_readable_but_cannot_be_replayed_with_new_semantics():
    old = ShadowPolicy(schema_version="shadow-action-policy-v1")
    assert old.identity != ShadowPolicy().identity
    with pytest.raises(ValueError, match="archived"):
        ShadowActionEvaluator(old)


def test_duplicate_outcomes_cannot_inflate_calibration_support():
    row = synthetic_outcome(0)
    with pytest.raises(ValueError, match="duplicate"):
        calibrate_from_shadow_report(
            shadow_report([row, row]),
            config=action_config(),
            calibrated_at=NOW + timedelta(days=8),
            valid_until=NOW + timedelta(days=9),
        )


def test_independent_execution_proof_does_not_require_training_on_old_broker_orders():
    evidence = [
        synthetic_outcome(index).model_copy(
            update={
                "candidate_id": f"historical-{index}",
                "actual_filled": True,
                "actual_passive_comparable": True,
            }
        )
        for index in range(120)
    ]
    training = [synthetic_outcome(index) for index in range(120)]
    validation = validate_passive_simulation(evidence, ShadowPolicy())
    report = shadow_report(training, passive_validation=validation, validation_outcomes=evidence)
    model, audit = calibrate_from_shadow_report(
        report,
        config=action_config(),
        calibrated_at=NOW + timedelta(hours=1),
        valid_until=NOW + timedelta(days=1),
        minimum_observation_days=0.001,
    )
    assert model.status == "validated"
    assert audit["accepted_samples"] == len(training)
    report["validation_outcomes"][0]["actual_filled"] = False
    model, audit = calibrate_from_shadow_report(
        report,
        config=action_config(),
        calibrated_at=NOW + timedelta(hours=1),
        valid_until=NOW + timedelta(days=1),
        minimum_observation_days=0.001,
    )
    assert model.status == "no_support"
    assert "PASSIVE_SIMULATION_NOT_VALIDATED" in audit["blocked_groups"][0]["reasons"]


def test_fresh_l1_is_recorded_without_refreshing_stale_l2_permission():
    from tradeagent.scalping_market import BookFeatureEngine, datetime_ns

    engine = BookFeatureEngine(action_config())
    tape = Tape()
    engine.on_event(tape.book(0, snapshot=True))
    current = tape.event("q", 6, bp="100", bs="10", ap="101", **{"as": "10"})
    engine.on_event(current)
    assert engine.quote("BTC/USD").exchange_time_ns == current.exchange_at_ns
    assert engine.quote_at_ns("BTC/USD", current.received_at_ns) is None
    assert engine.features("BTC/USD", current.received_at) is None
    assert (
        engine.fresh_depth(
            "BTC/USD",
            side="bid",
            at_ns=datetime_ns(current.received_at),
            maximum_age_ms=250,
        )
        == ()
    )


def test_audit_writes_no_support_artifact_without_database_mutation(tmp_path):
    from sqlalchemy import func, select

    from tradeagent.persistence import events
    from tradeagent.scalping_audit import audit_shadow_pipeline

    with Database(f"sqlite:///{tmp_path / 'audit.db'}") as database:
        database.initialize()
        frozen = action_config()
        ScalpStore(database).freeze_run(frozen, "c" * 40, at=NOW)
        with database.begin() as connection:
            before = connection.scalar(select(func.count()).select_from(events))
        result = audit_shadow_pipeline(
            database,
            cohort_id=frozen.cohort_id,
            historical_start=NOW - timedelta(days=2),
            historical_end=NOW - timedelta(days=1),
            at=NOW,
            valid_until=NOW + timedelta(days=1),
            output_dir=tmp_path / "audit-result",
        )
        with database.begin() as connection:
            assert before == connection.scalar(select(func.count()).select_from(events))
        assert result["calibration"]["model_status"] == "no_support"
    assert not result["model_deployed"] and not result["database_modified"]
    assert (tmp_path / "audit-result" / "model.json").is_file()


def test_runtime_commits_ready_outcomes_once_across_concurrent_flushes(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor

    from test_scalping_runtime import runtime_fixture

    with Database(f"sqlite:///{tmp_path / 'counter.db'}") as database:
        database.initialize()
        runtime, _, _, _ = runtime_fixture(database, monkeypatch, NOW)
        row = synthetic_outcome(0)
        runtime.shadow._ready[row.identity] = row
        committed = []
        monkeypatch.setattr(
            "tradeagent.scalping_runtime.persist_shadow_outcomes",
            lambda store, outcomes, at: committed.extend(outcomes),
        )
        with ThreadPoolExecutor(max_workers=4) as executor:
            list(executor.map(lambda _: runtime._persist_shadow(), range(8)))
        assert committed == [row]
        assert runtime._shadow_outcomes == 1
        assert runtime.shadow.unacknowledged_count == 0


def test_runtime_tick_matures_only_through_processed_tape(tmp_path, monkeypatch):
    from unittest.mock import MagicMock

    from test_scalping_runtime import runtime_fixture

    with Database(f"sqlite:///{tmp_path / 'watermark.db'}") as database:
        database.initialize()
        runtime, _, _, _ = runtime_fixture(database, monkeypatch, NOW)
        runtime.shadow.flush = MagicMock()
        runtime._processed_market_at = NOW - timedelta(seconds=30)
        runtime.tick()
        runtime.shadow.flush.assert_called_once_with(NOW - timedelta(seconds=30))
