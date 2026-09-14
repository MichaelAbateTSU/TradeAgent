from datetime import timedelta
from decimal import Decimal

import pytest
from sqlalchemy import func, select
from test_scalping_market import NOW, Tape, config

from tradeagent.persistence import Database, events
from tradeagent.scalping_config import ScalpQuote, ScalpSignal
from tradeagent.scalping_market import BookLevel, datetime_ns
from tradeagent.scalping_policy import calibrate_from_shadow_report
from tradeagent.scalping_shadow import (
    ShadowActionEvaluator,
    ShadowActionOutcome,
    ShadowPolicy,
    candidate_from_signal,
    estimate_collection_days,
    recover_abandoned_candidates,
    shadow_report,
    validate_passive_simulation,
)


def quote(at, bid="100", ask="100.01", size="10"):
    return ScalpQuote(
        symbol="BTC/USD",
        exchange_at=at,
        exchange_time_ns=datetime_ns(at),
        received_at=at,
        bid=bid,
        ask=ask,
        bid_size=size,
        ask_size=size,
    )


def signal(at=NOW):
    return ScalpSignal(
        decision_id=f"shadow-{datetime_ns(at)}",
        symbol="BTC/USD",
        observed_at=at,
        action="hold",
        family="momentum",
        score=0.5,
        reasons=("economic_no_trade",),
        quote=quote(at, size="1"),
        features={
            "source_event_id": "decision-source",
            "normalized_ofi_5s": 0.5,
            "spread_bps": 1.0,
        },
        estimated_round_trip_cost_bps=51,
    )


def action_config():
    return config(
        decision_policy="action-value-v1",
        catastrophic_stop_bps="100",
        exit_after_seconds=5,
    )


def market_quote(event, bid="100", ask="100.01", size="10"):
    return quote(event.received_at, bid=bid, ask=ask, size=size).model_copy(
        update={
            "exchange_at": event.exchange_at,
            "exchange_time_ns": event.exchange_at_ns,
        }
    )


def completed_outcomes(actual_filled=True):
    evaluator = ShadowActionEvaluator(
        ShadowPolicy(
            arrival_latency_ms=350,
            minimum_validation_candidates=20,
        )
    )
    candidate = candidate_from_signal(
        signal(),
        run_id="run",
        account_digest="a" * 64,
        config=action_config(),
        shadow_send_at=NOW,
        bid_levels=(BookLevel(price=100, quantity=1),),
        ask_levels=(BookLevel(price="100.01", quantity=10),),
        actual_filled=actual_filled,
        actual_passive_comparable=True,
    )
    evaluator.add(candidate)
    tape = Tape()
    arrival = tape.event("q", 0.4, bp="100", bs="1", ap="100.01", **{"as": "10"})
    assert (
        evaluator.on_market(
            arrival,
            quote=market_quote(arrival),
            bid_levels=(BookLevel(price=100, quantity=1),),
            ask_levels=(BookLevel(price="100.01", quantity=10),),
        )
        == ()
    )
    trade = tape.trade(0.5, price="100", quantity="2", side="S")
    assert (
        evaluator.on_market(
            trade,
            quote=market_quote(arrival),
            bid_levels=(BookLevel(price=100, quantity=1),),
            ask_levels=(BookLevel(price="100.01", quantity=10),),
        )
        == ()
    )
    horizon = tape.event("q", 4.9, bp="100.20", bs="10", ap="100.21", **{"as": "10"})
    assert (
        evaluator.on_market(
            horizon,
            quote=market_quote(horizon, bid="100.20", ask="100.21"),
            bid_levels=(BookLevel(price="100.20", quantity=10),),
            ask_levels=(BookLevel(price="100.21", quantity=10),),
        )
        == ()
    )
    finished = tape.event("q", 5.1, bp="100.20", bs="10", ap="100.21", **{"as": "10"})
    return evaluator.on_market(
        finished,
        quote=market_quote(finished, bid="100.20", ask="100.21"),
        bid_levels=(BookLevel(price="100.20", quantity=10),),
        ask_levels=(BookLevel(price="100.21", quantity=10),),
    )


def test_shadow_evaluator_compares_passive_and_aggressive_without_orders():
    outcomes = completed_outcomes()
    assert len(outcomes) == 2
    by_action = {row.action: row for row in outcomes}
    passive = by_action["PASSIVE_BUY"]
    aggressive = by_action["AGGRESSIVE_BUY"]
    assert passive.complete and aggressive.complete
    assert passive.filled and aggressive.filled
    assert passive.fill_delay_seconds == pytest.approx(0.51)
    assert passive.entry_value == passive.entry_quantity * Decimal(100)
    assert aggressive.entry_value == aggressive.entry_quantity * Decimal("100.01")
    assert passive.gross_return_bps > aggressive.gross_return_bps > 0
    assert evaluator_pending(outcomes) == 0


def test_completed_outcomes_remain_retryable_until_durable_acknowledgement():
    evaluator = ShadowActionEvaluator()
    candidate = candidate_from_signal(
        signal(),
        run_id="run",
        account_digest="a" * 64,
        config=action_config(),
        shadow_send_at=NOW,
    )
    evaluator.add(candidate)
    outcomes = evaluator.flush(NOW + timedelta(seconds=6))
    assert len(outcomes) == evaluator.unacknowledged_count == 2
    assert evaluator.flush(NOW + timedelta(seconds=7)) == outcomes
    evaluator.acknowledge(outcomes)
    assert evaluator.unacknowledged_count == 0
    assert evaluator.flush(NOW + timedelta(seconds=8)) == ()


def evaluator_pending(outcomes):
    return sum(not row.complete for row in outcomes)


def test_passive_validation_requires_high_precision_and_never_validates_aggressive():
    rows = []
    for index in range(100):
        base = completed_outcomes(actual_filled=index < 90)[0]
        rows.append(
            base.model_copy(
                update={
                    "candidate_id": f"candidate-{index}",
                    "actual_filled": index < 90,
                    "filled": index < 85,
                    "entry_quantity": base.entry_quantity if index < 85 else Decimal(0),
                    "entry_value": base.entry_value if index < 85 else Decimal(0),
                    "filled_fraction": base.filled_fraction if index < 85 else 0,
                    "fill_delay_seconds": base.fill_delay_seconds if index < 85 else None,
                    "exit_quantity": base.exit_quantity if index < 85 else Decimal(0),
                    "exit_value": base.exit_value if index < 85 else Decimal(0),
                    "gross_return_bps": base.gross_return_bps if index < 85 else None,
                }
            )
        )
    validation = validate_passive_simulation(rows, ShadowPolicy())
    assert validation["passed"] is True
    assert validation["precision"] == 1
    assert validation["aggressive_execution_validated"] is False
    report = shadow_report(rows, passive_validation=validation)
    assert report["groups"][0]["calibration_population_supported"] is True
    assert report["aggressive_execution_supported"] is False
    unrelated = [
        row.model_copy(
            update={
                "candidate_id": f"aggressive-{index}",
                "action": "AGGRESSIVE_BUY",
                "actual_passive_comparable": False,
            }
        )
        for index, row in enumerate(rows)
    ]
    assert validate_passive_simulation(unrelated, ShadowPolicy())["candidate_count"] == 0


def test_stale_arrival_market_is_incomplete_not_a_simulated_fill():
    evaluator = ShadowActionEvaluator()
    evaluator.add(
        candidate_from_signal(
            signal(),
            run_id="run",
            account_digest="a" * 64,
            config=action_config(),
            shadow_send_at=NOW,
        )
    )
    tape = Tape()
    late = tape.trade(1, price="100", quantity="100", side="S")
    stale_quote = quote(NOW, size="100")
    evaluator.on_market(
        late,
        quote=stale_quote,
        bid_levels=(BookLevel(price=100, quantity=100),),
        ask_levels=(BookLevel(price="100.01", quantity=100),),
    )
    outcomes = evaluator.flush(NOW + timedelta(seconds=6))
    assert all(not row.complete and not row.filled for row in outcomes)
    assert {reason for row in outcomes for reason in row.missing_reasons} == {
        "no_fresh_market_at_simulated_arrival",
        "horizon_quote_too_old",
    }


def synthetic_outcome(index, *, action="PASSIVE_BUY", complete=True, filled=True):
    at = NOW + timedelta(seconds=index * 10)
    gross = 100.0 if filled else None
    return ShadowActionOutcome(
        candidate_id=f"candidate-{index}",
        run_id="run",
        account_digest="a" * 64,
        decision_id=f"decision-{index}",
        symbol="BTC/USD",
        family="momentum",
        features={"normalized_ofi_5s": 0.5},
        action=action,
        decision_at=at,
        shadow_send_at=at + timedelta(milliseconds=10),
        decision_latency_seconds=0.01,
        arrival_at=at + timedelta(milliseconds=360),
        horizon_at=at + timedelta(seconds=5),
        quantity=Decimal(1),
        entry_quantity=Decimal(1) if filled else Decimal(0),
        entry_value=Decimal(100) if filled else Decimal(0),
        filled=filled,
        filled_fraction=1 if filled else 0,
        fill_delay_seconds=0.5 if filled else None,
        exit_quantity=Decimal(1) if filled else Decimal(0),
        exit_value=Decimal(101) if filled else Decimal(0),
        gross_return_bps=gross,
        directional_return_bps=100,
        decision_mid=Decimal(100),
        horizon_mid=Decimal(101),
        quote_age_seconds=0.05,
        spread_bps=1,
        notional_usd=Decimal(100),
        complete=complete,
        missing_reasons=() if complete else ("horizon_quote_too_old",),
        simulation_policy=ShadowPolicy(),
        source_event_first=f"event-{index}",
        source_event_last=f"event-{index}",
        source_event_count=1,
        source_event_sha256="a" * 64,
        actual_filled=None,
        actual_first_fill_at=None,
        actual_passive_comparable=False,
    )


def test_shadow_calibration_is_chronological_and_blocks_unvalidated_or_incomplete_groups():
    outcomes = [
        synthetic_outcome(index).model_copy(
            update={
                "actual_filled": True,
                "actual_passive_comparable": True,
            }
        )
        for index in range(120)
    ]
    validation = validate_passive_simulation(outcomes, ShadowPolicy())
    report = shadow_report(outcomes, passive_validation=validation)
    model, audit = calibrate_from_shadow_report(
        report,
        config=action_config(),
        calibrated_at=NOW + timedelta(hours=1),
        valid_until=NOW + timedelta(days=1),
        minimum_observation_days=0.001,
    )
    assert model.status == "validated"
    assert audit["accepted_samples"] == 120
    assert model.validation_start_at > model.training_label_cutoff_at
    aggressive = [synthetic_outcome(index, action="AGGRESSIVE_BUY") for index in range(120)]
    unsupported, audit = calibrate_from_shadow_report(
        shadow_report(aggressive, passive_validation=validation),
        config=action_config(),
        calibrated_at=NOW + timedelta(hours=1),
        valid_until=NOW + timedelta(days=1),
        minimum_observation_days=0.001,
    )
    assert unsupported.status == "no_support"
    assert (
        "AGGRESSIVE_EXECUTION_NOT_HISTORICALLY_VALIDATED" in audit["blocked_groups"][0]["reasons"]
    )
    incomplete = [
        *outcomes[:-7],
        *(synthetic_outcome(200 + i, complete=False) for i in range(7)),
    ]
    unsupported, audit = calibrate_from_shadow_report(
        shadow_report(incomplete, passive_validation=validation),
        config=action_config(),
        calibrated_at=NOW + timedelta(hours=1),
        valid_until=NOW + timedelta(days=1),
        minimum_observation_days=0.001,
    )
    assert unsupported.status == "no_support"
    assert (
        "INCOMPLETE_OUTCOME_RATE_ABOVE_PREDECLARED_LIMIT" in audit["blocked_groups"][0]["reasons"]
    )
    tampered = dict(validation)
    tampered["validation_sha256"] = "b" * 64
    unsupported, audit = calibrate_from_shadow_report(
        shadow_report(outcomes, passive_validation=tampered),
        config=action_config(),
        calibrated_at=NOW + timedelta(hours=1),
        valid_until=NOW + timedelta(days=1),
        minimum_observation_days=0.001,
    )
    assert unsupported.status == "no_support"
    assert "PASSIVE_SIMULATION_NOT_VALIDATED" in audit["blocked_groups"][0]["reasons"]


def test_wait_estimate_is_support_rate_not_a_profitability_promise():
    outcomes = [synthetic_outcome(index, filled=index % 4 == 0) for index in range(100)]
    estimate = estimate_collection_days(
        outcomes,
        target_candidates=200,
        target_fills=30,
        minimum_observation_days=7,
    )
    group = estimate["groups"][0]
    assert group["estimated_additional_days_for_support"] > 0
    assert estimate["valid_model_guaranteed"] is False
    assert "held-out" in estimate["interpretation"]


def test_restart_materializes_abandoned_candidate_as_incomplete_denominator(tmp_path):
    from tradeagent.scalping_store import ScalpStore

    with Database(f"sqlite:///{tmp_path / 'restart.db'}") as database:
        database.initialize()
        candidate = candidate_from_signal(
            signal(),
            run_id="run",
            account_digest="a" * 64,
            config=action_config(),
            shadow_send_at=NOW,
        )
        ScalpStore(database).audit(
            "shadow_action_candidate",
            candidate.model_dump(mode="json"),
            at=NOW,
            identity=f"shadow-candidate:{candidate.candidate_id}",
        )
        assert (
            recover_abandoned_candidates(
                database,
                run_id="run",
                account_digest="a" * 64,
                config=action_config(),
                now=NOW + timedelta(seconds=10),
            )
            == 2
        )
        assert (
            recover_abandoned_candidates(
                database,
                run_id="run",
                account_digest="a" * 64,
                config=action_config(),
                now=NOW + timedelta(seconds=11),
            )
            == 0
        )
        with database.begin() as connection:
            assert (
                connection.scalar(
                    select(func.count())
                    .select_from(events)
                    .where(events.c.event_type == "scalp_shadow_action_outcome")
                )
                == 2
            )
