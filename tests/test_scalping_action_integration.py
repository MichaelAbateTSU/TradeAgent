from datetime import datetime, timedelta

from test_scalping_execution import setup as execution_setup
from test_scalping_replay import filled_tape
from test_scalping_strategy import features

from tradeagent.scalping_economics import (
    CostEstimate,
    EconomicCosts,
    EconomicSample,
    calibrate_model,
    project_economic_features,
)
from tradeagent.scalping_policy import write_model_artifact
from tradeagent.scalping_replay import replay_events
from tradeagent.scalping_strategy import ScalpStrategy

setup = execution_setup


def embedded():
    return CostEstimate(
        included_in_return=True,
        kind="embedded",
        provenance="fixture executed prices already include this cost",
    )


def model(value, action="PASSIVE_BUY", account="a" * 64):
    projected = project_economic_features(value.signal_features())
    samples = []
    for index in range(48):
        at = value.as_of - timedelta(minutes=20) + timedelta(seconds=index * 10)
        samples.append(
            EconomicSample(
                sample_id=f"{action}-{index}",
                account_digest=account,
                source_sha256="b" * 64,
                symbol=value.symbol,
                family="momentum",
                action=action,
                decision_at=at,
                feature_observed_at=at,
                label_matured_at=at + timedelta(seconds=6),
                return_end_at=at + timedelta(seconds=5),
                horizon_seconds=5,
                cancellation_horizon_seconds=3,
                features=projected,
                quote_age_seconds=(value.as_of - value.quote.exchange_at).total_seconds(),
                decision_latency_seconds=(index % 2) * 0.1,
                spread_bps=value.spread_bps,
                notional_usd=100,
                filled=True,
                filled_fraction=1,
                fill_delay_seconds=0.2,
                gross_return_bps=100,
                directional_return_bps=100,
                return_basis="fill_to_fill",
                costs=EconomicCosts(
                    fees=CostEstimate(
                        bps=50,
                        kind="estimated",
                        provenance="25bps entry plus 25bps exit",
                    ),
                    spread=embedded(),
                    slippage=embedded(),
                    impact=embedded(),
                    adverse_selection=embedded(),
                ),
                execution_evidence="observed",
            )
        )
    return calibrate_model(
        samples,
        account_digest=account,
        source_sha256="b" * 64,
        maker_fee_bps=15,
        taker_fee_bps=25,
        horizon_seconds=5,
        cancellation_horizon_seconds=3,
        calibrated_at=value.as_of,
        valid_until=value.as_of + timedelta(hours=1),
    )


def economic_config(value):
    from test_scalping_market import config

    return config(
        decision_policy="action-value-v1",
        catastrophic_stop_bps="100",
        exit_after_seconds=5,
        approved_at=value.as_of - timedelta(hours=1),
    )


def test_strategy_selects_the_best_validated_execution_action_not_merely_direction():
    value = features()
    passive = ScalpStrategy(economic_config(value), model(value)).decide(
        value, inventory=None, now=value.as_of
    )
    assert passive.action == "buy"
    assert passive.economics is not None
    assert passive.economics.selected_action == "PASSIVE_BUY"
    assert passive.expected_net_edge_bps == passive.economics.expected_net_edge_bps
    from tradeagent.scalping_config import ScalpSignal

    assert ScalpSignal.model_validate_json(passive.model_dump_json()) == passive
    aggressive = ScalpStrategy(economic_config(value), model(value, "AGGRESSIVE_BUY")).decide(
        value, inventory=None, now=value.as_of
    )
    assert aggressive.economics is not None
    assert aggressive.economics.selected_action == "AGGRESSIVE_BUY"


def test_missing_or_failed_economic_model_makes_bullish_state_no_trade():
    value = features()
    signal = ScalpStrategy(economic_config(value), None).decide(
        value, inventory=None, now=value.as_of
    )
    assert signal.action == "hold"
    assert signal.economics is not None
    assert signal.economics.selected_action == "NO_TRADE"
    assert "MISSING_MODEL" in signal.economics.reason_codes
    assert signal.score > 0


def test_oms_revalidates_original_action_immediately_before_post(setup):
    database, broker, repo, clock, make = setup
    value = features()
    from test_scalping_execution import ACCOUNT

    current_model = model(value, account=ACCOUNT)
    config = economic_config(value).model_copy(
        update={"cohort_id": "v30-fixture", "account_digest": ACCOUNT}
    )
    strategy = ScalpStrategy(config, current_model)
    signal = strategy.decide(value, inventory=None, now=value.as_of)
    clock[0] = value.as_of
    engine = make(
        decision_policy="action-value-v1",
        catastrophic_stop_bps="100",
        exit_after_seconds=5,
        approved_at=value.as_of - timedelta(hours=1),
        economic_model=current_model,
        quote_provider=lambda _: signal.quote,
    )
    repo.refresh_worker_lock("tradeagent-event-worker", "worker", observed_at=clock[0])
    engine.initialize()
    engine.step((signal,), {"BTC/USD": signal.quote}, now=clock[0])
    assert signal.economics is not None
    with database.begin() as connection:
        event_types = list(connection.exec_driver_sql("SELECT event_type FROM events_v2").scalars())
    assert len(broker.posts) == 1, (engine.status()["error"], event_types)
    with database.begin() as connection:
        assert (
            connection.exec_driver_sql(
                "SELECT count(*) FROM events_v2 WHERE event_type='scalp_economic_dispatch_approved'"
            ).scalar_one()
            == 1
        )
        cycle = (
            connection.exec_driver_sql("SELECT exit_due_at FROM scalping_cycles").mappings().one()
        )
    exit_due = cycle["exit_due_at"]
    if isinstance(exit_due, str):
        exit_due = datetime.fromisoformat(exit_due)
    if exit_due.tzinfo is None:
        exit_due = exit_due.replace(tzinfo=signal.observed_at.tzinfo)
    assert exit_due == signal.economics.prediction_expires_at


def test_oms_rejects_when_passive_limit_is_no_longer_at_the_current_touch(setup):
    _, broker, repo, clock, make = setup
    value = features()
    from test_scalping_execution import ACCOUNT

    current_model = model(value, account=ACCOUNT)
    strategy = ScalpStrategy(
        economic_config(value).model_copy(update={"account_digest": ACCOUNT}),
        current_model,
    )
    signal = strategy.decide(value, inventory=None, now=value.as_of)
    moved = signal.quote.model_copy(
        update={"bid": signal.quote.bid + 1, "ask": signal.quote.ask + 1}
    )
    clock[0] = value.as_of
    engine = make(
        decision_policy="action-value-v1",
        catastrophic_stop_bps="100",
        exit_after_seconds=5,
        approved_at=value.as_of - timedelta(hours=1),
        economic_model=current_model,
        quote_provider=lambda _: moved,
    )
    repo.refresh_worker_lock("tradeagent-event-worker", "worker", observed_at=clock[0])
    engine.initialize()
    engine.step((signal,), {"BTC/USD": signal.quote}, now=clock[0])
    assert broker.posts == []


def test_replay_uses_the_same_loaded_action_model_and_missing_model_is_no_trade(tmp_path):
    value = features()
    current_model = model(value)
    path = tmp_path / "model.json"
    digest = write_model_artifact(current_model, path)
    configured = economic_config(value).model_copy(
        update={"economic_model_path": str(path), "economic_model_sha256": digest}
    )
    report = replay_events(filled_tape(), configured)
    assert report["orders"]
    assert all(
        decision["economics"] is not None
        for decision in report["decisions"]
        if decision["action"] == "buy"
    )
    short = replay_events(filled_tape(end=6), configured)
    first_buy = next(decision for decision in short["decisions"] if decision["action"] == "buy")
    assert (
        short["reported_inventory"][0]["exit_due_at"]
        == first_buy["economics"]["prediction_expires_at"]
    )
    missing = replay_events(
        filled_tape(),
        economic_config(value),
    )
    assert missing["orders"] == []
    evaluated = [
        decision["economics"]
        for decision in missing["decisions"]
        if decision["economics"] is not None
    ]
    assert evaluated
    assert {decision["selected_action"] for decision in evaluated} == {"NO_TRADE"}


def test_replay_counts_scheduled_decision_to_send_latency_once(tmp_path):
    from tradeagent.scalping_replay import ReplayLatency

    value = features()
    current_model = model(value)
    path = tmp_path / "latency-model.json"
    digest = write_model_artifact(current_model, path)
    configured = economic_config(value).model_copy(
        update={"economic_model_path": str(path), "economic_model_sha256": digest}
    )
    report = replay_events(
        filled_tape(),
        configured,
        latency=ReplayLatency(
            decision_to_send_ms=100,
            send_to_arrival_ms=20,
            arrival_to_ack_ms=20,
            cancel_latency_ms=20,
        ),
    )
    assert report["orders"], "100ms is inside the calibrated 0-100ms latency support"
    first_buy = next(decision for decision in report["decisions"] if decision["action"] == "buy")
    assert first_buy["economics"]["decision_latency_seconds"] == 0
