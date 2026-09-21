from datetime import timedelta
from decimal import Decimal

from sqlalchemy import select
from test_scalping_execution import (
    ACCOUNT,
    NOW,
    advance,
    quote,
    signal,
)
from test_scalping_execution import (
    setup as execution_setup,
)

from tradeagent.scalping_experiments import (
    ExecutionAcceptanceCohort,
    ExecutionAcceptancePolicy,
    ExperimentalScalpCohort,
    ExperimentalScalpPolicy,
)
from tradeagent.scalping_policy import (
    calibrate_from_actual_experiments,
    load_economic_model,
    write_model_artifact,
)
from tradeagent.scalping_reporting import scalping_experiment_status
from tradeagent.scalping_store import scalping_cycles

setup = execution_setup


def test_marketable_acceptance_completes_broker_round_trip(setup) -> None:
    database, broker, _, _, make = setup
    engine = make(entry_order_ttl_seconds=5)
    engine.initialize()
    policy = ExecutionAcceptancePolicy()
    cohort = ExecutionAcceptanceCohort(engine, policy)

    result = cohort.step({"BTC/USD": quote(NOW)}, now=NOW)

    assert result["state"] == "submitted"
    assert broker.posts[0].client_order_id.startswith("ta30x-")
    assert broker.values[broker.posts[0].client_order_id].filled_average_price == Decimal(
        "100.03"
    )
    now = advance(setup, 5)
    broker.market_price = Decimal("100.50")
    engine.step((), {"BTC/USD": quote(now)}, now=now)
    now = advance(setup, 6)
    engine.step((), {"BTC/USD": quote(now)}, now=now)

    with database.begin() as connection:
        cycle = (
            connection.execute(
                select(scalping_cycles).where(
                    scalping_cycles.c.payload["classification"].as_string()
                    == "execution_acceptance_test"
                )
            )
            .mappings()
            .one()
        )
    assert cycle["state"] == "closed_owned_flat"
    assert cycle["entry_quantity"] > 0
    assert cycle["exit_quantity"] > 0
    assert cycle["owned_quantity"] == 0
    assert len(broker.posts) == 2


def test_experimental_scalp_waits_for_acceptance_and_uses_signal(setup) -> None:
    _, broker, _, _, make = setup
    engine = make(entry_order_ttl_seconds=5)
    engine.initialize()
    experimental = ExperimentalScalpCohort(engine, ExperimentalScalpPolicy())
    candidate = signal(NOW, "candidate").model_copy(
        update={"features": {"return_5s_bps": 2.0}}
    )

    blocked = experimental.step(
        (candidate,), {"BTC/USD": quote(NOW)}, now=NOW
    )

    assert blocked["state"] == "blocked"
    assert blocked["block_reasons"]["execution_acceptance_complete"] == 1
    acceptance = ExecutionAcceptanceCohort(engine, ExecutionAcceptancePolicy())
    acceptance.step({"BTC/USD": quote(NOW)}, now=NOW)
    now = advance(setup, 5)
    engine.step((), {"BTC/USD": quote(now)}, now=now)
    now = advance(setup, 6)
    engine.step((), {"BTC/USD": quote(now)}, now=now)
    now = advance(setup, 60)
    candidate = signal(now, "candidate-2").model_copy(
        update={"features": {"return_5s_bps": 2.0}}
    )

    submitted = experimental.step(
        (candidate,), {"BTC/USD": quote(now)}, now=now
    )

    assert submitted["state"] == "submitted"
    assert broker.posts[-1].client_order_id.startswith("ta30e-")
    assert broker.values[broker.posts[-1].client_order_id].filled_average_price == Decimal(
        "100.03"
    )


def test_marketable_dispatch_rechecks_fresh_quote_inside_original_cap(setup) -> None:
    _, broker, _, clock, make = setup
    engine = make(
        entry_order_ttl_seconds=5,
        decision_policy="action-value-v1",
        catastrophic_stop_bps=Decimal("100"),
        decision_interval_seconds=1,
        feature_horizon_seconds=5,
        quote_provider=lambda _symbol: quote(clock[0]),
    )
    engine.initialize()

    def age_original_quote() -> None:
        clock[0] = NOW + timedelta(seconds=1.1)
        broker.account_hook = None

    broker.account_hook = age_original_quote
    client_id = engine.submit_paper_experiment(
        policy=ExecutionAcceptancePolicy(),
        quote=quote(NOW),
        now=NOW,
    )

    assert client_id is not None
    assert broker.posts[0].client_order_id == client_id
    assert broker.values[client_id].filled_average_price == Decimal("100.03")


def test_marketable_dispatch_rejects_fresh_quote_above_original_cap(setup) -> None:
    database, broker, _, clock, make = setup
    engine = make(
        entry_order_ttl_seconds=5,
        decision_policy="action-value-v1",
        catastrophic_stop_bps=Decimal("100"),
        decision_interval_seconds=1,
        feature_horizon_seconds=5,
        quote_provider=lambda _symbol: quote(clock[0], ask="100.04"),
    )
    engine.initialize()

    def age_original_quote() -> None:
        clock[0] = NOW + timedelta(seconds=1.1)
        broker.account_hook = None

    broker.account_hook = age_original_quote
    client_id = engine.submit_paper_experiment(
        policy=ExecutionAcceptancePolicy(),
        quote=quote(NOW),
        now=NOW,
    )

    assert client_id is not None
    assert not broker.posts
    report = scalping_experiment_status(database, account_digest=ACCOUNT)
    order = report["cycles"][0]["orders"][0]
    assert order["dispatch_state"] == "expired_unsent"
    assert order["submission_error"]["pre_submit_rejection"] is True
    assert (
        order["submission_error"]["reason"]
        == "FRESH_DISPATCH_ASK_EXCEEDS_ORIGINAL_CAP"
    )


def test_report_counts_one_cycle_not_order_updates(setup) -> None:
    database, broker, _, _, make = setup
    engine = make(entry_order_ttl_seconds=5)
    engine.initialize()
    policy = ExperimentalScalpPolicy()
    candidate = signal(NOW, "candidate").model_copy(
        update={"features": {"return_5s_bps": 2.0}}
    )
    engine.submit_paper_experiment(
        policy=policy, quote=quote(NOW), now=NOW, signal=candidate
    )
    now = advance(setup, 5)
    broker.market_price = Decimal("101")
    engine.step((), {"BTC/USD": quote(now)}, now=now)
    now = advance(setup, 6)
    engine.step((), {"BTC/USD": quote(now)}, now=now)

    report = scalping_experiment_status(database, account_digest=ACCOUNT)

    assert report["funnel"]["submissions"] == 2
    assert report["funnel"]["entry_submissions"] == 1
    assert report["funnel"]["broker_acceptances"] == 2
    assert report["funnel"]["completed_exits"] == 1
    assert report["evidence_accounting"]["qualifying_independent_round_trips"] == 1
    assert report["evidence_accounting"]["reason"] == (
        "INSUFFICIENT_ACTUAL_COMPLETED_ROUND_TRIPS"
    )


def test_actual_round_trips_calibrate_write_and_reload_validated_model(
    setup, tmp_path
) -> None:
    database, broker, _, _, make = setup
    engine = make(entry_order_ttl_seconds=5, feature_horizon_seconds=10)
    engine.initialize()
    policy = ExperimentalScalpPolicy()
    for index in range(30):
        now = advance(setup, index * 70)
        q = quote(now)
        candidate = signal(now, f"candidate-{index}").model_copy(
            update={"features": {"return_5s_bps": 2.0 + (index % 5) / 100}}
        )
        client_id = engine.submit_paper_experiment(
            policy=policy, quote=q, now=now, signal=candidate
        )
        assert client_id is not None, index
        now = advance(setup, index * 70 + 5)
        broker.market_price = Decimal("102")
        engine.step((), {"BTC/USD": quote(now)}, now=now)
        now = advance(setup, index * 70 + 6)
        engine.step((), {"BTC/USD": quote(now)}, now=now)
    calibrated_at = NOW + timedelta(hours=1)
    model, audit = calibrate_from_actual_experiments(
        database,
        account_digest=ACCOUNT,
        maker_fee_bps=15,
        taker_fee_bps=25,
        calibrated_at=calibrated_at,
        valid_until=calibrated_at + timedelta(hours=1),
    )

    assert audit["accepted_sample_count"] == 30
    assert audit["eligible_for_calibration"] is True
    assert model.status == "validated", model.reason_codes
    assert audit["worker_load_allowed"] is True, audit
    path = tmp_path / "actual-model.json"
    digest = write_model_artifact(model, path)
    config = make(
        entry_order_ttl_seconds=5,
        feature_horizon_seconds=10,
        decision_policy="action-value-v1",
        economic_model_path=str(path),
        economic_model_sha256=digest,
        catastrophic_stop_bps=Decimal("100"),
    ).config
    loaded = load_economic_model(config)
    assert loaded is not None
    assert loaded.model_id == model.model_id
