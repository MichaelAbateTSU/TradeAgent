from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx
from sqlalchemy import select, update
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
    PaperExperimentPolicy,
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
    entry_notional = broker.posts[0].quantity * Decimal("100.03")
    assert entry_notional * Decimal("0.9975") >= Decimal("10.05")
    assert entry_notional <= policy.max_order_notional_usd
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
    database, broker, _, _, make = setup
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
    now = advance(setup, 65)
    broker.market_price = Decimal("99")
    engine.step((), {"BTC/USD": quote(now)}, now=now)
    now = advance(setup, 66)
    engine.step((), {"BTC/USD": quote(now)}, now=now)
    report = scalping_experiment_status(database, account_digest=ACCOUNT)
    completed = next(
        cycle
        for cycle in report["cycles"]
        if cycle["classification"] == "experimental_signal_scalp"
    )
    assert completed["state"] == "closed_owned_flat"
    assert completed["signal"]["decision_id"] == candidate.decision_id
    assert completed["decision_id"].startswith("experiment:")
    assert completed["policy_hash"] == ExperimentalScalpPolicy().identity
    assert completed["owned_quantity"] == "0E-18"
    assert len(completed["orders"]) == 2
    assert all(order["broker_order_id"] for order in completed["orders"])
    assert all(order["first_positive_fill_at"] for order in completed["orders"])
    assert completed["modeled_net_pnl_usd"] is not None
    assert report["cohort_economics"]["observed_result"] == "negative_after_modeled_costs"


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


def test_marketable_dispatch_uses_bounded_experiment_quote_age(setup) -> None:
    _, broker, _, clock, make = setup
    original_quote = quote(NOW)
    engine = make(
        entry_order_ttl_seconds=5,
        decision_policy="action-value-v1",
        catastrophic_stop_bps=Decimal("100"),
        decision_interval_seconds=1,
        feature_horizon_seconds=5,
        quote_provider=lambda _symbol: original_quote,
    )
    engine.initialize()

    def age_original_quote() -> None:
        clock[0] = NOW + timedelta(seconds=1.5)
        broker.account_hook = None

    broker.account_hook = age_original_quote
    client_id = engine.submit_paper_experiment(
        policy=ExecutionAcceptancePolicy(),
        quote=original_quote,
        now=NOW,
    )

    assert client_id is not None
    assert broker.posts[0].client_order_id == client_id


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


def test_broker_rejection_details_are_exposed_in_experiment_report(setup) -> None:
    database, broker, _, _, make = setup
    engine = make(entry_order_ttl_seconds=5)
    engine.initialize()

    def reject(*_args, **_kwargs):
        response = httpx.Response(
            403,
            json={
                "code": 40310000,
                "message": "cost basis must be >= minimal amount of order 10",
            },
            request=httpx.Request("POST", broker.broker_host + "/v2/orders"),
        )
        raise httpx.HTTPStatusError(
            "rejected",
            request=response.request,
            response=response,
        )

    broker.submit_crypto_limit_order = reject
    engine.submit_paper_experiment(
        policy=ExecutionAcceptancePolicy(),
        quote=quote(NOW),
        now=NOW,
    )

    report = scalping_experiment_status(database, account_digest=ACCOUNT)
    error = report["cycles"][0]["orders"][0]["submission_error"]
    assert error == {
        "definitive_rejection": True,
        "status_code": 403,
        "broker_code": 40310000,
        "broker_message": "cost basis must be >= minimal amount of order 10",
    }


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
    assert report["cohort_economics"]["completed_round_trips"] == 1
    assert report["cohort_economics"]["modeled_net_coverage"] == 1
    assert report["cohort_economics"]["pending_fee_cycles"] == 0
    assert report["cohort_economics"]["actual_net_pnl_usd"] is not None
    assert report["funnel_by_symbol"] == [
        {
            "classification": "experimental_signal_scalp",
            "symbol": "BTC/USD",
            "broker_acceptances": 2,
            "completed_exits": 1,
            "cycles": 1,
            "entry_fills": 1,
            "entry_submissions": 1,
            "flat_reconciliations": 1,
            "submissions": 2,
        }
    ]


def test_experiment_report_excludes_wrong_cohort_and_cutoff_records(setup) -> None:
    database, _, _, _, make = setup
    engine = make(entry_order_ttl_seconds=5)
    engine.initialize()
    policy = ExecutionAcceptancePolicy()
    unrelated = PaperExperimentPolicy(
        policy_id="unrelated-acceptance",
        cohort_id="unrelated-acceptance-cohort",
        classification=policy.classification,
        decision_prefix="unrelated",
        strategy_id="unrelated-acceptance",
        daily_submitted_cap=1,
        daily_filled_cycle_cap=1,
        schedule_interval_seconds=60,
        entry_ttl_seconds=5,
        exit_after_seconds=5,
    )
    assert (
        engine.submit_paper_experiment(policy=unrelated, quote=quote(NOW), now=NOW)
        is not None
    )
    engine.store.audit(
        "paper_experiment_candidate",
        {
            "cohort_id": unrelated.cohort_id,
            "classification": unrelated.classification,
            "eligible": False,
        },
        at=NOW,
    )
    assert (
        engine.submit_paper_experiment(policy=policy, quote=quote(NOW), now=NOW)
        is None
    )
    with database.begin() as connection:
        connection.execute(
            update(scalping_cycles)
            .where(
                scalping_cycles.c.payload["probe_policy"]["cohort_id"].as_string()
                == unrelated.cohort_id
            )
            .values(
                payload={
                    "classification": policy.classification,
                    "probe_policy": policy.model_dump(mode="json"),
                },
                created_at=policy.authorization_cutoff,
            )
        )
    engine.store.audit(
        "paper_experiment_candidate",
        {
            "cohort_id": policy.cohort_id,
            "classification": policy.classification,
            "eligible": False,
        },
        at=policy.authorization_cutoff,
    )

    report = scalping_experiment_status(database, account_digest=ACCOUNT)

    assert report["cycles"] == []
    assert report["funnel"]["submissions"] == 0
    assert report["funnel"]["candidates"] == 0


def test_report_counts_all_candidates_but_returns_only_latest_200(setup) -> None:
    database, _, _, _, make = setup
    engine = make(entry_order_ttl_seconds=5)
    engine.initialize()
    policy = ExperimentalScalpPolicy()
    observed_at = datetime(2026, 9, 21, 12, tzinfo=UTC)
    for index in range(250):
        engine.store.audit(
            "paper_experiment_candidate",
            {
                "cohort_id": policy.cohort_id,
                "classification": policy.classification,
                "account_digest": ACCOUNT,
                "symbol": "BTC/USD" if index % 2 == 0 else "ETH/USD",
                "eligible": index % 3 != 0,
                "checks": [{"name": "signal_score", "actual": index, "threshold": 10}],
            },
            at=observed_at + timedelta(seconds=index),
        )
    report = scalping_experiment_status(database, account_digest=ACCOUNT)
    assert report["funnel"]["candidates"] == 250
    assert report["funnel"]["blocked_candidates"] == 84
    assert len(report["candidate_checks"]) == 200
    assert report["candidate_checks"][0]["payload"]["checks"][0]["actual"] == 50
    assert sum(row["candidates"] for row in report["funnel_by_symbol"]) == 250
    assert report["candidate_account_scope"]["candidate_totals_complete"] is True


def test_calibration_and_report_exclude_mismatched_policy_hash(setup) -> None:
    database, _, _, _, make = setup
    engine = make(entry_order_ttl_seconds=5)
    engine.initialize()
    policy = ExperimentalScalpPolicy()
    assert engine.submit_paper_experiment(
        policy=policy, quote=quote(NOW), now=NOW, signal=signal(NOW, "changed-policy")
    )
    with database.begin() as connection:
        row = connection.execute(
            select(scalping_cycles).where(
                scalping_cycles.c.payload["classification"].as_string()
                == "experimental_signal_scalp"
            )
        ).mappings().one()
        payload = dict(row["payload"])
        stored_policy = dict(payload["probe_policy"])
        stored_policy["max_order_notional_usd"] = "10.24"
        payload["probe_policy"] = stored_policy
        connection.execute(
            update(scalping_cycles)
            .where(scalping_cycles.c.cycle_id == row["cycle_id"])
            .values(payload=payload, state="closed_owned_flat", closed_at=NOW)
        )
    report = scalping_experiment_status(database, account_digest=ACCOUNT)
    assert report["cycles"] == []
    assert report["evidence_accounting"]["exclusion_reasons"] == {
        "POLICY_HASH_MISMATCH": 1
    }
    model, audit = calibrate_from_actual_experiments(
        database,
        account_digest=ACCOUNT,
        maker_fee_bps=15,
        taker_fee_bps=25,
        calibrated_at=NOW + timedelta(minutes=1),
        valid_until=NOW + timedelta(hours=1),
    )
    assert model.status == "no_support"
    assert audit["scoped_closed_cycle_count"] == 1
    assert audit["exclusion_reasons"] == {"POLICY_HASH_MISMATCH": 1}
    assert audit["accepted_sample_count"] == 0


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
    assert audit["cohort_id"] == policy.cohort_id
    assert audit["artifact_horizon_seconds"] == 10
    assert audit["five_second_worker_compatible"] is False
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
