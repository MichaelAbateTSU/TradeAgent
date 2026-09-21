import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError
from sqlalchemy import insert
from test_scalping_execution import Broker

from tradeagent.api import create_app
from tradeagent.persistence import Database, ProductionRepository, controls
from tradeagent.scalping_config import ScalpingConfig, ScalpQuote
from tradeagent.scalping_execution import ScalpOrderEngine
from tradeagent.scalping_probes import (
    AUTHORIZATION_CUTOFF,
    ExecutionValidationProbeCohort,
    ExecutionValidationProbePolicy,
    held_out_probe_report,
    probe_observations,
)
from tradeagent.scalping_shadow import load_historical_candidates


def quote(now: datetime, symbol: str = "BTC/USD") -> ScalpQuote:
    return ScalpQuote(
        symbol=symbol,
        exchange_at=now,
        exchange_time_ns=int(now.timestamp() * 1_000_000_000),
        received_at=now,
        bid=Decimal("100"),
        ask=Decimal("100.01"),
        bid_size=Decimal("2"),
        ask_size=Decimal("1"),
    )


@pytest.fixture
def probe_setup(tmp_path: Path):
    now = datetime(2026, 9, 16, 18, tzinfo=UTC)
    current = [now]
    with Database(f"sqlite:///{tmp_path / 'probes.db'}") as database:
        database.initialize()
        broker = Broker(lambda: current[0])
        account = sha256(broker.account_id.encode()).hexdigest()
        ProductionRepository(database).acquire_worker_lock(
            "tradeagent-event-worker", "owner", observed_at=now
        )
        engine = ScalpOrderEngine(
            database,
            broker,
            ScalpingConfig(
                cohort_id="v30-action-value-20260914-seven-day-r1",
                account_digest=account,
                approved_at=now - timedelta(minutes=1),
                decision_policy="action-value-v1",
                catastrophic_stop_bps=Decimal("100"),
            ),
            owner_id="owner",
            code_sha="a" * 40,
            clock=lambda: current[0],
        )
        engine.initialize()
        yield database, broker, engine, current


def test_policy_is_immutable_and_restricted_to_paper_pairs_and_cutoff() -> None:
    policy = ExecutionValidationProbePolicy()
    assert policy.description()["one_global_outstanding_order_or_owned_exposure"] is True
    with pytest.raises(ValidationError):
        ExecutionValidationProbePolicy(mode="live")
    with pytest.raises(ValidationError):
        ExecutionValidationProbePolicy(symbols=("BTC/USD",))
    with pytest.raises(ValidationError):
        ExecutionValidationProbePolicy(
            authorization_cutoff=AUTHORIZATION_CUTOFF + timedelta(seconds=1)
        )


def test_probe_is_passive_idempotent_and_globally_single_outstanding(probe_setup) -> None:
    _, broker, engine, current = probe_setup
    now = current[0]
    broker.partial = Decimal(0)
    cohort = ExecutionValidationProbeCohort(engine, ExecutionValidationProbePolicy())
    first = cohort.step({"BTC/USD": quote(now)}, now=now)
    assert first["scheduled_client_order_id"] and first["scheduled_client_order_id"].startswith(
        "ta30p-"
    )
    assert len(first["scheduled_client_order_id"]) < 48
    assert len(broker.posts) == 1
    assert broker.posts[0].side.value == "buy"
    assert broker.posts[0].quantity * Decimal("100") <= Decimal("10")
    later = now + timedelta(minutes=16)
    current[0] = later
    ProductionRepository(engine.database).refresh_worker_lock(
        "tradeagent-event-worker", "owner", observed_at=later
    )
    blocked = cohort.step({"ETH/USD": quote(later, "ETH/USD")}, now=later)
    assert blocked["scheduled_client_order_id"] is None
    assert len(broker.posts) == 1


def test_resolved_probe_allows_a_later_idempotent_schedule_slot(probe_setup) -> None:
    _, broker, engine, current = probe_setup
    now = current[0]
    cohort = ExecutionValidationProbeCohort(engine, ExecutionValidationProbePolicy())
    first = cohort.step({"BTC/USD": quote(now)}, now=now)
    assert first["scheduled_client_order_id"] is not None
    later = now + timedelta(minutes=16)
    # The runtime's normal execution step is responsible for the owned exit.
    current[0] = later
    ProductionRepository(engine.database).refresh_worker_lock(
        "tradeagent-event-worker", "owner", observed_at=later
    )
    engine.step((), {"BTC/USD": quote(later)}, now=later)
    second = cohort.step({"BTC/USD": quote(later)}, now=later)
    assert second["scheduled_client_order_id"] is not None
    assert second["scheduled_client_order_id"] != first["scheduled_client_order_id"]
    assert len(broker.posts) == 3


def test_idle_probe_ticks_defer_broker_reconciliation(probe_setup, monkeypatch) -> None:
    _, _, engine, current = probe_setup
    now = current[0]
    cohort = ExecutionValidationProbeCohort(engine, ExecutionValidationProbePolicy())
    reconciled: list[datetime] = []
    monkeypatch.setattr(engine, "reconcile", lambda *, now: reconciled.append(now))

    first = cohort.step({}, now=now)
    for second in range(1, 11):
        current[0] = now + timedelta(seconds=second)
        result = cohort.step({}, now=current[0])
        assert result["broker_reconciliation"] == "deferred_until_schedule_boundary"

    assert first["broker_reconciliation"] == "performed"
    assert reconciled == [now]


def test_probe_scheduling_balances_available_symbols(probe_setup) -> None:
    _, broker, engine, current = probe_setup
    now = current[0]
    cohort = ExecutionValidationProbeCohort(engine, ExecutionValidationProbePolicy())

    first = cohort.step(
        {"BTC/USD": quote(now), "ETH/USD": quote(now, "ETH/USD")},
        now=now,
    )
    assert first["scheduled_client_order_id"] is not None
    assert broker.posts[0].symbol == "BTC/USD"

    later = now + timedelta(minutes=16)
    current[0] = later
    ProductionRepository(engine.database).refresh_worker_lock(
        "tradeagent-event-worker", "owner", observed_at=later
    )
    engine.step((), {"BTC/USD": quote(later)}, now=later)
    second = cohort.step(
        {"BTC/USD": quote(later), "ETH/USD": quote(later, "ETH/USD")},
        now=later,
    )

    assert second["scheduled_client_order_id"] is not None
    assert broker.posts[-1].symbol == "ETH/USD"


def test_historical_loader_excludes_probe_payloads_with_audit_reason(probe_setup) -> None:
    database, _, engine, current = probe_setup
    now = current[0]
    cohort = ExecutionValidationProbeCohort(engine, ExecutionValidationProbePolicy())
    assert cohort.step({"BTC/USD": quote(now)}, now=now)["scheduled_client_order_id"]

    loaded = load_historical_candidates(
        database,
        account_digest=engine.config.account_digest,
        start=now - timedelta(seconds=1),
        end=now + timedelta(seconds=1),
    )

    assert loaded.candidates == ()
    assert loaded.exclusions == {"NON_COMPARABLE_PAPER_EXPERIMENT": 1}


def test_probe_observation_is_typed_and_only_resolves_as_of_its_closure(probe_setup) -> None:
    database, _, engine, current = probe_setup
    now = current[0]
    policy = ExecutionValidationProbePolicy()
    cohort = ExecutionValidationProbeCohort(engine, policy)
    assert cohort.step({"BTC/USD": quote(now)}, now=now)["scheduled_client_order_id"]

    before, exclusions = probe_observations(
        database, engine.config.account_digest, policy, now=now
    )
    assert exclusions == {}
    assert len(before) == 1
    assert before[0].schema_version == "probe-observation-v2"
    assert before[0].evidence_environment == "broker_paper"
    assert before[0].resolved_at is None
    assert before[0].exclusion_reasons == ("RESOLUTION_NOT_AVAILABLE_AS_OF_CUTOFF",)
    assert held_out_probe_report(
        database, engine.config.account_digest, policy, now=now
    )["qualifying_labels"] == 0

    later = now + timedelta(seconds=16)
    current[0] = later
    ProductionRepository(database).refresh_worker_lock(
        "tradeagent-event-worker", "owner", observed_at=later
    )
    engine.step((), {"BTC/USD": quote(later)}, now=later)
    after, exclusions = probe_observations(
        database, engine.config.account_digest, policy, now=later
    )

    assert exclusions == {}
    assert after[0].resolved_at == later
    assert after[0].orders[0].client_order_id.startswith("ta30p-")
    assert held_out_probe_report(
        database, engine.config.account_digest, policy, now=later
    )["qualifying_labels"] == 1


def test_probe_observation_requires_exact_stored_policy_hash(probe_setup) -> None:
    database, _, engine, current = probe_setup
    now = current[0]
    stored = ExecutionValidationProbePolicy().model_copy(
        update={"max_order_notional_usd": Decimal("9")}
    )
    assert engine.submit_execution_validation_probe(
        policy=stored, quote=quote(now), now=now
    )

    observations, exclusions = probe_observations(
        database,
        engine.config.account_digest,
        ExecutionValidationProbePolicy(),
        now=now,
    )

    assert observations == ()
    assert exclusions == {"POLICY_HASH_MISMATCH": 1}


def test_deadline_blocks_new_probe_and_actual_label_report_stays_ineligible(probe_setup) -> None:
    _, broker, engine, current = probe_setup
    now = AUTHORIZATION_CUTOFF
    current[0] = now
    ProductionRepository(engine.database).refresh_worker_lock(
        "tradeagent-event-worker", "owner", observed_at=now
    )
    cohort = ExecutionValidationProbeCohort(engine, ExecutionValidationProbePolicy())
    result = cohort.step({"BTC/USD": quote(now)}, now=now)
    assert result["scheduled_client_order_id"] is None and not broker.posts
    report = held_out_probe_report(
        engine.database, engine.config.account_digest, cohort.policy, now=now
    )
    assert report["eligible"] is False
    assert report["reason"] == "INSUFFICIENT_ACTUAL_RESOLVED_PROBE_LABELS"
    assert report["profitability_claim"] is False
    assert report["cohort_and_cutoff_scoped"] is True
    assert report["cohort_policy_and_as_of_scoped"] is True


def test_probe_status_api_exposes_recorded_isolated_contract(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    url = f"sqlite:///{tmp_path / 'api.db'}"
    with Database(url) as database:
        database.initialize()
        repo = ProductionRepository(database)
        repo.acquire_worker_lock("tradeagent-event-worker", "owner", observed_at=now)
        repo.heartbeat(
            "tradeagent-event-worker",
            "owner",
            {
                "entry_policy": "v30-paper-unrestricted",
                "cohort_id": "v30",
                "code_sha": "a" * 40,
                "config_hash": "b" * 64,
            },
            observed_at=now,
        )
        snapshot = {
            "state": "running",
            "cohort_id": "v30",
            "owner_id": "owner",
            "code_sha": "a" * 40,
            "config_hash": "b" * 64,
            "execution_validation_probes": {
                "policy": ExecutionValidationProbePolicy().description(),
                "actual_broker_labels_only": True,
            },
        }
        with database.begin() as connection:
            connection.execute(
                insert(controls).values(
                    control_key="scalping:v30:status",
                    control_value=json.dumps(snapshot),
                    updated_at=now,
                )
            )
    app = create_app(
        ledger_path=tmp_path / "ledger.db",
        experiments_path=tmp_path / "experiments.db",
        production_database_url=url,
    )
    with TestClient(app) as client:
        payload = client.get("/api/scalping-probes").json()
    assert payload["actual_broker_labels_only"] is True
    assert payload["policy"]["strategy_profitability_or_promotion"] is False
