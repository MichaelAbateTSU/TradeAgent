from datetime import timedelta
from hashlib import sha256

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import func, select
from test_shadow_dataset import FREEZE, protocol, refresh
from test_shadow_dataset import store_setup as dataset_setup

from tradeagent.alpaca_paper import AlpacaPaperSettings
from tradeagent.persistence import events
from tradeagent.shadow_dataset import (
    WINDOW_START,
    ShadowDatasetCollector,
    dataset_status,
)
from tradeagent.shadow_dataset_monitor import (
    SCHEDULE,
    STOP_KEY,
    AcceptanceScheduler,
    PaperReadOnlyMonitor,
    _decision,
    checkpoint_id,
    signal_stop,
)

store_setup = dataset_setup


class ReadOnlyFixture:
    def snapshot(self, p):
        return {
            "positions": 0,
            "open_orders": 0,
            "broker_order_records_since_freeze": 0,
            "account_digest": p.account_digest,
            "paper_host": p.strategy_config().mode,
        }

    def fee_evidence(self, p):
        return {
            "verified": False,
            "actual_crypto_fee_tier": None,
            "frozen_assumptions_changed": False,
            "account_digest": p.account_digest,
        }


def migration_fixture(database):
    with database.begin() as connection:
        connection.exec_driver_sql("CREATE TABLE alembic_version (version_num VARCHAR(32))")
        connection.exec_driver_sql(
            "INSERT INTO alembic_version VALUES ('0015_shadow_research_dataset')"
        )


def heartbeat(repo, p, at):
    repo.heartbeat(
        "tradeagent-event-worker",
        "owner",
        {
            "entry_policy": "shadow-research-dataset-v1",
            "code_sha": p.code_sha,
            "trading_authorization": "expired",
            "model_state": "no_support",
            "orders_submitted": 0,
            "feed": {"state": "streaming", "subscribed": True},
        },
        observed_at=at,
    )


def test_mature_coverage_includes_overdue_unwritten_but_excludes_young(store_setup):
    database, repo, store, p = store_setup
    c = ShadowDatasetCollector(p)
    rows = c.evaluate(WINDOW_START + timedelta(milliseconds=100))
    rows += c.evaluate(WINDOW_START + timedelta(seconds=10.1))
    rows += c.evaluate(WINDOW_START + timedelta(seconds=69.1))
    now = WINDOW_START + timedelta(seconds=73)
    refresh(repo, now)
    store.write(rows, now)
    label = {
        "kind": "label",
        "values": {
            "evaluation_id": rows[0]["values"]["evaluation_id"],
            "horizon_seconds": 60,
            "deadline_at": WINDOW_START + timedelta(seconds=60.1),
            "resolved_at": WINDOW_START + timedelta(seconds=62.2),
            "complete": True,
            "payload": {"missing_reasons": []},
        },
    }
    store.write([label], now)
    report = dataset_status(database, now=now, detailed=True)
    btc = next(
        row
        for row in report["coverage"]
        if row["symbol"] == "BTC/USD" and row["horizon_seconds"] == 60
    )
    assert btc["eligible_evaluations"] == 3
    assert btc["matured_evaluations"] == 2
    assert btc["complete"] == 1
    assert btc["not_yet_mature"] == 1
    assert btc["overdue_unwritten_labels"] == 1
    assert btc["coverage"] == 0.5
    assert btc["resolved_only_coverage"] == 1


def test_checkpoints_are_immutable_idempotent_and_not_backdated(store_setup):
    database, repo, _, p = store_setup
    migration_fixture(database)
    heartbeat(repo, p, FREEZE)
    monitor = AcceptanceScheduler(database, p, ReadOnlyFixture(), sleeper=lambda _: None)
    assert monitor.run_due(FREEZE) == ["readiness_initial", "fee_verification"]
    assert monitor.run_due(FREEZE) == []
    before = SCHEDULE["readiness_final"] - timedelta(seconds=1)
    assert monitor.run_due(before) == []
    actual = SCHEDULE["readiness_final"] + timedelta(seconds=2)
    assert monitor.run_due(actual) == ["readiness_final"]
    with database.begin() as connection:
        row = (
            connection.execute(
                select(events).where(
                    events.c.event_id == checkpoint_id("readiness_final", p.identity)
                )
            )
            .mappings()
            .one()
        )
        assert row["payload"]["snapshot_at"] == actual.isoformat()
        assert row["payload"]["protocol_hash"] == p.identity
        assert row["payload"]["protocol_modified"] is False
        assert row["payload"]["broker_state"]["positions"] == 0


def test_late_restart_records_missed_smoke_without_fabricating_past_success(store_setup):
    database, repo, _, p = store_setup
    migration_fixture(database)
    heartbeat(repo, p, FREEZE)
    monitor = AcceptanceScheduler(database, p, ReadOnlyFixture(), sleeper=lambda _: None)
    captured = monitor.run_due(WINDOW_START + timedelta(minutes=40))
    assert "startup_smoke" in captured
    with database.begin() as connection:
        payload = connection.scalar(
            select(events.c.payload).where(
                events.c.event_id == checkpoint_id("startup_smoke", p.identity)
            )
        )
    assert payload["state"] == "missed_bounded_checkpoint"
    assert payload["future_checks_not_backdated"] is True


def test_read_only_paper_transport_never_submits_orders_or_changes_fee_assumptions():
    calls = []

    def handler(request):
        calls.append((request.method, request.url.host, request.url.path))
        if request.url.path == "/v2/account":
            return httpx.Response(200, json={"id": "fixture-paper", "status": "ACTIVE"})
        if request.url.path == "/v2/account/configurations":
            return httpx.Response(200, json={"dtbp_check": "both"})
        return httpx.Response(200, json=[])

    p = protocol().model_copy(update={"account_digest": sha256(b"fixture-paper").hexdigest()})
    settings = AlpacaPaperSettings(
        key_id=SecretStr("fixture-key"), secret_key=SecretStr("fixture-secret")
    )
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        reader = PaperReadOnlyMonitor(settings, client)
        assert reader.snapshot(p)["positions"] == 0
        fees = reader.fee_evidence(p)
        assert fees["verified"] is False
        assert fees["actual_crypto_fee_tier"] is None
        assert fees["frozen_assumptions_changed"] is False
        with pytest.raises(ValueError, match="allowlisted"):
            reader._get("/v2/orders/cancel")
    assert all(method == "GET" and host == "paper-api.alpaca.markets" for method, host, _ in calls)


def test_btc_cannot_mask_eth_and_real_missingness_is_not_synthesized():
    rows = [
        {
            "symbol": s,
            "horizon_seconds": 60,
            "matured_evaluations": 100,
            "overdue_unwritten_beyond_10s_monitor_grace": 0,
            "coverage": value,
            "missing_reasons": {"HORIZON_QUOTE_STALE_OR_MISSING": 10},
        }
        for s, value in (("BTC/USD", 1.0), ("ETH/USD", 0.90))
    ]
    report = {
        "coverage": rows,
        "missing_evaluation_slots": {"BTC/USD": 0, "ETH/USD": 0},
        "quality": {"counts": {}},
    }
    safety = {
        "local_order_attempts_since_freeze": 0,
        "reported_order_attempts": 0,
        "trading_authorization_renewed": False,
        "feed_subscribed": True,
    }
    broker = {"positions": 0, "open_orders": 0, "broker_order_records_since_freeze": 0}
    assert _decision(report, safety, broker) == "retain_legitimate_missingness_no_synthesis"
    safety["local_order_attempts_since_freeze"] = 1
    assert _decision(report, safety, broker) == "stop_on_order_exposure_or_authorization"


def test_safety_stop_is_persistent_and_does_not_rearm_or_edit_protocol(store_setup):
    database, repo, _, _ = store_setup
    signal_stop(database, "UNEXPECTED_ORDER", FREEZE)
    first = repo.get_control(STOP_KEY)
    signal_stop(database, "ANOTHER_REASON", FREEZE + timedelta(seconds=1))
    assert repo.get_control(STOP_KEY) == first
    with database.begin() as connection:
        assert (
            connection.scalar(
                select(func.count()).where(events.c.event_type == "shadow_dataset_safety_stop")
            )
            == 1
        )
