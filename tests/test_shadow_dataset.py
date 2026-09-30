import ast
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pytest
from pydantic import ValidationError
from sqlalchemy import func, select, update

from tradeagent.persistence import Database, ProductionRepository, orders
from tradeagent.scalping_market import datetime_ns, decode_crypto_message
from tradeagent.shadow_dataset import (
    WINDOW_END,
    WINDOW_START,
    ShadowDatasetCollector,
    ShadowDatasetProtocol,
    ShadowDatasetStore,
    dataset_status,
    freeze_protocol,
    persist_daily_quality,
    shadow_evaluations,
    shadow_source_links,
)
from tradeagent.shadow_dataset_analysis import analyze_dataset, verify_source_manifest
from tradeagent.shadow_dataset_runtime import recover_pending, shadow_daily_email

FREEZE = datetime(2026, 9, 30, 12, tzinfo=UTC)


def protocol(**changes):
    return ShadowDatasetProtocol(
        frozen_at=FREEZE, code_sha="a" * 40, account_digest="b" * 64, **changes
    )


def event(at, *, sequence=1, symbol="BTC/USD", bid="100", ask="100.01", book=False):
    payload = (
        {"T": "o", "b": [{"p": bid, "s": "10"}], "a": [{"p": ask, "s": "10"}], "r": False}
        if book
        else {"T": "q", "bp": bid, "ap": ask, "bs": "10", "as": "10"}
    )
    return decode_crypto_message(
        {**payload, "S": symbol, "t": at.isoformat()},
        connection_id=UUID(int=1),
        receive_sequence=sequence,
        received_at=at + timedelta(milliseconds=10),
        received_at_ns=datetime_ns(at + timedelta(milliseconds=10)),
        received_monotonic_ns=datetime_ns(at),
    )


@pytest.fixture
def store_setup(tmp_path):
    with Database(f"sqlite:///{tmp_path / 'dataset.db'}") as database:
        database.initialize()
        repo = ProductionRepository(database)
        repo.acquire_worker_lock("tradeagent-event-worker", "owner", observed_at=FREEZE)
        p = protocol()
        store = ShadowDatasetStore(database, p, "owner")
        repo.research_clock = [FREEZE]
        store.clock = lambda: repo.research_clock[0]
        store.freeze(FREEZE)
        yield database, repo, store, p


def refresh(repo, now):
    repo.research_clock[0] = now
    assert repo.refresh_worker_lock("tradeagent-event-worker", "owner", observed_at=now)


def test_dates_thresholds_and_order_permission_are_immutable():
    p = protocol()
    assert p.end - p.start == timedelta(days=14)
    assert p.orders_enabled is False and p.promotion_enabled is False
    with pytest.raises(ValidationError):
        protocol(orders_enabled=True)
    with pytest.raises(ValidationError):
        protocol(minimum_coverage=0.9)
    with pytest.raises(ValidationError):
        protocol(start=WINDOW_START + timedelta(days=1))
    with pytest.raises(ValidationError):
        protocol(actual_fee_tier="tier1", fee_tier_verified=True)


def test_cannot_backdate_new_protocol_or_rewrite_existing(store_setup):
    database, _, _, p = store_setup
    freeze_protocol(database, p, FREEZE)
    changed = p.model_copy(update={"additional_slippage_bps": Decimal("6")})
    with pytest.raises(ValueError, match="rewrite"):
        freeze_protocol(database, changed, FREEZE)


def test_no_supported_family_is_recorded_for_each_symbol_and_slot():
    c = ShadowDatasetCollector(protocol())
    e = event(WINDOW_START)
    c.on_market(e, e.received_at)
    rows = c.evaluate(WINDOW_START + timedelta(milliseconds=100))
    assert len(rows) == 2
    btc, eth = rows[0]["values"]["payload"], rows[1]["values"]["payload"]
    assert btc["category"] == "no_supported_family"
    assert "NO_SUPPORTED_FAMILY" in btc["rejection_reasons"]
    assert btc["market_snapshot"]["midpoint"] == "100.005"
    assert btc["outputs"]["economic_model_state"] == "no_support"
    assert eth["decision_observation"] is None
    assert eth["data_quality_flags"]["decision_quote_missing"] is True
    assert c.evaluate(WINDOW_START + timedelta(seconds=1)) == []
    assert len(c.evaluate(WINDOW_START + timedelta(seconds=10))) == 2


def test_entry_and_forward_labels_are_executable_not_midpoint():
    c = ShadowDatasetCollector(protocol())
    initial = event(WINDOW_START)
    c.on_market(initial, initial.received_at)
    at = WINDOW_START + timedelta(milliseconds=100)
    rows = c.evaluate(at)
    later = event(at + timedelta(seconds=4.9), sequence=2, bid="101", ask="101.01")
    c.on_market(later, later.received_at)
    labels = c.resolve(at + timedelta(seconds=7.1))
    label = next(
        r["values"]["payload"]
        for r in labels
        if r["values"]["evaluation_id"] == rows[0]["values"]["evaluation_id"]
    )
    expected = (Decimal("101") / Decimal("100.01") - 1) * 10000
    assert label["complete"] is True
    assert label["long_gross_bps"] == pytest.approx(float(expected))
    assert label["short_gross_bps"] < 0
    assert label["no_midpoint_primary_outcome"] is True
    assert label["short_execution_available"] is False
    assert label["long_net_bps"] < label["long_gross_bps"]


def test_future_quote_cannot_supply_an_earlier_label():
    c = ShadowDatasetCollector(protocol())
    first = event(WINDOW_START)
    c.on_market(first, first.received_at)
    at = WINDOW_START + timedelta(milliseconds=100)
    c.evaluate(at)
    future = event(at + timedelta(seconds=5.5), sequence=2, bid="110", ask="110.01")
    c.on_market(future, future.received_at)
    labels = c.resolve(at + timedelta(seconds=7.1))
    assert all(row["values"]["complete"] is False for row in labels)
    assert all(row["values"]["payload"]["long_gross_bps"] is None for row in labels)


def test_failed_l2_delta_does_not_erase_native_l1_or_refresh_its_timestamp():
    c = ShadowDatasetCollector(protocol())
    q = event(WINDOW_START)
    c.on_market(q, q.received_at)
    delta = event(WINDOW_START + timedelta(milliseconds=200), sequence=2, book=True)
    c.on_market(delta, delta.received_at)
    rows = c.evaluate(WINDOW_START + timedelta(milliseconds=300))
    quote = rows[0]["values"]["payload"]["decision_observation"]
    assert quote["event_id"] == q.event_id
    assert quote["received_at"] == q.received_at.isoformat()
    assert rows[0]["values"]["payload"]["depth"]["book_current"] is False


def test_processing_availability_is_checked_for_decisions():
    c = ShadowDatasetCollector(protocol())
    q = event(WINDOW_START)
    c.on_market(q, WINDOW_START + timedelta(seconds=1))
    rows = c.evaluate(WINDOW_START + timedelta(milliseconds=100))
    assert rows[0]["values"]["payload"]["decision_observation"] is None


def test_duplicates_and_timestamp_reversals_are_exposed():
    c = ShadowDatasetCollector(protocol())
    q = event(WINDOW_START)
    c.on_market(q, q.received_at)
    c.on_market(q, q.received_at)
    bad = event(WINDOW_START - timedelta(seconds=1), sequence=2)
    c.on_market(bad, q.received_at)
    assert c.quality()["counts"]["duplicate_events"] == 1
    assert c.quality()["counts"]["timestamp_reversals"] == 1


def test_raw_manifest_hash_chain_and_immutable_evaluations(store_setup):
    database, repo, store, _ = store_setup
    q = event(WINDOW_START)
    refresh(repo, q.received_at)
    store.persist_tape((q,), q.received_at)
    store.persist_tape((q,), q.received_at)
    proof = verify_source_manifest(database)
    assert proof["verified"] and proof["batches"] == 1
    c = ShadowDatasetCollector(protocol())
    c.on_market(q, q.received_at)
    at = WINDOW_START + timedelta(milliseconds=100)
    rows = c.evaluate(at)
    store.write(rows, at)
    store.write(rows, at)
    with database.begin() as connection:
        assert connection.scalar(select(func.count()).select_from(shadow_evaluations)) == 2
        assert connection.scalar(select(func.count()).select_from(orders)) == 0
    changed = {
        "kind": "evaluation",
        "values": {
            **rows[0]["values"],
            "payload": {**rows[0]["values"]["payload"], "rejection_reasons": []},
        },
    }
    with pytest.raises(ValueError, match="conflicting"):
        store.write([changed], at)


def test_owner_loss_fences_all_dataset_writes(store_setup):
    _, repo, store, _ = store_setup
    repo.release_worker_lock("tradeagent-event-worker", "owner")
    with pytest.raises(RuntimeError, match="lease"):
        store.persist_tape((event(WINDOW_START),), WINDOW_START)


def test_queued_observation_time_does_not_falsely_lose_renewed_lease(store_setup):
    database, repo, store, _ = store_setup
    collector = ShadowDatasetCollector(protocol())
    newer = FREEZE + timedelta(milliseconds=10)
    refresh(repo, newer)
    store.update_quality(collector.quality(), FREEZE)
    assert dataset_status(database)["state"] == "warming_up"


def test_prestart_repair_is_append_only_and_never_poststart(store_setup):
    database, _, _, p = store_setup
    from tradeagent.shadow_dataset import approve_prestart_release, release_allowed

    assert not release_allowed(database, p, "c" * 40)
    approve_prestart_release(database, "c" * 40, now=FREEZE, reason="Lease race safety repair")
    assert release_allowed(database, p, "c" * 40)
    assert dataset_status(database)["protocol_hash"] == p.identity
    with pytest.raises(ValueError, match="pre-start"):
        approve_prestart_release(database, "d" * 40, now=WINDOW_START, reason="Too late")


def test_unfinished_labels_survive_restart_without_repeating_evaluation(store_setup):
    database, repo, store, p = store_setup
    c = ShadowDatasetCollector(p)
    q = event(WINDOW_START)
    c.on_market(q, q.received_at)
    at = WINDOW_START + timedelta(milliseconds=100)
    refresh(repo, at)
    rows = c.evaluate(at)
    store.write(rows, at)
    resumed = ShadowDatasetCollector(p)
    recover_pending(database, resumed)
    assert len(resumed.pending) == 2
    assert resumed.evaluate(at + timedelta(seconds=1)) == []
    missing = resumed.resolve(at + timedelta(seconds=902.1))
    assert len(missing) == 10
    assert all(row["values"]["complete"] is False for row in missing)


def test_quality_gate_blocks_finance_and_preserves_unknown_fee_tier(store_setup, tmp_path):
    database, _, _, p = store_setup
    status = dataset_status(database)
    assert status["profitability_analysis_allowed"] is False
    assert status["actual_fee_tier_verified"] is False
    report = analyze_dataset(database, output_dir=tmp_path / "forbidden-analysis")
    assert report["profitability_analysis_performed"] is False
    assert not (tmp_path / "forbidden-analysis").exists()
    assert p.actual_fee_tier is None


def test_coverage_gate_counts_missing_grid_slots_not_only_resolved_rows(store_setup):
    database, repo, store, _ = store_setup
    now = WINDOW_END + timedelta(minutes=16)
    refresh(repo, now)
    c = ShadowDatasetCollector(protocol())
    store.update_quality(c.quality(), now, sealed=True)
    status = dataset_status(database)
    assert status["expected_evaluations_per_symbol"] == 120960
    assert status["missing_evaluation_slots"]["BTC/USD"] == 120960
    assert status["profitability_analysis_allowed"] is False


def test_quality_errors_survive_new_process_session(store_setup):
    database, repo, store, _ = store_setup
    c = ShadowDatasetCollector(protocol())
    c.counts["timestamp_reversals"] = 1
    store.update_quality(c.quality(), FREEZE)
    second = ShadowDatasetCollector(protocol())
    refresh(repo, FREEZE)
    store.update_quality(second.quality(), FREEZE)
    assert dataset_status(database)["quality"]["counts"]["timestamp_reversals"] == 1


def test_daily_report_is_archived_without_orders_or_duplicate_recipients(store_setup):
    database, _, _, _ = store_setup
    report = persist_daily_quality(database, FREEZE.date(), now=FREEZE)
    assert report["expected_evaluations_per_symbol"] == 0
    mail = shadow_daily_email(database, FREEZE, "America/New_York")
    assert len(mail["text"].split("\n\n")) == 5
    assert mail["orders_submitted"] == 0
    assert "No orders" in mail["text"]


def test_no_order_client_or_execution_engine_in_observation_service():
    import tradeagent.shadow_dataset_runtime as runtime

    assert not hasattr(runtime, "AlpacaPaperClient")
    assert not hasattr(runtime, "ScalpOrderEngine")
    tree = ast.parse(Path(runtime.__file__).read_text(encoding="utf-8"))
    calls = [
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    ]
    assert not any(name.startswith("submit_") for name in calls)


def test_source_manifest_corruption_is_not_accepted(store_setup):
    database, repo, store, _ = store_setup
    q = event(WINDOW_START)
    refresh(repo, q.received_at)
    store.persist_tape((q,), q.received_at)
    with database.begin() as connection:
        connection.execute(update(shadow_source_links).values(root="0" * 64))
    with pytest.raises(ValueError, match="chain"):
        verify_source_manifest(database)


def test_short_benchmark_missing_size_is_not_assumed_executable():
    c = ShadowDatasetCollector(protocol())
    initial = event(WINDOW_START)
    initial = initial.model_copy(
        update={
            "bids": tuple(
                level.model_copy(update={"quantity": Decimal("0.0001")}) for level in initial.bids
            )
        }
    )
    c.on_market(initial, initial.received_at)
    at = WINDOW_START + timedelta(milliseconds=100)
    c.evaluate(at)
    exit_event = event(at + timedelta(seconds=4.9), sequence=2, bid="101", ask="101.01")
    c.on_market(exit_event, exit_event.received_at)
    label = c.resolve(at + timedelta(seconds=7.1))[0]["values"]["payload"]
    assert label["complete"] is True
    assert label["long_gross_bps"] is not None
    assert label["short_gross_bps"] is None


def test_missing_raw_quote_reference_blocks_manifest_proof(store_setup):
    database, repo, store, _ = store_setup
    c = ShadowDatasetCollector(protocol())
    q = event(WINDOW_START)
    c.on_market(q, q.received_at)
    at = WINDOW_START + timedelta(milliseconds=100)
    refresh(repo, at)
    store.write(c.evaluate(at), at)
    with pytest.raises(ValueError, match="references missing"):
        verify_source_manifest(database)


def test_quality_gated_api_and_observer_status_remain_trade_free(store_setup, tmp_path):
    from fastapi.testclient import TestClient

    from tradeagent.api import create_app
    from tradeagent.scalping_reporting import scalping_status

    database, repo, _, p = store_setup
    snapshot = {
        "state": "warming_up",
        "entry_policy": "shadow-research-dataset-v1",
        "cohort_id": p.dataset_id,
        "code_sha": p.code_sha,
        "config_hash": p.identity,
        "owner_id": "owner",
        "account_digest": p.account_digest,
        "orders_submitted": 0,
    }
    repo.heartbeat("tradeagent-event-worker", "owner", snapshot, observed_at=FREEZE)
    import json

    repo.set_control(f"shadow-dataset:{p.dataset_id}:status", json.dumps(snapshot))
    state = scalping_status(database, now=FREEZE)
    assert state["economics"]["model_status"] == "no_support"
    assert state["execution"]["broker_positions_status"] == "not_queried_by_observation_collector"
    assert state["mode"] == "observation_only"
    app = create_app(
        ledger_path=tmp_path / "local.db",
        experiments_path=tmp_path / "exp.db",
        production_database_url=str(database.engine.url),
    )
    with TestClient(app) as client:
        result = client.get("/api/shadow-dataset")
        assert result.status_code == 200
        assert result.json()["profitability_analysis_allowed"] is False
        assert result.json()["orders_submitted"] == 0
        assert client.get("/api/shadow-dataset/analysis").json()["state"] == (
            "waiting_for_sealed_dataset"
        )
        assert 'id="shadow-dataset-heading"' in client.get("/").text


def test_terminal_child_archives_quality_block_without_orders(store_setup, monkeypatch):
    import sys
    import types

    from tradeagent.persistence import events
    from tradeagent.shadow_dataset_runtime import run_shadow_dataset

    database, _, _, _ = store_setup
    monkeypatch.setenv("TRADEAGENT_DATABASE_URL", str(database.engine.url))
    fake_resource = types.ModuleType("resource")
    fake_resource.RLIMIT_AS = 0
    fake_resource.setrlimit = lambda *args: None
    monkeypatch.setitem(sys.modules, "resource", fake_resource)
    child = next(
        value
        for value in run_shadow_dataset.__code__.co_consts
        if isinstance(value, types.CodeType) and value.co_name == "terminal_analysis"
    )
    script = next(
        value
        for value in child.co_consts
        if isinstance(value, str) and value.startswith("import json,resource;")
    )
    exec(compile(script, "<trusted-terminal-analysis-test>", "exec"), {})
    with database.begin() as connection:
        payload = connection.scalar(
            select(events.c.payload).where(events.c.event_type == "shadow_dataset_analysis_result")
        )
        assert payload["state"] == "blocked_on_data_quality"
        assert payload["profitability_analysis_performed"] is False
        assert connection.scalar(select(func.count()).select_from(orders)) == 0
