from __future__ import annotations

import copy
import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import pytest
from pydantic import ValidationError
from sqlalchemy import select, update
from test_iex_practice import Clock, tick
from test_iex_practice import make_runtime as make_runtime
from test_operator_calibration import ACCOUNT, complete_history

from tradeagent import event_runtime, operator_calibration, scheduled_paper
from tradeagent.event_session import session_control_key
from tradeagent.operator_calibration import (
    OperatorPaperRequest,
    control_versions,
    final_entry_fence,
)
from tradeagent.paper_account_history import history_identity, save_baseline
from tradeagent.persistence import controls, notification_outbox, worker_locks
from tradeagent.scheduled_paper import (
    ScheduledPaperSession,
    publish_authority,
    validate_history_extension,
)

APPROVED = datetime(2026, 9, 10, 22, 0, tzinfo=UTC)
OPEN = datetime(2026, 9, 11, 13, 30, tzinfo=UTC)
ENTRY = datetime(2026, 9, 11, 14, 5, tzinfo=UTC)
DEADLINE = datetime(2026, 9, 11, 14, 30, tzinfo=UTC)


def advance(runtime, at):
    Clock.current = runtime.broker.now = at
    runtime.repo.refresh_worker_lock("tradeagent-event-worker", runtime.instance_id, observed_at=at)


@pytest.fixture
def prepared(make_runtime, monkeypatch):
    original_manifest = event_runtime.cohort_manifest
    monkeypatch.setattr(
        event_runtime,
        "cohort_manifest",
        lambda settings, code: original_manifest(settings, "a" * 40),
    )
    monkeypatch.setattr(operator_calibration, "datetime", Clock)
    monkeypatch.setattr(scheduled_paper, "datetime", Clock)
    monkeypatch.setattr("tradeagent.event_orders.datetime", Clock)
    runtime = make_runtime(
        confirmed=False,
        entry_policy="scheduled-operator",
        news_account_digest=ACCOUNT,
        practice_start_date=ENTRY.date(),
    )
    runtime.code_sha = "a" * 40
    advance(runtime, APPROVED)
    runtime.repo.set_control("kill_switch", "active")
    runtime.repo.set_control(f"{runtime.settings.cohort_id}:pause", "REVIEWED_EXACT_HOST_METADATA")
    snapshot = complete_history()
    snapshot["observed_at"] = APPROVED.isoformat()
    session_id, nonce = uuid4(), uuid4()
    from uuid import uuid5

    session = ScheduledPaperSession(
        session_id=session_id,
        nonce=nonce,
        worker_cohort_id=runtime.settings.cohort_id,
        worker_config_hash=runtime.config_hash,
        code_sha=runtime.code_sha,
        account_digest=ACCOUNT,
        session_date=ENTRY.date(),
        approved_at=APPROVED,
        not_before=ENTRY,
        entry_deadline=DEADLINE,
        owner_authority=(
            "Offline fixture explicit authorization tonight for tomorrow only, "
            "without another chat."
        ),
        reviewed_history=snapshot,
        reviewed_history_sha256=history_identity(snapshot),
        nightly_acceptance_sha256="f" * 64,
        nightly_acceptance_at=APPROVED - timedelta(minutes=1),
        checks=dict.fromkeys(
            (
                "reviewed_release",
                "reviewed_exact_host_pause",
                "nightly_resource_acceptance",
                "owner_tomorrow_authorization",
                "complete_history_review",
            ),
            True,
        ),
        control_versions=control_versions(
            runtime.repo,
            runtime.settings.cohort_id,
            "operator-paper-" + uuid5(session_id, str(nonce)).hex,
        ),
    )
    runtime.repo.heartbeat(
        "tradeagent-event-worker",
        runtime.instance_id,
        {
            "cohort_id": runtime.settings.cohort_id,
            "config_hash": runtime.config_hash,
            "code_sha": runtime.code_sha,
            "mode": "experimental-paper",
            "entry_policy": "scheduled-operator",
        },
        observed_at=APPROVED,
    )
    publish_authority(runtime.store, session, now=APPROVED)
    runtime.broker.account_history = lambda: {
        **copy.deepcopy(snapshot),
        "observed_at": Clock.current.isoformat(),
    }
    runtime._ensure_session_plan(APPROVED)
    runtime.order_stream = SimpleNamespace(
        drain=lambda: (),
        health_snapshot=lambda: dict(
            running=True,
            connected=True,
            authenticated=True,
            subscribed=True,
            gap_started_at=None,
            dropped_updates=0,
            invalid_messages=0,
        ),
    )
    monkeypatch.setattr(scheduled_paper, "process_rss_mib", lambda: 100)
    return runtime, session


def readiness_sample(runtime, session, now, since, *, include_physical=True):
    exchange = now - timedelta(seconds=1)
    count = max(1, int((now - since).total_seconds()))
    return {
        "observed_at": now.isoformat(),
        "checks": {"fixture_actual_readiness": True},
        "market_count_scope": {
            "since_exchange_at": since.isoformat(),
            "until_exchange_at": now.isoformat(),
            "symbols": list(scheduled_paper.RECORDER_SYMBOLS),
        },
        "market": {
            table: [
                {
                    "symbol": symbol,
                    "count": count,
                    "latest_exchange_at": exchange.isoformat(),
                    "latest_received_at": exchange.isoformat(),
                    "latest_committed_at": now.isoformat(),
                }
                for symbol in scheduled_paper.RECORDER_SYMBOLS
            ]
            for table in ("market_quotes", "market_trades", "market_bars")
        },
    }


def observe(runtime, session, monkeypatch, at):
    advance(runtime, at)
    runtime.context = runtime.context_client.poll(symbols=["AAPL"])
    runtime.last_source_success = at
    runtime.market_states["AAPL"] = runtime.market.state("AAPL", at)
    runtime.first_bar_receipts[("AAPL", runtime.market_states["AAPL"].completed_bar.timestamp)] = (
        at - timedelta(seconds=3)
    )
    monkeypatch.setattr(scheduled_paper, "collect_readiness", readiness_sample)
    return scheduled_paper.step(runtime, now=at)


def warmed(prepared, monkeypatch):
    runtime, session = prepared
    for i in range(61):
        result = observe(
            runtime, session, monkeypatch, ENTRY - timedelta(minutes=30) + timedelta(seconds=30 * i)
        )
    return runtime, session, result


def test_night_approval_warmup_one_fill_exit_nextday_no_repeat(prepared, monkeypatch):
    runtime, session = prepared
    pinned = control_versions(runtime.repo, session.worker_cohort_id, session.operator_cohort_id)
    assert scheduled_paper.step(runtime, now=APPROVED)["state"] == "scheduled_preopen"
    assert (
        scheduled_paper.step(runtime, now=OPEN - timedelta(seconds=1))["state"]
        == "scheduled_preopen"
    )
    assert runtime.broker.submissions == 0
    runtime, session, result = warmed(prepared, monkeypatch)
    assert result["state"] == "delegated", result
    request = OperatorPaperRequest.model_validate_json(
        runtime.repo.get_control(f"{session.key}:delegation")
    )
    assert request.approved_at == APPROVED
    assert request.issued_at == ENTRY and request.not_before == ENTRY
    assert request.scheduled_session_sha256 == session.identity
    budget_key = session_control_key(ACCOUNT, ENTRY.date())
    budget = json.loads(runtime.repo.get_control(budget_key))
    assert budget["total_entries_reserved"] == 1 and budget["scheduled_entry_reserved"]
    # Run the existing real OMS state machine against the in-memory paper broker.
    first = operator_calibration.step(runtime, observed_at=ENTRY)
    assert first["state"] == "filled", first
    assert runtime.broker.submissions == 1
    rows = runtime.store.linked_orders(request.cohort_id)
    assert rows[0]["quantity"] * Decimal(rows[0]["link"]["limit_price"]) <= 25
    assert rows[0]["link"]["protection"]["broker_native"] is False
    budget = json.loads(runtime.repo.get_control(budget_key))
    assert budget["total_entries_reserved"] == 1 and not budget["scheduled_entry_reserved"]
    advance(runtime, ENTRY + timedelta(seconds=61))
    result = operator_calibration.step(runtime, observed_at=Clock.current)
    assert result["state"] == "completed_flat", result
    assert runtime.broker.submissions == 2
    assert not runtime.broker.positions() and not runtime.broker.open_orders()
    advance(runtime, ENTRY + timedelta(days=1))
    finished = scheduled_paper.step(runtime, now=Clock.current)
    assert finished["state"] == "operator_finished"
    assert finished["result"]["state"] == "completed_flat"
    assert runtime.broker.submissions == 2
    assert (
        control_versions(runtime.repo, session.worker_cohort_id, session.operator_cohort_id)
        == pinned
    )
    old_budget = json.loads(
        runtime.repo.get_control(session_control_key(ACCOUNT, date(2026, 9, 9)))
    )
    assert old_budget["total_entries_reserved"] == 2
    assert runtime.repo.get_control(f"{session.key}:readiness-certificate")


def test_restart_reads_durable_warmup_without_reset_or_duplicate(prepared, monkeypatch):
    runtime, session = prepared
    for i in range(31):
        observe(
            runtime, session, monkeypatch, ENTRY - timedelta(minutes=30) + timedelta(seconds=30 * i)
        )
    # A new Python runtime object, same durable DB and lease; no in-memory readiness state.
    restarted = copy.copy(runtime)
    for i in range(31, 61):
        result = observe(
            restarted,
            session,
            monkeypatch,
            ENTRY - timedelta(minutes=30) + timedelta(seconds=30 * i),
        )
    assert result["state"] == "delegated"
    before = runtime.repo.get_control(session_control_key(ACCOUNT, ENTRY.date()))
    assert scheduled_paper.step(restarted, now=ENTRY)["state"] == "delegated"
    assert runtime.repo.get_control(session_control_key(ACCOUNT, ENTRY.date())) == before


@pytest.mark.parametrize("change", ["kill", "r1", "revoke", "terminal", "account"])
def test_control_changes_invalidate_authority_without_clearing_any_pause(prepared, change):
    runtime, session = prepared
    if change in {"kill", "r1"}:
        key = "kill_switch" if change == "kill" else f"{session.worker_cohort_id}:pause"
        with runtime.store.database.begin() as connection:
            connection.execute(
                update(controls)
                .where(controls.c.control_key == key)
                .values(updated_at=APPROVED + timedelta(seconds=1))
            )
    elif change == "revoke":
        runtime.repo.set_control(f"{session.key}:revoked", "owner emergency stop")
    elif change == "terminal":
        runtime.repo.set_control(f"{session.operator_cohort_id}:operator-terminal", "{}")
    else:
        runtime.settings = runtime.settings.model_copy(update={"news_account_digest": "b" * 64})
    result = scheduled_paper.step(runtime, now=OPEN)
    assert result["state"] == "MISSED"
    assert runtime.repo.get_control("kill_switch") == "active"
    assert runtime.broker.submissions == 0


def test_authority_record_mutation_and_publication_timestamp_change_fail_closed(prepared):
    runtime, session = prepared
    with runtime.store.database.begin() as connection:
        connection.execute(
            update(controls)
            .where(controls.c.control_key == session.key)
            .values(updated_at=APPROVED + timedelta(seconds=1))
        )
    with pytest.raises(ValueError, match="publication version"):
        scheduled_paper.load_session(runtime.repo, session.worker_cohort_id)


def test_deadline_and_notifications_are_idempotent(prepared):
    runtime, _session = prepared
    for _ in range(3):
        assert scheduled_paper.step(runtime, now=APPROVED)["state"] == "scheduled_preopen"
    for _ in range(3):
        assert scheduled_paper.step(runtime, now=DEADLINE)["state"] == "MISSED"
    with runtime.store.database.begin() as connection:
        notices = list(connection.execute(select(notification_outbox)).mappings())
    assert len(notices) == 2
    assert runtime.broker.submissions == 0
    assert runtime.repo.get_control(session_control_key(ACCOUNT, ENTRY.date())) is None


def test_interrupted_or_failed_readiness_does_not_invent_continuity(prepared, monkeypatch):
    runtime, session = prepared
    observe(runtime, session, monkeypatch, OPEN)
    result = observe(runtime, session, monkeypatch, ENTRY)
    assert result["state"] == "warming_up" and result["continuous_since"] == ENTRY.isoformat()
    assert runtime.repo.get_control(f"{session.key}:delegation") is None


@pytest.mark.parametrize(
    "corruption", ["mutated", "deleted", "future", "partial", "credit", "wrong_account", "stale"]
)
def test_history_extension_rejects_unreviewed_mutation_future_partial_or_funding(
    prepared, corruption
):
    _, session = prepared
    current = copy.deepcopy(session.reviewed_history)
    current["observed_at"] = ENTRY.isoformat()
    if corruption == "mutated":
        current["orders"][0]["status"] = "rejected"
    elif corruption == "deleted":
        current["activities"].pop()
    elif corruption == "future":
        current["orders"].append(
            {
                **current["orders"][0],
                "id": "future",
                "client_order_id": "future",
                "updated_at": (ENTRY + timedelta(days=1)).isoformat(),
            }
        )
    elif corruption == "partial":
        current["complete"] = False
    elif corruption == "credit":
        current["activities"].append({"id": "funding2", "activity_type": "JNLC"})
    elif corruption == "wrong_account":
        current["account_digest"] = "0" * 64
    else:
        current["observed_at"] = APPROVED.isoformat()
    with pytest.raises(ValueError):
        validate_history_extension(session, current, ENTRY)


def test_history_extension_accepts_unchanged_known_flat_history_and_preserves_accounting(prepared):
    runtime, session = prepared
    current = {**copy.deepcopy(session.reviewed_history), "observed_at": ENTRY.isoformat()}
    assert Decimal(validate_history_extension(session, current, ENTRY)["economic_pnl"]) < 0
    first = save_baseline(runtime.store, current, ENTRY)
    second = save_baseline(runtime.store, current, ENTRY + timedelta(seconds=1))
    assert first["identity"] == second["identity"]


@pytest.mark.parametrize("field,value", [("mode", "shadow"), ("symbols", "MSFT")])
def test_scheduled_host_cannot_be_shadow_or_other_symbol(prepared, field, value):
    runtime, _ = prepared
    from tradeagent.experimental_policy import ExperimentalSettings

    with pytest.raises(ValidationError):
        ExperimentalSettings(**{**runtime.settings.model_dump(), field: value}, _env_file=None)


def test_final_fence_rechecks_revocation_schedule_and_current_global_lease(prepared, monkeypatch):
    runtime, session, result = warmed(prepared, monkeypatch)
    assert result["state"] == "delegated"
    request = OperatorPaperRequest.model_validate_json(
        runtime.repo.get_control(f"{session.key}:delegation")
    )
    assert final_entry_fence(runtime.repo, request, runtime.instance_id, ENTRY)
    with runtime.store.database.begin() as connection:
        connection.execute(update(worker_locks).values(owner_id="replacement"))
    assert not final_entry_fence(runtime.repo, request, runtime.instance_id, ENTRY)
    with runtime.store.database.begin() as connection:
        connection.execute(update(worker_locks).values(owner_id=runtime.instance_id))
    runtime.repo.set_control(f"{session.key}:revoked", "stop")
    assert not final_entry_fence(runtime.repo, request, runtime.instance_id, ENTRY)


def test_runtime_collects_fresh_context_before_scheduled_request_consumption(prepared, monkeypatch):
    runtime, _session, result = warmed(prepared, monkeypatch)
    assert result["state"] == "delegated"
    runtime.context = None
    result = tick(runtime, ENTRY + timedelta(seconds=3))
    assert result["mode"] == "experimental-paper"
    assert result["ordinary_entries_enabled"] is False
    assert result["operator_paper"]["state"] == "filled", result
    assert runtime.broker.submissions == 1
    assert result["calibration"] is None


def seed_durable_recorder(runtime, now, *, symbols_by_table=None, bar_lag_seconds=0):
    from tradeagent.persistence import market_bars, market_quotes, market_trades

    event_at = now - timedelta(seconds=2)
    received_at = now - timedelta(seconds=1)
    processed_at = now - timedelta(milliseconds=500)
    tables = (market_bars, market_quotes, market_trades)
    selected = {
        table.name: (symbols_by_table or {}).get(table.name, scheduled_paper.RECORDER_SYMBOLS)
        for table in tables
    }
    inserted = sum(len(symbols) for symbols in selected.values())
    runtime.repo.acquire_worker_lock("tradeagent-shadow-recorder", "recorder", observed_at=now)
    runtime.repo.refresh_worker_lock("tradeagent-shadow-recorder", "recorder", observed_at=now)
    runtime.repo.heartbeat(
        "tradeagent-shadow-recorder",
        "recorder",
        {
            "state": "healthy",
            "healthy": True,
            "persistence_error": None,
            "dropped_events": 0,
            "notice_overflow": 0,
            "decision_errors": 0,
            "last_market_commit_at": processed_at.isoformat(),
        },
        observed_at=now,
    )
    runtime.repo.append_event(
        "shadow_recorder_batch",
        {
            "instance_id": "recorder",
            "received": inserted,
            "inserted": inserted,
            "duplicates": 0,
            "last_event_at": event_at.isoformat(),
            "last_received_at": received_at.isoformat(),
            "processing_started_at": processed_at.isoformat(),
        },
        occurred_at=processed_at,
        trace_id="scheduled-test-recorder",
    )
    with runtime.store.database.begin() as connection:
        for table in tables:
            for symbol in selected[table.name]:
                row = {
                    next(iter(table.primary_key)).name: str(uuid4()),
                    "symbol": symbol,
                    "feed_source": "iex",
                    "event_at": event_at,
                    "received_at": received_at,
                    "processed_at": processed_at,
                }
                if table is market_bars:
                    row["event_at"] = event_at - timedelta(seconds=bar_lag_seconds)
                    row.update(timeframe="1Min", open=100, high=101, low=99, close=100, volume=1)
                elif table is market_quotes:
                    row.update(
                        bid_price=100,
                        ask_price=101,
                        bid_exchange="V",
                        ask_exchange="V",
                        bid_size=1,
                        ask_size=1,
                    )
                else:
                    row.update(
                        provider_trade_id=str(uuid4()),
                        exchange="V",
                        price=100,
                        size=1,
                        conditions=[],
                    )
                connection.execute(table.insert().values(**row))


@pytest.mark.parametrize(
    "failure",
    [None, "missing_symbol", "stale_context", "halt", "macro", "stream", "rss", "quote", "risk"],
)
def test_real_collector_requires_durable_each_symbol_and_live_equipment(prepared, failure):
    runtime, session = prepared
    advance(runtime, ENTRY)
    runtime._ensure_session_plan(ENTRY)
    runtime.context = runtime.context_client.poll(symbols=["AAPL"])
    runtime.last_source_success = ENTRY
    runtime.source.poll(start=OPEN, end=ENTRY)
    runtime.market_states["AAPL"] = runtime.market.state("AAPL", ENTRY)
    seed_durable_recorder(runtime, ENTRY)
    if failure == "missing_symbol":
        from tradeagent.persistence import market_trades

        with runtime.store.database.begin() as connection:
            connection.execute(market_trades.delete().where(market_trades.c.symbol == "GLD"))
    elif failure == "stale_context":
        runtime.context = runtime.context.model_copy(
            update={"observed_at": ENTRY - timedelta(seconds=91)}
        )
    elif failure in {"halt", "macro"}:
        runtime.context_client.halted = failure == "halt"
        runtime.context_client.scheduled = (ENTRY,) if failure == "macro" else ()
        runtime.context = runtime.context_client.poll(symbols=["AAPL"])
    elif failure == "stream":
        runtime.order_stream.health_snapshot = lambda: {}
    elif failure == "rss":
        session = session.model_copy(update={"maximum_process_rss_mib": 50})
    elif failure == "quote":
        runtime.market.changes["quote_at"] = ENTRY - timedelta(seconds=6)
    elif failure == "risk":
        runtime.repo.set_control(
            "paper-risk:" + ACCOUNT,
            json.dumps({"capital": "10000", "peak": "11000"}),
        )
    sample = scheduled_paper.collect_readiness(
        runtime, session, ENTRY, ENTRY - timedelta(minutes=1)
    )
    failures = [key for key, passed in sample["checks"].items() if not passed]
    assert bool(failures) == bool(failure and failure != "missing_symbol"), sample
    assert sample["recorder_batch_id"] is not None
    assert sample["infrastructure_capacity_basis"] == "reviewed-nightly-not-live-market-metrics"
    assert (
        sample["nightly_infrastructure_evidence"]["morning_render_or_postgres_memory_observed"]
        is False
    )
    assert sample["account_risk_evidence"]["history_identity"]
    assert runtime.broker.submissions == 0


def additional_closed_buy(history, identity, at):
    order = copy.deepcopy(history["orders"][0])
    order.update(
        id=identity,
        client_order_id=identity,
        symbol="AAPL",
        created_at=(at - timedelta(minutes=5)).isoformat(),
        submitted_at=(at - timedelta(minutes=5)).isoformat(),
        canceled_at=(at - timedelta(minutes=4)).isoformat(),
        updated_at=(at - timedelta(minutes=4)).isoformat(),
    )
    history["orders"].append(order)


@pytest.mark.parametrize("previous_buys", [1, 2])
def test_complete_history_import_and_reservation_share_account_day_cap(
    prepared, monkeypatch, previous_buys
):
    runtime, session = prepared
    current = copy.deepcopy(session.reviewed_history)
    current["observed_at"] = ENTRY.isoformat()
    for i in range(previous_buys):
        additional_closed_buy(current, f"prior-closed-{i}", ENTRY)
    validate_history_extension(session, current, ENTRY)
    runtime.broker.account_history = lambda: copy.deepcopy(current)
    runtime, session, result = warmed(prepared, monkeypatch)
    budget = json.loads(runtime.repo.get_control(session_control_key(ACCOUNT, ENTRY.date())))
    assert budget["total_entries_reserved"] == 2
    if previous_buys == 1:
        assert result["state"] == "delegated"
        assert operator_calibration.step(runtime, observed_at=ENTRY)["state"] == "filled"
        budget = json.loads(runtime.repo.get_control(session_control_key(ACCOUNT, ENTRY.date())))
        assert budget["total_entries_reserved"] == 2
    else:
        assert result["state"] == "readiness_blocked"
        assert runtime.repo.get_control(f"{session.key}:delegation") is None
        assert runtime.broker.submissions == 0


def test_actual_earlier_owner_approval_is_not_rewritten_to_later_verification(prepared):
    _runtime, session = prepared
    earlier = session.model_copy(update={"approved_at": APPROVED - timedelta(hours=2)})
    validated = ScheduledPaperSession.model_validate(earlier.model_dump())
    assert validated.approved_at == APPROVED - timedelta(hours=2)
    assert validated.nightly_acceptance_at > validated.approved_at
    assert session.approved_at == APPROVED


def test_deadline_notification_happens_even_if_regular_source_collection_fails(
    prepared, monkeypatch
):
    runtime, session = prepared

    def fail(**kwargs):
        raise ValueError("source unavailable")

    monkeypatch.setattr(runtime.source, "poll", fail)
    with pytest.raises(ValueError, match="source unavailable"):
        tick(runtime, DEADLINE)
    assert json.loads(runtime.repo.get_control(f"{session.key}:terminal"))["state"] == "MISSED"
    assert runtime.broker.submissions == 0


@pytest.mark.parametrize("outcome", ["partial", "unknown"])
def test_scheduled_partial_or_unknown_uses_existing_recovery_and_no_second_buy(
    prepared, monkeypatch, outcome
):
    runtime, session, result = warmed(prepared, monkeypatch)
    assert result["state"] == "delegated"
    runtime.broker.partial = True
    runtime.broker.timeout = outcome == "unknown"
    first = operator_calibration.step(runtime, observed_at=ENTRY)
    assert first.get("_operator_active"), first
    advance(runtime, ENTRY + timedelta(seconds=31))
    runtime.broker.timeout = False
    operator_calibration.step(runtime, observed_at=Clock.current)
    advance(runtime, ENTRY + timedelta(seconds=61))
    operator_calibration.step(runtime, observed_at=Clock.current)
    rows = runtime.store.linked_orders(session.operator_cohort_id)
    assert len([row for row in rows if row["side"] == "buy"]) == 1
    assert rows[0]["link"]["expires_at"] == (ENTRY + timedelta(seconds=30)).isoformat()
    assert rows[0]["link"]["exit_at"] == (ENTRY + timedelta(seconds=60)).isoformat()


def test_readiness_certificate_cannot_be_replayed_after_restart_delay(prepared, monkeypatch):
    runtime, session, result = warmed(prepared, monkeypatch)
    assert result["state"] == "delegated"
    advance(runtime, ENTRY + timedelta(seconds=91))
    assert scheduled_paper.step(runtime, now=Clock.current)["state"] == "MISSED"
    assert (
        operator_calibration.step(runtime, observed_at=Clock.current)["state"]
        == "blocked_requires_review"
    )
    assert runtime.broker.submissions == 0
    assert runtime.repo.get_control(f"{session.key}:delegation") is not None


def test_scheduled_host_oms_rejects_direct_unapproved_entry(prepared):
    from tradeagent.experimental_policy import certificate

    runtime, _session = prepared
    advance(runtime, ENTRY)
    proof = certificate(
        runtime.settings,
        config_hash=runtime.config_hash,
        code_sha=runtime.code_sha,
        account_id=runtime.broker.account().id,
        checks={"synthetic": True},
        now=ENTRY,
    )
    result = runtime.oms.submit_entry(
        symbol="AAPL",
        cluster_key="ordinary",
        decision_id="ordinary",
        eligible_at=ENTRY,
        expires_at=DEADLINE,
        bid=Decimal("99.99"),
        ask=Decimal(100),
        quote_at=ENTRY,
        median_dollar_volume=Decimal(100000000),
        source_valid=True,
        certificate=proof,
        now=ENTRY,
    )
    assert result["reasons"] == ["SCHEDULE_DELEGATION_REQUIRED"]
    assert runtime.broker.submissions == 0


def test_cli_exposes_actual_scheduled_paper_mode(monkeypatch):
    import argparse

    from tradeagent import event_cli

    parser = argparse.ArgumentParser()
    event_cli.register_event_commands(parser.add_subparsers(dest="command"))
    args = parser.parse_args(
        [
            "run",
            "--mode",
            "experimental-paper",
            "--entry-policy",
            "scheduled-operator",
            "--purpose",
            "iex-practice",
            "--practice-start-date",
            "2026-09-11",
            "--news-account-digest",
            ACCOUNT,
            "--symbols",
            "AAPL",
            "--once",
        ]
    )
    seen = []

    async def run(settings, **kwargs):
        seen.append(settings)
        return {"fixture": True}

    monkeypatch.setattr(event_cli, "run_event_service", run)
    assert event_cli.handle_event_command(args)
    assert seen[0].mode == "experimental-paper" and seen[0].entry_policy == "scheduled-operator"


def test_invalid_schedule_and_collection_failure_never_starve_owned_exit(prepared, monkeypatch):
    runtime, session, result = warmed(prepared, monkeypatch)
    assert result["state"] == "delegated"
    assert operator_calibration.step(runtime, observed_at=ENTRY)["state"] == "filled"
    runtime.repo.set_control(f"scheduled-paper-host:{session.worker_cohort_id}", "invalid")

    def fail(**kwargs):
        raise ValueError("collection unavailable")

    monkeypatch.setattr(runtime.source, "poll", fail)
    with pytest.raises(ValueError, match="collection unavailable"):
        tick(runtime, ENTRY + timedelta(seconds=61))
    assert not runtime.broker.positions() and not runtime.broker.open_orders()
    rows = runtime.store.linked_orders(session.operator_cohort_id)
    assert len([row for row in rows if row["side"] == "buy"]) == 1
    assert len([row for row in rows if row["side"] == "sell"]) == 1


def test_night_worker_collects_without_ordinary_calibration_or_order(prepared):
    runtime, session = prepared
    result = tick(runtime, APPROVED)
    assert result["mode"] == "experimental-paper"
    assert result["scheduled_paper"]["state"] == "scheduled_preopen"
    assert result["calibration"] is None
    assert runtime.repo.get_control(f"{session.worker_cohort_id}:calibration_status") is None
    assert runtime.broker.submissions == 0


def test_thirty_minutes_of_real_durable_collector_evidence_delegates_once(prepared):
    runtime, session = prepared
    for i in range(61):
        at = ENTRY - timedelta(minutes=30) + timedelta(seconds=30 * i)
        advance(runtime, at)
        runtime._ensure_session_plan(at)
        runtime.context = runtime.context_client.poll(symbols=["AAPL"])
        runtime.last_source_success = at
        runtime.source.poll(start=OPEN, end=at)
        runtime.market_states["AAPL"] = runtime.market.state("AAPL", at)
        runtime.first_bar_receipts.setdefault(
            ("AAPL", runtime.market_states["AAPL"].completed_bar.timestamp),
            at - timedelta(seconds=3),
        )
        seed_durable_recorder(runtime, at)
        result = scheduled_paper.step(runtime, now=at)
    assert result["state"] == "delegated", result
    proof = json.loads(runtime.repo.get_control(f"{session.key}:readiness-certificate"))
    assert proof["continuous_seconds"] == 1800
    assert all(proof["checks"].values())
    assert proof["worker_owner"] == runtime.instance_id
    assert proof["account_risk_evidence"]["economic_risk"]["state"] == "valued"
    assert proof["account_risk_evidence"]["economic_risk"]["blocked"] is False
    assert len(proof["market"]["market_quotes"]) == 5
    assert operator_calibration.step(runtime, observed_at=ENTRY)["state"] == "filled"
    assert runtime.broker.submissions == 1


def test_tampered_readiness_certificate_cannot_authorize_final_dispatch(prepared, monkeypatch):
    runtime, session, result = warmed(prepared, monkeypatch)
    assert result["state"] == "delegated"
    request = OperatorPaperRequest.model_validate_json(
        runtime.repo.get_control(f"{session.key}:delegation")
    )
    runtime.repo.set_control(f"{session.key}:readiness-certificate", '{"fake":true}')
    assert not final_entry_fence(runtime.repo, request, runtime.instance_id, ENTRY)
    assert (
        operator_calibration.step(runtime, observed_at=ENTRY)["state"] == "blocked_requires_review"
    )
    assert runtime.broker.submissions == 0


def test_lease_lost_during_history_cannot_publish_request_or_reserve_budget(prepared, monkeypatch):
    runtime, session = prepared

    def history_after_lease_loss():
        with runtime.store.database.begin() as connection:
            connection.execute(
                update(worker_locks)
                .where(worker_locks.c.lock_name == "tradeagent-event-worker")
                .values(owner_id="new-owner")
            )
        return {**copy.deepcopy(session.reviewed_history), "observed_at": ENTRY.isoformat()}

    runtime.broker.account_history = history_after_lease_loss
    runtime, session, result = warmed(prepared, monkeypatch)
    assert result["state"] == "readiness_blocked"
    assert runtime.repo.get_control(f"{session.key}:delegation") is None
    assert runtime.repo.get_control(f"operator-paper-request:{session.worker_cohort_id}") is None
    assert runtime.repo.get_control(session_control_key(ACCOUNT, ENTRY.date())) is None
    assert runtime.broker.submissions == 0


def prearm(prepared, monkeypatch, *, persist=False, **changes):
    runtime, session = prepared
    monkeypatch.setattr("tradeagent.event_doctor.code_identity", lambda: runtime.code_sha)
    return scheduled_paper.prearm_baseline(
        runtime.store,
        runtime.broker,
        **{
            "reviewed_code_sha": runtime.code_sha,
            "expected_account_digest": session.account_digest,
            "reviewed_history_sha256": session.reviewed_history_sha256,
            "persist": persist,
            **changes,
        },
    )


def test_prearm_baseline_preview_is_read_only_not_market_acceptance(prepared, monkeypatch):
    from tradeagent.persistence import events

    runtime, session = prepared
    with runtime.store.database.begin() as connection:
        before_controls = list(connection.execute(select(controls)))
        before_events = list(connection.execute(select(events.c.event_id)))
    result = prearm(prepared, monkeypatch)
    assert result["persisted"] is False
    assert result["market_acceptance"] is False
    assert result["schedule_authority_created"] is False
    assert result["preview"]["history_identity"] == session.reviewed_history_sha256
    assert result["preview"]["budget_corrections"]
    with runtime.store.database.begin() as connection:
        assert list(connection.execute(select(controls))) == before_controls
        assert list(connection.execute(select(events.c.event_id))) == before_events
    assert runtime.broker.submissions == 0


def test_prearm_import_preserves_budgets_and_audits_each_upward_correction_once(
    prepared, monkeypatch
):
    from tradeagent.event_session import empty_session_budget
    from tradeagent.persistence import events

    runtime, session = prepared
    day = date(2026, 9, 4)
    budget = empty_session_budget(ACCOUNT, day)
    budget.update(
        total_entries_reserved=1,
        external_buy_client_ids=["early-spy"],
        retained_operator_note="never reset this metadata",
    )
    key = session_control_key(ACCOUNT, day)
    runtime.repo.set_control(key, json.dumps(budget))
    versions = control_versions(runtime.repo, session.worker_cohort_id, session.operator_cohort_id)
    for _ in range(2):
        result = prearm(prepared, monkeypatch, persist=True)
        assert result["persisted"] is True
        assert result["baseline_identity"] == session.reviewed_history_sha256
        assert all(
            not row["added_external_buy_client_ids"]
            for row in result["after_import_preview"]["budget_corrections"]
        )
    after = json.loads(runtime.repo.get_control(key))
    assert after["total_entries_reserved"] == 2
    assert after["retained_operator_note"] == budget["retained_operator_note"]
    assert (
        control_versions(runtime.repo, session.worker_cohort_id, session.operator_cohort_id)
        == versions
    )
    with runtime.store.database.begin() as connection:
        corrections = list(
            connection.scalars(
                select(events.c.payload).where(
                    events.c.event_type == "event_account_history_budget_correction"
                )
            )
        )
    target = [row for row in corrections if row["session_date"] == str(day)]
    assert len(target) == 1
    assert target[0]["before"] == budget
    assert target[0]["after"] == after
    assert target[0]["added_external_buy_client_ids"] == ["early-buy"]
    assert target[0]["upward_only"] is True
    assert runtime.broker.submissions == 0
    assert runtime.repo.get_control("operator-paper-active") is None


@pytest.mark.parametrize(
    "failure", ["code", "account", "history", "incomplete", "future", "active_command", "positions"]
)
def test_prearm_import_rejects_bad_identity_incomplete_or_active_account(
    prepared, monkeypatch, failure
):
    runtime, session = prepared
    changes = {}
    if failure == "code":
        changes["reviewed_code_sha"] = "e" * 40
    elif failure == "account":
        changes["expected_account_digest"] = "e" * 64
    elif failure == "history":
        changes["reviewed_history_sha256"] = "e" * 64
    elif failure == "incomplete":
        snapshot = {**copy.deepcopy(session.reviewed_history), "complete": False}
        runtime.broker.account_history = lambda: snapshot
    elif failure == "future":
        snapshot = copy.deepcopy(session.reviewed_history)
        snapshot["orders"][0]["updated_at"] = (APPROVED + timedelta(days=1)).isoformat()
        runtime.broker.account_history = lambda: snapshot
        changes["reviewed_history_sha256"] = history_identity(snapshot)
    elif failure == "active_command":
        runtime.repo.set_control("operator-paper-active", "invalid-command-still-blocks")
    else:
        runtime.broker.positions = lambda: [SimpleNamespace(symbol="AAPL")]
    with pytest.raises(ValueError):
        prearm(prepared, monkeypatch, persist=True, **changes)
    assert runtime.repo.get_control("paper-baseline:" + ACCOUNT) is None
    assert runtime.broker.submissions == 0


def test_budget_correction_audit_rolls_back_with_failed_baseline_import(prepared):
    from tradeagent.event_session import empty_session_budget
    from tradeagent.persistence import events

    runtime, session = prepared
    day = date(2026, 9, 9)
    bad_budget = empty_session_budget(ACCOUNT, day)
    bad_budget.update(total_entries_reserved=1, external_buy_client_ids=["unverified-retained-buy"])
    runtime.repo.set_control(session_control_key(ACCOUNT, day), json.dumps(bad_budget))
    with pytest.raises(ValueError, match="cannot remove previously counted"):
        save_baseline(runtime.store, session.reviewed_history, APPROVED)
    assert runtime.repo.get_control("paper-baseline:" + ACCOUNT) is None
    assert runtime.repo.get_control(session_control_key(ACCOUNT, date(2026, 9, 4))) is None
    with runtime.store.database.begin() as connection:
        assert not list(
            connection.scalars(
                select(events.c.event_id).where(
                    events.c.event_type == "event_account_history_budget_correction"
                )
            )
        )


def sparse_observe(runtime, session, at, *, missing_series=None):
    from tradeagent.domain import MarketBar

    advance(runtime, at)
    runtime.context = runtime.context_client.poll(symbols=["AAPL"])
    runtime.last_source_success = at
    runtime.source.poll(start=OPEN, end=at)
    available = at - timedelta(seconds=2)
    completed_at = available.replace(
        minute=available.minute // 5 * 5, second=0, microsecond=0
    ) - timedelta(minutes=5)
    runtime.market.changes["completed_bar"] = (
        MarketBar(
            symbol="AAPL",
            timestamp=completed_at,
            open=Decimal(100),
            high=Decimal("100.1"),
            low=Decimal("99.9"),
            close=Decimal(100),
            volume=Decimal(1000),
        )
        if completed_at >= OPEN
        else None
    )
    state = runtime.market.state("AAPL", at)
    runtime.market_states["AAPL"] = state
    if state.completed_bar:
        runtime.first_bar_receipts.setdefault(("AAPL", state.completed_bar.timestamp), at)
    minute = int((at - OPEN).total_seconds() // 60)
    trades, bars = [], []
    if at.second == 2:
        trades.extend(("SPY", "QQQ", "IWM"))
        bars.extend(("SPY", "QQQ", "IWM"))
        if minute % 4 == 1:
            trades.append("GLD")
        if minute % 4 == 2:
            bars.append("GLD")
        if minute % 3 == 2:
            trades.append("TLT")
        if minute % 3 == 0:
            bars.append("TLT")
    selected = {
        "market_quotes": list(scheduled_paper.RECORDER_SYMBOLS),
        "market_trades": trades,
        "market_bars": bars,
    }
    if missing_series:
        selected[missing_series] = [
            symbol for symbol in selected[missing_series] if symbol != "GLD"
        ]
    seed_durable_recorder(runtime, at, symbols_by_table=selected, bar_lag_seconds=60)
    return scheduled_paper.step(runtime, now=at)


def test_sparse_real_series_prove_three_windows_and_survive_post_issue_idle_minute(
    prepared, monkeypatch
):
    from tradeagent import market_progress

    runtime, session = prepared
    queries = []
    original = market_progress.market_window_counts

    def count_windows(database, *, since, until):
        queries.append((since, until))
        return original(database, since=since, until=until)

    monkeypatch.setattr(market_progress, "market_window_counts", count_windows)
    start = OPEN + timedelta(seconds=2)
    for i in range(70):
        result = sparse_observe(runtime, session, start + timedelta(seconds=30 * i))
        assert result["state"] == "warming_up", result
        assert result["continuous_since"] == start.isoformat()
        assert not result["failures"]
    result = sparse_observe(runtime, session, ENTRY)
    assert result["state"] == "delegated", result
    proof = json.loads(runtime.repo.get_control(f"{session.key}:readiness-certificate"))
    windows = proof["physical_progress_windows"]
    assert len(windows) == 3
    assert scheduled_paper.physical_coverage_valid(windows, ENTRY)
    assert proof["continuous_since"] == start.isoformat()
    assert proof["continuous_seconds"] == 2098
    assert len(windows[0]["first"]["market"]["market_trades"]) == 0
    last_trades = {row["symbol"]: row for row in windows[-1]["last"]["market"]["market_trades"]}
    last_bars = {row["symbol"]: row for row in windows[-1]["last"]["market"]["market_bars"]}
    assert ENTRY - datetime.fromisoformat(last_trades["GLD"]["latest_exchange_at"]) > timedelta(
        seconds=90
    )
    assert ENTRY - datetime.fromisoformat(last_bars["TLT"]["latest_exchange_at"]) > timedelta(
        seconds=150
    )
    assert runtime.broker.submissions == 0
    # Neither GLD nor TLT needs a new trade/bar in this final one-minute window.
    assert sparse_observe(runtime, session, ENTRY + timedelta(seconds=1))["state"] == "delegated"
    assert operator_calibration.step(runtime, observed_at=Clock.current)["state"] == "filled"
    assert runtime.broker.submissions == 1
    assert len(queries) == 7
    assert all(
        start <= since <= until <= ENTRY and until - since <= timedelta(minutes=20)
        for since, until in queries
    )


@pytest.mark.parametrize("series", ["market_quotes", "market_trades", "market_bars"])
def test_missing_series_blocks_at_aggregate_deadline_not_every_sparse_poll(prepared, series):
    runtime, session = prepared
    start = OPEN + timedelta(seconds=2)
    for i in range(20):
        result = sparse_observe(
            runtime, session, start + timedelta(seconds=30 * i), missing_series=series
        )
        assert result["continuous_since"] == start.isoformat()
        assert not result["failures"]
    result = sparse_observe(runtime, session, start + timedelta(minutes=10), missing_series=series)
    assert result["continuous_since"] is None
    assert f"physical:{series}:GLD:not_advancing" in result["failures"]
    assert runtime.broker.submissions == 0


@pytest.mark.parametrize(
    "failure",
    [
        "durable_exchange_stale",
        "reported_commit_stale",
        "quotes_stale",
        "owner",
        "gap",
        "recorder_gap",
    ],
)
def test_critical_recorder_ten_second_freshness_owner_and_gaps_reset_proof(prepared, failure):
    from tradeagent.persistence import events, heartbeats, market_quotes

    runtime, session = prepared
    start = OPEN + timedelta(seconds=2)
    for i in range(21):
        sparse_observe(runtime, session, start + timedelta(seconds=30 * i))
    at = start + timedelta(minutes=10, seconds=30)
    sparse_observe(runtime, session, at)
    if failure == "durable_exchange_stale":
        # A live-looking heartbeat is not a replacement for fresh durable exchange evidence.
        with runtime.store.database.begin() as connection:
            connection.execute(
                events.delete().where(
                    events.c.event_type == "shadow_recorder_batch",
                    events.c.occurred_at >= at - timedelta(seconds=10),
                )
            )
    elif failure == "reported_commit_stale":
        heartbeat = runtime.repo.latest_heartbeat("tradeagent-shadow-recorder")
        details = {
            **heartbeat[2],
            "last_market_commit_at": (at - timedelta(seconds=11)).isoformat(),
        }
        with runtime.store.database.begin() as connection:
            connection.execute(
                update(heartbeats)
                .where(heartbeats.c.service_name == "tradeagent-shadow-recorder")
                .values(details=details)
            )
    elif failure == "recorder_gap":
        heartbeat = runtime.repo.latest_heartbeat("tradeagent-shadow-recorder")
        details = {**heartbeat[2], "gaps": heartbeat[2].get("gaps", 0) + 1}
        with runtime.store.database.begin() as connection:
            connection.execute(
                update(heartbeats)
                .where(heartbeats.c.service_name == "tradeagent-shadow-recorder")
                .values(details=details)
            )
    elif failure == "quotes_stale":
        with runtime.store.database.begin() as connection:
            connection.execute(
                market_quotes.delete().where(
                    market_quotes.c.event_at >= at - timedelta(seconds=10),
                )
            )
    elif failure == "owner":
        runtime.instance_id = "replacement-owner"
        with runtime.store.database.begin() as connection:
            connection.execute(
                update(worker_locks)
                .where(worker_locks.c.lock_name == "tradeagent-event-worker")
                .values(owner_id=runtime.instance_id)
            )
    else:
        previous = runtime.order_stream.health_snapshot()
        runtime.order_stream.health_snapshot = lambda: {**previous, "gap_count": 1}
    result = scheduled_paper.step(runtime, now=at)
    progress = json.loads(runtime.repo.get_control(f"{session.key}:progress"))
    assert progress["physical_proofs"] == []
    if failure in {"owner", "gap", "recorder_gap"}:
        assert result["continuous_since"] == at.isoformat()
    else:
        assert result["continuous_since"] is None
        assert ("recorder_quote" if failure == "quotes_stale" else "recorder") in result["failures"]
    assert runtime.broker.submissions == 0


@pytest.mark.parametrize("slow_component", ["context", "market", "news"])
@pytest.mark.parametrize("outcome", ["filled", "partial", "unknown"])
def test_owned_operator_stop_skips_slow_ingestion(prepared, monkeypatch, slow_component, outcome):
    runtime, session, result = warmed(prepared, monkeypatch)
    assert result["state"] == "delegated"
    runtime.broker.partial = outcome != "filled"
    runtime.broker.timeout = outcome == "unknown"
    submitted = operator_calibration.step(runtime, observed_at=ENTRY)
    assert submitted["_operator_active"]
    assert runtime.broker.submissions == 1

    original_state = runtime.market.state
    slow_calls = []
    quote_calls = []

    def protective_quote(state):
        quote_calls.append(Clock.current)
        fresh = original_state(state.symbol, Clock.current)
        if Clock.current >= ENTRY + timedelta(seconds=22):
            fresh = fresh.model_copy(update={"bid": Decimal("99"), "ask": Decimal("99.01")})
        return fresh

    monkeypatch.setattr(runtime.market, "refresh_quote", protective_quote)
    target, method = {
        "context": (runtime.context_client, "poll"),
        "market": (runtime.market, "state"),
        "news": (runtime.source, "poll"),
    }[slow_component]
    original_call = getattr(target, method)

    def slow_call(*args, **kwargs):
        slow_calls.append(Clock.current)
        advance(runtime, Clock.current + timedelta(seconds=60))
        return original_call(*args, **kwargs)

    monkeypatch.setattr(target, method, slow_call)
    holding = tick(runtime, ENTRY + timedelta(seconds=2))
    assert quote_calls
    if outcome != "unknown":
        assert holding["_operator_active"]
        assert Clock.current == ENTRY + timedelta(seconds=2)
        assert slow_calls == []
        assert runtime.broker.submissions == 1
        # Full collection may resume only once owned exposure has actually been exited.
        tick(runtime, ENTRY + timedelta(seconds=22))
    buys = [order for order in runtime.broker.values.values() if order.side == "buy"]
    sells = [order for order in runtime.broker.values.values() if order.side == "sell"]
    assert len(buys) == len(sells) == 1
    # Unknown acknowledgements already pause and exit immediately after reconciliation.
    assert sells[0].created_at == ENTRY + timedelta(seconds=2 if outcome == "unknown" else 22)
    assert sells[0].filled_quantity == buys[0].filled_quantity
    assert not runtime.broker.positions() and not runtime.broker.open_orders()
    assert runtime.store.linked_orders(session.operator_cohort_id)


@pytest.mark.parametrize("collection_delay_seconds", [30, 60])
def test_completed_readiness_collection_rechecks_observation_gap(
    prepared, monkeypatch, collection_delay_seconds
):
    runtime, session = prepared
    start = OPEN + timedelta(seconds=2)
    for index in range(69):
        sparse_observe(runtime, session, start + timedelta(seconds=30 * index))
    prior = json.loads(runtime.repo.get_control(f"{session.key}:progress"))
    assert prior["last_at"] == (ENTRY - timedelta(seconds=58)).isoformat()
    assert len(prior["physical_proofs"]) == 3
    began = ENTRY + timedelta(seconds=2)
    finished = began + timedelta(seconds=collection_delay_seconds)
    advance(runtime, began)
    bar = runtime.market_states["AAPL"].completed_bar.model_copy(
        update={"timestamp": ENTRY - timedelta(minutes=5)}
    )
    runtime.market.changes["completed_bar"] = bar
    runtime.first_bar_receipts[("AAPL", bar.timestamp)] = began
    original_collect = scheduled_paper.collect_readiness

    def slow_collect(runtime, session, now, since, *, include_physical=True):
        assert now == began
        Clock.current = runtime.broker.now = finished
        runtime.context = runtime.context_client.poll(symbols=["AAPL"])
        runtime.last_source_success = finished
        runtime.source.poll(start=OPEN, end=finished)
        runtime.market_states["AAPL"] = runtime.market.state("AAPL", finished)
        seed_durable_recorder(runtime, finished)
        sample = original_collect(
            runtime, session, finished, since, include_physical=include_physical
        )
        assert sample["observed_at"] == finished.isoformat()
        assert all(sample["checks"].values()), sample
        return sample

    monkeypatch.setattr(scheduled_paper, "collect_readiness", slow_collect)
    result = scheduled_paper.step(runtime, now=began)
    if collection_delay_seconds == 30:
        assert result["state"] == "delegated", result
        assert operator_calibration.step(runtime, observed_at=finished)["state"] == "filled"
    else:
        assert result["state"] == "warming_up", result
        assert result["continuous_since"] == finished.isoformat()
        assert result["completed_physical_windows"] == 0
        current = json.loads(runtime.repo.get_control(f"{session.key}:progress"))
        assert current["physical_proofs"] == []
        assert current["anchor"]["market_count_scope"]["since_exchange_at"] == finished.isoformat()
        assert runtime.repo.get_control(f"{session.key}:delegation") is None
        assert (
            runtime.repo.get_control(f"operator-paper-request:{session.worker_cohort_id}") is None
        )
        assert runtime.broker.submissions == 0


def test_partial_remainder_cancel_and_timed_exit_do_not_wait_for_news(prepared, monkeypatch):
    runtime, session, result = warmed(prepared, monkeypatch)
    assert result["state"] == "delegated"
    runtime.broker.partial = True
    assert operator_calibration.step(runtime, observed_at=ENTRY)["state"] == "partially_filled"
    original_poll = runtime.source.poll
    calls = []

    def slow_news(**kwargs):
        calls.append(Clock.current)
        advance(runtime, Clock.current + timedelta(seconds=60))
        return original_poll(**kwargs)

    monkeypatch.setattr(runtime.source, "poll", slow_news)
    for seconds in (2, 31):
        at = ENTRY + timedelta(seconds=seconds)
        assert tick(runtime, at)["_operator_active"]
        assert Clock.current == at
        assert calls == []
    buy = next(order for order in runtime.broker.values.values() if order.side == "buy")
    assert buy.status.value == "canceled"
    rows = runtime.store.linked_orders(session.operator_cohort_id)
    assert rows[0]["link"]["cancel_requested_at"] == (ENTRY + timedelta(seconds=31)).isoformat()
    tick(runtime, ENTRY + timedelta(seconds=61))
    sell = next(order for order in runtime.broker.values.values() if order.side == "sell")
    assert sell.created_at == ENTRY + timedelta(seconds=61)
    assert sell.filled_quantity == buy.filled_quantity
    assert runtime.broker.submissions == 2


def test_unknown_order_with_invalid_pointer_keeps_recovery_fast_and_entry_free(
    prepared, monkeypatch
):
    runtime, _session, result = warmed(prepared, monkeypatch)
    assert result["state"] == "delegated"
    runtime.broker.partial = runtime.broker.timeout = True
    assert (
        operator_calibration.step(runtime, observed_at=ENTRY)["state"]
        == "submission_outcome_unknown"
    )
    runtime.repo.set_control("operator-paper-active", "invalid-pointer")
    original_find = runtime.broker.find_order_by_client_id
    monkeypatch.setattr(runtime.broker, "find_order_by_client_id", lambda client_id: None)
    source_calls = []
    original_poll = runtime.source.poll

    def slow_news(**kwargs):
        source_calls.append(Clock.current)
        advance(runtime, Clock.current + timedelta(seconds=60))
        return original_poll(**kwargs)

    monkeypatch.setattr(runtime.source, "poll", slow_news)
    pending = tick(runtime, ENTRY + timedelta(seconds=2))
    assert pending["_operator_active"]
    assert pending["ordinary_entries_enabled"] is False
    assert source_calls == []
    assert runtime.broker.submissions == 1
    monkeypatch.setattr(runtime.broker, "find_order_by_client_id", original_find)
    tick(runtime, ENTRY + timedelta(seconds=3))
    buys = [order for order in runtime.broker.values.values() if order.side == "buy"]
    sells = [order for order in runtime.broker.values.values() if order.side == "sell"]
    assert len(buys) == len(sells) == 1
    assert sells[0].created_at == ENTRY + timedelta(seconds=3)
    assert sells[0].filled_quantity == buys[0].filled_quantity
    assert not runtime.broker.positions()
