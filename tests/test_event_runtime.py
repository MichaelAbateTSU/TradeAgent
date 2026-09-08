from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import ClassVar

import pytest

from tradeagent.domain import MarketBar
from tradeagent.event_context import OfficialContextSnapshot
from tradeagent.event_market import EventMarketState
from tradeagent.event_replay import ReplayBroker, replay_event_pipeline
from tradeagent.event_research import SourceEvent, text_hash
from tradeagent.event_runtime import EventRuntime
from tradeagent.event_store import EventStore
from tradeagent.experimental_policy import ExperimentalSettings
from tradeagent.persistence import Database, ProductionRepository

NOW = datetime(2026, 9, 8, 15, tzinfo=UTC)


@pytest.mark.parametrize("busy", [True, False])
def test_eod_report_retries_independently_of_broker_completion_after_failure(monkeypatch, busy):
    import json
    from types import SimpleNamespace

    from sqlalchemy import func, select

    from tradeagent.persistence import events
    from tradeagent.reporting_reads import ReportBusyError

    close = NOW.replace(hour=20)
    calls = []
    with Database("sqlite:///:memory:") as database:
        database.initialize()
        repository = ProductionRepository(database)
        repository.set_control("kill_switch", "active")
        repository.set_control("eod-retry:pause", "OPERATOR_PAUSE")
        broker_result = {
            "healthy": True,
            "positions": {},
            "open_orders": 0,
            "mismatches": [],
            "recovery_cohorts": [],
        }

        def runtime():
            instance = object.__new__(EventRuntime)
            instance.repo = repository
            instance.store = EventStore(database)
            instance.settings = SimpleNamespace(cohort_id="eod-retry")
            instance.session_plan = SimpleNamespace(session_close=close, session_date=close.date())
            instance.oms = SimpleNamespace(
                reconcile=lambda now: dict(broker_result), scoped_orders=lambda: []
            )
            return instance

        def report(db, cohort, day, **kwargs):
            assert db is database and cohort == "eod-retry" and day == close.date()
            assert kwargs["persist"] is True
            calls.append(kwargs["observed_at"])
            if len(calls) == 1:
                error = ReportBusyError if busy else RuntimeError
                raise error("report initially unavailable")
            repository.append_event(
                "event_session_report",
                {"snapshot_persisted": True},
                occurred_at=kwargs["observed_at"],
                trace_id=cohort,
            )

        monkeypatch.setattr("tradeagent.event_session_report.session_report", report)
        if busy:
            runtime()._record_session_completion(close)
        else:
            with pytest.raises(RuntimeError, match="initially unavailable"):
                runtime()._record_session_completion(close)
        completion = repository.get_control("eod-retry:session-completion")
        assert json.loads(completion)["state"] == "complete"
        assert repository.get_control("eod-retry:session-completion-report") is None
        restarted = runtime()
        restarted._record_session_completion(close + timedelta(seconds=15))
        assert repository.get_control("eod-retry:session-completion-report") == completion
        restarted._record_session_completion(close + timedelta(seconds=30))
        assert len(calls) == 2
        with database.begin() as connection:
            assert (
                connection.scalar(
                    select(func.count())
                    .select_from(events)
                    .where(events.c.event_type == "event_session_report")
                )
                == 1
            )
        broker_result.update(healthy=False, positions={"AAPL": "1"}, open_orders=1)
        restarted._record_session_completion(close + timedelta(seconds=45))
        assert len(calls) == 3
        assert json.loads(repository.get_control("eod-retry:session-completion"))["state"] == (
            "incident_unresolved_exposure"
        )
        assert repository.get_control("kill_switch") == "active"
        assert repository.get_control("eod-retry:pause") == "OPERATOR_PAUSE"


class Clock(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW


class Source:
    last_errors = ()
    capabilities: ClassVar = {"sec_enabled": True, "primary_urls_configured": 0}

    def poll(self, **kwargs):
        return (
            SourceEvent(
                source_event_id="licensed-synthetic",
                source="fixture",
                source_url="https://example.org/story",
                source_version="one",
                published_at=NOW - timedelta(minutes=2),
                first_received_at=NOW - timedelta(minutes=1),
                content_available_at=NOW - timedelta(minutes=1),
                content_sha256=text_hash("headline"),
                content=None,
                headline="unverified headline",
                event_cluster_id="cluster",
                rights_profile="metadata-only",
                availability_basis="synthetic",
            ),
        )


class Market:
    def __init__(self, with_records=False):
        self.with_records = with_records

    def state(self, symbol, now):
        return EventMarketState(
            symbol=symbol,
            observed_at=now,
            feed="iex",
            bid=Decimal("100"),
            ask=Decimal("100.01"),
            bid_size=Decimal(100),
            ask_size=Decimal(100),
            quote_at=now,
            raw_quote={},
            raw_trade={"t": now.isoformat(), "p": "100", "s": "20", "i": "fixture-trade", "x": "V"}
            if self.with_records
            else None,
            completed_bar=MarketBar(
                symbol=symbol,
                timestamp=now,
                open=Decimal(100),
                high=Decimal(100),
                low=Decimal(100),
                close=Decimal(100),
                volume=Decimal(1000000),
            )
            if self.with_records
            else None,
            previous_close=Decimal(100),
            median_daily_dollar_volume=Decimal("100000000"),
            pre_event_volatility_bps=Decimal(100),
            completed_daily_sessions=30,
        )


class Context:
    capabilities: ClassVar = {"synthetic": True}

    def poll(self, **kwargs):
        return OfficialContextSnapshot(observed_at=NOW, evidence=(), halts=())


@pytest.mark.parametrize("with_records", [False, True])
def test_real_worker_flow_records_explicit_abstention_once_and_reconciles(
    tmp_path: Path, monkeypatch, with_records
):
    monkeypatch.setattr("tradeagent.event_runtime.datetime", Clock)
    with Database(f"sqlite:///{tmp_path / 'event-runtime.db'}") as database:
        database.initialize()
        store = EventStore(database)
        settings = ExperimentalSettings(mode="shadow", symbols="AAPL", cohort_id="test-runtime")
        repository = ProductionRepository(database)
        repository.acquire_worker_lock("tradeagent-event-worker", "fixture", observed_at=NOW)
        broker = ReplayBroker(NOW)
        runtime = EventRuntime(
            store,
            settings,
            Source(),
            Market(with_records),
            broker,
            instance_id="fixture",
            code_sha="fixture",
        )
        runtime.context_client.close()
        runtime.context_client = Context()
        first = runtime.tick(NOW)
        second = runtime.tick(NOW)
        report = store.report(settings.cohort_id)
        assert first["mode"] == second["mode"] == "shadow"
        assert report["decision_count"] == 1
        assert report["decisions"][0]["payload"]["action"] == "abstain"
        assert not broker.orders
        assert report["usable_forward_trading_sessions"] == 0
        assert repository.latest_heartbeat("tradeagent-event-worker") is not None
        cert = runtime.operational_preflight(NOW)
        assert not cert.permits_paper
        assert not cert.establishes_edge
        assert repository.market_data_counts() == ((1, 1, 1) if with_records else (0, 1, 0))


def test_replay_is_separate_from_live_orders_and_alpha():
    report = replay_event_pipeline()
    assert report["broker_network_calls"] == 0
    assert report["live_or_real_paper_orders_submitted"] == 0
    assert report["decision"]["action"] == "eligible"
    assert report["reconciled"]
    assert len(report["synthetic_orders"]) == 2
    assert report["excluded_from_strategy_performance"]


def test_numeric_extraction_failure_is_durable_dead_letter_not_silent_drop(
    tmp_path: Path, monkeypatch
):
    monkeypatch.setattr("tradeagent.event_runtime.datetime", Clock)
    with Database(f"sqlite:///{tmp_path / 'dead-letter.db'}") as database:
        database.initialize()
        settings = ExperimentalSettings(mode="shadow", symbols="AAPL", cohort_id="dead-letter")
        store = EventStore(database)
        repository = ProductionRepository(database)
        repository.acquire_worker_lock("tradeagent-event-worker", "fixture", observed_at=NOW)
        broker = ReplayBroker(NOW)
        runtime = EventRuntime(
            store, settings, Source(), Market(), broker, instance_id="fixture", code_sha="fixture"
        )
        runtime.context_client.close()
        runtime.context_client = Context()

        def malformed_numeric(*args, **kwargs):
            raise ArithmeticError("synthetic invalid numeric scale")

        monkeypatch.setattr(runtime, "_extraction", malformed_numeric)
        runtime.tick(NOW)
        runtime.tick(NOW)
        report = store.report(settings.cohort_id)
        assert report["decision_count"] == 1
        assert report["leading_no_trade_reasons"] == {"EXTRACTION_DEAD_LETTER": 1}
        assert not broker.orders
