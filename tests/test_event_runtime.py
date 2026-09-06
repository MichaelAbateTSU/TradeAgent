from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import ClassVar

from tradeagent.event_context import OfficialContextSnapshot
from tradeagent.event_market import EventMarketState
from tradeagent.event_replay import ReplayBroker, replay_event_pipeline
from tradeagent.event_research import SourceEvent, text_hash
from tradeagent.event_runtime import EventRuntime
from tradeagent.event_store import EventStore
from tradeagent.experimental_policy import ExperimentalSettings
from tradeagent.persistence import Database, ProductionRepository

NOW = datetime(2026, 9, 8, 15, tzinfo=UTC)


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
            raw_trade=None,
            completed_bar=None,
            previous_close=Decimal(100),
            median_daily_dollar_volume=Decimal("100000000"),
            pre_event_volatility_bps=Decimal(100),
        )


class Context:
    capabilities: ClassVar = {"synthetic": True}

    def poll(self, **kwargs):
        return OfficialContextSnapshot(observed_at=NOW, evidence=(), halts=())


def test_real_worker_flow_records_explicit_abstention_once_and_reconciles(
    tmp_path: Path, monkeypatch
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
            store, settings, Source(), Market(), broker, instance_id="fixture", code_sha="fixture"
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


def test_replay_is_separate_from_live_orders_and_alpha():
    report = replay_event_pipeline()
    assert report["broker_network_calls"] == 0
    assert report["live_or_real_paper_orders_submitted"] == 0
    assert report["decision"]["action"] == "eligible"
    assert report["reconciled"]
    assert len(report["synthetic_orders"]) == 2
    assert report["excluded_from_strategy_performance"]
