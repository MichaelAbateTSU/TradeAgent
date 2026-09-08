from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
from pydantic import SecretStr
from sqlalchemy import event
from websockets.asyncio.client import connect as websocket_connect
from websockets.asyncio.server import serve

from tradeagent.alpaca_paper import (
    AlpacaPaperAccount,
    AlpacaPaperOrder,
    AlpacaPaperPosition,
)
from tradeagent.alpaca_stream import (
    AlpacaMarketStream,
    AlpacaStreamSettings,
    MarketQuote,
    MarketTrade,
    ReceivedStreamEvent,
)
from tradeagent.config import AppConfig, IntradayConfig
from tradeagent.data import synthetic_bars
from tradeagent.persistence import Database, ProductionRepository, events
from tradeagent.runtime import (
    MarketFeedStatusMonitor,
    ProductionPaperReconciler,
    ShadowAuditProcessor,
    run_shadow_runtime,
)
from tradeagent.shadow_recorder import ShadowRecorderSettings, persist_shadow_batch
from tradeagent.worker import AutonomousPaperWorker, WorkerMode


class FakePaperClient:
    def account(self) -> AlpacaPaperAccount:
        return AlpacaPaperAccount(
            id="account-1",
            status="ACTIVE",
            currency="USD",
            cash=Decimal("100000"),
            portfolio_value=Decimal("100000"),
            buying_power=Decimal("100000"),
            trading_blocked=False,
            transfers_blocked=False,
            account_blocked=False,
        )

    def positions(self) -> tuple[AlpacaPaperPosition, ...]:
        return ()

    def open_orders(self) -> tuple[AlpacaPaperOrder, ...]:
        return ()


def _running_recorder(repository, now, *, owner="worker-1", details=None):
    repository.acquire_worker_lock("tradeagent-shadow-recorder", owner)
    repository.refresh_worker_lock("tradeagent-shadow-recorder", owner, observed_at=now)
    repository.heartbeat(
        "tradeagent-shadow-recorder",
        owner,
        {
            "state": "healthy",
            "healthy": True,
            "last_event_at": now.isoformat(),
            "last_committed_event_at": now.isoformat(),
            "last_market_commit_at": now.isoformat(),
            **(details or {}),
        },
        observed_at=now,
    )


def _durable_batch(
    repository,
    event_at,
    *,
    received_at=None,
    processing_at=None,
    owner="worker-1",
    market_event=None,
):
    received_at = received_at or event_at + timedelta(milliseconds=30)
    processing_at = processing_at or received_at + timedelta(milliseconds=100)
    quote = market_event or MarketQuote(
        symbol="SPY",
        timestamp=event_at,
        bid_price=Decimal(100),
        ask_price=Decimal(101),
        bid_size=Decimal(10),
        ask_size=Decimal(20),
    )
    batch_id = str(uuid4())
    persist_shadow_batch(
        repository,
        [ReceivedStreamEvent(quote, received_at)],
        [],
        batch_id,
        clock=lambda: processing_at,
        instance_id=owner,
    )
    return batch_id


def test_production_reconciler_and_shadow_audit(tmp_path: Path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'runtime.db'}")
    database.initialize()
    repository = ProductionRepository(database)
    reconciler = ProductionPaperReconciler(FakePaperClient(), repository)
    now = datetime(2026, 9, 4, 15, tzinfo=UTC)

    status = reconciler.reconcile(observed_at=now)
    processor = ShadowAuditProcessor(repository)
    bar = next(synthetic_bars(count=1)).model_copy(update={"timestamp": now})
    quote = MarketQuote(
        symbol="SPY",
        timestamp=now,
        bid_price=Decimal("99.9"),
        ask_price=Decimal("100.1"),
        bid_size=Decimal("10"),
        ask_size=Decimal("10"),
    )
    asyncio.run(processor.on_bar(bar, can_enter=False))
    asyncio.run(processor.on_quote(quote, can_enter=False))

    assert status.healthy
    assert status.position_count == 0
    assert status.open_order_count == 0
    assert repository.event_count() == 3
    database.dispose()


def test_market_feed_monitor_marks_open_session_stale_and_closed_session_safe(
    tmp_path: Path,
) -> None:
    database = Database(f"sqlite:///{tmp_path / 'feed-monitor.db'}")
    database.initialize()
    repository = ProductionRepository(database)
    now = datetime(2026, 9, 4, 15, tzinfo=UTC)
    monitor = MarketFeedStatusMonitor(
        repository,
        IntradayConfig(),
        instance_id="feed-1",
        clock=lambda: now,
    )

    assert monitor.check() == "stale"
    assert repository.get_control("kill_switch") is None
    _running_recorder(repository, now)
    _durable_batch(repository, now - timedelta(seconds=1))
    assert monitor.check() == "healthy"
    now += timedelta(seconds=11)
    _running_recorder(repository, now)
    assert monitor.check() == "stale"
    assert repository.get_control("kill_switch") is None

    closed = MarketFeedStatusMonitor(
        repository,
        IntradayConfig(),
        instance_id="feed-1",
        clock=lambda: datetime(2026, 9, 6, 15, tzinfo=UTC),
    )
    assert closed.check() == "market_closed"
    database.dispose()


@pytest.mark.parametrize(
    "unhealthy",
    [
        {"state": "stopped"},
        {"state": "failed"},
        {"state": "starting"},
        {"persistence_error": "OperationalError"},
        {"dropped_events": 1},
        {"decision_errors": 1},
        {"notice_overflow": 1},
        {"last_market_commit_at": "2026-09-08T14:34:01+00:00"},
        {"last_market_commit_at": "not-a-date"},
    ],
)
def test_fresh_batch_cannot_mask_stopped_or_faulted_recorder(
    tmp_path: Path,
    unhealthy,
) -> None:
    database = Database(f"sqlite:///{tmp_path / 'true-feed-health.db'}")
    database.initialize()
    repository = ProductionRepository(database)
    repository.set_control("kill_switch", "inactive")
    now = datetime(2026, 9, 8, 14, 34, tzinfo=UTC)
    _running_recorder(repository, now, details=unhealthy)
    _durable_batch(repository, now - timedelta(seconds=1))
    repository.heartbeat(
        "tradeagent-market-feed",
        "legacy",
        {
            "state": "healthy",
        },
        observed_at=datetime(2026, 9, 8, 14, 29, tzinfo=UTC),
    )
    monitor = MarketFeedStatusMonitor(
        repository,
        IntradayConfig(),
        instance_id="monitor",
        clock=lambda: now,
    )
    assert monitor.check() == "stale"
    assert repository.latest_heartbeat("tradeagent-shadow-market-feed")[2]["healthy"] is False
    assert repository.get_control("kill_switch") == "inactive"
    database.dispose()


@pytest.mark.parametrize(
    ("checked_at", "event_at", "received_at", "processing_at", "old_commit"),
    [
        (
            "2026-09-08T19:08:18.479512+00:00",
            "2026-09-08T19:08:09.067089+00:00",
            "2026-09-08T19:08:09.102519+00:00",
            "2026-09-08T19:08:13.191731+00:00",
            "2026-09-08T19:08:10.082895+00:00",
        ),
        (
            "2026-09-08T19:10:56.879044+00:00",
            "2026-09-08T19:10:53.995884+00:00",
            "2026-09-08T19:10:54.034377+00:00",
            "2026-09-08T19:10:54.916659+00:00",
            "2026-09-08T19:10:50.579280+00:00",
        ),
    ],
)
def test_durable_progress_resolves_live_double_sampling_without_freshening_heartbeat(
    tmp_path,
    checked_at,
    event_at,
    received_at,
    processing_at,
    old_commit,
):
    with Database(f"sqlite:///{tmp_path / 'sample-race.db'}") as database:
        database.initialize()
        repository = ProductionRepository(database)
        now = datetime.fromisoformat(checked_at)
        heartbeat_at = now - timedelta(seconds=5)
        _running_recorder(
            repository,
            heartbeat_at,
            details={
                "last_committed_event_at": (now - timedelta(seconds=12)).isoformat(),
                "last_event_at": (now - timedelta(seconds=12)).isoformat(),
                "last_market_commit_at": old_commit,
            },
        )
        original_heartbeat = repository.latest_heartbeat("tradeagent-shadow-recorder")
        batch_id = _durable_batch(
            repository,
            datetime.fromisoformat(event_at),
            received_at=datetime.fromisoformat(received_at),
            processing_at=datetime.fromisoformat(processing_at),
        )
        monitor = MarketFeedStatusMonitor(
            repository,
            IntradayConfig(),
            instance_id="monitor",
            clock=lambda: now,
        )
        assert monitor.check() == "healthy"
        details = repository.latest_heartbeat("tradeagent-shadow-market-feed")[2]
        assert details["freshness_basis"] == "durable_market_batch"
        assert details["freshness_reason"] == "fresh_durable_market_batch"
        assert details["durable_batch"]["event_id"] == batch_id
        assert details["durable_batch"]["last_event_at"] == event_at
        assert details["durable_batch"]["processing_started_at"] == processing_at
        assert details["durable_batch"]["visible_in_database"] is True
        assert details["last_market_commit_at"] == old_commit
        assert details["recorder_heartbeat_at"] == heartbeat_at.isoformat()
        assert repository.latest_heartbeat("tradeagent-shadow-recorder") == original_heartbeat


@pytest.mark.parametrize("late_kind", ["minute_bar", "quote", "trade"])
def test_later_old_packet_batch_preserves_durable_exchange_progress_until_expiry(
    tmp_path,
    late_kind,
):
    with Database(f"sqlite:///{tmp_path / 'late-batch.db'}") as database:
        database.initialize()
        repository = ProductionRepository(database)
        now = datetime(2026, 9, 8, 14, 35, 0, 500000, tzinfo=UTC)
        _running_recorder(repository, now)
        original_heartbeat = repository.latest_heartbeat("tradeagent-shadow-recorder")
        quote_at = now - timedelta(milliseconds=400)
        quote_processing_at = now - timedelta(milliseconds=200)
        quote_batch = _durable_batch(repository, quote_at, processing_at=quote_processing_at)
        monitor = MarketFeedStatusMonitor(
            repository,
            IntradayConfig(),
            instance_id="monitor",
            clock=lambda: now,
        )
        assert monitor.check() == "healthy"

        old_at = datetime(2026, 9, 8, 14, 34, tzinfo=UTC)
        late_event = None
        if late_kind == "minute_bar":
            late_event = next(synthetic_bars(count=1)).model_copy(update={"timestamp": old_at})
        elif late_kind == "trade":
            late_event = MarketTrade(
                symbol="SPY",
                timestamp=old_at,
                price=Decimal(100),
                size=Decimal(10),
                trade_id="late-trade",
            )
        _durable_batch(
            repository,
            old_at,
            market_event=late_event,
            received_at=now - timedelta(milliseconds=150),
            processing_at=now - timedelta(milliseconds=100),
        )

        assert monitor.check() == "healthy"
        details = repository.latest_heartbeat("tradeagent-shadow-market-feed")[2]
        assert details["durable_batch"]["event_id"] == quote_batch
        assert details["durable_batch"]["last_event_at"] == quote_at.isoformat()
        assert details["durable_batch"]["processing_started_at"] == quote_processing_at.isoformat()
        assert details["event_age_seconds"] == 0.4
        assert repository.latest_heartbeat("tradeagent-shadow-recorder") == original_heartbeat
        assert (
            repository.market_data_counts()
            == {
                "minute_bar": (1, 1, 0),
                "quote": (0, 2, 0),
                "trade": (0, 1, 1),
            }[late_kind]
        )

        now += timedelta(seconds=9.6)
        _running_recorder(repository, now)
        assert monitor.check() == "healthy"
        now += timedelta(milliseconds=1)
        _running_recorder(repository, now)
        repository.append_event(
            "shadow_stream_status",
            {"state": "connected"},
            occurred_at=now,
            trace_id="status",
        )
        assert monitor.check() == "stale"
        details = repository.latest_heartbeat("tradeagent-shadow-market-feed")[2]
        assert details["durable_batch"]["event_id"] == quote_batch
        assert details["event_age_seconds"] == 10.001
        assert details["market_commit_age_seconds"] == 0
        assert details["freshness_reason"] == "durable_exchange_progress_stale"
        assert repository.get_control("kill_switch") is None


def test_durable_progress_compares_aware_instants_not_iso_strings(tmp_path):
    with Database(f"sqlite:///{tmp_path / 'offset-order.db'}") as database:
        database.initialize()
        repository = ProductionRepository(database)
        now = datetime(2026, 9, 8, 14, 35, 0, 500000, tzinfo=UTC)
        _running_recorder(repository, now)
        newest_event = datetime.fromisoformat("2026-09-08T10:34:59.900000-04:00")
        expected_batch = _durable_batch(
            repository,
            newest_event,
            processing_at=now - timedelta(milliseconds=200),
        )
        _durable_batch(
            repository,
            datetime.fromisoformat("2026-09-08T14:34:59.800000+00:00"),
            processing_at=now - timedelta(milliseconds=100),
        )
        monitor = MarketFeedStatusMonitor(
            repository,
            IntradayConfig(),
            instance_id="monitor",
            clock=lambda: now,
        )
        assert monitor.check() == "healthy"
        proof = repository.latest_heartbeat("tradeagent-shadow-market-feed")[2]["durable_batch"]
        assert proof["event_id"] == expected_batch
        assert proof["last_event_at"] == newest_event.astimezone(UTC).isoformat()
        assert proof["event_age_seconds"] == 0.6


def test_durable_progress_streams_entire_index_bounded_interval_without_row_cap(
    tmp_path,
    monkeypatch,
):
    from tradeagent import shadow_health

    with Database(f"sqlite:///{tmp_path / 'bounded-progress.db'}") as database:
        database.initialize()
        repository = ProductionRepository(database)
        now = datetime(2026, 9, 8, 14, 35, 0, 500000, tzinfo=UTC)
        _running_recorder(repository, now)
        expected_batch = _durable_batch(
            repository,
            now - timedelta(milliseconds=400),
            processing_at=now - timedelta(milliseconds=200),
        )
        later_at = now - timedelta(milliseconds=100)
        payload = {
            "instance_id": "worker-1",
            "received": 1,
            "inserted": 1,
            "duplicates": 0,
            "last_event_at": (now - timedelta(minutes=1)).isoformat(),
            "last_received_at": (now - timedelta(milliseconds=150)).isoformat(),
            "processing_started_at": later_at.isoformat(),
        }
        rows = [
            {
                "event_id": str(uuid4()),
                "event_type": "shadow_recorder_batch",
                "occurred_at": later_at,
                "recorded_at": later_at,
                "trace_id": "bounded-scan",
                "payload": dict(payload),
            }
            for _ in range(600)
        ]
        for outside_at in (now - timedelta(days=30), now + timedelta(days=30)):
            rows.append(
                {
                    **rows[0],
                    "event_id": str(uuid4()),
                    "occurred_at": outside_at,
                    "payload": {**payload, "processing_started_at": outside_at.isoformat()},
                }
            )
        rows.append(
            {
                **rows[0],
                "event_id": str(uuid4()),
                "payload": {**payload, "instance_id": "other-owner"},
            }
        )
        rows.append(
            {
                **rows[0],
                "event_id": str(uuid4()),
                "event_type": "shadow_stream_status",
            }
        )
        with database.begin() as connection:
            connection.execute(events.insert(), rows)
        queries = []

        @event.listens_for(database.engine, "before_cursor_execute")
        def observe_query(_connection, _cursor, statement, _parameters, context, _executemany):
            if "FROM events_v2" in statement:
                queries.append((statement, dict(context.execution_options)))

        inspected = 0
        validate_progress = shadow_health._valid_market_progress

        def inspect_progress(*args, **kwargs):
            nonlocal inspected
            inspected += 1
            return validate_progress(*args, **kwargs)

        monkeypatch.setattr(shadow_health, "_valid_market_progress", inspect_progress)
        snapshot = shadow_health.read_shadow_recorder_snapshot(
            repository,
            batch_since=now - timedelta(seconds=10),
            clock=lambda: now,
        )
        assert snapshot.batch["event_id"] == expected_batch
        assert inspected == 601
        assert len(queries) == 1
        statement, options = queries[0]
        assert "LIMIT" not in statement.upper()
        assert "events_v2.occurred_at >=" in statement
        assert "events_v2.occurred_at <=" in statement
        assert options["stream_results"] is True
        assert options["yield_per"] == options["max_row_buffer"] == 1


@pytest.mark.parametrize(
    "fault",
    [
        "missing_lease",
        "foreign_lease",
        "expired_lease",
        "future_lease",
        "expired_heartbeat",
        "future_heartbeat",
    ],
)
def test_durable_progress_never_rescues_invalid_owner_or_liveness(tmp_path, fault):
    with Database(f"sqlite:///{tmp_path / 'ownership.db'}") as database:
        database.initialize()
        repository = ProductionRepository(database)
        now = datetime(2026, 9, 8, 19, 10, 56, tzinfo=UTC)
        config = IntradayConfig()
        heartbeat_at = now
        if fault == "expired_heartbeat":
            heartbeat_at -= timedelta(seconds=config.heartbeat_max_age_seconds + 1)
        if fault == "future_heartbeat":
            heartbeat_at += timedelta(seconds=1)
        _running_recorder(repository, heartbeat_at)
        _durable_batch(repository, now - timedelta(seconds=1))
        if fault in {"missing_lease", "foreign_lease"}:
            repository.release_worker_lock("tradeagent-shadow-recorder", "worker-1")
        if fault == "foreign_lease":
            repository.acquire_worker_lock("tradeagent-shadow-recorder", "other")
            repository.refresh_worker_lock("tradeagent-shadow-recorder", "other", observed_at=now)
        if fault in {"expired_lease", "future_lease"}:
            lease_at = (
                now - timedelta(seconds=config.heartbeat_max_age_seconds * 2 + 1)
                if fault == "expired_lease"
                else now + timedelta(seconds=1)
            )
            repository.refresh_worker_lock(
                "tradeagent-shadow-recorder",
                "worker-1",
                observed_at=lease_at,
            )
        monitor = MarketFeedStatusMonitor(
            repository,
            config,
            instance_id="monitor",
            clock=lambda: now,
        )
        assert monitor.check() == "stale"
        assert repository.latest_heartbeat("tradeagent-shadow-market-feed")[2]["healthy"] is False


@pytest.mark.parametrize(
    "fault",
    [
        "old_exchange",
        "future_exchange",
        "future_processing",
        "missing_timestamp",
        "naive_timestamp",
        "malformed_timestamp",
        "timestamp_underflow",
        "timestamp_overflow",
        "receipt_after_processing",
        "processing_mismatch",
        "zero_market_count",
        "invalid_count",
        "negative_count",
        "mismatched_count",
        "bool_inserted",
        "bool_duplicates",
        "foreign_batch",
        "untagged_batch",
        "status_only",
        "expired_batch",
    ],
)
@pytest.mark.parametrize("valid_previous", [False, True])
def test_recent_status_or_invalid_batch_never_becomes_durable_market_proof(
    tmp_path,
    fault,
    valid_previous,
):
    with Database(f"sqlite:///{tmp_path / 'invalid-proof.db'}") as database:
        database.initialize()
        repository = ProductionRepository(database)
        now = datetime(2026, 9, 8, 19, 10, 56, tzinfo=UTC)
        _running_recorder(repository, now)
        valid_batch = (
            _durable_batch(repository, now - timedelta(seconds=3)) if valid_previous else None
        )
        payload = {
            "instance_id": "worker-1",
            "received": 1,
            "inserted": 1,
            "duplicates": 0,
            "last_event_at": (now - timedelta(seconds=2)).isoformat(),
            "last_received_at": (now - timedelta(seconds=1)).isoformat(),
            "processing_started_at": now.isoformat(),
        }
        row_at = now
        kind = "shadow_recorder_batch"
        if fault == "old_exchange":
            payload["last_event_at"] = (now - timedelta(seconds=10.001)).isoformat()
        if fault == "future_exchange":
            payload["last_event_at"] = (now + timedelta(seconds=1)).isoformat()
        if fault == "future_processing":
            row_at = now + timedelta(seconds=1)
            payload["processing_started_at"] = row_at.isoformat()
        if fault == "missing_timestamp":
            del payload["last_event_at"]
        if fault == "naive_timestamp":
            payload["last_event_at"] = "2026-09-08T19:10:55"
        if fault == "malformed_timestamp":
            payload["last_event_at"] = "not-a-timestamp"
        if fault == "timestamp_underflow":
            payload["last_event_at"] = "0001-01-01T00:00:00+01:00"
        if fault == "timestamp_overflow":
            payload["last_event_at"] = "9999-12-31T23:59:59-01:00"
        if fault == "receipt_after_processing":
            payload["last_received_at"] = (now + timedelta(seconds=1)).isoformat()
        if fault == "processing_mismatch":
            payload["processing_started_at"] = (now - timedelta(milliseconds=100)).isoformat()
        if fault == "zero_market_count":
            payload.update(received=0, inserted=0)
        if fault == "invalid_count":
            payload["received"] = True
        if fault == "negative_count":
            payload.update(inserted=-1, duplicates=2)
        if fault == "mismatched_count":
            payload["received"] = 2
        if fault == "bool_inserted":
            payload["inserted"] = True
        if fault == "bool_duplicates":
            payload["duplicates"] = False
        if fault == "foreign_batch":
            payload["instance_id"] = "previous-recorder"
        if fault == "untagged_batch":
            del payload["instance_id"]
        if fault == "status_only":
            kind = "shadow_stream_status"
        if fault == "expired_batch":
            row_at = now - timedelta(seconds=11)
            payload["processing_started_at"] = row_at.isoformat()
        repository.append_event(kind, payload, occurred_at=row_at, trace_id="proof-test")
        monitor = MarketFeedStatusMonitor(
            repository,
            IntradayConfig(),
            instance_id="monitor",
            clock=lambda: now,
        )
        assert monitor.check() == ("healthy" if valid_previous else "stale")
        details = repository.latest_heartbeat("tradeagent-shadow-market-feed")[2]
        assert details["healthy"] is valid_previous
        if valid_previous:
            assert details["durable_batch"]["event_id"] == valid_batch


def test_monitor_evaluates_clock_after_snapshot_read(tmp_path, monkeypatch):
    from tradeagent.shadow_health import read_shadow_recorder_snapshot

    with Database(f"sqlite:///{tmp_path / 'read-clock.db'}") as database:
        database.initialize()
        repository = ProductionRepository(database)
        now = datetime(2026, 9, 8, 19, 10, 56, tzinfo=UTC)
        clock = [now]
        _running_recorder(repository, now + timedelta(milliseconds=500))
        _durable_batch(repository, now - timedelta(seconds=1))

        def delayed_snapshot(*args, **kwargs):
            result = read_shadow_recorder_snapshot(*args, **kwargs)
            clock[0] = now + timedelta(seconds=1)
            return result

        monkeypatch.setattr("tradeagent.runtime.read_shadow_recorder_snapshot", delayed_snapshot)
        monitor = MarketFeedStatusMonitor(
            repository,
            IntradayConfig(),
            instance_id="monitor",
            clock=lambda: clock[0],
        )
        assert monitor.check() == "healthy"
        details = repository.latest_heartbeat("tradeagent-shadow-market-feed")[2]
        assert details["heartbeat_age_seconds"] == 0.5


def test_foreign_latest_batch_does_not_mask_current_owner_durable_progress(tmp_path):
    with Database(f"sqlite:///{tmp_path / 'other-owner.db'}") as database:
        database.initialize()
        repository = ProductionRepository(database)
        now = datetime(2026, 9, 8, 19, 10, 56, tzinfo=UTC)
        _running_recorder(repository, now)
        own_batch = _durable_batch(repository, now - timedelta(seconds=2))
        _durable_batch(repository, now - timedelta(seconds=1), owner="previous-recorder")
        monitor = MarketFeedStatusMonitor(
            repository,
            IntradayConfig(),
            instance_id="monitor",
            clock=lambda: now,
        )
        assert monitor.check() == "healthy"
        details = repository.latest_heartbeat("tradeagent-shadow-market-feed")[2]
        assert details["durable_batch"]["event_id"] == own_batch
        assert details["durable_batch"]["instance_id"] == "worker-1"


def test_shadow_runtime_records_stale_packets_and_never_runs_broker_scheduler(
    tmp_path: Path,
    monkeypatch,
) -> None:
    now = datetime(2026, 9, 8, 14, 30, tzinfo=UTC)
    database = Database(f"sqlite:///{tmp_path / 'integrated-shadow.db'}")
    database.initialize()
    repository = ProductionRepository(database)
    repository.set_control("kill_switch", "operator-paused")
    delivered = []

    class Socket:
        def __init__(self):
            self.frames = [
                [{"T": "success", "msg": "connected"}],
                [{"T": "success", "msg": "authenticated"}],
                [{"T": "subscription", "bars": ["SPY"], "quotes": ["SPY"], "trades": ["SPY"]}],
                [
                    {
                        "T": "q",
                        "S": "SPY",
                        "t": (now - timedelta(seconds=11)).isoformat(),
                        "bp": 100,
                        "ap": 101,
                        "bs": 10,
                        "as": 10,
                    }
                ],
                [
                    {
                        "T": "q",
                        "S": "SPY",
                        "t": now.isoformat(),
                        "bp": 100,
                        "ap": 101,
                        "bs": 10,
                        "as": 10,
                    }
                ],
            ]

        async def recv(self, decode=None):
            if self.frames:
                frame = self.frames.pop(0)
                delivered.extend(frame)
                return json.dumps(frame)
            await asyncio.Event().wait()

        async def send(self, message):
            return None

    class Connection:
        async def __aenter__(self):
            return Socket()

        async def __aexit__(self, *args):
            return False

    class ForbiddenReconciler:
        def reconcile(self, **kwargs):
            pytest.fail("shadow must not run broker reconciliation or change its global control")

    class ForbiddenScheduler:
        async def run(self, stop_event):
            pytest.fail("shadow must not run the broker/global watchdog scheduler")

    monkeypatch.setattr("tradeagent.alpaca_stream.connect", lambda *args, **kwargs: Connection())
    worker = AutonomousPaperWorker(
        AppConfig(),
        repository,
        ForbiddenReconciler(),
        ShadowAuditProcessor(repository),
        mode=WorkerMode.SHADOW,
        instance_id="integration-shadow",
        strategy_authorized=lambda: False,
        clock=lambda: now,
        recorder_settings=ShadowRecorderSettings(
            flush_interval_seconds=0.01,
            heartbeat_interval_seconds=0.01,
        ),
    )
    stream = AlpacaMarketStream(
        AlpacaStreamSettings(key_id=SecretStr("test-key"), secret_key=SecretStr("test-secret")),
        clock=lambda: now,
    )

    async def run():
        runtime = asyncio.create_task(
            run_shadow_runtime(
                stream,
                worker,
                ForbiddenScheduler(),
                symbols=("SPY",),
                feed_monitor=MarketFeedStatusMonitor(
                    repository,
                    IntradayConfig(),
                    instance_id="final-health",
                    clock=lambda: now,
                ),
            )
        )
        for _ in range(100):
            heartbeat = await asyncio.to_thread(
                repository.latest_heartbeat, "tradeagent-shadow-recorder"
            )
            if heartbeat and heartbeat[2]["committed"] == 2:
                break
            await asyncio.sleep(0.01)
        assert heartbeat is not None
        assert heartbeat[2]["committed"] == 2
        assert heartbeat[2]["late_events"] == 1
        assert heartbeat[2]["healthy"] is True
        runtime.cancel()
        with pytest.raises(asyncio.CancelledError):
            await runtime

    asyncio.run(run())
    assert len(delivered) == 5
    assert repository.get_control("kill_switch") == "operator-paused"
    assert repository.market_data_counts() == (0, 2, 0)
    assert repository.latest_event_payload("shadow_market_quote") is None
    assert repository.latest_event_payload("shadow_recorder_batch")["received"] == 2
    final_feed = repository.latest_heartbeat("tradeagent-shadow-market-feed")[2]
    assert final_feed["state"] == "stale"
    assert final_feed["healthy"] is False
    assert final_feed["recorder_alive"] is False
    database.dispose()


def test_local_websocket_connection_limit_recovery_and_persistence_burst(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Exercise real websocket transport and database commits without contacting Alpaca."""
    now = datetime(2026, 9, 8, 14, 30, tzinfo=UTC)
    database = Database(f"sqlite:///{tmp_path / 'local-websocket.db'}")
    database.initialize()
    repository = ProductionRepository(database)
    repository.set_control("kill_switch", "inactive")
    connections = []
    messages = []

    async def serve_feed(socket):
        connections.append(socket)
        if len(connections) == 1:
            await socket.send(
                json.dumps(
                    [
                        {
                            "T": "error",
                            "code": 406,
                            "msg": "connection limit exceeded",
                        }
                    ]
                )
            )
            await socket.close()
            return
        await socket.send(json.dumps([{"T": "success", "msg": "connected"}]))
        messages.append(json.loads(await socket.recv()))
        await socket.send(json.dumps([{"T": "success", "msg": "authenticated"}]))
        subscription = json.loads(await socket.recv())
        messages.append(subscription)
        await socket.send(
            json.dumps(
                [
                    {
                        "T": "subscription",
                        **{
                            channel: subscription[channel]
                            for channel in ("bars", "quotes", "trades")
                        },
                    }
                ]
            )
        )
        for batch in range(10):
            await socket.send(
                json.dumps(
                    [
                        {
                            "T": "q",
                            "S": "SPY",
                            "t": (
                                now - timedelta(seconds=1) + timedelta(microseconds=index)
                            ).isoformat(),
                            "bp": 100,
                            "ap": 101,
                            "bs": 10,
                            "as": 20,
                        }
                        for index in range(batch * 100, (batch + 1) * 100)
                    ]
                )
            )
        await socket.wait_closed()

    class ForbiddenBroker:
        def reconcile(self, **kwargs):
            pytest.fail("recording may not run broker reconciliation")

        async def run(self, stop_event):
            pytest.fail("recording may not start the trading scheduler")

    async def run():
        async with serve(serve_feed, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            monkeypatch.setattr(
                "tradeagent.alpaca_stream.connect",
                lambda url, **kwargs: websocket_connect(f"ws://127.0.0.1:{port}", **kwargs),
            )
            settings = AlpacaStreamSettings(
                key_id=SecretStr("offline-key"),
                secret_key=SecretStr("offline-secret"),
                reconnect_initial_seconds=0.001,
                reconnect_max_seconds=0.002,
            )
            worker = AutonomousPaperWorker(
                AppConfig(),
                repository,
                ForbiddenBroker(),
                ShadowAuditProcessor(repository),
                mode=WorkerMode.SHADOW,
                instance_id="local-wire-test",
                strategy_authorized=lambda: False,
                clock=lambda: now,
                recorder_settings=ShadowRecorderSettings(
                    batch_size=100,
                    flush_interval_seconds=0.01,
                    heartbeat_interval_seconds=0.01,
                ),
            )
            runtime = asyncio.create_task(
                run_shadow_runtime(
                    AlpacaMarketStream(settings, clock=lambda: now),
                    worker,
                    ForbiddenBroker(),
                    symbols=("SPY",),
                )
            )
            try:
                for _ in range(200):
                    heartbeat = await asyncio.to_thread(
                        repository.latest_heartbeat, "tradeagent-shadow-recorder"
                    )
                    if heartbeat and heartbeat[2]["committed"] == 1000:
                        break
                    if runtime.done():
                        await runtime
                    await asyncio.sleep(0.02)
                assert heartbeat is not None
                details = heartbeat[2]
                assert details["committed"] == details["received"] == 1000
                assert details["dropped_events"] == details["queue_depth"] == 0
                assert details["gaps"] == 1
                assert details["stream"]["reconnects"] == 1
                assert details["stream"]["authenticated"] is True
                assert details["stream"]["subscribed"] is True
                assert details["healthy"] is True
                assert details["last_committed_received_at"] == now.isoformat()
                assert 0.99 <= details["receive_lag_seconds"] <= 1
            finally:
                runtime.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await runtime

    asyncio.run(run())
    assert len(connections) == 2
    assert [message["action"] for message in messages] == ["auth", "subscribe"]
    assert repository.market_data_counts() == (0, 1000, 0)
    assert repository.get_control("kill_switch") == "inactive"
    database.dispose()
