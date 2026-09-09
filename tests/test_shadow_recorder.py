from __future__ import annotations

import asyncio
import threading
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from itertools import pairwise
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import event, select
from sqlalchemy.exc import OperationalError

from tradeagent.alpaca_stream import MarketQuote, MarketTrade, ReceivedStreamEvent
from tradeagent.domain import MarketBar
from tradeagent.persistence import (
    Database,
    ProductionRepository,
    event_reporting_metadata,
    events,
    market_bars,
    market_quotes,
    market_trades,
)
from tradeagent.reporting_reads import REPORTING_PROJECTION_VERSION, event_reporting_projection
from tradeagent.shadow_recorder import (
    ShadowBatchRecorder,
    ShadowRecorderSettings,
    append_reporting_metadata,
    persist_shadow_batch,
)

NOW = datetime(2026, 9, 8, 14, 30, tzinfo=UTC)


def _receipt(index: int, received_at: datetime | None = None) -> ReceivedStreamEvent:
    event_at = NOW + timedelta(microseconds=index)
    return ReceivedStreamEvent(
        MarketQuote(
            symbol="SPY",
            timestamp=event_at,
            bid_price=Decimal("100"),
            ask_price=Decimal("101"),
            bid_size=Decimal(10),
            ask_size=Decimal(20),
        ),
        received_at or event_at + timedelta(seconds=11),
    )


@pytest.fixture
def repository(tmp_path: Path):
    with Database(f"sqlite:///{tmp_path / 'batches.db'}") as database:
        database.initialize()
        yield ProductionRepository(database)


def test_bulk_rows_preserve_exchange_receipt_processing_and_first_append(repository) -> None:
    quote = _receipt(0)
    trade = ReceivedStreamEvent(
        MarketTrade(
            symbol="SPY",
            timestamp=NOW,
            price=Decimal("100"),
            size=Decimal("10"),
            trade_id="IEX-42",
            exchange="V",
            conditions=("@",),
        ),
        NOW + timedelta(seconds=12),
    )
    bar = ReceivedStreamEvent(
        MarketBar(
            symbol="SPY",
            timestamp=NOW,
            open=Decimal(100),
            high=Decimal(101),
            low=Decimal(99),
            close=Decimal(100),
            volume=Decimal(500),
        ),
        NOW + timedelta(seconds=61),
    )
    receipts = [quote, trade, bar]
    batch_id = str(uuid4())
    result = persist_shadow_batch(
        repository, receipts, [], batch_id, clock=lambda: NOW + timedelta(seconds=65)
    )
    assert result.inserted == 3
    replay = [
        ReceivedStreamEvent(receipt.event, NOW + timedelta(minutes=2)) for receipt in receipts
    ]
    repeated = persist_shadow_batch(repository, receipts, [], batch_id)
    assert repeated.inserted == 3
    assert repository.event_count() == 1
    result = persist_shadow_batch(
        repository, replay, [], str(uuid4()), clock=lambda: NOW + timedelta(minutes=3)
    )
    assert result.inserted == 0
    assert result.duplicates == 3
    assert repository.market_data_counts() == (1, 1, 1)
    assert repository.event_count() == 2
    with repository._database.begin() as connection:
        for table, receipt in zip(
            (market_quotes, market_trades, market_bars), receipts, strict=True
        ):
            row = connection.execute(select(table)).mappings().one()
            assert row["event_at"].replace(tzinfo=UTC) == NOW
            assert row["received_at"].replace(tzinfo=UTC) == receipt.received_at
            assert row["processed_at"].replace(tzinfo=UTC) == NOW + timedelta(seconds=65)
    payload = repository.latest_event_payload("shadow_recorder_batch")
    assert payload["received"] == 3
    assert payload["execution_enabled"] is False


def test_parameter_batches_bound_sql_pages_and_count_conflicts_exactly(repository):
    original_receipt = _receipt(0, NOW + timedelta(milliseconds=35))
    persist_shadow_batch(
        repository,
        [original_receipt],
        [],
        str(uuid4()),
        clock=lambda: NOW + timedelta(seconds=1),
    )
    receipts = [_receipt(index) for index in range(1001)]
    receipts.extend(
        ReceivedStreamEvent(receipts[index].event, NOW + timedelta(seconds=30))
        for index in (500, 700)
    )
    templates = []
    quote_pages = []

    @event.listens_for(repository._database.engine, "before_execute")
    def observe_template(_connection, statement, parameters, _params, options):
        if getattr(statement, "is_insert", False) and statement.table in (market_quotes, events):
            templates.append(
                (
                    statement.table.name,
                    len(parameters) or int(bool(_params)),
                    statement._multi_values,
                    options.get("insertmanyvalues_page_size"),
                )
            )

    @event.listens_for(repository._database.engine, "before_cursor_execute")
    def observe_page(_connection, _cursor, statement, parameters, _context, _executemany):
        if statement.startswith(f"INSERT INTO {market_quotes.name} "):
            assert "ON CONFLICT DO NOTHING RETURNING" in statement
            quote_pages.append(len(parameters) // len(market_quotes.columns))

    batch_id = str(uuid4())
    result = persist_shadow_batch(
        repository,
        receipts,
        [],
        batch_id,
        clock=lambda: NOW + timedelta(seconds=31),
        instance_id="paged-recorder",
    )
    assert (result.inserted, result.duplicates) == (1000, 3)
    assert templates == [
        (market_quotes.name, 1003, (), 500),
        (events.name, 1, (), 500),
    ]
    assert quote_pages == [500, 500, 3]
    assert repository.market_data_counts() == (0, 1001, 0)
    with repository._database.begin() as connection:
        for index, expected in ((0, original_receipt), (500, receipts[500])):
            received_at = connection.scalar(
                select(market_quotes.c.received_at).where(
                    market_quotes.c.event_at == receipts[index].event.timestamp
                )
            )
            assert received_at.replace(tzinfo=UTC) == expected.received_at
        payload = connection.scalar(select(events.c.payload).where(events.c.event_id == batch_id))
        assert payload["received"] == 1003
        assert payload["inserted"] == 1000
        assert payload["duplicates"] == 3
        assert payload["instance_id"] == "paged-recorder"
        projection = connection.scalar(
            select(event_reporting_metadata.c.payload).where(
                event_reporting_metadata.c.event_id == batch_id
            )
        )
        assert projection == event_reporting_projection("shadow_recorder_batch", payload)
    retry = persist_shadow_batch(repository, receipts, [], batch_id)
    assert retry == result
    assert quote_pages == [500, 500, 3]


@pytest.mark.parametrize("failure_table", [market_quotes.name, events.name])
def test_partial_parameter_page_failure_rolls_back_and_retries_atomically(
    repository,
    failure_table,
):
    receipts = [_receipt(index) for index in range(1001)]
    notices = [_notice(str(uuid4()), {"state": "subscribed"}) for _ in range(500)]
    batch_id = str(uuid4())
    pages = 0

    def fail_second_page(_connection, _cursor, statement, _parameters, _context, _executemany):
        nonlocal pages
        if statement.startswith(f"INSERT INTO {failure_table} "):
            pages += 1
            if pages == 2:
                raise OperationalError("insert page", {}, RuntimeError("lost connection"))

    event.listen(repository._database.engine, "before_cursor_execute", fail_second_page)
    try:
        with pytest.raises(OperationalError, match="lost connection"):
            persist_shadow_batch(repository, receipts, notices, batch_id)
    finally:
        event.remove(repository._database.engine, "before_cursor_execute", fail_second_page)
    assert pages == 2
    assert repository.market_data_counts() == (0, 0, 0)
    assert repository.event_count() == 0
    with repository._database.begin() as connection:
        assert connection.execute(select(event_reporting_metadata)).all() == []

    result = persist_shadow_batch(
        repository,
        receipts,
        notices,
        batch_id,
        clock=lambda: NOW + timedelta(seconds=12),
    )
    assert (result.inserted, result.duplicates) == (1001, 0)
    assert repository.market_data_counts() == (0, 1001, 0)
    assert repository.event_count() == 501
    with repository._database.begin() as connection:
        assert len(connection.scalars(select(event_reporting_metadata.c.event_id)).all()) == 501
    retry = persist_shadow_batch(repository, receipts, notices, batch_id)
    assert retry == result
    assert repository.market_data_counts() == (0, 1001, 0)
    assert repository.event_count() == 501


def test_slow_database_does_not_block_receipt_and_overflow_is_durable(
    repository, monkeypatch
) -> None:
    entered = threading.Event()
    release = threading.Event()
    original = persist_shadow_batch

    def blocked(*args, **kwargs):
        entered.set()
        assert release.wait(5)
        return original(*args, **kwargs)

    monkeypatch.setattr("tradeagent.shadow_recorder.persist_shadow_batch", blocked)
    recorder = ShadowBatchRecorder(
        repository,
        settings=ShadowRecorderSettings(
            queue_capacity=4, batch_size=2, flush_interval_seconds=0.005
        ),
        clock=lambda: NOW + timedelta(seconds=20),
    )

    async def run():
        stop = asyncio.Event()
        recorder.offer(_receipt(0))
        recorder.offer(_receipt(1))
        writer = asyncio.create_task(recorder.run(stop))
        while not entered.is_set():
            await asyncio.sleep(0.002)
        for index in range(2, 12):
            recorder.offer(_receipt(index))
        # This executes while a real synchronous database writer is blocked in another thread.
        await asyncio.sleep(0.01)
        assert recorder.health()["in_flight"] == 2
        assert recorder.health()["queue_depth"] == 4
        assert recorder.health()["dropped_events"] == 6
        release.set()
        stop.set()
        await asyncio.wait_for(writer, 5)

    asyncio.run(run())
    assert recorder.received == 12
    assert recorder.committed == 6
    assert recorder.dropped == 6
    assert recorder.committed + recorder.dropped == recorder.received
    assert repository.market_data_counts() == (0, 6, 0)
    gap = repository.latest_event_payload("shadow_recorder_gap")
    assert gap["reason"] == "queue_overflow"
    assert gap["dropped_events"] == 6
    assert gap["first_event_at"] == _receipt(6).event.timestamp.isoformat()
    assert gap["last_event_at"] == _receipt(11).event.timestamp.isoformat()
    assert recorder.health()["queue_depth"] == 0
    assert recorder.health()["commit_lag_seconds"] > 8
    assert recorder.health()["receive_lag_seconds"] == 11


def test_steady_feed_commits_before_feed_ends_and_flushes_partial_batch(
    repository, monkeypatch
) -> None:
    original = persist_shadow_batch
    batch_sizes = []

    def slow_write(repository, receipts, *args, **kwargs):
        time.sleep(0.015)
        batch_sizes.append(len(receipts))
        return original(repository, receipts, *args, **kwargs)

    monkeypatch.setattr("tradeagent.shadow_recorder.persist_shadow_batch", slow_write)
    recorder = ShadowBatchRecorder(
        repository,
        settings=ShadowRecorderSettings(
            queue_capacity=512,
            batch_size=32,
            flush_interval_seconds=0.005,
        ),
    )

    async def run():
        stop = asyncio.Event()
        writer = asyncio.create_task(recorder.run(stop))
        progress = []
        depths = []
        for index in range(101):
            recorder.offer(_receipt(index, datetime.now(UTC)))
            await asyncio.sleep(0.003)
            if index % 20 == 0:
                progress.append(await asyncio.to_thread(repository.market_data_counts))
                depths.append(recorder.health()["queue_depth"])
        stop.set()
        await asyncio.wait_for(writer, 5)
        assert any(0 < counts[1] < 101 for counts in progress)
        assert max(depths) < 512

    asyncio.run(run())
    assert recorder.committed == 101
    assert recorder.dropped == 0
    assert recorder.inserted == 101
    assert repository.market_data_counts() == (0, 101, 0)
    assert all(0 < size <= 32 for size in batch_sizes)
    assert len(batch_sizes) < 60
    assert recorder.health()["last_commit_at"] is not None
    assert recorder.health()["batch_write_seconds"] >= 0.015


def test_transient_database_failure_retries_same_receipts_and_persists_status(
    repository, monkeypatch
):
    original = persist_shadow_batch
    attempts = []

    def fail_once(*args, **kwargs):
        attempts.append(args[3])
        if len(attempts) == 1:
            raise OperationalError("insert", {}, RuntimeError("connection reset"))
        return original(*args, **kwargs)

    monkeypatch.setattr("tradeagent.shadow_recorder.persist_shadow_batch", fail_once)
    recorder = ShadowBatchRecorder(
        repository,
        settings=ShadowRecorderSettings(
            batch_size=10,
            flush_interval_seconds=0.001,
            retry_initial_seconds=0.001,
        ),
    )
    recorder.stream_status({"state": "reconnecting", "code": 406, "gap": True})
    recorder.offer(_receipt(0))
    recorder.offer(_receipt(1))

    async def run():
        stop = asyncio.Event()
        stop.set()
        await recorder.run(stop)

    asyncio.run(run())
    assert len(attempts) == 2
    assert attempts[0] == attempts[1]
    assert recorder.committed == 2
    assert recorder.persistence_error is None
    assert recorder.gaps == 1
    assert repository.latest_event_payload("shadow_stream_status")["code"] == 406
    assert repository.market_data_counts() == (0, 2, 0)


def test_market_sized_burst_uses_bulk_transactions_and_drains(repository, monkeypatch) -> None:
    original = persist_shadow_batch
    batch_sizes = []

    def measured_write(repository, receipts, *args, **kwargs):
        batch_sizes.append(len(receipts))
        return original(repository, receipts, *args, **kwargs)

    monkeypatch.setattr("tradeagent.shadow_recorder.persist_shadow_batch", measured_write)
    recorder = ShadowBatchRecorder(
        repository,
        settings=ShadowRecorderSettings(
            queue_capacity=8000,
            batch_size=500,
            flush_interval_seconds=0.01,
        ),
    )

    async def run():
        stop = asyncio.Event()
        writer = asyncio.create_task(recorder.run(stop))
        for index in range(5003):
            assert recorder.offer(_receipt(index, datetime.now(UTC)))
            if index % 500 == 0:
                await asyncio.sleep(0)
        stop.set()
        await asyncio.wait_for(writer, 20)

    asyncio.run(run())
    assert recorder.received == recorder.committed == recorder.inserted == 5003
    assert recorder.dropped == 0
    assert len(batch_sizes) == 11
    assert max(batch_sizes) == 500
    assert repository.market_data_counts() == (0, 5003, 0)
    assert repository.event_count() == 11


def test_lost_commit_ack_retry_reports_actual_insert_progress(repository, monkeypatch) -> None:
    original = persist_shadow_batch
    attempts = 0
    metadata_calls = []

    def record_metadata(connection, event_id, event_type, payload):
        metadata_calls.append(event_id)
        append_reporting_metadata(connection, event_id, event_type, payload)

    def ambiguous_commit(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        result = original(*args, **kwargs)
        if attempts == 1:
            raise OperationalError("commit", {}, RuntimeError("lost acknowledgement"))
        return result

    monkeypatch.setattr("tradeagent.shadow_recorder.persist_shadow_batch", ambiguous_commit)
    monkeypatch.setattr("tradeagent.shadow_recorder.append_reporting_metadata", record_metadata)
    recorder = ShadowBatchRecorder(
        repository,
        settings=ShadowRecorderSettings(retry_initial_seconds=0.001),
    )
    recorder.offer(_receipt(0))

    async def run():
        stop = asyncio.Event()
        stop.set()
        await recorder.run(stop)

    asyncio.run(run())
    assert attempts == 2
    assert len(metadata_calls) == 1
    assert recorder.committed == recorder.inserted == 1
    assert recorder.duplicates == 0
    assert repository.market_data_counts() == (0, 1, 0)
    assert repository.event_count() == 1
    with repository._database.begin() as connection:
        metadata = connection.execute(select(event_reporting_metadata)).mappings().one()
    assert metadata["event_id"] == metadata_calls[0]
    assert metadata["projection_version"] == REPORTING_PROJECTION_VERSION


def test_status_commits_do_not_refresh_market_progress_or_relabel_old_exchange_data(repository):
    now = NOW + timedelta(seconds=12)
    recorder = ShadowBatchRecorder(repository, clock=lambda: now)
    recorder.offer(_receipt(0, NOW + timedelta(seconds=11)))

    async def flush():
        stop = asyncio.Event()
        stop.set()
        await recorder.run(stop)

    asyncio.run(flush())
    first = recorder.health()
    assert first["last_committed_event_at"] == NOW.isoformat()
    assert first["last_committed_received_at"] == (NOW + timedelta(seconds=11)).isoformat()
    assert first["commit_lag_seconds"] == 1
    assert first["exchange_to_commit_lag_seconds"] == 12
    assert first["last_market_commit_at"] == now.isoformat()
    now += timedelta(seconds=20)
    recorder.notice("shadow_stream_status", {"state": "subscribed"})
    asyncio.run(flush())
    later = recorder.health()
    assert later["last_commit_at"] == now.isoformat()
    assert later["last_market_commit_at"] == first["last_market_commit_at"]
    assert later["last_committed_event_at"] == first["last_committed_event_at"]
    assert later["committed_event_age_seconds"] == 32
    assert later["market_commit_age_seconds"] == 20


def test_minute_bar_does_not_regress_quote_exchange_progress_and_keeps_its_true_lag(repository):
    now = NOW + timedelta(milliseconds=100)
    recorder = ShadowBatchRecorder(repository, clock=lambda: now)
    recorder.offer(_receipt(0, NOW))
    bar = MarketBar(
        symbol="SPY",
        timestamp=NOW - timedelta(minutes=1),
        open=Decimal(100),
        high=Decimal(101),
        low=Decimal(99),
        close=Decimal(100),
        volume=Decimal(1000),
    )
    recorder.offer(ReceivedStreamEvent(bar, now))

    async def flush():
        stop = asyncio.Event()
        stop.set()
        await recorder.run(stop)

    asyncio.run(flush())
    health = recorder.health()
    assert health["last_event_at"] == NOW.isoformat()
    assert health["last_committed_event_at"] == NOW.isoformat()
    assert health["last_received_event_at"] == bar.timestamp.isoformat()
    assert health["receive_lag_seconds"] == 60.1
    assert health["committed_event_age_seconds"] == 0.1
    assert repository.market_data_counts() == (1, 1, 0)


def test_sustained_live_sized_frames_progress_with_delayed_database(repository, monkeypatch):
    original = persist_shadow_batch
    batch_sizes = []

    def delayed_write(repository, receipts, *args, **kwargs):
        time.sleep(0.035)
        batch_sizes.append(len(receipts))
        return original(repository, receipts, *args, **kwargs)

    monkeypatch.setattr("tradeagent.shadow_recorder.persist_shadow_batch", delayed_write)
    recorder = ShadowBatchRecorder(
        repository,
        settings=ShadowRecorderSettings(
            queue_capacity=1000,
            batch_size=200,
            flush_interval_seconds=0.025,
        ),
    )

    async def run():
        stop = asyncio.Event()
        writer = asyncio.create_task(recorder.run(stop))
        progress = []
        depths = []
        try:
            for frame in range(20):
                received_at = datetime.now(UTC)
                for item in range(50):
                    base = _receipt(frame * 50 + item, received_at)
                    event_at = (
                        received_at - timedelta(milliseconds=35) + timedelta(microseconds=item)
                    )
                    assert recorder.offer(
                        ReceivedStreamEvent(
                            base.event.model_copy(update={"timestamp": event_at}),
                            received_at,
                        )
                    )
                depths.append(recorder.health()["queue_depth"])
                await asyncio.sleep(0.1)
                if frame % 4 == 0:
                    progress.append((await asyncio.to_thread(repository.market_data_counts))[1])
        finally:
            stop.set()
            await asyncio.wait_for(writer, 5)
        assert all(a < b for a, b in pairwise(progress))
        assert max(depths) < recorder.settings.queue_capacity

    asyncio.run(run())
    assert recorder.received == recorder.committed == recorder.inserted == 1000
    assert recorder.dropped == 0
    assert repository.market_data_counts() == (0, 1000, 0)
    assert 1 < len(batch_sizes) <= 40
    health = recorder.health()
    assert health["queue_depth"] == health["in_flight"] == 0
    assert health["batch_write_seconds"] >= 0.035
    assert health["commit_lag_seconds"] >= 0.035


def _notice(event_id: str, payload: dict) -> dict:
    return {
        "event_id": event_id,
        "event_type": "shadow_stream_status",
        "occurred_at": NOW,
        "recorded_at": NOW,
        "trace_id": f"test:{event_id}",
        "payload": payload,
    }


def test_batch_metadata_projects_only_new_ids_and_preserves_first_original_body(repository):
    existing_id, new_id, batch_id = (str(uuid4()) for _ in range(3))
    original = {"session_date": "2026-09-08"}
    first_new = {"session_date": "2026-09-09"}
    conflicting_retry = {"session_date": "2026-09-10"}
    with repository._database.begin() as connection:
        # Simulate an archived original that has not yet received the required backfill.
        connection.execute(events.insert().values(_notice(existing_id, original)))
    persist_shadow_batch(
        repository,
        [_receipt(0)],
        [
            _notice(existing_id, conflicting_retry),
            _notice(new_id, first_new),
            _notice(new_id, conflicting_retry),
        ],
        batch_id,
    )
    with repository._database.begin() as connection:
        originals = {
            row["event_id"]: row["payload"] for row in connection.execute(select(events)).mappings()
        }
        projections = {
            row["event_id"]: row
            for row in connection.execute(select(event_reporting_metadata)).mappings()
        }
    assert originals[existing_id] == original
    assert originals[new_id] == first_new
    assert set(projections) == {new_id, batch_id}
    assert projections[new_id]["projection_version"] == REPORTING_PROJECTION_VERSION
    assert projections[new_id]["payload"] == event_reporting_projection(
        "shadow_stream_status", first_new
    )
    assert projections[batch_id]["payload"] == event_reporting_projection(
        "shadow_recorder_batch", originals[batch_id]
    )
    assert repository.market_data_counts() == (0, 1, 0)


def test_metadata_failure_rolls_back_raw_audit_and_all_sidecars_together(repository, monkeypatch):
    notice_id, batch_id = str(uuid4()), str(uuid4())
    calls = []

    def fail_second(connection, event_id, event_type, payload):
        assert connection.in_transaction()
        calls.append(connection)
        append_reporting_metadata(connection, event_id, event_type, payload)
        if len(calls) == 2:
            raise RuntimeError("test metadata failure")

    monkeypatch.setattr("tradeagent.shadow_recorder.append_reporting_metadata", fail_second)
    with pytest.raises(RuntimeError, match="metadata failure"):
        persist_shadow_batch(
            repository,
            [_receipt(0)],
            [_notice(notice_id, {"session_date": "2026-09-08"})],
            batch_id,
        )
    assert len(calls) == 2
    assert calls[0] is calls[1]
    assert repository.market_data_counts() == (0, 0, 0)
    assert repository.event_count() == 0
    with repository._database.begin() as connection:
        assert connection.execute(select(event_reporting_metadata)).all() == []
    monkeypatch.setattr(
        "tradeagent.shadow_recorder.append_reporting_metadata", append_reporting_metadata
    )
    result = persist_shadow_batch(
        repository,
        [_receipt(0)],
        [_notice(notice_id, {"session_date": "2026-09-08"})],
        batch_id,
    )
    assert result.inserted == 1
    with repository._database.begin() as connection:
        assert len(connection.execute(select(event_reporting_metadata)).all()) == 2


def test_existing_batch_retry_does_not_backfill_metadata_from_incoming_body(
    repository, monkeypatch
):
    batch_id = str(uuid4())
    original_payload = {"inserted": 7, "duplicates": 2, "session_date": "2026-09-08"}
    original_row = _notice(batch_id, original_payload)
    original_row["event_type"] = "shadow_recorder_batch"
    with repository._database.begin() as connection:
        connection.execute(events.insert().values(original_row))

    def forbidden(*args):
        pytest.fail(
            "pre-existing batch IDs require original-body backfill, not incoming retry data"
        )

    monkeypatch.setattr("tradeagent.shadow_recorder.append_reporting_metadata", forbidden)
    result = persist_shadow_batch(repository, [_receipt(0), _receipt(1)], [], batch_id)
    assert (result.inserted, result.duplicates) == (7, 2)
    assert repository.market_data_counts() == (0, 0, 0)
    with repository._database.begin() as connection:
        assert connection.scalar(select(events.c.payload)) == original_payload
        assert connection.execute(select(event_reporting_metadata)).all() == []


def test_batch_owner_is_immutable_on_retry(repository):
    batch_id = str(uuid4())
    persist_shadow_batch(repository, [_receipt(0)], [], batch_id, instance_id="first-recorder")
    persist_shadow_batch(repository, [_receipt(0)], [], batch_id, instance_id="new-recorder")
    original = repository.latest_event_payload("shadow_recorder_batch")
    assert original["instance_id"] == "first-recorder"
    assert repository.market_data_counts() == (0, 1, 0)
