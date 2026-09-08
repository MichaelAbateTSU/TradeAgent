from __future__ import annotations

import asyncio
import threading
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.exc import OperationalError

from tradeagent.alpaca_stream import MarketQuote, MarketTrade, ReceivedStreamEvent
from tradeagent.domain import MarketBar
from tradeagent.persistence import (
    Database,
    ProductionRepository,
    market_bars,
    market_quotes,
    market_trades,
)
from tradeagent.shadow_recorder import (
    ShadowBatchRecorder,
    ShadowRecorderSettings,
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

    def ambiguous_commit(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        result = original(*args, **kwargs)
        if attempts == 1:
            raise OperationalError("commit", {}, RuntimeError("lost acknowledgement"))
        return result

    monkeypatch.setattr("tradeagent.shadow_recorder.persist_shadow_batch", ambiguous_commit)
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
    assert recorder.committed == recorder.inserted == 1
    assert recorder.duplicates == 0
    assert repository.market_data_counts() == (0, 1, 0)
    assert repository.event_count() == 1


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
