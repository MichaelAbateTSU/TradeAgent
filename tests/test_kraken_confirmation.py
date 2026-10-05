"""Synthetic contract tests only: no provider connections, jobs, or trading calls."""

from __future__ import annotations

import asyncio
import base64
import importlib
import json
import sys
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import httpx
import pytest
from pydantic import JsonValue, SecretStr, ValidationError
from sqlalchemy import (
    Column,
    MetaData,
    String,
    Table,
    create_engine,
    delete,
    event,
    insert,
    inspect,
    select,
    update,
)
from sqlalchemy.engine import Connection, make_url
from sqlalchemy.pool import StaticPool

import tradeagent.kraken_confirmation as confirmation
import tradeagent.kraken_confirmation_runtime as runtime
from tradeagent.kraken_confirmation import (
    LOCK_NAME,
    NS,
    SLOTS,
    SOURCE_MODULES,
    STUDY_ID,
    V2_STUDY_ID,
    V3_STUDY_ID,
    CausalGrid,
    ChunkHeader,
    ConfirmationProtocol,
    ConfirmationStore,
    Evaluation,
    Evidence,
    Label,
    PausedV1Proof,
    Quote,
    SourceDigest,
    StudyId,
    Symbol,
    check_schema,
    confirmation_evidence,
    confirmation_schema_version,
    decode_blob,
    install_schema,
    legacy_journal_compatibility,
    measure,
    record,
)
from tradeagent.kraken_confirmation_book import Book, BookQuote, checksum, decimal
from tradeagent.kraken_confirmation_report import (
    BTC_ONLY,
    NO_GO,
    PASS,
    build_report,
    export_evidence,
    final_decision,
    load_report,
)
from tradeagent.kraken_confirmation_runtime import (
    BackpressureError,
    DurableWriter,
    GuardEvidence,
    Receiver,
    observer_guard,
    preflight,
    source_digests,
    validate_guard,
    verify_sources,
)
from tradeagent.persistence import (
    Database,
    ProductionRepository,
    controls,
    heartbeats,
    worker_locks,
)
from tradeagent.scalping_market import datetime_ns, ns_datetime
from tradeagent.scalping_store import canonical, scalping_cycles, scalping_order_links
from tradeagent.shadow_dataset import ShadowDatasetProtocol, shadow_datasets
from tradeagent.shadow_dataset_monitor import STOP_KEY, PaperReadOnlyMonitor, local_safety

legacy_version = Table(
    "alembic_version",
    MetaData(),
    Column("version_num", String(32), primary_key=True, nullable=False),
)


class Clock:
    def __init__(self, at: datetime) -> None:
        self.at = at

    def __call__(self) -> datetime:
        return self.at

    def ns(self) -> int:
        return datetime_ns(self.at)

    def set(self, at_ns: int) -> None:
        self.at = ns_datetime(at_ns)


@pytest.fixture
def protocol() -> ConfirmationProtocol:
    start = datetime(2027, 1, 8, tzinfo=UTC)  # Synthetic Friday; not an actual freeze.
    return ConfirmationProtocol(
        frozen_at=start - timedelta(seconds=60),
        start=start,
        end=start + timedelta(hours=72),
        code_sha="a" * 40,
        source_hashes=source_digests(),
        account_digest="b" * 64,
        v1_runtime_sha="c" * 40,
        v1_owner_id="pinned-v1-owner",
        v1_connection_id="pinned-v1-feed",
    )


@pytest.fixture
def database() -> Iterator[Database]:
    database = Database("sqlite://")
    database.engine.dispose()
    database.engine = create_engine(
        "sqlite://",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    worker_locks.create(database.engine)
    legacy_version.create(database.engine)
    with database.begin() as connection:
        connection.execute(
            insert(legacy_version).values(
                version_num=confirmation.LEGACY_SCHEMA_REVISION,
            )
        )
    install_schema(database)
    try:
        yield database
    finally:
        database.dispose()


@pytest.fixture
def clock(protocol: ConfirmationProtocol) -> Clock:
    return Clock(protocol.frozen_at + timedelta(seconds=1))


@pytest.fixture
def store(
    database: Database,
    protocol: ConfirmationProtocol,
    clock: Clock,
) -> ConfirmationStore:
    store = ConfirmationStore(database, "study-owner", clock=clock)
    store.claim(protocol)
    return store


def snapshot(
    *,
    bid: str = "999",
    ask: str = "1000",
    size: str = "0.011",
) -> dict[str, JsonValue]:
    return {
        "bids": [{"price": bid, "qty": size}],
        "asks": [{"price": ask, "qty": size}],
        "checksum": checksum({Decimal(bid): Decimal(size)}, {Decimal(ask): Decimal(size)}),
    }


def quote(
    at: int,
    *,
    symbol: Symbol = "BTC/USD",
    frame: int = 1,
    epoch: int = 0,
    received: int | None = None,
    accepted: int | None = None,
    bid: str = "999",
    ask: str = "1000",
    size: str = "0.011",
) -> Quote:
    parsed = Book().apply(snapshot(bid=bid, ask=ask, size=size), "snapshot", at)
    assert parsed is not None
    return Quote.model_validate(
        {
            **parsed.model_dump(),
            "symbol": symbol,
            "frame_id": frame,
            "connection_id": "synthetic",
            "epoch": epoch,
            "received_ns": at if received is None else received,
            "accepted_ns": at if accepted is None else accepted,
            "received_monotonic_ns": at,
        }
    )


def guard_evidence(protocol: ConfirmationProtocol, at: int) -> GuardEvidence:
    return GuardEvidence(
        account_digest=protocol.account_digest,
        checked_ns=at,
        v1_runtime_sha=protocol.v1_runtime_sha,
        v1_owner_id=protocol.v1_owner_id,
        v1_connection_id=protocol.v1_connection_id,
    )


def append_capture(store: ConfirmationStore, clock: Clock, item: Quote) -> None:
    clock.set(item.accepted_ns)
    row = Evidence(
        key=f"frame:{item.frame_id}",
        at_ns=item.accepted_ns,
        payload={
            "frame_id": item.frame_id,
            "payload_text": "synthetic raw proof",
            "quotes": [item.model_dump(mode="json")],
        },
    )
    store.refresh()
    store.append(
        "capture",
        row.key,
        {"records": [row.model_dump(mode="json")]},
        at_ns=clock.ns(),
    )


def append_record(store: ConfirmationStore, clock: Clock, item: Evaluation | Label) -> None:
    row = record(item)
    clock.set(row.at_ns)
    store.refresh()
    store.append(
        "label" if isinstance(item, Label) else "evaluation",
        row.key,
        {"records": [row.model_dump(mode="json")]},
        at_ns=clock.ns(),
    )


def write_one_label(
    store: ConfirmationStore,
    clock: Clock,
    protocol: ConfirmationProtocol,
) -> Label:
    at = protocol.slot_ns(0)
    store.append(
        "guard",
        "initial",
        guard_evidence(protocol, clock.ns()).model_dump(mode="json"),
        at_ns=clock.ns(),
    )
    entry = quote(at - NS)
    append_capture(store, clock, entry)
    evaluation = Evaluation(
        slot=0,
        symbol="BTC/USD",
        at_ns=at,
        recorded_ns=at,
        missed_scheduled_slot=False,
        entry=entry,
    )
    append_record(store, clock, evaluation)
    future = quote(at + 60 * NS, frame=2)
    append_capture(store, clock, future)
    label = Label(
        evaluation=evaluation,
        resolved_ns=at + 62 * NS,
        information_available_ns=at + 62 * NS,
        future=future,
        eligibility=measure(entry, future, at),
        long_gross_bps=(future.bid / entry.ask - 1) * 10000,
    )
    append_record(store, clock, label)
    store.append("quality", "quality", {"counts": {"frames": 2}}, at_ns=clock.ns())
    store.append(
        "guard",
        "final",
        guard_evidence(protocol, clock.ns()).model_dump(mode="json"),
        at_ns=clock.ns(),
    )
    return label


@pytest.mark.parametrize(
    "changes",
    [
        {"duration_hours": 24},
        {"evaluation_seconds": 11},
        {"horizon_seconds": 61},
        {"settle_seconds": 3},
        {"capture_tail_seconds": 74},
        {"freshness_seconds": 3},
        {"notional_usd": "10.24"},
        {"minimum_coverage": ".94"},
        {"symbols": ("BTC/USD",)},
        {"slots_per_symbol": 25919},
        {"depth": 25},
        {"orders_enabled": True},
        {"private_kraken_requests": 1},
        {"maximum_payload_bytes": 4 * 1024**3},
        {"maximum_connections": 5},
        {"trading_authorization": "active"},
        {"model_state": "supported"},
        {"unknown_threshold": 1},
    ],
)
def test_protocol_thresholds_are_not_tunable(
    protocol: ConfirmationProtocol,
    changes: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        ConfirmationProtocol.model_validate({**protocol.model_dump(), **changes})


def test_protocol_window_freeze_weekparts_and_hash(protocol: ConfirmationProtocol) -> None:
    assert protocol.end - protocol.start == timedelta(hours=72)
    assert protocol.slot_ns(SLOTS - 1) == datetime_ns(protocol.end) - 10 * NS
    assert protocol.last_settlement_ns == datetime_ns(protocol.end) + 52 * NS
    assert protocol.capture_end_ns == datetime_ns(protocol.end) + 75 * NS
    assert (
        ConfirmationProtocol.model_validate_json(protocol.model_dump_json()).identity
        == protocol.identity
    )
    for changes in (
        {"end": protocol.end + timedelta(seconds=1)},
        {"frozen_at": protocol.start},
        {
            "start": protocol.start + timedelta(seconds=1),
            "end": protocol.end + timedelta(seconds=1),
        },
        {"start": protocol.start + timedelta(days=3), "end": protocol.end + timedelta(days=3)},
        {"source_hashes": tuple(reversed(protocol.source_hashes))},
    ):
        with pytest.raises(ValidationError):
            ConfirmationProtocol.model_validate({**protocol.model_dump(), **changes})
    with pytest.raises(ValidationError):
        protocol.__setattr__("freshness_seconds", 3)


def test_full_grid_maturity_is_not_observation_count(protocol: ConfirmationProtocol) -> None:
    at = protocol.slot_ns(0)
    assert protocol.matured(at + 62 * NS - 1) == 0
    assert protocol.matured(at + 62 * NS) == 1
    assert protocol.matured(at + 72 * NS) == 2
    assert protocol.matured(protocol.last_settlement_ns - 1) == SLOTS - 1
    assert protocol.matured(protocol.last_settlement_ns) == SLOTS
    assert protocol.matured(protocol.capture_end_ns - 1) == SLOTS
    assert protocol.matured(protocol.capture_end_ns) == SLOTS
    assert protocol.matured(protocol.capture_end_ns + 1000 * NS) == SLOTS


@pytest.mark.parametrize("value", [True, 1.1, float("nan"), "NaN", "Infinity"])
def test_precision_decoder_rejects_non_decimal_scalars(value: object) -> None:
    with pytest.raises(ValueError):
        decimal(value)


def test_official_crc_example() -> None:
    bids = [
        ("45283.5", "0.10000000"),
        ("45283.4", "1.54582015"),
        ("45282.1", "0.10000000"),
        ("45281.0", "0.10000000"),
        ("45280.3", "1.54592586"),
        ("45279.0", "0.07990000"),
        ("45277.6", "0.03310103"),
        ("45277.5", "0.30000000"),
        ("45277.3", "1.54602737"),
        ("45276.6", "0.15445238"),
    ]
    asks = [
        ("45285.2", "0.00100000"),
        ("45286.4", "1.54571953"),
        ("45286.6", "1.54571109"),
        ("45289.6", "1.54560911"),
        ("45290.2", "0.15890660"),
        ("45291.8", "1.54553491"),
        ("45294.7", "0.04454749"),
        ("45296.1", "0.35380000"),
        ("45297.5", "0.09945542"),
        ("45299.5", "0.18772827"),
    ]
    assert (
        checksum(
            {Decimal(p): Decimal(q) for p, q in bids},
            {Decimal(p): Decimal(q) for p, q in asks},
        )
        == 3310070434
    )


def test_ordered_same_level_updates_change_separate_clocks_and_delete() -> None:
    book = Book()
    first = book.apply(snapshot(), "snapshot", 100)
    assert first is not None
    updated = book.apply(
        {
            "bids": [{"price": "999", "qty": ".3"}, {"price": "999", "qty": ".2"}],
            "checksum": checksum(
                {Decimal("999"): Decimal(".2")}, {Decimal("1000"): Decimal(".011")}
            ),
        },
        "update",
        101,
    )
    assert updated is not None and updated.bid_size == Decimal(".2")
    assert updated.bid_size_last_changed_ns == 101 and updated.bid_price_last_changed_ns == 100
    assert updated.ask_size_last_changed_ns == 100 and updated.provider_book_update_ns == 101
    deleted = book.apply(
        {
            "bids": [{"price": "998", "qty": ".1"}, {"price": "999", "qty": "0"}],
            "checksum": checksum(
                {Decimal("998"): Decimal(".1")}, {Decimal("1000"): Decimal(".011")}
            ),
        },
        "update",
        102,
    )
    assert deleted is not None and deleted.bid == 998 and 999 not in book.bids


def test_truncation_crc_failure_timestamp_and_disconnect_snapshot_requirement() -> None:
    book = Book()
    bids = {Decimal(900 + i): Decimal(".1") for i in range(11)}
    trimmed = dict(sorted(bids.items(), reverse=True)[:10])
    row: dict[str, JsonValue] = {
        "bids": [{"price": str(p), "qty": str(q)} for p, q in bids.items()],
        "asks": [{"price": "1000", "qty": ".011"}],
        "checksum": checksum(trimmed, {Decimal("1000"): Decimal(".011")}),
    }
    assert book.apply(row, "snapshot", 100) is not None and len(book.bids) == 10
    assert book.apply({**row, "checksum": 0}, "update", 101) is None
    assert not book.valid and book.apply(row, "update", 102) is None
    assert book.apply(row, "snapshot", 103) is not None
    assert book.apply(row, "update", 102) is None and not book.valid
    assert book.last_reason == "provider_book_timestamp_regression"
    assert book.apply(row, "snapshot", 104) is not None
    book.invalidate("disconnect")
    assert book.apply(row, "update", 105) is None
    assert book.apply(row, "snapshot", 106) is not None


def test_freshness_book_state_clock_not_price_or_quantity_change(
    protocol: ConfirmationProtocol,
) -> None:
    at = protocol.slot_ns(0)
    entry = quote(at - 2 * NS)
    future = quote(at + 58 * NS, frame=2)
    assert measure(entry, future, at).complete
    old_price = entry.model_copy(
        update={
            "bid_price_last_changed_ns": at - 1000 * NS,
            "ask_price_last_changed_ns": at - 1000 * NS,
        }
    )
    assert measure(old_price, future, at).complete
    stale = quote(at - 2 * NS - 1, received=at - 2 * NS - 1)
    metrics = measure(stale, future, at)
    assert not metrics.complete and metrics.integrity_eligible
    assert metrics.entry_size and metrics.future_size and not metrics.entry_fresh
    assert metrics.reasons == ("DECISION_QUOTE_STALE_OR_MISSING",)


def test_exact_usd_side_size_units_and_independent_eligibility(
    protocol: ConfirmationProtocol,
) -> None:
    at = protocol.slot_ns(0)
    entry = quote(at, ask="1000", size=".01025")
    future = quote(at + 60 * NS, frame=2, bid="1000", ask="1001", size=".01025")
    assert measure(entry, future, at).complete
    small = quote(at + 60 * NS, frame=2, bid="1000", ask="1001", size=".010249999")
    metrics = measure(entry, small, at)
    assert metrics.entry_fresh and metrics.future_fresh and metrics.integrity_eligible
    assert metrics.entry_size and not metrics.future_size and not metrics.complete
    assert metrics.reasons == ("INSUFFICIENT_DISPLAYED_LONG_SIZE",)
    crossed_epoch = small.model_copy(update={"epoch": 1})
    assert measure(entry, crossed_epoch, at).reasons == ("LOCAL_CONTINUITY_CHANGED",)
    with pytest.raises(ValidationError):
        BookQuote.model_validate(
            {**entry.model_dump(include=set(BookQuote.model_fields)), "bid_size": 10.25}
        )


def test_causal_cache_acceptance_and_fixed_horizon_settlement(
    protocol: ConfirmationProtocol,
) -> None:
    at = protocol.slot_ns(0)
    grid = CausalGrid(protocol)
    before = quote(at - NS)
    late_entry = quote(at, frame=2, accepted=at + 1)
    grid.accept(before)
    grid.accept(late_entry)
    assert grid.select("BTC/USD", at, at, decision=True) == before
    future = quote(at + 60 * NS, frame=3, accepted=at + 62 * NS)
    too_late = quote(at + 60 * NS, frame=4, accepted=at + 62 * NS + 1)
    grid.accept(future)
    grid.accept(too_late)
    assert grid.select("BTC/USD", at + 60 * NS, at + 62 * NS, decision=False) == future
    after_deadline = quote(at + 60 * NS + 1, frame=5, accepted=at + 62 * NS + 2)
    grid.accept(after_deadline)
    assert grid.select("BTC/USD", at + 60 * NS, at + 62 * NS, decision=False) == future


def test_disconnect_invalidates_cached_quotes_until_new_snapshot(
    protocol: ConfirmationProtocol,
) -> None:
    at = protocol.slot_ns(0)
    grid = CausalGrid(protocol)
    grid.accept(quote(at))
    grid.invalidate(at + NS, "disconnect", 1)
    assert grid.select("BTC/USD", at + NS, at + NS, decision=True) is None
    assert grid.select("BTC/USD", at, at, decision=True) is not None
    restored = quote(at + 2 * NS, frame=2, epoch=1)
    grid.accept(restored)
    assert grid.select("BTC/USD", at + 2 * NS, at + 2 * NS, decision=True) == restored


def test_missed_slots_not_backfilled_and_quote_ring_bounded(
    protocol: ConfirmationProtocol,
) -> None:
    at = protocol.slot_ns(0)
    grid = CausalGrid(protocol)
    grid.accept(quote(at + 19 * NS))
    evaluations, _ = grid.advance(at + 22 * NS)
    assert len(evaluations) == 6
    assert all(row.missed_scheduled_slot and row.entry is None for row in evaluations[:4])
    assert not evaluations[4].missed_scheduled_slot and evaluations[4].entry is not None
    grid.accept(quote(at + 100 * NS, frame=2))
    assert len(grid.history["BTC/USD"]) == 1
    with pytest.raises(RuntimeError, match="scheduler_backlog_budget"):
        grid.advance(protocol.capture_end_ns)
    with pytest.raises(ValueError, match="clock_regression"):
        grid.accept(quote(at + 99 * NS, frame=3))


def test_idempotent_append_hash_chain_and_terminal_immutability(
    store: ConfirmationStore,
    clock: Clock,
    protocol: ConfirmationProtocol,
) -> None:
    payload: dict[str, JsonValue] = {"counts": {"frames": 0}}
    first = store.append("quality", "q", payload, at_ns=clock.ns())
    same = store.append("quality", "q", payload, at_ns=clock.ns())
    assert first == same
    blob = next(row["payload"] for row in _all_rows(store.database) if row["key"] == "q")
    assert isinstance(blob, bytes)
    assert first.payload_hash == sha256(blob).hexdigest()
    with pytest.raises(ValueError, match="conflicting_duplicate"):
        store.append("quality", "q", {"different": True}, at_ns=clock.ns())
    rows = list(store.chunks(verify_payloads=True))
    assert len(rows) == 2 and rows[0][0].previous_hash == protocol.identity
    assert rows[1][0].previous_hash == rows[0][0].chain_hash
    store.append("terminal", "terminal", {"state": "failed_incomplete"}, at_ns=clock.ns())
    with pytest.raises(RuntimeError, match="terminal_confirmation_is_immutable"):
        store.append("quality", "after", payload, at_ns=clock.ns())


def _all_rows(database: Database) -> list[dict[str, object]]:
    with database.begin() as connection:
        return [dict(row) for row in connection.execute(select(confirmation_evidence)).mappings()]


def test_no_restart_even_after_stale_or_released_owner(
    store: ConfirmationStore,
    clock: Clock,
    protocol: ConfirmationProtocol,
) -> None:
    store.repository.release_worker_lock(LOCK_NAME, store.owner_id)
    replacement = ConfirmationStore(store.database, "replacement", clock=clock)
    with pytest.raises(RuntimeError, match="no_restart"):
        replacement.claim(protocol)
    assert len(_all_rows(store.database)) == 1
    report = build_report(store, at_ns=protocol.capture_end_ns)
    assert report["state"] == "failed_incomplete_process_lost_no_restart"
    assert report["decision"] == NO_GO
    overall = block(report, "overall", "ALL")
    assert overall["matured"] == 2 * SLOTS and overall["missing"] == 2 * SLOTS


def test_lease_owner_expiry_and_refresh_cannot_take_over(
    store: ConfirmationStore,
    clock: Clock,
) -> None:
    other = ConfirmationStore(store.database, "other", clock=clock)
    with pytest.raises(RuntimeError, match="lease_lost"):
        other.append("quality", "other", {}, at_ns=clock.ns())
    clock.at += timedelta(seconds=91)
    with pytest.raises(RuntimeError, match="lease_expired"):
        store.refresh()
    with pytest.raises(RuntimeError, match="lease_expired"):
        store.append("quality", "expired", {}, at_ns=clock.ns())


def test_hash_tampering_and_truncated_compression_never_succeed(
    store: ConfirmationStore,
    clock: Clock,
) -> None:
    header = store.append("quality", "q", {"counts": {}}, at_ns=clock.ns())
    with store.database.begin() as connection:
        blob = bytes(
            connection.execute(
                select(confirmation_evidence.c.payload).where(
                    confirmation_evidence.c.sequence == 1
                ),
            ).scalar_one()
        )
        connection.execute(
            update(confirmation_evidence)
            .where(
                confirmation_evidence.c.sequence == 1,
            )
            .values(payload=blob + b"tamper")
        )
    with pytest.raises(ValueError, match="payload_hash_mismatch"):
        list(store.chunks(verify_payloads=True))
    with pytest.raises(ValueError, match="compression/size"):
        decode_blob(blob[:-1], header.decoded_bytes)
    with pytest.raises(ValueError, match="compression verification"):
        decode_blob(b"not-zlib", header.decoded_bytes)


def test_chunk_and_total_payload_budgets_reserve_terminal_evidence(
    store: ConfirmationStore,
    clock: Clock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(RuntimeError, match="chunk_budget"):
        store.append(
            "capture", "oversized", {"wire": "x" * confirmation.MAX_CHUNK_BYTES}, at_ns=clock.ns()
        )
    charged = list(store.chunks())[-1][0].stored_bytes
    monkeypatch.setattr(
        confirmation,
        "PAYLOAD_BUDGET",
        charged + confirmation.TERMINAL_RESERVE + 600,
    )
    with pytest.raises(RuntimeError, match="payload_budget"):
        store.append(
            "capture",
            "budget",
            {"wire": "".join(sha256(str(i).encode()).hexdigest() for i in range(100))},
            at_ns=clock.ns(),
        )
    store.append(
        "terminal", "terminal", {"state": "failed_incomplete", "reason": "budget"}, at_ns=clock.ns()
    )


def block(report: dict[str, JsonValue], context: str, symbol: str) -> dict[str, JsonValue]:
    rows = report["blocks"]
    assert isinstance(rows, list)
    for row in rows:
        assert isinstance(row, dict)
        if row["block"] == context and row["symbol"] == symbol:
            return row
    raise AssertionError(f"missing block {context}/{symbol}")


def test_streaming_report_includes_unwritten_labels_and_fixed_blocks(
    store: ConfirmationStore,
    clock: Clock,
    protocol: ConfirmationProtocol,
) -> None:
    label = write_one_label(store, clock, protocol)
    report = build_report(store, at_ns=label.resolved_ns, verify_payloads=True)
    btc, eth = block(report, "overall", "BTC/USD"), block(report, "overall", "ETH/USD")
    assert btc["matured"] == eth["matured"] == 1
    assert btc["complete"] == 1 and eth["complete"] == 0
    assert btc["coverage"] == 1 and eth["coverage"] == 0
    assert btc["reasons"] == {}
    assert eth["reasons"] == {"OVERDUE_UNWRITTEN_LABEL": 1}
    assert block(report, "overall", "ALL")["reasons"] == {"OVERDUE_UNWRITTEN_LABEL": 1}
    assert eth["missing_label_records"] == 1
    assert btc["scheduled"] == eth["scheduled"] == SLOTS
    assert block(report, "6h:00", "BTC/USD")["scheduled"] == 2160
    assert block(report, "weekday", "BTC/USD")["scheduled"] == 8640
    assert block(report, "weekend", "BTC/USD")["scheduled"] == 17280
    assert block(report, "utc_day:2027-01-09", "ALL")["scheduled"] == 17280
    assert report["raw_frames_verified"] == 2 and report["decision"] == NO_GO
    final = build_report(
        store, at_ns=protocol.capture_end_ns, verify_payloads=True, completing=True
    )
    assert block(final, "overall", "BTC/USD")["matured"] == SLOTS
    assert block(final, "overall", "BTC/USD")["missing"] == SLOTS - 1
    assert not final["clean_integrity"] and final["decision"] == NO_GO


def test_completion_cannot_shortcut_fixed_capture_tail(
    store: ConfirmationStore,
    clock: Clock,
    protocol: ConfirmationProtocol,
) -> None:
    write_one_label(store, clock, protocol)
    report = build_report(
        store,
        at_ns=protocol.last_settlement_ns,
        verify_payloads=True,
        completing=True,
    )
    assert block(report, "overall", "BTC/USD")["matured"] == SLOTS
    assert report["state"] != "completed"
    assert report["decision"] == NO_GO
    assert protocol.capture_end_ns - datetime_ns(protocol.start) == (72 * 3600 + 75) * NS


def test_streaming_export_preserves_exact_hash_proof_without_writes(
    store: ConfirmationStore,
    clock: Clock,
    protocol: ConfirmationProtocol,
) -> None:
    write_one_label(store, clock, protocol)
    before = _all_rows(store.database)
    with store.database.begin() as connection:
        lease_before = dict(connection.execute(select(worker_locks)).mappings().one())
    exported = list(export_evidence(store.database))
    assert len(exported) == len(before)
    root = protocol.identity
    for expected, item in zip(before, exported, strict=True):
        assert item["encoding"] == "zlib-json-v1"
        header = ChunkHeader.model_validate(item["header"])
        encoded = item["payload_base64"]
        assert isinstance(encoded, str)
        blob = base64.b64decode(encoded, validate=True)
        assert blob == expected["payload"]
        assert sha256(blob).hexdigest() == header.payload_hash
        assert header.previous_hash == root
        assert header.chain_hash == header.calculated_hash()
        decode_blob(blob, header.decoded_bytes)
        root = header.chain_hash
    assert _all_rows(store.database) == before
    with store.database.begin() as connection:
        assert dict(connection.execute(select(worker_locks)).mappings().one()) == lease_before


def test_changed_quote_cannot_borrow_a_previous_raw_frame_proof(
    store: ConfirmationStore,
    clock: Clock,
    protocol: ConfirmationProtocol,
) -> None:
    original = write_one_label(store, clock, protocol)
    assert original.evaluation.entry is not None
    changed = original.evaluation.entry.model_copy(update={"ask": Decimal("1001")})
    at = protocol.slot_ns(6)
    evaluation = Evaluation(
        slot=6,
        symbol="BTC/USD",
        at_ns=at,
        recorded_ns=at + 3 * NS,
        missed_scheduled_slot=False,
        entry=changed,
    )
    append_record(store, clock, evaluation)
    with pytest.raises(ValueError, match="quote raw proof"):
        build_report(store, at_ns=clock.ns(), verify_payloads=True)


def test_prospective_claim_refuses_missed_start_without_acquiring_lease(
    database: Database,
    protocol: ConfirmationProtocol,
    clock: Clock,
) -> None:
    clock.at = protocol.start
    store = ConfirmationStore(database, "too-late", clock=clock)
    with pytest.raises(ValueError, match="missed prospective start"):
        store.claim(protocol)
    assert not store.exists()
    with database.begin() as connection:
        assert connection.scalar(select(worker_locks.c.owner_id)) is None


def test_late_evaluation_cannot_hide_missed_scheduled_slot(
    protocol: ConfirmationProtocol,
) -> None:
    at = protocol.slot_ns(0)
    with pytest.raises(ValidationError, match="missed-slot flag"):
        Evaluation(
            slot=0,
            symbol="BTC/USD",
            at_ns=at,
            recorded_ns=at + 10 * NS,
            missed_scheduled_slot=False,
            entry=quote(at),
        )


def test_label_forward_joins_and_scalar_return_are_rejected(
    protocol: ConfirmationProtocol,
) -> None:
    at = protocol.slot_ns(0)
    entry, future = quote(at), quote(at + 60 * NS, frame=2)
    evaluation = Evaluation(
        slot=0,
        symbol="BTC/USD",
        at_ns=at,
        recorded_ns=at,
        missed_scheduled_slot=False,
        entry=entry,
    )
    values = {
        "evaluation": evaluation,
        "resolved_ns": at + 62 * NS,
        "information_available_ns": at + 62 * NS,
        "future": future,
        "eligibility": measure(entry, future, at),
        "long_gross_bps": Decimal("-10"),
    }
    assert Label.model_validate(values).long_gross_bps == -10
    for changes in (
        {"long_gross_bps": Decimal("-.001")},
        {"information_available_ns": at + 63 * NS},
        {"future": future.model_copy(update={"accepted_ns": at + 62 * NS + 1})},
        {
            "evaluation": evaluation.model_copy(
                update={"entry": entry.model_copy(update={"accepted_ns": at + 1})}
            )
        },
    ):
        with pytest.raises(ValidationError):
            Label.model_validate({**values, **changes})


@pytest.mark.parametrize(
    ("btc", "eth", "size", "clean", "completed", "decision"),
    [
        (24624, 24624, 0, True, True, PASS),
        (24624, 24623, SLOTS - 24623, True, True, BTC_ONLY),
        (24624, 24623, 0, True, True, NO_GO),
        (24623, SLOTS, 0, True, True, NO_GO),
        (SLOTS, SLOTS, 0, False, True, NO_GO),
        (SLOTS, SLOTS, 0, True, False, NO_GO),
    ],
)
def test_final_decisions_keep_v2_blocked(
    btc: int,
    eth: int,
    size: int,
    clean: bool,
    completed: bool,
    decision: str,
) -> None:
    assert (
        final_decision(
            btc,
            eth,
            clean=clean,
            completed=completed,
            eth_size_only_failures=size,
        )
        == decision
    )


def test_source_modules_must_match_exact_frozen_bytes(protocol: ConfirmationProtocol) -> None:
    verify_sources(protocol)
    wrong = tuple(SourceDigest(name=name, sha256="0" * 64) for name in SOURCE_MODULES)
    with pytest.raises(ValueError, match="source modules differ"):
        verify_sources(protocol.model_copy(update={"source_hashes": wrong}))


def test_receiver_writes_raw_before_cache_and_invalidation_with_off_loop_db(
    store: ConfirmationStore,
    clock: Clock,
    protocol: ConfirmationProtocol,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    at = protocol.slot_ns(0)
    clock.set(at - NS)
    store.refresh()
    monkeypatch.setattr(runtime, "now_ns", clock.ns)
    main_thread = threading.get_ident()
    writing_threads: list[int] = []
    original = store.append

    def tracked(
        kind: str,
        key: str,
        payload: dict[str, JsonValue],
        *,
        at_ns: int,
        claim: bool = False,
    ) -> ChunkHeader:
        writing_threads.append(threading.get_ident())
        return original(kind, key, payload, at_ns=at_ns, claim=claim)

    monkeypatch.setattr(store, "append", tracked)

    async def scenario() -> Receiver:
        writer = DurableWriter(store)
        writer.task = asyncio.create_task(writer.work())
        grid = CausalGrid(protocol)
        receiver = Receiver(protocol, grid, writer)
        receiver.connection_id = "synthetic"
        row = {**snapshot(), "symbol": "BTC/USD", "timestamp": clock.at.isoformat()}
        raw = canonical({"channel": "book", "type": "snapshot", "data": [row]})
        await receiver.frame(raw)
        assert writer.queue.qsize() == 1
        assert grid.select("BTC/USD", at, at, decision=True) is not None
        await writer.flush()
        clock.set(at)
        bad = {**row, "checksum": 0, "timestamp": clock.at.isoformat()}
        with pytest.raises(ValueError, match="checksum_mismatch"):
            await receiver.frame(canonical({"channel": "book", "type": "update", "data": [bad]}))
        assert grid.select("BTC/USD", at, at, decision=True) is None
        await writer.close()
        return receiver

    receiver = asyncio.run(scenario())
    assert receiver.counts["checksum_failures"] == 1 and receiver.counts["frames"] == 2
    assert writing_threads and all(thread != main_thread for thread in writing_threads)
    chunks = [payload for header, payload in store.chunks() if header.kind == "capture"]
    assert len(chunks) == 2
    assert chunks[1] is not None
    rows = cast(list[JsonValue], chunks[1]["records"])
    persisted = Evidence.model_validate(rows[0])
    assert persisted.payload["rejection"] == "checksum_mismatch"
    assert persisted.payload["quotes"] == []


def test_writer_failure_and_backpressure_are_explicit_not_hangs(
    store: ConfirmationStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        writer = DurableWriter(store)

        async def inactive() -> None:
            await asyncio.Event().wait()

        writer.task = asyncio.create_task(inactive())
        monkeypatch.setattr(runtime, "QUEUE_TIMEOUT", 0.01)
        for index in range(runtime.QUEUE_CAPACITY):
            await writer.submit("quality", (Evidence(key=str(index), at_ns=0, payload={}),))
        with pytest.raises(BackpressureError):
            await writer.reserve()
        writer.task.cancel()
        await asyncio.gather(writer.task, return_exceptions=True)

    asyncio.run(scenario())

    def fail(
        kind: str,
        key: str,
        payload: dict[str, JsonValue],
        *,
        at_ns: int,
        claim: bool = False,
    ) -> ChunkHeader:
        raise RuntimeError("synthetic database failure")

    monkeypatch.setattr(store, "append", fail)

    async def failed_writer() -> None:
        writer = DurableWriter(store)
        writer.task = asyncio.create_task(writer.work())
        await writer.submit("capture", (Evidence(key="raw", at_ns=0, payload={}),))
        with pytest.raises(RuntimeError, match="synthetic database failure"):
            await writer.flush()
        with pytest.raises(RuntimeError, match="synthetic database failure"):
            await writer.reserve()

    asyncio.run(failed_writer())


@pytest.mark.parametrize("unhandled", [False, True])
def test_run_guard_failure_seals_incomplete_without_provider_or_restart(
    database: Database,
    protocol: ConfirmationProtocol,
    clock: Clock,
    monkeypatch: pytest.MonkeyPatch,
    unhandled: bool,
) -> None:
    def factory(database: Database, owner: str) -> ConfirmationStore:
        return ConfirmationStore(database, owner, clock=clock)

    monkeypatch.setattr(runtime, "ConfirmationStore", factory)
    monkeypatch.setattr(runtime, "now_ns", clock.ns)

    def unsafe() -> GuardEvidence:
        if unhandled:
            raise TypeError("synthetic process bug")
        return guard_evidence(protocol, clock.ns()).model_copy(update={"order_attempts": 1})

    expected = TypeError if unhandled else RuntimeError
    with pytest.raises(expected):
        asyncio.run(runtime.run(protocol, database, guard=unsafe))
    report = build_report(ConfirmationStore(database, "read-only", clock=clock), at_ns=clock.ns())
    assert report["state"] == "failed_incomplete" and report["decision"] == NO_GO
    assert report["orders_submitted"] == 0 and report["private_kraken_requests"] == 0
    with pytest.raises(RuntimeError, match="no_restart"):
        factory(database, "replacement").claim(protocol)


@pytest.mark.parametrize(
    "changes",
    [
        {"order_attempts": 1},
        {"trading_authorization": "active"},
        {"model_state": "supported"},
        {"positions": 1},
        {"open_orders": 1},
        {"broker_order_records_since_freeze": 1},
        {"paper_host": "https://api.alpaca.markets"},
        {"read_methods": ("POST",)},
        {"v1_owner_id": "replacement"},
        {"account_digest": "x"},
    ],
)
def test_injected_guard_cannot_bypass_safety_with_model_copy(
    protocol: ConfirmationProtocol,
    clock: Clock,
    monkeypatch: pytest.MonkeyPatch,
    changes: dict[str, object],
) -> None:
    monkeypatch.setattr(runtime, "now_ns", clock.ns)
    with pytest.raises(ValueError):
        validate_guard(guard_evidence(protocol, clock.ns()).model_copy(update=changes), protocol)


def test_default_observer_guard_is_get_only_pinned_and_redirects_disabled(
    database: Database,
    protocol: ConfirmationProtocol,
    clock: Clock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frozen = ShadowDatasetProtocol(
        frozen_at=datetime(2026, 9, 29, tzinfo=UTC),
        code_sha="d" * 40,
        account_digest=protocol.account_digest,
    )
    shadow_datasets.create(database.engine)
    with database.begin() as connection:
        connection.execute(
            insert(shadow_datasets).values(
                dataset_id=frozen.dataset_id,
                protocol_hash=frozen.identity,
                protocol=frozen.model_dump(mode="json"),
                created_at=frozen.frozen_at,
                state="collecting",
                source_root=frozen.identity,
                source_batches=0,
                stored_bytes=0,
                quality={},
            )
        )
        connection.execute(
            insert(worker_locks).values(
                lock_name="tradeagent-event-worker",
                owner_id=protocol.v1_owner_id,
                acquired_at=datetime.now(UTC),
            )
        )
    monkeypatch.setattr(
        ProductionRepository,
        "latest_heartbeat",
        lambda self, name: (
            protocol.v1_owner_id,
            datetime.now(UTC),
            {
                "code_sha": protocol.v1_runtime_sha,
                "feed": {"connection_id": protocol.v1_connection_id, "subscribed": True},
            },
        ),
    )
    monkeypatch.setattr(
        runtime,
        "local_safety",
        lambda db, p: {
            "local_order_attempts_since_freeze": 0,
            "reported_order_attempts": 0,
            "trading_authorization_renewed": False,
            "reported_trading_authorization": "expired",
            "reported_model_state": "no_support",
        },
    )
    account_id = "synthetic-paper-account"
    digest = sha256(account_id.encode()).hexdigest()
    protocol = protocol.model_copy(update={"account_digest": digest})
    frozen = frozen.model_copy(update={"account_digest": digest})
    with database.begin() as connection:
        connection.execute(update(shadow_datasets).values(protocol=frozen.model_dump(mode="json")))
    monkeypatch.setenv("ALPACA_KEY_ID", "synthetic-test-key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "synthetic-test-secret")
    requests: list[httpx.Request] = []

    def response(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "GET" and request.url.host == "paper-api.alpaca.markets"
        data: JsonValue = (
            {"id": account_id, "status": "ACTIVE"} if request.url.path == "/v2/account" else []
        )
        return httpx.Response(200, json=data)

    original_client = httpx.Client

    def client(*, timeout: int, follow_redirects: bool) -> httpx.Client:
        assert not follow_redirects
        return original_client(
            timeout=timeout, follow_redirects=False, transport=httpx.MockTransport(response)
        )

    monkeypatch.setattr(httpx, "Client", client)
    evidence = observer_guard(database, protocol)()
    validate_guard(evidence, protocol)
    assert len(requests) == 4 and evidence.read_methods == ("GET",)


def test_migration_only_adds_isolated_table_and_refuses_populated_downgrade(
    database: Database,
    store: ConfirmationStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    migration = importlib.import_module("migrations.versions.0016_kraken_confirmation_evidence")
    assert len(migration.revision) <= 32
    assert migration.down_revision == "0015_shadow_research_dataset"
    with database.begin() as connection:
        monkeypatch.setattr(migration.op, "get_bind", lambda: connection)
        migration.upgrade()
        with pytest.raises(ValueError, match="cannot discard populated"):
            migration.downgrade()
    assert confirmation_evidence.metadata is not worker_locks.metadata
    assert set(confirmation_evidence.metadata.tables) == {"kraken_confirmation_evidence"}


def test_scoped_install_only_creates_research_tables_and_preserves_v1(
    database: Database,
    clock: Clock,
) -> None:
    with database.begin() as connection:
        confirmation_evidence.drop(connection)
        confirmation_schema_version.drop(connection)
        connection.execute(
            insert(worker_locks).values(
                lock_name="tradeagent-event-worker",
                owner_id="unchanged-v1",
                acquired_at=clock.at,
            )
        )
        v1_before = dict(connection.execute(select(worker_locks)).mappings().one())
    writes: list[str] = []

    def record_write(
        connection: Connection,
        cursor: object,
        statement: str,
        parameters: object,
        context: object,
        executemany: bool,
    ) -> None:
        if (
            statement.lstrip()
            .upper()
            .startswith(
                ("CREATE ", "INSERT ", "UPDATE ", "ALTER ", "DROP ", "DELETE "),
            )
        ):
            writes.append(statement)

    event.listen(database.engine, "before_cursor_execute", record_write)
    try:
        result = install_schema(database)
    finally:
        event.remove(database.engine, "before_cursor_execute", record_write)
    assert result == {
        "global_revision": "0015_shadow_research_dataset",
        "global_revision_unchanged": True,
        "research_revision": "0016_kraken_confirmation",
        "research_version_table": "kraken_confirmation_alembic_version",
        "evidence_table": "kraken_confirmation_evidence",
    }
    assert len(writes) == 3
    assert all("kraken_confirmation_" in statement for statement in writes)
    assert not any("worker_locks" in statement for statement in writes)
    assert set(inspect(database.engine).get_table_names()) == {
        "alembic_version",
        "worker_locks",
        "kraken_confirmation_evidence",
        "kraken_confirmation_alembic_version",
    }
    with database.begin() as connection:
        assert connection.scalar(select(legacy_version.c.version_num)) == (
            "0015_shadow_research_dataset"
        )
        assert dict(connection.execute(select(worker_locks)).mappings().one()) == v1_before
        assert connection.scalar(select(confirmation_evidence.c.sequence)) is None
    assert check_schema(database) == result


def test_scoped_install_is_idempotent_on_tracked_populated_evidence(
    store: ConfirmationStore,
) -> None:
    before = _all_rows(store.database)
    install_schema(store.database)
    install_schema(store.database)
    assert _all_rows(store.database) == before


def test_scoped_install_never_rewrites_incompatible_global_revision(
    database: Database,
) -> None:
    with database.begin() as connection:
        connection.execute(update(legacy_version).values(version_num="0016_kraken_confirmation"))
    with pytest.raises(RuntimeError, match="unchanged global 0015"):
        install_schema(database)
    with database.begin() as connection:
        assert connection.scalar(select(legacy_version.c.version_num)) == (
            "0016_kraken_confirmation"
        )
    with pytest.raises(RuntimeError, match="unchanged global 0015"):
        check_schema(database)


def test_scoped_install_refuses_missing_previously_tracked_evidence(
    database: Database,
) -> None:
    with database.begin() as connection:
        confirmation_evidence.drop(connection)
    with pytest.raises(RuntimeError, match="no replacement permitted"):
        install_schema(database)
    assert not inspect(database.engine).has_table(confirmation_evidence.name)


def test_scoped_install_refuses_populated_untracked_evidence(
    store: ConfirmationStore,
) -> None:
    before = _all_rows(store.database)
    with store.database.begin() as connection:
        connection.execute(delete(confirmation_schema_version))
    with pytest.raises(RuntimeError, match="populated untracked"):
        install_schema(store.database)
    assert _all_rows(store.database) == before
    with store.database.begin() as connection:
        assert connection.scalar(select(confirmation_schema_version.c.version_num)) is None


def test_scoped_install_refuses_foreign_auxiliary_revision(
    database: Database,
) -> None:
    with database.begin() as connection:
        connection.execute(update(confirmation_schema_version).values(version_num="foreign"))
    with pytest.raises(RuntimeError, match="conflicting auxiliary"):
        install_schema(database)
    with database.begin() as connection:
        assert connection.scalar(select(confirmation_schema_version.c.version_num)) == "foreign"


def test_scoped_install_refuses_wrong_shape_without_altering_it(
    database: Database,
) -> None:
    with database.begin() as connection:
        connection.execute(delete(confirmation_schema_version))
        confirmation_evidence.drop(connection)
        wrong = Table(confirmation_evidence.name, MetaData(), Column("unexpected", String))
        wrong.create(connection)
    with pytest.raises(RuntimeError, match="schema differs from source"):
        install_schema(database)
    assert [
        item["name"]
        for item in inspect(database.engine).get_columns(
            confirmation_evidence.name,
        )
    ] == ["unexpected"]


@dataclass
class CurrentV1:
    database: Database
    protocol: ConfirmationProtocol
    details: dict[str, JsonValue]
    instance_id: str
    stop: dict[str, JsonValue]
    observed_at: datetime | None = None
    account_id: str = "synthetic-paper-account"
    positions: list[JsonValue] = field(default_factory=list)
    open_orders: list[JsonValue] = field(default_factory=list)
    recent_orders: list[JsonValue] = field(default_factory=list)
    requests: list[httpx.Request] = field(default_factory=list)
    unavailable: bool = False

    @property
    def feed(self) -> dict[str, JsonValue]:
        value = self.details["feed"]
        assert isinstance(value, dict)
        return value

    def publish(self) -> None:
        ProductionRepository(self.database).heartbeat(
            "tradeagent-event-worker",
            self.instance_id,
            self.details,
            observed_at=self.observed_at or datetime.now(UTC),
        )

    def write_stop(self, *, repin: bool = False) -> None:
        raw = canonical(self.stop)
        ProductionRepository(self.database).set_control(STOP_KEY, raw)
        if repin:
            self.protocol = ConfirmationProtocol.model_validate(
                {
                    **self.protocol.model_dump(mode="json"),
                    "v1_stop_control_sha256": sha256(raw.encode()).hexdigest(),
                }
            )


@pytest.fixture
def current_v1(
    database: Database,
    protocol: ConfirmationProtocol,
    monkeypatch: pytest.MonkeyPatch,
) -> CurrentV1:
    for target in (controls, heartbeats, shadow_datasets, scalping_cycles, scalping_order_links):
        target.create(database.engine, checkfirst=True)
    account = "synthetic-paper-account"
    stop: dict[str, JsonValue] = {
        "reason": "BROKER_SAFETY_MONITOR_UNAVAILABLE",
        "automatic_rearm": False,
        "protocol_changed": False,
        "observed_at": "2027-01-07T11:00:00+00:00",
    }
    current = ConfirmationProtocol.model_validate(
        {
            **protocol.model_dump(mode="json"),
            "schema_version": V2_STUDY_ID,
            "study_id": V2_STUDY_ID,
            "account_digest": sha256(account.encode()).hexdigest(),
            "v1_stop_control_sha256": sha256(canonical(stop).encode()).hexdigest(),
            "v1_protocol_hash": ShadowDatasetProtocol(
                frozen_at=datetime(2026, 9, 29, tzinfo=UTC),
                code_sha="d" * 40,
                account_digest=sha256(account.encode()).hexdigest(),
            ).identity,
        }
    )
    frozen = ShadowDatasetProtocol(
        frozen_at=datetime(2026, 9, 29, tzinfo=UTC),
        code_sha="d" * 40,
        account_digest=current.account_digest,
    )
    with database.begin() as connection:
        connection.execute(
            insert(shadow_datasets).values(
                dataset_id=frozen.dataset_id,
                protocol_hash=frozen.identity,
                protocol=frozen.model_dump(mode="json"),
                created_at=frozen.frozen_at,
                state="paused_invalid",
                source_root=frozen.identity,
                source_batches=0,
                stored_bytes=0,
                quality={},
            )
        )
        connection.execute(
            insert(worker_locks).values(
                lock_name="tradeagent-event-worker",
                owner_id=current.v1_owner_id,
                acquired_at=datetime.now(UTC),
            )
        )
    scenario = CurrentV1(
        database=database,
        protocol=current,
        instance_id=current.v1_owner_id,
        stop=stop,
        details={
            "state": "paused_invalid",
            "code_sha": current.v1_runtime_sha,
            "account_digest": current.account_digest,
            "orders_submitted": 0,
            "trading_authorization": "expired",
            "model_state": "no_support",
            "economic_entries_enabled": False,
            "feed": {
                "state": "stopped",
                "connection_id": current.v1_connection_id,
                "authenticated": False,
                "subscribed": False,
            },
        },
    )
    scenario.write_stop()
    scenario.publish()
    original_client = httpx.Client

    def response(request: httpx.Request) -> httpx.Response:
        scenario.requests.append(request)
        assert request.method == "GET" and request.url.host == "paper-api.alpaca.markets"
        if scenario.unavailable:
            raise httpx.ReadTimeout("synthetic current broker unavailable", request=request)
        if request.url.path == "/v2/account":
            data: JsonValue = {"id": scenario.account_id, "status": "ACTIVE"}
        elif request.url.path == "/v2/positions":
            data = scenario.positions
        elif request.url.params.get("status") == "open":
            data = scenario.open_orders
        else:
            data = scenario.recent_orders
        return httpx.Response(200, json=data)

    def client(*, timeout: int, follow_redirects: bool) -> httpx.Client:
        assert timeout == 10 and not follow_redirects
        return original_client(
            timeout=timeout,
            follow_redirects=False,
            transport=httpx.MockTransport(response),
        )

    monkeypatch.setenv("ALPACA_KEY_ID", "synthetic-key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "synthetic-secret")
    monkeypatch.setattr(httpx, "Client", client)
    return scenario


def test_legacy_frozen_protocol_roundtrip_identity_is_exact() -> None:
    path = (
        Path(__file__).parents[1]
        / "docs"
        / "experiments"
        / "2026-10-book-confirmation-72h"
        / "frozen-protocol.json"
    )
    original = json.loads(path.read_bytes())
    protocol = ConfirmationProtocol.model_validate(original)
    assert protocol.model_dump(mode="json") == original
    assert "v1_stop_control_sha256" not in json.loads(protocol.model_dump_json())
    assert "v1_protocol_hash" not in json.loads(protocol.model_dump_json())
    assert protocol.identity == "4a232f3b8ad5e1a02db909f4cae86cc90f6ea25e01578491cab8f5e3839dc982"


@pytest.mark.parametrize(
    "changes",
    [
        {"schema_version": V2_STUDY_ID},
        {"study_id": V2_STUDY_ID},
        {
            "schema_version": "kraken-book-confirmation-v4",
            "study_id": "kraken-book-confirmation-v4",
        },
        {"schema_version": V2_STUDY_ID, "study_id": V2_STUDY_ID},
        {"v1_stop_control_sha256": "a" * 64},
        {"v1_protocol_hash": "a" * 64},
    ],
)
def test_version_pairs_and_required_stop_pin_are_closed(
    protocol: ConfirmationProtocol,
    changes: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        ConfirmationProtocol.model_validate({**protocol.model_dump(), **changes})


def test_current_preserved_paused_v1_and_real_read_only_safety_pass_without_mutation(
    current_v1: CurrentV1,
) -> None:
    def snapshot() -> list[list[dict[str, object]]]:
        with current_v1.database.begin() as connection:
            return [
                [dict(row) for row in connection.execute(select(target)).mappings()]
                for target in (
                    controls,
                    heartbeats,
                    worker_locks,
                    shadow_datasets,
                    confirmation_evidence,
                )
            ]

    before = snapshot()
    evidence = observer_guard(current_v1.database, current_v1.protocol)()
    validate_guard(evidence, current_v1.protocol)
    assert evidence.guard_version == 2 and evidence.preserved_v1 is not None
    assert evidence.preserved_v1.stop_control_sha256 == current_v1.protocol.v1_stop_control_sha256
    assert snapshot() == before
    assert len(current_v1.requests) == 4
    assert all(request.method == "GET" for request in current_v1.requests)


def test_legacy_stopped_subscription_guard_remains_rejected(current_v1: CurrentV1) -> None:
    legacy = ConfirmationProtocol.model_validate(
        {
            **current_v1.protocol.model_dump(mode="json"),
            "schema_version": STUDY_ID,
            "study_id": STUDY_ID,
            "v1_stop_control_sha256": None,
            "v1_protocol_hash": None,
        }
    )
    with pytest.raises(ValueError, match="pinned source"):
        observer_guard(current_v1.database, legacy)()
    assert current_v1.requests == []
    legacy_evidence = guard_evidence(legacy, datetime_ns(datetime.now(UTC)))
    assert "guard_version" not in legacy_evidence.model_dump(mode="json")
    assert "preserved_v1" not in legacy_evidence.model_dump(mode="json")


@pytest.mark.parametrize("which", ["absent", "mismatch", "malformed"])
def test_v2_rejects_missing_changed_or_malformed_stop(current_v1: CurrentV1, which: str) -> None:
    repo = ProductionRepository(current_v1.database)
    if which == "absent":
        with current_v1.database.begin() as connection:
            connection.execute(delete(controls).where(controls.c.control_key == STOP_KEY))
    elif which == "mismatch":
        repo.set_control(STOP_KEY, canonical({**current_v1.stop, "extra": "changed"}))
    else:
        raw = "{bad-json"
        repo.set_control(STOP_KEY, raw)
        current_v1.protocol = ConfirmationProtocol.model_validate(
            {
                **current_v1.protocol.model_dump(mode="json"),
                "v1_stop_control_sha256": sha256(raw.encode()).hexdigest(),
            }
        )
    with pytest.raises(ValueError):
        observer_guard(current_v1.database, current_v1.protocol)()
    assert current_v1.requests == []


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("reason", "QUALITY_PAUSE"),
        ("reason", None),
        ("automatic_rearm", True),
        ("automatic_rearm", None),
        ("automatic_rearm", 0),
        ("protocol_changed", True),
        ("protocol_changed", None),
        ("protocol_changed", 0),
    ],
)
def test_v2_rejects_wrong_stop_semantics_even_when_hash_matches(
    current_v1: CurrentV1,
    key: str,
    value: JsonValue,
) -> None:
    current_v1.stop[key] = value
    current_v1.write_stop(repin=True)
    with pytest.raises(ValueError, match="containment"):
        observer_guard(current_v1.database, current_v1.protocol)()
    assert current_v1.requests == []


@pytest.mark.parametrize(
    ("scope", "key", "value"),
    [
        ("details", "state", "collecting"),
        ("details", "state", None),
        ("details", "code_sha", "0" * 40),
        ("details", "account_digest", "0" * 64),
        ("details", "orders_submitted", 1),
        ("details", "orders_submitted", None),
        ("details", "trading_authorization", "active"),
        ("details", "model_state", "supported"),
        ("details", "economic_entries_enabled", True),
        ("details", "economic_entries_enabled", None),
        ("details", "ordinary_entries_enabled", True),
        ("feed", "state", "streaming"),
        ("feed", "subscribed", True),
        ("feed", "subscribed", None),
        ("feed", "authenticated", True),
        ("feed", "authenticated", None),
        ("feed", "connection_id", "changed"),
    ],
)
def test_v2_rejects_changed_source_state_and_authority(
    current_v1: CurrentV1,
    scope: str,
    key: str,
    value: JsonValue,
) -> None:
    target = current_v1.details if scope == "details" else current_v1.feed
    target[key] = value
    current_v1.publish()
    with pytest.raises(ValueError):
        observer_guard(current_v1.database, current_v1.protocol)()
    assert current_v1.requests == []


@pytest.mark.parametrize(
    "which",
    [
        "heartbeat-owner",
        "lease-owner",
        "heartbeat-stale",
        "lease-stale",
        "heartbeat-future",
        "lease-future",
    ],
)
def test_v2_keeps_exact_identity_and_clock_fences(current_v1: CurrentV1, which: str) -> None:
    if which.startswith("heartbeat"):
        if which.endswith("owner"):
            current_v1.instance_id = "changed"
        else:
            current_v1.observed_at = datetime.now(UTC) + timedelta(
                seconds=5 if which.endswith("future") else -31,
            )
        current_v1.publish()
    else:
        values: dict[str, object] = (
            {"owner_id": "changed"}
            if which.endswith("owner")
            else {
                "acquired_at": datetime.now(UTC)
                + timedelta(
                    seconds=5 if which.endswith("future") else -91,
                )
            }
        )
        with current_v1.database.begin() as connection:
            connection.execute(
                update(worker_locks)
                .where(
                    worker_locks.c.lock_name == "tradeagent-event-worker",
                )
                .values(**values)
            )
    with pytest.raises(ValueError, match="pinned source"):
        observer_guard(current_v1.database, current_v1.protocol)()


@pytest.mark.parametrize(
    "which", ["positions", "open-orders", "recent-orders", "account", "unavailable"]
)
def test_v2_requires_direct_current_broker_proof(current_v1: CurrentV1, which: str) -> None:
    if which == "positions":
        current_v1.positions = [{"symbol": "unsafe"}]
    elif which == "open-orders":
        current_v1.open_orders = [{"id": "unsafe"}]
    elif which == "recent-orders":
        current_v1.recent_orders = [{"id": "unsafe"}]
    elif which == "account":
        current_v1.account_id = "wrong-account"
    else:
        current_v1.unavailable = True
    with pytest.raises((ValueError, runtime.CurrentBrokerProofError)):
        observer_guard(current_v1.database, current_v1.protocol)()


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("local_order_attempts_since_freeze", 1),
        ("local_order_attempts_since_freeze", None),
        ("reported_order_attempts", 1),
        ("reported_order_attempts", None),
        ("trading_authorization_renewed", True),
        ("trading_authorization_renewed", None),
        ("reported_trading_authorization", "active"),
        ("reported_model_state", "supported"),
    ],
)
def test_v2_rejects_unsafe_or_unavailable_local_safety(
    current_v1: CurrentV1,
    monkeypatch: pytest.MonkeyPatch,
    key: str,
    value: object,
) -> None:
    frozen = ShadowDatasetProtocol(
        frozen_at=datetime(2026, 9, 29, tzinfo=UTC),
        code_sha="d" * 40,
        account_digest=current_v1.protocol.account_digest,
    )
    safe: dict[str, object] = local_safety(current_v1.database, frozen)
    monkeypatch.setattr(runtime, "local_safety", lambda db, p: {**safe, key: value})
    with pytest.raises(ValueError, match="guard failed"):
        observer_guard(current_v1.database, current_v1.protocol)()


@pytest.mark.parametrize("change", ["feed", "stop", "schema", "local"])
def test_v2_rechecks_stop_and_source_after_current_broker_gets(
    current_v1: CurrentV1,
    monkeypatch: pytest.MonkeyPatch,
    change: str,
) -> None:
    original = PaperReadOnlyMonitor.snapshot

    def changed(monitor: PaperReadOnlyMonitor, frozen: ShadowDatasetProtocol) -> dict[str, object]:
        broker: dict[str, object] = original(monitor, frozen)
        if change == "feed":
            current_v1.feed["subscribed"] = True
            current_v1.publish()
        elif change == "stop":
            current_v1.stop["extra"] = "changed during broker reads"
            current_v1.write_stop()
        elif change == "schema":
            with current_v1.database.begin() as connection:
                connection.execute(
                    update(legacy_version).values(
                        version_num="0016_kraken_confirmation",
                    )
                )
        else:
            current_v1.details["orders_submitted"] = 1
            current_v1.publish()
        return broker

    monkeypatch.setattr(PaperReadOnlyMonitor, "snapshot", changed)
    with pytest.raises((ValueError, RuntimeError)):
        observer_guard(current_v1.database, current_v1.protocol)()


def test_v2_guard_cannot_be_omitted_or_replaced_by_legacy_evidence(
    current_v1: CurrentV1,
    monkeypatch: pytest.MonkeyPatch,
    clock: Clock,
) -> None:
    monkeypatch.setattr(runtime, "now_ns", clock.ns)
    legacy = guard_evidence(current_v1.protocol, clock.ns())
    with pytest.raises(ValueError, match="requires its exact"):
        validate_guard(legacy, current_v1.protocol)
    bad = legacy.model_copy(
        update={
            "guard_version": 2,
            "preserved_v1": PausedV1Proof(
                stop_control_sha256="0" * 64,
                protocol_hash="d" * 64,
            ),
        }
    )
    with pytest.raises(ValueError, match="requires its exact"):
        validate_guard(bad, current_v1.protocol)


def test_read_only_preflight_before_any_claim_and_source_pins_remain_exact(
    current_v1: CurrentV1,
    store: ConfirmationStore,
    clock: Clock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(runtime, "now_ns", clock.ns)
    evidence = observer_guard(current_v1.database, current_v1.protocol)().model_copy(
        update={"checked_ns": clock.ns()},
    )
    writes: list[str] = []

    def record_writes(
        connection: Connection,
        cursor: object,
        statement: str,
        parameters: object,
        context: object,
        executemany: bool,
    ) -> None:
        if (
            statement.lstrip()
            .upper()
            .startswith(("INSERT ", "UPDATE ", "CREATE ", "DELETE ", "ALTER "))
        ):
            writes.append(statement)

    event.listen(current_v1.database.engine, "before_cursor_execute", record_writes)
    try:
        report = asyncio.run(
            preflight(
                current_v1.protocol,
                current_v1.database,
                guard=lambda: evidence,
            )
        )
    finally:
        event.remove(current_v1.database.engine, "before_cursor_execute", record_writes)
    assert report["preflight"] == "passed" and report["claim_created"] is False
    assert report["guard_verified"] is True
    assert report["market_confirmation_evaluated"] is False
    assert report["clean_integrity"] is False
    assert report["decision"] == "PREFLIGHT_ONLY_NOT_MARKET_CONFIRMATION"
    assert report["v2_ready"] is False
    assert not ConfirmationStore(current_v1.database, "read-only", study_id=V2_STUDY_ID).exists()
    assert writes == []
    wrong = current_v1.protocol.model_copy(
        update={
            "source_hashes": tuple(
                SourceDigest(name=name, sha256="0" * 64) for name in SOURCE_MODULES
            ),
        }
    )
    with pytest.raises(ValueError, match="source modules differ"):
        asyncio.run(preflight(wrong, current_v1.database, guard=lambda: evidence))
    with pytest.raises(ValueError, match="requires its exact"):
        asyncio.run(
            preflight(
                current_v1.protocol,
                current_v1.database,
                guard=lambda: guard_evidence(current_v1.protocol, clock.ns()),
            )
        )


def test_two_literal_studies_coexist_without_original_root_or_row_change(
    store: ConfirmationStore,
    current_v1: CurrentV1,
    clock: Clock,
) -> None:
    store.append(
        "terminal",
        "terminal",
        {
            "state": "failed_incomplete",
            "reason": "original startup guard failure",
        },
        at_ns=clock.ns(),
    )
    original = list(export_evidence(store.database))
    original_rows = [row for row in _all_rows(store.database) if row["study_id"] == STUDY_ID]
    newer = ConfirmationStore(
        store.database,
        "confirmation-v2-owner",
        clock=clock,
        study_id=V2_STUDY_ID,
    )
    newer.claim(current_v1.protocol)
    newer.append("quality", "quality:final", {"counts": {"frames": 0}}, at_ns=clock.ns())
    assert list(export_evidence(store.database)) == original
    assert [
        row for row in _all_rows(store.database) if row["study_id"] == STUDY_ID
    ] == original_rows
    assert newer.load_protocol().study_id == V2_STUDY_ID
    assert all(
        ChunkHeader.model_validate(item["header"]).study_id == V2_STUDY_ID
        for item in export_evidence(store.database, study_id=V2_STUDY_ID)
    )
    assert load_report(store.database)["state"] == "failed_incomplete"
    newer_report = load_report(store.database, study_id=V2_STUDY_ID, verify_payloads=True)
    new_protocol = newer_report["protocol"]
    assert isinstance(new_protocol, dict) and new_protocol["study_id"] == V2_STUDY_ID
    assert newer_report["journal_chunks"] == 2
    with pytest.raises(RuntimeError, match="no_restart"):
        newer.claim(current_v1.protocol)
    with pytest.raises(RuntimeError, match="no_restart"):
        store.claim(store.load_protocol())


def test_unknown_study_selector_never_falls_back_to_original(database: Database) -> None:
    with pytest.raises(ValueError, match="unknown confirmation"):
        ConfirmationStore(database, "read-only", study_id="unknown")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="unknown confirmation"):
        list(export_evidence(database, study_id="unknown"))  # type: ignore[arg-type]


def test_v2_preflight_requires_existing_auxiliary_and_global_0015_schema(
    current_v1: CurrentV1,
    clock: Clock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(runtime, "now_ns", clock.ns)
    with current_v1.database.begin() as connection:
        connection.execute(update(legacy_version).values(version_num="0016_kraken_confirmation"))
    with pytest.raises(RuntimeError, match="unchanged global 0015"):
        asyncio.run(preflight(current_v1.protocol, current_v1.database))
    assert current_v1.requests == []
    with current_v1.database.begin() as connection:
        connection.execute(
            update(legacy_version).values(version_num=confirmation.LEGACY_SCHEMA_REVISION)
        )
        confirmation_schema_version.drop(connection)
    with pytest.raises(RuntimeError, match="research table missing"):
        asyncio.run(preflight(current_v1.protocol, current_v1.database))
    assert not inspect(current_v1.database.engine).has_table(confirmation_schema_version.name)


def test_v2_unavailable_current_broker_fails_before_claim_without_touching_original(
    current_v1: CurrentV1,
    store: ConfirmationStore,
    clock: Clock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = list(export_evidence(store.database))
    monkeypatch.setattr(runtime, "now_ns", clock.ns)
    current_v1.unavailable = True
    with pytest.raises(runtime.CurrentBrokerProofError):
        asyncio.run(runtime.run(current_v1.protocol, store.database))
    assert not ConfirmationStore(store.database, "read-only", study_id=V2_STUDY_ID).exists()
    assert list(export_evidence(store.database)) == original


def test_v2_post_claim_failure_seals_only_its_own_namespace(
    current_v1: CurrentV1,
    store: ConfirmationStore,
    clock: Clock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = list(export_evidence(store.database))
    evidence = observer_guard(current_v1.database, current_v1.protocol)().model_copy(
        update={"checked_ns": clock.ns()},
    )
    monkeypatch.setattr(runtime, "now_ns", clock.ns)

    def factory(
        database: Database, owner: str, *, study_id: StudyId = STUDY_ID
    ) -> ConfirmationStore:
        return ConfirmationStore(database, owner, clock=clock, study_id=study_id)

    async def failed_capture(receiver: Receiver, stop: asyncio.Event) -> None:
        raise RuntimeError("synthetic isolated capture failure")

    monkeypatch.setattr(runtime, "ConfirmationStore", factory)
    monkeypatch.setattr(Receiver, "work", failed_capture)
    with pytest.raises(RuntimeError, match="sealed failed/incomplete"):
        asyncio.run(
            runtime.run(
                current_v1.protocol,
                store.database,
                guard=lambda: evidence,
            )
        )
    assert list(export_evidence(store.database)) == original
    report = load_report(store.database, study_id=V2_STUDY_ID, verify_payloads=True)
    assert report["state"] == "failed_incomplete" and report["guard_checks"] == 1
    assert report["raw_frames_verified"] == 0 and report["decision"] == NO_GO
    with store.database.begin() as connection:
        assert (
            connection.scalar(
                select(worker_locks.c.owner_id).where(
                    worker_locks.c.lock_name == "research:" + V2_STUDY_ID,
                )
            )
            is None
        )
    with pytest.raises(RuntimeError, match="no_restart"):
        factory(store.database, "new-owner", study_id=V2_STUDY_ID).claim(current_v1.protocol)


def test_v2_durable_guard_proof_cannot_omit_its_stop_pin(
    current_v1: CurrentV1,
    clock: Clock,
) -> None:
    store = ConfirmationStore(
        current_v1.database,
        "new-owner",
        clock=clock,
        study_id=V2_STUDY_ID,
    )
    store.claim(current_v1.protocol)
    legacy = guard_evidence(current_v1.protocol, clock.ns())
    store.append("guard", "bad", legacy.model_dump(mode="json"), at_ns=clock.ns())
    with pytest.raises(ValueError, match="guard version missing"):
        build_report(store, at_ns=clock.ns())


@pytest.mark.parametrize(
    ("command", "selector"),
    [
        ("status", STUDY_ID),
        ("status", V2_STUDY_ID),
        ("export", STUDY_ID),
        ("export", V2_STUDY_ID),
    ],
)
def test_read_only_cli_selects_exact_literal_namespace(
    current_v1: CurrentV1,
    store: ConfirmationStore,
    clock: Clock,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    command: str,
    selector: StudyId,
) -> None:
    store.append("terminal", "terminal", {"state": "failed_incomplete"}, at_ns=clock.ns())
    newer = ConfirmationStore(
        store.database,
        "new-owner",
        clock=clock,
        study_id=V2_STUDY_ID,
    )
    newer.claim(current_v1.protocol)
    before = _all_rows(store.database)

    @contextmanager
    def diagnostic_database(url: str, *, pool_size: int) -> Iterator[Database]:
        yield store.database

    monkeypatch.setattr(runtime, "Database", diagnostic_database)
    monkeypatch.setattr(
        runtime,
        "AppConfig",
        lambda: SimpleNamespace(
            database_url=SecretStr("sqlite://"),
        ),
    )
    args = ["confirmation", command]
    if selector != STUDY_ID:
        args.extend(["--study-id", selector])
    monkeypatch.setattr(sys, "argv", args)
    runtime.main()
    output = capsys.readouterr().out
    if command == "status":
        assert json.loads(output)["protocol"]["study_id"] == selector
    else:
        assert all(
            json.loads(line)["header"]["study_id"] == selector for line in output.splitlines()
        )
    assert _all_rows(store.database) == before


def test_preflight_cli_routes_without_install_or_claim(
    current_v1: CurrentV1,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    @contextmanager
    def diagnostic_database(url: str, *, pool_size: int) -> Iterator[Database]:
        yield current_v1.database

    class SuppliedProtocol:
        def __init__(self, value: str) -> None:
            assert value == "PROTOCOL_V2.json"

        def read_bytes(self) -> bytes:
            return current_v1.protocol.model_dump_json().encode()

    async def checked(protocol: ConfirmationProtocol, database: Database) -> dict[str, JsonValue]:
        assert protocol.study_id == V2_STUDY_ID
        assert not ConfirmationStore(database, "read-only", study_id=V2_STUDY_ID).exists()
        return {"preflight": "passed", "claim_created": False}

    monkeypatch.setattr(runtime, "Database", diagnostic_database)
    monkeypatch.setattr(
        runtime, "AppConfig", lambda: SimpleNamespace(database_url=SecretStr("sqlite://"))
    )
    monkeypatch.setattr(runtime, "Path", SuppliedProtocol)
    monkeypatch.setattr(runtime, "preflight", checked)
    monkeypatch.setattr(sys, "argv", ["confirmation", "preflight", "PROTOCOL_V2.json"])
    runtime.main()
    assert json.loads(capsys.readouterr().out)["claim_created"] is False


def test_cli_rejects_unknown_id_before_database_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        sys, "argv", ["confirmation", "status", "--study-id", "kraken-book-confirmation-v4"]
    )
    with pytest.raises(SystemExit) as error:
        runtime.main()
    assert error.value.code == 2


@pytest.mark.parametrize("which", ["pin", "stored-hash", "actual-protocol"])
def test_v2_requires_the_original_actual_shadow_protocol_identity(
    current_v1: CurrentV1,
    which: str,
) -> None:
    if which == "pin":
        current_v1.protocol = ConfirmationProtocol.model_validate(
            {
                **current_v1.protocol.model_dump(mode="json"),
                "v1_protocol_hash": "0" * 64,
            }
        )
    elif which == "stored-hash":
        with current_v1.database.begin() as connection:
            connection.execute(update(shadow_datasets).values(protocol_hash="0" * 64))
    else:
        with current_v1.database.begin() as connection:
            value = connection.execute(select(shadow_datasets.c.protocol)).scalar_one()
            frozen = ShadowDatasetProtocol.model_validate(value)
            changed = frozen.model_copy(update={"fee_unknown_reason": "changed after freeze"})
            connection.execute(
                update(shadow_datasets).values(protocol=changed.model_dump(mode="json"))
            )
    with pytest.raises(ValueError, match="shadow protocol identity changed"):
        observer_guard(current_v1.database, current_v1.protocol)()
    assert current_v1.requests == []


def test_production_like_preflight_preserves_exact_original_archive_identity(
    current_v1: CurrentV1,
    clock: Clock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = (
        Path(__file__).parents[1]
        / "docs"
        / "experiments"
        / "2026-10-book-confirmation-72h"
        / "frozen-protocol.json"
    )
    original = ConfirmationProtocol.model_validate_json(path.read_bytes())
    old_clock = Clock(original.frozen_at + timedelta(seconds=1))
    archived = ConfirmationStore(current_v1.database, "archived-original", clock=old_clock)
    archived.claim(original)
    archived.append(
        "terminal",
        "terminal",
        {"state": "failed_incomplete", "reason": "original startup failure"},
        at_ns=old_clock.ns(),
    )
    before = list(export_evidence(current_v1.database))
    compat = legacy_journal_compatibility(current_v1.database)
    assert compat["protocol_hash"] == (
        "4a232f3b8ad5e1a02db909f4cae86cc90f6ea25e01578491cab8f5e3839dc982"
    )
    evidence = observer_guard(current_v1.database, current_v1.protocol)().model_copy(
        update={"checked_ns": clock.ns()},
    )
    monkeypatch.setattr(runtime, "now_ns", clock.ns)
    checked = asyncio.run(
        preflight(
            current_v1.protocol,
            current_v1.database,
            guard=lambda: evidence,
        )
    )
    assert checked["legacy_journal_compatibility"] == compat
    assert checked["clean_integrity"] is False
    assert list(export_evidence(current_v1.database)) == before


def test_safe_broker_diagnostics_exclude_body_headers_queries_and_secrets() -> None:
    request = httpx.Request(
        "GET",
        "https://paper-api.alpaca.markets/v2/orders?secret=never-log-this",
        headers={"APCA-API-KEY-ID": "never-log-key"},
    )
    response = httpx.Response(403, request=request, text="never-log-body")
    error = runtime.CurrentBrokerProofError(
        httpx.HTTPStatusError(
            "never-log-message",
            request=request,
            response=response,
        )
    )
    assert error.details == {
        "error_type": "HTTPStatusError",
        "endpoint": "/v2/orders",
        "http_status": 403,
    }
    assert "never-log" not in str(error)
    timeout = runtime.CurrentBrokerProofError(
        httpx.ReadTimeout(
            "never-log-message",
            request=request,
        )
    )
    assert timeout.details == {
        "error_type": "ReadTimeout",
        "endpoint": "/v2/orders",
        "http_status": None,
    }
    assert "never-log" not in str(timeout)


def test_post_claim_new_broker_failure_persists_safe_endpoint_and_status(
    current_v1: CurrentV1,
    store: ConfirmationStore,
    clock: Clock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = list(export_evidence(store.database))
    evidence = observer_guard(current_v1.database, current_v1.protocol)().model_copy(
        update={"checked_ns": clock.ns()},
    )
    request = httpx.Request("GET", "https://paper-api.alpaca.markets/v2/positions")
    response = httpx.Response(503, request=request, text="never-log-body")
    error = runtime.CurrentBrokerProofError(
        httpx.HTTPStatusError(
            "never-log-message",
            request=request,
            response=response,
        )
    )
    calls = 0

    def current_proof() -> GuardEvidence:
        nonlocal calls
        calls += 1
        if calls >= 3:
            raise error
        return evidence

    def factory(
        database: Database, owner: str, *, study_id: StudyId = STUDY_ID
    ) -> ConfirmationStore:
        return ConfirmationStore(database, owner, clock=clock, study_id=study_id)

    async def waiting_capture(receiver: Receiver, stop: asyncio.Event) -> None:
        await stop.wait()

    monkeypatch.setattr(runtime, "ConfirmationStore", factory)
    monkeypatch.setattr(runtime, "now_ns", clock.ns)
    monkeypatch.setattr(Receiver, "work", waiting_capture)
    with pytest.raises(RuntimeError, match="sealed failed/incomplete"):
        asyncio.run(runtime.run(current_v1.protocol, store.database, guard=current_proof))
    result = load_report(store.database, study_id=V2_STUDY_ID, verify_payloads=True)
    assert "503" in str(result["terminal_reason"])
    assert "/v2/positions" in str(result["terminal_reason"])
    assert "HTTPStatusError" in str(result["terminal_reason"])
    assert "never-log" not in canonical(result)
    quality = result["latest_quality"]
    assert isinstance(quality, dict) and "HTTPStatusError" in str(quality["last_error"])
    assert list(export_evidence(store.database)) == original


def protocol_v3(value: ConfirmationProtocol) -> ConfirmationProtocol:
    return ConfirmationProtocol.model_validate(
        {
            **value.model_dump(mode="json"),
            "schema_version": V3_STUDY_ID,
            "study_id": V3_STUDY_ID,
            "warmup_seconds": 60,
        }
    )


def test_demonstrated_legacy_slow_report_expires_lease_and_misses_grid(
    current_v1: CurrentV1,
    clock: Clock,
) -> None:
    protocol = current_v1.protocol
    store = ConfirmationStore(
        current_v1.database,
        "legacy-simulation",
        clock=clock,
        study_id=V2_STUDY_ID,
    )
    store.claim(protocol)
    clock.set(protocol.slot_ns(360))
    store.repository.refresh_worker_lock(store.lock_name, store.owner_id, observed_at=clock.at)
    grid = CausalGrid(protocol)
    grid.next_slot = 360
    grid.advance(clock.ns())
    renewed = clock.at

    async def original_await_path() -> None:
        def cumulative_report() -> None:
            clock.at += timedelta(seconds=110)

        await asyncio.to_thread(cumulative_report)
        evaluations, _ = grid.advance(clock.ns())
        assert sum(row.missed_scheduled_slot for row in evaluations) == 20
        with pytest.raises(RuntimeError, match="lease_expired"):
            store.refresh()

    asyncio.run(original_await_path())
    with store.database.begin() as connection:
        stored = connection.scalar(
            select(worker_locks.c.acquired_at).where(
                worker_locks.c.lock_name == store.lock_name,
            )
        )
    assert stored is not None and stored.replace(tzinfo=UTC) == renewed


@pytest.mark.parametrize("report_stall_seconds", [71, 82, 110])
def test_demonstrated_report_stall_evicts_otherwise_fresh_horizon_from_ring(
    protocol: ConfirmationProtocol,
    report_stall_seconds: int,
) -> None:
    grid = CausalGrid(protocol)
    start = protocol.slot_ns(0)
    horizon: dict[str, Quote] = {}
    resolved: list[Label] = []
    resolve_second = 60 + report_stall_seconds
    for second in range(resolve_second + 1):
        current = start + second * NS
        for offset, symbol in enumerate(confirmation.SYMBOLS):
            item = quote(current, symbol=symbol, frame=second * 2 + offset + 1)
            grid.accept(item)
            if second == 60:
                horizon[symbol] = item
        if second < 60 or second == resolve_second:
            _, labels = grid.advance(current)
            resolved.extend(labels)
    first = [item for item in resolved if item.evaluation.slot == 0]
    assert len(first) == 2
    for label in first:
        assert label.evaluation.entry is not None
        assert measure(label.evaluation.entry, horizon[label.evaluation.symbol], start).complete
        assert label.future is None
        assert label.resolved_ns == start + resolve_second * NS
        assert label.information_available_ns == start + 62 * NS
        assert "HORIZON_QUOTE_STALE_OR_MISSING" in label.eligibility.reasons


def test_owner_fence_samples_clock_after_database_row_acquisition(
    store: ConfirmationStore,
    clock: Clock,
) -> None:
    original = clock.at
    advanced = False

    def delayed_lock(
        connection: Connection,
        cursor: object,
        statement: str,
        parameters: object,
        context: object,
        executemany: bool,
    ) -> None:
        nonlocal advanced
        if (
            not advanced
            and statement.lstrip().upper().startswith("SELECT")
            and "worker_locks" in statement
        ):
            advanced = True
            clock.at += timedelta(seconds=91)

    event.listen(store.database.engine, "before_cursor_execute", delayed_lock)
    try:
        with pytest.raises(RuntimeError, match="lease_expired"):
            store.refresh()
    finally:
        event.remove(store.database.engine, "before_cursor_execute", delayed_lock)
    with store.database.begin() as connection:
        retained = connection.scalar(select(worker_locks.c.acquired_at))
    assert retained is not None and retained.replace(tzinfo=UTC) == original


def test_v3_bounded_warmup_and_legacy_v2_identity_are_exact(
    current_v1: CurrentV1,
) -> None:
    third = protocol_v3(current_v1.protocol)
    assert third.warmup_seconds == 60
    for seconds in (0, 61, 3600):
        with pytest.raises(ValidationError):
            ConfirmationProtocol.model_validate(
                {
                    **third.model_dump(mode="json"),
                    "warmup_seconds": seconds,
                }
            )
    with pytest.raises(ValidationError):
        ConfirmationProtocol.model_validate(
            {
                **current_v1.protocol.model_dump(mode="json"),
                "warmup_seconds": 60,
            }
        )
    path = (
        Path(__file__).parents[1]
        / "docs"
        / "experiments"
        / "2026-10-book-confirmation-72h-v2"
        / "frozen-protocol.json"
    )
    old = json.loads(path.read_bytes())
    parsed = ConfirmationProtocol.model_validate(old)
    assert parsed.model_dump(mode="json") == old
    assert parsed.identity == "e05f3c559cfc3841c8492500aeebdecbcb2812e0b2f4bc743c549ee9ed560f3a"
    assert "warmup_seconds" not in parsed.model_dump(mode="json")


def test_v3_early_launch_rejects_without_capture_claim_or_io(
    current_v1: CurrentV1,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    third = protocol_v3(current_v1.protocol)
    monkeypatch.setattr(runtime, "now_ns", lambda: datetime_ns(third.start) - 4 * 86400 * NS)
    with pytest.raises(ValueError, match="sixty seconds"):
        asyncio.run(runtime.run(third, current_v1.database))
    assert not ConfirmationStore(
        current_v1.database,
        "read-only",
        study_id=V3_STUDY_ID,
    ).exists()
    assert current_v1.requests == []


def test_slow_report_lane_does_not_starve_lease_cadence_or_safety(
    current_v1: CurrentV1,
    clock: Clock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    third = protocol_v3(current_v1.protocol)
    evidence = observer_guard(current_v1.database, third)()
    store = ConfirmationStore(
        current_v1.database,
        "third-simulation",
        clock=clock,
        study_id=V3_STUDY_ID,
    )
    store.claim(third)
    clock.set(third.slot_ns(360))
    store.repository.refresh_worker_lock(store.lock_name, store.owner_id, observed_at=clock.at)
    monkeypatch.setattr(runtime, "now_ns", clock.ns)
    entered, release = threading.Event(), threading.Event()
    report_threads: list[int] = []
    safety_times: list[int] = []

    def slow_report(
        reader: ConfirmationStore,
        *,
        at_ns: int,
        cancel: threading.Event | None = None,
    ) -> dict[str, JsonValue]:
        report_threads.append(threading.get_ident())
        entered.set()
        while not release.wait(0.001):
            if cancel is not None and cancel.is_set():
                raise confirmation.ReportCancelledError("test report cancelled")
        return {"as_of_ns": at_ns, "clean_integrity": False, "decision": NO_GO}

    async def virtual_pause(stop: asyncio.Event, seconds: float) -> None:
        until = clock.ns() + int(seconds * NS)
        while not stop.is_set() and clock.ns() < until:
            await asyncio.sleep(0.001)

    def safe() -> GuardEvidence:
        safety_times.append(clock.ns())
        return evidence.model_copy(update={"checked_ns": clock.ns()})

    monkeypatch.setattr(runtime, "build_report", slow_report)
    monkeypatch.setattr(runtime, "_pause", virtual_pause)

    async def simulation() -> None:
        io = runtime.RuntimeIO(
            current_v1.database,
            lease_database=current_v1.database,
            safety_database=current_v1.database,
            report_database=current_v1.database,
        )
        writer = DurableWriter(store)
        writer.task = asyncio.create_task(writer.work())
        stop = asyncio.Event()
        pulse = asyncio.create_task(runtime._lease_heartbeat(store, io, stop))
        safety = asyncio.create_task(runtime._safety_watch(third, safe, io, writer, stop))
        reporting = asyncio.create_task(runtime._report_watch(third, store, io, writer, stop))
        grid = CausalGrid(third)
        grid.next_slot = 354
        report_boundary = clock.ns()
        for second in range(-60, 1):
            current = report_boundary + second * NS
            for offset, symbol in enumerate(confirmation.SYMBOLS):
                grid.accept(
                    quote(current, symbol=symbol, frame=(second + 60) * 2 + offset + 1),
                )
            grid.advance(current)
        missed = 0
        resolved: list[Label] = []
        try:
            for _ in range(1000):
                if entered.is_set():
                    break
                await asyncio.sleep(0.001)
            assert entered.is_set()
            for second in range(110):
                clock.at += timedelta(seconds=1)
                for offset, symbol in enumerate(confirmation.SYMBOLS):
                    grid.accept(
                        quote(clock.ns(), symbol=symbol, frame=(second + 61) * 2 + offset + 1),
                    )
                evaluations, labels = grid.advance(clock.ns())
                resolved.extend(labels)
                missed += sum(row.missed_scheduled_slot for row in evaluations)
                assert all(row.eligibility.complete and row.future is not None for row in labels)
                assert all(
                    row.information_available_ns == row.evaluation.at_ns + 62 * NS for row in labels
                )
                await asyncio.sleep(0.005)
            assert missed == 0
            first = [row for row in resolved if row.evaluation.slot == 354]
            assert len(first) == 2
            assert all(
                row.resolved_ns == report_boundary + 2 * NS
                and row.future is not None
                and row.future.accepted_ns == report_boundary
                for row in first
            )
            assert not pulse.done() and not safety.done()
            store.refresh()
            assert len(safety_times) >= 2
            release.set()
            for _ in range(1000):
                if store.latest_header("report") is not None:
                    break
                await asyncio.sleep(0.001)
            assert store.latest_header("report") is not None
            assert report_threads
        finally:
            stop.set()
            release.set()
            await asyncio.gather(pulse, safety, reporting)
            await writer.close()
            await io.close()

    asyncio.run(simulation())


def test_read_only_report_cancellation_is_explicit_and_targeted_payload_api(
    store: ConfirmationStore,
    clock: Clock,
) -> None:
    store.append("quality", "latest", {"counts": {"frames": 0}}, at_ns=clock.ns())
    located = store.latest_header("quality")
    assert located is not None
    header, payload = store.read_chunk(located.sequence)
    assert header == located and payload == {"counts": {"frames": 0}}
    cancel = threading.Event()
    cancel.set()
    with pytest.raises(confirmation.ReportCancelledError):
        list(store.chunks(cancel=cancel))
    with pytest.raises(confirmation.ReportCancelledError):
        build_report(store, at_ns=clock.ns(), cancel=cancel)


def test_runtime_io_has_distinct_bounded_threads_and_no_default_executor_dependency(
    database: Database,
) -> None:
    async def simulation() -> None:
        io = runtime.RuntimeIO(
            database,
            lease_database=database,
            safety_database=database,
            report_database=database,
        )
        entered, release = threading.Event(), threading.Event()

        def blocked() -> int:
            entered.set()
            release.wait(5)
            return threading.get_ident()

        scan = asyncio.create_task(io.call("report", blocked))
        try:
            while not entered.is_set():
                await asyncio.sleep(0.001)
            lease_thread = await asyncio.wait_for(io.call("lease", threading.get_ident), 1)
            safety_thread = await asyncio.wait_for(io.call("safety", threading.get_ident), 1)
            release.set()
            report_thread = await scan
            assert len({lease_thread, safety_thread, report_thread}) == 3
        finally:
            release.set()
            await io.close()

    asyncio.run(simulation())


@pytest.mark.parametrize("failure_kind", ["capture", "safety"])
def test_v3_runtime_failure_seals_truthfully_without_changing_prior_studies(
    current_v1: CurrentV1,
    store: ConfirmationStore,
    clock: Clock,
    monkeypatch: pytest.MonkeyPatch,
    failure_kind: str,
) -> None:
    prior = list(export_evidence(store.database))
    second = ConfirmationStore(
        store.database,
        "preserved-failed-second",
        study_id=V2_STUDY_ID,
        clock=clock,
    )
    second.claim(current_v1.protocol)
    second.append(
        "terminal",
        "terminal",
        {"state": "failed_incomplete", "reason": "historical failed run"},
        at_ns=clock.ns(),
    )
    second.repository.release_worker_lock(second.lock_name, second.owner_id)
    prior_second = list(export_evidence(store.database, study_id=V2_STUDY_ID))
    third = protocol_v3(current_v1.protocol)
    evidence = observer_guard(store.database, third)().model_copy(
        update={"checked_ns": clock.ns()},
    )
    test_clock = clock

    def factory(
        database: Database,
        owner: str,
        *,
        study_id: StudyId = STUDY_ID,
        clock: Callable[[], datetime] | None = None,
    ) -> ConfirmationStore:
        return ConfirmationStore(
            database,
            owner,
            study_id=study_id,
            clock=clock or test_clock,
        )

    async def failed(receiver: Receiver, stop: asyncio.Event) -> None:
        if failure_kind == "safety":
            await stop.wait()
            return
        await asyncio.sleep(0.02)
        raise RuntimeError("synthetic v3 observation failure")

    proofs = 0

    def proof() -> GuardEvidence:
        nonlocal proofs
        proofs += 1
        if failure_kind == "safety" and proofs > 1:
            request = httpx.Request("GET", "https://paper-api.alpaca.markets/v2/positions")
            raise runtime.CurrentBrokerProofError(
                httpx.HTTPStatusError(
                    "synthetic unsafe message never saved",
                    request=request,
                    response=httpx.Response(503, request=request),
                ),
            )
        return evidence

    monkeypatch.setattr(runtime, "ConfirmationStore", factory)
    monkeypatch.setattr(runtime, "now_ns", clock.ns)
    monkeypatch.setattr(Receiver, "work", failed)

    async def simulation() -> None:
        io = runtime.RuntimeIO(
            store.database,
            lease_database=store.database,
            safety_database=store.database,
            report_database=store.database,
        )
        with pytest.raises(RuntimeError, match="sealed failed/incomplete"):
            await runtime.run(third, store.database, guard=proof, runtime_io=io)

    asyncio.run(simulation())
    result = load_report(store.database, study_id=V3_STUDY_ID, verify_payloads=True)
    assert result["state"] == "failed_incomplete"
    assert result["decision"] == NO_GO and result["clean_integrity"] is False
    assert result["raw_frames_verified"] == 0
    if failure_kind == "safety":
        assert "/v2/positions" in str(result["terminal_reason"])
        assert "503" in str(result["terminal_reason"])
        assert "synthetic unsafe" not in canonical(result)
    assert list(export_evidence(store.database)) == prior
    assert list(export_evidence(store.database, study_id=V2_STUDY_ID)) == prior_second
    third_export = list(export_evidence(store.database, study_id=V3_STUDY_ID))
    assert third_export and all(
        value["schema"] == "kraken-book-confirmation-evidence-export-v3" for value in third_export
    )
    with pytest.raises(RuntimeError, match="no_restart"):
        factory(store.database, "replacement", study_id=V3_STUDY_ID).claim(third)


def test_default_production_runtime_io_uses_three_distinct_pools(
    database: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    address = "postgresql+psycopg://synthetic:synthetic@not-a-real-host/research"
    monkeypatch.setattr(database.engine, "url", make_url(address))
    constructed: list[Database] = []

    def fresh(url: str, *, pool_size: int) -> Database:
        assert url == address and pool_size == 1
        value = Database("sqlite://", pool_size=1)
        constructed.append(value)
        return value

    monkeypatch.setattr(runtime, "Database", fresh)
    io = runtime.RuntimeIO(database)
    assert len(constructed) == 3
    assert len({id(value.engine.pool) for value in constructed}) == 3
    assert io.lease_database is not io.report_database
    assert io.safety_database is not io.report_database
    assert io.lease_database is not database
    asyncio.run(io.close())


@pytest.mark.parametrize("loss", ["owner", "expired"])
def test_v3_report_wait_aborts_on_fencing_failure_and_cannot_append(
    current_v1: CurrentV1,
    clock: Clock,
    loss: str,
) -> None:
    third = protocol_v3(current_v1.protocol)
    store = ConfirmationStore(
        current_v1.database,
        "fenced-third",
        clock=clock,
        study_id=V3_STUDY_ID,
    )
    store.claim(third)
    before = list(store.export_chunks())

    async def simulation() -> None:
        io = runtime.RuntimeIO(
            current_v1.database,
            lease_database=current_v1.database,
            safety_database=current_v1.database,
            report_database=current_v1.database,
        )
        entered = threading.Event()

        def report() -> dict[str, JsonValue]:
            entered.set()
            while not io.report_cancel.wait(0.001):
                pass
            raise confirmation.ReportCancelledError("verification cancelled explicitly")

        async def lose_fence() -> None:
            while not entered.is_set():
                await asyncio.sleep(0.001)
            if loss == "owner":
                with current_v1.database.begin() as connection:
                    connection.execute(
                        update(worker_locks)
                        .where(
                            worker_locks.c.lock_name == store.lock_name,
                        )
                        .values(owner_id="different-owner"),
                    )
            else:
                clock.at += timedelta(seconds=91)
            await io.call("lease", store.refresh)

        verification = asyncio.create_task(io.call("report", report))
        heartbeat = asyncio.create_task(lose_fence())
        try:
            with pytest.raises(RuntimeError, match=r"lease_lost|lease_expired"):
                await runtime._watched(verification, (heartbeat,))
            with pytest.raises(RuntimeError, match=r"lease_lost|lease_expired"):
                store.append("terminal", "terminal", {"state": "completed"}, at_ns=clock.ns())
        finally:
            io.report_cancel.set()
            await asyncio.gather(verification, heartbeat, return_exceptions=True)
            await io.close()

    asyncio.run(simulation())
    assert list(store.export_chunks()) == before


@pytest.mark.parametrize("safety_failure", [False, True])
def test_v3_final_verification_keeps_lease_and_safety_live(
    current_v1: CurrentV1,
    store: ConfirmationStore,
    clock: Clock,
    monkeypatch: pytest.MonkeyPatch,
    safety_failure: bool,
) -> None:
    third = protocol_v3(current_v1.protocol)
    initial = observer_guard(current_v1.database, third)()
    proof_times: list[int] = []
    entered, release = threading.Event(), threading.Event()
    unavailable = threading.Event()
    simulated_clock = clock

    def factory(
        database: Database,
        owner: str,
        *,
        study_id: StudyId = STUDY_ID,
        clock: Callable[[], datetime] | None = None,
    ) -> ConfirmationStore:
        return ConfirmationStore(
            database,
            owner,
            clock=clock or simulated_clock,
            study_id=study_id,
        )

    def proof() -> GuardEvidence:
        proof_times.append(clock.ns())
        if unavailable.is_set():
            request = httpx.Request("GET", "https://paper-api.alpaca.markets/v2/account")
            raise runtime.CurrentBrokerProofError(
                httpx.HTTPStatusError(
                    "never disclose this response description",
                    request=request,
                    response=httpx.Response(503, request=request),
                ),
            )
        return initial.model_copy(update={"checked_ns": clock.ns()})

    async def skip_history(receiver: Receiver, stop: asyncio.Event) -> None:
        # Only finalization is under test; synthesize the preceding healthy interval.
        receiver.grid.next_slot = SLOTS
        clock.set(third.capture_end_ns)
        receiver.writer.store.repository.refresh_worker_lock(
            receiver.writer.store.lock_name,
            receiver.writer.store.owner_id,
            observed_at=clock.at,
        )
        await stop.wait()

    async def no_hourly_history(
        protocol: ConfirmationProtocol,
        reader: ConfirmationStore,
        io: runtime.RuntimeIO,
        writer: DurableWriter,
        stop: asyncio.Event,
    ) -> None:
        await stop.wait()

    async def virtual_pause(stop: asyncio.Event, seconds: float) -> None:
        until = clock.ns() + int(seconds * NS)
        while not stop.is_set() and clock.ns() < until:
            await asyncio.sleep(0.001)

    def final_report(
        reader: ConfirmationStore,
        *,
        at_ns: int,
        verify_payloads: bool,
        completing: bool,
        cancel: threading.Event | None,
    ) -> dict[str, JsonValue]:
        assert verify_payloads and completing
        entered.set()
        while not release.wait(0.001):
            if cancel is not None and cancel.is_set():
                raise confirmation.ReportCancelledError("final verification cancelled")
        return {"state": "completed", "decision": NO_GO, "clean_integrity": False}

    monkeypatch.setattr(runtime, "ConfirmationStore", factory)
    monkeypatch.setattr(runtime, "now_ns", clock.ns)
    monkeypatch.setattr(runtime, "_pause", virtual_pause)
    monkeypatch.setattr(runtime, "_report_watch", no_hourly_history)
    monkeypatch.setattr(runtime, "build_report", final_report)
    monkeypatch.setattr(Receiver, "work", skip_history)

    async def simulation() -> None:
        io = runtime.RuntimeIO(
            current_v1.database,
            lease_database=current_v1.database,
            safety_database=current_v1.database,
            report_database=current_v1.database,
        )
        process = asyncio.create_task(
            runtime.run(third, current_v1.database, guard=proof, runtime_io=io),
        )
        try:
            for _ in range(1000):
                if entered.is_set() or process.done():
                    break
                await asyncio.sleep(0.001)
            if process.done():
                await process
            assert entered.is_set()
            for second in range(110):
                clock.at += timedelta(seconds=1)
                if safety_failure and second >= 30:
                    unavailable.set()
                await asyncio.sleep(0.005)
                if safety_failure and process.done():
                    break
                assert not process.done()
            release.set()
            if safety_failure:
                with pytest.raises(RuntimeError, match="sealed failed/incomplete"):
                    await process
            else:
                assert len(proof_times) >= 4
                result = await process
                assert result["state"] == "completed"
                assert result["decision"] == NO_GO and result["clean_integrity"] is False
        finally:
            release.set()
            if not process.done():
                process.cancel()
            await asyncio.gather(process, return_exceptions=True)

    asyncio.run(simulation())
    reader = factory(current_v1.database, "read-only", study_id=V3_STUDY_ID)
    terminal = reader.latest_header("terminal")
    assert terminal is not None
    _, payload = reader.read_chunk(terminal.sequence)
    if safety_failure:
        assert payload["state"] == "failed_incomplete"
        assert "503" in str(payload["reason"]) and "/v2/account" in str(payload["reason"])
        assert "never disclose" not in canonical(payload)


def test_hourly_scan_budget_cancels_explicitly_without_partial_success(
    current_v1: CurrentV1,
    clock: Clock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    third = protocol_v3(current_v1.protocol)
    store = ConfirmationStore(
        current_v1.database,
        "bounded-third",
        clock=clock,
        study_id=V3_STUDY_ID,
    )
    store.claim(third)
    clock.set(third.slot_ns(360))
    monkeypatch.setattr(runtime, "now_ns", clock.ns)
    monkeypatch.setattr(runtime, "REPORT_SCAN_TIMEOUT_SECONDS", 0.1)

    def scan(
        reader: ConfirmationStore,
        *,
        at_ns: int,
        cancel: threading.Event | None,
    ) -> dict[str, JsonValue]:
        assert cancel is not None
        cancel.wait(5)
        raise confirmation.ReportCancelledError("budget exhausted")

    monkeypatch.setattr(runtime, "build_report", scan)

    async def simulation() -> None:
        io = runtime.RuntimeIO(
            current_v1.database,
            lease_database=current_v1.database,
            safety_database=current_v1.database,
            report_database=current_v1.database,
        )
        writer = DurableWriter(store)
        writer.task = asyncio.create_task(writer.work())
        try:
            with pytest.raises(RuntimeError, match="hourly_report_duration_budget_exceeded"):
                await runtime._report_watch(third, store, io, writer, asyncio.Event())
            assert io.report_cancel.is_set()
            assert store.latest_header("report") is None
        finally:
            await writer.close()
            await io.close()

    asyncio.run(simulation())
