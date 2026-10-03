"""Synthetic contract tests only: no provider connections, jobs, or trading calls."""

from __future__ import annotations

import asyncio
import base64
import importlib
import threading
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from typing import cast

import httpx
import pytest
from pydantic import JsonValue, ValidationError
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
from sqlalchemy.engine import Connection
from sqlalchemy.pool import StaticPool

import tradeagent.kraken_confirmation as confirmation
import tradeagent.kraken_confirmation_runtime as runtime
from tradeagent.kraken_confirmation import (
    LOCK_NAME,
    NS,
    SLOTS,
    SOURCE_MODULES,
    CausalGrid,
    ChunkHeader,
    ConfirmationProtocol,
    ConfirmationStore,
    Evaluation,
    Evidence,
    Label,
    Quote,
    SourceDigest,
    Symbol,
    check_schema,
    confirmation_evidence,
    confirmation_schema_version,
    decode_blob,
    install_schema,
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
)
from tradeagent.kraken_confirmation_runtime import (
    BackpressureError,
    DurableWriter,
    GuardEvidence,
    Receiver,
    observer_guard,
    source_digests,
    validate_guard,
    verify_sources,
)
from tradeagent.persistence import Database, ProductionRepository, worker_locks
from tradeagent.scalping_market import datetime_ns, ns_datetime
from tradeagent.scalping_store import canonical
from tradeagent.shadow_dataset import ShadowDatasetProtocol, shadow_datasets

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
