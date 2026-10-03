"""Standalone public-only receiver. Importing this module does not launch a job.

Callable: ``await run(protocol, database, guard=callback)``. The module CLI accepts
``run PROTOCOL.json`` or read-only ``status [--verify]`` / ``export``.
No worker entry point changes.

Operator interface (nothing runs on import):
* With the old 0015 observer unchanged, do NOT run global ``alembic upgrade head``.
  Invoke ``install_schema(database)`` or the explicit ``init-store`` subcommand.
  Only the evidence table and ``kraken_confirmation_alembic_version`` are created;
  auxiliary revision ``0016_kraken_confirmation`` is tracked there while global
  ``alembic_version`` remains 0015. ``check_schema(database)`` verifies this path.
  The four isolated modules can be uploaded without a v1 redeployment.
* Construct ``ConfirmationProtocol`` with future UTC ten-second-aligned start/end
  exactly 72 hours apart and covering weekend/weekday, actual freeze/provenance and
  v1 account/runtime/owner/feed pins. Set ``source_hashes=source_digests()`` only
  after the uploaded source bytes are final. Save ``model_dump_json()`` at the
  operator-selected protocol path. This module does not choose/freeze dates.
* Launch before start: ``python -m tradeagent.kraken_confirmation_runtime run PATH``.
  ``status`` prints a bounded summary; ``status --verify`` also streams/verifies
  every blob. ``export`` emits JSONL with exact base64-encoded compressed bytes and
  per-chunk hash-chain headers; ``export_evidence(database)`` is its read-only
  iterator API. ``ConfirmationStore.chunks()`` reads decoded evidence by page.
  Terminal summaries are embedded in the immutable ``terminal`` journal record.
* The default guard reuses the existing GET-only paper monitor; an injected
  synchronous callback returns ``GuardEvidence`` and must prove the same pins.
  Broker credentials are never attached to the fixed public Kraken connection.

Callable modules/sequence:
``tradeagent.kraken_confirmation`` exports ``ConfirmationProtocol``,
``install_schema(database)`` and ``check_schema(database)``.
``tradeagent.kraken_confirmation_runtime`` exports ``source_digests()``,
``observer_guard(database, protocol)`` and ``await run(protocol, database, guard=None)``.
Use the existing ``Database(AppConfig().database_url.get_secret_value(), pool_size=2)``
context; the CLI reads the existing ``TRADEAGENT_DATABASE_URL`` configuration.
Install/check storage first, without claiming a study. Construct the model with
``frozen_at``, ``start``, ``end``, ``code_sha``, ``source_hashes=source_digests()``,
``account_digest``, ``v1_runtime_sha``, ``v1_owner_id`` and ``v1_connection_id``.
The operator may serialize ``protocol.model_dump_json(indent=2)`` to their chosen
path and archive ``protocol.identity``; do NOT call ``ConfirmationStore.claim``
before ``run`` because ``run`` itself makes the sole immutable claim.
The default guard loads the existing ``ShadowDatasetProtocol`` from the DB and
uses unchanged ``local_safety`` / ``PaperReadOnlyMonitor.snapshot`` with
``AlpacaPaperSettings.model_validate({})`` (``ALPACA_KEY_ID`` / ``ALPACA_SECRET_KEY``),
GET-only fixed paper host, no redirects, pinned account and unchanged v1 source/owner.

Operational limits: 3 GiB charged compressed payload plus estimated per-row charge
(4 MiB reserved for reports/terminal), not certified free database/WAL/index capacity.
Queue: 128 submissions, five-second backpressure limit; raw frames: 256 KiB;
DB chunks: at most 4 MiB decoded; quote rings: 70 seconds/60,000 per symbol;
verification pages: eight chunks; bounded reconnects: four total connections.
CRC failure, clock error, persistence/guard/source failure seals failed/incomplete.
Disconnects invalidate both books until new snapshots and are reported as unclean.
Hourly summaries use fixed elapsed-hour boundaries and full scheduled denominators;
the last horizon settles 52 seconds after the 72-hour evaluation window. Capture
continues for the fixed 75-second tail; it is not a 30-minute/1,800-second runner.
No minimum warmup duration or native-book clock/label thresholds are introduced.
An uncatchable process death is inferred after a 90-second study-specific lease
expiry. The durable single claim always forbids resume/replacement, even before
expiry; there is no restart, rearm, or delete command. Disk exhaustion/DB loss can
prevent terminal append; read-only status then exposes the incomplete frozen grid.
Production database connection timeouts/capacity and 72-hour provider endurance
must be established by the operator; these local contracts do not certify them.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from collections import Counter
from collections.abc import Callable
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Literal, Never, Self
from uuid import uuid4

import httpx
from pydantic import BaseModel, ConfigDict, JsonValue, TypeAdapter, model_validator
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake

from tradeagent.alpaca_paper import AlpacaPaperSettings
from tradeagent.config import AppConfig
from tradeagent.kraken_confirmation import (
    LOCK_NAME,
    NS,
    SOURCE_MODULES,
    SYMBOLS,
    CausalGrid,
    ConfirmationProtocol,
    ConfirmationStore,
    Evidence,
    Quote,
    SourceDigest,
    check_schema,
    install_schema,
    record,
)
from tradeagent.kraken_confirmation_book import Book
from tradeagent.kraken_confirmation_report import build_report, export_evidence, load_report
from tradeagent.persistence import Database, ProductionRepository, worker_locks
from tradeagent.scalping_market import datetime_ns, timestamp_ns
from tradeagent.scalping_store import canonical, utc
from tradeagent.shadow_dataset import DATASET_ID, ShadowDatasetProtocol, shadow_datasets
from tradeagent.shadow_dataset_monitor import PaperReadOnlyMonitor, local_safety

PUBLIC_URL = "wss://ws.kraken.com/v2"
FRAME_LIMIT = 256 * 1024
QUEUE_CAPACITY = 128
QUEUE_TIMEOUT = 5
JSON_OBJECT = TypeAdapter(dict[str, JsonValue])


def _nonfinite_json(value: str) -> Never:
    raise ValueError(f"non-finite public JSON token: {value}")


def now_ns() -> int:
    return time.time_ns()


def source_digests() -> tuple[SourceDigest, ...]:
    root = Path(__file__).parent
    return tuple(
        SourceDigest(name=name, sha256=sha256((root / name).read_bytes()).hexdigest())
        for name in SOURCE_MODULES
    )


def verify_sources(protocol: ConfirmationProtocol) -> None:
    if protocol.source_hashes != source_digests():
        raise ValueError("confirmation source modules differ from the frozen protocol")


class GuardEvidence(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    account_digest: str
    checked_ns: int
    paper_host: Literal["https://paper-api.alpaca.markets"] = "https://paper-api.alpaca.markets"
    read_methods: tuple[Literal["GET"], ...] = ("GET",)
    trading_authorization: Literal["expired"] = "expired"
    model_state: Literal["no_support"] = "no_support"
    order_attempts: Literal[0] = 0
    positions: Literal[0] = 0
    open_orders: Literal[0] = 0
    broker_order_records_since_freeze: Literal[0] = 0
    authorization_renewed: Literal[False] = False
    v1_runtime_sha: str
    v1_owner_id: str
    v1_connection_id: str

    @model_validator(mode="after")
    def get_only(self) -> Self:
        if self.read_methods != ("GET",):
            raise ValueError("broker safety observer must be GET-only")
        return self


Guard = Callable[[], GuardEvidence]


class BackpressureError(RuntimeError):
    """No frame may silently disappear when durable persistence falls behind."""


def observer_guard(database: Database, protocol: ConfirmationProtocol) -> Guard:
    """Reuse the previous read-only guard, with pinned account/runtime/owner/feed."""

    def check() -> GuardEvidence:
        with database.begin() as connection:
            frozen = ShadowDatasetProtocol.model_validate(
                connection.scalar(
                    select(shadow_datasets.c.protocol).where(
                        shadow_datasets.c.dataset_id == DATASET_ID
                    )
                )
            )
            lease = (
                connection.execute(
                    select(worker_locks).where(
                        worker_locks.c.lock_name == "tradeagent-event-worker"
                    )
                )
                .mappings()
                .one()
            )
        if frozen.account_digest != protocol.account_digest:
            raise ValueError("observer account differs from the confirmation pin")
        safety = local_safety(database, frozen)
        heartbeat = ProductionRepository(database).latest_heartbeat("tradeagent-event-worker")
        if heartbeat is None:
            raise ValueError("v1 heartbeat unavailable")
        details = heartbeat[2]
        feed = details.get("feed") or {}
        checked = datetime.now(UTC)
        if (
            not timedelta(0) <= checked - utc(heartbeat[1]) <= timedelta(seconds=30)
            or not timedelta(0) <= checked - utc(lease["acquired_at"]) <= timedelta(seconds=90)
            or heartbeat[0] != protocol.v1_owner_id
            or lease["owner_id"] != protocol.v1_owner_id
            or details.get("code_sha") != protocol.v1_runtime_sha
            or feed.get("connection_id") != protocol.v1_connection_id
            or feed.get("subscribed") is not True
        ):
            raise ValueError("v1 pinned source, ownership, connection or freshness changed")
        with httpx.Client(timeout=10, follow_redirects=False) as client:
            broker = PaperReadOnlyMonitor(AlpacaPaperSettings.model_validate({}), client).snapshot(
                frozen
            )
        if (
            safety["local_order_attempts_since_freeze"] != 0
            or safety["reported_order_attempts"] != 0
            or safety["trading_authorization_renewed"] is not False
            or safety["reported_trading_authorization"] != "expired"
            or safety["reported_model_state"] != "no_support"
            or broker["positions"] != 0
            or broker["open_orders"] != 0
            or broker["broker_order_records_since_freeze"] != 0
        ):
            raise ValueError("zero-order or expired-authority guard failed")
        return GuardEvidence(
            account_digest=protocol.account_digest,
            checked_ns=datetime_ns(checked),
            v1_runtime_sha=protocol.v1_runtime_sha,
            v1_owner_id=protocol.v1_owner_id,
            v1_connection_id=protocol.v1_connection_id,
        )

    return check


def validate_guard(evidence: GuardEvidence, protocol: ConfirmationProtocol) -> None:
    # Revalidate callbacks too: model_construct/model_copy must not bypass safety.
    evidence = GuardEvidence.model_validate(evidence.model_dump(mode="json"))
    if (
        evidence.account_digest != protocol.account_digest
        or evidence.v1_runtime_sha != protocol.v1_runtime_sha
        or evidence.v1_owner_id != protocol.v1_owner_id
        or evidence.v1_connection_id != protocol.v1_connection_id
        or not 0 <= now_ns() - evidence.checked_ns <= 30 * NS
    ):
        raise ValueError("injected observer guard did not prove the frozen safety pins")


class _Submission(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    kind: str
    records: tuple[Evidence, ...]


class DurableWriter:
    """One FIFO writer with bounded backlog; DB/compression never run on the loop."""

    def __init__(self, store: ConfirmationStore) -> None:
        self.store = store
        self.queue: asyncio.Queue[_Submission | None] = asyncio.Queue(QUEUE_CAPACITY)
        self.slots = asyncio.Semaphore(QUEUE_CAPACITY)
        self.maximum_queue = 0
        self.maximum_write_ns = 0
        self.next_chunk = 1
        self.task: asyncio.Task[None] | None = None
        self.append_lock = asyncio.Lock()

    async def reserve(self) -> None:
        if self.task is None:
            raise RuntimeError("durable writer not running")
        if self.task.done():
            await self.task
            raise RuntimeError("durable writer unexpectedly stopped")
        try:
            await asyncio.wait_for(self.slots.acquire(), QUEUE_TIMEOUT)
        except TimeoutError as error:
            raise BackpressureError("durable_writer_queue_timeout") from error
        if self.task.done():
            self.slots.release()
            await self.task
            raise RuntimeError("durable writer unexpectedly stopped")

    def submit_reserved(self, kind: str, records: tuple[Evidence, ...]) -> None:
        submission = _Submission(kind=kind, records=records)
        if len(canonical(submission.model_dump(mode="json")).encode()) > FRAME_LIMIT * 2:
            self.slots.release()
            raise RuntimeError("submission_budget_exceeded")
        self.queue.put_nowait(submission)
        self.maximum_queue = max(self.maximum_queue, self.queue.qsize())

    async def submit(self, kind: str, records: tuple[Evidence, ...]) -> None:
        await self.reserve()
        self.submit_reserved(kind, records)

    async def flush(self) -> None:
        if self.task is None:
            raise RuntimeError("durable writer not running")
        joining = asyncio.create_task(self.queue.join())
        try:
            done, _ = await asyncio.wait(
                (joining, self.task),
                return_when=asyncio.FIRST_COMPLETED,
                timeout=45,
            )
            if not done:
                raise TimeoutError("durable_writer_flush_timeout")
            if self.task in done:
                await self.task
                raise RuntimeError("durable writer unexpectedly stopped")
            await joining
        finally:
            if not joining.done():
                joining.cancel()
                await asyncio.gather(joining, return_exceptions=True)

    async def work(self) -> None:
        # Pack consecutive same-kind submissions; do not reorder raw proof after
        # evaluations/labels that consume it, even when all arrive within one tick.
        carry: _Submission | None = None
        while True:
            submission = carry if carry is not None else await self.queue.get()
            carry = None
            if submission is None:
                self.queue.task_done()
                return
            rows = list(submission.records)
            consumed = 1
            size = len(canonical([row.model_dump(mode="json") for row in rows]).encode())
            deadline = time.monotonic() + 0.25
            while size < FRAME_LIMIT and consumed < 64:
                timeout = deadline - time.monotonic()
                if timeout <= 0:
                    break
                try:
                    following = await asyncio.wait_for(self.queue.get(), timeout)
                except TimeoutError:
                    break
                if following is None:
                    carry = None
                    self.queue.task_done()
                    raise RuntimeError("writer stop before explicit flush")
                if following.kind != submission.kind:
                    carry = following
                    break
                rows.extend(following.records)
                consumed += 1
                size += len(canonical(following.model_dump(mode="json")).encode())
            began = time.monotonic_ns()
            await self.append_metadata(
                submission.kind,
                f"chunk:{self.next_chunk}",
                {"records": [row.model_dump(mode="json") for row in rows]},
            )
            self.next_chunk += 1
            self.maximum_write_ns = max(self.maximum_write_ns, time.monotonic_ns() - began)
            for _ in range(consumed):
                self.queue.task_done()
                self.slots.release()

    async def close(self) -> None:
        await self.flush()
        await self.queue.put(None)
        if self.task is not None:
            await self.task

    async def append_metadata(self, kind: str, key: str, payload: dict[str, JsonValue]) -> None:
        async with self.append_lock:
            await asyncio.to_thread(self.store.append, kind, key, payload, at_ns=now_ns())


class Receiver:
    def __init__(
        self,
        protocol: ConfirmationProtocol,
        grid: CausalGrid,
        writer: DurableWriter,
    ) -> None:
        self.protocol, self.grid, self.writer = protocol, grid, writer
        self.books = {symbol: Book() for symbol in SYMBOLS}
        self.counts: Counter[str] = Counter()
        self.symbol_counts: dict[str, Counter[str]] = {symbol: Counter() for symbol in SYMBOLS}
        self.connection_id = ""
        self.epoch = 0
        self.frame_id = 0
        self.last_received = 0
        self.last_monotonic = 0
        self.last_fingerprint: dict[str, tuple[int, int]] = {}
        self.max_provider_receipt_ns = 0
        self.max_receipt_acceptance_ns = 0
        self.connected = False
        self.last_error: str | None = None

    def _book_count(self, symbol: str, name: str, amount: int = 1) -> None:
        self.counts[name] += amount
        self.symbol_counts[symbol][name] += amount

    async def invalidate(self, reason: str) -> None:
        await self.writer.reserve()
        at = now_ns()
        self.epoch += 1
        for book in self.books.values():
            book.invalidate(reason)
        self.last_fingerprint.clear()
        self.writer.submit_reserved(
            "health",
            (
                Evidence(
                    key=f"invalidation:{self.epoch}",
                    at_ns=at,
                    payload={
                        "reason": reason,
                        "epoch": self.epoch,
                        "connection_id": self.connection_id,
                    },
                ),
            ),
        )
        self.grid.invalidate(at, reason, self.epoch)

    async def frame(self, raw: str | bytes) -> None:
        received, monotonic = now_ns(), time.monotonic_ns()
        await self.writer.reserve()
        text = raw.hex() if isinstance(raw, bytes) else raw
        self.frame_id += 1
        quotes: list[Quote] = []
        error: ValueError | None = None
        try:
            if isinstance(raw, bytes):
                raise ValueError("unexpected_binary_public_frame_preserved_as_hex")
            if received < self.last_received or monotonic < self.last_monotonic:
                self.counts["clock_errors"] += 1
                raise ValueError("local_receive_clock_regression")
            if len(text.encode()) > FRAME_LIMIT:
                raise ValueError("public_frame_budget_exceeded")
            # Decimal tokens stay strings: no binary float round-trip before CRC.
            message = JSON_OBJECT.validate_python(
                json.loads(text, parse_float=str, parse_constant=_nonfinite_json),
            )
            self.last_received, self.last_monotonic = received, monotonic
            if message.get("method") == "subscribe":
                if message.get("success") is not True:
                    raise ValueError("public_book_subscription_rejected")
                self.counts["subscription_acknowledgments"] += 1
            elif message.get("channel") == "book":
                rows = message.get("data")
                if not isinstance(rows, list) or len(rows) != 1:
                    raise ValueError("expected one book object per public frame")
                for row in rows:
                    if not isinstance(row, dict):
                        raise ValueError("invalid book message object")
                    symbol, timestamp = row.get("symbol"), row.get("timestamp")
                    if not isinstance(symbol, str) or symbol not in SYMBOLS:
                        raise ValueError("unsubscribed public book symbol")
                    if not isinstance(timestamp, str):
                        raise ValueError("book timestamp missing")
                    provider = timestamp_ns(timestamp)
                    if provider > received:
                        self._book_count(symbol, "timestamp_errors")
                        raise ValueError("provider_timestamp_after_receipt")
                    message_type = message.get("type")
                    if not isinstance(message_type, str):
                        raise ValueError("public book message type missing")
                    book = self.books[symbol]
                    prior_checks = book.checks
                    validated = book.apply(row, message_type, provider)
                    self._book_count(symbol, "checksum_checks", book.checks - prior_checks)
                    self._book_count(symbol, "book_messages")
                    if validated is None:
                        self._book_count(symbol, "rejected_book_messages")
                        if book.last_reason == "checksum_mismatch":
                            self._book_count(symbol, "checksum_failures")
                        elif book.last_reason == "provider_book_timestamp_regression":
                            self._book_count(symbol, "timestamp_errors")
                        if book.last_reason != "update_before_valid_snapshot":
                            raise ValueError(book.last_reason)
                        self._book_count(symbol, "unverified_book_updates")
                    else:
                        accepted = now_ns()
                        quote = Quote.model_validate(
                            {
                                **validated.model_dump(),
                                "symbol": symbol,
                                "frame_id": self.frame_id,
                                "connection_id": self.connection_id,
                                "epoch": self.epoch,
                                "received_ns": received,
                                "received_monotonic_ns": monotonic,
                                "accepted_ns": accepted,
                            }
                        )
                        quotes.append(quote)
                        fingerprint = (provider, quote.checksum_computed)
                        previous = self.last_fingerprint.get(symbol)
                        if previous is not None and previous[0] == provider:
                            self._book_count(symbol, "equal_provider_timestamps")
                        if self.last_fingerprint.get(symbol) == fingerprint:
                            self._book_count(symbol, "duplicate_book_states")
                        self.last_fingerprint[symbol] = fingerprint
                        self.max_provider_receipt_ns = max(
                            self.max_provider_receipt_ns, received - provider
                        )
                        self.max_receipt_acceptance_ns = max(
                            self.max_receipt_acceptance_ns, accepted - received
                        )
                        self._book_count(symbol, "checksum_valid_quotes")
            elif (
                message.get("channel") not in ("heartbeat", "status")
                and message.get("method") != "pong"
            ):
                raise ValueError("unexpected_public_book_frame")
        except (ValueError, KeyError, UnicodeError) as failure:
            self.counts["protocol_errors"] += 1
            error = ValueError(str(failure))
        at = quotes[-1].accepted_ns if quotes else now_ns()
        self.counts["frames"] += 1
        if error is not None:
            self.epoch += 1
            for book in self.books.values():
                book.invalidate(str(error))
        self.writer.submit_reserved(
            "capture",
            (
                Evidence(
                    key=f"frame:{self.frame_id}",
                    at_ns=at,
                    payload={
                        "frame_id": self.frame_id,
                        "connection_id": self.connection_id,
                        "endpoint": PUBLIC_URL,
                        "received_ns": received,
                        "received_monotonic_ns": monotonic,
                        "accepted_ns": at,
                        "payload_text": text,
                        "quotes": [quote.model_dump(mode="json") for quote in quotes],
                        "wire_encoding": "hex" if isinstance(raw, bytes) else "utf-8",
                        "rejection": str(error) if error is not None else None,
                        "epoch": self.epoch,
                    },
                ),
            ),
        )
        for quote in quotes:
            self.grid.accept(quote)
        if error is not None:
            self.grid.invalidate(at, str(error), self.epoch)
            raise error

    async def work(self, stop: asyncio.Event) -> None:
        try:
            await self._connections(stop)
        finally:
            self.connected = False
            for book in self.books.values():
                book.invalidate("closed_connection")

    async def _connections(self, stop: asyncio.Event) -> None:
        for attempt in range(self.protocol.maximum_connections):
            if stop.is_set() or now_ns() >= self.protocol.capture_end_ns:
                return
            self.connection_id = str(uuid4())
            await self.invalidate("new_connection_requires_snapshot")
            self.counts["connections"] += 1
            try:
                async with connect(
                    PUBLIC_URL,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=5,
                    max_queue=16,
                    max_size=FRAME_LIMIT,
                    open_timeout=10,
                ) as websocket:
                    self.connected = True
                    await websocket.send(
                        json.dumps(
                            {
                                "method": "subscribe",
                                "params": {
                                    "channel": "book",
                                    "symbol": list(SYMBOLS),
                                    "depth": 10,
                                    "snapshot": True,
                                },
                                "req_id": 1,
                            }
                        )
                    )
                    while not stop.is_set() and now_ns() < self.protocol.capture_end_ns:
                        try:
                            raw = await asyncio.wait_for(websocket.recv(), 1)
                        except TimeoutError:
                            continue
                        await self.frame(raw)
            except (ConnectionClosed, InvalidHandshake, OSError, TimeoutError) as error:
                self.connected = False
                self.counts["disconnects"] += 1
                for counter in self.symbol_counts.values():
                    counter["disconnects"] += 1
                self.last_error = type(error).__name__
                await self.invalidate("disconnect_requires_new_snapshot")
                if attempt + 1 == self.protocol.maximum_connections:
                    raise RuntimeError("bounded_public_reconnects_exhausted") from error
                self.counts["reconnects"] += 1
                with suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), 5)
            else:
                self.connected = False
                return

    def quality(self) -> dict[str, JsonValue]:
        return {
            "counts": dict(self.counts),
            "per_symbol_counts": {
                symbol: {
                    name: counts[name]
                    for name in (
                        "book_messages",
                        "checksum_checks",
                        "checksum_valid_quotes",
                        "rejected_book_messages",
                        "checksum_failures",
                        "timestamp_errors",
                        "equal_provider_timestamps",
                        "duplicate_book_states",
                        "unverified_book_updates",
                        "disconnects",
                    )
                }
                for symbol, counts in self.symbol_counts.items()
            },
            "connected": self.connected,
            "connection_id": self.connection_id,
            "epoch": self.epoch,
            "last_error": self.last_error,
            "maximum_provider_receipt_latency_ns": self.max_provider_receipt_ns,
            "maximum_receipt_acceptance_latency_ns": self.max_receipt_acceptance_ns,
            "maximum_db_write_latency_ns": self.writer.maximum_write_ns,
            "maximum_writer_queue": self.writer.maximum_queue,
            "maximum_label_backlog": self.grid.maximum_backlog,
            "maximum_quote_cache": self.grid.maximum_cache,
        }


async def run(
    protocol: ConfirmationProtocol,
    database: Database,
    *,
    guard: Guard | None = None,
) -> dict[str, JsonValue]:
    """Claim once, run once. Fatal errors seal incomplete; no replay/resume mode."""
    protocol = ConfirmationProtocol.model_validate(protocol.model_dump(mode="json"))
    verify_sources(protocol)
    await asyncio.to_thread(check_schema, database)
    store = ConfirmationStore(database, str(uuid4()))
    await asyncio.to_thread(store.claim, protocol)
    guard = guard if guard is not None else observer_guard(database, protocol)
    writer = DurableWriter(store)
    writer.task = asyncio.create_task(writer.work())
    grid = CausalGrid(protocol)
    receiver = Receiver(protocol, grid, writer)
    stop = asyncio.Event()
    capture: asyncio.Task[None] | None = None
    guard_task: asyncio.Task[GuardEvidence] | None = None
    failure: str | None = None
    last_quality = last_lease = last_guard = 0
    report_hour = 0
    try:
        initial = await asyncio.to_thread(guard)
        validate_guard(initial, protocol)
        await writer.flush()
        await writer.append_metadata("guard", "guard:initial", initial.model_dump(mode="json"))
        capture = asyncio.create_task(receiver.work(stop))
        while now_ns() < protocol.capture_end_ns:
            if writer.task.done():
                await writer.task
                raise RuntimeError("durable_writer_stopped")
            if capture.done():
                await capture
                if now_ns() < protocol.capture_end_ns:
                    raise RuntimeError("public_capture_stopped_before_fixed_end")
                break
            at = now_ns()
            evaluations, labels = grid.advance(at)
            for kind, rows in (("evaluation", evaluations), ("label", labels)):
                if rows:
                    await writer.submit(kind, tuple(record(row) for row in rows))
            if at - last_lease >= 10 * NS:
                await asyncio.to_thread(store.refresh)
                last_lease = at
            if at - last_quality >= 10 * NS:
                await writer.flush()
                await writer.append_metadata("quality", f"quality:{at}", receiver.quality())
                last_quality = at
            if guard_task is not None and guard_task.done():
                evidence = await guard_task
                validate_guard(evidence, protocol)
                await writer.flush()
                await writer.append_metadata(
                    "guard",
                    f"guard:{at}",
                    evidence.model_dump(mode="json"),
                )
                guard_task = None
                last_guard = at
            if guard_task is None and at - last_guard >= 60 * NS:
                guard_task = asyncio.create_task(asyncio.to_thread(guard))
                last_guard = at
            hour = min(72, max(0, (at - datetime_ns(protocol.start)) // (3600 * NS)))
            if hour > report_hour:
                verify_sources(protocol)
                report = await asyncio.to_thread(
                    build_report,
                    store,
                    at_ns=datetime_ns(protocol.start) + hour * 3600 * NS,
                )
                await writer.flush()
                await writer.append_metadata("report", f"hour:{hour}", report)
                report_hour = hour
            await asyncio.sleep(0.1)
        stop.set()
        if capture is not None:
            await capture
        evaluations, labels = grid.advance(now_ns())
        for kind, rows in (("evaluation", evaluations), ("label", labels)):
            if rows:
                await writer.submit(kind, tuple(record(row) for row in rows))
        await writer.flush()
        final_guard = await asyncio.to_thread(guard)
        validate_guard(final_guard, protocol)
        await writer.append_metadata(
            "guard", "guard:pre_verification", final_guard.model_dump(mode="json")
        )
    except (
        ValueError,
        RuntimeError,
        OSError,
        TimeoutError,
        SQLAlchemyError,
        httpx.HTTPError,
    ) as error:
        failure = f"{type(error).__name__}:{error}"
        if isinstance(error, BackpressureError):
            receiver.counts["backpressure_failures"] += 1
        if "clock" in str(error):
            receiver.counts["clock_errors"] += 1
        if "cache" in str(error):
            receiver.counts["cache_budget_failures"] += 1
    except asyncio.CancelledError:
        failure = "process_cancelled_no_restart"
        raise
    finally:
        active_exception = sys.exception()
        if failure is None and active_exception is not None:
            failure = f"unhandled_process_failure:{type(active_exception).__name__}"
        stop.set()
        if capture is not None and not capture.done():
            capture.cancel()
        if guard_task is not None and not guard_task.done():
            guard_task.cancel()
        tasks = [task for task in (capture, guard_task) if task is not None]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception) and failure is None:
                failure = f"{type(result).__name__}:{result}"
        try:
            try:
                await writer.close()
            except (
                ValueError,
                RuntimeError,
                OSError,
                TimeoutError,
                SQLAlchemyError,
            ) as writer_error:
                failure = failure or f"durable_writer_failure:{writer_error}"
                receiver.counts["persistence_failures"] += 1
                if writer.task is not None and not writer.task.done():
                    writer.task.cancel()
                    await asyncio.gather(writer.task, return_exceptions=True)
            await asyncio.to_thread(store.refresh)
            await writer.append_metadata("quality", "quality:final", receiver.quality())
            try:
                report = await _verified_report(store, completing=failure is None)
            except (ValueError, RuntimeError, OSError, TimeoutError, SQLAlchemyError) as error:
                failure = failure or f"final_evidence_verification_failed:{error}"
                report = {
                    "schema": "kraken-book-confirmation-report-v1",
                    "protocol_hash": protocol.identity,
                    "decision": "NO_GO",
                    "state": "failed_incomplete",
                    "clean_integrity": False,
                    "full_payload_verification": False,
                    "verification_error": str(error),
                    "orders_submitted": 0,
                    "private_kraken_requests": 0,
                    "v2_ready": False,
                }
            if failure is not None:
                report.update(
                    {
                        "state": "failed_incomplete",
                        "decision": "NO_GO",
                        "clean_integrity": False,
                        "terminal_reason": failure,
                    }
                )
            if failure is None:
                try:
                    verify_sources(protocol)
                    final_guard = await asyncio.to_thread(guard)
                    validate_guard(final_guard, protocol)
                    await writer.append_metadata(
                        "guard",
                        "guard:final",
                        final_guard.model_dump(mode="json"),
                    )
                except (ValueError, RuntimeError, OSError, TimeoutError, httpx.HTTPError) as error:
                    failure = f"final_safety_or_source_failure:{type(error).__name__}:{error}"
                    report.update(
                        {
                            "state": "failed_incomplete",
                            "decision": "NO_GO",
                            "clean_integrity": False,
                            "terminal_reason": failure,
                        }
                    )
            await writer.append_metadata(
                "terminal",
                "terminal",
                {
                    "state": "completed" if failure is None else "failed_incomplete",
                    "reason": failure,
                    "report": report,
                },
            )
        except (ValueError, RuntimeError, OSError, TimeoutError, SQLAlchemyError) as sealing_error:
            # A DB/lease/hash failure must be visible to the caller. The immutable
            # claim and read-only expired-owner status prevent favorable replacement.
            if writer.task is not None and not writer.task.done():
                writer.task.cancel()
                await asyncio.gather(writer.task, return_exceptions=True)
            raise RuntimeError(
                f"confirmation failed to seal; original={failure}; sealing={sealing_error}",
            ) from sealing_error
        finally:
            await asyncio.to_thread(
                store.repository.release_worker_lock,
                LOCK_NAME,
                store.owner_id,
            )
    if failure is not None:
        raise RuntimeError(f"confirmation sealed failed/incomplete: {failure}")
    return report


async def _verified_report(store: ConfirmationStore, *, completing: bool) -> dict[str, JsonValue]:
    """Keep ownership alive during the bounded-page final verification, not a resume."""
    stop = asyncio.Event()

    async def renew() -> None:
        while not stop.is_set():
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), 10)
            if not stop.is_set():
                await asyncio.to_thread(store.refresh)

    heartbeat = asyncio.create_task(renew())
    report = asyncio.create_task(
        asyncio.to_thread(
            build_report,
            store,
            at_ns=now_ns(),
            verify_payloads=True,
            completing=completing,
        )
    )
    try:
        done, _ = await asyncio.wait((heartbeat, report), return_when=asyncio.FIRST_COMPLETED)
        if heartbeat in done:
            await heartbeat
            raise RuntimeError("confirmation verification heartbeat unexpectedly stopped")
        return await report
    finally:
        stop.set()
        await heartbeat


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)
    start = subcommands.add_parser("run")
    start.add_argument("protocol", type=Path)
    status = subcommands.add_parser("status")
    status.add_argument("--verify", action="store_true")
    subcommands.add_parser("init-store")
    subcommands.add_parser("export")
    args = parser.parse_args()
    with Database(AppConfig().database_url.get_secret_value(), pool_size=2) as database:
        if args.command == "run":
            protocol = ConfirmationProtocol.model_validate_json(args.protocol.read_bytes())
            result = asyncio.run(run(protocol, database))
        elif args.command == "init-store":
            result = install_schema(database)
        elif args.command == "status":
            result = load_report(database, verify_payloads=args.verify)
        else:
            for chunk in export_evidence(database):
                print(canonical(chunk), flush=True)
            return
    print(canonical(result), flush=True)


if __name__ == "__main__":
    main()
