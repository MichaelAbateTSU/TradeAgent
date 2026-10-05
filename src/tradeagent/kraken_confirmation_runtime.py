"""Standalone public-only receiver. Importing this module does not launch a job.

Callable: ``await run(protocol, database, guard=callback)``. The module CLI accepts
``run PROTOCOL.json`` or read-only ``preflight PROTOCOL.json``,
``status [--verify] [--study-id ID]`` / ``export [--study-id ID]``.
No worker entry point changes.

Confirmation infrastructure v2 (not shadow research dataset v2):
``ConfirmationProtocol(schema_version="kraken-book-confirmation-v2",
study_id="kraken-book-confirmation-v2", v1_stop_control_sha256=HASH,
v1_protocol_hash=ORIGINAL_SHADOW_PROTOCOL_HASH, ...)`` requires
the exact SHA256 of the persisted UTF-8 stop-control value returned by
``ProductionRepository(database).get_control(STOP_KEY)``. Do not reserialize parsed
JSON to obtain this hash. The pinned existing control must identify
``BROKER_SAFETY_MONITOR_UNAVAILABLE``, ``automatic_rearm=false`` and
``protocol_changed=false``. Guard v2 requires fresh unchanged code/owner/account/
connection and a specifically preserved ``paused_invalid`` v1 with stopped,
unsubscribed, unauthenticated feed. This is NOT a safety-proof fallback: every
check still performs current fixed-host GET-only paper/account/exposure/order
verification and local authorization/model/order checks, and fails on unavailable
or changed evidence. No v1 control is written and v1 is never rearmed.
V1's protocol JSON, ID, subscription guard and immutable original journal remain
backward compatible. Each literal ID has a distinct lease, claim and hash chain.
V2 also verifies both the stored and recomputed original shadow-protocol identity.
Preflight and v2 export first verify the original confirmation-v1 journal without
writing it. V1 serialization excludes new optional fields, preserving its hash.
New v2 broker GET failures report only exception class, allowlisted endpoint path
and HTTP status when available, never headers/query/body/secrets. The old monitor
is unchanged and its historical missing HTTP details remain unknown.

Confirmation infrastructure v3 uses the exact matched schema/study ID
``kraken-book-confirmation-v3`` and requires ``warmup_seconds=60``. It retains v2's
preserved-paused-v1 safety guard and every original observation threshold.
Launch admission is only within the sixty seconds before the frozen start;
schedule externally rather than starting a multi-day warmup capture. This is a
maximum lead/admission bound, not a minimum book age or a changed freshness rule.
Lease heartbeat, current safety proof and cumulative reporting each have their own
single-worker executor and single-connection pool. The writer retains its supplied
pool. Reporting cannot delay cadence or renewals; at most one hourly scan is active,
and a scan lasting an hour or falling more than an hour behind fails closed.
Mature labels resolve in that independent cadence loop at the unchanged +62-second
boundary; the quote ring remains seventy seconds and missing inputs are not backfilled.
The unchanged 90-second owner
fence samples current time after acquiring the lock, never from quote/event time.
Heartbeat and safety checks remain active during final verification; loss of
ownership cannot seal or rewrite the immutable claim. Old v1/v2 paths, serialized
identities, claims and roots remain unchanged. Use explicit v3 status/export IDs.

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
  First use ``preflight PATH`` to verify source bytes, existing schema and the full
  current safety guard without installation or claim. V2 run repeats preflight
  before claiming and performs another guard after the sole claim.
  ``status`` prints a bounded summary; ``status --verify`` also streams/verifies
  every blob. ``export`` emits JSONL with exact base64-encoded compressed bytes and
  per-chunk hash-chain headers; ``export_evidence(database)`` is its read-only
  iterator API. ``ConfirmationStore.chunks()`` reads decoded evidence by page.
  Terminal summaries are embedded in the immutable ``terminal`` journal record.
  Status/export default to v1 forever; use ``--study-id kraken-book-confirmation-v2``
  explicitly for the new namespace. Unknown IDs are rejected, never inferred.
* The default guard reuses the existing GET-only paper monitor; an injected
  synchronous callback returns ``GuardEvidence`` and must prove the same pins.
  Broker credentials are never attached to the fixed public Kraken connection.

Callable modules/sequence:
``tradeagent.kraken_confirmation`` exports ``ConfirmationProtocol``,
``install_schema(database)`` and ``check_schema(database)``.
``tradeagent.kraken_confirmation_runtime`` exports ``source_digests()``,
``observer_guard(database, protocol)`` and
``await run(protocol, database, guard=None, runtime_io=None)``.
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
For bounded read-only forensics, ``ConfirmationStore(..., study_id=ID).
latest_header(kind)`` locates a single journal row and ``read_chunk(sequence)``
returns its header and decoded payload after checking that row's hashes. Batch
payloads contain ``records[*].payload``; quality/report/terminal are direct objects.
These targeted reads do not certify full-prefix completeness; use status --verify
or the streaming export for that. Cooperative report cancellation raises explicitly
at page/row and scheduled-grid boundaries, never returns a successful partial scan.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from collections import Counter
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from threading import Event as ThreadEvent
from typing import Literal, Never, Self
from uuid import uuid4

import httpx
from pydantic import (
    BaseModel,
    ConfigDict,
    JsonValue,
    SerializerFunctionWrapHandler,
    TypeAdapter,
    model_serializer,
    model_validator,
)
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake

from tradeagent.alpaca_paper import AlpacaPaperSettings
from tradeagent.config import AppConfig
from tradeagent.kraken_confirmation import (
    NS,
    SOURCE_MODULES,
    STUDY_ID,
    STUDY_IDS,
    SYMBOLS,
    V2_STUDY_ID,
    V3_STUDY_ID,
    CausalGrid,
    ConfirmationProtocol,
    ConfirmationStore,
    Evidence,
    PausedV1Proof,
    Quote,
    SourceDigest,
    check_schema,
    install_schema,
    legacy_journal_compatibility,
    record,
    study_id,
)
from tradeagent.kraken_confirmation_book import Book
from tradeagent.kraken_confirmation_report import build_report, export_evidence, load_report
from tradeagent.persistence import Database, ProductionRepository, worker_locks
from tradeagent.scalping_market import datetime_ns, timestamp_ns
from tradeagent.scalping_store import canonical, utc
from tradeagent.shadow_dataset import DATASET_ID, ShadowDatasetProtocol, shadow_datasets
from tradeagent.shadow_dataset_monitor import STOP_KEY, PaperReadOnlyMonitor, local_safety

PUBLIC_URL = "wss://ws.kraken.com/v2"
FRAME_LIMIT = 256 * 1024
QUEUE_CAPACITY = 128
QUEUE_TIMEOUT = 5
REPORT_SCAN_TIMEOUT_SECONDS: Literal[3600] = 3600
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
    guard_version: Literal[1, 2] = 1
    preserved_v1: PausedV1Proof | None = None

    @model_validator(mode="after")
    def get_only(self) -> Self:
        if self.read_methods != ("GET",):
            raise ValueError("broker safety observer must be GET-only")
        if (self.guard_version == 2) != (self.preserved_v1 is not None):
            raise ValueError("guard version and paused-v1 preservation proof disagree")
        return self

    @model_serializer(mode="wrap")
    def legacy_shape(self, handler: SerializerFunctionWrapHandler) -> dict[str, object]:
        values: dict[str, object] = handler(self)
        if self.guard_version == 1:
            values.pop("guard_version", None)
            values.pop("preserved_v1", None)
        return values


Guard = Callable[[], GuardEvidence]


class BackpressureError(RuntimeError):
    """No frame may silently disappear when durable persistence falls behind."""


class CurrentBrokerProofError(RuntimeError):
    """Safe new-guard diagnostics; never response bodies, headers, queries or secrets."""

    def __init__(self, error: httpx.HTTPError) -> None:
        request: httpx.Request | None = None
        status: int | None = None
        if isinstance(error, httpx.HTTPStatusError):
            request, status = error.request, error.response.status_code
        elif isinstance(error, httpx.RequestError):
            try:
                request = error.request
            except RuntimeError:
                request = None
        endpoint = (
            request.url.path
            if (
                request is not None
                and request.url.host == "paper-api.alpaca.markets"
                and request.url.path in {"/v2/account", "/v2/positions", "/v2/orders"}
            )
            else None
        )
        self.details: dict[str, JsonValue] = {
            "error_type": type(error).__name__,
            "endpoint": endpoint,
            "http_status": status,
        }
        super().__init__("current GET-only broker proof failed: " + canonical(self.details))


def _frozen_v1(database: Database, protocol: ConfirmationProtocol) -> ShadowDatasetProtocol:
    with database.begin() as connection:
        saved = (
            connection.execute(
                select(
                    shadow_datasets.c.protocol,
                    shadow_datasets.c.protocol_hash,
                ).where(shadow_datasets.c.dataset_id == DATASET_ID)
            )
            .mappings()
            .one()
        )
    frozen = ShadowDatasetProtocol.model_validate(saved["protocol"])
    if frozen.account_digest != protocol.account_digest:
        raise ValueError("observer account differs from the confirmation pin")
    if protocol.study_id != STUDY_ID and (
        saved["protocol_hash"] != protocol.v1_protocol_hash
        or frozen.identity != protocol.v1_protocol_hash
    ):
        raise ValueError("original pinned shadow protocol identity changed")
    return frozen


def _paused_proof(
    database: Database,
    protocol: ConfirmationProtocol,
    details: Mapping[str, object],
    feed: Mapping[str, object],
) -> PausedV1Proof:
    raw = ProductionRepository(database).get_control(STOP_KEY)
    if raw is None or sha256(raw.encode("utf-8")).hexdigest() != protocol.v1_stop_control_sha256:
        raise ValueError("pinned existing v1 stop control is absent or changed")
    stop = JSON_OBJECT.validate_json(raw)
    if (
        details.get("state") != "paused_invalid"
        or feed.get("state") != "stopped"
        or feed.get("subscribed") is not False
        or feed.get("authenticated") is not False
        or stop.get("reason") != "BROKER_SAFETY_MONITOR_UNAVAILABLE"
        or stop.get("automatic_rearm") is not False
        or stop.get("protocol_changed") is not False
        or details.get("account_digest") != protocol.account_digest
        or details.get("trading_authorization") != "expired"
        or details.get("model_state") != "no_support"
        or details.get("orders_submitted") != 0
        or details.get("economic_entries_enabled") is not False
        or (
            details.get("ordinary_entries_enabled") is not None
            and details.get("ordinary_entries_enabled") is not False
        )
    ):
        raise ValueError("specifically pinned paused v1 containment or authority changed")
    if protocol.v1_protocol_hash is None:
        raise ValueError("original shadow protocol pin unavailable")
    return PausedV1Proof(
        stop_control_sha256=sha256(raw.encode("utf-8")).hexdigest(),
        protocol_hash=protocol.v1_protocol_hash,
    )


def _v1_sample(
    database: Database,
    protocol: ConfirmationProtocol,
) -> tuple[datetime, PausedV1Proof | None]:
    with database.begin() as connection:
        lease = (
            connection.execute(
                select(worker_locks).where(
                    worker_locks.c.lock_name == "tradeagent-event-worker",
                )
            )
            .mappings()
            .one()
        )
    heartbeat = ProductionRepository(database).latest_heartbeat("tradeagent-event-worker")
    if heartbeat is None:
        raise ValueError("v1 heartbeat unavailable")
    details = heartbeat[2]
    feed = details.get("feed") or {}
    checked = datetime.now(UTC)
    if (
        not isinstance(feed, dict)
        or not timedelta(0) <= checked - utc(heartbeat[1]) <= timedelta(seconds=30)
        or not timedelta(0) <= checked - utc(lease["acquired_at"]) <= timedelta(seconds=90)
        or heartbeat[0] != protocol.v1_owner_id
        or lease["owner_id"] != protocol.v1_owner_id
        or details.get("code_sha") != protocol.v1_runtime_sha
        or feed.get("connection_id") != protocol.v1_connection_id
        or (protocol.study_id == STUDY_ID and feed.get("subscribed") is not True)
    ):
        raise ValueError("v1 pinned source, ownership, connection or freshness changed")
    paused = (
        _paused_proof(database, protocol, details, feed)
        if (protocol.study_id != STUDY_ID)
        else None
    )
    return checked, paused


def _safe_local(safety: Mapping[str, object]) -> None:
    if (
        safety.get("local_order_attempts_since_freeze") != 0
        or safety.get("reported_order_attempts") != 0
        or safety.get("trading_authorization_renewed") is not False
        or safety.get("reported_trading_authorization") != "expired"
        or safety.get("reported_model_state") != "no_support"
    ):
        raise ValueError("zero-order or expired-authority guard failed")


def _safe_current(
    safety: Mapping[str, object],
    broker: Mapping[str, object],
    protocol: ConfirmationProtocol,
) -> None:
    _safe_local(safety)
    if (
        broker.get("positions") != 0
        or broker.get("open_orders") != 0
        or broker.get("broker_order_records_since_freeze") != 0
    ):
        raise ValueError("zero-order or expired-authority guard failed")
    if protocol.study_id != STUDY_ID and (
        broker.get("account_digest") != protocol.account_digest
        or broker.get("paper_host") != "https://paper-api.alpaca.markets"
        or broker.get("read_methods") != ["GET"]
        or broker.get("order_submission_calls") != 0
    ):
        raise ValueError("direct current GET-only pinned paper broker proof failed")


def observer_guard(database: Database, protocol: ConfirmationProtocol) -> Guard:
    """V1 requires subscription; v2 preserves one exact paused-v1 safety containment."""

    def check() -> GuardEvidence:
        if protocol.study_id != STUDY_ID:
            check_schema(database)
        frozen = _frozen_v1(database, protocol)
        safety = local_safety(database, frozen)
        if protocol.study_id != STUDY_ID:
            _safe_local(safety)
        checked, paused = _v1_sample(database, protocol)
        try:
            with httpx.Client(timeout=10, follow_redirects=False) as client:
                broker = PaperReadOnlyMonitor(
                    AlpacaPaperSettings.model_validate({}),
                    client,
                ).snapshot(frozen)
        except httpx.HTTPError as error:
            if protocol.study_id == STUDY_ID:
                raise
            raise CurrentBrokerProofError(error) from None
        _safe_current(safety, broker, protocol)
        if protocol.study_id != STUDY_ID:
            # Catch state/control/authority changes while the broker GETs were in flight.
            check_schema(database)
            _frozen_v1(database, protocol)
            safety = local_safety(database, frozen)
            _, paused = _v1_sample(database, protocol)
            _safe_current(safety, broker, protocol)
        return GuardEvidence(
            account_digest=protocol.account_digest,
            checked_ns=datetime_ns(checked),
            v1_runtime_sha=protocol.v1_runtime_sha,
            v1_owner_id=protocol.v1_owner_id,
            v1_connection_id=protocol.v1_connection_id,
            guard_version=2 if protocol.study_id != STUDY_ID else 1,
            preserved_v1=paused,
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
    if protocol.study_id != STUDY_ID:
        if (
            evidence.guard_version != 2
            or evidence.preserved_v1 is None
            or evidence.preserved_v1.stop_control_sha256 != protocol.v1_stop_control_sha256
            or evidence.preserved_v1.protocol_hash != protocol.v1_protocol_hash
        ):
            raise ValueError("confirmation v2 requires its exact paused-v1 preservation guard")
    elif evidence.guard_version != 1:
        raise ValueError("confirmation v1 guard semantics cannot be changed")


async def preflight(
    protocol: ConfirmationProtocol,
    database: Database,
    *,
    guard: Guard | None = None,
) -> dict[str, JsonValue]:
    """Read-only source/schema/full-safety proof before any claim; no installation."""
    protocol = ConfirmationProtocol.model_validate(protocol.model_dump(mode="json"))
    verify_sources(protocol)
    if not datetime_ns(protocol.frozen_at) <= now_ns() < datetime_ns(protocol.start):
        raise ValueError("preflight requires a frozen prospective confirmation window")
    schema = await asyncio.to_thread(check_schema, database)
    compatibility = (
        await asyncio.to_thread(legacy_journal_compatibility, database)
        if (protocol.study_id != STUDY_ID)
        else None
    )
    check = guard if guard is not None else observer_guard(database, protocol)
    evidence = await asyncio.to_thread(check)
    validate_guard(evidence, protocol)
    return {
        "preflight": "passed",
        "guard_verified": True,
        "market_confirmation_evaluated": False,
        "clean_integrity": False,
        "decision": "PREFLIGHT_ONLY_NOT_MARKET_CONFIRMATION",
        "v2_ready": False,
        "study_id": protocol.study_id,
        "protocol_hash": protocol.identity,
        "storage": schema,
        "legacy_journal_compatibility": compatibility,
        "guard": evidence.model_dump(mode="json"),
        "claim_created": False,
        "orders_submitted": 0,
        "private_kraken_requests": 0,
    }


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
    runtime_io: RuntimeIO | None = None,
) -> dict[str, JsonValue]:
    """Claim once, run once. Fatal errors seal incomplete; no replay/resume mode."""
    protocol = ConfirmationProtocol.model_validate(protocol.model_dump(mode="json"))
    if protocol.study_id == V3_STUDY_ID:
        return await _run_v3(protocol, database, guard=guard, runtime_io=runtime_io)
    if runtime_io is not None:
        raise ValueError("isolated runtime IO is a confirmation-v3 capability only")
    verify_sources(protocol)
    await asyncio.to_thread(check_schema, database)
    guard = guard if guard is not None else observer_guard(database, protocol)
    if protocol.study_id == V2_STUDY_ID:
        await preflight(protocol, database, guard=guard)
    store = (
        ConfirmationStore(database, str(uuid4()), study_id=protocol.study_id)
        if protocol.study_id == V2_STUDY_ID
        else ConfirmationStore(database, str(uuid4()))
    )
    await asyncio.to_thread(store.claim, protocol)
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
        if isinstance(error, CurrentBrokerProofError):
            receiver.last_error = canonical(error.details)
            receiver.counts["current_broker_guard_failures"] += 1
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
                    "schema": (
                        "kraken-book-confirmation-report-v1"
                        if protocol.study_id == STUDY_ID
                        else "kraken-book-confirmation-report-v2"
                    ),
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
                store.lock_name,
                store.owner_id,
            )
    if failure is not None:
        raise RuntimeError(f"confirmation sealed failed/incomplete: {failure}")
    return report


class RuntimeIO:
    """Three bounded IO lanes/pools so a cumulative scan cannot starve fencing/safety."""

    def __init__(
        self,
        database: Database,
        *,
        lease_database: Database | None = None,
        safety_database: Database | None = None,
        report_database: Database | None = None,
    ) -> None:
        supplied = (lease_database, safety_database, report_database)
        self.owned: tuple[Database, ...] = ()
        if any(item is not None for item in supplied):
            if any(item is None for item in supplied):
                raise ValueError("supply all three isolated IO database handles")
            assert lease_database is not None and safety_database is not None
            assert report_database is not None
        else:
            url = database.engine.url
            if url.get_backend_name() == "sqlite" and url.database in (None, "", ":memory:"):
                raise ValueError("v3 requires isolated pools; memory tests must supply IO handles")
            address = url.render_as_string(hide_password=False)
            lease_database = Database(address, pool_size=1)
            safety_database = Database(address, pool_size=1)
            report_database = Database(address, pool_size=1)
            self.owned = (lease_database, safety_database, report_database)
        self.lease_database = lease_database
        self.safety_database = safety_database
        self.report_database = report_database
        self.executors = {
            name: ThreadPoolExecutor(max_workers=1, thread_name_prefix="confirmation-" + name)
            for name in ("lease", "safety", "report")
        }
        self.report_cancel = ThreadEvent()

    async def call[T](
        self, lane: Literal["lease", "safety", "report"], function: Callable[[], T]
    ) -> T:
        return await asyncio.get_running_loop().run_in_executor(self.executors[lane], function)

    async def close(self) -> None:
        self.report_cancel.set()
        for executor in self.executors.values():
            await asyncio.to_thread(executor.shutdown, wait=True, cancel_futures=True)
        for database in self.owned:
            database.dispose()


async def _pause(stop: asyncio.Event, seconds: float) -> None:
    with suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), seconds)


async def _lease_heartbeat(store: ConfirmationStore, io: RuntimeIO, stop: asyncio.Event) -> None:
    while not stop.is_set():
        await io.call("lease", store.refresh)
        await _pause(stop, 10)


async def _safety_watch(
    protocol: ConfirmationProtocol,
    guard: Guard,
    io: RuntimeIO,
    writer: DurableWriter,
    stop: asyncio.Event,
) -> None:
    sequence = 0
    while not stop.is_set():
        verify_sources(protocol)
        evidence = await io.call("safety", guard)
        validate_guard(evidence, protocol)
        await writer.flush()
        validate_guard(evidence, protocol)
        await writer.append_metadata(
            "guard",
            f"guard:watch:{sequence}",
            evidence.model_dump(mode="json"),
        )
        sequence += 1
        await _pause(stop, 60)


async def _quality_watch(receiver: Receiver, writer: DurableWriter, stop: asyncio.Event) -> None:
    sequence = 0
    while not stop.is_set():
        await writer.flush()
        await writer.append_metadata("quality", f"quality:watch:{sequence}", receiver.quality())
        sequence += 1
        await _pause(stop, 10)


async def _guarded_capture(
    protocol: ConfirmationProtocol,
    guard: Guard,
    io: RuntimeIO,
    receiver: Receiver,
    writer: DurableWriter,
    stop: asyncio.Event,
) -> None:
    verify_sources(protocol)
    evidence = await io.call("safety", guard)
    validate_guard(evidence, protocol)
    await writer.append_metadata("guard", "guard:initial", evidence.model_dump(mode="json"))
    await receiver.work(stop)


async def _report_watch(
    protocol: ConfirmationProtocol,
    reader: ConfirmationStore,
    io: RuntimeIO,
    writer: DurableWriter,
    stop: asyncio.Event,
) -> None:
    completed_hour = 0
    while not stop.is_set():
        hour = min(72, max(0, (now_ns() - datetime_ns(protocol.start)) // (3600 * NS)))
        if hour > completed_hour:
            if hour != completed_hour + 1:
                raise RuntimeError("hourly_report_backlog_budget_exceeded")
            verify_sources(protocol)
            at = datetime_ns(protocol.start) + hour * 3600 * NS

            def calculate_report(target: int = at) -> dict[str, JsonValue]:
                return build_report(reader, at_ns=target, cancel=io.report_cancel)

            try:
                report = await asyncio.wait_for(
                    io.call("report", calculate_report),
                    REPORT_SCAN_TIMEOUT_SECONDS,
                )
            except TimeoutError as error:
                io.report_cancel.set()
                raise RuntimeError("hourly_report_duration_budget_exceeded") from error
            await writer.flush()
            await writer.append_metadata("report", f"hour:{hour}", report)
            completed_hour = hour
        await _pause(stop, 1)


async def _watched[T](
    operation: asyncio.Future[T],
    watchers: tuple[asyncio.Task[None], ...],
) -> T:
    async def signal() -> None:
        await asyncio.shield(operation)

    ready = asyncio.create_task(signal())
    try:
        done, _ = await asyncio.wait((ready, *watchers), return_when=asyncio.FIRST_COMPLETED)
        for watcher in watchers:
            if watcher in done:
                await watcher
                raise RuntimeError("essential confirmation service unexpectedly stopped")
        return await operation
    finally:
        if not ready.done():
            ready.cancel()
        await asyncio.gather(ready, return_exceptions=True)


async def _run_v3(
    protocol: ConfirmationProtocol,
    database: Database,
    *,
    guard: Guard | None,
    runtime_io: RuntimeIO | None,
) -> dict[str, JsonValue]:
    start = datetime_ns(protocol.start)
    if not start - 60 * NS <= now_ns() < start:
        raise ValueError("v3 launch admission is only the sixty seconds before its frozen start")
    io = runtime_io or RuntimeIO(database)
    check = guard if guard is not None else observer_guard(io.safety_database, protocol)
    try:
        await preflight(protocol, io.safety_database, guard=check)
    except (ValueError, RuntimeError, OSError, SQLAlchemyError, httpx.HTTPError):
        await io.close()
        raise
    store = ConfirmationStore(database, str(uuid4()), study_id=protocol.study_id)
    lease_store = ConfirmationStore(
        io.lease_database,
        store.owner_id,
        study_id=protocol.study_id,
        clock=store.clock,
    )
    reader = ConfirmationStore(
        io.report_database,
        store.owner_id,
        study_id=protocol.study_id,
        clock=store.clock,
    )
    try:
        await asyncio.to_thread(store.claim, protocol)
    except (ValueError, RuntimeError, OSError, SQLAlchemyError):
        await io.close()
        raise
    writer = DurableWriter(store)
    writer.task = asyncio.create_task(writer.work())
    grid = CausalGrid(protocol)
    receiver = Receiver(protocol, grid, writer)
    capture_stop, lease_stop, safety_stop, auxiliary_stop = (
        asyncio.Event(),
        asyncio.Event(),
        asyncio.Event(),
        asyncio.Event(),
    )
    heartbeat = asyncio.create_task(_lease_heartbeat(lease_store, io, lease_stop))
    safety = asyncio.create_task(_safety_watch(protocol, check, io, writer, safety_stop))
    capture = asyncio.create_task(
        _guarded_capture(protocol, check, io, receiver, writer, capture_stop),
    )
    quality = asyncio.create_task(_quality_watch(receiver, writer, auxiliary_stop))
    reports = asyncio.create_task(_report_watch(protocol, reader, io, writer, auxiliary_stop))
    services = (heartbeat, safety, quality, reports)
    failure: str | None = None
    sealed = False
    report: dict[str, JsonValue] = {}
    try:
        while now_ns() < protocol.capture_end_ns:
            for task in (writer.task, capture, *services):
                if task.done():
                    await task
                    if task is capture and now_ns() >= protocol.capture_end_ns:
                        break
                    raise RuntimeError("essential confirmation task stopped before fixed end")
            evaluations, labels = grid.advance(now_ns())
            for kind, rows in (("evaluation", evaluations), ("label", labels)):
                if rows:
                    await writer.submit(kind, tuple(record(row) for row in rows))
            await asyncio.sleep(0.1)
    except (
        ValueError,
        RuntimeError,
        OSError,
        TimeoutError,
        SQLAlchemyError,
        httpx.HTTPError,
    ) as error:
        failure = f"{type(error).__name__}:{error}"
        if isinstance(error, CurrentBrokerProofError):
            receiver.last_error = canonical(error.details)
            receiver.counts["current_broker_guard_failures"] += 1
        if isinstance(error, BackpressureError):
            receiver.counts["backpressure_failures"] += 1
    except asyncio.CancelledError:
        failure = "process_cancelled_no_restart"
        raise
    finally:
        active = sys.exception()
        if failure is None and active is not None:
            failure = f"unhandled_process_failure:{type(active).__name__}"
        capture_stop.set()
        if not capture.done():
            capture.cancel()
        capture_results = await asyncio.gather(capture, return_exceptions=True)
        for result in capture_results:
            if isinstance(result, Exception) and failure is None:
                failure = f"{type(result).__name__}:{result}"
        auxiliary_stop.set()
        if failure is not None:
            io.report_cancel.set()
            for task in (quality, reports):
                task.cancel()
        auxiliary = asyncio.gather(quality, reports, return_exceptions=True)
        try:
            watchers = (heartbeat, safety) if failure is None else (heartbeat,)
            results = await _watched(auxiliary, watchers)
            for result in results:
                if isinstance(result, Exception) and failure is None:
                    failure = f"{type(result).__name__}:{result}"
            if failure is None:
                evaluations, labels = grid.advance(now_ns())
                for kind, rows in (("evaluation", evaluations), ("label", labels)):
                    if rows:
                        await writer.submit(kind, tuple(record(row) for row in rows))
            try:
                await writer.flush()
            except (ValueError, RuntimeError, OSError, TimeoutError, SQLAlchemyError) as error:
                failure = failure or f"durable_writer_failure:{error}"
                receiver.counts["persistence_failures"] += 1
                if writer.task is not None and not writer.task.done():
                    writer.task.cancel()
                    await asyncio.gather(writer.task, return_exceptions=True)
            await io.call("lease", lease_store.refresh)
            await writer.append_metadata("quality", "quality:final", receiver.quality())
            if failure is not None:
                # Cancelled hourly scans must not be mistaken for final verification.
                io.report_cancel = ThreadEvent()
            verification = asyncio.create_task(
                io.call(
                    "report",
                    lambda: build_report(
                        reader,
                        at_ns=now_ns(),
                        verify_payloads=True,
                        completing=failure is None,
                        cancel=io.report_cancel,
                    ),
                )
            )
            watchers = (heartbeat, safety) if failure is None else (heartbeat,)
            try:
                verify_sources(protocol)
                report = await _watched(verification, watchers)
            except (
                ValueError,
                RuntimeError,
                OSError,
                TimeoutError,
                SQLAlchemyError,
                httpx.HTTPError,
            ) as error:
                failure = failure or f"final_verification_or_safety_failed:{error}"
                io.report_cancel.set()
                verification.cancel()
                await asyncio.gather(verification, return_exceptions=True)
                report = {
                    "schema": "kraken-book-confirmation-report-v3",
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
            safety_stop.set()
            if failure is not None and not safety.done():
                safety.cancel()
            outcomes = await asyncio.gather(safety, return_exceptions=True)
            for outcome in outcomes:
                if isinstance(outcome, Exception) and failure is None:
                    failure = f"{type(outcome).__name__}:{outcome}"
            if failure is None:
                try:
                    verify_sources(protocol)
                    final = await io.call("safety", check)
                    validate_guard(final, protocol)
                    await writer.append_metadata(
                        "guard", "guard:final", final.model_dump(mode="json")
                    )
                except (
                    ValueError,
                    RuntimeError,
                    OSError,
                    TimeoutError,
                    SQLAlchemyError,
                    httpx.HTTPError,
                ) as error:
                    failure = f"final_current_safety_failed:{error}"
            if writer.task is not None and not writer.task.done():
                try:
                    await writer.close()
                except (ValueError, RuntimeError, OSError, TimeoutError, SQLAlchemyError) as error:
                    failure = failure or f"durable_writer_close_failure:{error}"
                    writer.task.cancel()
                    await asyncio.gather(writer.task, return_exceptions=True)
            if failure is not None:
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
            sealed = True
        except (
            ValueError,
            RuntimeError,
            OSError,
            TimeoutError,
            SQLAlchemyError,
            httpx.HTTPError,
        ) as error:
            io.report_cancel.set()
            raise RuntimeError(
                "confirmation v3 failed/incomplete and could not seal; "
                f"original={failure}; sealing={error}",
            ) from error
        finally:
            for event in (lease_stop, safety_stop, auxiliary_stop):
                event.set()
            io.report_cancel.set()
            for task in (writer.task, capture, *services):
                if not task.done():
                    task.cancel()
            await asyncio.gather(writer.task, capture, *services, return_exceptions=True)
            await io.close()
            if sealed:
                await asyncio.to_thread(
                    store.repository.release_worker_lock, store.lock_name, store.owner_id
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
    inspect_start = subcommands.add_parser("preflight")
    inspect_start.add_argument("protocol", type=Path)
    status = subcommands.add_parser("status")
    status.add_argument("--verify", action="store_true")
    status.add_argument("--study-id", type=study_id, choices=STUDY_IDS, default=STUDY_ID)
    subcommands.add_parser("init-store")
    export = subcommands.add_parser("export")
    export.add_argument("--study-id", type=study_id, choices=STUDY_IDS, default=STUDY_ID)
    args = parser.parse_args()
    with Database(AppConfig().database_url.get_secret_value(), pool_size=2) as database:
        if args.command in ("run", "preflight"):
            protocol = ConfirmationProtocol.model_validate_json(args.protocol.read_bytes())
            result = asyncio.run(
                preflight(protocol, database)
                if args.command == "preflight"
                else run(protocol, database),
            )
        elif args.command == "init-store":
            result = install_schema(database)
        elif args.command == "status":
            result = load_report(
                database,
                verify_payloads=args.verify,
                study_id=args.study_id,
            )
        else:
            for chunk in export_evidence(database, study_id=args.study_id):
                print(canonical(chunk), flush=True)
            return
    print(canonical(result), flush=True)


if __name__ == "__main__":
    main()
