"""One fixed prospective confirmation, causal labels and an append-only blob journal.

This module never writes trading, v1 observation, or Alpaca market-data records.
"""

from __future__ import annotations

import base64
import json
import zlib
from collections import deque
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from typing import Literal, Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue, model_validator
from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    Integer,
    LargeBinary,
    MetaData,
    String,
    Table,
    UniqueConstraint,
    case,
    column,
    func,
    insert,
    inspect,
    select,
    table,
    update,
)
from sqlalchemy.engine import Connection

from tradeagent.kraken_confirmation_book import BookQuote
from tradeagent.persistence import Database, ProductionRepository, worker_locks
from tradeagent.scalping_market import datetime_ns, ns_datetime
from tradeagent.scalping_store import canonical, insert_once, utc

NS = 1_000_000_000
SLOTS: Literal[25920] = 25_920
type Symbol = Literal["BTC/USD", "ETH/USD"]
SYMBOLS: tuple[Symbol, Symbol] = ("BTC/USD", "ETH/USD")
STUDY_ID: Literal["kraken-book-confirmation-v1"] = "kraken-book-confirmation-v1"
LOCK_NAME = "research:" + STUDY_ID
PAYLOAD_BUDGET: Literal[3221225472] = 3_221_225_472
TERMINAL_RESERVE = 4 * 1024**2
MAX_CHUNK_BYTES = 4 * 1024**2
SOURCE_MODULES = (
    "kraken_confirmation.py",
    "kraken_confirmation_book.py",
    "kraken_confirmation_report.py",
    "kraken_confirmation_runtime.py",
)
LEGACY_SCHEMA_REVISION = "0015_shadow_research_dataset"
AUXILIARY_SCHEMA_REVISION = "0016_kraken_confirmation"


class SourceDigest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    name: str
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class ConfirmationProtocol(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    schema_version: Literal["kraken-book-confirmation-v1"] = STUDY_ID
    study_id: Literal["kraken-book-confirmation-v1"] = STUDY_ID
    frozen_at: AwareDatetime
    start: AwareDatetime
    end: AwareDatetime
    code_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    source_hashes: tuple[SourceDigest, ...]
    account_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    v1_runtime_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    v1_owner_id: str = Field(min_length=1, max_length=128)
    v1_connection_id: str = Field(min_length=1, max_length=128)
    symbols: tuple[Literal["BTC/USD", "ETH/USD"], ...] = ("BTC/USD", "ETH/USD")
    duration_hours: Literal[72] = 72
    slots_per_symbol: Literal[25920] = SLOTS
    evaluation_seconds: Literal[10] = 10
    horizon_seconds: Literal[60] = 60
    settle_seconds: Literal[2] = 2
    capture_tail_seconds: Literal[75] = 75
    freshness_seconds: Literal[2] = 2
    notional_usd: Decimal = Field(
        default=Decimal("10.25"), ge=Decimal("10.25"), le=Decimal("10.25")
    )
    minimum_coverage: Decimal = Field(
        default=Decimal("0.95"),
        ge=Decimal(".95"),
        le=Decimal(".95"),
    )
    depth: Literal[10] = 10
    maximum_payload_bytes: Literal[3221225472] = PAYLOAD_BUDGET
    maximum_connections: Literal[4] = 4
    orders_enabled: Literal[False] = False
    private_kraken_requests: Literal[0] = 0
    trading_authorization: Literal["expired"] = "expired"
    model_state: Literal["no_support"] = "no_support"

    @model_validator(mode="after")
    def contract(self) -> Self:
        if self.end - self.start != timedelta(hours=72) or self.frozen_at >= self.start:
            raise ValueError("freeze before one exactly 72-hour prospective window")
        if self.symbols != SYMBOLS:
            raise ValueError("the ordered two-symbol universe is immutable")
        if tuple(item.name for item in self.source_hashes) != SOURCE_MODULES:
            raise ValueError("pin every isolated confirmation source module, in canonical order")
        days: set[bool] = set()
        cursor = self.start.astimezone(UTC)
        while cursor < self.end:
            days.add(cursor.weekday() >= 5)
            cursor = datetime.combine(cursor.date() + timedelta(days=1), datetime.min.time(), UTC)
        if days != {False, True}:
            raise ValueError("the fixed window must contain both UTC weekend and weekday slots")
        if datetime_ns(self.start) % (10 * NS):
            raise ValueError("start must be aligned to the fixed UTC ten-second grid")
        return self

    @property
    def identity(self) -> str:
        return sha256(canonical(self.model_dump(mode="json")).encode()).hexdigest()

    @property
    def capture_end_ns(self) -> int:
        return datetime_ns(self.end) + 75 * NS

    @property
    def last_settlement_ns(self) -> int:
        return self.slot_ns(SLOTS - 1) + 62 * NS

    def slot_ns(self, slot: int) -> int:
        if not 0 <= slot < SLOTS:
            raise ValueError("slot outside the frozen grid")
        return datetime_ns(self.start) + slot * 10 * NS

    def matured(self, at_ns: int) -> int:
        return max(0, min(SLOTS, (at_ns - datetime_ns(self.start) - 62 * NS) // (10 * NS) + 1))


class Quote(BookQuote):
    symbol: Literal["BTC/USD", "ETH/USD"]
    frame_id: int = Field(gt=0)
    connection_id: str
    epoch: int = Field(ge=0)
    received_ns: int
    received_monotonic_ns: int
    accepted_ns: int

    @model_validator(mode="after")
    def causal(self) -> Self:
        if not self.provider_book_update_ns <= self.received_ns <= self.accepted_ns:
            raise ValueError("quote provider/receipt/acceptance clocks are not causal")
        if self.bid >= self.ask:
            raise ValueError("locked or crossed quote")
        if any(
            value > self.provider_book_update_ns
            for value in (
                self.bid_price_last_changed_ns,
                self.ask_price_last_changed_ns,
                self.bid_size_last_changed_ns,
                self.ask_size_last_changed_ns,
            )
        ):
            raise ValueError("side change clocks exceed validated book-state clock")
        return self


class Invalidation(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    accepted_ns: int
    reason: str
    epoch: int


class Eligibility(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    integrity_eligible: bool
    entry_fresh: bool
    future_fresh: bool
    entry_size: bool
    future_size: bool
    complete: bool
    reasons: tuple[str, ...]


def measure(entry: Quote | None, future: Quote | None, at_ns: int) -> Eligibility:
    def fresh(quote: Quote | None, target: int) -> bool:
        return quote is not None and all(
            0 <= target - clock <= 2 * NS
            for clock in (quote.provider_book_update_ns, quote.received_ns)
        )

    entry_fresh, future_fresh = fresh(entry, at_ns), fresh(future, at_ns + 60 * NS)
    continuity = entry is not None and future is not None and entry.epoch == future.epoch
    entry_size = entry is not None and entry.ask * entry.ask_size >= Decimal("10.25")
    future_size = future is not None and future.bid * future.bid_size >= Decimal("10.25")
    reasons: list[str] = []
    if not entry_fresh:
        reasons.append("DECISION_QUOTE_STALE_OR_MISSING")
    if not future_fresh:
        reasons.append("HORIZON_QUOTE_STALE_OR_MISSING")
    if entry is not None and future is not None and not continuity:
        reasons.append("LOCAL_CONTINUITY_CHANGED")
    # The primary contract classifies size only after freshness/continuity; retain
    # independent size indicators even when freshness already disqualifies a label.
    if not reasons and not (entry_size and future_size):
        reasons.append("INSUFFICIENT_DISPLAYED_LONG_SIZE")
    return Eligibility(
        integrity_eligible=continuity,
        entry_fresh=entry_fresh,
        future_fresh=future_fresh,
        entry_size=entry_size,
        future_size=future_size,
        complete=not reasons,
        reasons=tuple(reasons),
    )


class Evaluation(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    slot: int = Field(ge=0, lt=SLOTS)
    symbol: Literal["BTC/USD", "ETH/USD"]
    at_ns: int
    recorded_ns: int
    missed_scheduled_slot: bool
    entry: Quote | None

    @model_validator(mode="after")
    def contract(self) -> Self:
        if self.recorded_ns < self.at_ns or self.missed_scheduled_slot != (
            self.recorded_ns - self.at_ns >= 10 * NS
        ):
            raise ValueError("scheduled slot timing and missed-slot flag disagree")
        if self.entry is not None and (
            self.missed_scheduled_slot
            or self.entry.symbol != self.symbol
            or self.entry.accepted_ns > self.at_ns
        ):
            raise ValueError("missed or forward-looking decision input")
        return self


class Label(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    evaluation: Evaluation
    resolved_ns: int
    information_available_ns: int
    future: Quote | None
    eligibility: Eligibility
    long_gross_bps: Decimal | None

    @model_validator(mode="after")
    def contract(self) -> Self:
        at = self.evaluation.at_ns
        known = at + 62 * NS
        if self.information_available_ns != known or self.resolved_ns < known:
            raise ValueError("labels cannot mature before the fixed settlement clock")
        entry = self.evaluation.entry
        if entry is not None and entry.accepted_ns > at:
            raise ValueError("forward-looking decision quote")
        if self.future is not None and (
            self.future.received_ns > at + 60 * NS or self.future.accepted_ns > known
        ):
            raise ValueError("forward-looking horizon quote")
        if self.eligibility != measure(entry, self.future, at):
            raise ValueError("label differs from the immutable primary contract")
        gross = (
            (self.future.bid / entry.ask - 1) * 10000
            if self.eligibility.complete and entry is not None and self.future is not None
            else None
        )
        if self.long_gross_bps != gross:
            raise ValueError("label scalar units or price contract changed")
        return self


class Evidence(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    key: str
    at_ns: int
    payload: dict[str, JsonValue]


class CausalGrid:
    """Bounded quote ring and at most seven pending slots per symbol; no 72h tape."""

    def __init__(self, protocol: ConfirmationProtocol) -> None:
        self.protocol = protocol
        self.history: dict[str, deque[Quote | Invalidation]] = {
            symbol: deque() for symbol in SYMBOLS
        }
        self.pending: deque[Evaluation] = deque()
        self.next_slot = 0
        self.last_clock = datetime_ns(protocol.frozen_at)
        self.maximum_backlog = 0
        self.maximum_cache = 0

    def accept(self, quote: Quote) -> None:
        self._clock(quote.accepted_ns)
        self.history[quote.symbol].append(quote)
        self._prune(quote.accepted_ns)

    def invalidate(self, at_ns: int, reason: str, epoch: int) -> None:
        self._clock(at_ns)
        for history in self.history.values():
            history.append(Invalidation(accepted_ns=at_ns, reason=reason, epoch=epoch))
        self._prune(at_ns)

    def _clock(self, at_ns: int) -> None:
        if at_ns < self.last_clock:
            raise ValueError("local_acceptance_clock_regression")
        self.last_clock = at_ns

    def _prune(self, at_ns: int) -> None:
        for history in self.history.values():
            while history and history[0].accepted_ns < at_ns - 70 * NS:
                history.popleft()
            if len(history) > 60_000:
                raise RuntimeError("quote_cache_budget_exceeded")
            self.maximum_cache = max(self.maximum_cache, len(history))

    def select(self, symbol: str, target: int, known: int, *, decision: bool) -> Quote | None:
        for item in reversed(self.history[symbol]):
            if item.accepted_ns > known:
                continue
            if isinstance(item, Invalidation):
                return None
            cutoff = item.accepted_ns if decision else item.received_ns
            if cutoff <= target:
                return item
        return None

    def advance(self, now_ns: int) -> tuple[list[Evaluation], list[Label]]:
        self._clock(now_ns)
        if self.next_slot < SLOTS and now_ns - self.protocol.slot_ns(self.next_slot) >= 2560 * NS:
            raise RuntimeError("scheduler_backlog_budget_exceeded")
        evaluations: list[Evaluation] = []
        labels: list[Label] = []
        while self.next_slot < SLOTS and self.protocol.slot_ns(self.next_slot) <= now_ns:
            slot = self.next_slot
            at_ns = self.protocol.slot_ns(slot)
            missed = now_ns - at_ns >= 10 * NS
            for symbol in SYMBOLS:
                entry = None if missed else self.select(symbol, at_ns, at_ns, decision=True)
                evaluation = Evaluation(
                    slot=slot,
                    symbol=symbol,
                    at_ns=at_ns,
                    recorded_ns=now_ns,
                    missed_scheduled_slot=missed,
                    entry=entry,
                )
                evaluations.append(evaluation)
                self.pending.append(evaluation)
            self.next_slot += 1
        self.maximum_backlog = max(self.maximum_backlog, len(self.pending))
        while self.pending and self.pending[0].at_ns + 62 * NS <= now_ns:
            evaluation = self.pending.popleft()
            known = evaluation.at_ns + 62 * NS
            future = self.select(
                evaluation.symbol, evaluation.at_ns + 60 * NS, known, decision=False
            )
            eligibility = measure(evaluation.entry, future, evaluation.at_ns)
            gross = (
                (future.bid / evaluation.entry.ask - 1) * 10000
                if eligibility.complete and future is not None and evaluation.entry is not None
                else None
            )
            labels.append(
                Label(
                    evaluation=evaluation,
                    resolved_ns=now_ns,
                    information_available_ns=known,
                    future=future,
                    eligibility=eligibility,
                    long_gross_bps=gross,
                )
            )
        self._prune(now_ns)
        return evaluations, labels


evidence_metadata = MetaData()
confirmation_evidence = Table(
    "kraken_confirmation_evidence",
    evidence_metadata,
    Column("study_id", String(64), primary_key=True),
    Column("sequence", Integer, primary_key=True),
    Column("key", String(128), nullable=False),
    Column("kind", String(32), nullable=False),
    Column("owner_id", String(128), nullable=False),
    Column("at_ns", BigInteger, nullable=False),
    Column("previous_hash", String(64), nullable=False),
    Column("payload_hash", String(64), nullable=False),
    Column("chain_hash", String(64), nullable=False),
    Column("decoded_bytes", Integer, nullable=False),
    Column("stored_bytes", BigInteger, nullable=False),
    Column("payload", LargeBinary, nullable=False),
    UniqueConstraint("study_id", "key", name="uq_kraken_confirmation_evidence_key"),
    CheckConstraint("sequence >= 0", name="ck_kraken_confirmation_sequence"),
    CheckConstraint(
        "decoded_bytes > 0 AND decoded_bytes <= 4194304",
        name="ck_kraken_confirmation_decoded_bytes",
    ),
    CheckConstraint(
        "stored_bytes > 0 AND stored_bytes <= 3221225472",
        name="ck_kraken_confirmation_stored_bytes",
    ),
)
confirmation_schema_version = Table(
    "kraken_confirmation_alembic_version",
    MetaData(),
    Column("version_num", String(32), primary_key=True, nullable=False),
)
_legacy_version = table("alembic_version", column("version_num", String(32)))


def _legacy_schema_revision(connection: Connection) -> str:
    if not inspect(connection).has_table("alembic_version"):
        raise RuntimeError("legacy global schema revision is unavailable; do not initialize v1")
    revisions = connection.execute(select(_legacy_version.c.version_num).limit(2)).scalars().all()
    if revisions != [LEGACY_SCHEMA_REVISION]:
        raise RuntimeError("legacy observer requires unchanged global 0015 schema revision")
    return LEGACY_SCHEMA_REVISION


def _check_table(connection: Connection, target: Table) -> None:
    inspector = inspect(connection)
    if not inspector.has_table(target.name):
        raise RuntimeError(f"isolated research table missing: {target.name}")
    expected = {
        item.name: (item.type.compile(dialect=connection.dialect).upper(), item.nullable)
        for item in target.c
    }
    actual = {
        item["name"]: (
            item["type"].compile(dialect=connection.dialect).upper(),
            item["nullable"],
        )
        for item in inspector.get_columns(target.name)
    }
    primary = tuple(inspector.get_pk_constraint(target.name)["constrained_columns"])
    unique = {tuple(item["column_names"]) for item in inspector.get_unique_constraints(target.name)}
    expected_unique = {
        tuple(item.columns.keys())
        for item in target.constraints
        if isinstance(item, UniqueConstraint)
    }
    checks = {item["name"] for item in inspector.get_check_constraints(target.name)}
    expected_checks = {
        item.name for item in target.constraints if isinstance(item, CheckConstraint)
    }
    if (
        actual != expected
        or primary != tuple(target.primary_key.columns.keys())
        or unique != expected_unique
        or checks != expected_checks
    ):
        raise RuntimeError(f"isolated research schema differs from source: {target.name}")


def _auxiliary_revision(connection: Connection) -> str:
    _check_table(connection, confirmation_schema_version)
    revisions = (
        connection.execute(
            select(confirmation_schema_version.c.version_num).limit(2),
        )
        .scalars()
        .all()
    )
    if revisions != [AUXILIARY_SCHEMA_REVISION]:
        raise RuntimeError("isolated research revision missing or conflicting")
    return AUXILIARY_SCHEMA_REVISION


def check_schema(database: Database) -> dict[str, JsonValue]:
    """Read-only compatibility check; global Alembic stays at the old observer's 0015."""
    with database.begin() as connection:
        if connection.dialect.name == "postgresql":
            connection.exec_driver_sql("SET TRANSACTION READ ONLY")
            connection.exec_driver_sql("SET LOCAL statement_timeout = '15000ms'")
        legacy = _legacy_schema_revision(connection)
        revision = _auxiliary_revision(connection)
        _check_table(connection, confirmation_evidence)
    return {
        "global_revision": legacy,
        "global_revision_unchanged": True,
        "research_revision": revision,
        "research_version_table": confirmation_schema_version.name,
        "evidence_table": confirmation_evidence.name,
    }


def install_schema(database: Database) -> dict[str, JsonValue]:
    """Create only two research-owned tables; never upgrade/stamp global Alembic.

    Call before freezing/claiming a study. This is not ``Database.initialize`` or a
    generic Alembic upgrade: only the evidence table and a one-row auxiliary
    revision table are created. Existing rows, v1 leases, controls, protocol and
    global revision are never updated. Unknown/populated untracked schemas and
    missing previously tracked evidence tables are rejected, not repaired.
    """
    if database.engine.dialect.name not in {"postgresql", "sqlite"}:
        raise RuntimeError("isolated research schema supports PostgreSQL or SQLite only")
    with database.begin() as connection:
        if connection.dialect.name == "postgresql":
            connection.exec_driver_sql("SET LOCAL statement_timeout = '15000ms'")
            connection.exec_driver_sql("SET LOCAL lock_timeout = '5000ms'")
            connection.exec_driver_sql(
                "SELECT pg_advisory_xact_lock(hashtext('kraken_confirmation_schema_install'))",
            )
        legacy = _legacy_schema_revision(connection)
        inspector = inspect(connection)
        evidence_exists = inspector.has_table(confirmation_evidence.name)
        revision_exists = inspector.has_table(confirmation_schema_version.name)
        tracked = False
        if revision_exists:
            _check_table(connection, confirmation_schema_version)
            revisions = (
                connection.execute(
                    select(confirmation_schema_version.c.version_num).limit(2),
                )
                .scalars()
                .all()
            )
            if revisions not in ([], [AUXILIARY_SCHEMA_REVISION]):
                raise RuntimeError("refusing to replace a conflicting auxiliary revision")
            tracked = bool(revisions)
        if tracked and not evidence_exists:
            raise RuntimeError("tracked research evidence is missing; no replacement permitted")
        if evidence_exists:
            _check_table(connection, confirmation_evidence)
            if (
                not tracked
                and connection.scalar(
                    select(confirmation_evidence.c.sequence).limit(1),
                )
                is not None
            ):
                raise RuntimeError("refusing to adopt populated untracked research evidence")
        confirmation_evidence.create(bind=connection, checkfirst=True)
        confirmation_schema_version.create(bind=connection, checkfirst=True)
        if not tracked:
            connection.execute(
                insert(confirmation_schema_version).values(
                    version_num=AUXILIARY_SCHEMA_REVISION,
                )
            )
        _check_table(connection, confirmation_evidence)
        _auxiliary_revision(connection)
        if _legacy_schema_revision(connection) != legacy:
            raise RuntimeError("global revision changed during isolated research installation")
    return check_schema(database)


class ChunkHeader(BaseModel):
    model_config = ConfigDict(frozen=True, extra="ignore")
    study_id: str
    sequence: int
    key: str
    kind: str
    owner_id: str
    at_ns: int
    previous_hash: str
    payload_hash: str
    chain_hash: str
    decoded_bytes: int
    stored_bytes: int

    def calculated_hash(self) -> str:
        return sha256(canonical(self.model_dump(exclude={"chain_hash"})).encode()).hexdigest()


def decode_blob(blob: bytes, size: int) -> dict[str, JsonValue]:
    if not 0 < size <= MAX_CHUNK_BYTES:
        raise ValueError("invalid evidence decoded size")
    decoder = zlib.decompressobj()
    try:
        data = decoder.decompress(blob, size + 1)
    except zlib.error as error:
        raise ValueError("evidence compression verification failed") from error
    if len(data) != size or not decoder.eof or decoder.unused_data or decoder.unconsumed_tail:
        raise ValueError("evidence compression/size verification failed")
    value: object = json.loads(data)
    if not isinstance(value, dict):
        raise ValueError("evidence payload must be an object")
    # Pydantic rejects non-JSON scalars; no lossy coercion of durable proof.
    return Evidence.model_validate({"key": "decode", "at_ns": 0, "payload": value}).payload


class ConfirmationStore:
    def __init__(
        self,
        database: Database,
        owner_id: str,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.database, self.owner_id, self.clock = database, owner_id, clock
        self.repository = ProductionRepository(database)

    @staticmethod
    def _read_only(connection: Connection) -> None:
        if connection.dialect.name == "postgresql":
            connection.exec_driver_sql("SET TRANSACTION READ ONLY")
            connection.exec_driver_sql("SET LOCAL statement_timeout = '15000ms'")

    def _lease(self, connection: Connection) -> None:
        if connection.dialect.name == "postgresql":
            connection.exec_driver_sql("SET LOCAL statement_timeout = '15000ms'")
            connection.exec_driver_sql("SET LOCAL lock_timeout = '5000ms'")
        row = (
            connection.execute(
                select(worker_locks).where(worker_locks.c.lock_name == LOCK_NAME).with_for_update()
            )
            .mappings()
            .one_or_none()
        )
        if row is None or row["owner_id"] != self.owner_id:
            raise RuntimeError("confirmation_lease_lost")
        age = self.clock() - utc(row["acquired_at"])
        if not timedelta(0) <= age <= timedelta(seconds=90):
            raise RuntimeError("confirmation_lease_expired")

    def claim(self, protocol: ConfirmationProtocol) -> None:
        protocol = ConfirmationProtocol.model_validate(protocol.model_dump(mode="json"))
        now = self.clock()
        if not protocol.frozen_at <= now < protocol.start:
            raise ValueError("missed prospective start or future freeze")
        with self.database.begin() as connection:
            if connection.scalar(select(confirmation_evidence.c.sequence).limit(1)) is not None:
                raise RuntimeError("single_confirmation_already_claimed_no_restart")
        if not self.repository.acquire_worker_lock(
            LOCK_NAME,
            self.owner_id,
            stale_after_seconds=90,
            observed_at=now,
        ):
            raise RuntimeError("confirmation_already_owned")
        try:
            self.append(
                "protocol",
                "protocol",
                {"protocol": protocol.model_dump(mode="json"), "protocol_hash": protocol.identity},
                at_ns=datetime_ns(now),
                claim=True,
            )
        except (RuntimeError, ValueError):
            self.repository.release_worker_lock(LOCK_NAME, self.owner_id)
            raise

    def refresh(self) -> None:
        with self.database.begin() as connection:
            self._lease(connection)
            connection.execute(
                update(worker_locks)
                .where(
                    worker_locks.c.lock_name == LOCK_NAME,
                    worker_locks.c.owner_id == self.owner_id,
                )
                .values(acquired_at=self.clock()),
            )

    def append(
        self,
        kind: str,
        key: str,
        payload: dict[str, JsonValue],
        *,
        at_ns: int,
        claim: bool = False,
    ) -> ChunkHeader:
        body = canonical(payload).encode()
        if len(body) > MAX_CHUNK_BYTES:
            raise RuntimeError("evidence_chunk_budget_exceeded")
        blob = zlib.compress(body)
        digest = sha256(blob).hexdigest()
        with self.database.begin() as connection:
            self._lease(connection)
            previous = (
                connection.execute(
                    select(confirmation_evidence)
                    .order_by(
                        confirmation_evidence.c.sequence.desc(),
                    )
                    .limit(1)
                    .with_for_update()
                )
                .mappings()
                .one_or_none()
            )
            existing = (
                connection.execute(
                    select(confirmation_evidence).where(confirmation_evidence.c.key == key)
                )
                .mappings()
                .one_or_none()
            )
            if existing is not None:
                header = ChunkHeader.model_validate(existing)
                if (
                    header.payload_hash != digest
                    or header.kind != kind
                    or header.at_ns != at_ns
                    or header.owner_id != self.owner_id
                ):
                    raise ValueError("conflicting_duplicate_append_only_evidence")
                return header
            if claim and previous is not None:
                raise RuntimeError("single_confirmation_already_claimed_no_restart")
            if previous is not None:
                prior = ChunkHeader.model_validate(previous)
                if prior.owner_id != self.owner_id or prior.calculated_hash() != prior.chain_hash:
                    raise RuntimeError("confirmation_owner_or_chain_changed")
                if prior.kind == "terminal":
                    raise RuntimeError("terminal_confirmation_is_immutable")
                sequence, root, charged = prior.sequence + 1, prior.chain_hash, prior.stored_bytes
                if at_ns < prior.at_ns:
                    raise ValueError("evidence_append_clock_regression")
            elif not claim:
                raise RuntimeError("confirmation_protocol_not_claimed")
            else:
                claim_root = payload["protocol_hash"]
                if not isinstance(claim_root, str):
                    raise ValueError("invalid protocol hash")
                sequence, root, charged = 0, claim_root, 0
            charge = len(blob) + 512
            limit = (
                PAYLOAD_BUDGET
                if kind in {"terminal", "report"}
                else (PAYLOAD_BUDGET - TERMINAL_RESERVE)
            )
            if charged + charge > limit:
                raise RuntimeError("confirmation_payload_budget_exceeded")
            header = ChunkHeader(
                study_id=STUDY_ID,
                sequence=sequence,
                key=key,
                kind=kind,
                owner_id=self.owner_id,
                at_ns=at_ns,
                previous_hash=root,
                payload_hash=digest,
                chain_hash="",
                decoded_bytes=len(body),
                stored_bytes=charged + charge,
            )
            header = header.model_copy(update={"chain_hash": header.calculated_hash()})
            values = {**header.model_dump(), "payload": blob}
            if claim:
                if not insert_once(connection, confirmation_evidence, values):
                    raise RuntimeError("single_confirmation_already_claimed_no_restart")
            else:
                connection.execute(insert(confirmation_evidence).values(**values))
        return header

    def chunks(
        self,
        *,
        verify_payloads: bool = False,
        kinds: frozenset[str] | None = None,
    ) -> Iterator[tuple[ChunkHeader, dict[str, JsonValue] | None]]:
        """Page by sequence; never read the whole capture into memory."""
        for header, blob in self._blob_chunks(verify_payloads=verify_payloads, kinds=kinds):
            yield header, decode_blob(blob, header.decoded_bytes) if blob is not None else None

    def export_chunks(self) -> Iterator[dict[str, JsonValue]]:
        """Read-only exact compressed proof export, one independently hashed chunk."""
        self.load_protocol()
        for header, blob in self._blob_chunks(verify_payloads=True):
            if blob is None:
                raise ValueError("evidence export payload unavailable")
            decode_blob(blob, header.decoded_bytes)
            yield {
                "schema": "kraken-book-confirmation-evidence-export-v1",
                "encoding": "zlib-json-v1",
                "header": header.model_dump(mode="json"),
                "payload_base64": base64.b64encode(blob).decode("ascii"),
            }

    def _blob_chunks(
        self,
        *,
        verify_payloads: bool = False,
        kinds: frozenset[str] | None = None,
    ) -> Iterator[tuple[ChunkHeader, bytes | None]]:
        sequence = -1
        expected_root: str | None = None
        charged = 0
        owner: str | None = None
        last_at = -1
        columns = [column for column in confirmation_evidence.c if column.name != "payload"]
        blob_column = (
            confirmation_evidence.c.payload
            if verify_payloads or kinds is None
            else case(
                (confirmation_evidence.c.kind.in_(sorted(kinds)), confirmation_evidence.c.payload),
                else_=None,
            ).label("payload")
        )
        with self.database.begin() as connection:
            self._read_only(connection)
            last_sequence = connection.scalar(select(func.max(confirmation_evidence.c.sequence)))
        if last_sequence is None:
            return
        while True:
            with self.database.begin() as connection:
                self._read_only(connection)
                headers = (
                    connection.execute(
                        select(*columns, blob_column)
                        .where(confirmation_evidence.c.sequence > sequence)
                        .where(confirmation_evidence.c.sequence <= last_sequence)
                        .order_by(confirmation_evidence.c.sequence)
                        .limit(8)
                    )
                    .mappings()
                    .all()
                )
            if not headers:
                if sequence != last_sequence:
                    raise ValueError("durable_evidence_snapshot_incomplete")
                return
            for row in headers:
                header = ChunkHeader.model_validate(row)
                if (
                    header.sequence != sequence + 1
                    or header.study_id != STUDY_ID
                    or header.calculated_hash() != header.chain_hash
                    or (expected_root is not None and header.previous_hash != expected_root)
                    or (owner is not None and owner != header.owner_id)
                    or header.at_ns < last_at
                    or header.stored_bytes <= charged
                    or header.stored_bytes > PAYLOAD_BUDGET
                ):
                    raise ValueError("durable_evidence_hash_chain_verification_failed")
                blob = None
                if verify_payloads or kinds is None or header.kind in kinds:
                    blob = bytes(row["payload"])
                    if sha256(blob).hexdigest() != header.payload_hash:
                        raise ValueError("durable_evidence_payload_hash_mismatch")
                    if header.stored_bytes != charged + len(blob) + 512:
                        raise ValueError("durable_evidence_payload_charge_mismatch")
                yield header, blob
                sequence, expected_root = header.sequence, header.chain_hash
                owner, charged, last_at = header.owner_id, header.stored_bytes, header.at_ns

    def load_protocol(self) -> ConfirmationProtocol:
        with self.database.begin() as connection:
            self._read_only(connection)
            row = (
                connection.execute(
                    select(confirmation_evidence).where(confirmation_evidence.c.sequence == 0)
                )
                .mappings()
                .one()
            )
        header = ChunkHeader.model_validate(row)
        blob = bytes(row["payload"])
        if header.kind != "protocol" or sha256(blob).hexdigest() != header.payload_hash:
            raise ValueError("invalid durable confirmation protocol")
        payload = decode_blob(blob, header.decoded_bytes)
        protocol = ConfirmationProtocol.model_validate(payload["protocol"])
        if (
            payload["protocol_hash"] != protocol.identity
            or header.previous_hash != protocol.identity
            or header.chain_hash != header.calculated_hash()
        ):
            raise ValueError("durable protocol freeze hash mismatch")
        return protocol

    def exists(self) -> bool:
        with self.database.begin() as connection:
            self._read_only(connection)
            return bool(connection.scalar(select(func.count()).select_from(confirmation_evidence)))

    def owner_alive(self) -> bool:
        with self.database.begin() as connection:
            self._read_only(connection)
            row = (
                connection.execute(
                    select(worker_locks).where(worker_locks.c.lock_name == LOCK_NAME)
                )
                .mappings()
                .one_or_none()
            )
        return bool(
            row is not None
            and row["owner_id"] == self.owner_id
            and timedelta(0) <= self.clock() - utc(row["acquired_at"]) <= timedelta(seconds=90)
        )


def record(model: Evaluation | Label) -> Evidence:
    evaluation = model.evaluation if isinstance(model, Label) else model
    kind = "label" if isinstance(model, Label) else "evaluation"
    at = model.resolved_ns if isinstance(model, Label) else model.recorded_ns
    return Evidence(
        key=f"{kind}:{evaluation.slot}:{evaluation.symbol}",
        at_ns=at,
        payload=model.model_dump(mode="json"),
    )


def slot_context(protocol: ConfirmationProtocol, slot: int) -> tuple[str, str, str]:
    at = ns_datetime(protocol.slot_ns(slot))
    return (
        f"6h:{slot // 2160:02d}",
        f"utc_day:{at.date()}",
        ("weekend" if at.weekday() >= 5 else "weekday"),
    )
