"""Prospective, trade-free dataset: immutable evaluations, causal labels and quality gates."""

from __future__ import annotations

import statistics
import zlib
from collections import Counter, defaultdict, deque
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from typing import Any, Literal, Self
from uuid import NAMESPACE_URL, uuid4, uuid5

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Table,
    UniqueConstraint,
    func,
    select,
    update,
)

from tradeagent.persistence import Database, ProductionRepository, events, metadata, worker_locks
from tradeagent.scalping_config import ScalpingConfig
from tradeagent.scalping_market import BookFeatureEngine, MarketEvent, datetime_ns
from tradeagent.scalping_research import _QuoteReplay
from tradeagent.scalping_store import (
    canonical,
    insert_once,
    json_value,
    scalping_market_batches,
    utc,
)
from tradeagent.scalping_strategy import ScalpStrategy

DATASET_ID: Literal["shadow-research-20261001-v1"] = "shadow-research-20261001-v1"
WINDOW_START = datetime(2026, 10, 1, tzinfo=UTC)
WINDOW_END = datetime(2026, 10, 15, tzinfo=UTC)
LOCK_NAME = "tradeagent-event-worker"
PROFILE: Literal["shadow-research-dataset-v1"] = "shadow-research-dataset-v1"


class ShadowDatasetProtocol(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    schema_version: Literal["shadow-research-dataset-v1"] = PROFILE
    dataset_id: Literal["shadow-research-20261001-v1"] = DATASET_ID
    start: AwareDatetime = WINDOW_START
    end: AwareDatetime = WINDOW_END
    validation_start: AwareDatetime = datetime(2026, 10, 8, tzinfo=UTC)
    frozen_at: AwareDatetime
    code_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    account_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    symbols: tuple[str, ...] = ("BTC/USD", "ETH/USD")
    evaluation_seconds: Literal[10] = 10
    horizons: tuple[int, ...] = (5, 30, 60, 300, 900)
    primary_horizon: Literal[60] = 60
    maximum_quote_age_seconds: Literal[2] = 2
    label_settle_seconds: Literal[2] = 2
    feature_horizon_seconds: Literal[5] = 5
    momentum_threshold: float = Field(default=0.25, ge=0.25, le=0.25)
    reversion_threshold: float = Field(default=0.65, ge=0.65, le=0.65)
    minimum_coverage: float = Field(default=0.95, ge=0.95, le=0.95)
    minimum_episodes_per_symbol: Literal[100] = 100
    assumed_order_type: Literal["marketable_limit_gtc"] = "marketable_limit_gtc"
    assumed_notional_usd: Decimal = Field(default=Decimal("10.25"), gt=0, le=Decimal("10.25"))
    actual_fee_tier: str | None = None
    fee_tier_verified: bool = False
    fee_verification_reference: str | None = None
    fee_unknown_reason: str = (
        "Paper account does not provide independently verified fee-tier evidence"
    )
    assumed_maker_fee_bps: Decimal = Field(default=Decimal("15"), ge=15)
    assumed_entry_fee_bps: Decimal = Field(default=Decimal("25"), ge=25, lt=10000)
    assumed_exit_fee_bps: Decimal = Field(default=Decimal("25"), ge=25, lt=10000)
    additional_slippage_bps: Decimal = Field(default=Decimal("5"), ge=5)
    latency_penalty_bps: Decimal = Field(default=Decimal("3"), ge=3)
    friction_margin_multiple: float = Field(default=1.5, ge=1.5, le=1.5)
    maximum_dataset_bytes: int = Field(default=3 * 1024**3, ge=1, le=3 * 1024**3)
    quote_cache_capacity: int = Field(default=60000, ge=1000, le=60000)
    orders_enabled: Literal[False] = False
    promotion_enabled: Literal[False] = False
    trading_authorization: Literal["expired"] = "expired"
    trading_model_state: Literal["no_support"] = "no_support"

    @model_validator(mode="after")
    def frozen_contract(self) -> Self:
        if self.start != WINDOW_START or self.end != WINDOW_END:
            raise ValueError("the prospective fourteen-day observation dates are immutable")
        if self.frozen_at >= self.start or not self.start < self.validation_start < self.end:
            raise ValueError("freeze before collection and keep chronological validation dates")
        if self.symbols != ("BTC/USD", "ETH/USD") or self.horizons != (5, 30, 60, 300, 900):
            raise ValueError("the universe and markout horizons are frozen")
        if self.fee_tier_verified and (
            not self.actual_fee_tier or not self.fee_verification_reference
        ):
            raise ValueError("a verified fee tier requires an actual tier and evidence reference")
        return self

    @property
    def identity(self) -> str:
        return sha256(canonical(self.model_dump(mode="json")).encode()).hexdigest()

    def strategy_config(self) -> ScalpingConfig:
        return ScalpingConfig(
            cohort_id=self.dataset_id,
            account_digest=self.account_digest,
            approved_at=datetime(2026, 9, 21, tzinfo=UTC),
            autonomous_until=datetime(2026, 9, 28, 14, 40, 7, tzinfo=UTC),
            decision_policy="action-value-v1",
            catastrophic_stop_bps=Decimal("100"),
            symbols=self.symbols,
            feature_horizon_seconds=5,
            momentum_threshold=0.25,
            reversion_threshold=0.65,
        )


shadow_datasets = Table(
    "shadow_datasets",
    metadata,
    Column("dataset_id", String(64), primary_key=True),
    Column("protocol_hash", String(64), nullable=False),
    Column("protocol", JSON, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("state", String(32), nullable=False),
    Column("source_root", String(64), nullable=False),
    Column("source_batches", Integer, nullable=False),
    Column("stored_bytes", BigInteger, nullable=False),
    Column("quality", JSON, nullable=False),
    Column("sealed_at", DateTime(timezone=True)),
)
shadow_source_links = Table(
    "shadow_source_links",
    metadata,
    Column("dataset_id", String(64), ForeignKey("shadow_datasets.dataset_id"), primary_key=True),
    Column(
        "batch_id", String(64), ForeignKey("scalping_market_batches.batch_id"), primary_key=True
    ),
    Column("recorded_at", DateTime(timezone=True), nullable=False),
    Column("sequence", Integer, nullable=False),
    Column("root", String(64), nullable=False),
    UniqueConstraint("dataset_id", "sequence", name="uq_shadow_source_sequence"),
)
shadow_evaluations = Table(
    "shadow_evaluations",
    metadata,
    Column("evaluation_id", String(36), primary_key=True),
    Column("dataset_id", String(64), ForeignKey("shadow_datasets.dataset_id"), nullable=False),
    Column("symbol", String(32), nullable=False),
    Column("evaluated_at", DateTime(timezone=True), nullable=False),
    Column("payload", JSON, nullable=False),
    UniqueConstraint("dataset_id", "symbol", "evaluated_at", name="uq_shadow_evaluation_grid"),
)
Index(
    "ix_shadow_eval_dataset_time",
    shadow_evaluations.c.dataset_id,
    shadow_evaluations.c.evaluated_at,
)
shadow_labels = Table(
    "shadow_labels",
    metadata,
    Column(
        "evaluation_id",
        String(36),
        ForeignKey("shadow_evaluations.evaluation_id"),
        primary_key=True,
    ),
    Column("horizon_seconds", Integer, primary_key=True),
    Column("deadline_at", DateTime(timezone=True), nullable=False),
    Column("resolved_at", DateTime(timezone=True), nullable=False),
    Column("complete", Boolean, nullable=False),
    Column("payload", JSON, nullable=False),
)
shadow_daily_reports = Table(
    "shadow_daily_reports",
    metadata,
    Column("dataset_id", String(64), ForeignKey("shadow_datasets.dataset_id"), primary_key=True),
    Column("report_date", String(10), primary_key=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("payload", JSON, nullable=False),
)
DATASET_TABLES = (
    shadow_datasets,
    shadow_source_links,
    shadow_evaluations,
    shadow_labels,
    shadow_daily_reports,
)


class ShadowDatasetStore:
    def __init__(self, database: Database, protocol: ShadowDatasetProtocol, owner_id: str):
        self.database, self.protocol, self.owner_id = database, protocol, owner_id
        self.clock: Callable[[], datetime] = lambda: datetime.now(UTC)

    def _lease(self, connection: Any, now: datetime) -> None:
        checked_at = self.clock()
        row = (
            connection.execute(
                select(worker_locks).where(worker_locks.c.lock_name == LOCK_NAME).with_for_update()
            )
            .mappings()
            .one_or_none()
        )
        if (
            not row
            or row["owner_id"] != self.owner_id
            or not (timedelta(0) <= checked_at - utc(row["acquired_at"]) <= timedelta(seconds=90))
        ):
            raise RuntimeError("observation worker lease lost")

    def freeze(self, now: datetime) -> None:
        with self.database.begin() as connection:
            self._lease(connection, now)
        freeze_protocol(self.database, self.protocol, now)

    def write(self, rows: list[dict[str, Any]], now: datetime) -> None:
        with self.database.begin() as connection:
            self._lease(connection, now)
            stored = (
                connection.execute(
                    select(shadow_datasets)
                    .where(shadow_datasets.c.dataset_id == self.protocol.dataset_id)
                    .with_for_update()
                )
                .mappings()
                .one()
            )
            amount = 0
            for row in rows:
                table = shadow_evaluations if row["kind"] == "evaluation" else shadow_labels
                values = dict(row["values"])
                values["payload"] = json_value(values["payload"])
                previous = connection.scalar(
                    select(table.c.payload).where(
                        *(
                            table.c[column.name] == values[column.name]
                            for column in table.primary_key
                        )
                    )
                )
                if previous is not None:
                    if canonical(previous) != canonical(values["payload"]):
                        raise ValueError("conflicting duplicate immutable dataset record")
                    continue
                charge = len(canonical(row).encode())
                if stored["stored_bytes"] + amount + charge > self.protocol.maximum_dataset_bytes:
                    raise RuntimeError(
                        "dataset storage budget exhausted; observation is incomplete"
                    )
                if insert_once(connection, table, values):
                    amount += charge
                else:
                    previous = connection.execute(
                        select(table.c.payload).where(
                            *(
                                table.c[column.name] == values[column.name]
                                for column in table.primary_key
                            )
                        )
                    ).scalar_one()
                    if canonical(previous) != canonical(values["payload"]):
                        raise ValueError("conflicting duplicate immutable dataset record")
            connection.execute(
                update(shadow_datasets)
                .where(shadow_datasets.c.dataset_id == self.protocol.dataset_id)
                .values(stored_bytes=stored["stored_bytes"] + amount)
            )

    def persist_tape(self, batch: tuple[MarketEvent, ...], now: datetime) -> None:
        body = canonical([event.model_dump(mode="json") for event in batch]).encode()
        identity = sha256(body).hexdigest()
        compressed = zlib.compress(body)
        with self.database.begin() as connection:
            self._lease(connection, now)
            dataset = (
                connection.execute(
                    select(shadow_datasets)
                    .where(shadow_datasets.c.dataset_id == self.protocol.dataset_id)
                    .with_for_update()
                )
                .mappings()
                .one()
            )
            existing = connection.scalar(
                select(shadow_source_links.c.batch_id).where(
                    shadow_source_links.c.dataset_id == self.protocol.dataset_id,
                    shadow_source_links.c.batch_id == identity,
                )
            )
            if existing:
                return
            if dataset["stored_bytes"] + len(compressed) > self.protocol.maximum_dataset_bytes:
                raise RuntimeError("raw dataset storage budget exhausted")
            insert_once(
                connection,
                scalping_market_batches,
                {
                    "batch_id": identity,
                    "recorded_at": now,
                    "event_count": len(batch),
                    "first_exchange_ns": min(event.exchange_at_ns for event in batch),
                    "last_exchange_ns": max(event.exchange_at_ns for event in batch),
                    "encoding": "zlib-json-v1",
                    "raw": compressed,
                },
            )
            root = sha256((dataset["source_root"] + identity).encode()).hexdigest()
            insert_once(
                connection,
                shadow_source_links,
                {
                    "dataset_id": self.protocol.dataset_id,
                    "batch_id": identity,
                    "recorded_at": now,
                    "sequence": dataset["source_batches"] + 1,
                    "root": root,
                },
            )
            connection.execute(
                update(shadow_datasets)
                .where(shadow_datasets.c.dataset_id == self.protocol.dataset_id)
                .values(
                    source_root=root,
                    source_batches=dataset["source_batches"] + 1,
                    stored_bytes=dataset["stored_bytes"] + len(compressed),
                )
            )

    def update_quality(
        self, quality: dict[str, Any], now: datetime, *, sealed: bool = False
    ) -> None:
        with self.database.begin() as connection:
            self._lease(connection, now)
            previous = (
                connection.scalar(
                    select(shadow_datasets.c.quality).where(
                        shadow_datasets.c.dataset_id == self.protocol.dataset_id
                    )
                )
                or {}
            )
            processes = dict(previous.get("processes") or {})
            processes[str(quality["scope_id"])] = quality
            cumulative: Counter[str] = Counter()
            for process in processes.values():
                cumulative.update(process.get("counts") or {})
            combined = {
                **quality,
                "processes": processes,
                "counts": dict(cumulative),
                "counts_scope": "cumulative across recorded process sessions",
            }
            connection.execute(
                update(shadow_datasets)
                .where(shadow_datasets.c.dataset_id == self.protocol.dataset_id)
                .values(
                    quality=json_value(combined),
                    state="sealed"
                    if sealed
                    else ("collecting" if now >= self.protocol.start else "warming_up"),
                    sealed_at=now if sealed else None,
                )
            )


class ShadowDatasetCollector:
    """Pure observation logic; deliberately has no order engine or broker object."""

    def __init__(self, protocol: ShadowDatasetProtocol):
        self.protocol = protocol
        config = protocol.strategy_config()
        self.features = BookFeatureEngine(config, stale_after_seconds=5)
        self.strategy = ScalpStrategy(config, None)
        self.quotes = _QuoteReplay(protocol.symbols, protocol.maximum_quote_age_seconds * 1000)
        self.history: dict[str, deque[dict[str, Any]]] = {
            symbol: deque() for symbol in protocol.symbols
        }
        self.pending: dict[str, dict[str, Any]] = {}
        self.completed: dict[str, set[int]] = defaultdict(set)
        self.last_slot: int | None = None
        self.previous: MarketEvent | None = None
        self.epoch = 0
        self.session_id = str(uuid4())
        self.capture_code_sha = protocol.code_sha
        self.counts: Counter[str] = Counter()
        self.latencies: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=2000))
        self.seen_quote_ids: dict[str, str] = {}
        self.quote_signatures: dict[str, tuple[Any, ...]] = {}

    def on_market(self, event: MarketEvent, processed_at: datetime) -> None:
        if processed_at < event.received_at:
            self.counts["timestamp_reversals"] += 1
            return
        previous = self.previous
        if previous is not None and event == previous:
            self.counts["duplicate_events"] += 1
            return
        changed = previous is None or event.connection_id != previous.connection_id
        reversed_time = (
            previous is not None
            and not changed
            and (
                event.received_at_ns < previous.received_at_ns
                or event.received_monotonic_ns < previous.received_monotonic_ns
                or event.receive_sequence <= previous.receive_sequence
            )
        )
        gap = (
            previous is not None
            and not changed
            and (event.receive_sequence != previous.receive_sequence + 1)
        )
        if changed or gap or reversed_time or event.event_type == "reset":
            self.epoch += 1
            self.quotes.reset()
            self.counts["continuity_changes"] += 1
        if reversed_time:
            self.counts["timestamp_reversals"] += 1
            return
        self.previous = event
        self.counts["market_events"] += 1
        if event.exchange_at_ns > event.received_at_ns:
            self.counts["timestamp_reversals"] += 1
            return
        self.latencies["exchange_to_receipt_ms"].append(
            (event.received_at_ns - event.exchange_at_ns) / 1_000_000
        )
        self.latencies["receipt_to_processing_ms"].append(
            (processed_at - event.received_at).total_seconds() * 1000
        )
        self.features.on_event(event)
        if event.event_type == "trade":
            return
        quote, source = self.quotes.on_event(event)
        if quote is None:
            self.counts["unusable_quote_events"] += 1
            return
        identity = self.quotes.last_event.get(event.symbol, event.event_id)
        if self.seen_quote_ids.get(event.symbol) == identity:
            self.counts["reused_quote_without_timestamp_refresh"] += 1
            return
        self.seen_quote_ids[event.symbol] = identity
        signature = (quote.exchange_time_ns, quote.bid, quote.ask, quote.bid_size, quote.ask_size)
        if self.quote_signatures.get(event.symbol) == signature:
            self.counts["duplicate_quote_content"] += 1
        self.quote_signatures[event.symbol] = signature
        record = {
            "event_id": identity,
            "quote": quote.model_dump(mode="json"),
            "received_at": quote.received_at.isoformat(),
            "processed_at": processed_at.isoformat(),
            "epoch": f"{self.session_id}:{self.epoch}",
            "source_kind": source,
        }
        history = self.history[event.symbol]
        history.append(record)
        cutoff = processed_at - timedelta(seconds=960)
        while history and (
            len(history) > self.protocol.quote_cache_capacity
            or datetime.fromisoformat(history[0]["received_at"]) < cutoff
        ):
            history.popleft()
            self.counts["quote_cache_evictions"] += 1

    def _at(
        self,
        symbol: str,
        at: datetime,
        *,
        for_decision: bool,
        information_available_at: datetime | None = None,
    ) -> dict[str, Any] | None:
        for row in reversed(self.history[symbol]):
            known = datetime.fromisoformat(
                row["processed_at"] if for_decision else row["received_at"]
            )
            if known <= at and (
                information_available_at is None
                or datetime.fromisoformat(row["processed_at"]) <= information_available_at
            ):
                return row
        return None

    def _fresh(self, observation: dict[str, Any] | None, at: datetime) -> bool:
        if observation is None:
            return False
        q = observation["quote"]
        ages = (
            (at - datetime.fromisoformat(q["received_at"].replace("Z", "+00:00"))).total_seconds(),
            (datetime_ns(at) - int(q["exchange_time_ns"])) / 1_000_000_000,
        )
        return all(0 <= age <= self.protocol.maximum_quote_age_seconds for age in ages)

    def evaluate(self, now: datetime) -> list[dict[str, Any]]:
        if not self.protocol.start <= now < self.protocol.end:
            return []
        slot = int(now.timestamp()) // self.protocol.evaluation_seconds
        if slot == self.last_slot:
            return []
        self.last_slot = slot
        rows = []
        for symbol in self.protocol.symbols:
            identity = str(uuid5(NAMESPACE_URL, f"{self.protocol.dataset_id}:{symbol}:{slot}"))
            quote = self._at(symbol, now, for_decision=True)
            fresh = self._fresh(quote, now)
            self.counts["evaluations"] += 1
            if not fresh:
                self.counts["stale_or_missing_decision_quotes"] += 1
            features = self.features.features(symbol, now)
            raw = features.signal_features() if features is not None else None
            decision = self.strategy.decide(features, inventory=None, now=now) if features else None
            family = decision.family if decision else "none"
            outputs = {
                "momentum_score": decision.features.get("momentum_score") if decision else None,
                "reversion_score": decision.features.get("reversion_score") if decision else None,
                "family": family,
                "qualified_economic_action": decision.action if decision else "hold",
                "economic_model_state": "no_support",
            }
            reasons = list(decision.reasons) if decision else ["FEATURES_UNAVAILABLE"]
            if family == "none":
                reasons.append("NO_SUPPORTED_FAMILY")
            if not fresh:
                reasons.append("DECISION_QUOTE_STALE_OR_MISSING")
            momentum = raw.get("return_5s_bps") if raw else None
            direction = (
                1
                if isinstance(momentum, (float, int)) and momentum > 0
                else (-1 if isinstance(momentum, (float, int)) and momentum < 0 else 0)
            )
            randomized = int(sha256((identity + ":random-v1").encode()).hexdigest()[:8], 16) % 2
            near_miss = False
            for key, threshold in (("momentum_score", 0.25), ("reversion_score", 0.65)):
                value = outputs[key]
                if isinstance(value, (int, float)) and 0.8 * threshold <= value < threshold:
                    near_miss = True
            book = self.quotes.books[symbol]
            depth = {
                "book_current": book.is_current(datetime_ns(now), 2_000_000_000),
                "book_exchange_at_ns": book.exchange_ns,
                "book_received_at_ns": book.received_ns,
                "bids": [
                    [str(price), str(book.bids[price])]
                    for price in sorted(book.bids, reverse=True)[:5]
                ],
                "asks": [[str(price), str(book.asks[price])] for price in sorted(book.asks)[:5]],
            }
            payload = {
                "schema": PROFILE,
                "source_code_sha": self.capture_code_sha,
                "evaluation_id": identity,
                "protocol_hash": self.protocol.identity,
                "timestamp": now.isoformat(),
                "symbol": symbol,
                "outputs": outputs,
                "raw_features": raw,
                "category": "heuristic_candidate"
                if family != "none"
                else ("near_miss" if near_miss else "no_supported_family"),
                "rejection_reasons": reasons,
                "decision_observation": quote,
                "decision_quote_fresh": fresh,
                "depth": depth,
                "data_quality_flags": {
                    "features_unavailable": features is None,
                    "decision_quote_missing": quote is None,
                    "decision_quote_stale": quote is not None and not fresh,
                    "l2_depth_stale_or_unavailable": not depth["book_current"],
                    "economic_model_unqualified": True,
                    "fee_tier_unverified": not self.protocol.fee_tier_verified,
                },
                "market_snapshot": (
                    {
                        "bid": quote["quote"]["bid"],
                        "ask": quote["quote"]["ask"],
                        "midpoint": str(
                            (Decimal(quote["quote"]["bid"]) + Decimal(quote["quote"]["ask"])) / 2
                        ),
                        "spread_bps": str(
                            (Decimal(quote["quote"]["ask"]) - Decimal(quote["quote"]["bid"]))
                            / (
                                (Decimal(quote["quote"]["ask"]) + Decimal(quote["quote"]["bid"]))
                                / 2
                            )
                            * 10000
                        ),
                        "exchange_at": quote["quote"]["exchange_at"],
                        "received_at": quote["quote"]["received_at"],
                        "processed_at": quote["processed_at"],
                    }
                    if quote
                    else None
                ),
                "directions": {
                    "signal": 1 if family != "none" else 0,
                    "random_time_matched": 1 if randomized else -1,
                    "simple_momentum": direction,
                    "simple_mean_reversion": -direction,
                },
                "short_execution_available": False,
                "features_available_at": now.isoformat() if raw else None,
                "costs": {
                    "fee_tier": self.protocol.actual_fee_tier,
                    "fee_tier_verified": self.protocol.fee_tier_verified,
                    "fee_verification_reference": self.protocol.fee_verification_reference,
                    "fee_unknown_reason": self.protocol.fee_unknown_reason,
                    "assumed_entry_fee_bps": str(self.protocol.assumed_entry_fee_bps),
                    "assumed_exit_fee_bps": str(self.protocol.assumed_exit_fee_bps),
                    "spread": "embedded_in_ask_bid_primary_return",
                    "slippage_additional_bps": str(self.protocol.additional_slippage_bps),
                    "latency_penalty_bps": str(self.protocol.latency_penalty_bps),
                    "assumed_order_type": self.protocol.assumed_order_type,
                    "orders_submitted": 0,
                },
            }
            self.pending[identity] = payload
            rows.append(
                {
                    "kind": "evaluation",
                    "values": {
                        "evaluation_id": identity,
                        "dataset_id": self.protocol.dataset_id,
                        "symbol": symbol,
                        "evaluated_at": now,
                        "payload": payload,
                    },
                }
            )
        return rows

    def resolve(self, now: datetime) -> list[dict[str, Any]]:
        rows = []
        for identity, evaluation in list(self.pending.items()):
            at = datetime.fromisoformat(evaluation["timestamp"])
            for horizon in self.protocol.horizons:
                if horizon in self.completed[identity]:
                    continue
                deadline = at + timedelta(seconds=horizon)
                if now < deadline + timedelta(seconds=self.protocol.label_settle_seconds):
                    continue
                entry = evaluation["decision_observation"]
                exit_quote = self._at(
                    evaluation["symbol"],
                    deadline,
                    for_decision=False,
                    information_available_at=now,
                )
                reasons = []
                if not evaluation["decision_quote_fresh"]:
                    reasons.append("DECISION_QUOTE_STALE_OR_MISSING")
                if not self._fresh(exit_quote, deadline):
                    reasons.append("HORIZON_QUOTE_STALE_OR_MISSING")
                if entry and exit_quote and entry["epoch"] != exit_quote["epoch"]:
                    reasons.append("LOCAL_CONTINUITY_CHANGED")
                values: dict[str, Any] = {
                    "long_gross_bps": None,
                    "short_gross_bps": None,
                    "long_net_bps": None,
                    "short_net_bps": None,
                }
                if not reasons:
                    assert entry is not None and exit_quote is not None
                    e, x = entry["quote"], exit_quote["quote"]
                    notional = self.protocol.assumed_notional_usd
                    if (
                        Decimal(e["ask_size"]) * Decimal(e["ask"]) < notional
                        or Decimal(x["bid_size"]) * Decimal(x["bid"]) < notional
                    ):
                        reasons.append("INSUFFICIENT_DISPLAYED_LONG_SIZE")
                    else:
                        ratio = Decimal(x["bid"]) / Decimal(e["ask"])
                        short_ratio = Decimal(x["ask"]) / Decimal(e["bid"])
                        ef, xf = (
                            self.protocol.assumed_entry_fee_bps / 10000,
                            self.protocol.assumed_exit_fee_bps / 10000,
                        )
                        extra = (
                            self.protocol.additional_slippage_bps
                            + self.protocol.latency_penalty_bps
                        )
                        values = {
                            "long_gross_bps": float((ratio - 1) * 10000),
                            "short_gross_bps": float((1 - short_ratio) * 10000),
                            "long_net_bps": float(
                                (ratio * (1 - ef) * (1 - xf) - 1) * 10000 - extra
                            ),
                            "short_net_bps": float(
                                ((1 - ef) - short_ratio * (1 + xf)) * 10000 - extra
                            ),
                        }
                        if (
                            Decimal(e["bid_size"]) * Decimal(e["bid"]) < notional
                            or Decimal(x["ask_size"]) * Decimal(x["ask"]) < notional
                        ):
                            values["short_gross_bps"] = None
                            values["short_net_bps"] = None
                payload = {
                    "schema": "shadow-forward-label-v1",
                    "evaluation_id": identity,
                    "horizon_seconds": horizon,
                    "deadline_at": deadline.isoformat(),
                    "information_available_at": now.isoformat(),
                    "entry_observation": entry,
                    "exit_observation": exit_quote,
                    "complete": not reasons,
                    "missing_reasons": reasons,
                    **values,
                    "short_execution_available": False,
                    "no_midpoint_primary_outcome": True,
                    "no_model_promotion": True,
                }
                latency_stress = {}
                for seconds in (1, 3.5):
                    delayed = self._at(
                        evaluation["symbol"],
                        at + timedelta(seconds=seconds),
                        for_decision=False,
                        information_available_at=now,
                    )
                    if (
                        not reasons
                        and delayed is not None
                        and exit_quote is not None
                        and self._fresh(delayed, at + timedelta(seconds=seconds))
                        and entry is not None
                        and delayed["epoch"] == entry["epoch"]
                        and Decimal(delayed["quote"]["ask"])
                        <= Decimal(entry["quote"]["ask"]) * Decimal("1.0002")
                    ):
                        delayed_ratio = Decimal(exit_quote["quote"]["bid"]) / Decimal(
                            delayed["quote"]["ask"]
                        )
                        stress_net = (
                            delayed_ratio
                            * (1 - self.protocol.assumed_entry_fee_bps / 10000)
                            * (1 - self.protocol.assumed_exit_fee_bps / 10000)
                            - 1
                        ) * 10000 - self.protocol.additional_slippage_bps
                        latency_stress[str(seconds)] = {
                            "complete": True,
                            "entry_observation": delayed,
                            "long_net_bps": float(stress_net),
                            "latency_price_change_embedded": True,
                            "additional_latency_penalty_charged": False,
                        }
                    else:
                        latency_stress[str(seconds)] = {
                            "complete": False,
                            "long_net_bps": None,
                            "reason": "DELAYED_ENTRY_PRICE_UNAVAILABLE_OR_INVALID",
                        }
                payload["latency_stress"] = latency_stress
                rows.append(
                    {
                        "kind": "label",
                        "values": {
                            "evaluation_id": identity,
                            "horizon_seconds": horizon,
                            "deadline_at": deadline,
                            "resolved_at": now,
                            "complete": not reasons,
                            "payload": payload,
                        },
                    }
                )
                self.completed[identity].add(horizon)
                self.counts[f"labels_{horizon}_{'complete' if not reasons else 'missing'}"] += 1
            if len(self.completed[identity]) == len(self.protocol.horizons):
                self.pending.pop(identity)
                self.completed.pop(identity)
        return rows

    def quality(self) -> dict[str, Any]:
        distributions = {}
        for key, values in self.latencies.items():
            ordered = sorted(values)
            distributions[key] = {
                "samples": len(ordered),
                "p50": statistics.median(ordered) if ordered else None,
                "p95": ordered[int((len(ordered) - 1) * 0.95)] if ordered else None,
                "p99": ordered[int((len(ordered) - 1) * 0.99)] if ordered else None,
            }
        return {
            "counts_scope": "current process; durable tables supply dataset-wide totals",
            "scope_id": self.session_id,
            "source_code_sha": self.capture_code_sha,
            "counts": dict(self.counts),
            "latency_ms": distributions,
            "pending_evaluations": len(self.pending),
            "orders_submitted": 0,
            "trading_authorization": "expired",
            "model_state": "no_support",
        }


def dataset_status(
    database: Database, dataset_id: str = DATASET_ID, *, period: date | None = None
) -> dict[str, Any]:
    with database.begin() as connection:
        dataset = (
            connection.execute(
                select(shadow_datasets).where(shadow_datasets.c.dataset_id == dataset_id)
            )
            .mappings()
            .one_or_none()
        )
        if not dataset:
            return {"state": "not_frozen", "orders_submitted": 0, "promotion_allowed": False}
        protocol = ShadowDatasetProtocol.model_validate(dataset["protocol"])
        evaluation_scope = [shadow_evaluations.c.dataset_id == dataset_id]
        if period is not None:
            start = datetime.combine(period, datetime.min.time(), tzinfo=UTC)
            evaluation_scope.extend(
                (
                    shadow_evaluations.c.evaluated_at >= start,
                    shadow_evaluations.c.evaluated_at < start + timedelta(days=1),
                )
            )
        counts = (
            connection.execute(
                select(
                    shadow_evaluations.c.symbol,
                    shadow_labels.c.horizon_seconds,
                    shadow_labels.c.complete,
                    func.count().label("count"),
                )
                .join(
                    shadow_labels,
                    shadow_labels.c.evaluation_id == shadow_evaluations.c.evaluation_id,
                )
                .where(*evaluation_scope)
                .group_by(
                    shadow_evaluations.c.symbol,
                    shadow_labels.c.horizon_seconds,
                    shadow_labels.c.complete,
                )
            )
            .mappings()
            .all()
        )
        evaluations = {
            row["symbol"]: int(row["count"])
            for row in connection.execute(
                select(shadow_evaluations.c.symbol, func.count().label("count"))
                .where(*evaluation_scope)
                .group_by(shadow_evaluations.c.symbol)
            ).mappings()
        }
        latest = connection.scalar(
            select(func.max(shadow_evaluations.c.evaluated_at)).where(
                shadow_evaluations.c.dataset_id == dataset_id
            )
        )
    groups: dict[tuple[str, int], Counter[str]] = defaultdict(Counter)
    for row in counts:
        groups[(row["symbol"], row["horizon_seconds"])][
            "complete" if row["complete"] else "missing"
        ] += row["count"]
    coverage: list[dict[str, Any]] = []
    for symbol in protocol.symbols:
        for horizon in protocol.horizons:
            group = groups[(symbol, horizon)]
            total = group["complete"] + group["missing"]
            coverage.append(
                {
                    "symbol": symbol,
                    "horizon_seconds": horizon,
                    "resolved": total,
                    "complete": group["complete"],
                    "missing": group["missing"],
                    "coverage": group["complete"] / total if total else None,
                    "unresolved": evaluations.get(symbol, 0) - total,
                }
            )
    primary = [row for row in coverage if row["horizon_seconds"] == protocol.primary_horizon]
    reversals = (dataset["quality"].get("counts") or {}).get("timestamp_reversals", 0)
    if period is not None:
        day_start = datetime.combine(period, datetime.min.time(), tzinfo=UTC)
        expected_per_symbol = (
            max(
                0,
                int(
                    (
                        min(protocol.end, day_start + timedelta(days=1))
                        - max(protocol.start, day_start)
                    ).total_seconds()
                ),
            )
            // 10
        )
    else:
        expected_per_symbol = int((protocol.end - protocol.start).total_seconds()) // 10
    passed = (
        period is None
        and dataset["state"] == "sealed"
        and bool(evaluations)
        and all(
            row["complete"] / expected_per_symbol >= 0.95 and row["unresolved"] == 0
            for row in primary
        )
        and reversals == 0
    )
    return {
        "schema": PROFILE,
        "dataset_id": dataset_id,
        "state": dataset["state"],
        "protocol_hash": dataset["protocol_hash"],
        "protocol": dataset["protocol"],
        "start": protocol.start.isoformat(),
        "end": protocol.end.isoformat(),
        "source_manifest_root": dataset["source_root"],
        "source_batches": dataset["source_batches"],
        "stored_bytes": dataset["stored_bytes"],
        "quality": dataset["quality"],
        "evaluations_by_symbol": evaluations,
        "coverage": coverage,
        "expected_evaluations_per_symbol": expected_per_symbol,
        "missing_evaluation_slots": {
            symbol: max(0, expected_per_symbol - evaluations.get(symbol, 0))
            for symbol in protocol.symbols
        },
        "latest_evaluation_at": utc(latest).isoformat() if latest else None,
        "profitability_analysis_allowed": passed,
        "profitability_analysis_blocker": None if passed else "DATASET_OPEN_OR_QUALITY_GATE_FAILED",
        "orders_submitted": 0,
        "trading_authorization": "expired",
        "model_state": "no_support",
        "promotion_allowed": False,
        "actual_fee_tier_verified": protocol.fee_tier_verified,
        "sealed_at": utc(dataset["sealed_at"]).isoformat() if dataset["sealed_at"] else None,
    }


def freeze_protocol(database: Database, protocol: ShadowDatasetProtocol, now: datetime) -> None:
    """Administrative metadata freeze grants no execution authority or worker lease."""
    with database.begin() as connection:
        existing = (
            connection.execute(
                select(shadow_datasets)
                .where(shadow_datasets.c.dataset_id == protocol.dataset_id)
                .with_for_update()
            )
            .mappings()
            .one_or_none()
        )
        if existing:
            if existing["protocol_hash"] != protocol.identity:
                raise ValueError("cannot rewrite a frozen observation protocol")
            return
        if protocol.frozen_at > now:
            raise ValueError("a frozen protocol cannot claim a future registration time")
        if now >= protocol.start:
            raise ValueError("prospective window missed: cannot freeze or backdate after start")
        inserted = insert_once(
            connection,
            shadow_datasets,
            {
                "dataset_id": protocol.dataset_id,
                "protocol_hash": protocol.identity,
                "protocol": protocol.model_dump(mode="json"),
                "created_at": now,
                "state": "warming_up",
                "source_root": protocol.identity,
                "source_batches": 0,
                "stored_bytes": 0,
                "quality": {},
            },
        )
        if not inserted:
            actual = connection.scalar(
                select(shadow_datasets.c.protocol_hash).where(
                    shadow_datasets.c.dataset_id == protocol.dataset_id
                )
            )
            if actual != protocol.identity:
                raise ValueError("concurrent frozen protocol conflict")


def approve_prestart_release(
    database: Database, code_sha: str, *, now: datetime, reason: str
) -> None:
    """Append a safety-repair release; never rewrite the frozen protocol or post-start rules."""
    with database.begin() as connection:
        saved = connection.scalar(
            select(shadow_datasets.c.protocol).where(shadow_datasets.c.dataset_id == DATASET_ID)
        )
        if not saved:
            raise ValueError("a frozen dataset is required before release approval")
        protocol = ShadowDatasetProtocol.model_validate(saved)
        count = connection.scalar(
            select(func.count()).where(shadow_evaluations.c.dataset_id == DATASET_ID)
        )
        if (
            now >= protocol.start
            or count
            or len(code_sha) != 40
            or any(character not in "0123456789abcdef" for character in code_sha)
            or not reason.strip()
        ):
            raise ValueError("release repair requires pre-start time and zero evaluations")
    ProductionRepository(database).append_event(
        "shadow_dataset_release_approved",
        {
            "dataset_id": DATASET_ID,
            "protocol_hash": protocol.identity,
            "code_sha": code_sha,
            "reason": reason,
            "approved_at": now.isoformat(),
        },
        occurred_at=now,
        trace_id=DATASET_ID,
    )


def release_allowed(database: Database, protocol: ShadowDatasetProtocol, code_sha: str) -> bool:
    if code_sha == protocol.code_sha:
        return True
    with database.begin() as connection:
        return bool(
            connection.scalar(
                select(func.count()).where(
                    events.c.event_type == "shadow_dataset_release_approved",
                    events.c.trace_id == protocol.dataset_id,
                    events.c.payload["code_sha"].as_string() == code_sha,
                    events.c.payload["protocol_hash"].as_string() == protocol.identity,
                    events.c.occurred_at < protocol.start,
                )
            )
        )


def persist_daily_quality(
    database: Database, report_date: date, *, now: datetime
) -> dict[str, Any]:
    report = dataset_status(database, period=report_date)
    report = {**report, "report_date": report_date.isoformat(), "created_at": now.isoformat()}
    with database.begin() as connection:
        insert_once(
            connection,
            shadow_daily_reports,
            {
                "dataset_id": DATASET_ID,
                "report_date": report_date.isoformat(),
                "created_at": now,
                "payload": json_value(report),
            },
        )
    return report
