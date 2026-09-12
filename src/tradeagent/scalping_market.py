from __future__ import annotations

import asyncio
import json
import logging
import math
import re
import time
from collections import OrderedDict, deque
from collections.abc import (
    AsyncIterator,
    Awaitable,
    Callable,
    Iterable,
    Iterator,
    Mapping,
    Sequence,
)
from contextlib import AbstractAsyncContextManager, asynccontextmanager, suppress
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, DecimalException, InvalidOperation
from itertools import pairwise
from threading import Lock
from typing import Any, Literal, Self
from uuid import UUID, uuid4

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from tradeagent.alpaca_paper import AlpacaPaperSettings
from tradeagent.alpaca_stream import (
    StreamProtocolError,
    StreamProviderError,
    WebSocketConnection,
)
from tradeagent.scalping_config import ScalpingConfig, ScalpQuote, crypto_symbol

CRYPTO_STREAM_URL = "wss://stream.data.alpaca.markets/v1beta3/crypto/us"
VENUE: Literal["alpaca-crypto-us"] = "alpaca-crypto-us"
NS = 1_000_000_000
ZERO = Decimal(0)
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
LOGGER = logging.getLogger(__name__)
_RFC3339 = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})$")


def datetime_ns(value: datetime) -> int:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("an aware timestamp is required")
    delta = value.astimezone(UTC) - EPOCH
    return ((delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds) * 1000


def ns_datetime(value: int) -> datetime:
    return EPOCH + timedelta(microseconds=value // 1000)


def timestamp_ns(value: str) -> int:
    """Parse RFC3339 without routing the fractional seconds through a float or datetime."""
    match = _RFC3339.fullmatch(value)
    if match is None:
        raise ValueError("an RFC3339 timestamp with at most nine fractional digits is required")
    seconds, fraction, zone = match.groups()
    whole = datetime.fromisoformat(seconds + ("+00:00" if zone == "Z" else zone))
    return datetime_ns(whole) + int((fraction or "").ljust(9, "0"))


def _now() -> datetime:
    return datetime.now(UTC)


def _number(value: object) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ValueError("a finite decimal number is required")
    try:
        number = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("a finite decimal number is required") from exc
    if not number.is_finite():
        raise ValueError("a finite decimal number is required")
    return number


class _MarketDataError(ValueError):
    pass


def _float(value: Decimal | float | int) -> float:
    result = float(value)
    if not math.isfinite(result):
        raise _MarketDataError("feature is outside the finite numeric range")
    return result


class BookLevel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    price: Decimal = Field(gt=0)
    quantity: Decimal = Field(ge=0)


class MarketEvent(BaseModel):
    """Immutable L2 tape record; receive_sequence is local, never an exchange sequence."""

    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    schema_version: Literal[1] = 1
    event_id: str = Field(min_length=1, max_length=128)
    venue: Literal["alpaca-crypto-us"] = VENUE
    symbol: str
    event_type: Literal["book", "quote", "trade", "reset"]
    exchange_at: AwareDatetime
    exchange_at_ns: int = Field(gt=0)
    exchange_timestamp: str
    received_at: AwareDatetime
    received_at_ns: int = Field(gt=0)
    received_monotonic_ns: int = Field(ge=0)
    decoded_at_ns: int | None = Field(default=None, gt=0)
    decoded_monotonic_ns: int | None = Field(default=None, ge=0)
    connection_id: UUID
    receive_sequence: int = Field(gt=0)
    bids: tuple[BookLevel, ...] = ()
    asks: tuple[BookLevel, ...] = ()
    trade_price: Decimal | None = Field(default=None, gt=0)
    trade_quantity: Decimal | None = Field(default=None, gt=0)
    trade_id: str | None = None
    taker_side: Literal["buy", "sell"] | None = None
    reset: bool = False
    reset_reason: str | None = Field(default=None, max_length=128)
    provider_sequence: int | str | None = None

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return crypto_symbol(value)

    @model_validator(mode="after")
    def validate_record(self) -> Self:
        if (
            timestamp_ns(self.exchange_timestamp) != self.exchange_at_ns
            or datetime_ns(self.exchange_at) != self.exchange_at_ns // 1000 * 1000
            or datetime_ns(self.received_at) != self.received_at_ns // 1000 * 1000
        ):
            raise ValueError("canonical timestamp fields disagree")
        if (self.decoded_at_ns is not None and self.decoded_at_ns < self.received_at_ns) or (
            self.decoded_monotonic_ns is not None
            and self.decoded_monotonic_ns < self.received_monotonic_ns
        ):
            raise ValueError("decode timestamps cannot precede the recorded receipt")
        for levels in (self.bids, self.asks):
            if len({level.price for level in levels}) != len(levels):
                raise ValueError("duplicate prices within a book side")
        if self.event_type == "trade":
            if (
                self.trade_price is None
                or self.trade_quantity is None
                or not self.trade_id
                or self.bids
                or self.asks
                or self.reset
            ):
                raise ValueError("trade requires price, quantity and identity, not book fields")
        elif any(
            value is not None
            for value in (self.trade_price, self.trade_quantity, self.trade_id, self.taker_side)
        ):
            raise ValueError("non-trade event contains trade fields")
        if self.event_type == "quote" and (
            len(self.bids) != 1 or len(self.asks) != 1 or self.reset
        ):
            raise ValueError("quote requires exactly one level on each side")
        if self.event_type == "reset" and (
            not self.reset or not self.reset_reason or self.bids or self.asks
        ):
            raise ValueError("connection reset requires a reason and no price levels")
        if self.event_type != "reset" and self.reset_reason is not None:
            raise ValueError("reset_reason belongs to connection reset records")
        return self


def _levels(value: object) -> tuple[BookLevel, ...]:
    if not isinstance(value, list):
        raise ValueError("orderbook side must be an array")
    result = []
    for level in value:
        if not isinstance(level, dict):
            raise ValueError("orderbook level must be an object")
        result.append(BookLevel(price=_number(level.get("p")), quantity=_number(level.get("s"))))
    return tuple(result)


def decode_crypto_message(
    message: Mapping[str, object],
    *,
    connection_id: UUID,
    receive_sequence: int,
    received_at: datetime,
    received_monotonic_ns: int,
    received_at_ns: int | None = None,
) -> MarketEvent:
    symbol, stamp = message.get("S"), message.get("t")
    if not isinstance(symbol, str) or not isinstance(stamp, str):
        raise ValueError("market message requires symbol and RFC3339 timestamp")
    exchange_ns = timestamp_ns(stamp)
    kind = message.get("T")
    fields: dict[str, Any] = {}
    if kind == "o":
        reset = message.get("r", False)
        if not isinstance(reset, bool):
            raise ValueError("orderbook reset must be boolean")
        fields.update(
            event_type="book",
            bids=_levels(message.get("b", [])),
            asks=_levels(message.get("a", [])),
            reset=reset,
        )
    elif kind == "q":
        fields.update(
            event_type="quote",
            bids=(
                BookLevel(price=_number(message.get("bp")), quantity=_number(message.get("bs"))),
            ),
            asks=(
                BookLevel(price=_number(message.get("ap")), quantity=_number(message.get("as"))),
            ),
        )
    elif kind == "t":
        trade_id, side = message.get("i"), message.get("tks")
        if isinstance(trade_id, bool) or not isinstance(trade_id, (int, str)) or not str(trade_id):
            raise ValueError("trade identity is missing")
        if side not in ("B", "S", None):
            raise ValueError("unknown native taker side")
        fields.update(
            event_type="trade",
            trade_price=_number(message.get("p")),
            trade_quantity=_number(message.get("s")),
            trade_id=str(trade_id),
            taker_side={"B": "buy", "S": "sell"}.get(side) if isinstance(side, str) else None,
        )
    else:
        raise ValueError("unsupported crypto market event")
    return MarketEvent(
        event_id=f"{connection_id}:{receive_sequence}",
        symbol=symbol,
        exchange_at=ns_datetime(exchange_ns),
        exchange_at_ns=exchange_ns,
        exchange_timestamp=stamp,
        received_at=received_at,
        received_at_ns=datetime_ns(received_at) if received_at_ns is None else received_at_ns,
        received_monotonic_ns=received_monotonic_ns,
        connection_id=connection_id,
        receive_sequence=receive_sequence,
        **fields,
    )


class _CaptureRecord(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    connection_id: UUID
    receive_sequence: int = Field(gt=0, strict=True)
    received_at: AwareDatetime
    received_monotonic_ns: int = Field(ge=0, strict=True)
    received_at_ns: int | None = Field(default=None, gt=0, strict=True)
    payload: dict[str, Any]


def capture_events(records: Iterable[Mapping[str, Any]]) -> Iterator[MarketEvent]:
    """Convert captured rows without renumbering or inventing market/reset events.

    Only leading handshake acknowledgements on each connection are omitted. A later control
    message requires an explicit connection/reset boundary rather than an inferred book snapshot.
    Load the source JSON with parse_float=Decimal to retain its price and size precision.
    """
    connection: UUID | None = None
    market_started = False
    last_sequence = 0
    last_received_ns = 0
    last_monotonic_ns = 0
    for value in records:
        record = _CaptureRecord.model_validate(value)
        received_ns = (
            datetime_ns(record.received_at)
            if record.received_at_ns is None
            else record.received_at_ns
        )
        if received_ns < last_received_ns:
            raise ValueError("capture receive timestamps regress")
        if record.connection_id != connection:
            connection = record.connection_id
            market_started = False
            last_sequence = last_monotonic_ns = 0
        if (
            record.receive_sequence <= last_sequence
            or record.received_monotonic_ns < last_monotonic_ns
        ):
            raise ValueError("capture local connection ordering regresses")
        last_sequence, last_received_ns, last_monotonic_ns = (
            record.receive_sequence,
            received_ns,
            record.received_monotonic_ns,
        )
        kind = record.payload.get("T")
        if kind in ("o", "q", "t"):
            market_started = True
            yield decode_crypto_message(
                record.payload,
                connection_id=record.connection_id,
                receive_sequence=record.receive_sequence,
                received_at=record.received_at,
                received_at_ns=record.received_at_ns,
                received_monotonic_ns=record.received_monotonic_ns,
            )
        elif kind == "error":
            code = record.payload.get("code")
            if isinstance(code, bool) or not isinstance(code, int):
                raise ValueError("captured provider error requires an integer code")
            raise StreamProviderError(code, stage="captured crypto stream")
        elif market_started:
            raise ValueError("midstream control requires an explicit recorded connection boundary")
        elif kind == "subscription":
            for channel in ("trades", "quotes", "orderbooks"):
                symbols = record.payload.get(channel)
                if not isinstance(symbols, list) or any(
                    not isinstance(item, str) for item in symbols
                ):
                    raise ValueError("captured subscription acknowledgement is invalid")
        elif not (
            kind == "success" and record.payload.get("msg") in ("connected", "authenticated")
        ):
            raise ValueError("unsupported captured control message")


@dataclass
class _Book:
    bids: dict[Decimal, Decimal] = field(default_factory=dict)
    asks: dict[Decimal, Decimal] = field(default_factory=dict)
    valid: bool = False
    awaiting_current_update: bool = False
    reason: str = "snapshot_required"
    exchange_ns: int = 0
    received_ns: int = 0
    version: str | None = None

    def invalidate(self, reason: str) -> None:
        self.bids.clear()
        self.asks.clear()
        self.valid = False
        self.awaiting_current_update = False
        self.reason = reason
        self.version = None

    def apply(self, event: MarketEvent, *, max_levels: int, stale_ns: int) -> str | None:
        reason = None
        if event.exchange_at_ns > event.received_at_ns:
            reason = "future_book"
        elif event.exchange_at_ns < self.exchange_ns:
            reason = "out_of_order_book"
        elif not event.reset and event.received_at_ns - event.exchange_at_ns > stale_ns:
            reason = "stale_book_event"
        elif not event.reset and not self.valid:
            reason = "snapshot_required"
        if reason:
            self.invalidate(reason)
            return reason
        bids = {} if event.reset else self.bids.copy()
        asks = {} if event.reset else self.asks.copy()
        for levels, target in ((event.bids, bids), (event.asks, asks)):
            for level in levels:
                if level.quantity == 0:
                    target.pop(level.price, None)
                else:
                    target[level.price] = level.quantity
        if len(bids) > max_levels or len(asks) > max_levels:
            reason = "book_capacity_exceeded"
        elif not bids or not asks:
            reason = "empty_book"
        elif max(bids) >= min(asks):
            reason = "locked_or_crossed_book"
        if reason:
            self.invalidate(reason)
            return reason
        self.bids, self.asks = bids, asks
        self.exchange_ns, self.received_ns = event.exchange_at_ns, event.received_at_ns
        self.awaiting_current_update = event.received_at_ns - event.exchange_at_ns > stale_ns
        self.valid, self.version = True, event.event_id
        self.reason = "awaiting_current_book_update" if self.awaiting_current_update else "valid"
        return None

    def is_current(self, now_ns: int, stale_ns: int) -> bool:
        return (
            self.valid
            and not self.awaiting_current_update
            and 0 <= now_ns - self.exchange_ns <= stale_ns
            and 0 <= now_ns - self.received_ns <= stale_ns
        )


class _IntegrityError(StreamProtocolError):
    pass


class _QueueOverflowError(_IntegrityError):
    pass


class _ResnapshotRequestedError(_IntegrityError):
    pass


@asynccontextmanager
async def _transport() -> AsyncIterator[WebSocketConnection]:
    async with connect(
        CRYPTO_STREAM_URL,
        ping_interval=20,
        ping_timeout=20,
        close_timeout=5,
        max_queue=16,
        max_size=1_048_576,
    ) as websocket:
        yield websocket


class CryptoMarketFeed:
    def __init__(
        self,
        symbols: Sequence[str],
        credentials: AlpacaPaperSettings,
        *,
        transport: Callable[[], AbstractAsyncContextManager[WebSocketConnection]] | None = None,
        clock: Callable[[], datetime] = _now,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        connection_ids: Callable[[], UUID] = uuid4,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        queue_capacity: int = 5000,
        stale_after_seconds: float = 5.0,
        reconnect_initial_seconds: float = 0.25,
        reconnect_max_seconds: float = 30.0,
        handshake_timeout_seconds: float = 10.0,
    ) -> None:
        self.symbols = tuple(crypto_symbol(symbol) for symbol in symbols)
        if not self.symbols or len(set(self.symbols)) != len(self.symbols):
            raise ValueError("a nonempty, unique crypto universe is required")
        if queue_capacity < len(self.symbols):
            raise ValueError("queue must accommodate a reset for every symbol")
        for seconds in (
            stale_after_seconds,
            reconnect_initial_seconds,
            reconnect_max_seconds,
            handshake_timeout_seconds,
        ):
            if not math.isfinite(seconds) or seconds <= 0:
                raise ValueError("feed timeouts and backoff must be finite and positive")
        if reconnect_initial_seconds > reconnect_max_seconds:
            raise ValueError("initial backoff exceeds maximum")
        self._credentials = credentials
        self._transport = transport or _transport
        self._clock, self._monotonic_ns = clock, monotonic_ns
        self._connection_ids, self._sleep = connection_ids, sleep
        self._stale_ns = int(stale_after_seconds * NS)
        self._initial_backoff, self._max_backoff = (
            reconnect_initial_seconds,
            reconnect_max_seconds,
        )
        self._handshake_timeout = handshake_timeout_seconds
        self._queue: asyncio.Queue[MarketEvent] = asyncio.Queue(maxsize=queue_capacity)
        self._runner: asyncio.Task[None] | None = None
        self._control_lock = Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._startup_request: tuple[str, UUID | None] | None = None
        self._resnapshot_event = asyncio.Event()
        self._resnapshot_reason: str | None = None
        self._stop_requested = False
        self._connection_id: UUID | None = None
        self._sequence = 0
        self._subscription_started_ns: int | None = None
        self._books = {symbol: _Book() for symbol in self.symbols}
        self._trade_counts = {symbol: 0 for symbol in self.symbols}
        self._native_side_counts = {symbol: 0 for symbol in self.symbols}
        self._health: dict[str, Any] = {
            "state": "idle",
            "authenticated": False,
            "subscribed": False,
            "connections": 0,
            "reconnects": 0,
            "gap_count": 0,
            "queue_overflows": 0,
            "dropped_events": 0,
            "market_events": 0,
            "last_error": None,
            "provider_code": None,
            "resnapshot_requests": 0,
            "coalesced_resnapshot_requests": 0,
            "stale_resnapshot_requests": 0,
            "ignored_resnapshot_requests": 0,
            "last_resnapshot_reason": None,
        }

    def health_snapshot(self) -> dict[str, Any]:
        now_ns = datetime_ns(self._clock())
        return {
            **self._health,
            "connection_id": str(self._connection_id) if self._connection_id else None,
            "receive_sequence": self._sequence,
            "resnapshot_pending": self._resnapshot_reason is not None,
            "queued_events": self._queue.qsize(),
            "queue_capacity": self._queue.maxsize,
            "book_state": {
                symbol: {
                    "snapshot_received": book.valid,
                    "current": book.is_current(now_ns, self._stale_ns),
                    "reason": book.reason,
                    "exchange_at_ns": book.exchange_ns or None,
                    "received_at_ns": book.received_ns or None,
                    "observed_trades": self._trade_counts[symbol],
                    "native_taker_side_observed": self._native_side_counts[symbol] > 0,
                }
                for symbol, book in self._books.items()
            },
            "venue": VENUE,
            "data_kind": "aggregate_l2",
            "exchange_sequence_available": False,
            "exact_queue_position_available": False,
            "market_by_order_available": False,
            "transport_liveness": "websocket_ping_pong",
            "market_silence_is_sequence_gap": False,
            "exchange_gap_coverage": "unprovable; only local continuity and book integrity checked",
        }

    def request_resnapshot(self, reason: str, connection_id: UUID | None = None) -> None:
        """Schedule recovery without caller-thread I/O; ignore requests for an old connection."""
        if not reason.strip() or len(reason) > 128:
            raise ValueError("a nonempty resnapshot reason of at most 128 characters is required")
        with self._control_lock:
            if self._stop_requested:
                self._health["ignored_resnapshot_requests"] += 1
            elif self._loop is None:
                self._startup_request = reason, connection_id
            else:
                self._loop.call_soon_threadsafe(self._request_resnapshot, reason, connection_id)

    def _request_resnapshot(self, reason: str, connection_id: UUID | None) -> None:
        if self._stop_requested:
            self._health["ignored_resnapshot_requests"] += 1
            return
        if connection_id is not None and connection_id != self._connection_id:
            self._health["stale_resnapshot_requests"] += 1
            return
        self._health["resnapshot_requests"] += 1
        self._health["last_resnapshot_reason"] = reason
        if self._resnapshot_reason is not None or self._health["state"] in {
            "idle",
            "connecting",
            "reconnecting",
            "awaiting_snapshots",
            "failed",
            "stopped",
        }:
            self._health["coalesced_resnapshot_requests"] += 1
            return
        self._resnapshot_reason = reason
        self._health["gap_count"] += 1
        self._health["state"] = "resnapshot_requested"
        self._reset_events(reason)
        self._resnapshot_event.set()

    def stop(self) -> None:
        with self._control_lock:
            if self._stop_requested:
                return
            self._stop_requested = True
            if self._loop is not None and self._runner is not None:
                self._loop.call_soon_threadsafe(self._runner.cancel)
            elif self._runner is None:
                self._health["state"] = "stopped"

    async def stream(self) -> AsyncIterator[MarketEvent]:
        if self._runner is not None:
            raise RuntimeError("a feed instance supports only one stream")
        if self._stop_requested:
            return
        with self._control_lock:
            self._loop = asyncio.get_running_loop()
            startup_request = self._startup_request
            self._startup_request = None
            self._runner = asyncio.create_task(self._run())
        if startup_request is not None:
            self._request_resnapshot(*startup_request)
        try:
            while True:
                if not self._queue.empty():
                    yield self._queue.get_nowait()
                    continue
                if self._runner.done():
                    await self._runner
                    return
                pending = asyncio.create_task(self._queue.get())
                try:
                    done, _ = await asyncio.wait(
                        (pending, self._runner), return_when=asyncio.FIRST_COMPLETED
                    )
                    if pending in done:
                        yield pending.result()
                finally:
                    if not pending.done():
                        pending.cancel()
                        with suppress(asyncio.CancelledError):
                            await pending
        finally:
            self.stop()
            try:
                with suppress(asyncio.CancelledError):
                    await self._runner
            finally:
                with self._control_lock:
                    self._loop = None

    def _reset_events(self, reason: str) -> None:
        self._health.update(authenticated=False, subscribed=False)
        if self._connection_id is None:
            return
        if self._queue.qsize() + len(self.symbols) > self._queue.maxsize:
            while not self._queue.empty():
                self._queue.get_nowait()
                self._health["dropped_events"] += 1
        at = self._clock()
        for symbol in self.symbols:
            self._books[symbol] = _Book(reason=reason)
            self._sequence += 1
            self._queue.put_nowait(
                MarketEvent(
                    event_id=f"{self._connection_id}:{self._sequence}",
                    symbol=symbol,
                    event_type="reset",
                    exchange_at=at,
                    exchange_at_ns=datetime_ns(at),
                    exchange_timestamp=at.isoformat(),
                    received_at=at,
                    received_at_ns=datetime_ns(at),
                    received_monotonic_ns=self._monotonic_ns(),
                    connection_id=self._connection_id,
                    receive_sequence=self._sequence,
                    reset=True,
                    reset_reason=reason,
                )
            )

    def _publish(self, event: MarketEvent) -> None:
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull as exc:
            self._health["queue_overflows"] += 1
            self._health["gap_count"] += 1
            self._health["dropped_events"] += 1
            self._health.update(state="reconnecting", last_error="queue_overflow")
            self._reset_events("queue_overflow")
            raise _QueueOverflowError("queue_overflow") from exc

    async def _messages(
        self, websocket: WebSocketConnection, *, timeout: float | None
    ) -> tuple[list[dict[str, Any]], datetime, int]:
        payload = await asyncio.wait_for(websocket.recv(), timeout=timeout)
        at, monotonic = self._clock(), self._monotonic_ns()
        if len(payload) > 1_048_576:
            raise _IntegrityError("oversized_frame")
        try:
            values = json.loads(payload, parse_float=Decimal)
        except (ValueError, UnicodeDecodeError) as exc:
            raise _IntegrityError("invalid_json") from exc
        if not isinstance(values, list) or any(not isinstance(value, dict) for value in values):
            raise _IntegrityError("invalid_frame")
        for value in values:
            if value.get("T") == "error":
                code = value.get("code")
                if isinstance(code, bool) or not isinstance(code, int):
                    raise _IntegrityError("invalid_provider_error")
                raise StreamProviderError(code, stage="crypto stream")
        return values, at, monotonic

    async def _expect(
        self, websocket: WebSocketConnection, expected: str
    ) -> tuple[list[dict[str, Any]], datetime, int]:
        messages, at, monotonic = await self._messages(websocket, timeout=self._handshake_timeout)
        found = False
        remainder = []
        for message in messages:
            if message.get("T") == "success" and message.get("msg") == expected:
                found = True
            elif message.get("T") == "subscription" and expected == "subscription":
                for channel in ("trades", "quotes", "orderbooks"):
                    selected = message.get(channel)
                    if (
                        not isinstance(selected, list)
                        or any(not isinstance(symbol, str) for symbol in selected)
                        or not set(self.symbols).issubset(selected)
                    ):
                        raise _IntegrityError("required_crypto_channel_not_subscribed")
                found = True
            else:
                remainder.append(message)
        if not found or (remainder and expected != "subscription"):
            raise _IntegrityError("crypto_handshake_acknowledgement_missing")
        return remainder, at, monotonic

    def _consume(self, messages: list[dict[str, Any]], at: datetime, monotonic: int) -> None:
        if self._resnapshot_reason is not None:
            raise _ResnapshotRequestedError(self._resnapshot_reason)
        if self._connection_id is None:
            raise RuntimeError("no active connection identity")
        for message in messages:
            if message.get("T") not in ("o", "q", "t"):
                raise _IntegrityError("unexpected_stream_message")
            self._sequence += 1
            try:
                event = decode_crypto_message(
                    message,
                    connection_id=self._connection_id,
                    receive_sequence=self._sequence,
                    received_at=at,
                    received_monotonic_ns=monotonic,
                )
                event = MarketEvent.model_validate(
                    {
                        **event.model_dump(),
                        "decoded_at_ns": datetime_ns(self._clock()),
                        "decoded_monotonic_ns": self._monotonic_ns(),
                    }
                )
            except (ValueError, TypeError, OverflowError) as exc:
                raise _IntegrityError("invalid_market_message") from exc
            if event.symbol not in self._books:
                raise _IntegrityError("unsubscribed_symbol")
            self._publish(event)
            self._health["market_events"] += 1
            if event.exchange_at_ns > event.received_at_ns:
                raise _IntegrityError("future_market_event")
            if event.event_type == "book":
                error = self._books[event.symbol].apply(
                    event, max_levels=2000, stale_ns=self._stale_ns
                )
                if error:
                    raise _IntegrityError(error)
            elif event.event_type == "quote" and (
                event.bids[0].price >= event.asks[0].price
                or not event.bids[0].quantity
                or not event.asks[0].quantity
            ):
                raise _IntegrityError("invalid_quote")
            elif event.event_type == "trade":
                self._trade_counts[event.symbol] += 1
                self._native_side_counts[event.symbol] += int(event.taker_side is not None)
        now_ns = datetime_ns(at)
        if (
            self._subscription_started_ns is not None
            and now_ns - self._subscription_started_ns > self._stale_ns
            and any(not book.valid for book in self._books.values())
        ):
            raise _IntegrityError("snapshot_timeout")
        if all(book.valid for book in self._books.values()):
            self._health["state"] = (
                "awaiting_current_book_update"
                if any(book.awaiting_current_update for book in self._books.values())
                else "streaming"
            )

    async def _connection(self) -> None:
        async with self._transport() as websocket:
            await self._expect(websocket, "connected")
            await websocket.send(
                json.dumps(
                    {
                        "action": "auth",
                        "key": self._credentials.key_id.get_secret_value(),
                        "secret": self._credentials.secret_key.get_secret_value(),
                    }
                )
            )
            await self._expect(websocket, "authenticated")
            self._health["authenticated"] = True
            await websocket.send(
                json.dumps(
                    {
                        "action": "subscribe",
                        "trades": self.symbols,
                        "quotes": self.symbols,
                        "orderbooks": self.symbols,
                    }
                )
            )
            remainder, at, monotonic = await self._expect(websocket, "subscription")
            self._subscription_started_ns = datetime_ns(at)
            self._health.update(subscribed=True, state="awaiting_snapshots")
            self._consume(remainder, at, monotonic)
            while not self._stop_requested:
                # Book freshness is not socket liveness: unchanged books need not emit messages.
                timeout = None
                if any(not book.valid for book in self._books.values()):
                    timeout = max(
                        0.0,
                        (
                            self._subscription_started_ns
                            + self._stale_ns
                            - datetime_ns(self._clock())
                        )
                        / NS,
                    )
                messages, at, monotonic = await self._messages(websocket, timeout=timeout)
                self._consume(messages, at, monotonic)

    async def _connection_until_resnapshot(self) -> None:
        connection = asyncio.create_task(self._connection())
        recovery = asyncio.create_task(self._resnapshot_event.wait())
        try:
            done, _ = await asyncio.wait(
                (connection, recovery), return_when=asyncio.FIRST_COMPLETED
            )
            if connection in done:
                await connection
                if not self._stop_requested:
                    raise OSError("crypto stream ended")
                return
            if self._resnapshot_reason is None:
                raise RuntimeError("snapshot recovery wakeup has no reason")
            connection.cancel()
            with suppress(asyncio.CancelledError):
                await connection
            raise _ResnapshotRequestedError(self._resnapshot_reason)
        finally:
            for task in (connection, recovery):
                if not task.done():
                    task.cancel()
            # The selected outcome propagates above; join both tasks even during owner cancellation.
            await asyncio.gather(connection, recovery, return_exceptions=True)

    async def _run(self) -> None:
        backoff = self._initial_backoff
        try:
            while not self._stop_requested:
                connected_at = self._monotonic_ns()
                self._connection_id, self._sequence = self._connection_ids(), 0
                self._health["connections"] += 1
                self._health.update(state="connecting", provider_code=None)
                self._reset_events("connected_snapshot_required")
                try:
                    await self._connection_until_resnapshot()
                except (
                    ConnectionClosed,
                    OSError,
                    TimeoutError,
                    StopAsyncIteration,
                    StreamProtocolError,
                ) as exc:
                    if not isinstance(exc, _QueueOverflowError) and self._resnapshot_reason is None:
                        self._health["gap_count"] += 1
                    retryable = not isinstance(exc, StreamProtocolError) or isinstance(
                        exc, _IntegrityError
                    )
                    if isinstance(exc, StreamProviderError):
                        retryable = exc.retryable
                        self._health["provider_code"] = exc.code
                    reason = str(exc) if isinstance(exc, _IntegrityError) else type(exc).__name__
                    self._health.update(last_error=reason, state="reconnecting")
                    if self._resnapshot_reason is None:
                        self._reset_events(reason)
                    self._resnapshot_reason = None
                    self._resnapshot_event.clear()
                    if not retryable:
                        self._health["state"] = "failed"
                        raise
                    if self._monotonic_ns() - connected_at >= 60 * NS:
                        backoff = self._initial_backoff
                    self._health["reconnects"] += 1
                    self._health["retry_in_seconds"] = backoff
                    await self._sleep(backoff)
                    backoff = min(backoff * 2, self._max_backoff)
        except asyncio.CancelledError:
            if not self._stop_requested:
                raise
        finally:
            if self._health["state"] != "failed":
                self._health["state"] = "stopped"
            self._resnapshot_reason = None
            self._resnapshot_event.clear()
            self._reset_events("feed_stopped")


class BookFeatures(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    symbol: str
    as_of: AwareDatetime
    as_of_ns: int = Field(gt=0)
    quote: ScalpQuote
    source_event_id: str
    history_seconds: float = Field(ge=0)
    ready: bool
    l1_imbalance: float
    l5_imbalance: float
    ofi_1s: float
    ofi_5s: float
    normalized_ofi_1s: float
    normalized_ofi_5s: float
    taker_delta_1s: float | None
    taker_delta_5s: float | None
    known_taker_fraction_5s: float | None
    microprice: Decimal = Field(gt=0)
    microprice_displacement_bps: float
    spread_bps: float = Field(ge=0)
    bid_depth_l5: float = Field(gt=0)
    ask_depth_l5: float = Field(gt=0)
    bid_slope_bps_per_unit: float | None
    ask_slope_bps_per_unit: float | None
    bid_additions_1s: float = Field(ge=0)
    ask_additions_1s: float = Field(ge=0)
    bid_additions_5s: float = Field(ge=0)
    ask_additions_5s: float = Field(ge=0)
    bid_removals_proxy_5s: float = Field(ge=0)
    ask_removals_proxy_5s: float = Field(ge=0)
    bid_depletion_fraction_5s: float = Field(ge=0)
    ask_depletion_fraction_5s: float = Field(ge=0)
    trade_intensity_5s: float = Field(ge=0)
    event_intensity_5s: float = Field(ge=0)
    trade_volume_5s: float = Field(ge=0)
    trade_count_5s: int = Field(default=0, ge=0)
    native_taker_trade_count_5s: int = Field(default=0, ge=0)
    return_1s_bps: float | None
    return_5s_bps: float | None
    return_15s_bps: float | None
    volatility_1s_bps: float | None
    volatility_5s_bps: float | None
    volatility_15s_bps: float | None
    session_vwap: Decimal | None = Field(default=None, gt=0)
    session_poc: Decimal | None = Field(default=None, gt=0)
    session_trade_count: int = Field(ge=0)
    session_observed_volume: Decimal = Field(ge=0)
    session_vwap_displacement_bps: float | None
    atr_1m: float | None = Field(default=None, ge=0)
    book_exchange_at_ns: int | None = Field(default=None, gt=0)
    book_received_at_ns: int | None = Field(default=None, gt=0)
    quote_book_time_difference_ms: float | None = Field(default=None, ge=0)
    cancellation_intensity_5s: None = None
    aggregate_change_proxies: Literal[True] = True
    exact_queue_position_available: Literal[False] = False
    session_coverage: Literal["observed_trades_only"] = "observed_trades_only"

    @model_validator(mode="after")
    def validate_causality(self) -> Self:
        if (
            self.symbol != self.quote.symbol
            or self.quote.received_at > self.as_of
            or datetime_ns(self.as_of) != self.as_of_ns // 1000 * 1000
            or self.quote.exchange_time_ns > self.as_of_ns
        ):
            raise ValueError("features must contain only already-received evidence")
        return self

    def signal_features(self) -> dict[str, float | int | str | None]:
        values: dict[str, float | int | str | None] = {}
        for key, value in self.model_dump().items():
            if key in ("quote", "as_of"):
                continue
            if value is None or isinstance(value, (str, int, float)):
                values[key] = value
            elif isinstance(value, Decimal):
                values[key] = _float(value)
        return values


@dataclass(frozen=True)
class _BBO:
    exchange_ns: int
    received_ns: int
    quote: ScalpQuote
    ofi: Decimal
    source_id: str

    @property
    def mid(self) -> Decimal:
        return (self.quote.bid + self.quote.ask) / 2


@dataclass(frozen=True)
class _Change:
    exchange_ns: int
    received_ns: int
    bid_add: Decimal
    ask_add: Decimal
    bid_remove: Decimal
    ask_remove: Decimal
    bid_depletion: Decimal
    ask_depletion: Decimal


@dataclass
class _Minute:
    start_ns: int
    high: Decimal
    low: Decimal
    close: Decimal


@dataclass
class _SymbolState:
    book: _Book = field(default_factory=_Book)
    connection_id: UUID | None = None
    last_sequence: int = 0
    last_received_ns: int = 0
    last_monotonic_ns: int = 0
    last_quote_ns: int = 0
    last_trade_ns: int = 0
    history_start_ns: int = 0
    bbo: deque[_BBO] = field(default_factory=deque)
    changes: deque[_Change] = field(default_factory=deque)
    trades: deque[MarketEvent] = field(default_factory=deque)
    events: deque[tuple[int, int]] = field(default_factory=deque)
    trade_ids: OrderedDict[tuple[str, str, str, int], None] = field(default_factory=OrderedDict)
    dedup_exhausted_at_ns: int = 0
    session_day: date | None = None
    session_notional: Decimal = ZERO
    session_volume: Decimal = ZERO
    session_count: int = 0
    price_volume: dict[Decimal, Decimal] = field(default_factory=dict)
    poc_overflow: bool = False
    minute: _Minute | None = None
    minute_covered: bool = False
    previous_close: Decimal | None = None
    true_ranges: deque[Decimal] = field(default_factory=lambda: deque(maxlen=14))
    counters: dict[str, int] = field(default_factory=dict)
    resnapshot_required: bool = False
    resnapshot_reason: str | None = None

    def count(self, name: str) -> None:
        self.counters[name] = self.counters.get(name, 0) + 1

    def clear_history(self) -> None:
        self.history_start_ns = 0
        self.bbo.clear()
        self.changes.clear()
        self.trades.clear()
        self.events.clear()
        self.minute = None
        self.minute_covered = False
        self.previous_close = None
        self.true_ranges.clear()


class BookFeatureEngine:
    def __init__(
        self,
        config: ScalpingConfig,
        *,
        max_levels: int = 2000,
        max_events: int = 20000,
        stale_after_seconds: float = 5.0,
    ) -> None:
        if max_levels < 1 or max_events < 2:
            raise ValueError("book and rolling capacities must be positive")
        if not math.isfinite(stale_after_seconds) or stale_after_seconds <= 0:
            raise ValueError("staleness interval must be finite and positive")
        self.config = config
        self._max_levels, self._max_events = max_levels, max_events
        self._stale_ns = int(stale_after_seconds * NS)
        self._retention_ns = int(max(90.0, config.feature_horizon_seconds + 15) * NS)
        self._states = {symbol: _SymbolState() for symbol in config.symbols}
        self._connection_id: UUID | None = None
        self._retired_connections: OrderedDict[UUID, None] = OrderedDict()
        self._last_event: MarketEvent | None = None
        self._local_receive_gaps = 0
        self._resnapshot_generation = 0
        self.on_resnapshot: Callable[[str, UUID | None], None] | None = None

    def _state(self, symbol: str) -> _SymbolState:
        normalized = crypto_symbol(symbol)
        if normalized not in self._states:
            raise ValueError("symbol outside the configured crypto universe")
        return self._states[normalized]

    def _invalidate(
        self, state: _SymbolState, reason: str, *, request_snapshot: bool = True
    ) -> None:
        state.book.invalidate(reason)
        state.clear_history()
        state.count(reason)
        if not request_snapshot:
            state.resnapshot_required = False
            state.resnapshot_reason = None
        elif not state.resnapshot_required:
            state.resnapshot_required = True
            state.resnapshot_reason = reason
            self._resnapshot_generation += 1
            LOGGER.warning("Crypto feature state requires a fresh snapshot: %s", reason)
            if self.on_resnapshot is not None:
                self.on_resnapshot(reason, state.connection_id)

    def reset(self, symbol: str | None = None, *, reason: str = "external_reset") -> None:
        if not reason:
            raise ValueError("reset reason is required")
        states = [self._state(symbol)] if symbol is not None else list(self._states.values())
        for state in states:
            self._invalidate(state, reason)

    def _prune(self, state: _SymbolState, at_ns: int) -> None:
        cutoff = at_ns - self._retention_ns
        for samples in (state.bbo, state.changes):
            while samples and samples[0].received_ns < cutoff:
                samples.popleft()
        while state.trades and state.trades[0].received_at_ns < cutoff:
            state.trades.popleft()
        while state.events and state.events[0][1] < cutoff:
            state.events.popleft()
        if any(
            len(samples) >= self._max_events
            for samples in (state.bbo, state.changes, state.trades, state.events)
        ):
            # The book is intact, but a truncated rolling window is not valid feature history.
            state.clear_history()
            state.count("feature_window_overflow")

    def _receive(self, event: MarketEvent, state: _SymbolState) -> bool:
        if self._connection_id != event.connection_id:
            if event.connection_id in self._retired_connections:
                self.reset(reason="retired_connection_event")
                return False
            if self._connection_id is not None:
                self._retired_connections[self._connection_id] = None
                if len(self._retired_connections) > 64:
                    self._retired_connections.popitem(last=False)
            self._connection_id, self._last_event = event.connection_id, None
            for item in self._states.values():
                self._invalidate(item, "connection_changed", request_snapshot=False)
                item.book = _Book()
                item.connection_id = event.connection_id
                item.last_sequence = item.last_received_ns = item.last_monotonic_ns = 0
                item.last_quote_ns = 0
        previous = self._last_event
        if previous is not None:
            if event == previous:
                state.count("duplicate_event")
                return False
            if event.receive_sequence <= previous.receive_sequence:
                self.reset(reason="local_receive_order_error")
                return False
            if (
                event.received_at_ns < previous.received_at_ns
                or event.received_monotonic_ns < previous.received_monotonic_ns
            ):
                self.reset(reason="local_clock_regression")
                return False
            if event.receive_sequence != previous.receive_sequence + 1:
                self._local_receive_gaps += 1
                self.reset(reason="local_receive_sequence_gap")
        self._last_event = event
        state.last_sequence = event.receive_sequence
        state.last_received_ns, state.last_monotonic_ns = (
            event.received_at_ns,
            event.received_monotonic_ns,
        )
        return True

    def on_event(self, event: MarketEvent) -> bool:
        state = self._states.get(event.symbol)
        if state is None:
            self.reset(reason="unconfigured_market_symbol")
            return False
        try:
            return self._apply_event(state, event)
        except (_MarketDataError, DecimalException, ValidationError, OverflowError):
            self._invalidate(state, "invalid_market_numeric_domain")
            return False

    def _apply_event(self, state: _SymbolState, event: MarketEvent) -> bool:
        if not self._receive(event, state):
            return False
        if event.event_type == "reset":
            self._invalidate(
                state, event.reset_reason or "connection_reset", request_snapshot=False
            )
            return True
        if event.exchange_at_ns > event.received_at_ns:
            self._invalidate(state, "future_market_event")
            return False
        for level in (*event.bids, *event.asks):
            _float(level.price)
            _float(level.quantity)
        for value in (event.trade_price, event.trade_quantity):
            if value is not None:
                _float(value)
        self._prune(state, event.received_at_ns)
        if event.event_type == "book":
            old_bids, old_asks = state.book.bids.copy(), state.book.asks.copy()
            awaiting_current = state.book.awaiting_current_update
            reason = state.book.apply(event, max_levels=self._max_levels, stale_ns=self._stale_ns)
            if reason:
                self._invalidate(state, reason)
                return False
            if event.reset:
                state.resnapshot_required = False
                state.resnapshot_reason = None
            if event.reset or awaiting_current:
                state.clear_history()
            else:
                bid_add, bid_remove = self._difference(old_bids, state.book.bids)
                ask_add, ask_remove = self._difference(old_asks, state.book.asks)
                state.changes.append(
                    _Change(
                        event.exchange_at_ns,
                        event.received_at_ns,
                        bid_add,
                        ask_add,
                        bid_remove,
                        ask_remove,
                        bid_remove / sum(old_bids.values(), ZERO) if old_bids else ZERO,
                        ask_remove / sum(old_asks.values(), ZERO) if old_asks else ZERO,
                    )
                )
            # An old provider snapshot seeds reconstruction, not elapsed feature history or OFI.
            if not state.book.awaiting_current_update:
                bid, ask = max(state.book.bids), min(state.book.asks)
                self._observe_bbo(
                    state, event, bid, ask, state.book.bids[bid], state.book.asks[ask]
                )
            else:
                state.count("old_snapshot_awaiting_current_update")
        elif event.event_type == "quote":
            if event.exchange_at_ns < state.last_quote_ns:
                state.count("out_of_order_quote")
                return False
            state.last_quote_ns = event.exchange_at_ns
            bid_level, ask_level = event.bids[0], event.asks[0]
            if (
                bid_level.price >= ask_level.price
                or not bid_level.quantity
                or not ask_level.quantity
            ):
                self._invalidate(state, "invalid_quote")
                return False
            if state.book.is_current(event.received_at_ns, self._stale_ns):
                self._observe_bbo(
                    state,
                    event,
                    bid_level.price,
                    ask_level.price,
                    bid_level.quantity,
                    ask_level.quantity,
                )
        elif event.event_type == "trade":
            identity = (event.venue, event.symbol, event.trade_id or "", event.exchange_at_ns)
            if identity in state.trade_ids:
                state.count("duplicate_trade")
                return False
            if event.exchange_at_ns < state.last_trade_ns:
                state.count("late_trade_discarded")
                return False
            if event.exchange_at_ns == state.dedup_exhausted_at_ns or (
                len(state.trade_ids) >= self._max_events
                and next(iter(state.trade_ids))[3] == event.exchange_at_ns
            ):
                state.dedup_exhausted_at_ns = event.exchange_at_ns
                state.clear_history()
                state.count("trade_identity_bucket_overflow")
                return False
            state.last_trade_ns = event.exchange_at_ns
            state.trade_ids[identity] = None
            if len(state.trade_ids) > self._max_events:
                state.trade_ids.popitem(last=False)
                state.count("dedup_identity_evictions")
            state.trades.append(event)
            self._observe_trade(state, event)
            state.count("observed_trades")
            if event.taker_side is not None:
                state.count("native_taker_trades")
        state.events.append((event.exchange_at_ns, event.received_at_ns))
        state.count("accepted_events")
        return True

    @staticmethod
    def _difference(
        before: dict[Decimal, Decimal], after: dict[Decimal, Decimal]
    ) -> tuple[Decimal, Decimal]:
        additions = sum(
            (max(ZERO, quantity - before.get(price, ZERO)) for price, quantity in after.items()),
            ZERO,
        )
        removals = sum(
            (max(ZERO, quantity - after.get(price, ZERO)) for price, quantity in before.items()),
            ZERO,
        )
        return additions, removals

    @staticmethod
    def _observe_bbo(
        state: _SymbolState,
        event: MarketEvent,
        bid: Decimal,
        ask: Decimal,
        bid_size: Decimal,
        ask_size: Decimal,
    ) -> None:
        previous = state.bbo[-1] if state.bbo else None
        # Quote and L2 channels interleave normally. Only an advancing BBO contributes to OFI.
        if previous is not None and event.exchange_at_ns < previous.exchange_ns:
            state.count("cross_channel_bbo_interleaving")
            return
        quote = ScalpQuote(
            symbol=event.symbol,
            exchange_at=event.exchange_at,
            exchange_time_ns=event.exchange_at_ns,
            received_at=event.received_at,
            bid=bid,
            ask=ask,
            bid_size=bid_size,
            ask_size=ask_size,
        )
        ofi = ZERO
        if previous is not None:
            old = previous.quote
            ofi = (
                (bid_size if bid >= old.bid else ZERO)
                - (old.bid_size if bid <= old.bid else ZERO)
                - (ask_size if ask <= old.ask else ZERO)
                + (old.ask_size if ask >= old.ask else ZERO)
            )
        state.bbo.append(
            _BBO(event.exchange_at_ns, event.received_at_ns, quote, ofi, event.event_id)
        )
        if not state.history_start_ns:
            state.history_start_ns = event.exchange_at_ns

    @staticmethod
    def _observe_trade(state: _SymbolState, event: MarketEvent) -> None:
        price, quantity = event.trade_price, event.trade_quantity
        if price is None or quantity is None:
            raise _MarketDataError("validated trade lost its price or quantity")
        day = event.exchange_at.astimezone(UTC).date()
        if state.session_day != day:
            state.session_day = day
            state.session_notional = state.session_volume = ZERO
            state.session_count = 0
            state.price_volume.clear()
            state.poc_overflow = False
        updated_volume = state.session_volume + quantity
        updated_notional = state.session_notional + price * quantity
        _float(updated_volume)
        _float(updated_notional / updated_volume)
        state.session_notional = updated_notional
        state.session_volume = updated_volume
        state.session_count += 1
        if not state.poc_overflow:
            if price not in state.price_volume and len(state.price_volume) >= 4096:
                state.poc_overflow = True
                state.price_volume.clear()
                state.count("session_poc_capacity_exceeded")
            else:
                state.price_volume[price] = state.price_volume.get(price, ZERO) + quantity
        minute_ns = event.exchange_at_ns // (60 * NS) * (60 * NS)
        bar = state.minute
        if bar is not None and minute_ns == bar.start_ns:
            bar.high, bar.low, bar.close = max(bar.high, price), min(bar.low, price), price
            return
        if bar is not None:
            consecutive = minute_ns == bar.start_ns + 60 * NS
            if consecutive and state.minute_covered and state.previous_close is not None:
                state.true_ranges.append(
                    max(
                        bar.high - bar.low,
                        abs(bar.high - state.previous_close),
                        abs(bar.low - state.previous_close),
                    )
                )
            elif not consecutive:
                state.true_ranges.clear()
            state.previous_close = bar.close if consecutive else None
            state.minute_covered = consecutive
        state.minute = _Minute(minute_ns, price, price, price)

    def quote(self, symbol: str) -> ScalpQuote | None:
        state = self._state(symbol)
        return state.bbo[-1].quote if state.book.valid and state.bbo else None

    def quote_at_ns(self, symbol: str, now_ns: int) -> ScalpQuote | None:
        state = self._state(symbol)
        quote = self.quote(symbol)
        if quote is None:
            return None
        times = (
            state.book.exchange_ns,
            state.book.received_ns,
            quote.exchange_time_ns,
            state.bbo[-1].received_ns,
        )
        return quote if all(0 <= now_ns - at <= self._stale_ns for at in times) else None

    def levels(
        self, symbol: str, *, side: Literal["bid", "ask"], depth: int = 5
    ) -> tuple[BookLevel, ...]:
        if side not in ("bid", "ask") or depth < 1:
            raise ValueError("positive depth and bid/ask side are required")
        state = self._state(symbol)
        if not state.book.valid:
            return ()
        levels = state.book.bids if side == "bid" else state.book.asks
        return tuple(
            BookLevel(price=price, quantity=levels[price])
            for price in sorted(levels, reverse=side == "bid")[:depth]
        )

    @staticmethod
    def _price_statistics(
        samples: list[_BBO], now_ns: int, seconds: int
    ) -> tuple[float | None, float | None]:
        cutoff = now_ns - seconds * NS
        anchors = [sample for sample in samples if sample.exchange_ns <= cutoff]
        if not anchors:
            return None, None
        anchor = anchors[-1]
        window = [anchor, *(sample for sample in samples if sample.exchange_ns > cutoff)]
        change = _float((samples[-1].mid / anchor.mid - 1) * 10000)
        if len(window) < 2:
            return change, None
        ratios = [_float(right.mid / left.mid) for left, right in pairwise(window)]
        if any(ratio <= 0 for ratio in ratios):
            raise _MarketDataError("return ratio is outside the finite numeric domain")
        returns = [math.log(ratio) for ratio in ratios]
        return change, math.sqrt(sum(value * value for value in returns)) * 10000

    @staticmethod
    def _delta(trades: list[MarketEvent]) -> float | None:
        if not trades or any(trade.taker_side is None for trade in trades):
            return None
        return _float(
            sum(
                (
                    (trade.trade_quantity or ZERO) * (1 if trade.taker_side == "buy" else -1)
                    for trade in trades
                ),
                ZERO,
            )
        )

    def features(self, symbol: str, now: datetime) -> BookFeatures | None:
        state = self._state(symbol)
        now_ns = datetime_ns(now)
        try:
            return self._features(state, symbol, now, now_ns)
        except (_MarketDataError, DecimalException, ValidationError, OverflowError):
            self._invalidate(state, "nonfinite_feature")
            return None

    def _features(
        self, state: _SymbolState, symbol: str, now: datetime, now_ns: int
    ) -> BookFeatures | None:
        quote = self.quote_at_ns(symbol, now_ns)
        if quote is None or state.last_received_ns > now_ns:
            return None
        samples = [
            sample
            for sample in state.bbo
            if sample.exchange_ns <= now_ns and sample.received_ns <= now_ns
        ]
        if not samples:
            return None
        latest = samples[-1]
        five = [sample for sample in samples if now_ns - 5 * NS < sample.exchange_ns <= now_ns]
        one = [sample for sample in five if sample.exchange_ns > now_ns - NS]
        trades = [
            trade
            for trade in state.trades
            if now_ns - 5 * NS < trade.exchange_at_ns <= now_ns and trade.received_at_ns <= now_ns
        ]
        changes = [
            change
            for change in state.changes
            if now_ns - 5 * NS < change.exchange_ns <= now_ns and change.received_ns <= now_ns
        ]
        recent_changes = [change for change in changes if change.exchange_ns > now_ns - NS]
        bids, asks = self.levels(symbol, side="bid"), self.levels(symbol, side="ask")
        bid_depth = sum((level.quantity for level in bids), ZERO)
        ask_depth = sum((level.quantity for level in asks), ZERO)
        mid = latest.mid
        micro = (quote.ask * quote.bid_size + quote.bid * quote.ask_size) / (
            quote.bid_size + quote.ask_size
        )
        ofi_one = sum((sample.ofi for sample in one), ZERO)
        ofi_five = sum((sample.ofi for sample in five), ZERO)
        depth_one = sum((sample.quote.bid_size + sample.quote.ask_size for sample in one), ZERO)
        depth_five = sum((sample.quote.bid_size + sample.quote.ask_size for sample in five), ZERO)
        observed_start = max(state.history_start_ns, samples[0].exchange_ns)
        history = max(0.0, (latest.exchange_ns - observed_start) / NS)
        current_session = state.session_day == now.astimezone(UTC).date()
        volume = state.session_volume if current_session else ZERO
        vwap = state.session_notional / volume if volume else None
        poc = (
            min(state.price_volume, key=lambda price: (-state.price_volume[price], price))
            if state.price_volume and current_session
            else None
        )
        try:
            return_one, volatility_one = self._price_statistics(samples, now_ns, 1)
            return_five, volatility_five = self._price_statistics(samples, now_ns, 5)
            return_fifteen, volatility_fifteen = self._price_statistics(samples, now_ns, 15)
            return BookFeatures(
                symbol=quote.symbol,
                as_of=now,
                as_of_ns=now_ns,
                quote=quote,
                source_event_id=latest.source_id,
                history_seconds=history,
                ready=history >= self.config.feature_horizon_seconds,
                l1_imbalance=_float(
                    (quote.bid_size - quote.ask_size) / (quote.bid_size + quote.ask_size)
                ),
                l5_imbalance=_float((bid_depth - ask_depth) / (bid_depth + ask_depth)),
                ofi_1s=_float(ofi_one),
                ofi_5s=_float(ofi_five),
                normalized_ofi_1s=_float(ofi_one / depth_one) if depth_one else 0.0,
                normalized_ofi_5s=_float(ofi_five / depth_five) if depth_five else 0.0,
                taker_delta_1s=self._delta(
                    [trade for trade in trades if trade.exchange_at_ns > now_ns - NS]
                ),
                taker_delta_5s=self._delta(trades),
                known_taker_fraction_5s=(
                    sum(trade.taker_side is not None for trade in trades) / len(trades)
                    if trades
                    else None
                ),
                microprice=micro,
                microprice_displacement_bps=_float((micro / mid - 1) * 10000),
                spread_bps=_float((quote.ask - quote.bid) / mid * 10000),
                bid_depth_l5=_float(bid_depth),
                ask_depth_l5=_float(ask_depth),
                bid_slope_bps_per_unit=(
                    _float((bids[0].price - bids[-1].price) / mid * 10000 / bid_depth)
                    if len(bids) > 1
                    else None
                ),
                ask_slope_bps_per_unit=(
                    _float((asks[-1].price - asks[0].price) / mid * 10000 / ask_depth)
                    if len(asks) > 1
                    else None
                ),
                bid_additions_1s=_float(sum((change.bid_add for change in recent_changes), ZERO)),
                ask_additions_1s=_float(sum((change.ask_add for change in recent_changes), ZERO)),
                bid_additions_5s=_float(sum((change.bid_add for change in changes), ZERO)),
                ask_additions_5s=_float(sum((change.ask_add for change in changes), ZERO)),
                bid_removals_proxy_5s=_float(sum((change.bid_remove for change in changes), ZERO)),
                ask_removals_proxy_5s=_float(sum((change.ask_remove for change in changes), ZERO)),
                bid_depletion_fraction_5s=_float(
                    sum((change.bid_depletion for change in changes), ZERO)
                ),
                ask_depletion_fraction_5s=_float(
                    sum((change.ask_depletion for change in changes), ZERO)
                ),
                trade_intensity_5s=len(trades) / 5,
                event_intensity_5s=sum(
                    now_ns - 5 * NS < exchange <= now_ns and received <= now_ns
                    for exchange, received in state.events
                )
                / 5,
                trade_volume_5s=_float(
                    sum((trade.trade_quantity or ZERO for trade in trades), ZERO)
                ),
                trade_count_5s=len(trades),
                native_taker_trade_count_5s=sum(trade.taker_side is not None for trade in trades),
                session_vwap=vwap,
                session_poc=poc,
                session_trade_count=state.session_count if current_session else 0,
                session_observed_volume=volume,
                session_vwap_displacement_bps=_float((mid / vwap - 1) * 10000) if vwap else None,
                atr_1m=_float(sum(state.true_ranges, ZERO) / 14)
                if len(state.true_ranges) == 14
                else None,
                book_exchange_at_ns=state.book.exchange_ns,
                book_received_at_ns=state.book.received_ns,
                quote_book_time_difference_ms=abs(quote.exchange_time_ns - state.book.exchange_ns)
                / 1_000_000,
                return_1s_bps=return_one,
                return_5s_bps=return_five,
                return_15s_bps=return_fifteen,
                volatility_1s_bps=volatility_one,
                volatility_5s_bps=volatility_five,
                volatility_15s_bps=volatility_fifteen,
            )
        except (ValueError, OverflowError) as exc:
            raise _MarketDataError("market features exceeded their finite numeric domain") from exc

    def health_snapshot(self) -> dict[str, Any]:
        return {
            "venue": VENUE,
            "data_kind": "aggregate_l2",
            "exchange_sequence_available": False,
            "exact_queue_position_available": False,
            "market_by_order_available": False,
            "market_silence_is_sequence_gap": False,
            "exchange_gap_coverage": "unprovable",
            "local_receive_gaps": self._local_receive_gaps,
            "resnapshot_required": any(
                state.resnapshot_required for state in self._states.values()
            ),
            "resnapshot_generation": self._resnapshot_generation,
            "automatic_resnapshot_callback": self.on_resnapshot is not None,
            "resnapshot_reasons": {
                symbol: state.resnapshot_reason
                for symbol, state in self._states.items()
                if state.resnapshot_required
            },
            "connection_id": str(self._connection_id) if self._connection_id is not None else None,
            "change_measurement": "aggregate additions/removals, not identified cancellations",
            "trade_deduplication": (
                "bounded venue/symbol/trade_id/exchange_ns; older trades rejected"
            ),
            "max_levels_per_side": self._max_levels,
            "max_events_per_window": self._max_events,
            "stale_after_seconds": self._stale_ns / NS,
            "symbols": {
                symbol: {
                    "book_valid": state.book.valid and not state.book.awaiting_current_update,
                    "reconstruction_valid": state.book.valid,
                    "awaiting_current_book_update": state.book.awaiting_current_update,
                    "reason": state.book.reason,
                    "resnapshot_required": state.resnapshot_required,
                    "resnapshot_reason": state.resnapshot_reason,
                    "connection_id": str(state.connection_id) if state.connection_id else None,
                    "receive_sequence": state.last_sequence,
                    "book_exchange_at_ns": state.book.exchange_ns or None,
                    "book_received_at_ns": state.book.received_ns or None,
                    "bid_levels": len(state.book.bids),
                    "ask_levels": len(state.book.asks),
                    "bbo_samples": len(state.bbo),
                    "trade_samples": len(state.trades),
                    "observed_trades": state.counters.get("observed_trades", 0),
                    "native_taker_side_observed": state.counters.get("native_taker_trades", 0) > 0,
                    "change_samples": len(state.changes),
                    "event_samples": len(state.events),
                    "dedup_identities": len(state.trade_ids),
                    "session_poc_available": bool(state.price_volume) and not state.poc_overflow,
                    "counters": dict(state.counters),
                }
                for symbol, state in self._states.items()
            },
        }
