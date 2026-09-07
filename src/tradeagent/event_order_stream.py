"""Read-only PAPER protocol: https://docs.alpaca.markets/us/docs/websocket-streaming."""

from __future__ import annotations

import asyncio
import json
import logging
import math
from collections import deque
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal
from threading import Lock
from typing import Annotated, Literal, Protocol

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, ValidationError, model_validator
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake

from tradeagent.alpaca_paper import AlpacaOrderStatus, AlpacaPaperSettings

PAPER_TRADE_UPDATES_URL = "wss://paper-api.alpaca.markets/stream"

# websockets DEBUG logs include outbound frames, including the authentication secrets.
_TRANSPORT_LOGGER = logging.Logger(f"{__name__}.transport")
_TRANSPORT_LOGGER.disabled = True
_TRANSPORT_LOGGER.propagate = False


def _broker_timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("broker timestamp must be an ISO-8601 string")
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("broker timestamp must be timezone-aware")
    return timestamp.astimezone(UTC)


BrokerTimestamp = Annotated[datetime, BeforeValidator(_broker_timestamp)]
Identifier = Annotated[str, Field(min_length=1, max_length=128, pattern=r"^\S+$")]
NonnegativeDecimal = Annotated[Decimal, Field(ge=0, allow_inf_nan=False)]
PositiveDecimal = Annotated[Decimal, Field(gt=0, allow_inf_nan=False)]


class _WireOrder(BaseModel):
    model_config = ConfigDict(frozen=True, extra="ignore", hide_input_in_errors=True)

    id: Identifier
    client_order_id: Identifier
    symbol: Annotated[str, Field(min_length=1, max_length=64, pattern=r"^\S+$")]
    side: Literal["buy", "sell"]
    status: AlpacaOrderStatus
    qty: PositiveDecimal | None
    notional: PositiveDecimal | None = None
    filled_qty: NonnegativeDecimal
    filled_avg_price: PositiveDecimal | None
    created_at: BrokerTimestamp
    updated_at: BrokerTimestamp | None = None

    @model_validator(mode="after")
    def validate_fill_price(self) -> _WireOrder:
        if self.filled_qty > 0 and self.filled_avg_price is None:
            raise ValueError("filled order requires average fill price")
        return self


class _WireUpdate(BaseModel):
    model_config = ConfigDict(frozen=True, extra="ignore", hide_input_in_errors=True)

    event: Annotated[str, Field(min_length=1, max_length=64, pattern=r"^[a-z_]+$")]
    order: _WireOrder
    timestamp: BrokerTimestamp | None = None
    execution_id: Identifier | None = None
    event_id: Identifier | None = None
    qty: PositiveDecimal | None = None
    price: PositiveDecimal | None = None
    position_qty: Annotated[Decimal | None, Field(allow_inf_nan=False)] = None

    @model_validator(mode="after")
    def validate_event_fields(self) -> _WireUpdate:
        if self.event in {"fill", "partial_fill"} and (
            self.qty is None or self.price is None or self.position_qty is None
        ):
            raise ValueError("fill event requires quantity, price and position quantity")
        if (
            self.event in {"fill", "partial_fill", "canceled", "expired", "replaced", "rejected"}
            and self.timestamp is None
        ):
            raise ValueError("event requires broker timestamp")
        return self


class PaperTradeUpdate(BaseModel):
    """A reconciliation hint, never authority to change owned/risk quantities.

    Arrival sequence is local to this adapter, not a broker sequence. Duplicate and
    out-of-order messages are deliberately retained. ``timestamp`` is absent when
    the broker supplies no event time; ``received_at`` is always the local UTC time.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    event: str
    timestamp: datetime | None
    received_at: datetime
    sequence: int
    connection_id: int
    order_id: str
    client_order_id: str
    symbol: str
    side: Literal["buy", "sell"]
    status: AlpacaOrderStatus
    quantity: Decimal | None
    notional: Decimal | None
    filled_quantity: Decimal
    filled_average_price: Decimal | None
    order_created_at: datetime
    order_updated_at: datetime | None
    execution_id: str | None
    event_id: str | None
    fill_quantity: Decimal | None
    fill_price: Decimal | None
    position_quantity: Decimal | None


@dataclass
class _Health:
    running: bool = False
    connected: bool = False
    authenticated: bool = False
    subscribed: bool = False
    state: str = "stopped"
    connection_attempts: int = 0
    connections: int = 0
    reconnects: int = 0
    disconnects: int = 0
    messages_received: int = 0
    received_updates: int = 0
    dropped_updates: int = 0
    invalid_messages: int = 0
    error_count: int = 0
    gap_count: int = 0
    retry_delay_seconds: float = 0
    started_at: datetime | None = None
    stopped_at: datetime | None = None
    connected_at: datetime | None = None
    authenticated_at: datetime | None = None
    subscribed_at: datetime | None = None
    last_message_at: datetime | None = None
    last_update_at: datetime | None = None
    last_error_at: datetime | None = None
    last_error: str | None = None
    last_gap_at: datetime | None = None
    last_gap_reason: str | None = None
    gap_started_at: datetime | None = None


class _StreamError(Exception):
    def __init__(self, code: str, *, terminal: bool = False, invalid: bool = False) -> None:
        super().__init__(code)
        self.code = code
        self.terminal = terminal
        self.invalid = invalid


class _WebSocket(Protocol):
    async def send(self, message: str) -> None: ...

    async def recv(self, decode: bool | None = None) -> str | bytes: ...


class _PaperConnect(connect):
    def process_redirect(self, exc: Exception) -> Exception:
        return exc


class AlpacaPaperTradeUpdatesStream:
    """Read-only PAPER order stream with a bounded, single-consumer handoff.

    Start ``run(asyncio.Event())`` on the service loop, then call ``drain()`` and
    ``health_snapshot()`` from the runtime owner thread. The adapter never calls
    REST or mutates OMS state; the caller must reconcile orders AND positions via
    REST, including at startup, after gaps, and independently of stream health.

    Authentication/subscription rejection and unexpected internal failures stop
    the reader with health ``state="failed"``. Transport/protocol failures retry
    with capped exponential backoff. A quiet subscribed account is not considered
    a failed feed; websocket ping/pong detects transport loss. Gap/error counters
    are historical and do not clear on reconnect or imply REST is unavailable.

    Stop/cancellation interrupts connect, handshake, receive or backoff, closes
    the socket (production close timeout: 5s), and keeps queued evidence drainable.
    Cancellation propagates; an ordinary stop or terminal failure returns None.
    The queue drops the oldest update on overflow and records explicit loss.
    """

    def __init__(
        self,
        settings: AlpacaPaperSettings,
        *,
        queue_capacity: int = 1024,
        reconnect_initial_seconds: float = 1.0,
        reconnect_max_seconds: float = 30.0,
        handshake_timeout_seconds: float = 10.0,
    ) -> None:
        if settings.paper_url != "https://paper-api.alpaca.markets":
            raise ValueError("only the Alpaca PAPER host is permitted")
        if not settings.key_id.get_secret_value().strip() or not (
            settings.secret_key.get_secret_value().strip()
        ):
            raise ValueError("PAPER stream credentials must be nonempty")
        if isinstance(queue_capacity, bool) or not isinstance(queue_capacity, int):
            raise ValueError("queue_capacity must be a positive integer")
        if queue_capacity < 1:
            raise ValueError("queue_capacity must be a positive integer")
        for value in (reconnect_initial_seconds, reconnect_max_seconds, handshake_timeout_seconds):
            if not math.isfinite(value) or value <= 0:
                raise ValueError("stream timeouts and backoff must be finite and positive")
        if reconnect_max_seconds < reconnect_initial_seconds:
            raise ValueError("maximum backoff must be at least the initial backoff")
        self._settings = settings
        self._initial_delay = reconnect_initial_seconds
        self._max_delay = reconnect_max_seconds
        self._handshake_timeout = handshake_timeout_seconds
        self._queue: deque[PaperTradeUpdate] = deque(maxlen=queue_capacity)
        self._lock = Lock()
        self._health = _Health()

    def drain(self) -> tuple[PaperTradeUpdate, ...]:
        """Atomically remove the current FIFO batch; safe from asyncio.to_thread."""
        with self._lock:
            updates = tuple(self._queue)
            self._queue.clear()
            return updates

    def health_snapshot(self) -> dict[str, object]:
        """Return JSON-safe evidence without credentials, payloads or exception text."""
        with self._lock:
            snapshot = {
                name: value.isoformat() if isinstance(value, datetime) else value
                for name, value in asdict(self._health).items()
            }
            snapshot.update(
                endpoint=PAPER_TRADE_UPDATES_URL,
                queue_depth=len(self._queue),
                queue_capacity=self._queue.maxlen,
                has_gaps=self._health.gap_count > 0,
                observed_at=datetime.now(UTC).isoformat(),
            )
            return snapshot

    async def run(self, stop_event: asyncio.Event) -> None:
        with self._lock:
            if self._health.running:
                raise RuntimeError("PAPER trade updates reader is already running")
            self._health.running = True
            self._health.started_at = datetime.now(UTC)
            self._health.stopped_at = None
            self._health.state = "connecting"
            if self._health.connections:
                self._record_gap("reader_restarted")
            if self._health.gap_started_at is None:
                self._health.gap_started_at = self._health.started_at
        reader: asyncio.Task[None] | None = None
        stopper: asyncio.Task[bool] | None = None
        try:
            if stop_event.is_set():
                return
            reader = asyncio.create_task(self._read_forever())
            stopper = asyncio.create_task(stop_event.wait())
            await asyncio.wait((reader, stopper), return_when=asyncio.FIRST_COMPLETED)
            if reader.done():
                await reader
        finally:
            tasks = [task for task in (reader, stopper) if task is not None]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            with self._lock:
                self._health.running = False
                self._clear_connection()
                self._health.retry_delay_seconds = 0
                self._health.stopped_at = datetime.now(UTC)
                if self._health.gap_started_at is None:
                    self._health.gap_started_at = self._health.stopped_at
                if self._health.state != "failed":
                    self._health.state = "stopped"

    async def _read_forever(self) -> None:
        delay = self._initial_delay
        while True:
            subscribed_since: float | None = None
            was_connected = False
            with self._lock:
                self._health.connection_attempts += 1
                self._health.reconnects = self._health.connection_attempts - 1
                self._health.state = "connecting"
                self._health.retry_delay_seconds = 0
            failure: _StreamError
            try:
                # Disable proxies and redirects: credentials may go only to the fixed PAPER host.
                async with _PaperConnect(
                    PAPER_TRADE_UPDATES_URL,
                    additional_headers={"Content-Type": "application/json"},
                    proxy=None,
                    open_timeout=self._handshake_timeout,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=5,
                    max_size=262144,
                    max_queue=16,
                    logger=_TRANSPORT_LOGGER,
                ) as websocket:
                    was_connected = True
                    with self._lock:
                        self._health.connections += 1
                        self._health.connected = True
                        self._health.connected_at = datetime.now(UTC)
                        self._health.state = "authenticating"
                    try:
                        await self._handshake(websocket)
                        subscribed_since = asyncio.get_running_loop().time()
                        while True:
                            message, received_at = await self._receive(websocket)
                            if message.get("stream") == "listening":
                                self._check_subscription(message)
                                continue
                            if message.get("stream") != "trade_updates":
                                raise _StreamError("unexpected_stream", invalid=True)
                            try:
                                update = _WireUpdate.model_validate(message["data"])
                            except ValidationError:
                                raise _StreamError("invalid_trade_update", invalid=True) from None
                            self._enqueue(update, received_at)
                    finally:
                        with self._lock:
                            self._clear_connection()
            except _StreamError as error:
                failure = error
            except (ConnectionClosed, OSError, TimeoutError, InvalidHandshake):
                failure = _StreamError("transport_lost")
            except Exception:
                failure = _StreamError("reader_failed", terminal=True)
            finally:
                with self._lock:
                    self._clear_connection()
            with self._lock:
                self._health.disconnects += int(was_connected)
                self._health.invalid_messages += int(failure.invalid)
                self._record_gap(failure.code)
                self._health.state = "failed" if failure.terminal else "backoff"
                if self._health.gap_started_at is None:
                    self._health.gap_started_at = datetime.now(UTC)
            if failure.terminal:
                return
            # A connect/auth/close loop must not reset backoff on every successful auth.
            if (
                subscribed_since is not None
                and asyncio.get_running_loop().time() - subscribed_since >= 60
            ):
                delay = self._initial_delay
            with self._lock:
                self._health.retry_delay_seconds = delay
            await self._retry_pause(delay)
            delay = min(delay * 2, self._max_delay)

    @staticmethod
    async def _retry_pause(delay: float) -> None:
        await asyncio.sleep(delay)

    async def _handshake(self, websocket: _WebSocket) -> None:
        try:
            async with asyncio.timeout(self._handshake_timeout):
                await websocket.send(
                    json.dumps(
                        {
                            "action": "auth",
                            "key": self._settings.key_id.get_secret_value(),
                            "secret": self._settings.secret_key.get_secret_value(),
                        }
                    )
                )
                message, _ = await self._receive(websocket)
                data = message.get("data")
                if (
                    message.get("stream") != "authorization"
                    or not isinstance(data, dict)
                    or data.get("status") != "authorized"
                    or data.get("action") != "authenticate"
                ):
                    raise _StreamError("invalid_authorization", invalid=True)
                with self._lock:
                    self._health.authenticated = True
                    self._health.authenticated_at = datetime.now(UTC)
                    self._health.state = "subscribing"
                await websocket.send(
                    json.dumps({"action": "listen", "data": {"streams": ["trade_updates"]}})
                )
                message, _ = await self._receive(websocket)
                self._check_subscription(message)
                with self._lock:
                    self._health.subscribed = True
                    self._health.subscribed_at = datetime.now(UTC)
                    self._health.gap_started_at = None
                    self._health.state = "subscribed"
        except TimeoutError:
            raise _StreamError("handshake_timeout") from None

    @staticmethod
    def _check_subscription(message: dict[str, object]) -> None:
        data = message.get("data")
        if (
            message.get("stream") != "listening"
            or not isinstance(data, dict)
            or data.get("streams") != ["trade_updates"]
        ):
            raise _StreamError("subscription_rejected", terminal=True, invalid=True)

    async def _receive(self, websocket: _WebSocket) -> tuple[dict[str, object], datetime]:
        payload = await websocket.recv()
        received_at = datetime.now(UTC)
        with self._lock:
            self._health.messages_received += 1
            self._health.last_message_at = received_at
        try:
            decoded = json.loads(
                payload.decode("utf-8") if isinstance(payload, bytes) else payload,
                parse_float=Decimal,
            )
        except (ValueError, UnicodeError):
            raise _StreamError("invalid_json", invalid=True) from None
        if not isinstance(decoded, dict) or not isinstance(decoded.get("data"), dict):
            raise _StreamError("invalid_envelope", invalid=True)
        if decoded.get("action") == "error":
            raise _StreamError("server_error")
        if decoded.get("stream") == "authorization" and (
            decoded["data"].get("status") == "unauthorized"
        ):
            raise _StreamError("authentication_rejected", terminal=True)
        if not isinstance(decoded.get("stream"), str):
            raise _StreamError("invalid_envelope", invalid=True)
        return decoded, received_at

    def _enqueue(self, data: _WireUpdate, received_at: datetime) -> None:
        order = data.order
        with self._lock:
            self._health.received_updates += 1
            update = PaperTradeUpdate(
                event=data.event,
                timestamp=data.timestamp,
                received_at=received_at,
                sequence=self._health.received_updates,
                connection_id=self._health.connections,
                order_id=order.id,
                client_order_id=order.client_order_id,
                symbol=order.symbol,
                side=order.side,
                status=order.status,
                quantity=order.qty,
                notional=order.notional,
                filled_quantity=order.filled_qty,
                filled_average_price=order.filled_avg_price,
                order_created_at=order.created_at,
                order_updated_at=order.updated_at,
                execution_id=data.execution_id,
                event_id=data.event_id,
                fill_quantity=data.qty,
                fill_price=data.price,
                position_quantity=data.position_qty,
            )
            if len(self._queue) == self._queue.maxlen:
                self._health.dropped_updates += 1
                self._record_gap("queue_overflow")
            self._queue.append(update)
            self._health.last_update_at = received_at

    def _record_gap(self, code: str) -> None:
        now = datetime.now(UTC)
        self._health.error_count += 1
        self._health.gap_count += 1
        self._health.last_error_at = now
        self._health.last_gap_at = now
        self._health.last_error = code
        self._health.last_gap_reason = code

    def _clear_connection(self) -> None:
        self._health.connected = False
        self._health.authenticated = False
        self._health.subscribed = False
