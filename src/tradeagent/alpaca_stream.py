from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from tradeagent.domain import MarketBar


class AlpacaStreamSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="ALPACA_",
        env_file=".env",
        env_file_encoding="utf-8",
        frozen=True,
        extra="ignore",
    )

    key_id: SecretStr
    secret_key: SecretStr
    data_stream_url: Literal["wss://stream.data.alpaca.markets/v2/iex"] = (
        "wss://stream.data.alpaca.markets/v2/iex"
    )
    reconnect_initial_seconds: float = Field(default=1, gt=0)
    reconnect_max_seconds: float = Field(default=30, gt=0)
    handshake_timeout_seconds: float = Field(default=10, gt=0)


class MarketQuote(BaseModel):
    model_config = ConfigDict(frozen=True)

    symbol: str
    timestamp: datetime
    bid_price: Decimal = Field(ge=0)
    ask_price: Decimal = Field(ge=0)
    bid_size: Decimal = Field(ge=0)
    ask_size: Decimal = Field(ge=0)
    bid_exchange: str = ""
    ask_exchange: str = ""
    feed_source: Literal["iex"] = "iex"

    @model_validator(mode="after")
    def validate_quote(self) -> MarketQuote:
        if self.bid_price > 0 and self.ask_price > 0 and self.bid_price > self.ask_price:
            raise ValueError("crossed quote is invalid")
        return self


class MarketTrade(BaseModel):
    model_config = ConfigDict(frozen=True)

    symbol: str
    timestamp: datetime
    price: Decimal = Field(gt=0)
    size: Decimal = Field(gt=0)
    exchange: str = ""
    trade_id: int | str
    conditions: tuple[str, ...] = ()
    tape: str | None = None
    feed_source: Literal["iex"] = "iex"


class StreamProtocolError(RuntimeError):
    pass


class StreamProviderError(StreamProtocolError):
    def __init__(self, code: int, *, stage: str) -> None:
        self.code = code
        self.retryable = code in {404, 406, 407, 500, 503}
        # Never echo arbitrary provider messages: they can contain authentication input.
        description = {
            400: "invalid syntax",
            401: "not authenticated",
            402: "authentication failed",
            403: "already authenticated",
            404: "authentication timeout",
            405: "symbol limit exceeded",
            406: "connection limit exceeded",
            407: "slow client/backpressure",
            408: "stream version not enabled",
            409: "insufficient subscription/entitlement",
            410: "invalid subscription action",
            500: "provider internal error",
            503: "provider unavailable",
        }.get(code, "unrecognized provider error")
        super().__init__(f"Alpaca {stage} error {code}: {description}")


class WebSocketConnection(Protocol):
    async def send(self, message: str) -> None: ...

    async def recv(self, decode: bool | None = None) -> str | bytes: ...


StreamEvent = MarketBar | MarketQuote | MarketTrade


@dataclass(frozen=True)
class ReceivedStreamEvent:
    event: StreamEvent
    received_at: datetime


class AlpacaMarketStream:
    def __init__(
        self,
        settings: AlpacaStreamSettings,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._settings = settings
        self._clock = clock
        self.on_status: Callable[[dict[str, object]], None] | None = None
        self._health: dict[str, object] = {
            "state": "disconnected",
            "authenticated": False,
            "subscribed": False,
            "reconnects": 0,
        }

    def health(self) -> dict[str, object]:
        return dict(self._health)

    def _status(self, state: str, **details: object) -> None:
        self._health.update(state=state, observed_at=self._clock().isoformat(), **details)
        logging.getLogger(__name__).info("Alpaca stream status %s", self._health)
        if self.on_status is not None:
            self.on_status(self.health())

    async def events(self, symbols: Sequence[str]) -> AsyncIterator[StreamEvent]:
        async for receipt in self.received_events(symbols):
            yield receipt.event

    async def received_events(self, symbols: Sequence[str]) -> AsyncIterator[ReceivedStreamEvent]:
        normalized = self._symbols(symbols)
        delay = min(self._settings.reconnect_initial_seconds, self._settings.reconnect_max_seconds)
        reconnects = 0
        while True:
            started = asyncio.get_running_loop().time()
            try:
                self._status(
                    "connecting", authenticated=False, subscribed=False, error=None, code=None
                )
                async with connect(
                    self._settings.data_stream_url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=5,
                    max_queue=16,
                ) as websocket:
                    async for receipt in self._received_connection(websocket, normalized):
                        yield receipt
                raise OSError("stream ended")
            except (ConnectionClosed, OSError, TimeoutError, StreamProviderError) as exc:
                if isinstance(exc, StreamProviderError) and not exc.retryable:
                    self._status(
                        "failed",
                        authenticated=False,
                        subscribed=False,
                        error=str(exc),
                        code=exc.code,
                        retryable=False,
                        gap=True,
                    )
                    raise
                if asyncio.get_running_loop().time() - started >= 60:
                    delay = min(
                        self._settings.reconnect_initial_seconds,
                        self._settings.reconnect_max_seconds,
                    )
                reconnects += 1
                self._status(
                    "reconnecting",
                    authenticated=False,
                    subscribed=False,
                    reconnects=reconnects,
                    gap=True,
                    retry_in_seconds=delay,
                    error=str(exc) if isinstance(exc, StreamProviderError) else type(exc).__name__,
                    code=exc.code if isinstance(exc, StreamProviderError) else None,
                    retryable=True,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, self._settings.reconnect_max_seconds)
            except StreamProtocolError as exc:
                self._status(
                    "failed",
                    authenticated=False,
                    subscribed=False,
                    error=str(exc),
                    retryable=False,
                    gap=True,
                )
                raise
            except (asyncio.CancelledError, GeneratorExit):
                self._status("stopped", authenticated=False, subscribed=False)
                raise

    async def stream_connection(
        self,
        websocket: WebSocketConnection,
        symbols: Sequence[str],
    ) -> AsyncIterator[StreamEvent]:
        async for receipt in self._received_connection(websocket, self._symbols(symbols)):
            yield receipt.event

    @staticmethod
    def _symbols(symbols: Sequence[str]) -> tuple[str, ...]:
        normalized = tuple(sorted({symbol.strip().upper() for symbol in symbols if symbol.strip()}))
        if not normalized:
            raise ValueError("at least one stream symbol is required")
        return normalized

    async def _handshake(
        self, websocket: WebSocketConnection, stage: str
    ) -> list[dict[str, object]]:
        try:
            payload = await asyncio.wait_for(
                websocket.recv(), timeout=self._settings.handshake_timeout_seconds
            )
        except StopAsyncIteration as exc:
            raise StreamProtocolError(f"Alpaca {stage} acknowledgement missing") from exc
        messages = self._decode(payload)
        self._check_errors(messages, stage=stage)
        return messages

    @staticmethod
    def _check_errors(messages: list[dict[str, object]], *, stage: str) -> None:
        for message in messages:
            if message.get("T") == "error":
                try:
                    code = int(str(message.get("code")))
                except ValueError as exc:
                    raise StreamProtocolError("Alpaca error has no valid provider code") from exc
                raise StreamProviderError(code, stage=stage)

    async def _received_connection(
        self, websocket: WebSocketConnection, normalized: tuple[str, ...]
    ) -> AsyncIterator[ReceivedStreamEvent]:
        self._status("connecting", authenticated=False, subscribed=False)
        connection = await self._handshake(websocket, "connection")
        if not any(
            message.get("T") == "success" and message.get("msg") == "connected"
            for message in connection
        ):
            raise StreamProtocolError("Alpaca stream connection banner was not received")
        self._status("connected")
        await websocket.send(
            json.dumps(
                {
                    "action": "auth",
                    "key": self._settings.key_id.get_secret_value(),
                    "secret": self._settings.secret_key.get_secret_value(),
                }
            )
        )
        authentication = await self._handshake(websocket, "authentication")
        if not any(
            message.get("T") == "success" and message.get("msg") == "authenticated"
            for message in authentication
        ):
            raise StreamProtocolError("Alpaca stream authentication failed")
        self._status("authenticated", authenticated=True)
        await websocket.send(
            json.dumps(
                {
                    "action": "subscribe",
                    "bars": normalized,
                    "quotes": normalized,
                    "trades": normalized,
                }
            )
        )
        subscription = await self._handshake(websocket, "subscription")
        received_at = self._clock()
        self._validate_subscription(subscription, normalized)
        self._status("subscribed", subscribed=True, gap=False, retry_in_seconds=None)
        for receipt in self._parse_events(subscription, received_at):
            yield receipt
        while True:
            try:
                payload = await websocket.recv()
            except StopAsyncIteration:
                return
            received_at = self._clock()
            messages = self._decode(payload)
            self._check_errors(messages, stage="stream")
            if any(message.get("T") == "subscription" for message in messages):
                self._validate_subscription(messages, normalized)
            for receipt in self._parse_events(messages, received_at):
                self._health.update(
                    last_received_at=received_at.isoformat(),
                    last_event_at=receipt.event.timestamp.isoformat(),
                )
                yield receipt

    @staticmethod
    def _validate_subscription(
        messages: list[dict[str, object]], normalized: tuple[str, ...]
    ) -> None:
        for message in messages:
            if message.get("T") != "subscription":
                continue
            for channel in ("bars", "quotes", "trades"):
                values = message.get(channel)
                if (
                    not isinstance(values, list)
                    or not all(isinstance(value, str) for value in values)
                    or not set(normalized).issubset(values)
                ):
                    raise StreamProtocolError(f"Alpaca subscription missing requested {channel}")
            return
        raise StreamProtocolError("Alpaca subscription acknowledgement missing")

    @classmethod
    def _parse_events(
        cls, messages: list[dict[str, object]], received_at: datetime
    ) -> list[ReceivedStreamEvent]:
        result: list[ReceivedStreamEvent] = []
        parsers: dict[str, Callable[[dict[str, object]], StreamEvent]] = {
            "b": cls._bar,
            "q": cls._quote,
            "t": cls._trade,
        }
        for message in messages:
            parser = parsers.get(str(message.get("T")))
            if parser is not None:
                try:
                    result.append(ReceivedStreamEvent(parser(message), received_at))
                except (KeyError, ValueError, ArithmeticError) as exc:
                    raise StreamProtocolError("Alpaca market event failed validation") from exc
        return result

    @staticmethod
    def _decode(payload: str | bytes) -> list[dict[str, object]]:
        try:
            decoded = json.loads(payload)
        except (ValueError, UnicodeDecodeError) as exc:
            raise StreamProtocolError("Alpaca stream payload is not valid JSON") from exc
        if not isinstance(decoded, list) or not all(isinstance(item, dict) for item in decoded):
            raise StreamProtocolError("Alpaca stream payload must be an array of objects")
        return decoded

    @staticmethod
    def _timestamp(value: object) -> datetime:
        timestamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise StreamProtocolError("Alpaca stream timestamp must be timezone-aware")
        return timestamp.astimezone(UTC)

    @classmethod
    def _bar(cls, message: dict[str, object]) -> MarketBar:
        return MarketBar(
            symbol=str(message["S"]),
            timestamp=cls._timestamp(message["t"]),
            open=Decimal(str(message["o"])),
            high=Decimal(str(message["h"])),
            low=Decimal(str(message["l"])),
            close=Decimal(str(message["c"])),
            volume=Decimal(str(message["v"])),
        )

    @classmethod
    def _quote(cls, message: dict[str, object]) -> MarketQuote:
        return MarketQuote(
            symbol=str(message["S"]).upper(),
            timestamp=cls._timestamp(message["t"]),
            bid_price=Decimal(str(message["bp"])),
            ask_price=Decimal(str(message["ap"])),
            bid_size=Decimal(str(message["bs"])),
            ask_size=Decimal(str(message["as"])),
            bid_exchange=str(message.get("bx", "")),
            ask_exchange=str(message.get("ax", "")),
        )

    @classmethod
    def _trade(cls, message: dict[str, object]) -> MarketTrade:
        raw_conditions = message.get("c", [])
        if not isinstance(raw_conditions, list):
            raise StreamProtocolError("Alpaca trade conditions must be an array")
        return MarketTrade(
            symbol=str(message["S"]).upper(),
            timestamp=cls._timestamp(message["t"]),
            price=Decimal(str(message["p"])),
            size=Decimal(str(message["s"])),
            exchange=str(message.get("x", "")),
            trade_id=str(message["i"]),
            conditions=tuple(str(value) for value in raw_conditions),
            tape=str(message["z"]) if message.get("z") is not None else None,
        )
