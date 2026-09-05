from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Sequence
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


class MarketQuote(BaseModel):
    model_config = ConfigDict(frozen=True)

    symbol: str
    timestamp: datetime
    bid_price: Decimal = Field(ge=0)
    ask_price: Decimal = Field(ge=0)
    bid_size: Decimal = Field(ge=0)
    ask_size: Decimal = Field(ge=0)

    @model_validator(mode="after")
    def validate_quote(self) -> MarketQuote:
        if self.bid_price > 0 and self.ask_price > 0 and self.bid_price > self.ask_price:
            raise ValueError("crossed quote is invalid")
        return self


class StreamProtocolError(RuntimeError):
    pass


class WebSocketConnection(Protocol):
    async def send(self, message: str) -> None: ...

    async def recv(self, decode: bool | None = None) -> str | bytes: ...


StreamEvent = MarketBar | MarketQuote


class AlpacaMarketStream:
    def __init__(self, settings: AlpacaStreamSettings) -> None:
        self._settings = settings

    async def events(self, symbols: Sequence[str]) -> AsyncIterator[StreamEvent]:
        delay = self._settings.reconnect_initial_seconds
        while True:
            try:
                async with connect(
                    self._settings.data_stream_url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=5,
                ) as websocket:
                    async for event in self.stream_connection(websocket, symbols):
                        delay = self._settings.reconnect_initial_seconds
                        yield event
            except (ConnectionClosed, OSError):
                await asyncio.sleep(delay)
                delay = min(delay * 2, self._settings.reconnect_max_seconds)

    async def stream_connection(
        self,
        websocket: WebSocketConnection,
        symbols: Sequence[str],
    ) -> AsyncIterator[StreamEvent]:
        normalized = tuple(sorted({symbol.strip().upper() for symbol in symbols}))
        if not normalized:
            raise ValueError("at least one stream symbol is required")
        await websocket.send(
            json.dumps(
                {
                    "action": "auth",
                    "key": self._settings.key_id.get_secret_value(),
                    "secret": self._settings.secret_key.get_secret_value(),
                }
            )
        )
        authentication = self._decode(await websocket.recv())
        if not any(
            message.get("T") == "success" and message.get("msg") == "authenticated"
            for message in authentication
        ):
            raise StreamProtocolError("Alpaca stream authentication failed")
        await websocket.send(
            json.dumps(
                {
                    "action": "subscribe",
                    "bars": normalized,
                    "quotes": normalized,
                }
            )
        )

        while True:
            try:
                payload = await websocket.recv()
            except StopAsyncIteration:
                return
            for message in self._decode(payload):
                event_type = message.get("T")
                if event_type == "error":
                    raise StreamProtocolError(
                        f"Alpaca stream error {message.get('code')}: {message.get('msg')}"
                    )
                if event_type == "b":
                    yield self._bar(message)
                elif event_type == "q":
                    yield self._quote(message)

    @staticmethod
    def _decode(payload: str | bytes) -> list[dict[str, object]]:
        decoded = json.loads(payload)
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
        )
