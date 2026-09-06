from __future__ import annotations

import asyncio
import json
from decimal import Decimal

import pytest
from pydantic import SecretStr, ValidationError

from tradeagent.alpaca_stream import (
    AlpacaMarketStream,
    AlpacaStreamSettings,
    MarketQuote,
    MarketTrade,
    StreamProtocolError,
)
from tradeagent.domain import MarketBar


class FakeWebSocket:
    def __init__(self, messages: list[str]) -> None:
        self.messages = messages
        self.sent: list[dict[str, object]] = []

    async def send(self, message: str) -> None:
        self.sent.append(json.loads(message))

    async def recv(self, decode: bool | None = None) -> str:
        if not self.messages:
            raise StopAsyncIteration
        return self.messages.pop(0)


def _settings() -> AlpacaStreamSettings:
    return AlpacaStreamSettings(
        key_id=SecretStr("stream-key"),
        secret_key=SecretStr("stream-secret"),
    )


def test_stream_authenticates_subscribes_and_parses_events() -> None:
    socket = FakeWebSocket(
        [
            json.dumps([{"T": "success", "msg": "connected"}]),
            json.dumps([{"T": "success", "msg": "authenticated"}]),
            json.dumps(
                [
                    {
                        "T": "b",
                        "S": "SPY",
                        "t": "2026-09-04T14:35:00Z",
                        "o": 100,
                        "h": 101,
                        "l": 99,
                        "c": 100.5,
                        "v": 1000,
                    },
                    {
                        "T": "q",
                        "S": "SPY",
                        "t": "2026-09-04T14:35:01Z",
                        "bp": 100.4,
                        "ap": 100.6,
                        "bs": 20,
                        "as": 30,
                        "bx": "P",
                        "ax": "Q",
                    },
                    {
                        "T": "t",
                        "S": "SPY",
                        "t": "2026-09-04T14:35:02Z",
                        "p": 100.5,
                        "s": 4,
                        "x": "V",
                        "i": 42,
                        "c": ["@"],
                        "z": "C",
                    },
                ]
            ),
        ]
    )

    async def collect() -> list[MarketBar | MarketQuote | MarketTrade]:
        return [
            event
            async for event in AlpacaMarketStream(_settings()).stream_connection(socket, ["spy"])
        ]

    events = asyncio.run(collect())

    assert isinstance(events[0], MarketBar)
    assert isinstance(events[1], MarketQuote)
    assert events[1].bid_price == Decimal("100.4")
    assert events[1].bid_exchange == "P"
    assert isinstance(events[2], MarketTrade)
    assert events[2].price == Decimal("100.5")
    assert socket.sent[0] == {
        "action": "auth",
        "key": "stream-key",
        "secret": "stream-secret",
    }
    assert socket.sent[1] == {
        "action": "subscribe",
        "bars": ["SPY"],
        "quotes": ["SPY"],
        "trades": ["SPY"],
    }


def test_stream_rejects_authentication_and_protocol_errors() -> None:
    async def collect(socket: FakeWebSocket) -> list[object]:
        return [
            event
            async for event in AlpacaMarketStream(_settings()).stream_connection(socket, ["SPY"])
        ]

    unauthenticated = FakeWebSocket(
        [
            json.dumps([{"T": "success", "msg": "connected"}]),
            json.dumps([{"T": "error", "code": 401, "msg": "not authenticated"}]),
        ]
    )
    with pytest.raises(StreamProtocolError, match="authentication failed"):
        asyncio.run(collect(unauthenticated))

    stream_error = FakeWebSocket(
        [
            json.dumps([{"T": "success", "msg": "connected"}]),
            json.dumps([{"T": "success", "msg": "authenticated"}]),
            json.dumps([{"T": "error", "code": 405, "msg": "symbol limit"}]),
        ]
    )
    with pytest.raises(StreamProtocolError, match="symbol limit"):
        asyncio.run(collect(stream_error))


def test_stream_validates_symbols_payload_and_quotes() -> None:
    async def collect(socket: FakeWebSocket, symbols: list[str]) -> list[object]:
        return [
            event
            async for event in AlpacaMarketStream(_settings()).stream_connection(socket, symbols)
        ]

    with pytest.raises(ValueError, match="at least one"):
        asyncio.run(collect(FakeWebSocket([]), []))
    invalid_payload = FakeWebSocket(
        [
            json.dumps([{"T": "success", "msg": "connected"}]),
            json.dumps([{"T": "success", "msg": "authenticated"}]),
            json.dumps({"T": "q"}),
        ]
    )
    with pytest.raises(StreamProtocolError, match="array"):
        asyncio.run(collect(invalid_payload, ["SPY"]))
    with pytest.raises(ValidationError, match="crossed quote"):
        MarketQuote(
            symbol="SPY",
            timestamp="2026-09-04T14:35:00Z",
            bid_price=Decimal("101"),
            ask_price=Decimal("100"),
            bid_size=Decimal("1"),
            ask_size=Decimal("1"),
        )


def test_stream_endpoint_cannot_be_changed_to_live_sip() -> None:
    with pytest.raises(ValidationError):
        AlpacaStreamSettings(
            key_id=SecretStr("key"),
            secret_key=SecretStr("secret"),
            data_stream_url="wss://stream.data.alpaca.markets/v2/sip",
        )
