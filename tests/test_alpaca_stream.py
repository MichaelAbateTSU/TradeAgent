from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import SecretStr, ValidationError

from tradeagent.alpaca_stream import (
    AlpacaMarketStream,
    AlpacaStreamSettings,
    MarketQuote,
    MarketTrade,
    StreamProtocolError,
    StreamProviderError,
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


def _subscription(symbols: tuple[str, ...] = ("SPY",)) -> str:
    return json.dumps(
        [{"T": "subscription", **{channel: symbols for channel in ("bars", "quotes", "trades")}}]
    )


def test_stream_authenticates_subscribes_and_parses_events() -> None:
    socket = FakeWebSocket(
        [
            json.dumps([{"T": "success", "msg": "connected"}]),
            json.dumps([{"T": "success", "msg": "authenticated"}]),
            _subscription(),
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
    with pytest.raises(StreamProtocolError, match="authentication error 401: not authenticated"):
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


@pytest.mark.parametrize(
    ("code", "retryable", "description"),
    [
        (406, True, "connection limit"),
        (407, True, "slow client/backpressure"),
        (404, True, "authentication timeout"),
        (402, False, "authentication failed"),
        (409, False, "entitlement"),
    ],
)
def test_real_shaped_provider_errors_preserve_safe_code(code, retryable, description) -> None:
    socket = FakeWebSocket(
        [
            json.dumps([{"T": "success", "msg": "connected"}]),
            json.dumps([{"T": "error", "code": code, "msg": "echo stream-secret stream-key"}]),
        ]
    )
    stream = AlpacaMarketStream(_settings())

    async def collect():
        return [event async for event in stream.stream_connection(socket, ["SPY"])]

    with pytest.raises(StreamProviderError, match=description) as failure:
        asyncio.run(collect())
    assert failure.value.code == code
    assert failure.value.retryable is retryable
    assert "stream-secret" not in str(failure.value)
    assert "stream-key" not in str(failure.value)
    assert stream.health()["authenticated"] is False
    assert stream.health()["subscribed"] is False


@pytest.mark.parametrize(
    "ack",
    [
        json.dumps([{"T": "q", "S": "SPY"}]),
        json.dumps([{"T": "subscription", "bars": ["SPY"], "quotes": [], "trades": ["SPY"]}]),
    ],
)
def test_stream_requires_complete_subscription_ack_before_market_data(ack) -> None:
    socket = FakeWebSocket(
        [
            json.dumps([{"T": "success", "msg": "connected"}]),
            json.dumps([{"T": "success", "msg": "authenticated"}]),
            ack,
        ]
    )
    stream = AlpacaMarketStream(_settings())

    async def collect():
        return [event async for event in stream.stream_connection(socket, ["SPY"])]

    with pytest.raises(StreamProtocolError, match="subscription"):
        asyncio.run(collect())
    assert stream.health()["subscribed"] is False


def test_connection_limit_then_disconnect_reconnects_and_stamps_at_receipt(monkeypatch) -> None:
    exchange_at = datetime(2026, 9, 8, 14, 30, tzinfo=UTC)
    received_at = datetime(2026, 9, 8, 14, 30, 11, tzinfo=UTC)
    quote = json.dumps(
        [
            {
                "T": "q",
                "S": "SPY",
                "t": exchange_at.isoformat(),
                "bp": 100,
                "ap": 101,
                "bs": 10,
                "as": 20,
            }
        ]
    )
    sockets = [
        FakeWebSocket([json.dumps([{"T": "error", "code": 406, "msg": "connection limit"}])]),
        FakeWebSocket(
            [
                json.dumps([{"T": "success", "msg": "connected"}]),
                json.dumps([{"T": "success", "msg": "authenticated"}]),
                _subscription(),
                quote,
            ]
        ),
        FakeWebSocket(
            [
                json.dumps([{"T": "success", "msg": "connected"}]),
                json.dumps([{"T": "success", "msg": "authenticated"}]),
                _subscription(),
                quote,
            ]
        ),
    ]
    opens = []

    class Connection:
        async def __aenter__(self):
            socket = sockets.pop(0)
            opens.append(socket)
            return socket

        async def __aexit__(self, *args):
            return False

    monkeypatch.setattr("tradeagent.alpaca_stream.connect", lambda *args, **kwargs: Connection())
    stream = AlpacaMarketStream(
        _settings().model_copy(
            update={
                "reconnect_initial_seconds": 0.001,
                "reconnect_max_seconds": 0.002,
            }
        ),
        clock=lambda: received_at,
    )
    statuses = []
    stream.on_status = statuses.append

    async def collect():
        iterator = stream.received_events(["SPY"])
        try:
            return [await anext(iterator), await anext(iterator)]
        finally:
            await iterator.aclose()

    receipts = asyncio.run(collect())
    assert len(opens) == 3
    assert all(receipt.event.timestamp == exchange_at for receipt in receipts)
    assert all(receipt.received_at == received_at for receipt in receipts)
    retries = [status for status in statuses if status["state"] == "reconnecting"]
    assert [status["retry_in_seconds"] for status in retries] == [0.001, 0.002]
    assert retries[0]["code"] == 406
    assert retries[0]["authenticated"] is False
    assert retries[1]["gap"] is True
    assert stream.health()["state"] == "stopped"
    assert stream.health()["authenticated"] is False
    assert sum(status["state"] == "subscribed" for status in statuses) == 2


def test_bad_auth_does_not_retry_or_claim_authentication(monkeypatch) -> None:
    class Connection:
        async def __aenter__(self):
            return FakeWebSocket(
                [
                    json.dumps([{"T": "success", "msg": "connected"}]),
                    json.dumps([{"T": "error", "code": 402, "msg": "auth failed"}]),
                ]
            )

        async def __aexit__(self, *args):
            return False

    monkeypatch.setattr("tradeagent.alpaca_stream.connect", lambda *args, **kwargs: Connection())
    stream = AlpacaMarketStream(_settings())

    async def collect():
        return await anext(stream.received_events(["SPY"]))

    with pytest.raises(StreamProviderError, match="402"):
        asyncio.run(collect())
    assert stream.health()["state"] == "failed"
    assert stream.health()["authenticated"] is False
    assert stream.health()["reconnects"] == 0
