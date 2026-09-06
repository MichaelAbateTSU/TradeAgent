from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest
from pydantic import SecretStr, ValidationError

from tradeagent.alpaca import AlpacaDataClient, AlpacaDataSettings


def _settings() -> AlpacaDataSettings:
    return AlpacaDataSettings(
        key_id=SecretStr("test-key"),
        secret_key=SecretStr("test-secret"),
    )


def test_alpaca_data_client_paginates_and_normalizes_bars() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.headers["APCA-API-KEY-ID"] == "test-key"
        page = request.url.params.get("page_token")
        if page is None:
            return httpx.Response(
                200,
                json={
                    "bars": [
                        {
                            "t": "2025-01-02T21:00:00Z",
                            "o": 100,
                            "h": 102,
                            "l": 99,
                            "c": 101,
                            "v": 1000,
                        }
                    ],
                    "next_page_token": "page-2",
                },
            )
        return httpx.Response(
            200,
            json={
                "bars": [
                    {
                        "t": "2025-01-03T21:00:00Z",
                        "o": 101,
                        "h": 103,
                        "l": 100,
                        "c": 102,
                        "v": 1200,
                    }
                ],
                "next_page_token": None,
            },
        )

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = AlpacaDataClient(_settings(), client=http_client)
    bars = list(
        client.bars(
            "spy",
            start=datetime(2025, 1, 1, tzinfo=UTC),
            end=datetime(2025, 1, 4, tzinfo=UTC),
        )
    )

    assert len(requests) == 2
    assert [bar.symbol for bar in bars] == ["SPY", "SPY"]
    assert bars[0].close == 101


def test_intraday_bars_are_normalized_to_close_time() -> None:
    client = AlpacaDataClient(
        _settings(),
        client=httpx.Client(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(
                    200,
                    json={
                        "bars": [
                            {
                                "t": "2025-01-02T14:30:00Z",
                                "o": 100,
                                "h": 101,
                                "l": 99,
                                "c": 100,
                                "v": 1000,
                            }
                        ],
                        "next_page_token": None,
                    },
                )
            )
        ),
    )

    bar = next(
        client.bars(
            "SPY",
            start=datetime(2025, 1, 2, tzinfo=UTC),
            end=datetime(2025, 1, 3, tzinfo=UTC),
            timeframe="5Min",
        )
    )

    assert bar.timestamp == datetime(2025, 1, 2, 14, 35, tzinfo=UTC)

    minute_bar = next(
        client.bars(
            "SPY",
            start=datetime(2025, 1, 2, tzinfo=UTC),
            end=datetime(2025, 1, 3, tzinfo=UTC),
            timeframe="1Min",
        )
    )
    assert minute_bar.timestamp == datetime(2025, 1, 2, 14, 31, tzinfo=UTC)

    thirty_minute_bar = next(
        client.bars(
            "SPY",
            start=datetime(2025, 1, 2, tzinfo=UTC),
            end=datetime(2025, 1, 3, tzinfo=UTC),
            timeframe="30Min",
        )
    )
    assert thirty_minute_bar.timestamp == datetime(2025, 1, 2, 15, 0, tzinfo=UTC)


def test_historical_quotes_and_trades_are_typed_and_paginated() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        resource = request.url.path.rsplit("/", 1)[-1]
        if resource == "quotes":
            return httpx.Response(
                200,
                json={
                    "quotes": [
                        {
                            "t": "2025-01-02T14:30:00.123456Z",
                            "bx": "P",
                            "bp": 100,
                            "bs": 10,
                            "ax": "Q",
                            "ap": 100.02,
                            "as": 12,
                            "c": ["R"],
                            "z": "C",
                        }
                    ],
                    "next_page_token": None,
                },
            )
        return httpx.Response(
            200,
            json={
                "trades": [
                    {
                        "t": "2025-01-02T14:30:00.223456Z",
                        "x": "P",
                        "p": 100.01,
                        "s": 3,
                        "i": 123,
                        "c": ["@"],
                        "z": "C",
                    }
                ],
                "next_page_token": None,
            },
        )

    client = AlpacaDataClient(
        _settings(),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    start = datetime(2025, 1, 2, 14, 30, tzinfo=UTC)
    end = datetime(2025, 1, 2, 14, 31, tzinfo=UTC)

    quote = next(client.quotes("spy", start=start, end=end, feed="sip"))
    trade = next(client.trades("spy", start=start, end=end, feed="sip"))

    assert quote.symbol == "SPY"
    assert quote.bid_exchange == "P"
    assert quote.ask_size == Decimal("12")
    assert quote.feed_source == "sip"
    assert trade.exchange == "P"
    assert trade.trade_id == 123
    assert trade.conditions == ("@",)
    assert all(request.url.params["feed"] == "sip" for request in requests)


def test_bulk_historical_quotes_and_trades_preserve_symbols() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["symbols"] == "SPY,QQQ"
        resource = request.url.path.rsplit("/", 1)[-1]
        if resource == "quotes":
            return httpx.Response(
                200,
                json={
                    "quotes": {
                        "SPY": [
                            {
                                "t": "2025-01-02T14:30:00Z",
                                "bp": 100,
                                "bs": 10,
                                "ap": 100.02,
                                "as": 12,
                            }
                        ],
                        "QQQ": None,
                    },
                    "next_page_token": None,
                },
            )
        return httpx.Response(
            200,
            json={
                "trades": {
                    "SPY": None,
                    "QQQ": [
                        {
                            "t": "2025-01-02T14:30:00Z",
                            "p": 200,
                            "s": 5,
                            "i": 42,
                        }
                    ],
                },
                "next_page_token": None,
            },
        )

    client = AlpacaDataClient(
        _settings(),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    start = datetime(2025, 1, 2, 14, 30, tzinfo=UTC)
    end = datetime(2025, 1, 2, 14, 31, tzinfo=UTC)

    quotes = tuple(client.quotes_many(("spy", "qqq"), start=start, end=end, feed="sip"))
    trades = tuple(client.trades_many(("spy", "qqq"), start=start, end=end, feed="sip"))

    assert [quote.symbol for quote in quotes] == ["SPY"]
    assert [trade.symbol for trade in trades] == ["QQQ"]


def test_sip_entitlement_probe_reports_forbidden_without_fallback() -> None:
    client = AlpacaDataClient(
        _settings(),
        client=httpx.Client(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(
                    403,
                    json={"message": "subscription does not permit querying SIP data"},
                    request=request,
                )
            )
        ),
    )
    checked_at = datetime(2025, 1, 3, tzinfo=UTC)

    result = client.probe_historical_sip(
        "SPY",
        start=datetime(2025, 1, 2, 14, 30, tzinfo=UTC),
        end=datetime(2025, 1, 2, 14, 31, tzinfo=UTC),
        checked_at=checked_at,
    )

    assert not result.historical_quotes
    assert result.checked_at == checked_at
    assert "subscription" in str(result.reason)


def test_alpaca_data_client_validates_range_and_response() -> None:
    http_client = httpx.Client(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json={}))
    )
    client = AlpacaDataClient(_settings(), client=http_client)
    aware = datetime(2025, 1, 1, tzinfo=UTC)

    with pytest.raises(ValueError, match="timezone-aware"):
        list(client.bars("SPY", start=datetime(2025, 1, 1), end=aware))
    with pytest.raises(ValueError, match="precede"):
        list(client.bars("SPY", start=aware, end=aware))
    with pytest.raises(ValueError, match="bars array"):
        list(
            client.bars(
                "SPY",
                start=aware,
                end=datetime(2025, 1, 2, tzinfo=UTC),
            )
        )


def test_alpaca_data_url_cannot_be_redirected_to_broker() -> None:
    with pytest.raises(ValidationError):
        AlpacaDataSettings(
            key_id=SecretStr("key"),
            secret_key=SecretStr("secret"),
            data_url="https://api.alpaca.markets",
        )
