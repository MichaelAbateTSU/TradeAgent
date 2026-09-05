from __future__ import annotations

from datetime import UTC, datetime

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
