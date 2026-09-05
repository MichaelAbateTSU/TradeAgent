from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest
from pydantic import SecretStr

from tradeagent.alpaca import AlpacaDataSettings
from tradeagent.alpaca_news import AlpacaNewsClient
from tradeagent.news import NewsCategory, SourceReliability

NOW = datetime(2026, 9, 5, 15, tzinfo=UTC)


def test_alpaca_news_paginates_and_normalizes() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        page = request.url.params.get("page_token")
        article = {
            "created_at": "2026-09-05T14:00:00Z",
            "updated_at": "2026-09-05T14:01:00Z",
            "headline": "Federal Reserve releases results",
            "source": "benzinga",
            "url": "https://example.test/article",
            "symbols": ["SPY"],
        }
        return httpx.Response(
            200,
            json={
                "news": [article] if page is None else [],
                "next_page_token": "next" if page is None else None,
            },
        )

    settings = AlpacaDataSettings(
        key_id=SecretStr("key"),
        secret_key=SecretStr("secret"),
    )
    client = AlpacaNewsClient(
        settings,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        clock=lambda: NOW,
    )

    articles = list(
        client.articles(
            symbols=["SPY"],
            start=datetime(2026, 9, 5, tzinfo=UTC),
            end=NOW,
        )
    )

    assert len(requests) == 2
    assert requests[0].url.path == "/v1beta1/news"
    assert requests[0].url.params["limit"] == "50"
    assert articles[0].category is NewsCategory.MACRO
    assert articles[0].reliability is SourceReliability.LICENSED
    assert articles[0].received_at == NOW


def test_alpaca_news_rejects_invalid_interval_and_response() -> None:
    settings = AlpacaDataSettings(
        key_id=SecretStr("key"),
        secret_key=SecretStr("secret"),
    )
    client = AlpacaNewsClient(
        settings,
        client=httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(200, json={}))),
    )

    with pytest.raises(ValueError, match="increasing"):
        list(client.articles(symbols=["SPY"], start=NOW, end=NOW))
    with pytest.raises(ValueError, match="news array"):
        list(
            client.articles(
                symbols=["SPY"],
                start=datetime(2026, 9, 5, tzinfo=UTC),
                end=NOW,
            )
        )
