from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, datetime

import httpx

from tradeagent.alpaca import AlpacaDataSettings
from tradeagent.news import (
    MarketNewsItem,
    NewsCategory,
    SourceReliability,
)


class AlpacaNewsClient:
    def __init__(
        self,
        settings: AlpacaDataSettings,
        *,
        client: httpx.Client | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._settings = settings
        self._client = client or httpx.Client(timeout=30)
        self._owns_client = client is None
        self._clock = clock

    def articles(
        self,
        *,
        symbols: Sequence[str],
        start: datetime,
        end: datetime,
    ) -> Iterator[MarketNewsItem]:
        if start.tzinfo is None or end.tzinfo is None or start >= end:
            raise ValueError("news interval must be timezone-aware and increasing")
        params: dict[str, str | int | bool] = {
            "start": start.astimezone(UTC).isoformat(),
            "end": end.astimezone(UTC).isoformat(),
            "sort": "asc",
            "symbols": ",".join(sorted({symbol.upper() for symbol in symbols})),
            "limit": 50,
            "include_content": False,
        }
        tokens: set[str] = set()
        while True:
            response = self._client.get(
                f"{self._settings.data_url}/v1beta1/news",
                headers={
                    "APCA-API-KEY-ID": self._settings.key_id.get_secret_value(),
                    "APCA-API-SECRET-KEY": self._settings.secret_key.get_secret_value(),
                },
                params=params,
            )
            response.raise_for_status()
            payload = response.json()
            articles = payload.get("news")
            if not isinstance(articles, list):
                raise ValueError("Alpaca news response is missing the news array")
            received_at = self._clock()
            for article in articles:
                published_at = _timestamp(article["created_at"])
                updated_at = _timestamp(article["updated_at"])
                yield MarketNewsItem(
                    source=f"Alpaca:{article.get('source', 'unknown')}",
                    source_url=str(article["url"]),
                    category=_category(str(article["headline"])),
                    symbols=tuple(article.get("symbols", ())),
                    headline=str(article["headline"]),
                    published_at=published_at,
                    received_at=max(received_at, published_at),
                    updated_at=updated_at,
                    reliability=SourceReliability.LICENSED,
                )
            next_token = payload.get("next_page_token")
            if not next_token:
                break
            token = str(next_token)
            if token in tokens:
                raise ValueError("Alpaca news returned a repeated pagination token")
            tokens.add(token)
            params["page_token"] = token

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> AlpacaNewsClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def _timestamp(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("Alpaca news timestamp must be timezone-aware")
    return parsed.astimezone(UTC)


def _category(headline: str) -> NewsCategory:
    normalized = headline.lower()
    if "halt" in normalized or "suspend" in normalized:
        return NewsCategory.HALT
    if any(term in normalized for term in ("federal reserve", "cpi", "payroll", "gdp")):
        return NewsCategory.MACRO
    if "earnings" in normalized or "results" in normalized:
        return NewsCategory.EARNINGS
    if "sec filing" in normalized or "form 8-k" in normalized:
        return NewsCategory.FILING
    return NewsCategory.GENERAL
