from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Literal

import httpx
from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from tradeagent.domain import MarketBar


class AlpacaDataSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="ALPACA_",
        env_file=".env",
        env_file_encoding="utf-8",
        frozen=True,
        extra="ignore",
    )

    key_id: SecretStr
    secret_key: SecretStr
    data_url: Literal["https://data.alpaca.markets"] = "https://data.alpaca.markets"
    feed: Literal["iex", "sip"] = "iex"


class AlpacaDataClient:
    """Historical market-data client with no brokerage or order endpoint."""

    def __init__(
        self,
        settings: AlpacaDataSettings,
        *,
        client: httpx.Client | None = None,
    ) -> None:
        self._settings = settings
        self._client = client or httpx.Client(timeout=30)
        self._owns_client = client is None

    def bars(
        self,
        symbol: str,
        *,
        start: datetime,
        end: datetime,
        timeframe: Literal["1Day", "1Hour", "5Min", "1Min"] = "1Day",
    ) -> Iterator[MarketBar]:
        if start.tzinfo is None or end.tzinfo is None:
            raise ValueError("start and end must be timezone-aware")
        if start >= end:
            raise ValueError("start must precede end")

        normalized_symbol = symbol.strip().upper()
        url = f"{self._settings.data_url}/v2/stocks/{normalized_symbol}/bars"
        headers = {
            "APCA-API-KEY-ID": self._settings.key_id.get_secret_value(),
            "APCA-API-SECRET-KEY": self._settings.secret_key.get_secret_value(),
        }
        params: dict[str, str | int] = {
            "start": start.astimezone(UTC).isoformat(),
            "end": end.astimezone(UTC).isoformat(),
            "timeframe": timeframe,
            "adjustment": "all",
            "feed": self._settings.feed,
            "sort": "asc",
            "limit": 10_000,
        }
        prior_tokens: set[str] = set()
        while True:
            response = self._client.get(url, headers=headers, params=params)
            response.raise_for_status()
            payload = response.json()
            raw_bars = payload.get("bars")
            if not isinstance(raw_bars, list):
                raise ValueError("Alpaca response is missing the bars array")
            for raw_bar in raw_bars:
                yield MarketBar(
                    symbol=normalized_symbol,
                    timestamp=(
                        datetime.fromisoformat(str(raw_bar["t"]).replace("Z", "+00:00"))
                        + _timeframe_duration(timeframe)
                    ),
                    open=Decimal(str(raw_bar["o"])),
                    high=Decimal(str(raw_bar["h"])),
                    low=Decimal(str(raw_bar["l"])),
                    close=Decimal(str(raw_bar["c"])),
                    volume=Decimal(str(raw_bar["v"])),
                )

            next_token = payload.get("next_page_token")
            if not next_token:
                break
            token = str(next_token)
            if token in prior_tokens:
                raise ValueError("Alpaca returned a repeated pagination token")
            prior_tokens.add(token)
            params["page_token"] = token

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> AlpacaDataClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def _timeframe_duration(timeframe: str) -> timedelta:
    return {
        "1Day": timedelta(0),
        "1Hour": timedelta(hours=1),
        "5Min": timedelta(minutes=5),
        "1Min": timedelta(minutes=1),
    }[timeframe]
