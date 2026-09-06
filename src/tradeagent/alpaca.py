from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Literal

import httpx
from pydantic import BaseModel, ConfigDict, Field, SecretStr
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


class HistoricalQuote(BaseModel):
    model_config = ConfigDict(frozen=True)

    symbol: str
    timestamp: datetime
    bid_exchange: str
    bid_price: Decimal = Field(ge=0)
    bid_size: Decimal = Field(ge=0)
    ask_exchange: str
    ask_price: Decimal = Field(ge=0)
    ask_size: Decimal = Field(ge=0)
    conditions: tuple[str, ...] = ()
    tape: str | None = None
    feed_source: Literal["iex", "sip"]


class HistoricalTrade(BaseModel):
    model_config = ConfigDict(frozen=True)

    symbol: str
    timestamp: datetime
    exchange: str
    price: Decimal = Field(gt=0)
    size: Decimal = Field(gt=0)
    trade_id: int | str
    conditions: tuple[str, ...] = ()
    tape: str | None = None
    feed_source: Literal["iex", "sip"]


class FeedEntitlement(BaseModel):
    model_config = ConfigDict(frozen=True)

    feed: Literal["sip"]
    historical_quotes: bool
    checked_at: datetime
    reason: str | None = None


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
        timeframe: Literal["1Day", "1Hour", "30Min", "5Min", "1Min"] = "1Day",
        feed: Literal["iex", "sip"] | None = None,
    ) -> Iterator[MarketBar]:
        normalized_symbol = _validated_request(symbol, start, end)
        selected_feed = feed or self._settings.feed
        params: dict[str, str | int] = {
            "start": start.astimezone(UTC).isoformat(),
            "end": end.astimezone(UTC).isoformat(),
            "timeframe": timeframe,
            "adjustment": "all",
            "feed": selected_feed,
            "sort": "asc",
            "limit": 10_000,
        }
        for payload in self._pages(normalized_symbol, "bars", params):
            raw_bars = _records(payload, "bars")
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

    def quotes(
        self,
        symbol: str,
        *,
        start: datetime,
        end: datetime,
        feed: Literal["iex", "sip"] | None = None,
    ) -> Iterator[HistoricalQuote]:
        normalized_symbol = _validated_request(symbol, start, end)
        selected_feed = feed or self._settings.feed
        params = _point_in_time_params(start, end, selected_feed)
        for payload in self._pages(normalized_symbol, "quotes", params):
            for quote in _records(payload, "quotes"):
                yield HistoricalQuote(
                    symbol=normalized_symbol,
                    timestamp=_timestamp(quote["t"]),
                    bid_exchange=str(quote.get("bx", "")),
                    bid_price=Decimal(str(quote["bp"])),
                    bid_size=Decimal(str(quote["bs"])),
                    ask_exchange=str(quote.get("ax", "")),
                    ask_price=Decimal(str(quote["ap"])),
                    ask_size=Decimal(str(quote["as"])),
                    conditions=_conditions(quote.get("c")),
                    tape=str(quote["z"]) if quote.get("z") is not None else None,
                    feed_source=selected_feed,
                )

    def trades(
        self,
        symbol: str,
        *,
        start: datetime,
        end: datetime,
        feed: Literal["iex", "sip"] | None = None,
    ) -> Iterator[HistoricalTrade]:
        normalized_symbol = _validated_request(symbol, start, end)
        selected_feed = feed or self._settings.feed
        params = _point_in_time_params(start, end, selected_feed)
        for payload in self._pages(normalized_symbol, "trades", params):
            for trade in _records(payload, "trades"):
                yield HistoricalTrade(
                    symbol=normalized_symbol,
                    timestamp=_timestamp(trade["t"]),
                    exchange=str(trade.get("x", "")),
                    price=Decimal(str(trade["p"])),
                    size=Decimal(str(trade["s"])),
                    trade_id=trade["i"],
                    conditions=_conditions(trade.get("c")),
                    tape=str(trade["z"]) if trade.get("z") is not None else None,
                    feed_source=selected_feed,
                )

    def probe_historical_sip(
        self,
        symbol: str,
        *,
        start: datetime,
        end: datetime,
        checked_at: datetime | None = None,
    ) -> FeedEntitlement:
        try:
            next(self.quotes(symbol, start=start, end=end, feed="sip"), None)
        except httpx.HTTPStatusError as error:
            if error.response.status_code != 403:
                raise
            return FeedEntitlement(
                feed="sip",
                historical_quotes=False,
                checked_at=checked_at or datetime.now(UTC),
                reason=_alpaca_error_message(error.response),
            )
        return FeedEntitlement(
            feed="sip",
            historical_quotes=True,
            checked_at=checked_at or datetime.now(UTC),
        )

    def _pages(
        self,
        symbol: str,
        resource: Literal["bars", "quotes", "trades"],
        params: dict[str, str | int],
    ) -> Iterator[dict[str, Any]]:
        url = f"{self._settings.data_url}/v2/stocks/{symbol}/{resource}"
        headers = {
            "APCA-API-KEY-ID": self._settings.key_id.get_secret_value(),
            "APCA-API-SECRET-KEY": self._settings.secret_key.get_secret_value(),
        }
        prior_tokens: set[str] = set()
        while True:
            response = self._client.get(url, headers=headers, params=params)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError("Alpaca response must be an object")
            yield payload
            next_token = payload.get("next_page_token")
            if not next_token:
                return
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
        "30Min": timedelta(minutes=30),
        "5Min": timedelta(minutes=5),
        "1Min": timedelta(minutes=1),
    }[timeframe]


def _validated_request(symbol: str, start: datetime, end: datetime) -> str:
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("start and end must be timezone-aware")
    if start >= end:
        raise ValueError("start must precede end")
    normalized_symbol = symbol.strip().upper()
    if not normalized_symbol:
        raise ValueError("symbol is required")
    return normalized_symbol


def _point_in_time_params(
    start: datetime,
    end: datetime,
    feed: Literal["iex", "sip"],
) -> dict[str, str | int]:
    return {
        "start": start.astimezone(UTC).isoformat(),
        "end": end.astimezone(UTC).isoformat(),
        "feed": feed,
        "sort": "asc",
        "limit": 10_000,
    }


def _records(payload: dict[str, Any], key: str) -> list[dict[str, Any]]:
    records = payload.get(key)
    if not isinstance(records, list) or not all(isinstance(item, dict) for item in records):
        raise ValueError(f"Alpaca response is missing the {key} array")
    return records


def _timestamp(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("Alpaca timestamp must be timezone-aware")
    return parsed.astimezone(UTC)


def _conditions(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise ValueError("Alpaca conditions must be an array")
    return tuple(str(item) for item in value)


def _alpaca_error_message(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        return response.text or "SIP access forbidden"
    if isinstance(payload, dict) and payload.get("message"):
        return str(payload["message"])
    return response.text or "SIP access forbidden"
