from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from statistics import median
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict

from tradeagent.alpaca import AlpacaDataSettings
from tradeagent.domain import MarketBar


class EventMarketState(BaseModel):
    model_config = ConfigDict(frozen=True)
    symbol: str
    observed_at: datetime
    feed: str
    bid: Decimal
    ask: Decimal
    bid_size: Decimal
    ask_size: Decimal
    quote_at: datetime
    raw_quote: dict[str, Any]
    completed_bar: MarketBar | None
    previous_close: Decimal | None
    median_daily_dollar_volume: Decimal | None
    pre_event_volatility_bps: Decimal | None


class EventMarketClient:
    """Read-only raw-price market state; IEX is explicitly not consolidated NBBO."""

    def __init__(self, settings: AlpacaDataSettings, client: httpx.Client | None = None):
        self.settings = settings
        self.client = client or httpx.Client(timeout=20)
        self.owns_client = client is None
        self.daily_cache: dict[tuple[str, str], list[dict[str, Any]]] = {}

    def get(self, path: str, params: dict[str, str | int]) -> dict[str, Any]:
        response = self.client.get(
            f"{self.settings.data_url}{path}",
            params=params,
            headers={
                "APCA-API-KEY-ID": self.settings.key_id.get_secret_value(),
                "APCA-API-SECRET-KEY": self.settings.secret_key.get_secret_value(),
            },
        )
        response.raise_for_status()
        value = response.json()
        if not isinstance(value, dict):
            raise ValueError("market response must be object")
        return value

    def state(self, symbol: str, now: datetime) -> EventMarketState:
        if not symbol.isalpha():
            raise ValueError("common equity symbol required")
        feed = self.settings.feed
        quote = self.get(f"/v2/stocks/{symbol}/quotes/latest", {"feed": feed})["quote"]
        day_key = symbol, now.date().isoformat()
        if day_key not in self.daily_cache:
            payload = self.get(
                f"/v2/stocks/{symbol}/bars",
                {
                    "timeframe": "1Day",
                    "start": (now - timedelta(days=60)).isoformat(),
                    "end": now.replace(hour=0, minute=0, second=0).isoformat(),
                    "feed": "sip",
                    "adjustment": "raw",
                    "sort": "desc",
                    "limit": 30,
                },
            )
            self.daily_cache[day_key] = payload.get("bars") or []
        daily = self.daily_cache[day_key]
        minutes = (
            self.get(
                f"/v2/stocks/{symbol}/bars",
                {
                    "timeframe": "5Min",
                    "start": (now - timedelta(minutes=30)).isoformat(),
                    "end": now.isoformat(),
                    "feed": feed,
                    "adjustment": "raw",
                    "sort": "desc",
                    "limit": 6,
                },
            ).get("bars")
            or []
        )
        completed: MarketBar | None = None
        for raw in minutes:
            close_at = _timestamp(raw["t"]) + timedelta(minutes=5)
            if close_at <= now:
                completed = MarketBar(
                    symbol=symbol,
                    timestamp=close_at,
                    open=raw["o"],
                    high=raw["h"],
                    low=raw["l"],
                    close=raw["c"],
                    volume=raw["v"],
                )
                break
        volume = (
            Decimal(str(median(Decimal(str(bar["c"])) * Decimal(str(bar["v"])) for bar in daily)))
            if len(daily) >= 20
            else None
        )
        volatility = (
            Decimal(
                str(
                    median(
                        (Decimal(str(bar["h"])) - Decimal(str(bar["l"])))
                        / Decimal(str(bar["c"]))
                        * 10000
                        for bar in daily
                    )
                )
            )
            if len(daily) >= 20
            else None
        )
        return EventMarketState(
            symbol=symbol,
            observed_at=now,
            feed=feed,
            bid=quote["bp"],
            ask=quote["ap"],
            bid_size=quote["bs"],
            ask_size=quote["as"],
            quote_at=_timestamp(quote["t"]),
            raw_quote=quote,
            completed_bar=completed,
            previous_close=Decimal(str(daily[0]["c"])) if daily else None,
            median_daily_dollar_volume=volume,
            pre_event_volatility_bps=volatility,
        )

    def close(self) -> None:
        if self.owns_client:
            self.client.close()


def _timestamp(value: object) -> datetime:
    result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("exchange timestamp must be timezone aware")
    return result.astimezone(UTC)
