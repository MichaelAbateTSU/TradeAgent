from __future__ import annotations

from datetime import UTC, date, datetime, time
from decimal import Decimal
from enum import StrEnum
from typing import Any, Literal, Self
from zoneinfo import ZoneInfo

import httpx
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from tradeagent.domain import OrderRequest, OrderType


class AlpacaPaperSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="ALPACA_",
        env_file=".env",
        env_file_encoding="utf-8",
        frozen=True,
        extra="ignore",
    )

    key_id: SecretStr
    secret_key: SecretStr
    paper_url: Literal["https://paper-api.alpaca.markets"] = "https://paper-api.alpaca.markets"


class AlpacaOrderStatus(StrEnum):
    NEW = "new"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    DONE_FOR_DAY = "done_for_day"
    CANCELED = "canceled"
    EXPIRED = "expired"
    REPLACED = "replaced"
    PENDING_CANCEL = "pending_cancel"
    PENDING_REPLACE = "pending_replace"
    ACCEPTED = "accepted"
    PENDING_NEW = "pending_new"
    ACCEPTED_FOR_BIDDING = "accepted_for_bidding"
    STOPPED = "stopped"
    REJECTED = "rejected"
    SUSPENDED = "suspended"
    CALCULATED = "calculated"
    HELD = "held"


class AlpacaPaperAccount(BaseModel):
    model_config = ConfigDict(frozen=True, extra="ignore")

    id: str
    status: str
    currency: str
    cash: Decimal
    portfolio_value: Decimal
    buying_power: Decimal
    non_marginable_buying_power: Decimal | None = None
    pattern_day_trader: bool = False
    trading_blocked: bool
    transfers_blocked: bool
    account_blocked: bool


class AlpacaPaperPosition(BaseModel):
    model_config = ConfigDict(frozen=True, extra="ignore", populate_by_name=True)

    symbol: str
    quantity: Decimal = Field(alias="qty")
    average_entry_price: Decimal = Field(alias="avg_entry_price")
    market_value: Decimal
    unrealized_pnl: Decimal = Field(alias="unrealized_pl")
    available_quantity: Decimal | None = Field(default=None, alias="qty_available")


class AlpacaPaperOrder(BaseModel):
    model_config = ConfigDict(frozen=True, extra="allow", populate_by_name=True)

    id: str
    client_order_id: str
    status: AlpacaOrderStatus
    symbol: str
    side: Literal["buy", "sell"]
    quantity: Decimal | None = Field(alias="qty")
    notional: Decimal | None = None
    filled_quantity: Decimal = Field(alias="filled_qty")
    filled_average_price: Decimal | None = Field(alias="filled_avg_price")
    created_at: datetime
    updated_at: datetime | None = None
    submitted_at: datetime | None = None
    filled_at: datetime | None = None
    canceled_at: datetime | None = None

    @model_validator(mode="after")
    def validate_size_basis(self) -> Self:
        if self.quantity is None and (self.notional is None or self.notional <= 0):
            raise ValueError("order must identify requested quantity or positive notional")
        return self


class PaperAsset(BaseModel):
    model_config = ConfigDict(frozen=True, extra="ignore", populate_by_name=True)
    id: str
    symbol: str
    name: str
    asset_class: str = Field(alias="class")
    exchange: str
    status: str
    tradable: bool
    fractionable: bool = False


class PaperCryptoAsset(BaseModel):
    model_config = ConfigDict(
        frozen=True, extra="allow", populate_by_name=True, allow_inf_nan=False
    )
    id: str
    symbol: str
    asset_class: Literal["crypto"] = Field(alias="class")
    status: str
    tradable: bool
    min_order_size: Decimal = Field(gt=0)
    min_trade_increment: Decimal = Field(gt=0)
    price_increment: Decimal = Field(gt=0)


def canonical_crypto_symbol(symbol: str) -> str:
    value = symbol.strip().upper()
    if "/" not in value and value.endswith("USD"):
        value = value[:-3] + "/USD"
    base, separator, quote = value.partition("/")
    if not separator or not base.isalnum() or quote != "USD":
        raise ValueError("a USD crypto pair is required")
    return value


class PaperClock(BaseModel):
    model_config = ConfigDict(frozen=True, extra="ignore")
    timestamp: datetime
    is_open: bool
    next_open: datetime
    next_close: datetime


class PaperCalendarSession(BaseModel):
    model_config = ConfigDict(frozen=True)
    session_date: date
    open_at: AwareDatetime
    close_at: AwareDatetime

    @model_validator(mode="after")
    def validate_bounds(self) -> Self:
        zone = ZoneInfo("America/New_York")
        if (
            self.open_at >= self.close_at
            or self.open_at.astimezone(zone).date() != self.session_date
            or self.close_at.astimezone(zone).date() != self.session_date
        ):
            raise ValueError("invalid broker calendar session bounds")
        return self


class AlpacaPaperClient:
    """Typed client that is structurally unable to address Alpaca's live endpoint."""

    def __init__(
        self,
        settings: AlpacaPaperSettings,
        *,
        client: httpx.Client | None = None,
    ) -> None:
        self._settings = settings
        self._client = client or httpx.Client(timeout=30)
        self._owns_client = client is None

    def account(self) -> AlpacaPaperAccount:
        payload = self._request("GET", "/v2/account")
        if not isinstance(payload, dict):
            raise ValueError("Alpaca account response must be an object")
        return AlpacaPaperAccount.model_validate(payload)

    def account_history(self) -> dict[str, Any]:
        from hashlib import sha256

        account = self.account()
        if account.currency != "USD":
            raise ValueError("USD paper account history required")
        raw_orders = self._request(
            "GET", "/v2/orders", params={"status": "all", "limit": "500", "direction": "asc"}
        )
        if not isinstance(raw_orders, list) or len(raw_orders) >= 500:
            raise ValueError("complete bounded account order history unavailable")
        activities: list[dict[str, Any]] = []
        cursor: str | None = None
        for _ in range(20):
            parameters = {"page_size": "100", "direction": "asc"}
            if cursor is not None:
                parameters["page_token"] = cursor
            page = self._request("GET", "/v2/account/activities", params=parameters)
            if not isinstance(page, list) or any(not isinstance(row, dict) for row in page):
                raise ValueError("invalid account activity history")
            activities.extend(page)
            if len(page) < 100:
                break
            next_cursor = str(page[-1]["id"])
            if next_cursor == cursor:
                raise ValueError("account activity pagination did not advance")
            cursor = next_cursor
        else:
            raise ValueError("account activity history exceeds verified bounded pagination")
        ending_account = self.account()
        if ending_account.id != account.id or ending_account.currency != "USD":
            raise ValueError("account changed while reading history")
        return {
            "account_digest": sha256(account.id.encode()).hexdigest(),
            "broker_host": self.broker_host,
            "observed_at": datetime.now(UTC).isoformat(),
            "complete": True,
            "account_cash": str(ending_account.cash),
            "orders": [
                AlpacaPaperOrder.model_validate(row).model_dump(mode="json") for row in raw_orders
            ],
            "activities": activities,
        }

    def positions(self) -> tuple[AlpacaPaperPosition, ...]:
        payload = self._request("GET", "/v2/positions")
        if not isinstance(payload, list):
            raise ValueError("Alpaca positions response must be an array")
        return tuple(AlpacaPaperPosition.model_validate(item) for item in payload)

    @property
    def broker_host(self) -> str:
        return self._settings.paper_url

    def clock(self) -> PaperClock:
        return PaperClock.model_validate(self._request("GET", "/v2/clock"))

    def calendar(self, *, start: date, end: date) -> tuple[PaperCalendarSession, ...]:
        if start > end:
            raise ValueError("broker calendar range must increase")
        payload = self._request(
            "GET", "/v2/calendar", params={"start": start.isoformat(), "end": end.isoformat()}
        )
        if not isinstance(payload, list):
            raise ValueError("broker calendar response must be an array")
        sessions: list[PaperCalendarSession] = []
        zone = ZoneInfo("America/New_York")
        for row in payload:
            day = date.fromisoformat(row["date"])
            if not start <= day <= end or any(item.session_date == day for item in sessions):
                raise ValueError("broker calendar contains an unexpected or duplicate date")
            sessions.append(
                PaperCalendarSession(
                    session_date=day,
                    open_at=datetime.combine(day, time.fromisoformat(row["open"]), zone).astimezone(
                        UTC
                    ),
                    close_at=datetime.combine(
                        day, time.fromisoformat(row["close"]), zone
                    ).astimezone(UTC),
                )
            )
        return tuple(sorted(sessions, key=lambda item: item.session_date))

    def asset(self, symbol: str) -> PaperAsset:
        if not symbol.isalpha():
            raise ValueError("experimental equity symbol must be alphabetic")
        return PaperAsset.model_validate(self._request("GET", f"/v2/assets/{symbol.upper()}"))

    def submit_limit_order(self, order: OrderRequest, limit_price: Decimal) -> AlpacaPaperOrder:
        if not order.symbol.isalpha() or limit_price <= 0 or len(order.client_order_id) > 48:
            raise ValueError("invalid regular-session equity paper limit order")
        if order.order_type is not OrderType.LIMIT:
            raise ValueError("paper limit endpoint requires a limit intent")
        payload = self._request(
            "POST",
            "/v2/orders",
            json={
                "symbol": order.symbol,
                "qty": format(order.quantity, "f"),
                "side": order.side.value,
                "type": "limit",
                "time_in_force": "day",
                "limit_price": format(limit_price, "f"),
                "extended_hours": False,
                "client_order_id": order.client_order_id,
            },
        )
        return AlpacaPaperOrder.model_validate(payload)

    def submit_market_order(self, order: OrderRequest) -> AlpacaPaperOrder:
        if order.order_type is not OrderType.MARKET:
            raise ValueError("paper market endpoint requires a market intent")
        if len(order.client_order_id) > 48:
            raise ValueError("Alpaca client_order_id cannot exceed 48 characters")
        time_in_force = "gtc" if "/" in order.symbol else "day"
        payload = self._request(
            "POST",
            "/v2/orders",
            json={
                "symbol": order.symbol,
                "qty": format(order.quantity, "f"),
                "side": order.side.value,
                "type": "market",
                "time_in_force": time_in_force,
                "client_order_id": order.client_order_id,
            },
        )
        if not isinstance(payload, dict):
            raise ValueError("Alpaca order response must be an object")
        return AlpacaPaperOrder.model_validate(payload)

    def bitcoin_asset(self) -> dict[str, Any]:
        payload = self._request(
            "GET", "/v2/assets", params={"asset_class": "crypto", "status": "active"}
        )
        if not isinstance(payload, list):
            raise ValueError("crypto assets must be an array")
        for item in payload:
            if isinstance(item, dict) and item.get("symbol") == "BTC/USD":
                return dict(item)
        raise ValueError("BTC/USD is not available on this paper account")

    def crypto_asset(self, symbol: str) -> PaperCryptoAsset:
        wanted = canonical_crypto_symbol(symbol)
        payload = self._request(
            "GET", "/v2/assets", params={"asset_class": "crypto", "status": "active"}
        )
        if not isinstance(payload, list):
            raise ValueError("crypto assets must be an array")
        for row in payload:
            if not isinstance(row, dict) or not row.get("symbol"):
                continue
            try:
                matches = canonical_crypto_symbol(str(row["symbol"])) == wanted
            except ValueError:
                matches = False
            if matches:
                return PaperCryptoAsset.model_validate({**row, "symbol": wanted})
        raise ValueError(f"{wanted} is unavailable on this paper account")

    def submit_crypto_limit_order(
        self, order: OrderRequest, limit_price: Decimal, *, asset: PaperCryptoAsset | None = None
    ) -> AlpacaPaperOrder:
        symbol = canonical_crypto_symbol(order.symbol)
        asset = asset or self.crypto_asset(symbol)
        self._validate_crypto_order(order, asset)
        if (
            order.order_type is not OrderType.LIMIT
            or not limit_price.is_finite()
            or limit_price <= 0
            or limit_price % asset.price_increment
        ):
            raise ValueError("crypto limit price must match the broker price increment")
        return AlpacaPaperOrder.model_validate(
            self._request(
                "POST",
                "/v2/orders",
                json={
                    "symbol": symbol,
                    "qty": format(order.quantity, "f"),
                    "side": order.side.value,
                    "type": "limit",
                    "time_in_force": "gtc",
                    "limit_price": format(limit_price, "f"),
                    "client_order_id": order.client_order_id,
                },
            )
        )

    def submit_crypto_market_order(
        self, order: OrderRequest, *, asset: PaperCryptoAsset | None = None
    ) -> AlpacaPaperOrder:
        symbol = canonical_crypto_symbol(order.symbol)
        asset = asset or self.crypto_asset(symbol)
        self._validate_crypto_order(order, asset)
        if order.order_type is not OrderType.MARKET:
            raise ValueError("crypto market endpoint requires a market intent")
        return AlpacaPaperOrder.model_validate(
            self._request(
                "POST",
                "/v2/orders",
                json={
                    "symbol": symbol,
                    "qty": format(order.quantity, "f"),
                    "side": order.side.value,
                    "type": "market",
                    "time_in_force": "gtc",
                    "client_order_id": order.client_order_id,
                },
            )
        )

    @staticmethod
    def _validate_crypto_order(order: OrderRequest, asset: PaperCryptoAsset) -> None:
        if (
            canonical_crypto_symbol(order.symbol) != asset.symbol
            or asset.status != "active"
            or not asset.tradable
            or not order.quantity.is_finite()
            or order.quantity < asset.min_order_size
            or order.quantity % asset.min_trade_increment
            or len(order.client_order_id) > 48
        ):
            raise ValueError("crypto intent violates broker identity, tradability or increments")

    def account_activity_page(
        self,
        *,
        after: datetime,
        until: datetime,
        page_token: str | None = None,
        page_size: int = 100,
        activity_types: tuple[str, ...] | None = None,
    ) -> dict[str, Any]:
        """One incremental page, without the legacy account-order-history size ceiling."""
        if (
            after.tzinfo is None
            or until.tzinfo is None
            or after > until
            or not 1 <= page_size <= 100
        ):
            raise ValueError(
                "an ordered aware activity window and supported page size are required"
            )
        params = {
            "after": after.isoformat(),
            "until": until.isoformat(),
            "direction": "asc",
            "page_size": str(page_size),
        }
        if page_token is not None:
            params["page_token"] = page_token
        if activity_types:
            params["activity_types"] = ",".join(activity_types)
        payload = self._request("GET", "/v2/account/activities", params=params)
        if not isinstance(payload, list) or any(
            not isinstance(row, dict) or not row.get("id") for row in payload
        ):
            raise ValueError("invalid incremental activity page")
        next_token = str(payload[-1]["id"]) if len(payload) == page_size else None
        if next_token is not None and next_token == page_token:
            raise ValueError("activity pagination did not advance")
        return {
            "activities": payload,
            "next_page_token": next_token,
            "complete": next_token is None,
        }

    def submit_bitcoin_test_buy(self, client_order_id: str) -> AlpacaPaperOrder:
        if not client_order_id.startswith("ta-crypto-test-") or len(client_order_id) > 48:
            raise ValueError("explicit crypto paper test identity required")
        return AlpacaPaperOrder.model_validate(
            self._request(
                "POST",
                "/v2/orders",
                json={
                    "symbol": "BTC/USD",
                    "notional": "20",
                    "side": "buy",
                    "type": "market",
                    "time_in_force": "gtc",
                    "client_order_id": client_order_id,
                },
            )
        )

    def order_by_client_id(self, client_order_id: str) -> AlpacaPaperOrder:
        payload = self._request(
            "GET",
            "/v2/orders:by_client_order_id",
            params={"client_order_id": client_order_id},
        )
        if not isinstance(payload, dict):
            raise ValueError("Alpaca order response must be an object")
        return AlpacaPaperOrder.model_validate(payload)

    def find_order_by_client_id(self, client_order_id: str) -> AlpacaPaperOrder | None:
        try:
            return self.order_by_client_id(client_order_id)
        except httpx.HTTPStatusError as error:
            if error.response.status_code == 404:
                return None
            raise

    def open_orders(self) -> tuple[AlpacaPaperOrder, ...]:
        payload = self._request(
            "GET",
            "/v2/orders",
            params={"status": "open", "direction": "asc"},
        )
        if not isinstance(payload, list):
            raise ValueError("Alpaca orders response must be an array")
        return tuple(AlpacaPaperOrder.model_validate(item) for item in payload)

    def cancel_order(self, order_id: str) -> None:
        self._request("DELETE", f"/v2/orders/{order_id}", expect_json=False)

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None = None,
        json: dict[str, Any] | None = None,
        expect_json: bool = True,
    ) -> Any:
        response = self._client.request(
            method,
            f"{self._settings.paper_url}{path}",
            headers={
                "APCA-API-KEY-ID": self._settings.key_id.get_secret_value(),
                "APCA-API-SECRET-KEY": self._settings.secret_key.get_secret_value(),
            },
            params=params,
            json=json,
        )
        response.raise_for_status()
        return response.json() if expect_json else None

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> AlpacaPaperClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
