from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Literal

import httpx
from pydantic import BaseModel, ConfigDict, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from tradeagent.domain import OrderRequest


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


class AlpacaPaperOrder(BaseModel):
    model_config = ConfigDict(frozen=True, extra="ignore", populate_by_name=True)

    id: str
    client_order_id: str
    status: AlpacaOrderStatus
    symbol: str
    side: Literal["buy", "sell"]
    quantity: Decimal = Field(alias="qty")
    filled_quantity: Decimal = Field(alias="filled_qty")
    filled_average_price: Decimal | None = Field(alias="filled_avg_price")
    created_at: datetime
    updated_at: datetime | None = None


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

    def positions(self) -> tuple[AlpacaPaperPosition, ...]:
        payload = self._request("GET", "/v2/positions")
        if not isinstance(payload, list):
            raise ValueError("Alpaca positions response must be an array")
        return tuple(AlpacaPaperPosition.model_validate(item) for item in payload)

    def submit_market_order(self, order: OrderRequest) -> AlpacaPaperOrder:
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
