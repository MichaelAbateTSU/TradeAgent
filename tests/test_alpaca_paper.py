from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest
from conftest import make_order
from pydantic import SecretStr, ValidationError

from tradeagent.alpaca_paper import (
    AlpacaOrderStatus,
    AlpacaPaperClient,
    AlpacaPaperSettings,
)


def _settings() -> AlpacaPaperSettings:
    return AlpacaPaperSettings(
        key_id=SecretStr("paper-key"),
        secret_key=SecretStr("paper-secret"),
    )


def _order_payload(**updates: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "id": "order-1",
        "client_order_id": "strategy:decision:1",
        "status": "new",
        "symbol": "SPY",
        "side": "buy",
        "qty": "10",
        "filled_qty": "0",
        "filled_avg_price": None,
        "created_at": "2025-01-02T15:00:00Z",
        "updated_at": "2025-01-02T15:00:00Z",
    }
    payload.update(updates)
    return payload


def test_paper_client_reads_account_and_positions() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.host == "paper-api.alpaca.markets"
        assert request.headers["APCA-API-KEY-ID"] == "paper-key"
        if request.url.path == "/v2/account":
            return httpx.Response(
                200,
                json={
                    "id": "account-1",
                    "status": "ACTIVE",
                    "currency": "USD",
                    "cash": "100000",
                    "portfolio_value": "100500",
                    "buying_power": "200000",
                    "pattern_day_trader": False,
                    "trading_blocked": False,
                    "transfers_blocked": False,
                    "account_blocked": False,
                },
            )
        return httpx.Response(
            200,
            json=[
                {
                    "symbol": "SPY",
                    "qty": "5",
                    "avg_entry_price": "100",
                    "market_value": "505",
                    "unrealized_pl": "5",
                }
            ],
        )

    client = AlpacaPaperClient(
        _settings(),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    account = client.account()
    positions = client.positions()

    assert account.portfolio_value == Decimal("100500")
    assert positions[0].quantity == Decimal("5")


def test_paper_client_submits_idempotent_market_order_and_tracks_state(
    timestamp: datetime,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "POST":
            return httpx.Response(200, json=_order_payload())
        if request.url.path.endswith(":by_client_order_id"):
            return httpx.Response(
                200,
                json=_order_payload(status="partially_filled", filled_qty="4"),
            )
        if request.method == "DELETE":
            return httpx.Response(204)
        return httpx.Response(200, json=[_order_payload()])

    client = AlpacaPaperClient(
        _settings(),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    order = make_order(timestamp)

    submitted = client.submit_market_order(order)
    refreshed = client.order_by_client_id(order.client_order_id)
    open_orders = client.open_orders()
    client.cancel_order(submitted.id)

    assert submitted.status is AlpacaOrderStatus.NEW
    assert refreshed.status is AlpacaOrderStatus.PARTIALLY_FILLED
    assert refreshed.filled_quantity == Decimal("4")
    assert open_orders == (submitted,)
    assert [request.method for request in requests] == ["POST", "GET", "GET", "DELETE"]
    assert requests[0].read().decode().count(order.client_order_id) == 1


def test_paper_client_rejects_invalid_endpoint_and_long_client_id(
    timestamp: datetime,
) -> None:
    with pytest.raises(ValidationError):
        AlpacaPaperSettings(
            key_id=SecretStr("key"),
            secret_key=SecretStr("secret"),
            paper_url="https://api.alpaca.markets",
        )

    client = AlpacaPaperClient(
        _settings(),
        client=httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(500))),
    )
    order = make_order(timestamp, client_order_id="x" * 49)
    with pytest.raises(ValueError, match="48"):
        client.submit_market_order(order)


def test_paper_order_model_parses_utc_timestamp() -> None:
    client = AlpacaPaperClient(
        _settings(),
        client=httpx.Client(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(
                    200,
                    json=_order_payload(created_at=datetime(2025, 1, 2, tzinfo=UTC).isoformat()),
                )
            )
        ),
    )

    order = client.order_by_client_id("strategy:decision:1")

    assert order.created_at.tzinfo is not None
