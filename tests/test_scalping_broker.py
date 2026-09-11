from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx
import pytest
from test_alpaca_paper import _order_payload, _settings

from tradeagent.alpaca_paper import AlpacaPaperClient, PaperCryptoAsset, canonical_crypto_symbol
from tradeagent.domain import OrderRequest, OrderType, Side

NOW = datetime(2026, 9, 11, 7, tzinfo=UTC)


def asset(symbol="ETH/USD"):
    return PaperCryptoAsset(
        id="asset",
        symbol=symbol,
        **{"class": "crypto"},
        status="active",
        tradable=True,
        min_order_size="0.001",
        min_trade_increment="0.000001",
        price_increment="0.01",
    )


def order(**changes):
    return OrderRequest(
        **{
            "client_order_id": "ta30-fixture",
            "decision_id": "fixture",
            "strategy_id": "v30-fixture",
            "symbol": "ETH/USD",
            "side": Side.BUY,
            "quantity": Decimal("0.005"),
            "order_type": OrderType.LIMIT,
            "submitted_at": NOW,
            **changes,
        }
    )


def test_generic_crypto_assets_normalize_symbols_and_limit_orders_use_gtc():
    requests = []

    def handler(request):
        requests.append(request)
        assert request.url.host == "paper-api.alpaca.markets"
        if request.method == "GET":
            return httpx.Response(
                200, json=[{"symbol": "BTC/USDT"}, asset().model_dump(mode="json", by_alias=True)]
            )
        body = json.loads(request.content)
        assert body["time_in_force"] == "gtc" and body["type"] == "limit"
        assert body["symbol"] == "ETH/USD" and body["qty"] == "0.005"
        assert body["limit_price"] == "3000.01"
        assert "notional" not in body and "extended_hours" not in body
        return httpx.Response(
            200, json=_order_payload(symbol="ETH/USD", qty="0.005", client_order_id="ta30-fixture")
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as transport:
        broker = AlpacaPaperClient(_settings(), client=transport)
        metadata = broker.crypto_asset("ethusd")
        result = broker.submit_crypto_limit_order(order(), Decimal("3000.01"), asset=metadata)
        assert result.quantity == Decimal(".005")
    assert len(requests) == 2
    assert canonical_crypto_symbol(" btcusd ") == "BTC/USD"


@pytest.mark.parametrize(
    "bad_quantity,bad_price",
    [
        ("0.0001", "3000"),
        ("0.0010001", "3000"),
        ("0.005", "3000.001"),
    ],
)
def test_broker_increment_errors_never_post(bad_quantity, bad_price):
    calls = []

    def handler(request):
        calls.append(request)
        raise AssertionError("invalid increments must not reach broker")

    with httpx.Client(transport=httpx.MockTransport(handler)) as transport:
        broker = AlpacaPaperClient(_settings(), client=transport)
        with pytest.raises(ValueError):
            broker.submit_crypto_limit_order(
                order(quantity=Decimal(bad_quantity)), Decimal(bad_price), asset=asset()
            )
    assert calls == []


def test_crypto_market_exit_uses_quantity_gtc_and_no_quote_requirement():
    def handler(request):
        body = json.loads(request.content)
        assert body["type"] == "market" and body["time_in_force"] == "gtc"
        assert body["side"] == "sell" and body["symbol"] == "ETH/USD"
        assert "limit_price" not in body
        return httpx.Response(
            200,
            json=_order_payload(
                symbol="ETH/USD", side="sell", qty="0.005", client_order_id="ta30-fixture"
            ),
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as transport:
        broker = AlpacaPaperClient(_settings(), client=transport)
        broker.submit_crypto_market_order(
            order(side=Side.SELL, order_type=OrderType.MARKET), asset=asset()
        )


def test_incremental_activity_pages_are_not_capped_at_500():
    pages = []

    def handler(request):
        assert request.url.path == "/v2/account/activities"
        assert request.url.params["direction"] == "asc"
        start = int(request.url.params.get("page_token", "-1")) + 1
        pages.append(start)
        rows = [
            {"id": str(index), "activity_type": "FILL"}
            for index in range(start, min(start + 100, 650))
        ]
        return httpx.Response(200, json=rows)

    with httpx.Client(transport=httpx.MockTransport(handler)) as transport:
        broker = AlpacaPaperClient(_settings(), client=transport)
        result, cursor = [], None
        while True:
            page = broker.account_activity_page(
                after=NOW - timedelta(days=1), until=NOW, page_token=cursor
            )
            result.extend(page["activities"])
            if page["complete"]:
                break
            cursor = page["next_page_token"]
    assert len(result) == 650 and pages == [0, 100, 200, 300, 400, 500, 600]


def test_nonadvancing_activity_cursor_fails_instead_of_repeating_forever():
    def handler(request):
        return httpx.Response(200, json=[{"id": "same"}] * 100)

    with httpx.Client(transport=httpx.MockTransport(handler)) as transport:
        broker = AlpacaPaperClient(_settings(), client=transport)
        with pytest.raises(ValueError, match="did not advance"):
            broker.account_activity_page(
                after=NOW - timedelta(days=1), until=NOW, page_token="same"
            )
