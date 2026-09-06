from datetime import UTC, datetime, timedelta

import httpx
import pytest
from pydantic import SecretStr

from tradeagent.alpaca import AlpacaDataSettings
from tradeagent.event_sources import EventSourceClient

NOW = datetime(2026, 9, 8, 14, tzinfo=UTC)
ARTICLE = {
    "id": 1,
    "headline": "synthetic",
    "url": "https://example.org/news",
    "created_at": NOW.isoformat(),
    "updated_at": NOW.isoformat(),
    "symbols": ["AAPL"],
}


@pytest.mark.parametrize(
    "payload",
    [
        [],
        {"news": [1]},
        {"news": [{}]},
        {"news": [{**ARTICLE, "content": 42}]},
        {"news": [{**ARTICLE, "symbols": "AAPL"}]},
    ],
)
def test_invalid_news_pages_never_become_partial_success(payload):
    settings = AlpacaDataSettings(key_id=SecretStr("fixture"), secret_key=SecretStr("fixture"))
    with (
        httpx.Client(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, json=payload))
        ) as http,
        EventSourceClient(settings, client=http, clock=lambda: NOW, sleep=lambda _: None) as source,
    ):
        assert source.poll(symbols=("AAPL",), start=NOW - timedelta(minutes=1), end=NOW) == ()
        assert source.last_errors
        assert source.capabilities["news_entitlement"] == "request_failed_or_incomplete"


@pytest.mark.parametrize(
    "payload",
    [
        [],
        {"cik": "wrong"},
        {"cik": 320193},
        {"cik": 320193, "filings": {"recent": {}}},
        {
            "cik": 320193,
            "filings": {
                "recent": {
                    "accessionNumber": [],
                    "acceptanceDateTime": [],
                    "primaryDocument": [],
                    "form": ["8-K"],
                }
            },
        },
        {
            "cik": 320193,
            "filings": {
                "recent": {
                    "accessionNumber": ["0000320193-26-000001"],
                    "acceptanceDateTime": [NOW.isoformat()],
                    "primaryDocument": ["../escape.htm"],
                    "form": ["8-K"],
                }
            },
        },
    ],
)
def test_invalid_sec_identity_and_document_schema_do_not_authorize_fetch(payload):
    requests = []

    def response(request):
        requests.append(request)
        return httpx.Response(
            200, json={"news": []} if request.url.host == "data.alpaca.markets" else payload
        )

    settings = AlpacaDataSettings(key_id=SecretStr("fixture"), secret_key=SecretStr("fixture"))
    with (
        httpx.Client(transport=httpx.MockTransport(response)) as http,
        EventSourceClient(
            settings,
            client=http,
            clock=lambda: NOW,
            sleep=lambda _: None,
            sec_user_agent="TradeAgent fixture@example.org",
        ) as source,
    ):
        assert source.poll(symbols=("AAPL",), start=NOW - timedelta(minutes=1), end=NOW) == ()
        assert any(error.startswith("sec:AAPL:") for error in source.last_errors)
        assert all(request.url.host != "www.sec.gov" for request in requests)
