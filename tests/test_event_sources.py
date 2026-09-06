from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from pydantic import SecretStr

from tradeagent.alpaca import AlpacaDataSettings
from tradeagent.event_research import extract_event, supported_issuer_mappings, text_hash
from tradeagent.event_sources import EventSourceClient, SourceAcquisitionError, verify_primary_url

NOW = datetime(2026, 9, 8, 14, tzinfo=UTC)
URL = "https://www.apple.com/newsroom/2026/09/outlook/"
BODY = (
    '<script type="application/ld+json">{"datePublished":"2026-09-08T13:59:59Z"}</script>'
    "<h1>Apple updates outlook</h1>"
    "<p>For fiscal 2027, GAAP revenue guidance increased "
    "from USD 100 million to USD 110 million.</p>"
)


def settings() -> AlpacaDataSettings:
    return AlpacaDataSettings(key_id=SecretStr("test-key"), secret_key=SecretStr("test-secret"))


def article(**updates: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "id": 101,
        "created_at": "2026-09-08T13:59:59Z",
        "updated_at": "2026-09-08T13:59:59Z",
        "headline": "Apple updates outlook",
        "content": BODY,
        "source": "benzinga",
        "url": "https://news.invalid/aapl-outlook",
        "symbols": ["AAPL", "MSFT"],
    }
    row.update(updates)
    return row


def test_actual_alpaca_metadata_and_receipt_without_symbol_authority() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"news": [article()], "next_page_token": None})

    client = EventSourceClient(
        settings(),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        clock=lambda: NOW,
        retain_news_content=True,
        news_rights_profile="synthetic_test_permitted",
    )
    result = client.poll(start=NOW - timedelta(minutes=1), end=NOW, symbols=("AAPL",))
    assert len(result) == 1
    event = result[0]
    assert requests[0].url.host == "data.alpaca.markets"
    assert requests[0].url.params["include_content"] == "true"
    assert requests[0].headers["APCA-API-KEY-ID"] == "test-key"
    assert event.source_event_id == "101"
    assert event.first_received_at == NOW
    assert event.content_available_at == NOW
    assert event.published_at == NOW - timedelta(seconds=1)
    assert event.provider_updated_at == NOW - timedelta(seconds=1)
    assert event.provider_symbols == ("AAPL", "MSFT")
    assert event.issuer_id is None
    assert event.related_instruments == ()
    assert not event.is_primary_source
    assert event.content is not None and event.content_sha256 == text_hash(event.content)
    assert json.loads(event.raw_metadata_json)["content"] == BODY
    assert "verified_primary_issuer" in extract_event(event, now=NOW).missing_required_fields
    assert client.capabilities["inference_provider"] is None
    assert client.capabilities["news_entitlement"] == "observed_available"


def test_default_news_retention_preserves_hash_but_not_unlicensed_body() -> None:
    client = EventSourceClient(
        settings(),
        client=httpx.Client(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(200, json={"news": [article(summary="secret body")]})
            )
        ),
        clock=lambda: NOW,
    )
    event = client.poll(start=NOW - timedelta(minutes=1), end=NOW, symbols=("AAPL",))[0]
    assert event.content is None
    assert "content" not in json.loads(event.raw_metadata_json)
    assert "summary" not in json.loads(event.raw_metadata_json)
    assert len(event.content_sha256) == 64
    assert "retained_source_content" in extract_event(event, now=NOW).missing_required_fields
    with pytest.raises(ValueError, match="rights"):
        EventSourceClient(settings(), retain_news_content=True)


def test_versions_cache_and_restart_seed_keep_first_receipt_and_corrections() -> None:
    tick = [0.0]
    wall = [NOW]
    body = [BODY]
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, text=body[0], headers={"etag": text_hash(body[0])})

    http = httpx.Client(transport=httpx.MockTransport(handler))
    client = EventSourceClient(
        settings(),
        client=http,
        clock=lambda: wall[0],
        monotonic=lambda: tick[0],
        primary_urls={"AAPL": (URL,)},
        sleep=lambda _: None,
        cache_ttl_seconds=60,
    )
    first = client.fetch_primary(url=URL, symbol="AAPL")
    tick[0], wall[0] = 20, NOW + timedelta(seconds=20)
    assert client.fetch_primary(url=URL, symbol="AAPL") == first
    assert len(calls) == 1
    tick[0], wall[0] = 65, NOW + timedelta(seconds=65)
    assert client.fetch_primary(url=URL, symbol="AAPL") == first
    assert calls[-1].headers["If-None-Match"] == text_hash(BODY)
    body[0] = BODY.replace("110", "90")
    tick[0], wall[0] = 130, NOW + timedelta(seconds=130)
    corrected = client.fetch_primary(url=URL, symbol="AAPL")
    assert corrected.revision_of == first.evidence_id
    assert corrected.event_cluster_id == first.event_cluster_id
    assert corrected.content_sha256 != first.content_sha256
    assert corrected.first_received_at == wall[0]
    assert first.content is not None and "110" in first.content
    restarted = EventSourceClient(
        settings(),
        client=http,
        clock=lambda: NOW + timedelta(minutes=10),
        primary_urls={"AAPL": (URL,)},
        seed_evidence=(first, corrected),
    )
    assert restarted.fetch_primary(url=URL, symbol="AAPL") == corrected


def test_not_modified_response_preserves_earlier_observed_availability() -> None:
    tick = [0.0]
    calls = [0]

    def handler(request: httpx.Request) -> httpx.Response:
        calls[0] += 1
        if calls[0] == 1:
            return httpx.Response(200, text=BODY, headers={"etag": "1"})
        assert request.headers["If-None-Match"] == "1"
        return httpx.Response(304)

    client = EventSourceClient(
        settings(),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        clock=lambda: NOW + timedelta(seconds=tick[0]),
        monotonic=lambda: tick[0],
        primary_urls={"AAPL": (URL,)},
        sleep=lambda _: None,
    )
    first = client.fetch_primary(url=URL, symbol="AAPL")
    tick[0] = 61
    assert client.fetch_primary(url=URL, symbol="AAPL") == first


def test_primary_source_verified_and_no_credentials_or_article_link_authorization() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, text=BODY)

    client = EventSourceClient(
        settings(),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        clock=lambda: NOW,
        primary_urls={"AAPL": (URL,)},
    )
    event = client.fetch_primary(url=URL, symbol="AAPL")
    assert event.issuer_id == "sec:0000320193"
    assert event.cik == "0000320193"
    assert event.is_primary_source
    assert event.published_at == NOW - timedelta(seconds=1)
    assert event.first_received_at == NOW
    assert event.related_instruments == ("AAPL",)
    assert extract_event(event, now=NOW).reason_for_abstention is None
    assert "APCA-API-KEY-ID" not in requests[0].headers
    assert "APCA-API-SECRET-KEY" not in requests[0].headers
    for url in (
        "https://attacker.invalid/exfiltrate",
        "https://www.apple.com/newsroom/not-configured",
        "https://www.apple.com.attacker.invalid/",
        "https://127.0.0.1/",
        "http://www.apple.com/",
        "https://www.apple.com:444/",
        "https://user:pass@www.apple.com/",
        "https://www.apple.com/%2e%2e/else",
    ):
        with pytest.raises(ValueError):
            client.fetch_primary(url=url, symbol="AAPL")
    assert len(requests) == 1


def test_redirect_and_unidentified_sec_requests_never_followed() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(302, headers={"location": "http://169.254.169.254/metadata"})

    client = EventSourceClient(
        settings(),
        client=httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=True),
        clock=lambda: NOW,
        primary_urls={"AAPL": (URL,)},
    )
    with pytest.raises(SourceAcquisitionError, match="redirect"):
        client.fetch_primary(url=URL, symbol="AAPL")
    assert len(requests) == 1
    with pytest.raises(ValueError, match="contact"):
        EventSourceClient(settings(), sec_user_agent="anonymous")
    with pytest.raises(ValueError, match="CIK"):
        verify_primary_url(
            "https://www.sec.gov/Archives/edgar/data/789019/wrong.htm",
            supported_issuer_mappings()[0],
        )


def test_rate_limit_retry_after_and_redacted_diagnostics() -> None:
    requests: list[httpx.Request] = []
    tick = [0.0]

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(429, headers={"retry-after": "120"})

    client = EventSourceClient(
        settings(),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        clock=lambda: NOW,
        monotonic=lambda: tick[0],
        sleep=lambda _: None,
    )
    for offset in (0, 1, 2):
        tick[0] = float(offset)
        assert client.poll(start=NOW - timedelta(minutes=1), end=NOW, symbols=("AAPL",)) == ()
    assert len(requests) == 1
    assert "source_rate_limited" in str(client.last_errors)
    assert "test-secret" not in str(client.capabilities)
    tick[0] = 121
    client.poll(start=NOW - timedelta(minutes=1), end=NOW, symbols=("AAPL",))
    assert len(requests) == 2


def test_sec_documents_from_metadata_not_acceptance_as_publication() -> None:
    requests: list[httpx.Request] = []
    sleep_calls: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.host == "data.alpaca.markets":
            return httpx.Response(200, json={"news": []})
        assert request.headers["User-Agent"] == "TradeAgent research ops@example.test"
        assert "APCA-API-KEY-ID" not in request.headers
        if request.url.host == "data.sec.gov":
            return httpx.Response(
                200,
                json={
                    "cik": "320193",
                    "filings": {
                        "recent": {
                            "accessionNumber": ["0000320193-26-000101"],
                            "acceptanceDateTime": ["2026-09-08T13:59:00Z"],
                            "primaryDocument": ["form8k.htm"],
                            "form": ["8-K/A"],
                        }
                    },
                },
            )
        return httpx.Response(200, text="<p>A corrected filing with no publication timestamp.</p>")

    client = EventSourceClient(
        settings(),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        clock=lambda: NOW,
        monotonic=lambda: 100,
        sleep=sleep_calls.append,
        sec_user_agent="TradeAgent research ops@example.test",
    )
    events = client.poll(start=NOW - timedelta(minutes=5), end=NOW, symbols=("AAPL",))
    assert len(events) == 1
    event = events[0]
    assert event.source == "sec_edgar"
    assert event.is_correction
    assert event.published_at is None
    assert event.first_received_at == NOW
    assert event.content_available_at == NOW
    assert "/Archives/edgar/data/320193/000032019326000101/form8k.htm" in event.source_url
    assert (
        json.loads(event.raw_metadata_json)["filing"]["accepted_at"] == "2026-09-08T13:59:00+00:00"
    )
    assert sleep_calls == [0.5]


def test_pagination_bounded_partial_results_not_represented_as_complete() -> None:
    client = EventSourceClient(
        settings(),
        client=httpx.Client(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(200, json={"news": [article()], "next_page_token": "same"})
            )
        ),
        clock=lambda: NOW,
        sleep=lambda _: None,
        max_pages=2,
    )
    assert client.poll(start=NOW - timedelta(minutes=1), end=NOW, symbols=("AAPL",)) == ()
    assert "pagination_token" in str(client.last_errors)


def test_future_provider_timestamp_does_not_move_actual_local_receipt() -> None:
    client = EventSourceClient(
        settings(),
        client=httpx.Client(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(
                    200,
                    json={
                        "news": [
                            article(
                                created_at="2026-09-08T15:00:00Z", updated_at="2026-09-08T15:00:00Z"
                            )
                        ]
                    },
                )
            )
        ),
        clock=lambda: NOW,
        retain_news_content=True,
        news_rights_profile="synthetic_test",
    )
    event = client.poll(start=NOW - timedelta(minutes=1), end=NOW, symbols=("AAPL",))[0]
    assert event.first_received_at == NOW
    assert event.published_at == NOW + timedelta(hours=1)
    assert "provider_timestamp_in_future" in extract_event(event, now=NOW).contradictions


def test_invalid_intervals_and_fixed_universe() -> None:
    client = EventSourceClient(settings())
    with pytest.raises(ValueError, match="increase"):
        client.poll(start=NOW, end=NOW, symbols=("AAPL",))
    with pytest.raises(ValueError, match="fixed"):
        client.poll(start=NOW - timedelta(minutes=1), end=NOW, symbols=("SPY",))
    with pytest.raises(ValueError, match="aware"):
        client.poll(start=NOW.replace(tzinfo=None), end=NOW, symbols=("AAPL",))
    client.close()


def test_near_duplicate_primary_stories_share_semantic_event_cluster() -> None:
    second_url = "https://www.apple.com/newsroom/2026/09/outlook-reprint/"
    client = EventSourceClient(
        settings(),
        client=httpx.Client(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(
                    200, text=BODY.replace("Apple updates outlook", request.url.path)
                )
            )
        ),
        clock=lambda: NOW,
        sleep=lambda _: None,
        primary_urls={"AAPL": (URL, second_url)},
    )
    first = client.fetch_primary(url=URL, symbol="AAPL")
    second = client.fetch_primary(url=second_url, symbol="AAPL")
    assert first.evidence_id != second.evidence_id
    assert first.event_cluster_id == second.event_cluster_id
