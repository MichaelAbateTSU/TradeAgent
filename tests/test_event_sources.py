from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any
from xml.sax.saxutils import escape

import httpx
import pytest
from pydantic import SecretStr

from tradeagent.alpaca import AlpacaDataSettings
from tradeagent.event_research import (
    SourceEvent,
    config_hash,
    extract_event,
    supported_issuer_mappings,
    text_hash,
)
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
        issuer_feeds_enabled=False,
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
        issuer_feeds_enabled=False,
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
        issuer_feeds_enabled=False,
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
        issuer_feeds_enabled=False,
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
        issuer_feeds_enabled=False,
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
        issuer_feeds_enabled=False,
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
        issuer_feeds_enabled=False,
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
        issuer_feeds_enabled=False,
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
        if request.url.path.endswith("-index.html"):
            return httpx.Response(
                200,
                text="<table><tr><td>1</td><td>Form</td>"
                '<td><a href="form8k.htm">form8k.htm</a></td><td>8-K/A</td></tr></table>',
            )
        return httpx.Response(200, text="<p>A corrected filing with no publication timestamp.</p>")

    client = EventSourceClient(
        settings(),
        issuer_feeds_enabled=False,
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
    assert sleep_calls == [0.5, 0.5]
    assert client.last_poll_stats["coverage_complete"]


def test_pagination_bounded_partial_results_not_represented_as_complete() -> None:
    client = EventSourceClient(
        settings(),
        issuer_feeds_enabled=False,
        client=httpx.Client(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(200, json={"news": [article()], "next_page_token": "same"})
            )
        ),
        clock=lambda: NOW,
        sleep=lambda _: None,
        max_pages=2,
    )
    events = client.poll(start=NOW - timedelta(minutes=1), end=NOW, symbols=("AAPL",))
    assert len(events) == 1
    assert "pagination_token" in str(client.last_errors)
    assert client.last_poll_stats["coverage_complete"] is False
    assert client.last_poll_stats["coverage_watermark"] is None
    assert client.last_poll_stats["raw_items_received"] == 2
    assert client.last_poll_stats["unique_evidence_versions"] == 1
    assert client.last_poll_stats["duplicates"] == 1


def test_future_provider_timestamp_does_not_move_actual_local_receipt() -> None:
    client = EventSourceClient(
        settings(),
        issuer_feeds_enabled=False,
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
        issuer_feeds_enabled=False,
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


def test_holiday_weekend_pagination_continues_without_skipping_or_new_receipt() -> None:
    start = datetime(2026, 9, 4, 20, tzinfo=UTC)
    end = datetime(2026, 9, 8, 13, tzinfo=UTC)
    wall = [end]
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        assert request.url.params["start"] == start.isoformat()
        assert request.url.params["end"] == end.isoformat()
        page = request.url.params.get("page_token")
        return httpx.Response(
            200,
            json={
                "news": [
                    article(
                        id=102 if page else 101,
                        created_at="2026-09-05T10:00:00Z",
                        updated_at="2026-09-05T10:00:00Z",
                    )
                ],
                "next_page_token": None if page else "second",
            },
        )

    client = EventSourceClient(
        settings(),
        issuer_feeds_enabled=False,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        clock=lambda: wall[0],
        sleep=lambda _: None,
        max_pages=1,
    )
    first = client.poll(start=start, end=end, symbols=("AAPL",))[0]
    assert client.last_poll_stats["coverage_watermark"] is None
    assert "bound_reached" in str(client.last_errors)
    wall[0] = end + timedelta(minutes=1)
    second = client.poll(start=start, end=wall[0], symbols=("AAPL",))[0]
    assert client.last_poll_stats["coverage_watermark"] == end.isoformat()
    assert client.last_poll_stats["requested_end"] == wall[0].isoformat()
    assert second.first_received_at == wall[0]
    assert first.first_received_at == end
    assert calls[-1].url.params["page_token"] == "second"
    restarted = EventSourceClient(
        settings(),
        issuer_feeds_enabled=False,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        clock=lambda: wall[0],
        sleep=lambda _: None,
        max_pages=1,
        seed_evidence=(first, second),
    )
    observed = restarted.poll(start=start, end=end, symbols=("AAPL",))[0]
    assert observed == first
    assert restarted.last_poll_stats["new_evidence_versions"] == 0


def test_news_revisions_keep_publisher_clocks_and_original_version_receipt() -> None:
    wall = [NOW]
    tick = [0.0]
    changed = [False]

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "news": [
                    article(
                        updated_at="2026-09-08T14:01:00Z" if changed[0] else "2026-09-08T13:59:59Z",
                        content=BODY.replace("110", "90") if changed[0] else BODY,
                    )
                ]
            },
        )

    client = EventSourceClient(
        settings(),
        issuer_feeds_enabled=False,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        clock=lambda: wall[0],
        monotonic=lambda: tick[0],
        sleep=lambda _: None,
    )
    first = client.poll(start=NOW - timedelta(days=4), end=NOW, symbols=("AAPL",))[0]
    changed[0], tick[0], wall[0] = True, 61, NOW + timedelta(seconds=61)
    second = client.poll(start=NOW - timedelta(days=4), end=wall[0], symbols=("AAPL",))[0]
    assert second.revision_of == first.evidence_id
    assert second.published_at == first.published_at
    assert second.provider_updated_at == NOW + timedelta(minutes=1)
    assert first.first_received_at == NOW
    assert second.first_received_at == wall[0]
    assert client.last_poll_stats["revision_versions"] == 1


@pytest.mark.parametrize("status", [401, 403, 500])
def test_source_failure_is_not_healthy_empty_coverage(status: int) -> None:
    client = EventSourceClient(
        settings(),
        issuer_feeds_enabled=False,
        client=httpx.Client(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(status, json={"message": "test-secret"})
            )
        ),
        clock=lambda: NOW,
    )
    assert not client.poll(start=NOW - timedelta(days=4), end=NOW, symbols=("AAPL",))
    stats = client.last_poll_stats
    assert stats["coverage_complete"] is False
    assert stats["coverage_watermark"] is None
    assert stats["source_health"]["news"]["http_attempts"] == 1
    assert stats["source_health"]["news"]["http_successes"] == 0
    assert stats["source_health"]["news"]["status"] == "incomplete_or_failed"
    assert "test-secret" not in json.dumps(stats)


def test_valid_earlier_rows_survive_broken_article_schema() -> None:
    client = EventSourceClient(
        settings(),
        issuer_feeds_enabled=False,
        client=httpx.Client(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(200, json={"news": [article(), {"id": 102}]})
            )
        ),
        clock=lambda: NOW,
    )
    assert len(client.poll(start=NOW - timedelta(days=4), end=NOW, symbols=("AAPL",))) == 1
    assert client.last_poll_stats["coverage_complete"] is False
    assert client.last_poll_stats["raw_items_received"] == 2
    assert "required_metadata_missing" in str(client.last_errors)


def test_metadata_rights_do_not_retain_unrecognized_provider_body_fields() -> None:
    client = EventSourceClient(
        settings(),
        issuer_feeds_enabled=False,
        client=httpx.Client(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(
                    200,
                    json={
                        "news": [
                            article(
                                article_text="licensed body",
                                nested={"text": "licensed nested text"},
                            )
                        ]
                    },
                )
            )
        ),
        clock=lambda: NOW,
    )
    event = client.poll(start=NOW - timedelta(days=4), end=NOW, symbols=("AAPL",))[0]
    assert event.content is None
    assert "licensed" not in event.raw_metadata_json
    assert event.raw_payload_sha256 is not None


def sec_client(
    *,
    exhibit_link: str = "release.htm",
    limit: int = 2,
    documents: int = 1,
    prior_status: int = 200,
) -> tuple[EventSourceClient, list[httpx.Request]]:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.host == "data.alpaca.markets":
            return httpx.Response(200, json={"news": []})
        assert "APCA-API-KEY-ID" not in request.headers
        if request.url.host == "data.sec.gov":
            return httpx.Response(
                200,
                json={
                    "cik": "320193",
                    "filings": {
                        "recent": {
                            "accessionNumber": [
                                f"0000320193-26-{index:06}" for index in range(documents)
                            ]
                            + ["0000320193-25-000999"],
                            "acceptanceDateTime": ["2026-09-08T13:00:00Z" for _ in range(documents)]
                            + ["2025-11-01T12:00:00Z"],
                            "primaryDocument": ["form8k.htm"] * documents + ["form10k.htm"],
                            "form": ["8-K"] * documents + ["10-K"],
                        }
                    },
                },
            )
        if request.url.path.endswith("-index.html"):
            return httpx.Response(
                200,
                text=(
                    '<table><tr><td>1</td><td>Form</td><td><a href="form8k.htm">Form</a></td>'
                    f'<td>8-K</td></tr><tr><td>2</td><td>Release</td><td><a href="{exhibit_link}">'
                    "Release</a></td><td>EX-99.1</td></tr></table>"
                ),
            )
        if request.url.path.endswith("release.htm"):
            return httpx.Response(200, text=BODY)
        if request.url.path.endswith("form10k.htm"):
            return httpx.Response(
                prior_status,
                text="<p>For fiscal 2025, GAAP annual revenue was USD 1000 million.</p>",
            )
        return httpx.Response(200, text="<p>Results furnished in Exhibit 99.1.</p>")

    return EventSourceClient(
        settings(),
        issuer_feeds_enabled=False,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        clock=lambda: NOW,
        monotonic=lambda: 100,
        sleep=lambda _: None,
        sec_user_agent="TradeAgent research ops@example.test",
        max_sec_filings_per_symbol=limit,
    ), requests


def test_sec_exhibits_and_older_comparison_use_only_typed_verified_index() -> None:
    client, requests = sec_client()
    result = client.poll(start=NOW - timedelta(days=4), end=NOW, symbols=("AAPL",))
    assert len(result) == 3
    exhibit = next(event for event in result if event.source_url.endswith("release.htm"))
    assert extract_event(exhibit, now=NOW).reason_for_abstention is None
    filing = json.loads(exhibit.raw_metadata_json)["filing"]
    assert filing["document_type"] == "EX-99.1"
    assert filing["index_url"].endswith("0000320193-26-000000-index.html")
    older = next(event for event in result if event.source_url.endswith("form10k.htm"))
    assert older.published_at is None
    assert older.first_received_at == NOW
    assert json.loads(older.raw_metadata_json)["filing"]["collection_role"] == "older_comparison"
    assert client.last_poll_stats["coverage_complete"] is True
    assert client.last_poll_stats["source_health"]["sec:AAPL"]["exhibits_discovered"] == 1
    assert len(requests) == 6


@pytest.mark.parametrize(
    "link",
    [
        "https://attacker.invalid/steal",
        "https://www.sec.gov/Archives/edgar/data/789019/wrong/release.htm",
        "../release.htm",
        "/Archives/edgar/data/320193/999999999999999999/release.htm",
    ],
)
def test_sec_index_cannot_authorize_other_issuer_or_filing(link: str) -> None:
    client, requests = sec_client(exhibit_link=link)
    result = client.poll(start=NOW - timedelta(days=4), end=NOW, symbols=("AAPL",))
    assert result
    assert not any(request.url.path.endswith("release.htm") for request in requests)
    assert client.last_poll_stats["coverage_complete"] is False
    assert "exhibits:" in str(client.last_errors)


def test_sec_bounded_batch_continues_pending_filings_without_false_watermark() -> None:
    client, _ = sec_client(limit=1, documents=2)
    result = client.poll(start=NOW - timedelta(days=4), end=NOW, symbols=("AAPL",))
    assert len(result) == 3
    assert client.last_poll_stats["coverage_complete"] is False
    assert "filing_batch_bound" in str(client.last_errors)
    more = client.poll(start=NOW - timedelta(days=4), end=NOW, symbols=("AAPL",))
    assert len(more) == 3
    assert client.last_poll_stats["coverage_complete"] is True
    assert len({event.evidence_id for event in (*result, *more)}) == 5


def test_optional_prior_document_failure_is_explicit_and_does_not_stall_interval() -> None:
    client, requests = sec_client(prior_status=503)
    result = client.poll(start=NOW - timedelta(days=4), end=NOW, symbols=("AAPL",))
    assert len(result) == 2
    assert client.last_poll_stats["coverage_complete"] is True
    health = client.last_poll_stats["source_health"]["sec:AAPL"]
    assert health["prior_comparison_status"] == "unavailable_context_request_failed"
    assert health["prior_comparison_error"] == "http_503"
    client.poll(start=NOW - timedelta(days=4), end=NOW, symbols=("AAPL",))
    assert sum(request.url.path.endswith("form10k.htm") for request in requests) == 1
    assert (
        client.last_poll_stats["source_health"]["sec:AAPL"]["prior_comparison_error"] == "http_503"
    )


def test_primary_explicit_revision_clock_does_not_substitute_http_last_modified() -> None:
    client = EventSourceClient(
        settings(),
        issuer_feeds_enabled=False,
        client=httpx.Client(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(
                    200,
                    text=BODY + '<script>{"dateModified":"2026-09-08T14:00:00Z"}</script>',
                    headers={"last-modified": "Tue, 08 Sep 2026 15:00:00 GMT"},
                )
            )
        ),
        clock=lambda: NOW,
        primary_urls={"AAPL": (URL,)},
    )
    event = client.fetch_primary(url=URL, symbol="AAPL")
    assert event.published_at == NOW - timedelta(seconds=1)
    assert event.provider_updated_at == NOW
    assert event.first_received_at == NOW
    raw = json.loads(event.raw_metadata_json)
    assert raw["revision_timestamp_basis"] == "explicit_dateModified"
    assert raw["last_modified"] == "Tue, 08 Sep 2026 15:00:00 GMT"


NVDA_URL = "https://nvidianews.nvidia.com/news/outlook"
NVDA_BODY = BODY.replace("Apple", "NVIDIA")


def rss_item(
    url: str = NVDA_URL,
    *,
    published: str = "Tue, 08 Sep 2026 13:59:59 GMT",
    updated: str = "Tue, 08 Sep 2026 13:59:59 GMT",
    headline: str = "NVIDIA updates outlook",
) -> str:
    return (
        f"<item><guid>{escape(url)}</guid><link>{escape(url)}</link>"
        f"<title>{escape(headline)}</title>"
        f"<pubDate>{published}</pubDate><modDate>{updated}</modDate></item>"
    )


def rss_feed(*items: str, history: bool = True) -> str:
    older = (
        rss_item(
            "https://nvidianews.nvidia.com/news/old",
            published="Thu, 03 Sep 2026 12:00:00 GMT",
            updated="Thu, 03 Sep 2026 12:00:00 GMT",
        )
        if history
        else ""
    )
    return "<rss version='2.0'><channel>" + "".join(items) + older + "</channel></rss>"


def feed_client(
    content: list[str],
    *,
    symbol: str = "NVDA",
    body: list[str] | None = None,
    wall: list[datetime] | None = None,
    ticks: list[float] | None = None,
    maximum: int = 5,
    seed_evidence: tuple[SourceEvent, ...] = (),
) -> tuple[EventSourceClient, list[httpx.Request]]:
    calls: list[httpx.Request] = []
    body = body or [NVDA_BODY]
    wall = wall or [NOW]
    ticks = ticks or [0.0]
    feed_url = {
        "AAPL": "https://www.apple.com/newsroom/rss-feed.rss",
        "MSFT": "https://www.microsoft.com/en-us/microsoft-cloud/blog/feed/",
        "NVDA": "https://nvidianews.nvidia.com/rss.xml",
    }[symbol]

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if request.url.host == "data.alpaca.markets":
            return httpx.Response(200, json={"news": []})
        assert "APCA-API-KEY-ID" not in request.headers
        assert request.url.host in {"www.apple.com", "www.microsoft.com", "nvidianews.nvidia.com"}
        if str(request.url) == feed_url:
            return httpx.Response(200, text=content[0])
        return httpx.Response(200, text=body[0])

    return EventSourceClient(
        settings(),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        clock=lambda: wall[0],
        monotonic=lambda: ticks[0],
        sleep=lambda _: None,
        max_feed_documents_per_symbol=maximum,
        seed_evidence=seed_evidence,
    ), calls


def test_official_rss_publication_and_provenance_work_without_configured_primary_urls() -> None:
    feed = [rss_feed(rss_item())]
    client, calls = feed_client(feed)
    result = client.poll(start=NOW - timedelta(days=4), end=NOW, symbols=("NVDA",))
    assert len(result) == 1
    event = result[0]
    assert event.source == "issuer_primary:official_feed"
    assert event.is_primary_source and event.related_instruments == ("NVDA",)
    assert event.published_at == NOW - timedelta(seconds=1)
    assert event.first_received_at == NOW
    assert event.provider_updated_at == NOW - timedelta(seconds=1)
    assert extract_event(event, now=NOW).reason_for_abstention is None
    metadata = json.loads(event.raw_metadata_json)
    assert metadata["publication_timestamp_basis"] == "official_feed_item_publication"
    assert metadata["official_feed"]["published_raw"] == "Tue, 08 Sep 2026 13:59:59 GMT"
    assert metadata["official_feed"]["feed_url"] == "https://nvidianews.nvidia.com/rss.xml"
    assert metadata["official_feed"]["feed_sha256"] == text_hash(feed[0])
    assert metadata["raw_document"] == NVDA_BODY
    assert client.capabilities["primary_urls_configured"] == 0
    assert client.last_poll_stats["coverage_complete"] is True
    assert len(calls) == 3


def test_feed_only_revision_of_cached_document_uses_changed_feed_observation() -> None:
    feed, wall, ticks = [rss_feed(rss_item())], [NOW], [0.0]
    client, calls = feed_client(feed, wall=wall, ticks=ticks)
    first = client.poll(start=NOW - timedelta(days=4), end=NOW, symbols=("NVDA",))[0]
    # Keep the article cached while forcing a new feed observation.
    client._http_cache.pop(config_hash(("https://nvidianews.nvidia.com/rss.xml", None)))
    feed[0] = rss_feed(
        rss_item(updated="Tue, 08 Sep 2026 14:00:30 GMT", headline="Correction: NVIDIA outlook")
    )
    wall[0] = NOW + timedelta(seconds=61)
    revised = client.poll(start=NOW - timedelta(days=4), end=wall[0], symbols=("NVDA",))[0]
    assert len([call for call in calls if str(call.url) == NVDA_URL]) == 1
    assert revised.content_sha256 == first.content_sha256
    assert revised.revision_of == first.evidence_id
    assert revised.published_at == first.published_at
    assert revised.provider_updated_at == NOW + timedelta(seconds=30)
    assert revised.first_received_at == wall[0]
    assert revised.content_available_at == wall[0]
    assert first.first_received_at == NOW
    again = client.poll(
        start=NOW - timedelta(days=3), end=wall[0] + timedelta(seconds=10), symbols=("NVDA",)
    )[0]
    assert again.first_received_at == revised.first_received_at
    assert again.evidence_id == revised.evidence_id


def test_atom_updated_and_date_only_document_are_not_publication_timestamps() -> None:
    content = [
        (
            '<feed xmlns="http://www.w3.org/2005/Atom"><entry><id>new</id>'
            f'<title>Apple outlook</title><link href="{URL}"/>'
            "<updated>2026-09-08T13:59:59Z</updated></entry><entry><id>old</id>"
            '<link href="https://www.apple.com/newsroom/older/"/>'
            "<updated>2026-09-03T12:00:00Z</updated></entry></feed>"
        )
    ]
    client, _ = feed_client(
        content, symbol="AAPL", body=[BODY.replace("2026-09-08T13:59:59Z", "2026-09-08Z")]
    )
    event = client.poll(start=NOW - timedelta(days=4), end=NOW, symbols=("AAPL",))[0]
    assert event.published_at is None
    assert event.provider_updated_at == NOW - timedelta(seconds=1)
    assert event.first_received_at == NOW
    metadata = json.loads(event.raw_metadata_json)
    assert metadata["publication_timestamp_basis"] == "unknown"
    assert (
        metadata["official_feed"]["publication_basis"] == "unknown_feed_updated_is_not_publication"
    )
    assert metadata["document_publication_date_values"] == ["2026-09-08Z"]
    health = client.capabilities["issuer_feed_status"]["AAPL"]
    assert health["publication_timestamp_status"].startswith("feed_publication_missing")


@pytest.mark.parametrize(
    "url",
    [
        "https://attacker.invalid/override",
        "https://blogs.nvidia.com/blog/official-but-not-allowlisted",
        "https://nvidianews.nvidia.com.attacker.invalid/news",
        "http://nvidianews.nvidia.com/news/insecure",
        "https://nvidianews.nvidia.com/%2e%2e/private",
    ],
)
def test_official_feed_cannot_expand_verified_issuer_url_allowlist(url: str) -> None:
    client, calls = feed_client([rss_feed(rss_item(url))])
    assert not client.poll(start=NOW - timedelta(days=4), end=NOW, symbols=("NVDA",))
    assert len(calls) == 2
    health = client.last_poll_stats["source_health"]["issuer_feed:NVDA"]
    assert health["rejected_link_count"] == 1
    assert health["scope"] == "current_feed_allowlisted_article_URLs_only_no_archive_pagination"


def test_feed_revision_and_restart_preserve_version_receipt_and_publication() -> None:
    feed, body, wall, ticks = [rss_feed(rss_item())], [NVDA_BODY], [NOW], [0.0]
    client, _ = feed_client(feed, body=body, wall=wall, ticks=ticks)
    first = client.poll(start=NOW - timedelta(days=4), end=NOW, symbols=("NVDA",))[0]
    feed[0] = rss_feed(rss_item(updated="Tue, 08 Sep 2026 14:00:30 GMT"))
    body[0], wall[0], ticks[0] = NVDA_BODY.replace("110", "90"), NOW + timedelta(seconds=61), 61
    revised = client.poll(start=NOW - timedelta(days=4), end=wall[0], symbols=("NVDA",))[0]
    assert revised.revision_of == first.evidence_id
    assert revised.published_at == first.published_at
    assert revised.first_received_at == wall[0]
    assert first.first_received_at == NOW
    assert revised.provider_updated_at == NOW + timedelta(seconds=30)
    assert client.last_poll_stats["revision_versions"] == 1
    restarted, _ = feed_client(
        feed, body=body, wall=[NOW + timedelta(minutes=5)], seed_evidence=(first, revised)
    )
    observed = restarted.poll(start=NOW - timedelta(days=4), end=wall[0], symbols=("NVDA",))[0]
    assert observed == revised


def test_feed_bound_and_finite_history_never_make_false_coverage_claim() -> None:
    client, _ = feed_client(
        [rss_feed(rss_item(), rss_item("https://nvidianews.nvidia.com/news/outlook-copy"))],
        maximum=1,
    )
    first = client.poll(start=NOW - timedelta(days=4), end=NOW, symbols=("NVDA",))
    assert len(first) == 1 and not client.last_poll_stats["coverage_complete"]
    assert "document_bound" in str(client.last_errors)
    assert len(client.poll(start=NOW - timedelta(days=4), end=NOW, symbols=("NVDA",))) == 1
    assert client.last_poll_stats["coverage_complete"]
    short, _ = feed_client([rss_feed(rss_item(), history=False)])
    assert len(short.poll(start=NOW - timedelta(days=4), end=NOW, symbols=("NVDA",))) == 1
    assert short.last_poll_stats["coverage_watermark"] is None
    assert "history_does_not_reach" in str(short.last_errors)


def test_cached_feed_does_not_certify_a_later_unobserved_interval() -> None:
    wall = [NOW]
    client, _ = feed_client([rss_feed(rss_item())], wall=wall)
    client.poll(start=NOW - timedelta(days=4), end=NOW, symbols=("NVDA",))
    wall[0] = NOW + timedelta(seconds=10)
    client.poll(start=NOW - timedelta(days=4), end=wall[0], symbols=("NVDA",))
    assert client.last_poll_stats["coverage_watermark"] == NOW.isoformat()
    assert client.last_poll_stats["source_health"]["issuer_feed:NVDA"]["cache_hits"] == 1


@pytest.mark.parametrize(
    "feed,error",
    [
        ("<!DOCTYPE rss [<!ENTITY unsafe 'data'>]><rss/>", "DTD_or_entity"),
        (rss_feed(rss_item(published="not a date")), "timestamp_invalid"),
        ("<html>not a feed</html>", "format_unsupported"),
    ],
)
def test_invalid_official_feed_is_an_explicit_failure(feed: str, error: str) -> None:
    client, calls = feed_client([feed])
    assert not client.poll(start=NOW - timedelta(days=4), end=NOW, symbols=("NVDA",))
    assert not client.last_poll_stats["coverage_complete"]
    assert error in str(client.last_errors)
    assert len(calls) == 2


def test_feed_and_document_disagreeing_on_publication_time_abstain() -> None:
    client, _ = feed_client(
        [rss_feed(rss_item())],
        body=[NVDA_BODY.replace("2026-09-08T13:59:59Z", "2026-09-08T13:59:58Z")],
    )
    assert not client.poll(start=NOW - timedelta(days=4), end=NOW, symbols=("NVDA",))
    assert "publication_timestamp_conflict" in str(client.last_errors)


def test_microsoft_cloud_blog_feed_keeps_corporate_coverage_gap_explicit() -> None:
    url = "https://www.microsoft.com/en-us/microsoft-cloud/blog/2026/09/08/outlook/"
    client, _ = feed_client(
        [rss_feed(rss_item(url, headline="Microsoft updates outlook"))],
        symbol="MSFT",
        body=[BODY.replace("Apple", "Microsoft")],
    )
    event = client.poll(start=NOW - timedelta(days=4), end=NOW, symbols=("MSFT",))[0]
    assert event.related_instruments == ("MSFT",)
    assert event.published_at == NOW - timedelta(seconds=1)
    health = client.capabilities["issuer_feed_status"]["MSFT"]
    assert health["status"] == "healthy"
    assert "cloud_blog_feed_only_not_full_IR" in health["gap"]
    assert "returned_404" in health["gap"]
    assert "news.microsoft.com_outside_existing_allowlist" in health["gap"]


def test_atom_published_is_used_but_enclosure_is_never_a_document_url() -> None:
    feed = [
        (
            '<feed xmlns="http://www.w3.org/2005/Atom"><entry><id>apple</id>'
            "<title>Apple updates outlook</title>"
            f'<link rel="alternate" href="{URL}"/>'
            '<link rel="enclosure" href="https://attacker.invalid/ignored"/>'
            "<published>2026-09-08T13:59:59Z</published><updated>2026-09-08T13:59:59Z</updated>"
            '</entry><entry><id>old</id><link href="https://www.apple.com/newsroom/old/"/>'
            "<updated>2026-09-03T12:00:00Z</updated></entry></feed>"
        )
    ]
    client, calls = feed_client(
        feed, symbol="AAPL", body=[BODY.replace("2026-09-08T13:59:59Z", "2026-09-08Z")]
    )
    event = client.poll(start=NOW - timedelta(days=4), end=NOW, symbols=("AAPL",))[0]
    assert event.published_at == NOW - timedelta(seconds=1)
    assert len(calls) == 3
    assert json.loads(event.raw_metadata_json)["publication_timestamp_basis"] == (
        "official_feed_item_publication"
    )


def test_feed_title_only_correction_is_a_new_version_without_rewriting_first_receipt() -> None:
    feed, wall, ticks = [rss_feed(rss_item())], [NOW], [0.0]
    client, _ = feed_client(feed, wall=wall, ticks=ticks)
    first = client.poll(start=NOW - timedelta(days=4), end=NOW, symbols=("NVDA",))[0]
    feed[0] = rss_feed(rss_item(headline="Correction: NVIDIA updates outlook"))
    wall[0], ticks[0] = NOW + timedelta(seconds=61), 61
    correction = client.poll(start=NOW - timedelta(days=4), end=wall[0], symbols=("NVDA",))[0]
    assert correction.is_correction
    assert correction.revision_of == first.evidence_id
    assert correction.first_received_at == wall[0]
    assert first.first_received_at == NOW
    assert correction.content_sha256 == first.content_sha256
    assert correction.published_at == first.published_at
