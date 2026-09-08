from __future__ import annotations

import gzip
import subprocess
import sys
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from pydantic import SecretStr

from tradeagent.alpaca import AlpacaDataSettings
from tradeagent.event_sources import EventSourceClient, SourceAcquisitionError

NOW = datetime(2026, 9, 8, 14, tzinfo=UTC)


def settings():
    return AlpacaDataSettings(
        key_id=SecretStr("synthetic"), secret_key=SecretStr("synthetic"), _env_file=None
    )


def test_service_cli_does_not_import_offline_machine_learning_stack():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import tradeagent.cli; "
            "assert 'sklearn' not in sys.modules; "
            "assert 'tradeagent.economic_ml' not in sys.modules; "
            "assert 'tradeagent.meta_label' not in sys.modules",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_changing_news_request_windows_cannot_grow_http_cache_without_bound():
    with httpx.Client(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, content=b"x" * 1200))
    ) as http:
        client = EventSourceClient(
            settings(),
            client=http,
            issuer_feeds_enabled=False,
            clock=lambda: NOW,
            sleep=lambda _: None,
            cache_max_bytes=10000,
            cache_max_entries=8,
        )
        for index in range(2000):
            response, received = client._request(
                "https://data.alpaca.markets/v1beta1/news", headers={}, params={"window": index}
            )
            assert len(response.content) == 1200
            assert received == NOW
        cache = client.capabilities["http_cache"]
        assert cache["entries"] <= 8
        assert cache["bytes"] <= 10000
        assert cache["evictions"] >= 1992
        assert len(client._http_verified_at) == len(client._http_cache)


def test_cache_eviction_does_not_make_old_primary_evidence_fresh():
    now = NOW
    url = "https://www.apple.com/newsroom/2026/09/synthetic/"
    body = (
        '<script type="application/ld+json">{"datePublished":"2026-09-08T13:59:59Z"}</script>'
        "<p>For fiscal 2027, GAAP revenue guidance increased from USD 100 million "
        "to USD 110 million.</p>"
    )
    with httpx.Client(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, text=body))
    ) as http:
        client = EventSourceClient(
            settings(),
            client=http,
            issuer_feeds_enabled=False,
            clock=lambda: now,
            sleep=lambda _: None,
            cache_max_entries=1,
            primary_urls={"AAPL": (url,)},
        )
        first = client.fetch_primary(url=url, symbol="AAPL")
        client._request("https://www.apple.com/newsroom/other/", headers={})
        now += timedelta(minutes=10)
        repeated = client.fetch_primary(url=url, symbol="AAPL")
        assert repeated.evidence_id == first.evidence_id
        assert repeated.first_received_at == first.first_received_at == NOW
        assert repeated.content_available_at == first.content_available_at
        assert client.capabilities["http_cache"]["evictions"] >= 2


def test_large_uncached_response_is_returned_without_retaining_its_bytes():
    with httpx.Client(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, content=b"x" * 1024))
    ) as http:
        client = EventSourceClient(
            settings(),
            client=http,
            issuer_feeds_enabled=False,
            clock=lambda: NOW,
            sleep=lambda _: None,
            cache_max_bytes=128,
        )
        response, received = client._request("https://www.apple.com/newsroom/example/", headers={})
        assert len(response.content) == 1024
        assert received == NOW
        assert client.capabilities["http_cache"]["bytes"] == 0
        assert not client._http_cache
        assert not client._http_verified_at


def test_document_size_limit_stops_download_before_buffering_the_entire_response():
    class LargeDocument(httpx.SyncByteStream):
        def __init__(self):
            self.chunks_read = 0
            self.closed = False

        def __iter__(self):
            for _ in range(1000):
                self.chunks_read += 1
                yield b"x" * 65536

        def close(self):
            self.closed = True

    document = LargeDocument()
    with httpx.Client(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, stream=document))
    ) as http:
        client = EventSourceClient(
            settings(), client=http, issuer_feeds_enabled=False, sleep=lambda _: None
        )
        with pytest.raises(SourceAcquisitionError, match="size_bound"):
            client._request("https://www.apple.com/newsroom/example/", headers={})
    assert document.chunks_read <= 78
    assert document.closed
    assert not client._http_cache


def test_bounded_download_does_not_decode_compressed_content_twice():
    payload = b'{"news":[],"next_page_token":null}'
    with httpx.Client(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                headers={"content-encoding": "gzip", "etag": "version-one"},
                content=gzip.compress(payload),
            )
        )
    ) as http:
        client = EventSourceClient(
            settings(), client=http, issuer_feeds_enabled=False, sleep=lambda _: None
        )
        response, _ = client._request("https://data.alpaca.markets/v1beta1/news", headers={})
        assert response.json() == {"news": [], "next_page_token": None}
        assert response.headers["etag"] == "version-one"
