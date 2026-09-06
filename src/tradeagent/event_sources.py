"""Read-only source acquisition with immutable local receipt and bounded requests.

News symbols are discovery hints, never issuer authority. Primary URLs must come
from trusted configuration, or SEC submission metadata under the issuer's CIK.
No article link is followed. Callers persist returned versions for restart-safe
deduplication; ``seed_evidence`` restores previously recorded receipt timestamps.
"""

from __future__ import annotations

import json
import re
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx

from tradeagent.alpaca import AlpacaDataSettings
from tradeagent.event_research import (
    IssuerMapping,
    SourceEvent,
    config_hash,
    semantic_event_key,
    supported_issuer_mappings,
    text_hash,
)


class SourceAcquisitionError(RuntimeError):
    pass


class _PlainText(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.hidden_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in {"script", "style"}:
            self.hidden_depth += 1
        if tag in {"p", "div", "br", "tr", "h1", "h2", "li"}:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag in {"script", "style"}:
            self.hidden_depth = max(0, self.hidden_depth - 1)
        if tag in {"p", "div", "tr", "h1", "h2", "li"}:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if not self.hidden_depth:
            self.parts.append(data)


def _plain_text(content: str) -> str:
    parser = _PlainText()
    parser.feed(content)
    return "\n".join(
        line
        for line in (
            re.sub(r"\s+", " ", line).strip() for line in "".join(parser.parts).splitlines()
        )
        if line
    )


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("source timestamp must be timezone-aware")
    return value.astimezone(UTC)


def _timestamp(value: object) -> datetime | None:
    if value is None or value == "":
        return None
    return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))


def _canonical_url(url: str) -> str:
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.port not in {None, 443}
        or "\\" in url
    ):
        raise ValueError("source URL must be credential-free HTTPS on port 443")
    return urlunsplit(("https", parsed.hostname.lower(), parsed.path or "/", parsed.query, ""))


def verify_primary_url(url: str, mapping: IssuerMapping) -> str:
    normalized = _canonical_url(url)
    parsed = urlsplit(normalized)
    # Do not allow encoded separators or dot-segment escapes in the trusted path.
    if "%" in parsed.path or ".." in parsed.path or "//" in parsed.path:
        raise ValueError("ambiguous primary URL path")
    if parsed.hostname == "www.sec.gov":
        if not parsed.path.startswith(f"/Archives/edgar/data/{int(mapping.cik)}/"):
            raise ValueError("SEC document CIK does not match verified issuer")
    elif parsed.hostname not in mapping.primary_domains:
        raise ValueError("primary URL host does not match independently verified issuer")
    return normalized


def _cluster_id(
    *,
    issuer_id: str | None,
    url: str,
    headline: str,
    content: str,
    published_at: datetime | None,
) -> str:
    # Copied headlines and simple punctuation/HTML syndication are one event.
    # The day bound deliberately conflates same-day repeats rather than adding exposure.
    normalized = re.sub(r"[^a-z0-9]+", " ", _plain_text(headline or content[:500]).lower()).strip()
    if not normalized:
        normalized = urlsplit(url)._replace(query="", fragment="").geturl()
    return config_hash((issuer_id, published_at.date() if published_at else None, normalized))


class EventSourceClient:
    """Alpaca news plus explicitly configured issuer releases/SEC 8-K documents.

    ``retain_news_content`` requires the account's permitted retention policy.
    It defaults off; hashes and permitted metadata remain available, but extraction
    abstains without retained text. Existing Alpaca settings are never modified.
    """

    def __init__(
        self,
        settings: AlpacaDataSettings,
        *,
        client: httpx.Client | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        primary_urls: Mapping[str, Sequence[str]] | None = None,
        sec_user_agent: str | None = None,
        retain_news_content: bool = False,
        news_rights_profile: str = "metadata_only_retention_not_authorized",
        cache_ttl_seconds: int = 60,
        max_pages: int = 5,
        max_sec_filings_per_symbol: int = 2,
        seed_evidence: Sequence[SourceEvent] = (),
    ) -> None:
        if settings.data_url.rstrip("/") != "https://data.alpaca.markets":
            raise ValueError("news credentials may only be sent to the fixed Alpaca data host")
        if cache_ttl_seconds < 1 or not 1 <= max_pages <= 20:
            raise ValueError("cache TTL and pagination bounds must be positive and bounded")
        if not 0 <= max_sec_filings_per_symbol <= 5:
            raise ValueError("SEC filing batch must be bounded")
        if retain_news_content and news_rights_profile == "metadata_only_retention_not_authorized":
            raise ValueError("full news retention requires an explicitly configured rights profile")
        if sec_user_agent is not None and (
            "@" not in sec_user_agent or len(sec_user_agent) < 12 or "\n" in sec_user_agent
        ):
            raise ValueError("SEC requests require an identifying application and contact email")
        self._settings = settings
        self._client = client or httpx.Client(timeout=20, follow_redirects=False)
        self._owns_client = client is None
        self._clock = clock
        self._monotonic = monotonic
        self._sleep = sleep
        self._mappings: dict[str, IssuerMapping] = {
            mapping.symbol: mapping for mapping in supported_issuer_mappings()
        }
        self._primary_urls: dict[str, tuple[str, ...]] = {}
        for symbol, urls in (primary_urls or {}).items():
            if symbol not in self._mappings:
                raise ValueError("primary URL configured for unsupported issuer")
            if len(urls) > 5:
                raise ValueError("at most five configured primary URLs per issuer")
            self._primary_urls[symbol] = tuple(
                verify_primary_url(url, self._mappings[symbol]) for url in urls
            )
        self._sec_user_agent = sec_user_agent
        self._retain_news_content = retain_news_content
        self._news_rights_profile = news_rights_profile
        self._cache_ttl = cache_ttl_seconds
        self._max_pages = max_pages
        self._max_sec_filings = max_sec_filings_per_symbol
        self._versions: dict[tuple[str, str, str], SourceEvent] = {}
        self._latest: dict[tuple[str, str], SourceEvent] = {}
        self._http_cache: dict[str, tuple[float, httpx.Response, datetime]] = {}
        self._last_request: dict[str, float] = {}
        self._blocked_until: dict[str, float] = {}
        self._news_entitlement = "unprobed_until_poll"
        self.last_errors: tuple[str, ...] = ()
        for event in sorted(seed_evidence, key=lambda item: item.first_received_at):
            self._remember(event)

    @property
    def capabilities(self) -> dict[str, object]:
        return {
            "news_content_requested": True,
            "news_retention_enabled": self._retain_news_content,
            "news_entitlement": self._news_entitlement,
            "historical_revision_delivery": "not_provided_no_historical_receipt_reconstruction",
            "primary_urls_configured": sum(len(urls) for urls in self._primary_urls.values()),
            "sec_enabled": self._sec_user_agent is not None,
            "point_in_time_consensus": "missing",
            "inference_provider": None,
            "inference_status": "missing_deterministic_fallback",
            "supported_symbols": tuple(self._mappings),
            "primary_rate_limit_per_second": 2,
            "news_rate_limit_per_second": 2,
            "cache_ttl_seconds": self._cache_ttl,
            "max_pages": self._max_pages,
            "last_errors": self.last_errors,
        }

    def _request(
        self,
        url: str,
        *,
        headers: dict[str, str],
        params: dict[str, str | int | bool] | None = None,
    ) -> tuple[httpx.Response, datetime]:
        key = config_hash((url, params))
        tick = self._monotonic()
        cached = self._http_cache.get(key)
        if cached is not None and tick - cached[0] < self._cache_ttl:
            return cached[1], cached[2]
        host = urlsplit(url).hostname or ""
        # SEC's two hosts share a single request budget, below its 10 requests/sec limit.
        budget = "sec.gov" if host in {"www.sec.gov", "data.sec.gov"} else host
        if tick < self._blocked_until.get(budget, 0):
            raise SourceAcquisitionError("source_rate_limited_retry_after")
        wait = 0.5 - (tick - self._last_request.get(budget, -1000))
        if wait > 0:
            self._sleep(wait)
        self._last_request[budget] = self._monotonic()
        request_headers = dict(headers)
        if cached is not None:
            if cached[1].headers.get("etag"):
                request_headers["If-None-Match"] = cached[1].headers["etag"]
            if cached[1].headers.get("last-modified"):
                request_headers["If-Modified-Since"] = cached[1].headers["last-modified"]
        response = self._client.get(
            url,
            headers=request_headers,
            params=params,
            follow_redirects=False,
            timeout=20,
        )
        received = _utc(self._clock())
        if response.status_code == 429:
            try:
                retry = min(max(float(response.headers.get("retry-after", "60")), 1), 3600)
            except ValueError:
                retry = 60
            self._blocked_until[budget] = self._monotonic() + retry
            raise SourceAcquisitionError("source_rate_limited")
        if response.status_code == 304 and cached is not None:
            self._http_cache[key] = (self._monotonic(), cached[1], cached[2])
            return cached[1], cached[2]
        if response.is_redirect:
            raise SourceAcquisitionError("source_redirect_not_authorized")
        response.raise_for_status()
        if len(response.content) > 5_000_000:
            raise SourceAcquisitionError("source_document_exceeds_size_bound")
        self._http_cache[key] = (self._monotonic(), response, received)
        return response, received

    def _remember(self, event: SourceEvent) -> SourceEvent:
        key = (event.source, event.source_event_id, event.source_version)
        existing = self._versions.get(key)
        if existing is not None:
            if (
                existing.content_sha256 != event.content_sha256
                or existing.raw_payload_sha256 != event.raw_payload_sha256
            ):
                raise SourceAcquisitionError("immutable_source_version_collision")
            return existing
        previous = self._latest.get(key[:2])
        semantic_key = semantic_event_key(event)
        if semantic_key is not None:
            data = event.model_dump()
            data["event_cluster_id"] = semantic_key
            event = SourceEvent.model_validate(data)
        if previous is not None and previous.source_version != event.source_version:
            data = event.model_dump()
            data["revision_of"] = previous.evidence_id
            data["event_cluster_id"] = previous.event_cluster_id
            event = SourceEvent.model_validate(data)
        self._versions[key] = event
        self._latest[key[:2]] = event
        return event

    def poll(
        self, *, start: datetime, end: datetime, symbols: Sequence[str]
    ) -> tuple[SourceEvent, ...]:
        start, end = _utc(start), _utc(end)
        if start >= end:
            raise ValueError("source poll interval must increase")
        requested = tuple(sorted(set(symbol.upper() for symbol in symbols)))
        if not requested or any(symbol not in self._mappings for symbol in requested):
            raise ValueError("only AAPL/MSFT/NVDA are in the fixed v20 universe")
        events: list[SourceEvent] = []
        errors: list[str] = []
        try:
            events.extend(self._news(start=start, end=end, symbols=requested))
            self._news_entitlement = "observed_available"
        except (httpx.HTTPError, ValueError, SourceAcquisitionError) as exc:
            self._news_entitlement = (
                "observed_denied"
                if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code in {401, 403}
                else "request_failed_or_incomplete"
            )
            errors.append(f"news:{type(exc).__name__}:{self._safe_error(exc)}")
        for symbol in requested:
            for url in self._primary_urls.get(symbol, ()):
                try:
                    events.append(self.fetch_primary(url=url, symbol=symbol))
                except (httpx.HTTPError, ValueError, SourceAcquisitionError) as exc:
                    errors.append(f"primary:{symbol}:{type(exc).__name__}:{self._safe_error(exc)}")
            if self._sec_user_agent is not None:
                try:
                    events.extend(self._sec(symbol=symbol, start=start, end=end))
                except (httpx.HTTPError, ValueError, SourceAcquisitionError) as exc:
                    errors.append(f"sec:{symbol}:{type(exc).__name__}:{self._safe_error(exc)}")
        self.last_errors = tuple(errors)
        unique = {event.evidence_id: event for event in events}
        return tuple(
            sorted(unique.values(), key=lambda item: (item.first_received_at, item.evidence_id))
        )

    @staticmethod
    def _safe_error(exc: Exception) -> str:
        if isinstance(exc, SourceAcquisitionError):
            return str(exc)
        if isinstance(exc, httpx.HTTPStatusError):
            return f"http_{exc.response.status_code}"
        return "source_request_or_schema_invalid"

    def _news(
        self, *, start: datetime, end: datetime, symbols: Sequence[str]
    ) -> tuple[SourceEvent, ...]:
        params: dict[str, str | int | bool] = {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "sort": "asc",
            "symbols": ",".join(symbols),
            "limit": 50,
            "include_content": True,
            "exclude_contentless": False,
        }
        tokens: set[str] = set()
        events: list[SourceEvent] = []
        for _ in range(self._max_pages):
            response, received = self._request(
                "https://data.alpaca.markets/v1beta1/news",
                headers={
                    "APCA-API-KEY-ID": self._settings.key_id.get_secret_value(),
                    "APCA-API-SECRET-KEY": self._settings.secret_key.get_secret_value(),
                },
                params=params,
            )
            payload = response.json()
            if not isinstance(payload, dict) or not isinstance(payload.get("news"), list):
                raise SourceAcquisitionError("news_array_missing")
            for row in payload["news"]:
                if not isinstance(row, dict):
                    raise SourceAcquisitionError("news_article_schema_invalid")
                required = ("id", "headline", "url", "created_at", "updated_at")
                if any(key not in row for key in required):
                    raise SourceAcquisitionError("news_article_required_metadata_missing")
                headline, raw_content = str(row["headline"]), row.get("content")
                if raw_content is not None and not isinstance(raw_content, str):
                    raise SourceAcquisitionError("news_content_schema_invalid")
                created, updated = _timestamp(row["created_at"]), _timestamp(row["updated_at"])
                raw = json.dumps(row, sort_keys=True, separators=(",", ":"))
                content = _plain_text(raw_content or "")
                version = text_hash(raw)
                source = f"alpaca:{row.get('source', 'unknown')}"
                url = str(row["url"])
                provider_symbols = row.get("symbols", [])
                if not isinstance(provider_symbols, list) or any(
                    not isinstance(symbol, str) for symbol in provider_symbols
                ):
                    raise SourceAcquisitionError("news_symbols_schema_invalid")
                retained_row = (
                    row
                    if self._retain_news_content
                    else {
                        key: value
                        for key, value in row.items()
                        if key not in {"content", "summary"}
                    }
                )
                event = SourceEvent(
                    source_event_id=str(row["id"]),
                    source=source,
                    source_url=url,
                    source_version=version,
                    published_at=created,
                    provider_created_at=created,
                    provider_updated_at=updated,
                    first_received_at=received,
                    content_available_at=received,
                    content_sha256=text_hash(content),
                    content=content if self._retain_news_content else None,
                    raw_metadata_json=json.dumps(retained_row, sort_keys=True),
                    raw_payload_sha256=text_hash(raw),
                    event_cluster_id=_cluster_id(
                        issuer_id=None,
                        url=url,
                        headline=headline,
                        content=content,
                        published_at=created,
                    ),
                    provider_symbols=tuple(provider_symbols),
                    is_correction=bool(
                        re.search(r"\bcorrect(?:ion|ed)\b", headline, re.IGNORECASE)
                    ),
                    is_retraction=bool(
                        re.search(r"\bretract(?:ion|ed)\b", headline, re.IGNORECASE)
                    ),
                    rights_profile=self._news_rights_profile,
                    headline=headline,
                )
                events.append(self._remember(event))
            token = payload.get("next_page_token")
            if not token:
                return tuple(events)
            if not isinstance(token, str) or token in tokens:
                raise SourceAcquisitionError("news_pagination_token_invalid_or_repeated")
            tokens.add(token)
            params["page_token"] = token
        raise SourceAcquisitionError("news_pagination_bound_reached_incomplete_poll")

    def fetch_primary(
        self,
        *,
        url: str,
        symbol: str,
    ) -> SourceEvent:
        """Fetch only an exact, independently configured primary URL."""
        mapping = self._mappings.get(symbol)
        if mapping is None:
            raise ValueError("unsupported primary issuer")
        normalized = verify_primary_url(url, mapping)
        if normalized not in self._primary_urls.get(symbol, ()):
            raise ValueError("primary URL is not in trusted configuration")
        return self._primary(url=normalized, mapping=mapping)

    def _primary(
        self,
        *,
        url: str,
        mapping: IssuerMapping,
        published_at: datetime | None = None,
        filing_metadata: dict[str, Any] | None = None,
    ) -> SourceEvent:
        url = verify_primary_url(url, mapping)
        if urlsplit(url).hostname == "www.sec.gov" and not self._sec_user_agent:
            raise ValueError("SEC contact identification is not configured")
        response, received = self._request(
            url,
            headers={
                "User-Agent": self._sec_user_agent or "TradeAgent-event-research/20",
                "Accept": "text/html,application/xhtml+xml,application/json,text/plain",
            },
        )
        if mapping.available_at > received or mapping.valid_from > received:
            raise SourceAcquisitionError("issuer_mapping_not_yet_available")
        raw = response.text
        content = _plain_text(raw)
        # Publication is only explicit datePublished, never HTTP Last-Modified or receipt.
        date_matches = re.findall(
            r'["\']datePublished["\']\s*:\s*["\']([^"\']+)["\']', raw, re.IGNORECASE
        )
        if published_at is None and len(set(date_matches)) == 1:
            try:
                published_at = _timestamp(date_matches[0])
            except ValueError:
                published_at = None
        metadata = {
            "url": url,
            "status": response.status_code,
            "etag": response.headers.get("etag"),
            "last_modified": response.headers.get("last-modified"),
            "raw_document": raw,
            "filing": filing_metadata,
        }
        metadata_json = json.dumps(metadata, sort_keys=True)
        version = text_hash(raw)
        headline_match = re.search(r"<h1[^>]*>(.*?)</h1>", raw, re.IGNORECASE | re.DOTALL)
        headline = _plain_text(headline_match[1]) if headline_match else ""
        is_amendment = filing_metadata is not None and str(
            filing_metadata.get("form", "")
        ).endswith("/A")
        source = "sec_edgar" if urlsplit(url).hostname == "www.sec.gov" else "issuer_primary"
        event = SourceEvent(
            source_event_id=url,
            source=source,
            source_url=url,
            source_version=version,
            published_at=published_at,
            first_received_at=received,
            content_available_at=received,
            content_sha256=text_hash(content),
            content=content,
            raw_metadata_json=metadata_json,
            raw_payload_sha256=text_hash(raw),
            event_cluster_id=_cluster_id(
                issuer_id=mapping.issuer_id,
                url=url,
                headline=headline,
                content=content,
                published_at=published_at,
            ),
            issuer_id=mapping.issuer_id,
            cik=mapping.cik,
            related_instruments=(mapping.symbol,),
            mapping_available_at=mapping.available_at,
            is_primary_source=True,
            is_correction=is_amendment
            or bool(re.search(r"\bcorrection\b", headline, re.IGNORECASE)),
            is_retraction=bool(re.search(r"\bretract(?:ed|ion)\b", headline, re.IGNORECASE)),
            rights_profile="public_primary_document_research_retention",
            headline=headline,
        )
        return self._remember(event)

    def _sec(self, *, symbol: str, start: datetime, end: datetime) -> tuple[SourceEvent, ...]:
        mapping = self._mappings[symbol]
        response, received = self._request(
            f"https://data.sec.gov/submissions/CIK{mapping.cik}.json",
            headers={"User-Agent": self._sec_user_agent or "", "Accept": "application/json"},
        )
        payload = response.json()
        if not isinstance(payload, dict):
            raise SourceAcquisitionError("SEC_submission_schema_invalid")
        if str(payload.get("cik", "")).lstrip("0") != mapping.cik.lstrip("0"):
            raise SourceAcquisitionError("SEC_submission_CIK_mismatch")
        filings = payload.get("filings")
        if not isinstance(filings, dict) or not isinstance(filings.get("recent"), dict):
            raise SourceAcquisitionError("SEC_recent_filings_schema_invalid")
        recent = filings["recent"]
        keys = ("accessionNumber", "acceptanceDateTime", "primaryDocument", "form")
        if any(not isinstance(recent.get(key), list) for key in keys):
            raise SourceAcquisitionError("SEC_recent_filings_schema_invalid")
        columns = [recent[key] for key in keys]
        if len({len(column) for column in columns}) != 1:
            raise SourceAcquisitionError("SEC_recent_filings_columns_mismatch")
        events: list[SourceEvent] = []
        for accession, accepted, document, form in zip(*columns, strict=True):
            accepted_at = _timestamp(accepted)
            if (
                accepted_at is None
                or not start <= accepted_at <= end
                or form not in {"8-K", "8-K/A"}
            ):
                continue
            if len(events) >= self._max_sec_filings:
                break
            if not re.fullmatch(r"\d{10}-\d{2}-\d{6}", str(accession)) or not re.fullmatch(
                r"[A-Za-z0-9_.-]+", str(document)
            ):
                raise SourceAcquisitionError("SEC_filing_document_path_invalid")
            url = (
                f"https://www.sec.gov/Archives/edgar/data/{int(mapping.cik)}/"
                f"{str(accession).replace('-', '')}/{document}"
            )
            # Acceptance is NOT a proven first-publication timestamp.
            events.append(
                self._primary(
                    url=url,
                    mapping=mapping,
                    filing_metadata={
                        "accession": accession,
                        "form": form,
                        "accepted_at": accepted_at.isoformat(),
                        "submission_received_at": received.isoformat(),
                        "public_availability_limitation": "acceptance_is_not_publication",
                    },
                )
            )
        return tuple(events)

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> EventSourceClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
