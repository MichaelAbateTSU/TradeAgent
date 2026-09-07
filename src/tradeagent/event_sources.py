"""Read-only source acquisition with immutable local receipt and bounded requests.

News symbols are discovery hints, never issuer authority. Primary URLs must come
from trusted configuration, fixed official feeds, or SEC metadata under the issuer's CIK.
No article link is followed. Callers persist returned versions for restart-safe
deduplication; ``seed_evidence`` restores previously recorded receipt timestamps.
"""

from __future__ import annotations

import json
import re
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4
from xml.etree import ElementTree

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


OFFICIAL_ISSUER_FEEDS = {
    "AAPL": "https://www.apple.com/newsroom/rss-feed.rss",
    "MSFT": "https://www.microsoft.com/en-us/microsoft-cloud/blog/feed/",
    "NVDA": "https://nvidianews.nvidia.com/rss.xml",
}
ISSUER_FEED_GAPS = {
    "MSFT": (
        "verified_cloud_blog_feed_only_not_full_IR_or_corporate_news:"
        "https://www.microsoft.com/en-us/Investor/rss/investor.rss_returned_404;"
        "current_IR_site_links_news.microsoft.com_outside_existing_allowlist"
    ),
}


@dataclass(frozen=True)
class _FeedEntry:
    identifier: str
    url: str
    headline: str
    published: datetime | None
    updated: datetime | None
    published_raw: str | None
    updated_raw: str | None


def _feed_entries(content: bytes) -> tuple[_FeedEntry, ...]:
    if re.search(rb"<!\s*(?:DOCTYPE|ENTITY)\b", content, re.IGNORECASE):
        raise SourceAcquisitionError("issuer_feed_DTD_or_entity_not_permitted")
    try:
        root = ElementTree.fromstring(content)
    except ElementTree.ParseError as error:
        raise SourceAcquisitionError("issuer_feed_XML_invalid") from error
    atom = root.tag == "{http://www.w3.org/2005/Atom}feed"
    if not atom and root.tag != "rss":
        raise SourceAcquisitionError("issuer_feed_format_unsupported")
    nodes = (
        root.findall("{http://www.w3.org/2005/Atom}entry")
        if atom
        else root.findall("./channel/item")
    )
    if len(nodes) > 200:
        raise SourceAcquisitionError("issuer_feed_item_bound_exceeded")

    def text(node: ElementTree.Element, name: str) -> str | None:
        value = node.findtext(f"{{http://www.w3.org/2005/Atom}}{name}" if atom else name)
        return value.strip() if value and value.strip() else None

    def timestamp(raw: str | None) -> datetime | None:
        if raw is None:
            return None
        try:
            return _timestamp(raw) if atom else _utc(parsedate_to_datetime(raw))
        except (ValueError, TypeError, OverflowError) as error:
            raise SourceAcquisitionError("issuer_feed_item_timestamp_invalid") from error

    rows = []
    for node in nodes:
        links = (
            [
                link.attrib["href"]
                for link in node.findall("{http://www.w3.org/2005/Atom}link")
                if link.get("rel", "alternate") == "alternate" and link.get("href")
            ]
            if atom
            else [text(node, "link")]
        )
        if len(links) != 1 or not links[0]:
            raise SourceAcquisitionError("issuer_feed_item_link_missing_or_ambiguous")
        published_raw = text(node, "published" if atom else "pubDate")
        updated_raw = text(node, "updated" if atom else "modDate")
        published, updated = timestamp(published_raw), timestamp(updated_raw)
        if published is not None and updated is not None and updated < published:
            raise SourceAcquisitionError("issuer_feed_publication_revision_order_invalid")
        rows.append(
            _FeedEntry(
                identifier=text(node, "id" if atom else "guid") or links[0],
                url=links[0],
                headline=text(node, "title") or "",
                published=published,
                updated=updated,
                published_raw=published_raw,
                updated_raw=updated_raw,
            )
        )
    return tuple(rows)


@dataclass
class _NewsCursor:
    start: datetime
    end: datetime
    symbols: tuple[str, ...]
    token: str
    seen: set[str]


class _FilingIndex(HTMLParser):
    """Only the SEC filing's typed document table can authorize exhibit retrieval."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: list[tuple[tuple[str, ...], tuple[str, ...]]] = []
        self.cells: list[str] = []
        self.links: list[str] = []
        self.cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "tr":
            self.cells, self.links = [], []
        elif tag == "td":
            self.cell = []
        elif tag == "a" and self.cell is not None:
            self.links.extend(value for key, value in attrs if key == "href" and value)

    def handle_data(self, data: str) -> None:
        if self.cell is not None:
            self.cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "td" and self.cell is not None:
            self.cells.append("".join(self.cell).strip())
            self.cell = None
        elif tag == "tr" and self.cells:
            self.rows.append((tuple(self.cells), tuple(self.links)))


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
        issuer_feeds_enabled: bool = True,
        max_feed_documents_per_symbol: int = 5,
        seed_evidence: Sequence[SourceEvent] = (),
    ) -> None:
        if settings.data_url.rstrip("/") != "https://data.alpaca.markets":
            raise ValueError("news credentials may only be sent to the fixed Alpaca data host")
        if cache_ttl_seconds < 1 or not 1 <= max_pages <= 20:
            raise ValueError("cache TTL and pagination bounds must be positive and bounded")
        if not 0 <= max_sec_filings_per_symbol <= 5:
            raise ValueError("SEC filing batch must be bounded")
        if not 1 <= max_feed_documents_per_symbol <= 20:
            raise ValueError("issuer feed document batch must be bounded")
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
        self._issuer_feeds_enabled = issuer_feeds_enabled
        self._max_feed_documents = max_feed_documents_per_symbol
        self._feed_completed: set[tuple[str, datetime, str]] = set()
        self._issuer_feed_status: dict[str, Any] = {
            symbol: {
                "url": OFFICIAL_ISSUER_FEEDS.get(symbol),
                "status": "unprobed_until_poll"
                if symbol in OFFICIAL_ISSUER_FEEDS
                else "unavailable",
                "gap": ISSUER_FEED_GAPS.get(symbol),
            }
            for symbol in self._mappings
        }
        self._versions: dict[tuple[str, str, str], SourceEvent] = {}
        self._latest: dict[tuple[str, str], SourceEvent] = {}
        self._http_cache: dict[str, tuple[float, httpx.Response, datetime]] = {}
        self._http_verified_at: dict[str, datetime] = {}
        self._last_request: dict[str, float] = {}
        self._blocked_until: dict[str, float] = {}
        self._news_entitlement = "unprobed_until_poll"
        self.last_errors: tuple[str, ...] = ()
        self.last_poll_stats: dict[str, Any] = {}
        self._news_cursor: _NewsCursor | None = None
        self._sec_completed: set[tuple[str, datetime, str]] = set()
        self._sec_prior_failures: dict[str, tuple[float, str]] = {}
        self._poll_events: list[SourceEvent] | None = None
        self._health: dict[str, Any] | None = None
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
            "sec_scope": "recent_submissions_8K_8KA_and_filing_index_EX99",
            "sec_older_comparison_scope": "latest_prior_10K_within_550_days_recent_submissions",
            "sec_older_comparison_refresh": "reuse_observed_accession_retry_failures_after_3600s",
            "sec_archive_pagination": "unavailable_older_submissions_not_fetched",
            "primary_discovery": "configured_urls_verified_SEC_metadata_and_fixed_official_feeds",
            "issuer_newsroom_discovery": (
                "fixed_official_RSS_Atom_feeds" if self._issuer_feeds_enabled else "disabled"
            ),
            "issuer_feeds_enabled": self._issuer_feeds_enabled,
            "issuer_feed_status": self._issuer_feed_status,
            "max_feed_documents_per_symbol": self._max_feed_documents,
            "provider_receipt_timestamp": "unavailable_Alpaca_created_updated_are_not_receipt",
            "primary_publication_timestamp": (
                "explicit_aware_datePublished_or_official_feed_pubDate_Atom_published_only"
            ),
            "extraction_scope": "existing_deterministic_grammar_only_no_inference",
            "point_in_time_consensus": "missing",
            "inference_provider": None,
            "inference_status": "missing_deterministic_fallback",
            "supported_symbols": tuple(self._mappings),
            "primary_rate_limit_per_second": 2,
            "news_rate_limit_per_second": 2,
            "cache_ttl_seconds": self._cache_ttl,
            "max_pages": self._max_pages,
            "max_sec_filings_per_symbol": self._max_sec_filings,
            "last_errors": self.last_errors,
            "last_poll_stats": self.last_poll_stats,
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
            if self._health is not None:
                self._health["cache_hits"] += 1
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
        if self._health is not None:
            self._health["http_attempts"] += 1
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
            self._http_verified_at[key] = received
            if self._health is not None:
                self._health["http_successes"] += 1
                self._health["last_http_received_at"] = received.isoformat()
            return cached[1], cached[2]
        if response.is_redirect:
            raise SourceAcquisitionError("source_redirect_not_authorized")
        response.raise_for_status()
        if len(response.content) > 5_000_000:
            raise SourceAcquisitionError("source_document_exceeds_size_bound")
        self._http_cache[key] = (self._monotonic(), response, received)
        self._http_verified_at[key] = received
        if self._health is not None:
            self._health["http_successes"] += 1
            self._health["last_http_received_at"] = received.isoformat()
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
            if self._poll_events is not None:
                self._poll_events.append(existing)
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
        if self._poll_events is not None:
            self._poll_events.append(event)
        return event

    def poll(
        self, *, start: datetime, end: datetime, symbols: Sequence[str]
    ) -> tuple[SourceEvent, ...]:
        """Return even partial verified versions; persist them before moving a watermark.

        A bounded news continuation pins its original end, so the only safe new
        watermark is ``last_poll_stats["coverage_watermark"]``, never the caller's
        current clock. Restarts replay rather than skip uncompleted pages.
        """
        start, end = _utc(start), _utc(end)
        if start >= end:
            raise ValueError("source poll interval must increase")
        requested = tuple(sorted(set(symbol.upper() for symbol in symbols)))
        if not requested or any(symbol not in self._mappings for symbol in requested):
            raise ValueError("only AAPL/MSFT/NVDA are in the fixed v20 universe")
        scan_end = end
        if (
            self._news_cursor is not None
            and self._news_cursor.start == start
            and self._news_cursor.symbols == requested
            and self._news_cursor.end <= end
        ):
            scan_end = self._news_cursor.end
        else:
            self._news_cursor = None
        before = {event.evidence_id for event in self._versions.values()}
        self._poll_events = []
        errors: list[str] = []
        health: dict[str, Any] = {}
        self.last_poll_stats = {
            "poll_id": str(uuid4()),
            "requested_start": start.isoformat(),
            "requested_end": end.isoformat(),
            "interval_end": scan_end.isoformat(),
            "observed_at": _utc(self._clock()).isoformat(),
            "symbols": requested,
            "coverage_complete": False,
            "coverage_watermark": None,
            "coverage_scope": "configured_sources_only_not_all_company_news",
            "coverage_basis": (
                "provider_news_interval_SEC_acceptance_interval_official_feed_item_discovery"
            ),
            "coverage_excludes": (
                "optional_older_comparison",
                "issuer_sources_outside_fixed_feeds_and_verified_URLs",
                "proof_of_SEC_publication_time",
                "historical_revision_completeness",
            ),
            "raw_items_received": 0,
            "source_health": health,
        }

        def source_health(name: str) -> dict[str, Any]:
            state: dict[str, Any] = {
                "status": "checking",
                "http_attempts": 0,
                "http_successes": 0,
                "cache_hits": 0,
                "pages": 0,
                "raw_items_received": 0,
                "coverage_complete": False,
            }
            health[name] = self._health = state
            return state

        state = source_health("news")
        coverage_end = scan_end
        try:
            self._news(start=start, end=scan_end, symbols=requested)
            self._news_entitlement = "observed_available"
            state.update(status="healthy", coverage_complete=True)
        except (httpx.HTTPError, ValueError, SourceAcquisitionError) as exc:
            self._news_entitlement = (
                "observed_denied"
                if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code in {401, 403}
                else "request_failed_or_incomplete"
            )
            errors.append(f"news:{type(exc).__name__}:{self._safe_error(exc)}")
            state.update(status="incomplete_or_failed", error=errors[-1])
        for symbol in requested:
            if self._issuer_feeds_enabled and symbol in OFFICIAL_ISSUER_FEEDS:
                state = source_health(f"issuer_feed:{symbol}")
                try:
                    verified_at = self._issuer_feed(symbol=symbol, start=start, end=scan_end)
                    coverage_end = min(coverage_end, verified_at)
                    state.update(status="healthy", coverage_complete=True)
                except (httpx.HTTPError, ValueError, SourceAcquisitionError) as exc:
                    errors.append(
                        f"issuer_feed:{symbol}:{type(exc).__name__}:{self._safe_error(exc)}"
                    )
                    state.update(status="incomplete_or_failed", error=errors[-1])
                self._issuer_feed_status[symbol] = dict(state)
            else:
                health[f"issuer_feed:{symbol}"] = {
                    "status": (
                        "unavailable_no_verified_feed" if self._issuer_feeds_enabled else "disabled"
                    ),
                    "gap": ISSUER_FEED_GAPS.get(symbol),
                }
                self._issuer_feed_status[symbol] = dict(health[f"issuer_feed:{symbol}"])
            for url in self._primary_urls.get(symbol, ()):
                state = source_health(f"primary:{symbol}:{url}")
                try:
                    self.fetch_primary(url=url, symbol=symbol)
                    state.update(status="healthy", coverage_complete=True, scope="exact_url_only")
                except (httpx.HTTPError, ValueError, SourceAcquisitionError) as exc:
                    errors.append(f"primary:{symbol}:{type(exc).__name__}:{self._safe_error(exc)}")
                    state.update(status="incomplete_or_failed", error=errors[-1])
            if self._sec_user_agent is not None:
                state = source_health(f"sec:{symbol}")
                try:
                    self._sec(symbol=symbol, start=start, end=scan_end)
                    state.update(status="healthy", coverage_complete=True)
                except (httpx.HTTPError, ValueError, SourceAcquisitionError) as exc:
                    errors.append(f"sec:{symbol}:{type(exc).__name__}:{self._safe_error(exc)}")
                    state.update(status="incomplete_or_failed", error=errors[-1])
            else:
                health[f"sec:{symbol}"] = {"status": "disabled_contact_not_configured"}
        self.last_errors = tuple(errors)
        complete = not errors and coverage_end > start
        received_events = self._poll_events
        self._poll_events = None
        self._health = None
        unique = {event.evidence_id: event for event in received_events}
        self.last_poll_stats.update(
            observed_at=_utc(self._clock()).isoformat(),
            coverage_complete=complete,
            coverage_watermark=coverage_end.isoformat() if complete else None,
            raw_items_received=sum(s.get("raw_items_received", 0) for s in health.values()),
            unique_evidence_versions=len(unique),
            new_evidence_versions=len(unique.keys() - before),
            duplicates=len(received_events) - len(unique),
            previously_received_versions=len(unique.keys() & before),
            revision_versions=sum(event.revision_of is not None for event in unique.values()),
            unique_events=len({event.event_cluster_id for event in unique.values()}),
            supported_company_matches=sum(
                bool(set(event.provider_symbols + event.related_instruments) & set(requested))
                for event in unique.values()
            ),
            verified_issuer_matches=sum(event.is_primary_source for event in unique.values()),
            errors=errors,
        )
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
        if self._news_cursor is not None:
            params["page_token"] = self._news_cursor.token
            tokens = set(self._news_cursor.seen)
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
            if self._health is not None:
                self._health["pages"] += 1
                self._health["raw_items_received"] += len(payload["news"])
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
                        if key
                        in {
                            "id",
                            "headline",
                            "url",
                            "source",
                            "author",
                            "symbols",
                            "created_at",
                            "updated_at",
                        }
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
                self._news_cursor = None
                return tuple(events)
            if not isinstance(token, str) or token in tokens:
                self._news_cursor = None
                raise SourceAcquisitionError("news_pagination_token_invalid_or_repeated")
            tokens.add(token)
            self._news_cursor = _NewsCursor(start, end, tuple(symbols), token, set(tokens))
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

    def _issuer_feed(self, *, symbol: str, start: datetime, end: datetime) -> datetime:
        mapping = self._mappings[symbol]
        feed_url = verify_primary_url(OFFICIAL_ISSUER_FEEDS[symbol], mapping)
        response, received = self._request(
            feed_url,
            headers={
                "User-Agent": "TradeAgent-event-research/20",
                "Accept": "application/rss+xml,application/atom+xml,application/xml,text/xml",
            },
        )
        verified_at = self._http_verified_at.get(config_hash((feed_url, None)), received)
        entries = _feed_entries(response.content)
        clocks = [item.updated or item.published for item in entries]
        known_clocks = [value for value in clocks if value is not None]
        if self._health is not None:
            self._health.update(
                url=feed_url,
                feed_received_at=received.isoformat(),
                feed_verified_at=verified_at.isoformat(),
                feed_sha256=text_hash(response.text),
                gap=ISSUER_FEED_GAPS.get(symbol),
                feed_entries_observed=len(entries),
                feed_entries_without_publication_time=sum(
                    item.published is None for item in entries
                ),
                feed_oldest_discovery_time=(
                    min(known_clocks).isoformat() if known_clocks else None
                ),
                discovery_time_basis="item_updated_or_published_never_updated_as_publication",
                scope="current_feed_allowlisted_article_URLs_only_no_archive_pagination",
                publication_timestamp_status=(
                    "feed_publication_times_available"
                    if entries and all(item.published is not None for item in entries)
                    else "feed_publication_missing_requires_explicit_document_timestamp"
                ),
                revision_timestamp_status=(
                    "item_revision_times_available"
                    if entries and all(item.updated is not None for item in entries)
                    else "item_revision_times_not_always_provided"
                ),
                rejected_link_count=0,
                rejected_link_reasons=[],
                raw_items_received=len(entries),
            )
        self._feed_completed = {
            key for key in self._feed_completed if key[0] != symbol or key[1] == start
        }
        pending: list[tuple[_FeedEntry, str, str]] = []
        for item in entries:
            discovery_at = item.updated or item.published
            if discovery_at is not None and not start <= discovery_at <= end:
                continue
            try:
                url = verify_primary_url(item.url, mapping)
            except ValueError:
                if self._health is not None:
                    self._health["rejected_link_count"] += 1
                    self._health["rejected_link_reasons"].append(
                        "outside_verified_primary_URL_policy"
                    )
                continue
            key = config_hash(
                (item.identifier, url, item.headline, item.published_raw, item.updated_raw)
            )
            if (symbol, start, key) not in self._feed_completed:
                pending.append((item, url, key))
        for item, url, key in pending[: self._max_feed_documents]:
            self._primary(
                url=url,
                mapping=mapping,
                published_at=item.published,
                feed_metadata={
                    "feed_url": feed_url,
                    "feed_received_at": received.isoformat(),
                    "feed_verified_at": verified_at.isoformat(),
                    "feed_sha256": text_hash(response.text),
                    "item_id": item.identifier,
                    "item_url": item.url,
                    "item_title": item.headline,
                    "published_raw": item.published_raw,
                    "updated_raw": item.updated_raw,
                    "published_at": item.published.isoformat() if item.published else None,
                    "updated_at": item.updated.isoformat() if item.updated else None,
                    "entry_version_key": key,
                    "publication_basis": (
                        "official_feed_item_publication"
                        if item.published
                        else "unknown_feed_updated_is_not_publication"
                    ),
                },
            )
            self._feed_completed.add((symbol, start, key))
        if len(pending) > self._max_feed_documents:
            raise SourceAcquisitionError("issuer_feed_document_bound_reached_incomplete_poll")
        if not known_clocks or len(known_clocks) != len(entries):
            raise SourceAcquisitionError("issuer_feed_discovery_interval_unknown")
        if min(known_clocks) > start:
            raise SourceAcquisitionError("issuer_feed_history_does_not_reach_interval_start")
        return verified_at

    def _primary(
        self,
        *,
        url: str,
        mapping: IssuerMapping,
        published_at: datetime | None = None,
        filing_metadata: dict[str, Any] | None = None,
        feed_metadata: dict[str, Any] | None = None,
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
        if self._health is not None:
            self._health["raw_items_received"] += 1
        content = _plain_text(raw)
        # Publication is only explicit datePublished, never HTTP Last-Modified or receipt.
        date_matches = re.findall(
            r'["\']datePublished["\']\s*:\s*["\']([^"\']+)["\']', raw, re.IGNORECASE
        )
        document_published_at = None
        if len(set(date_matches)) == 1:
            try:
                document_published_at = _timestamp(date_matches[0])
            except ValueError:
                document_published_at = None
        if (
            published_at is not None
            and document_published_at is not None
            and published_at != document_published_at
        ):
            raise SourceAcquisitionError("official_feed_document_publication_timestamp_conflict")
        published_at = published_at or document_published_at
        modified_at = None
        modified_matches = re.findall(
            r'["\']dateModified["\']\s*:\s*["\']([^"\']+)["\']', raw, re.IGNORECASE
        )
        if len(set(modified_matches)) == 1:
            try:
                modified_at = _timestamp(modified_matches[0])
            except ValueError:
                modified_at = None
        metadata = {
            "url": url,
            "status": response.status_code,
            "etag": response.headers.get("etag"),
            "last_modified": response.headers.get("last-modified"),
            "raw_document": raw,
            "filing": filing_metadata,
            "official_feed": feed_metadata,
            "document_publication_date_values": date_matches,
            "publication_timestamp_basis": (
                "official_feed_item_publication"
                if feed_metadata and feed_metadata["published_at"]
                else "explicit_datePublished"
                if published_at
                else "unknown"
            ),
            "revision_timestamp_basis": (
                "official_feed_item_updated"
                if feed_metadata and feed_metadata.get("updated_at")
                else "explicit_dateModified"
                if modified_at
                else "unknown"
            ),
            "document_modified_at": modified_at.isoformat() if modified_at else None,
        }
        metadata_json = json.dumps(metadata, sort_keys=True)
        version = text_hash(raw)
        if feed_metadata is not None:
            version = config_hash((version, feed_metadata["entry_version_key"]))
        headline_match = re.search(r"<h1[^>]*>(.*?)</h1>", raw, re.IGNORECASE | re.DOTALL)
        headline = _plain_text(headline_match[1]) if headline_match else ""
        if feed_metadata is not None:
            headline = str(feed_metadata["item_title"]) or headline
        is_amendment = filing_metadata is not None and str(
            filing_metadata.get("form", "")
        ).endswith("/A")
        source = "sec_edgar" if urlsplit(url).hostname == "www.sec.gov" else "issuer_primary"
        if feed_metadata is not None:
            source += ":official_feed"
        event = SourceEvent(
            source_event_id=url,
            source=source,
            source_url=url,
            source_version=version,
            published_at=published_at,
            provider_updated_at=(
                _timestamp(feed_metadata.get("updated_at"))
                if feed_metadata and feed_metadata.get("updated_at")
                else modified_at
            ),
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
        candidates: list[tuple[datetime, str, str, str]] = []
        prior_annual: list[tuple[datetime, str, str, str]] = []
        for accession, accepted, document, form in zip(*columns, strict=True):
            accepted_at = _timestamp(accepted)
            if accepted_at is None or accepted_at > end:
                continue
            current = start <= accepted_at and form in {"8-K", "8-K/A"}
            comparison = end - timedelta(days=550) <= accepted_at < start and form in {
                "10-K",
                "10-K/A",
            }
            if not current and not comparison:
                continue
            if not re.fullmatch(r"\d{10}-\d{2}-\d{6}", str(accession)) or not re.fullmatch(
                r"[A-Za-z0-9_.-]+", str(document)
            ):
                raise SourceAcquisitionError("SEC_filing_document_path_invalid")
            row = (accepted_at, str(accession), str(document), str(form))
            (candidates if current else prior_annual).append(row)
        candidates.sort()
        if self._health is not None:
            self._health["eligible_filings"] = len(candidates)
            self._health["filing_batch_limit"] = self._max_sec_filings
            self._health["submission_received_at"] = received.isoformat()
            self._health["prior_comparison_status"] = (
                "available_for_collection" if prior_annual else "missing_recent_10K_within_550_days"
            )
        self._sec_completed = {
            key for key in self._sec_completed if key[0] != symbol or key[1] == start
        }
        pending = [row for row in candidates if (symbol, start, row[1]) not in self._sec_completed]
        selected = pending[: self._max_sec_filings]
        if prior_annual and self._max_sec_filings:
            selected.append(max(prior_annual))
        events: list[SourceEvent] = []
        document_errors: list[str] = []
        context_error: str | None = None
        context_collected = False
        for accepted_at, accession, document, form in selected:
            is_context = accepted_at < start
            directory = (
                f"https://www.sec.gov/Archives/edgar/data/{int(mapping.cik)}/"
                f"{accession.replace('-', '')}/"
            )
            metadata = {
                "accession": accession,
                "form": form,
                "accepted_at": accepted_at.isoformat(),
                "submission_received_at": received.isoformat(),
                "public_availability_limitation": "acceptance_is_not_publication",
                "collection_role": "older_comparison" if is_context else "interval_event",
            }
            document_url = directory + document
            if is_context:
                cached_prior = self._latest.get(("sec_edgar", document_url))
                if cached_prior is not None:
                    events.append(self._remember(cached_prior))
                    context_collected = True
                    continue
                failure = self._sec_prior_failures.get(document_url)
                if failure is not None and self._monotonic() < failure[0]:
                    context_error = failure[1]
                    continue
            try:
                events.append(
                    self._primary(url=document_url, mapping=mapping, filing_metadata=metadata)
                )
            except (httpx.HTTPError, ValueError, SourceAcquisitionError) as exc:
                if is_context:
                    context_error = self._safe_error(exc)
                    self._sec_prior_failures[document_url] = (
                        self._monotonic() + 3600,
                        context_error,
                    )
                    continue
                document_errors.append(f"primary_document:{self._safe_error(exc)}")
                continue
            if is_context:
                context_collected = True
            if not is_context:
                try:
                    events.extend(
                        self._sec_exhibits(
                            mapping=mapping, directory=directory, filing_metadata=metadata
                        )
                    )
                    self._sec_completed.add((symbol, start, accession))
                except (httpx.HTTPError, ValueError, SourceAcquisitionError) as exc:
                    document_errors.append(f"exhibits:{self._safe_error(exc)}")
        if self._health is not None:
            self._health["document_errors"] = document_errors
            self._health["prior_comparison_error"] = context_error
            self._health["prior_comparison_status"] = (
                "unavailable_context_request_failed"
                if context_error
                else "disabled_zero_filing_batch"
                if not self._max_sec_filings
                else "collected_current_version_publication_may_be_unknown"
                if context_collected
                else self._health["prior_comparison_status"]
            )
        if document_errors:
            raise SourceAcquisitionError(
                "SEC_document_coverage_incomplete:" + ";".join(document_errors)
            )
        if len(pending) > self._max_sec_filings:
            raise SourceAcquisitionError("SEC_filing_batch_bound_reached_incomplete_poll")
        return tuple(events)

    def _sec_exhibits(
        self,
        *,
        mapping: IssuerMapping,
        directory: str,
        filing_metadata: dict[str, Any],
    ) -> tuple[SourceEvent, ...]:
        accession = str(filing_metadata["accession"])
        index_url = verify_primary_url(f"{directory}{accession}-index.html", mapping)
        response, received = self._request(
            index_url,
            headers={"User-Agent": self._sec_user_agent or "", "Accept": "text/html"},
        )
        parser = _FilingIndex()
        parser.feed(response.text)
        document_rows = [row for row in parser.rows if len(row[0]) >= 4 and row[1]]
        if not any(cells[3] == filing_metadata["form"] for cells, _ in document_rows):
            raise SourceAcquisitionError("SEC_filing_index_document_table_missing")
        urls: list[tuple[str, str]] = []
        for cells, links in document_rows:
            exhibit_type = cells[3].upper()
            if not re.fullmatch(r"EX-99(?:\.\d+)?", exhibit_type):
                continue
            if len(links) != 1:
                raise SourceAcquisitionError("SEC_exhibit_link_ambiguous")
            link = links[0]
            url = (
                "https://www.sec.gov" + link
                if link.startswith("/")
                else link
                if "://" in link
                else directory + link
            )
            url = verify_primary_url(url, mapping)
            if not url.startswith(directory) or not re.fullmatch(
                r"[A-Za-z0-9_-][A-Za-z0-9_.-]*\.(?:html?|txt)", url[len(directory) :]
            ):
                raise SourceAcquisitionError("SEC_exhibit_not_in_verified_filing")
            if (url, exhibit_type) not in urls:
                urls.append((url, exhibit_type))
        if self._health is not None:
            self._health["exhibits_discovered"] = self._health.get("exhibits_discovered", 0) + len(
                urls
            )
        events = []
        for url, exhibit_type in urls[:3]:
            events.append(
                self._primary(
                    url=url,
                    mapping=mapping,
                    filing_metadata={
                        **filing_metadata,
                        "document_type": exhibit_type,
                        "index_url": index_url,
                        "index_received_at": received.isoformat(),
                        "index_sha256": text_hash(response.text),
                    },
                )
            )
        if len(urls) > 3:
            raise SourceAcquisitionError("SEC_exhibit_bound_reached_incomplete_poll")
        return tuple(events)

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> EventSourceClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
