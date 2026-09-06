"""Read-only official calendar/halt context, never a claim of a quiet future market.

Nasdaq permits one RSS request per minute. Calendar responses are cached for an
hour; failures never reuse a previously clear status. FOMC meeting dates become
explicit whole-day risk windows, not invented intraday release timestamps.
"""

from __future__ import annotations

import base64
import calendar
import re
import time
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime, timedelta
from email.utils import parsedate_to_datetime
from hashlib import sha256
from typing import Literal, Self
from xml.etree import ElementTree
from zoneinfo import ZoneInfo

import httpx
from pydantic import AwareDatetime, Field, model_validator

from tradeagent.event_research import EvidenceModel, config_hash

FED_CALENDAR_URL = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
BLS_CALENDAR_URL = "https://www.bls.gov/schedule/news_release/bls.ics"
NASDAQ_HALTS_URL = "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts"
NASDAQ_RSS_DOCUMENTATION_URL = "https://www.nasdaqtrader.com/Trader.aspx?id=TradeHaltRSS"
LATEST_QUOTE_SCHEMA_URL = "https://docs.alpaca.markets/us/reference/stocklatestquotes-1"
QUOTE_SIZE_CHANGE_URL = (
    "https://docs.alpaca.markets/us/v1.1/changelog/marketdata-bid-and-ask-size-display-change"
)
NY = ZoneInfo("America/New_York")
SUPPORTED_SYMBOLS = frozenset(("AAPL", "MSFT", "NVDA"))
NDAQ = "{http://www.nasdaqtrader.com/}"


class ContextResponse(EvidenceModel):
    evidence_id: str
    source_url: str
    http_status: int | None
    first_received_at: AwareDatetime
    received_at: AwareDatetime
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    body_base64: str
    response_headers: tuple[tuple[str, str], ...] = ()
    error: str | None = None

    @model_validator(mode="after")
    def validate_content(self) -> Self:
        if (
            sha256(base64.b64decode(self.body_base64, validate=True)).hexdigest()
            != self.content_sha256
        ):
            raise ValueError("context_content_hash_mismatch")
        if self.first_received_at > self.received_at:
            raise ValueError("context_first_receipt_after_current_receipt")
        return self

    @property
    def text(self) -> str:
        return base64.b64decode(self.body_base64, validate=True).decode(
            "utf-8-sig", errors="strict"
        )


class MacroRiskWindow(EvidenceModel):
    start: AwareDatetime
    end: AwareDatetime
    name: str
    precision: Literal["date"] = "date"
    evidence_id: str


class HaltStatus(EvidenceModel):
    symbol: str
    halted: bool | None = None
    available_at: AwareDatetime | None = None
    valid_until: AwareDatetime | None = None
    feed_published_at: AwareDatetime | None = None
    evidence_id: str | None = None
    reason: str


class OfficialContextSnapshot(EvidenceModel):
    observed_at: AwareDatetime
    evidence: tuple[ContextResponse, ...]
    macro_calendar_available_at: AwareDatetime | None = None
    macro_calendar_covers_until: AwareDatetime | None = None
    scheduled_macro_events: tuple[AwareDatetime, ...] = ()
    macro_risk_windows: tuple[MacroRiskWindow, ...] = ()
    halts: tuple[HaltStatus, ...]
    errors: tuple[str, ...] = ()

    def halted_for(self, symbol: str, *, now: datetime | None = None) -> bool | None:
        evaluation = _utc(now) if now is not None else self.observed_at
        for item in self.halts:
            if (
                item.symbol == symbol
                and item.available_at is not None
                and item.valid_until is not None
                and item.available_at <= evaluation <= item.valid_until
            ):
                return item.halted
        return None

    def blocking_reasons(
        self, *, now: datetime, horizon_minutes: int = 60, margin_minutes: int = 30
    ) -> tuple[str, ...]:
        """Add these to MarketContext.contradictions; do not omit date-only FOMC risk."""
        now = _utc(now)
        if horizon_minutes <= 0 or margin_minutes < 0:
            raise ValueError("context horizon and margin must be nonnegative")
        if (
            self.macro_calendar_available_at is None
            or self.macro_calendar_available_at > now
            or self.macro_calendar_covers_until is None
            or self.macro_calendar_covers_until < now + timedelta(minutes=horizon_minutes)
            or now - self.observed_at > timedelta(days=1)
        ):
            return ("official_macro_calendar_unavailable_or_incomplete",)
        start = now - timedelta(minutes=margin_minutes)
        end = now + timedelta(minutes=horizon_minutes + margin_minutes)
        if any(window.start <= end and window.end > start for window in self.macro_risk_windows):
            return ("official_macro_date_only_risk_window",)
        return ()


def latest_rest_quote_size_unit(
    *, feed: str, quote_at: datetime, endpoint: str = "/v2/stocks/quotes/latest"
) -> Literal["shares", "unknown"]:
    """Verified latest REST schema only; never converts sizes or authorizes IEX orders.

    The endpoint's stock_quote schema explicitly says shares after Nov 3, 2025,
    and its feed enum includes IEX. The older stream schema still says round lots,
    so this contract must not be applied to stream payloads or historical bars.
    """
    if (
        endpoint == "/v2/stocks/quotes/latest"
        and feed in {"iex", "sip"}
        and _utc(quote_at) >= datetime(2025, 11, 3, 5, tzinfo=UTC)
    ):
        return "shares"
    return "unknown"


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("context time must be timezone-aware")
    return value.astimezone(UTC)


def _local_date(value: date) -> datetime:
    return datetime(value.year, value.month, value.day, tzinfo=NY).astimezone(UTC)


def _text(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]*>", "", value)).strip()


def _fed_calendar(
    response: ContextResponse, *, now: datetime
) -> tuple[datetime, tuple[MacroRiskWindow, ...]]:
    text = response.text
    if "Meeting calendars and information" not in text:
        raise ValueError("fed_calendar_identity_missing")
    sections = re.split(r"(\d{4}) FOMC Meetings", text)
    windows: list[MacroRiskWindow] = []
    complete_years: list[int] = []
    months = {name.lower(): index for index, name in enumerate(calendar.month_name) if name}
    for offset in range(1, len(sections), 2):
        year = int(sections[offset])
        if not now.year <= year <= now.year + 1:
            continue
        section = sections[offset + 1]
        month_values = re.findall(
            r'class="[^"]*fomc-meeting__month[^"]*"[^>]*>(.*?)</div>', section, re.DOTALL
        )
        day_values = re.findall(
            r'class="[^"]*fomc-meeting__date[^"]*"[^>]*>(.*?)</div>', section, re.DOTALL
        )
        if not 8 <= len(month_values) <= 12 or len(month_values) != len(day_values):
            raise ValueError(f"fed_{year}_calendar_incomplete_or_schema_changed")
        for raw_month, raw_day in zip(month_values, day_values, strict=True):
            month_parts = _text(raw_month).lower().split("/")
            days = re.fullmatch(r"(\d{1,2})(?:-(\d{1,2}))?\*?", _text(raw_day))
            if (
                days is None
                or not 1 <= len(month_parts) <= 2
                or any(month.strip() not in months for month in month_parts)
            ):
                raise ValueError("fed_meeting_date_schema_unsupported")
            first = date(year, months[month_parts[0].strip()], int(days[1]))
            last = date(year, months[month_parts[-1].strip()], int(days[2] or days[1]))
            if not 0 <= (last - first).days <= 3:
                raise ValueError("fed_meeting_date_range_invalid")
            windows.append(
                MacroRiskWindow(
                    start=_local_date(first),
                    end=_local_date(last + timedelta(days=1)),
                    name=f"FOMC meeting {first.isoformat()} through {last.isoformat()}",
                    evidence_id=response.evidence_id,
                )
            )
        complete_years.append(year)
    if now.year not in complete_years:
        raise ValueError("fed_current_year_coverage_missing")
    return _local_date(date(max(complete_years) + 1, 1, 1)), tuple(windows)


def _ics_start(line: str) -> tuple[datetime, bool]:
    key, separator, value = line.partition(":")
    if not separator:
        raise ValueError("bls_dtstart_missing")
    if key == "DTSTART;VALUE=DATE":
        parsed = datetime.strptime(value, "%Y%m%d").date()
        return _local_date(parsed), True
    if key == "DTSTART" and value.endswith("Z"):
        return datetime.strptime(value, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC), False
    if key in {
        "DTSTART;TZID=US-Eastern",
        "DTSTART;TZID=America/New_York",
        'DTSTART;TZID="America/New_York"',
    }:
        naive = datetime.strptime(value, "%Y%m%dT%H%M%S")
        localized = naive.replace(tzinfo=NY)
        # Refuse nonexistent/ambiguous DST-local times rather than silently shift.
        if (
            localized.astimezone(UTC).astimezone(NY).replace(tzinfo=None) != naive
            or localized.utcoffset() != localized.replace(fold=1).utcoffset()
        ):
            raise ValueError("bls_ambiguous_local_release_time")
        return localized.astimezone(UTC), False
    raise ValueError("bls_release_timezone_or_recurrence_unsupported")


def _bls_calendar(
    response: ContextResponse, *, now: datetime
) -> tuple[datetime, tuple[datetime, ...], tuple[MacroRiskWindow, ...]]:
    text = re.sub(r"\r?\n[ \t]", "", response.text).replace("\r\n", "\n")
    if (
        not text.startswith("BEGIN:VCALENDAR")
        or not text.rstrip().endswith("END:VCALENDAR")
        or "Bureau of Labor Statistics" not in text
    ):
        raise ValueError("bls_calendar_identity_or_completeness_invalid")
    blocks = re.findall(r"BEGIN:VEVENT\n(.*?)\nEND:VEVENT", text, re.DOTALL)
    if not blocks or len(blocks) != text.count("BEGIN:VEVENT"):
        raise ValueError("bls_events_missing_or_truncated")
    versions: dict[str, tuple[int, str]] = {}
    for block in blocks:
        lines = block.splitlines()
        fields = {line.partition(":")[0]: line.partition(":")[2] for line in lines}
        uid = fields.get("UID")
        if not uid or "SUMMARY" not in fields:
            raise ValueError("bls_uid_or_summary_missing")
        sequence = int(fields.get("SEQUENCE", "0"))
        if uid in versions:
            old_sequence, old_block = versions[uid]
            if sequence == old_sequence and block != old_block:
                raise ValueError("bls_conflicting_same_sequence_revision")
            if sequence < old_sequence:
                continue
        versions[uid] = (sequence, block)
    events: set[datetime] = set()
    windows: list[MacroRiskWindow] = []
    latest: datetime | None = None
    for _, block in versions.values():
        lines = block.splitlines()
        fields = {line.partition(":")[0]: line.partition(":")[2] for line in lines}
        if fields.get("STATUS") == "CANCELLED":
            continue
        if any(key in fields for key in ("RRULE", "RDATE", "EXDATE", "RECURRENCE-ID")):
            raise ValueError("bls_recurring_release_not_supported")
        starts = [line for line in lines if line.startswith(("DTSTART:", "DTSTART;"))]
        if len(starts) != 1:
            raise ValueError("bls_unique_release_time_missing")
        start, date_only = _ics_start(starts[0])
        latest = max(start, latest) if latest is not None else start
        if date_only:
            next_day = start.astimezone(NY).date() + timedelta(days=1)
            windows.append(
                MacroRiskWindow(
                    start=start,
                    end=_local_date(next_day),
                    name=fields["SUMMARY"],
                    evidence_id=response.evidence_id,
                )
            )
        else:
            events.add(start)
    if latest is None or latest <= now:
        raise ValueError("bls_future_calendar_coverage_missing")
    # Bound coverage by the last actual release in the complete official feed,
    # not by an invented end-of-year completeness guarantee.
    return latest, tuple(sorted(events)), tuple(windows)


def _nasdaq_halts(
    response: ContextResponse, *, symbols: Sequence[str], now: datetime
) -> tuple[HaltStatus, ...]:
    text = response.text
    if "<!DOCTYPE" in text.upper() or "<!ENTITY" in text.upper():
        raise ValueError("halt_xml_entity_declaration_forbidden")
    root = ElementTree.fromstring(text)
    channel = root.find("channel")
    if root.tag != "rss" or channel is None:
        raise ValueError("halt_rss_channel_missing")
    if channel.findtext("description") != "NASDAQ Trade Halts":
        raise ValueError("halt_feed_identity_mismatch")
    published = _utc(parsedate_to_datetime(channel.findtext("pubDate") or ""))
    if not 0 <= (now - published).total_seconds() <= 90:
        raise ValueError("halt_feed_publication_stale_or_future")
    items = channel.findall("item")
    if channel.findtext(f"{NDAQ}numItems") != str(len(items)):
        raise ValueError("halt_feed_item_count_mismatch")
    result: list[HaltStatus] = []
    for symbol in symbols:
        halted = False
        for item in items:
            item_symbol = (item.findtext(f"{NDAQ}IssueSymbol") or "").strip()
            if not item_symbol:
                raise ValueError("halt_item_symbol_missing")
            if item_symbol != symbol:
                continue
            halt_date = item.findtext(f"{NDAQ}HaltDate") or ""
            halt_time = item.findtext(f"{NDAQ}HaltTime") or ""
            start = _halt_time(halt_date, halt_time)
            if start > now:
                raise ValueError("halt_start_in_future")
            resume_date = item.findtext(f"{NDAQ}ResumptionDate") or ""
            resume_time = item.findtext(f"{NDAQ}ResumptionTradeTime") or ""
            # Quote resumption alone does not establish trading resumption.
            if not resume_date or not resume_time:
                halted = True
                continue
            resumed = _halt_time(resume_date, resume_time)
            if resumed < start:
                raise ValueError("halt_resumption_precedes_halt")
            if resumed > now:
                halted = True
        result.append(
            HaltStatus(
                symbol=symbol,
                halted=halted,
                available_at=response.received_at,
                valid_until=min(
                    published + timedelta(seconds=90),
                    response.received_at + timedelta(seconds=90),
                ),
                feed_published_at=published,
                evidence_id=response.evidence_id,
                reason="reported_halt" if halted else "absent_or_resumed_in_complete_current_feed",
            )
        )
    return tuple(result)


def _halt_time(day: str, moment: str) -> datetime:
    value = f"{day} {moment}".strip()
    pattern = "%m/%d/%Y %H:%M:%S.%f" if "." in moment else "%m/%d/%Y %H:%M:%S"
    return datetime.strptime(value, pattern).replace(tzinfo=NY).astimezone(UTC)


class OfficialContextClient:
    def __init__(
        self,
        *,
        client: httpx.Client | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._client = client or httpx.Client(timeout=15, follow_redirects=False)
        self._owns_client = client is None
        self._clock = clock
        self._monotonic = monotonic
        self._cache: dict[str, tuple[float, ContextResponse]] = {}
        self._first_receipts: dict[tuple[str, int | None, str], datetime] = {}
        self._last_snapshot: OfficialContextSnapshot | None = None
        self._backoff: dict[str, float] = {}

    @property
    def capabilities(self) -> dict[str, object]:
        snapshot = self._last_snapshot
        return {
            "observed_at": snapshot.observed_at.isoformat() if snapshot is not None else None,
            "availability_status_is_as_of_last_poll": True,
            "official_urls": (FED_CALENDAR_URL, BLS_CALENDAR_URL, NASDAQ_HALTS_URL),
            "calendar_cache_seconds": 3600,
            "halt_cache_seconds": 60,
            "halt_max_age_seconds": 90,
            "macro_scope": "published_FOMC_meeting_days_and_all_scheduled_BLS_releases",
            "macro_precision": "FOMC_date_only_whole_day_windows_BLS_explicit_times",
            "unscheduled_macro_coverage": "not_available",
            "quote_size_rest_contract": "shares_on_current_latest_REST_sip_and_iex",
            "quote_size_documentation": (LATEST_QUOTE_SCHEMA_URL, QUOTE_SIZE_CHANGE_URL),
            "iex_is_nbbo": False,
            "paper_execution_authorized": False,
            "macro_available": snapshot is not None
            and snapshot.macro_calendar_available_at is not None,
            "halt_available": snapshot is not None
            and all(item.halted is not None for item in snapshot.halts),
            "last_errors": snapshot.errors if snapshot is not None else ("not_yet_probed",),
        }

    def _get(self, url: str) -> ContextResponse:
        if url not in {FED_CALENDAR_URL, BLS_CALENDAR_URL, NASDAQ_HALTS_URL}:
            raise ValueError("context URL is not independently allowlisted")
        tick = self._monotonic()
        cached = self._cache.get(url)
        ttl = 60 if url == NASDAQ_HALTS_URL else 3600
        if cached is not None:
            cached_tick, cached_response = cached
            effective_ttl = ttl if cached_response.error is None else 60
            if tick < max(cached_tick + effective_ttl, self._backoff.get(url, 0)):
                return cached_response
        body = b""
        headers: tuple[tuple[str, str], ...] = ()
        status: int | None = None
        error: str | None = None
        try:
            with self._client.stream(
                "GET",
                url,
                headers={
                    "User-Agent": "TradeAgent event-research/20 (public calendar validation)",
                    "Accept": "text/calendar,application/xml,text/xml,text/html",
                },
                timeout=15,
                follow_redirects=False,
            ) as response:
                status = response.status_code
                headers = tuple(
                    (key, value)
                    for key, value in response.headers.items()
                    if key in {"date", "content-type", "etag", "last-modified", "retry-after"}
                )
                chunks: list[bytes] = []
                size = 0
                for chunk in response.iter_bytes():
                    size += len(chunk)
                    if size > 2_000_000:
                        error = "context_response_size_exceeded"
                        break
                    chunks.append(chunk)
                body = b"".join(chunks)
                if status != 200:
                    error = f"http_{status}"
                if status == 429:
                    try:
                        delay = max(60, min(3600, int(response.headers.get("retry-after", "60"))))
                    except ValueError:
                        delay = 60
                    self._backoff[url] = tick + delay
        except httpx.HTTPError as exc:
            error = f"transport_{type(exc).__name__}"
        received = _utc(self._clock())
        digest = sha256(body).hexdigest()
        version_key = (url, status, digest)
        first = self._first_receipts.setdefault(version_key, received)
        evidence = ContextResponse(
            evidence_id=config_hash((url, status, digest, received.isoformat())),
            source_url=url,
            http_status=status,
            first_received_at=first,
            received_at=received,
            content_sha256=digest,
            body_base64=base64.b64encode(body).decode("ascii"),
            response_headers=headers,
            error=error,
        )
        self._cache[url] = (tick, evidence)
        return evidence

    def poll(
        self, *, symbols: Sequence[str], now: datetime | None = None
    ) -> OfficialContextSnapshot:
        requested = tuple(sorted(set(symbol.upper() for symbol in symbols)))
        if not requested or not set(requested) <= SUPPORTED_SYMBOLS:
            raise ValueError("official context supports only AAPL/MSFT/NVDA")
        responses = tuple(
            self._get(url) for url in (FED_CALENDAR_URL, BLS_CALENDAR_URL, NASDAQ_HALTS_URL)
        )
        evaluation = _utc(now) if now is not None else _utc(self._clock())
        errors: list[str] = []
        parsed: dict[str, ContextResponse] = {}
        for response in responses:
            reason = response.error
            if response.received_at > evaluation:
                reason = "response_not_yet_received_at_evaluation"
            max_age = 90 if response.source_url == NASDAQ_HALTS_URL else 86400
            if (evaluation - response.received_at).total_seconds() > max_age:
                reason = "context_response_stale"
            if reason is not None:
                errors.append(f"{response.source_url}:{reason}")
            else:
                parsed[response.source_url] = response
        macro_available: datetime | None = None
        coverage: datetime | None = None
        events: tuple[datetime, ...] = ()
        windows: tuple[MacroRiskWindow, ...] = ()
        if FED_CALENDAR_URL in parsed and BLS_CALENDAR_URL in parsed:
            try:
                fed = parsed[FED_CALENDAR_URL]
                bls = parsed[BLS_CALENDAR_URL]
                fed_end, fed_windows = _fed_calendar(fed, now=evaluation)
                bls_end, events, bls_windows = _bls_calendar(bls, now=evaluation)
                macro_available = max(fed.first_received_at, bls.first_received_at)
                coverage = min(fed_end, bls_end)
                windows = (*fed_windows, *bls_windows)
            except (ValueError, UnicodeError) as exc:
                errors.append(f"macro_schema:{exc}")
        halts = tuple(
            HaltStatus(symbol=symbol, reason="official_halt_feed_unavailable")
            for symbol in requested
        )
        if NASDAQ_HALTS_URL in parsed:
            try:
                halts = _nasdaq_halts(parsed[NASDAQ_HALTS_URL], symbols=requested, now=evaluation)
            except (ValueError, UnicodeError, ElementTree.ParseError, TypeError) as exc:
                errors.append(f"halt_schema:{exc}")
        snapshot = OfficialContextSnapshot(
            observed_at=evaluation,
            evidence=responses,
            macro_calendar_available_at=macro_available,
            macro_calendar_covers_until=coverage,
            scheduled_macro_events=events,
            macro_risk_windows=windows,
            halts=halts,
            errors=tuple(errors),
        )
        self._last_snapshot = snapshot
        return snapshot

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> OfficialContextClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
