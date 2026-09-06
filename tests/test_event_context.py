from __future__ import annotations

import base64
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from tradeagent.event_context import (
    BLS_CALENDAR_URL,
    FED_CALENDAR_URL,
    NASDAQ_HALTS_URL,
    ContextResponse,
    OfficialContextClient,
    latest_rest_quote_size_unit,
)

NOW = datetime(2026, 9, 8, 14, tzinfo=UTC)
FED = "<title>Meeting calendars and information</title><h4>2026 FOMC Meetings</h4>" + "".join(
    f'<div class="fomc-meeting__month"><strong>{month}</strong></div>'
    f'<div class="fomc-meeting__date">{days}</div>'
    for month, days in (
        ("January", "27-28"),
        ("March", "17-18*"),
        ("April", "28-29"),
        ("June", "16-17*"),
        ("July", "28-29"),
        ("September", "15-16*"),
        ("October", "27-28"),
        ("December", "8-9*"),
    )
)
BLS = """BEGIN:VCALENDAR
PRODID:-//Department of Labor//Bureau of Labor Statistics//EN
VERSION:2.0
BEGIN:VEVENT
UID:cpi-september
SEQUENCE:1
DTSTART;TZID=US-Eastern:20260911T083000
SUMMARY:Consumer Price Index
END:VEVENT
BEGIN:VEVENT
UID:last-release
SEQUENCE:1
DTSTART;TZID=US-Eastern:20261218T100000
SUMMARY:Employment report
END:VEVENT
END:VCALENDAR
"""


def halt_feed(
    *,
    symbol: str | None = None,
    published: str = "Tue, 08 Sep 2026 14:00:00 GMT",
    resume_date: str = "",
    resume_time: str = "",
) -> str:
    item = (
        ""
        if symbol is None
        else f"""
<item><ndaq:IssueSymbol>{symbol}</ndaq:IssueSymbol>
<ndaq:HaltDate>09/08/2026</ndaq:HaltDate><ndaq:HaltTime>09:45:00.000</ndaq:HaltTime>
<ndaq:ResumptionDate>{resume_date}</ndaq:ResumptionDate>
<ndaq:ResumptionTradeTime>{resume_time}</ndaq:ResumptionTradeTime>
<ndaq:ResumptionQuoteTime>09:50:00</ndaq:ResumptionQuoteTime></item>
"""
    )
    return f"""<?xml version="1.0"?>
<rss version="2.0" xmlns:ndaq="http://www.nasdaqtrader.com/"><channel>
<description>NASDAQ Trade Halts</description><pubDate>{published}</pubDate>
<ndaq:numItems>{int(symbol is not None)}</ndaq:numItems>{item}
</channel></rss>"""


def make_client(
    *,
    overrides: dict[str, str | int] | None = None,
    clock: Any = None,
    monotonic: Any = None,
    requests: list[httpx.Request] | None = None,
) -> OfficialContextClient:
    bodies: dict[str, str | int] = {
        FED_CALENDAR_URL: FED,
        BLS_CALENDAR_URL: BLS,
        NASDAQ_HALTS_URL: halt_feed(),
    }
    bodies.update(overrides or {})

    def handler(request: httpx.Request) -> httpx.Response:
        if requests is not None:
            requests.append(request)
        body = bodies[str(request.url)]
        if isinstance(body, int):
            return httpx.Response(body, text="Unavailable", headers={"Retry-After": "120"})
        return httpx.Response(200, text=body, headers={"Content-Type": "text/plain"})

    return OfficialContextClient(
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        clock=clock or (lambda: NOW),
        monotonic=monotonic or (lambda: 0.0),
    )


def test_official_context_actionable_with_immutable_actual_receipts() -> None:
    requests: list[httpx.Request] = []
    client = make_client(requests=requests)
    snapshot = client.poll(symbols=("AAPL", "MSFT", "NVDA"))
    assert snapshot.errors == ()
    assert snapshot.macro_calendar_available_at == NOW
    assert snapshot.macro_calendar_covers_until == datetime(2026, 12, 18, 15, tzinfo=UTC)
    assert datetime(2026, 9, 11, 12, 30, tzinfo=UTC) in snapshot.scheduled_macro_events
    assert len(snapshot.macro_risk_windows) == 8
    assert snapshot.halted_for("AAPL") is False
    assert snapshot.halted_for("MSFT") is False
    assert snapshot.halted_for("NVDA") is False
    assert snapshot.blocking_reasons(now=NOW) == ()
    assert len(requests) == 3
    for evidence in snapshot.evidence:
        assert evidence.received_at == NOW
        assert evidence.first_received_at == NOW
        assert sha256(base64.b64decode(evidence.body_base64)).hexdigest() == evidence.content_sha256
        assert evidence.http_status == 200
        with pytest.raises(ValidationError):
            evidence.content_sha256 = "0" * 64
    assert client.capabilities["iex_is_nbbo"] is False
    assert client.capabilities["paper_execution_authorized"] is False


@pytest.mark.parametrize("url", (FED_CALENDAR_URL, BLS_CALENDAR_URL, NASDAQ_HALTS_URL))
@pytest.mark.parametrize("status", (403, 429, 500, 302))
def test_no_healthy_or_clear_defaults_on_http_errors(url: str, status: int) -> None:
    client = make_client(overrides={url: status})
    snapshot = client.poll(symbols=("AAPL",))
    assert any(f"http_{status}" in error for error in snapshot.errors)
    if url == NASDAQ_HALTS_URL:
        assert snapshot.halted_for("AAPL") is None
    else:
        assert snapshot.macro_calendar_available_at is None
        assert snapshot.macro_calendar_covers_until is None
        assert snapshot.blocking_reasons(now=NOW)
    evidence = next(item for item in snapshot.evidence if item.source_url == url)
    assert evidence.http_status == status
    assert evidence.text == "Unavailable"


def test_halt_poll_minimum_one_minute_and_calendar_hour_cache() -> None:
    tick = [0.0]
    requests: list[httpx.Request] = []
    client = make_client(monotonic=lambda: tick[0], requests=requests)
    first = client.poll(symbols=("AAPL",))
    tick[0] = 59
    second = client.poll(symbols=("AAPL",))
    assert first == second
    assert len(requests) == 3
    tick[0] = 60
    client.poll(symbols=("AAPL",))
    assert len(requests) == 4
    assert str(requests[-1].url) == NASDAQ_HALTS_URL
    tick[0] = 3600
    client.poll(symbols=("AAPL",))
    assert len(requests) == 7


def test_retry_after_backoff_respected() -> None:
    tick = [0.0]
    requests: list[httpx.Request] = []
    client = make_client(
        overrides={NASDAQ_HALTS_URL: 429}, monotonic=lambda: tick[0], requests=requests
    )
    client.poll(symbols=("AAPL",))
    tick[0] = 61
    assert client.poll(symbols=("AAPL",)).halted_for("AAPL") is None
    assert len(requests) == 3
    tick[0] = 120
    client.poll(symbols=("AAPL",))
    assert len(requests) == 4


@pytest.mark.parametrize(
    ("resume_date", "resume_time", "expected"),
    [
        ("", "", True),
        ("09/08/2026", "", True),
        ("09/08/2026", "10:30:00", True),
        ("09/08/2026", "09:55:00", False),
    ],
)
def test_halt_resumption_is_trade_time_not_quote_time(
    resume_date: str, resume_time: str, expected: bool
) -> None:
    snapshot = make_client(
        overrides={
            NASDAQ_HALTS_URL: halt_feed(
                symbol="AAPL", resume_date=resume_date, resume_time=resume_time
            )
        }
    ).poll(symbols=("AAPL", "MSFT"))
    assert snapshot.halted_for("AAPL") is expected
    assert snapshot.halted_for("MSFT") is False


@pytest.mark.parametrize(
    "feed",
    [
        halt_feed(published="Tue, 08 Sep 2026 13:58:00 GMT"),
        halt_feed(published="Tue, 08 Sep 2026 14:01:00 GMT"),
        halt_feed().replace("<ndaq:numItems>0", "<ndaq:numItems>1"),
        halt_feed().replace("NASDAQ Trade Halts", "Unknown feed"),
        "<rss>truncated",
        '<!DOCTYPE rss [<!ENTITY example SYSTEM "https://attacker.invalid">]><rss/>',
    ],
)
def test_stale_truncated_or_wrong_identity_halt_feed_abstains(feed: str) -> None:
    snapshot = make_client(overrides={NASDAQ_HALTS_URL: feed}).poll(symbols=("AAPL",))
    assert snapshot.halted_for("AAPL") is None
    assert any("halt_schema:" in error for error in snapshot.errors)


def test_context_cannot_be_used_before_receipt_or_after_expiry() -> None:
    client = make_client()
    snapshot = client.poll(symbols=("AAPL",), now=NOW - timedelta(seconds=1))
    assert snapshot.halted_for("AAPL") is None
    assert snapshot.macro_calendar_available_at is None
    snapshot = client.poll(symbols=("AAPL",))
    assert snapshot.halted_for("AAPL", now=NOW + timedelta(seconds=91)) is None
    assert snapshot.blocking_reasons(now=NOW + timedelta(days=2))


def test_fomc_is_whole_day_risk_not_invented_announcement_time() -> None:
    later = datetime(2026, 9, 15, 15, tzinfo=UTC)
    snapshot = make_client(clock=lambda: later).poll(symbols=("AAPL",))
    assert snapshot.blocking_reasons(now=later) == ("official_macro_date_only_risk_window",)
    assert all(instant.date() != later.date() for instant in snapshot.scheduled_macro_events)
    window = next(item for item in snapshot.macro_risk_windows if item.start.month == 9)
    assert window.start == datetime(2026, 9, 15, 4, tzinfo=UTC)
    assert window.end == datetime(2026, 9, 17, 4, tzinfo=UTC)
    assert window.precision == "date"


@pytest.mark.parametrize(
    "ics",
    [
        BLS.replace("DTSTART;TZID=US-Eastern", "DTSTART;TZID=Unknown"),
        BLS.replace(
            "SUMMARY:Consumer Price Index", "RRULE:FREQ=DAILY\nSUMMARY:Consumer Price Index"
        ),
        BLS.replace("END:VCALENDAR", ""),
        BLS.replace("Bureau of Labor Statistics", "Unofficial Calendar"),
    ],
)
def test_unsupported_bls_schemas_remain_unknown(ics: str) -> None:
    snapshot = make_client(overrides={BLS_CALENDAR_URL: ics}).poll(symbols=("AAPL",))
    assert snapshot.macro_calendar_available_at is None
    assert any("macro_schema:" in error for error in snapshot.errors)


def test_latest_rest_size_contract_is_endpoint_and_date_scoped() -> None:
    assert latest_rest_quote_size_unit(feed="iex", quote_at=NOW) == "shares"
    assert latest_rest_quote_size_unit(feed="sip", quote_at=NOW) == "shares"
    assert (
        latest_rest_quote_size_unit(
            feed="iex", quote_at=NOW, endpoint="wss://stream.data.alpaca.markets/v2/iex"
        )
        == "unknown"
    )
    assert (
        latest_rest_quote_size_unit(feed="iex", quote_at=datetime(2025, 10, 31, 15, tzinfo=UTC))
        == "unknown"
    )
    assert latest_rest_quote_size_unit(feed="unknown", quote_at=NOW) == "unknown"


def test_context_rejects_changed_hash_and_unsupported_symbols() -> None:
    response = make_client().poll(symbols=("AAPL",)).evidence[0]
    payload = response.model_dump()
    payload["content_sha256"] = "0" * 64
    with pytest.raises(ValidationError, match="hash_mismatch"):
        ContextResponse.model_validate(payload)
    with pytest.raises(ValueError, match="only"):
        make_client().poll(symbols=("SPY",))
