from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
from pydantic import SecretStr, ValidationError
from sqlalchemy import select

from tradeagent.daily_status import (
    DailyStatusScheduler,
    DailyStatusSettings,
    build_daily_status,
    daily_notification_id,
)
from tradeagent.event_store import EventStore
from tradeagent.notifications import (
    EmailDeliveryError,
    EmailSettings,
    NotificationDispatcher,
    NotificationStatus,
    ResendEmailProvider,
    RoundTripNotificationRepository,
)
from tradeagent.notifier import NotifierService
from tradeagent.persistence import Database, ProductionRepository, notification_outbox

SUNDAY = datetime(2026, 9, 6, 22, tzinfo=UTC)  # 18:00 Eastern, even on weekends.


@pytest.fixture
def database(tmp_path: Path):
    with Database(f"sqlite:///{tmp_path / 'daily.db'}") as db:
        db.initialize()
        yield db


def test_schedule_runs_daily_at_six_eastern_and_survives_restart(database: Database):
    settings = DailyStatusSettings(_env_file=None)
    scheduler = DailyStatusScheduler(database, settings)
    assert not scheduler.enqueue_due(observed_at=SUNDAY - timedelta(seconds=1))
    assert scheduler.enqueue_due(observed_at=SUNDAY)
    assert not scheduler.enqueue_due(observed_at=SUNDAY + timedelta(hours=2))
    restarted = DailyStatusScheduler(database, settings)
    # UTC date has changed, but the report is still Sunday's in Eastern time.
    assert not restarted.enqueue_due(observed_at=SUNDAY + timedelta(hours=5))
    assert not restarted.enqueue_due(observed_at=SUNDAY + timedelta(hours=6))
    assert restarted.enqueue_due(observed_at=SUNDAY + timedelta(days=1))
    with database.begin() as connection:
        rows = list(connection.execute(select(notification_outbox)).mappings())
    assert len(rows) == 2
    assert all(row["cycle_id"] is None for row in rows)
    assert {row["payload"]["local_date"] for row in rows} == {"2026-09-06", "2026-09-07"}


@pytest.mark.parametrize(
    "utc_time",
    [
        datetime(2026, 3, 8, 22, tzinfo=UTC),  # DST start, 18 EDT
        datetime(2026, 11, 1, 23, tzinfo=UTC),  # DST end, 18 EST
    ],
)
def test_schedule_tracks_daylight_saving_time(database: Database, utc_time):
    scheduler = DailyStatusScheduler(database, DailyStatusSettings(_env_file=None))
    assert not scheduler.enqueue_due(observed_at=utc_time - timedelta(seconds=1))
    assert scheduler.enqueue_due(observed_at=utc_time)


def test_late_start_enqueues_only_today_not_historical_backlog(database: Database):
    scheduler = DailyStatusScheduler(database, DailyStatusSettings(_env_file=None))
    late = SUNDAY + timedelta(days=8, hours=3)
    assert scheduler.enqueue_due(observed_at=late)
    assert scheduler.outbox.count() == 1
    assert scheduler.outbox.contains(daily_notification_id(date(2026, 9, 14), "America/New_York"))


def test_schedule_validates_time_and_can_be_disabled(database: Database):
    with pytest.raises(ValidationError, match="IANA"):
        DailyStatusSettings(timezone="Imaginary/Zone", _env_file=None)
    scheduler = DailyStatusScheduler(database, DailyStatusSettings(enabled=False, _env_file=None))
    assert not scheduler.enqueue_due(observed_at=SUNDAY)
    with pytest.raises(ValueError, match="timezone-aware"):
        scheduler.enqueue_due(observed_at=SUNDAY.replace(tzinfo=None))


def test_status_uses_actual_cohort_today_counts_and_latest_scoped_pnl(database: Database):
    repo = ProductionRepository(database)
    store = EventStore(database)
    now = SUNDAY + timedelta(hours=5)  # Local Sunday, UTC Monday.
    store.freeze(
        "actual-cohort",
        "config",
        {"settings": {"max_entry_notional": "25"}},
        "shadow",
        now - timedelta(days=2),
    )
    repo.heartbeat(
        "tradeagent-event-worker",
        "owner",
        {
            "cohort_id": "actual-cohort",
            "mode": "shadow",
            "state": "market_closed",
            "market_phase": "closed",
            "next_open": "2026-09-08T09:30:00-04:00",
            "code_sha": "pinned-agent-code",
            "config_hash": "config",
            "blockers": ["IEX_SHADOW_ONLY_FROZEN_POLICY", "NO_CONFIGURED_INFERENCE_PROVIDER"],
            "source_capabilities": {"last_errors": ["sec:AAPL:source_error"]},
        },
        observed_at=now,
    )
    for key, when, action in (
        ("today", SUNDAY, "abstain"),
        ("yesterday", now - timedelta(days=1), "eligible"),
        ("future", now + timedelta(minutes=30), "eligible"),
    ):
        store.evidence(key, {}, when)
        store.decision(
            "actual-cohort", key, {"action": action, "reasons": ["primary_missing"]}, when
        )
    store.audit(
        "performance",
        {
            "positions": {},
            "broker_paper_pnl": "0.50",
            "economic_paper_pnl": "-0.25",
            "closed_round_trips": 1,
            "fixed_service_cost_usd": None,
        },
        now,
        "actual-cohort",
    )
    store.audit("performance", {"broker_paper_pnl": "999999"}, now, "different-cohort")
    repo.set_control("actual-cohort:pause", "SOURCE_REVIEW")
    summary = build_daily_status(database, now, "America/New_York")
    assert summary["local_date"] == "2026-09-06"
    text = summary["text"]
    assert "Decisions: 1; eligible: 0; abstained: 1" in text
    assert "Economic-paper cumulative P&L: -0.25 USD" in text
    assert "999999" not in text
    assert "pinned-agent-code" in text
    assert "Resolve latest-SIP access" in text
    assert "deterministic extractor" in text
    assert "Reconcile pending orders" in text
    assert "Resolve the reported source errors" in text
    assert "Fixed service costs: unknown / not recorded" in text
    assert "No profitability or alpha claim" in text


def test_missing_or_stale_worker_is_reported_not_pretended_healthy(database: Database):
    result = build_daily_status(database, SUNDAY, "America/New_York")
    assert "Event worker: stale_or_missing" in result["text"]
    assert "Restore the event worker" in result["text"]
    assert "Broker-paper cumulative P&L: unknown / not recorded" in result["text"]
    ProductionRepository(database).heartbeat(
        "tradeagent-event-worker",
        "old",
        {"mode": "experimental-paper", "state": "collecting"},
        observed_at=SUNDAY - timedelta(minutes=10),
    )
    assert (
        "Event worker: stale_or_missing"
        in build_daily_status(database, SUNDAY, "America/New_York")["text"]
    )


class Provider:
    def __init__(self):
        self.messages = []

    def send(self, message):
        self.messages.append(message)
        return "provider-accepted-id"


def test_render_notifier_enqueues_daily_and_reuses_existing_provider(database: Database):
    outbox = RoundTripNotificationRepository(database)
    provider = Provider()
    scheduler = DailyStatusScheduler(database, DailyStatusSettings(_env_file=None))
    notifier = NotifierService(
        NotificationDispatcher(outbox, provider),
        ProductionRepository(database),
        instance_id="email-worker",
        clock=lambda: SUNDAY,
        daily_scheduler=scheduler,
    )
    assert notifier.run_once()
    assert not notifier.run_once()
    assert len(provider.messages) == 1
    message = provider.messages[0]
    assert message.cycle_id is None
    assert message.notification_type == "daily_agent_status"
    assert outbox.status(message.notification_id) == NotificationStatus.SENT


def test_status_uniqueness_does_not_make_fictional_position_cycles(database: Database):
    outbox = RoundTripNotificationRepository(database)
    identity = daily_notification_id(SUNDAY.date(), "America/New_York")
    payload = {"subject": "daily", "text": "status"}
    assert outbox.enqueue_status(identity, payload, created_at=SUNDAY)
    assert not outbox.enqueue_status(identity, {"subject": "overwrite"}, created_at=SUNDAY)
    message = outbox.claim_next(observed_at=SUNDAY)
    assert message is not None and message.payload == payload and message.cycle_id is None


def test_interrupted_claim_recovers_with_same_id_and_old_unknown_is_not_resent(database: Database):
    outbox = RoundTripNotificationRepository(database)
    identity = daily_notification_id(SUNDAY.date(), "America/New_York")
    outbox.enqueue_status(identity, {"subject": "daily", "text": "status"}, created_at=SUNDAY)
    first = outbox.claim_next(observed_at=SUNDAY)
    assert first is not None
    assert outbox.claim_next(observed_at=SUNDAY + timedelta(seconds=60)) is None
    retry = outbox.claim_next(observed_at=SUNDAY + timedelta(seconds=121))
    assert retry is not None
    assert retry.notification_id == first.notification_id
    assert retry.attempts == 2
    assert outbox.claim_next(observed_at=SUNDAY + timedelta(days=2)) is None
    assert outbox.status(identity) is NotificationStatus.NEEDS_REVIEW


def test_unknown_transport_outcome_remains_recoverable_sending(database: Database):
    now = datetime.now(UTC)
    outbox = RoundTripNotificationRepository(database)
    identity = daily_notification_id(now.date(), "America/New_York")
    outbox.enqueue_status(identity, {"subject": "daily", "text": "status"}, created_at=now)

    def timeout(request):
        raise httpx.ReadTimeout("synthetic lost acknowledgement", request=request)

    settings = EmailSettings(
        api_key=SecretStr("fixture"), sender="fixture@example.org", recipient="owner@example.org"
    )
    with httpx.Client(transport=httpx.MockTransport(timeout)) as client:
        dispatcher = NotificationDispatcher(outbox, ResendEmailProvider(settings, client=client))
        with pytest.raises(EmailDeliveryError) as captured:
            dispatcher.dispatch_one()
    assert captured.value.outcome_unknown
    assert outbox.status(identity) is NotificationStatus.SENDING


def test_round_trip_delivery_still_works_next_to_daily_messages(database: Database):
    outbox = RoundTripNotificationRepository(database)
    cycle = outbox.open_cycle(
        strategy_version="fixture",
        symbol="AAPL",
        opened_at=SUNDAY,
        quantity=Decimal("0.1"),
        opening_vwap=Decimal(100),
        fees=Decimal(0),
    )
    outbox.close_cycle_and_enqueue(
        cycle, closed_at=SUNDAY, closing_vwap=Decimal(101), closing_fees=Decimal(0)
    )
    DailyStatusScheduler(database, DailyStatusSettings(_env_file=None)).enqueue_due(
        observed_at=SUNDAY
    )
    provider = Provider()
    dispatcher = NotificationDispatcher(outbox, provider)
    assert dispatcher.dispatch_one()
    assert dispatcher.dispatch_one()
    assert not dispatcher.dispatch_one()
    assert {message.notification_type for message in provider.messages} == {
        "round_trip_closed",
        "daily_agent_status",
    }
