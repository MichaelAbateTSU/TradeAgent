from datetime import UTC, datetime, timedelta
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import select

from tradeagent.daily_status import DailyStatusScheduler
from tradeagent.email_schedule import DAILY_SUMMARY_FORMAT, DailyEmailPolicy, DailyStatusSettings
from tradeagent.notifications import (
    EmailDeliveryError,
    NotificationDispatcher,
    NotificationStatus,
    RoundTripNotificationRepository,
)
from tradeagent.persistence import Database, notification_outbox

AT_SIX = datetime(2026, 9, 11, 22, tzinfo=UTC)


class Provider:
    def __init__(self):
        self.messages = []

    def send(self, message):
        self.messages.append(message)
        return "accepted-once"


def daily_payload(at=AT_SIX):
    return {
        "subject": "Daily summary",
        "text": "\n\n".join(f"This is readable paragraph {number}." for number in range(1, 6)),
        "summary_format": DAILY_SUMMARY_FORMAT,
        "local_date": at.astimezone(ZoneInfo("America/New_York")).date().isoformat(),
        "timezone": "America/New_York",
    }


@pytest.fixture
def setup(tmp_path):
    with Database(f"sqlite:///{tmp_path / 'daily-policy.db'}") as database:
        database.initialize()
        repository = RoundTripNotificationRepository(database)
        clock = [AT_SIX - timedelta(seconds=1)]
        provider = Provider()
        settings = DailyStatusSettings(_env_file=None)
        dispatcher = NotificationDispatcher(
            repository, provider, daily_settings=settings, clock=lambda: clock[0]
        )
        yield database, repository, clock, provider, dispatcher, DailyEmailPolicy(settings)


def test_only_daily_identity_sends_at_six_and_does_not_repeat(setup):
    _, repo, clock, provider, dispatch, policy = setup
    startup, trade, digest = uuid4(), uuid4(), uuid4()
    for identity in (startup, trade, digest):
        repo.enqueue_status(
            identity, {"subject": "No longer wanted", "text": "raw details"}, created_at=clock[0]
        )
    identity = policy.current_id(AT_SIX)
    repo.enqueue_status(identity, daily_payload(), created_at=clock[0])
    assert dispatch.dispatch_one() is False
    assert provider.messages == []
    assert all(
        repo.status(item) is NotificationStatus.SUPPRESSED for item in (startup, trade, digest)
    )
    clock[0] = AT_SIX
    assert dispatch.dispatch_one() is True
    assert dispatch.dispatch_one() is False
    assert len(provider.messages) == 1
    assert provider.messages[0].notification_id == identity
    assert repo.status(identity) is NotificationStatus.SENT
    restarted = NotificationDispatcher(repo, provider, clock=lambda: clock[0])
    assert restarted.dispatch_one() is False


def test_old_pending_email_does_not_become_a_morning_backlog(setup):
    _, repo, clock, provider, dispatch, policy = setup
    yesterday = AT_SIX - timedelta(days=1)
    old_id = policy.current_id(yesterday)
    repo.enqueue_status(old_id, daily_payload(yesterday), created_at=yesterday)
    clock[0] = AT_SIX - timedelta(hours=9)
    assert dispatch.dispatch_one() is False
    assert repo.status(old_id) is NotificationStatus.SUPPRESSED
    assert provider.messages == []


def test_accepted_and_ambiguous_non_daily_messages_are_never_resent(setup):
    _, repo, clock, provider, dispatch, _ = setup
    sent, ambiguous = uuid4(), uuid4()
    repo.enqueue_status(sent, {"text": "old"}, created_at=clock[0])
    repo.mark_sent(sent, "original-provider-id")
    repo.enqueue_status(ambiguous, {"text": "unknown"}, created_at=clock[0])
    claimed = repo.claim_next(observed_at=clock[0])
    assert claimed is not None and claimed.notification_id == ambiguous
    clock[0] = AT_SIX + timedelta(minutes=10)
    assert dispatch.dispatch_one() is False
    assert provider.messages == []
    assert repo.status(sent) is NotificationStatus.SENT
    assert repo.status(ambiguous) is NotificationStatus.SENDING


def test_concurrent_digest_enqueue_cannot_escape_daily_claim_filter(setup, monkeypatch):
    _, repo, clock, provider, dispatch, policy = setup
    identity = policy.current_id(AT_SIX)
    repo.enqueue_status(identity, daily_payload(), created_at=AT_SIX)
    original = repo.suppress_non_daily
    rogue = uuid4()

    def raced(*args, **kwargs):
        result = original(*args, **kwargs)
        repo.enqueue_status(
            rogue,
            {"subject": "digest", "text": "not daily"},
            created_at=AT_SIX - timedelta(hours=1),
        )
        return result

    monkeypatch.setattr(repo, "suppress_non_daily", raced)
    clock[0] = AT_SIX
    assert dispatch.dispatch_one()
    assert [message.notification_id for message in provider.messages] == [identity]


def test_malformed_daily_body_is_not_delivered(setup):
    _, repo, clock, provider, dispatch, policy = setup
    identity = policy.current_id(AT_SIX)
    repo.enqueue_status(
        identity, {**daily_payload(), "text": "Only one paragraph."}, created_at=AT_SIX
    )
    clock[0] = AT_SIX
    with pytest.raises(EmailDeliveryError, match="five-paragraph"):
        dispatch.dispatch_one()
    assert provider.messages == []
    assert repo.status(identity) is NotificationStatus.NEEDS_REVIEW


def test_disabled_daily_schedule_never_falls_back_to_other_emails(setup):
    _, repo, clock, provider, _, policy = setup
    identity = policy.current_id(AT_SIX)
    repo.enqueue_status(identity, daily_payload(), created_at=AT_SIX)
    clock[0] = AT_SIX
    dispatcher = NotificationDispatcher(
        repo,
        provider,
        daily_settings=DailyStatusSettings(enabled=False, _env_file=None),
        clock=lambda: clock[0],
    )
    assert dispatcher.dispatch_one() is False
    assert provider.messages == []
    assert repo.status(identity) is NotificationStatus.SUPPRESSED


def test_unattempted_legacy_daily_is_refreshed_without_replaying_an_accepted_email(setup):
    database, repo, clock, provider, dispatch, policy = setup
    identity = policy.current_id(AT_SIX)
    repo.enqueue_status(
        identity, {"subject": "Legacy", "text": '{"large":"old report"}'}, created_at=AT_SIX
    )
    scheduler = DailyStatusScheduler(database, policy.settings)
    assert scheduler.enqueue_due(observed_at=AT_SIX)
    with database.begin() as connection:
        row = (
            connection.execute(
                select(notification_outbox).where(
                    notification_outbox.c.notification_id == str(identity)
                )
            )
            .mappings()
            .one()
        )
    assert row["payload"]["summary_format"] == DAILY_SUMMARY_FORMAT
    assert len(row["payload"]["text"].split("\n\n")) == 5
    assert row["attempts"] == 0
    clock[0] = AT_SIX
    assert dispatch.dispatch_one()
    assert DailyStatusScheduler(database, policy.settings).enqueue_due(observed_at=AT_SIX) is False
    assert len(provider.messages) == 1


@pytest.mark.parametrize(
    "end,hours",
    [
        (datetime(2026, 3, 8, 22, tzinfo=UTC), 23),
        (datetime(2026, 11, 1, 23, tzinfo=UTC), 25),
    ],
)
def test_reporting_windows_cover_overnight_activity_across_dst(end, hours):
    policy = DailyEmailPolicy(DailyStatusSettings(_env_file=None))
    start, actual_end = policy.reporting_window(end)
    assert actual_end == end
    assert (actual_end - start).total_seconds() == hours * 3600
