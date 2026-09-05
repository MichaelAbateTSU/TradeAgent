from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
from pydantic import SecretStr

from tradeagent.notifications import (
    EmailDeliveryError,
    EmailSettings,
    NotificationDispatcher,
    NotificationStatus,
    OutboxMessage,
    ResendEmailProvider,
    RoundTripNotificationRepository,
)
from tradeagent.persistence import Database


class FakeProvider:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.messages: list[OutboxMessage] = []

    def send(self, message: OutboxMessage) -> str:
        self.messages.append(message)
        if self.fail:
            raise EmailDeliveryError("provider unavailable")
        return "provider-message-1"


def _repository(tmp_path: Path) -> tuple[Database, RoundTripNotificationRepository]:
    database = Database(f"sqlite:///{tmp_path / 'notifications.db'}")
    database.initialize()
    return database, RoundTripNotificationRepository(database)


def test_closed_cycle_enqueues_exactly_one_notification(tmp_path: Path) -> None:
    database, repository = _repository(tmp_path)
    opened_at = datetime(2026, 1, 2, 15, tzinfo=UTC)
    cycle_id = repository.open_cycle(
        strategy_version="opening-range-v1",
        symbol="SPY",
        opened_at=opened_at,
        quantity=Decimal("2"),
        opening_vwap=Decimal("100"),
        fees=Decimal("0.10"),
    )

    first = repository.close_cycle_and_enqueue(
        cycle_id,
        closed_at=opened_at + timedelta(minutes=30),
        closing_vwap=Decimal("101"),
        closing_fees=Decimal("0.10"),
    )
    second = repository.close_cycle_and_enqueue(
        cycle_id,
        closed_at=opened_at + timedelta(minutes=30),
        closing_vwap=Decimal("101"),
        closing_fees=Decimal("0.10"),
    )

    assert first == second
    assert repository.count() == 1
    message = repository.claim_next()
    assert message is not None
    assert message.payload["outcome"] == "profit"
    assert Decimal(message.payload["realized_pnl"]) == Decimal("1.80")
    database.dispose()


def test_dispatcher_marks_sent_and_does_not_resend(tmp_path: Path) -> None:
    database, repository = _repository(tmp_path)
    now = datetime(2026, 1, 2, 15, tzinfo=UTC)
    cycle_id = repository.open_cycle(
        strategy_version="strategy-v1",
        symbol="SPY",
        opened_at=now,
        quantity=Decimal("1"),
        opening_vwap=Decimal("100"),
        fees=Decimal(0),
    )
    notification_id = repository.close_cycle_and_enqueue(
        cycle_id,
        closed_at=now + timedelta(minutes=5),
        closing_vwap=Decimal("101"),
        closing_fees=Decimal(0),
    )
    provider = FakeProvider()
    dispatcher = NotificationDispatcher(repository, provider)

    assert dispatcher.dispatch_one()
    assert not dispatcher.dispatch_one()
    assert repository.status(notification_id) is NotificationStatus.SENT
    assert len(provider.messages) == 1
    database.dispose()


def test_failed_delivery_is_retried_with_same_idempotency_key(tmp_path: Path) -> None:
    database, repository = _repository(tmp_path)
    now = datetime(2026, 1, 2, 15, tzinfo=UTC)
    cycle_id = repository.open_cycle(
        strategy_version="strategy-v1",
        symbol="SPY",
        opened_at=now,
        quantity=Decimal("1"),
        opening_vwap=Decimal("100"),
        fees=Decimal(0),
    )
    notification_id = repository.close_cycle_and_enqueue(
        cycle_id,
        closed_at=now + timedelta(minutes=5),
        closing_vwap=Decimal("99"),
        closing_fees=Decimal(0),
    )
    failing = FakeProvider(fail=True)
    with pytest.raises(EmailDeliveryError, match="unavailable"):
        NotificationDispatcher(repository, failing).dispatch_one()

    assert repository.status(notification_id) is NotificationStatus.FAILED
    succeeding = FakeProvider()
    assert NotificationDispatcher(repository, succeeding).dispatch_one()
    assert succeeding.messages[0].notification_id == notification_id
    assert succeeding.messages[0].attempts == 2
    database.dispose()


def test_resend_provider_uses_notification_id_as_idempotency_key() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["authorization"] = request.headers["Authorization"]
        captured["idempotency"] = request.headers["Idempotency-Key"]
        captured["body"] = json.loads(request.read())
        return httpx.Response(200, json={"id": "email-1"})

    settings = EmailSettings(
        api_key=SecretStr("email-secret"),
        sender="TradeAgent <paper@example.com>",
        recipient="owner@example.com",
    )
    database = Database("sqlite:///:memory:")
    database.initialize()
    repository = RoundTripNotificationRepository(database)
    now = datetime(2026, 1, 2, 15, tzinfo=UTC)
    cycle_id = repository.open_cycle(
        strategy_version="strategy-v1",
        symbol="SPY",
        opened_at=now,
        quantity=Decimal("1"),
        opening_vwap=Decimal("100"),
        fees=Decimal(0),
    )
    repository.close_cycle_and_enqueue(
        cycle_id,
        closed_at=now + timedelta(minutes=5),
        closing_vwap=Decimal("101"),
        closing_fees=Decimal(0),
    )
    message = repository.claim_next()
    assert message is not None
    provider = ResendEmailProvider(
        settings,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    assert provider.send(message) == "email-1"
    assert captured["authorization"] == "Bearer email-secret"
    assert captured["idempotency"] == str(message.notification_id)
    assert captured["body"]["to"] == ["owner@example.com"]  # type: ignore[index]
    database.dispose()
