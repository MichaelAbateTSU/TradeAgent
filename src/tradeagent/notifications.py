from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Literal, Protocol, cast
from uuid import UUID, uuid4

import httpx
from pydantic import BaseModel, ConfigDict, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import insert, select, update

from tradeagent.persistence import Database, notification_outbox, position_cycles


class RoundTripOutcome(StrEnum):
    PROFIT = "profit"
    LOSS = "loss"
    FLAT = "flat"


class NotificationStatus(StrEnum):
    PENDING = "pending"
    SENDING = "sending"
    SENT = "sent"
    FAILED = "failed"


class RoundTripEmail(BaseModel):
    model_config = ConfigDict(frozen=True)

    cycle_id: UUID
    notification_id: UUID
    symbol: str
    strategy_version: str
    outcome: RoundTripOutcome
    realized_pnl: Decimal
    opening_vwap: Decimal
    closing_vwap: Decimal
    quantity: Decimal
    fees: Decimal
    opened_at: datetime
    closed_at: datetime
    subject: str
    text: str


class OutboxMessage(BaseModel):
    model_config = ConfigDict(frozen=True)

    notification_id: UUID
    cycle_id: UUID
    notification_type: str
    payload: dict[str, Any]
    status: NotificationStatus
    attempts: int = Field(ge=0)


class EmailProvider(Protocol):
    def send(self, message: OutboxMessage) -> str: ...


class EmailDeliveryError(RuntimeError):
    pass


class EmailSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="EMAIL_",
        env_file=".env",
        env_file_encoding="utf-8",
        frozen=True,
        extra="ignore",
    )

    api_key: SecretStr
    sender: str
    recipient: str
    provider: Literal["resend"] = "resend"
    resend_url: Literal["https://api.resend.com/emails"] = "https://api.resend.com/emails"


class ResendEmailProvider:
    def __init__(
        self,
        settings: EmailSettings,
        *,
        client: httpx.Client | None = None,
    ) -> None:
        self._settings = settings
        self._client = client or httpx.Client(timeout=30)
        self._owns_client = client is None

    def send(self, message: OutboxMessage) -> str:
        try:
            response = self._client.post(
                self._settings.resend_url,
                headers={
                    "Authorization": (f"Bearer {self._settings.api_key.get_secret_value()}"),
                    "Idempotency-Key": str(message.notification_id),
                },
                json={
                    "from": self._settings.sender,
                    "to": [self._settings.recipient],
                    "subject": str(message.payload["subject"]),
                    "text": str(message.payload["text"]),
                },
            )
            response.raise_for_status()
        except httpx.HTTPError as error:
            raise EmailDeliveryError("email provider request failed") from error
        payload = response.json()
        message_id = payload.get("id")
        if not message_id:
            raise EmailDeliveryError("email provider response did not contain an id")
        return str(message_id)

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> ResendEmailProvider:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


class RoundTripNotificationRepository:
    def __init__(self, database: Database) -> None:
        self._database = database

    def open_cycle(
        self,
        *,
        strategy_version: str,
        symbol: str,
        opened_at: datetime,
        quantity: Decimal,
        opening_vwap: Decimal,
        fees: Decimal,
    ) -> UUID:
        cycle_id = uuid4()
        with self._database.begin() as connection:
            connection.execute(
                insert(position_cycles).values(
                    cycle_id=str(cycle_id),
                    strategy_version=strategy_version,
                    symbol=symbol.upper(),
                    opened_at=opened_at,
                    closed_at=None,
                    opening_quantity=quantity,
                    opening_vwap=opening_vwap,
                    closing_vwap=None,
                    fees=fees,
                    realized_pnl=None,
                    outcome=None,
                    status="open",
                )
            )
        return cycle_id

    def close_cycle_and_enqueue(
        self,
        cycle_id: UUID,
        *,
        closed_at: datetime,
        closing_vwap: Decimal,
        closing_fees: Decimal,
    ) -> UUID:
        with self._database.begin() as connection:
            row = (
                connection.execute(
                    select(position_cycles).where(position_cycles.c.cycle_id == str(cycle_id))
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                raise KeyError(f"position cycle {cycle_id} does not exist")
            existing = connection.execute(
                select(notification_outbox.c.notification_id).where(
                    notification_outbox.c.cycle_id == str(cycle_id),
                    notification_outbox.c.notification_type == "round_trip_closed",
                )
            ).scalar_one_or_none()
            if existing is not None:
                return UUID(str(existing))

            quantity = Decimal(str(row["opening_quantity"]))
            opening_vwap = Decimal(str(row["opening_vwap"]))
            opening_fees = Decimal(str(row["fees"]))
            total_fees = opening_fees + closing_fees
            realized_pnl = (closing_vwap - opening_vwap) * quantity - total_fees
            outcome = (
                RoundTripOutcome.PROFIT
                if realized_pnl > 0
                else RoundTripOutcome.LOSS
                if realized_pnl < 0
                else RoundTripOutcome.FLAT
            )
            notification_id = uuid4()
            email = _round_trip_email(
                cycle_id=cycle_id,
                notification_id=notification_id,
                symbol=str(row["symbol"]),
                strategy_version=str(row["strategy_version"]),
                outcome=outcome,
                realized_pnl=realized_pnl,
                opening_vwap=opening_vwap,
                closing_vwap=closing_vwap,
                quantity=quantity,
                fees=total_fees,
                opened_at=cast(datetime, row["opened_at"]),
                closed_at=closed_at,
            )
            connection.execute(
                update(position_cycles)
                .where(position_cycles.c.cycle_id == str(cycle_id))
                .values(
                    closed_at=closed_at,
                    closing_vwap=closing_vwap,
                    fees=total_fees,
                    realized_pnl=realized_pnl,
                    outcome=outcome.value,
                    status="reconciled",
                )
            )
            connection.execute(
                insert(notification_outbox).values(
                    notification_id=str(notification_id),
                    cycle_id=str(cycle_id),
                    notification_type="round_trip_closed",
                    payload=email.model_dump(mode="json"),
                    status=NotificationStatus.PENDING.value,
                    attempts=0,
                    provider_message_id=None,
                    created_at=datetime.now(UTC),
                    sent_at=None,
                )
            )
        return notification_id

    def claim_next(self) -> OutboxMessage | None:
        with self._database.begin() as connection:
            row = (
                connection.execute(
                    select(notification_outbox)
                    .where(
                        notification_outbox.c.status.in_(
                            [
                                NotificationStatus.PENDING.value,
                                NotificationStatus.FAILED.value,
                            ]
                        )
                    )
                    .order_by(notification_outbox.c.created_at)
                    .with_for_update(skip_locked=True)
                    .limit(1)
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                return None
            attempts = int(row["attempts"]) + 1
            connection.execute(
                update(notification_outbox)
                .where(notification_outbox.c.notification_id == row["notification_id"])
                .values(
                    status=NotificationStatus.SENDING.value,
                    attempts=attempts,
                )
            )
            return OutboxMessage(
                notification_id=UUID(str(row["notification_id"])),
                cycle_id=UUID(str(row["cycle_id"])),
                notification_type=str(row["notification_type"]),
                payload=dict(row["payload"]),
                status=NotificationStatus.SENDING,
                attempts=attempts,
            )

    def mark_sent(self, notification_id: UUID, provider_message_id: str) -> None:
        with self._database.begin() as connection:
            connection.execute(
                update(notification_outbox)
                .where(notification_outbox.c.notification_id == str(notification_id))
                .values(
                    status=NotificationStatus.SENT.value,
                    provider_message_id=provider_message_id,
                    sent_at=datetime.now(UTC),
                )
            )

    def mark_failed(self, notification_id: UUID) -> None:
        with self._database.begin() as connection:
            connection.execute(
                update(notification_outbox)
                .where(notification_outbox.c.notification_id == str(notification_id))
                .values(status=NotificationStatus.FAILED.value)
            )

    def status(self, notification_id: UUID) -> NotificationStatus:
        with self._database.begin() as connection:
            value = connection.scalar(
                select(notification_outbox.c.status).where(
                    notification_outbox.c.notification_id == str(notification_id)
                )
            )
        if value is None:
            raise KeyError(f"notification {notification_id} does not exist")
        return NotificationStatus(str(value))

    def count(self) -> int:
        with self._database.begin() as connection:
            rows = connection.execute(select(notification_outbox.c.notification_id))
            return len(rows.all())


class NotificationDispatcher:
    def __init__(
        self,
        repository: RoundTripNotificationRepository,
        provider: EmailProvider,
    ) -> None:
        self._repository = repository
        self._provider = provider

    def dispatch_one(self) -> bool:
        message = self._repository.claim_next()
        if message is None:
            return False
        try:
            provider_message_id = self._provider.send(message)
        except EmailDeliveryError:
            self._repository.mark_failed(message.notification_id)
            raise
        self._repository.mark_sent(message.notification_id, provider_message_id)
        return True


def _round_trip_email(
    *,
    cycle_id: UUID,
    notification_id: UUID,
    symbol: str,
    strategy_version: str,
    outcome: RoundTripOutcome,
    realized_pnl: Decimal,
    opening_vwap: Decimal,
    closing_vwap: Decimal,
    quantity: Decimal,
    fees: Decimal,
    opened_at: datetime,
    closed_at: datetime,
) -> RoundTripEmail:
    label = outcome.value.upper()
    subject = f"[PAPER] {symbol} round trip closed: {label} {realized_pnl:+.4f}"
    text = "\n".join(
        [
            "PAPER TRADING RESULT",
            f"Outcome: {label}",
            f"Symbol: {symbol}",
            f"Strategy: {strategy_version}",
            f"Quantity: {quantity}",
            f"Opening VWAP: {opening_vwap}",
            f"Closing VWAP: {closing_vwap}",
            f"Fees: {fees}",
            f"Net realized P&L: {realized_pnl:+.4f}",
            f"Opened: {opened_at.isoformat()}",
            f"Closed: {closed_at.isoformat()}",
            f"Cycle: {cycle_id}",
        ]
    )
    return RoundTripEmail(
        cycle_id=cycle_id,
        notification_id=notification_id,
        symbol=symbol,
        strategy_version=strategy_version,
        outcome=outcome,
        realized_pnl=realized_pnl,
        opening_vwap=opening_vwap,
        closing_vwap=closing_vwap,
        quantity=quantity,
        fees=fees,
        opened_at=opened_at,
        closed_at=closed_at,
        subject=subject,
        text=text,
    )
