from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from uuid import NAMESPACE_URL, UUID, uuid5
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

DAILY_SUMMARY_FORMAT = "five-paragraph-daily-summary-v1"


class DailyStatusSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="EMAIL_DAILY_", env_file=".env", extra="ignore", frozen=True
    )

    enabled: bool = True
    timezone: str = "America/New_York"
    hour: int = Field(default=18, ge=0, le=23)
    minute: int = Field(default=0, ge=0, le=59)

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except ZoneInfoNotFoundError as error:
            raise ValueError("daily email timezone must be a valid IANA timezone") from error
        return value


def daily_notification_id(day: date, timezone: str) -> UUID:
    return uuid5(NAMESPACE_URL, f"tradeagent:daily-agent-status:{timezone}:{day.isoformat()}")


@dataclass(frozen=True)
class DailyEmailPolicy:
    settings: DailyStatusSettings

    def current_id(self, now: datetime) -> UUID:
        local = self.local_time(now)
        return daily_notification_id(local.date(), self.settings.timezone)

    def local_time(self, now: datetime) -> datetime:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("email policy requires an aware clock")
        return now.astimezone(ZoneInfo(self.settings.timezone))

    def eligible_id(self, now: datetime) -> UUID | None:
        local = self.local_time(now)
        if not self.settings.enabled or local.time() < time(
            self.settings.hour, self.settings.minute
        ):
            return None
        return self.current_id(now)

    def reporting_window(self, now: datetime) -> tuple[datetime, datetime]:
        local = self.local_time(now)
        zone = ZoneInfo(self.settings.timezone)
        scheduled_end = datetime.combine(
            local.date(), time(self.settings.hour, self.settings.minute), zone
        )
        start = datetime.combine(
            local.date() - timedelta(days=1), time(self.settings.hour, self.settings.minute), zone
        )
        return start.astimezone(UTC), min(now, scheduled_end).astimezone(UTC)

    def validates_payload(self, payload: dict[str, object], now: datetime) -> bool:
        text = payload.get("text")
        if not isinstance(text, str):
            return False
        paragraphs = text.strip().split("\n\n")
        return (
            payload.get("summary_format") == DAILY_SUMMARY_FORMAT
            and payload.get("local_date") == self.local_time(now).date().isoformat()
            and payload.get("timezone") == self.settings.timezone
            and len(paragraphs) == 5
            and all(paragraph.strip() and "\n" not in paragraph for paragraph in paragraphs)
        )
