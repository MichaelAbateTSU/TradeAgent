from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from tradeagent.config import AppConfig

ExperimentMode = Literal["shadow", "experimental-paper", "offline-replay"]


def reject_live_environment() -> None:
    if (
        any(
            os.getenv(key)
            for key in (
                "ALPACA_LIVE_KEY",
                "ALPACA_LIVE_KEY_ID",
                "ALPACA_LIVE_SECRET_KEY",
            )
        )
        or os.getenv("APCA_API_BASE_URL", "").rstrip("/") == "https://api.alpaca.markets"
    ):
        raise ValueError("live broker configuration forbidden in event worker")


class ExperimentalSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="EVENT_",
        env_file=".env",
        extra="ignore",
        frozen=True,
    )
    mode: ExperimentMode = "shadow"
    purpose: Literal["research", "iex-practice"] = "research"
    entry_policy: Literal["event-strategy", "equipment-only-demo", "news-paper"] = "event-strategy"
    demo_account_digest: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    news_account_digest: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    practice_start_date: date | None = None
    # Evidence traces reserve 77 of the 128 characters for the hash and suffix.
    cohort_id: str = Field(default="v20-event-cohort-001", min_length=1, max_length=51)
    virtual_equity: Decimal = Field(default=Decimal("10000"), gt=0, le=Decimal("10000"))
    max_entry_notional: Decimal = Field(default=Decimal("25"), gt=0, le=25)
    max_positions: Literal[1] = 1
    max_entries_per_session: int = Field(default=2, ge=1, le=2)
    daily_loss_fraction: Decimal = Field(default=Decimal("0.005"), gt=0, le=Decimal("0.005"))
    drawdown_fraction: Decimal = Field(default=Decimal("0.015"), gt=0, le=Decimal("0.015"))
    weekly_loss_fraction: Decimal = Field(default=Decimal("0.005"), gt=0, le=Decimal("0.005"))
    max_holding_minutes: int = Field(default=60, ge=1, le=60)
    poll_seconds: int = Field(default=30, ge=10, le=60)
    initial_lookback_minutes: int = Field(default=60, ge=15, le=1440)
    symbols: str = "AAPL,MSFT,NVDA"
    sec_contact_email: str | None = None
    primary_urls: dict[str, tuple[str, ...]] = Field(default_factory=dict)
    minimum_sessions: Literal[60] = 60
    minimum_round_trips: Literal[60] = 60
    max_inference_calls_daily: Literal[0] = 0
    # No inference provider is configured; unknown forecasts stay unknown.
    inference_provider: Literal["deterministic-only"] = "deterministic-only"
    estimated_fixed_monthly_usd: Decimal | None = None

    @model_validator(mode="after")
    def validate_practice(self) -> Self:
        symbols = self.symbols.split(",")
        if not symbols or any(symbol not in {"AAPL", "MSFT", "NVDA"} for symbol in symbols):
            raise ValueError("v20 supports only AAPL, MSFT and NVDA")
        if self.purpose == "iex-practice":
            if self.practice_start_date is None:
                raise ValueError("IEX practice requires an explicit start date")
            if "AAPL" not in self.symbols.split(","):
                raise ValueError("IEX practice requires AAPL for its declared calibration")
        elif self.practice_start_date is not None:
            raise ValueError("a practice start date cannot alter a research cohort")
        if self.entry_policy == "equipment-only-demo":
            if (
                self.purpose != "iex-practice"
                or self.symbols != "AAPL"
                or self.max_entries_per_session != 1
                or self.demo_account_digest is None
            ):
                raise ValueError(
                    "equipment-only demo requires IEX, AAPL, one entry and account pin"
                )
        elif self.demo_account_digest is not None:
            raise ValueError("demo account pin requires the equipment-only policy")
        if self.entry_policy == "news-paper":
            if (
                self.purpose != "iex-practice"
                or self.max_entries_per_session != 2
                or self.news_account_digest is None
            ):
                raise ValueError("news paper requires IEX, two bounded entries and account pin")
        elif self.news_account_digest is not None:
            raise ValueError("news account pin requires the news-paper policy")
        return self

    @property
    def execution_feed(self) -> Literal["iex", "sip"]:
        return "iex" if self.purpose == "iex-practice" else "sip"

    @property
    def calibration_window_minutes(self) -> tuple[int, int]:
        return (40, 60) if self.entry_policy in {"equipment-only-demo", "news-paper"} else (0, 30)

    def effective_notional(self, app: AppConfig) -> Decimal:
        return min(
            self.max_entry_notional,
            app.intraday.maximum_order_notional,
            self.virtual_equity * app.intraday.maximum_position_exposure,
            self.virtual_equity * app.intraday.maximum_gross_exposure,
            self.virtual_equity * app.risk.max_order_exposure,
        )

    def fingerprint(self, protocol: dict[str, object], code_sha: str) -> str:
        return sha256(
            json.dumps(
                {
                    "settings": self.model_dump(mode="json"),
                    "protocol": protocol,
                    "code_sha": code_sha,
                    "policy_change": (
                        "v20 operational evidence permits bounded unqualified paper experiments"
                    ),
                },
                sort_keys=True,
            ).encode()
        ).hexdigest()


class OperationalCertificate(BaseModel):
    model_config = ConfigDict(frozen=True)

    certificate_id: str
    cohort_id: str
    config_hash: str
    code_sha: str
    account_digest: str
    issued_at: datetime
    expires_at: datetime
    checks: dict[str, bool]
    limitations: tuple[str, ...]
    permits_paper: bool
    establishes_edge: Literal[False] = False


def certificate(
    settings: ExperimentalSettings,
    *,
    config_hash: str,
    code_sha: str,
    account_id: str,
    checks: dict[str, bool],
    now: datetime,
    limitations: tuple[str, ...] = (),
) -> OperationalCertificate:
    account_digest = sha256(account_id.encode()).hexdigest()
    identity = f"{settings.cohort_id}|{config_hash}|{account_digest}|{now.isoformat()}"
    return OperationalCertificate(
        certificate_id=sha256(identity.encode()).hexdigest(),
        cohort_id=settings.cohort_id,
        config_hash=config_hash,
        code_sha=code_sha,
        account_digest=account_digest,
        issued_at=now.astimezone(UTC),
        expires_at=now + timedelta(hours=24),
        checks=checks,
        limitations=limitations,
        permits_paper=bool(checks) and all(checks.values()),
    )
