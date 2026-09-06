from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field
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
    cohort_id: str = "v20-event-cohort-001"
    virtual_equity: Decimal = Field(default=Decimal("10000"), gt=0, le=Decimal("10000"))
    max_entry_notional: Decimal = Field(default=Decimal("25"), gt=0, le=25)
    max_positions: Literal[1] = 1
    max_entries_per_session: int = Field(default=2, ge=1, le=2)
    daily_loss_fraction: Decimal = Field(default=Decimal("0.005"), gt=0, le=Decimal("0.005"))
    drawdown_fraction: Decimal = Field(default=Decimal("0.015"), gt=0, le=Decimal("0.015"))
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
