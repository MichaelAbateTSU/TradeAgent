from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from tradeagent.notifications import RoundTripNotificationRepository
from tradeagent.persistence import Database, ProductionRepository
from tradeagent.scalping_config import ScalpingConfig


class ScalpingNotifications:
    """Record all execution events; send bounded-cadence summaries through the existing outbox."""

    def __init__(self, database: Database, config: ScalpingConfig, code_sha: str):
        self.repo = ProductionRepository(database)
        self.outbox = RoundTripNotificationRepository(database)
        self.config = config
        self.code_sha = code_sha

    def publish(self, snapshot: dict[str, Any], now: datetime) -> None:
        if snapshot.get("cohort_id") != self.config.cohort_id:
            raise ValueError("notification snapshot must belong to the configured scalper")
        startup_id = uuid5(
            NAMESPACE_URL, f"tradeagent:v30:{self.config.cohort_id}:{self.code_sha}:startup"
        )
        self.outbox.enqueue_status(
            startup_id,
            {
                "subject": "[TradeAgent PAPER v30] Autonomous scalper started",
                "text": (
                    "The Render worker has started the v30 unrestricted paper-crypto profile.\n"
                    "No daily approval or paper loss/drawdown/trade-count ceiling is required.\n"
                    "This startup notice is NOT a filled-trade or profitability claim.\n"
                    f"Symbols: {', '.join(self.config.symbols)}\n"
                    f"Strategy ticket: {self.config.order_notional_usd} USD (not a risk ceiling)\n"
                    f"Current state: {snapshot.get('state', 'not reported')}\n"
                    f"Worker: {snapshot.get('owner_id', 'not reported')}\n"
                    f"Release: {self.code_sha}\n"
                    "Execution/accounting digests every "
                    f"{self.config.email_digest_seconds} seconds.\n"
                    "Paper-only routing, actual broker constraints, ownership "
                    "and reconciliation remain."
                ),
                "cohort_id": self.config.cohort_id,
                "profile": self.config.profile,
                "qualification_eligible": False,
            },
            created_at=now,
        )
        bucket = int(now.timestamp()) // self.config.email_digest_seconds
        epoch_key = f"scalping:{self.config.cohort_id}:notification_epoch"
        epoch = self.repo.get_control(epoch_key)
        if epoch is None:
            self.repo.set_control(epoch_key, str(bucket))
            return
        if bucket <= int(epoch):
            return
        digest_id = uuid5(NAMESPACE_URL, f"tradeagent:v30:{self.config.cohort_id}:digest:{bucket}")
        self.outbox.enqueue_status(
            digest_id,
            {
                "subject": f"[TradeAgent PAPER v30] Scalping digest - {now.isoformat()}",
                "text": (
                    "AUTONOMOUS PAPER SCALPING - RECORDED RUNTIME SNAPSHOT\n"
                    f"Snapshot: {now.isoformat()}\n"
                    f"State: {snapshot.get('state', 'not reported')}\n"
                    f"Run: {self.config.cohort_id}\n\n"
                    "EXECUTION AND ACCOUNTING\n"
                    + json.dumps(
                        {
                            "summary": snapshot.get("trade_summary"),
                            "execution": snapshot.get("execution"),
                            "operator_stop": snapshot.get("operator_stop"),
                            "errors": snapshot.get("errors"),
                        },
                        indent=2,
                        sort_keys=True,
                        default=str,
                    )
                    + "\n\nPending/estimated fees are not final actual net P&L. "
                    "Paper fills do not establish real queue priority or live profitability.\n"
                    "Every order/cycle remains in the durable ledger; emails are digests, "
                    "not a separate provider message for every high-frequency event."
                ),
                "cohort_id": self.config.cohort_id,
                "profile": self.config.profile,
                "bucket": bucket,
                "qualification_eligible": False,
            },
            created_at=now,
        )
