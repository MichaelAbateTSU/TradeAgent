from datetime import UTC, datetime, timedelta
from pathlib import Path

from sqlalchemy import select

from tradeagent.persistence import Database, notification_outbox
from tradeagent.scalping_config import ScalpingConfig
from tradeagent.scalping_notifications import ScalpingNotifications


def test_start_and_digests_are_idempotent_across_restart(tmp_path: Path) -> None:
    now = datetime(2026, 9, 11, 7, tzinfo=UTC)
    config = ScalpingConfig(
        cohort_id="v30-notices",
        account_digest="a" * 64,
        approved_at=now,
        email_digest_seconds=1800,
    )
    snapshot = {
        "cohort_id": config.cohort_id,
        "owner_id": "owner",
        "state": "running",
        "trade_summary": {"closed_round_trips": 5, "fees_pending": 1},
    }
    with Database(f"sqlite:///{tmp_path / 'notifications.db'}") as database:
        database.initialize()
        sender = ScalpingNotifications(database, config, "c" * 40)
        sender.publish(snapshot, now)
        sender.publish(snapshot, now + timedelta(seconds=10))
        restarted = ScalpingNotifications(database, config, "c" * 40)
        restarted.publish(snapshot, now + timedelta(seconds=1801))
        restarted.publish(snapshot, now + timedelta(seconds=1810))
        with database.begin() as connection:
            rows = list(connection.execute(select(notification_outbox)).mappings())
        assert len(rows) == 2
        assert all(row["attempts"] == 0 and row["status"] == "pending" for row in rows)
        assert "NOT a filled-trade" in rows[0]["payload"]["text"]
        assert "Pending/estimated fees" in rows[1]["payload"]["text"]
        assert "fees_pending" in rows[1]["payload"]["text"]
