from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import inspect

from tradeagent.notifications import RoundTripNotificationRepository
from tradeagent.persistence import Database


def test_outbox_migration_preserves_trade_history_and_protects_daily_history(tmp_path, monkeypatch):
    url = f"sqlite:///{tmp_path / 'migration.db'}"
    monkeypatch.setenv("TRADEAGENT_DATABASE_URL", url)
    config = Config("alembic.ini")
    command.upgrade(config, "head")
    command.downgrade(config, "0006_event_experiments")
    now = datetime(2026, 9, 6, 22, tzinfo=UTC)
    with Database(url) as db:
        columns = {
            column["name"]: column
            for column in inspect(db.engine).get_columns("notification_outbox")
        }
        assert not columns["cycle_id"]["nullable"]
        assert "claimed_at" not in columns
        repo = RoundTripNotificationRepository(db)
        cycle = repo.open_cycle(
            strategy_version="fixture",
            symbol="SPY",
            opened_at=now,
            quantity=Decimal(1),
            opening_vwap=Decimal(100),
            fees=Decimal(0),
        )
        trade_email = repo.close_cycle_and_enqueue(
            cycle, closed_at=now, closing_vwap=Decimal(101), closing_fees=Decimal(0)
        )
    command.upgrade(config, "head")
    with Database(url) as db:
        columns = {
            column["name"]: column
            for column in inspect(db.engine).get_columns("notification_outbox")
        }
        assert columns["cycle_id"]["nullable"]
        assert "claimed_at" in columns
        repo = RoundTripNotificationRepository(db)
        message = repo.claim_next(observed_at=now)
        assert message is not None and message.notification_id == trade_email
        assert message.cycle_id == cycle
        assert Decimal(message.payload["realized_pnl"]) == 1
        repo.mark_sent(trade_email, "fixture-accepted")
        repo.enqueue_status(uuid4(), {"subject": "status", "text": "update"}, created_at=now)
    with pytest.raises(ValueError, match="archive it explicitly"):
        command.downgrade(config, "0006_event_experiments")
    with Database(url) as db:
        assert RoundTripNotificationRepository(db).count() == 2
