from datetime import UTC, datetime

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import inspect

from tradeagent.event_store import EventStore
from tradeagent.persistence import Database


def test_deferred_candidate_history_survives_restart_and_blocks_destructive_downgrade(
    tmp_path, monkeypatch
):
    url = f"sqlite:///{tmp_path / 'candidate-migration.db'}"
    monkeypatch.setenv("TRADEAGENT_DATABASE_URL", url)
    config = Config("alembic.ini")
    command.upgrade(config, "head")
    command.downgrade(config, "0008_control_values")
    with Database(url) as database:
        assert not inspect(database.engine).has_table("event_candidate_states")
    command.upgrade(config, "head")
    now = datetime(2026, 9, 8, 13, 35, tzinfo=UTC)
    with Database(url) as database:
        store = EventStore(database)
        store.freeze("fixture", "hash", {}, "experimental-paper", now)
        store.evidence("evidence", {"fixture": True}, now)
        decision = store.decision(
            "fixture", "evidence", {"action": "eligible", "first_quote": "100"}, now
        )
        store.candidate_state(
            decision, "fixture", "waiting", {"reasons": ["POSITION_OR_ORDER_RESERVED"]}, now
        )
        assert len(store.pending_evidence("fixture")) == 1
    with Database(url) as database:
        store = EventStore(database)
        assert len(store.pending_evidence("fixture")) == 1
        assert store.decision("fixture", "evidence", {"first_quote": "999"}, now) == decision
        assert store.report("fixture")["decisions"][0]["payload"]["first_quote"] == "100"
    with pytest.raises(ValueError, match="archive it explicitly"):
        command.downgrade(config, "0008_control_values")
    with Database(url) as database:
        assert inspect(database.engine).has_table("event_candidate_states")
        assert len(EventStore(database).pending_evidence("fixture")) == 1
