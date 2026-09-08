from datetime import UTC, datetime

from alembic import command
from alembic.config import Config
from sqlalchemy import inspect, text

from tradeagent.persistence import Database, ProductionRepository


def test_audit_lookup_index_preserves_history_and_supports_trace_reads(tmp_path, monkeypatch):
    url = f"sqlite:///{tmp_path / 'audit-index.db'}"
    monkeypatch.setenv("TRADEAGENT_DATABASE_URL", url)
    config = Config("alembic.ini")
    command.upgrade(config, "0009_candidate_states")
    with Database(url) as database:
        repository = ProductionRepository(database)
        repository.append_event(
            "event_incident",
            {"outcome": "MISSED", "loss": "-2.50"},
            occurred_at=datetime.now(UTC),
            trace_id="preserved:incident",
        )
        with database.begin() as connection:
            connection.execute(text("DROP INDEX IF EXISTS ix_events_v2_trace_type_time"))
    command.upgrade(config, "head")
    with Database(url) as database:
        indexes = inspect(database.engine).get_indexes("events_v2")
        assert any(index["name"] == "ix_events_v2_trace_type_time" for index in indexes)
        with database.begin() as connection:
            plan = connection.execute(
                text("EXPLAIN QUERY PLAN SELECT event_id FROM events_v2 WHERE trace_id=:trace"),
                {"trace": "preserved:incident"},
            ).all()
        assert "ix_events_v2_trace_type_time" in str(plan)
        assert ProductionRepository(database).latest_event_payload("event_incident") == {
            "outcome": "MISSED",
            "loss": "-2.50",
        }
    command.downgrade(config, "0009_candidate_states")
    with Database(url) as database:
        assert (
            ProductionRepository(database).latest_event_payload("event_incident")["loss"] == "-2.50"
        )
