from datetime import UTC, datetime
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import event, func, inspect, select

from tradeagent.event_store import EventStore
from tradeagent.persistence import (
    Database,
    ProductionRepository,
    append_reporting_metadata,
    event_reporting_metadata,
    events,
)
from tradeagent.reporting_metadata import (
    backfill_reporting_metadata_page,
    missing_reporting_metadata,
)
from tradeagent.reporting_reads import REPORTING_PROJECTION_VERSION


def test_backfill_command_is_resumable_and_does_not_change_controls(tmp_path, monkeypatch, capsys):
    from infra.render import backfill_reporting_metadata as cli

    url = f"sqlite:///{tmp_path / 'backfill-command.db'}"
    monkeypatch.setenv("TRADEAGENT_DATABASE_URL", url)
    monkeypatch.setattr("sys.argv", ["backfill", "--pause-seconds", "0"])
    with Database(url) as database:
        database.initialize()
        repository = ProductionRepository(database)
        repository.set_control("kill_switch", "active")
        with database.begin() as connection:
            connection.execute(
                events.insert().values(
                    event_id=str(uuid4()),
                    event_type="event_incident",
                    trace_id="fixture",
                    occurred_at=datetime.now(UTC),
                    recorded_at=datetime.now(UTC),
                    payload={"outcome": "MISSED", "retained": "original"},
                )
            )
    cli.main()
    first = capsys.readouterr().out
    assert '"inserted": 1' in first and '"remaining": 0' in first
    cli.main()
    assert '"inserted": 0' in capsys.readouterr().out
    with Database(url) as database:
        assert ProductionRepository(database).get_control("kill_switch") == "active"
        assert ProductionRepository(database).event_count() == 1


def test_backfill_yields_to_recorder_backlog_and_stops_at_its_deadline(tmp_path, monkeypatch):
    from infra.render import backfill_reporting_metadata as cli

    url = f"sqlite:///{tmp_path / 'backfill-backpressure.db'}"
    monkeypatch.setenv("TRADEAGENT_DATABASE_URL", url)
    monkeypatch.setattr("sys.argv", ["backfill", "--max-seconds", "1"])
    clock = [0.0]
    monkeypatch.setattr(cli.time, "monotonic", lambda: clock[0])

    def sleep(seconds):
        clock[0] += seconds

    monkeypatch.setattr(cli.time, "sleep", sleep)
    with Database(url) as database:
        database.initialize()
        repository = ProductionRepository(database)
        repository.heartbeat(
            "tradeagent-shadow-recorder",
            "recorder",
            {"queue_depth": 3000},
            observed_at=datetime.now(UTC),
        )
        repository.set_control("kill_switch", "active")
    with pytest.raises(RuntimeError, match="deadline"):
        cli.main()
    with Database(url) as database:
        assert ProductionRepository(database).get_control("kill_switch") == "active"
        assert ProductionRepository(database).event_count() == 0


def test_event_writers_atomically_project_original_scope_and_never_overwrite_metadata():
    with Database("sqlite:///:memory:") as database:
        database.initialize()
        repository = ProductionRepository(database)
        payload = {"synthetic": False, "session_date": None, "original_body": "retain"}
        identity = repository.append_event(
            "event_incident", payload, occurred_at=datetime.now(UTC), trace_id="fixture"
        )
        EventStore(database).audit(
            "incident", {"mode": "experimental-paper"}, datetime.now(UTC), "fixture"
        )
        with database.begin() as connection:
            append_reporting_metadata(
                connection, str(identity), "event_incident", {"synthetic": True}
            )
            stored = connection.scalar(
                select(event_reporting_metadata.c.payload).where(
                    event_reporting_metadata.c.event_id == str(identity)
                )
            )
            assert stored["scope"] == {"synthetic": False, "session_date": None}
            assert "original_body" not in stored["scope"]
            assert connection.scalar(select(func.count()).select_from(events)) == 2
            assert (
                connection.scalar(select(func.count()).select_from(event_reporting_metadata)) == 2
            )
        assert missing_reporting_metadata(database) == 0


def test_projection_failure_rolls_back_new_original(monkeypatch):
    import tradeagent.reporting_reads as reads

    def fail(*args):
        raise ValueError("invalid projection")

    with Database("sqlite:///:memory:") as database:
        database.initialize()
        monkeypatch.setattr(reads, "event_reporting_projection", fail)
        with pytest.raises(ValueError, match="invalid projection"):
            ProductionRepository(database).append_event(
                "event_incident", {}, occurred_at=datetime.now(UTC), trace_id="fixture"
            )
        assert ProductionRepository(database).event_count() == 0


def test_migration_and_bounded_backfill_preserve_all_originals_controls_and_versions(
    tmp_path, monkeypatch
):
    url = f"sqlite:///{tmp_path / 'report-metadata.db'}"
    monkeypatch.setenv("TRADEAGENT_DATABASE_URL", url)
    config = Config("alembic.ini")
    command.upgrade(config, "0010_event_audit_lookup")
    now = datetime.now(UTC)
    originals = [
        {
            "event_id": f"{index:036d}",
            "event_type": "event_source_poll" if index == 0 else "event_official_context",
            "occurred_at": now,
            "recorded_at": now,
            "trace_id": "fixture",
            "payload": {
                "synthetic": False,
                "session_date": None,
                "raw_items_received": 3,
                "coverage_complete": False,
                "large_original": "immutable" * 8000,
            },
        }
        for index in range(65)
    ]
    with Database(url) as database:
        with database.begin() as connection:
            connection.execute(events.insert(), originals)
            connection.execute(
                events.insert().values(
                    event_id=str(uuid4()),
                    event_type="event_session_report",
                    occurred_at=now,
                    recorded_at=now,
                    trace_id="fixture",
                    payload={"retained": True},
                )
            )
        ProductionRepository(database).set_control("kill_switch", "active")
    command.upgrade(config, "head")
    with Database(url, pool_size=1) as database:
        assert inspect(database.engine).has_table("event_reporting_metadata")
        assert missing_reporting_metadata(database) == 65
        with database.begin() as connection:
            connection.execute(
                event_reporting_metadata.insert().values(
                    event_id=originals[0]["event_id"],
                    projection_version=0,
                    payload={"scope": {"mode": "old-version"}},
                )
            )
        payload_reads = []

        @event.listens_for(database.engine, "before_cursor_execute")
        def record(connection, cursor, statement, parameters, context, executemany):
            if statement.startswith("SELECT") and "events_v2.payload" in statement:
                payload_reads.append((len(parameters), context.execution_options.get("yield_per")))

        cursor = None
        total = 0
        while True:
            page = backfill_reporting_metadata_page(database, after_event_id=cursor)
            total += page["inserted"]
            cursor = page["last_event_id"]
            if not page["selected"]:
                break
        assert total == 65
        assert payload_reads == [(32, 1), (32, 1), (1, 1)]
        assert missing_reporting_metadata(database) == 0
        assert backfill_reporting_metadata_page(database)["inserted"] == 0
        with database.begin() as connection:
            after = list(
                connection.execute(
                    select(events)
                    .where(events.c.event_type != "event_session_report")
                    .order_by(events.c.event_id)
                ).mappings()
            )
            assert [row["payload"] for row in after] == [row["payload"] for row in originals]
            assert connection.scalar(select(func.count()).select_from(events)) == 66
            assert (
                connection.scalar(
                    select(event_reporting_metadata.c.payload).where(
                        event_reporting_metadata.c.event_id == originals[0]["event_id"],
                        event_reporting_metadata.c.projection_version
                        == REPORTING_PROJECTION_VERSION,
                    )
                )["poll"]["coverage_complete"]
                is False
            )
            assert connection.scalar(
                select(event_reporting_metadata.c.payload).where(
                    event_reporting_metadata.c.projection_version == 0
                )
            ) == {"scope": {"mode": "old-version"}}
        assert ProductionRepository(database).get_control("kill_switch") == "active"
    command.downgrade(config, "0010_event_audit_lookup")
    with Database(url) as database:
        assert ProductionRepository(database).event_count() == 66
        assert not inspect(database.engine).has_table("event_reporting_metadata")
