from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path

import pytest

from tradeagent.notifier import NotifierAlreadyRunningError, NotifierService
from tradeagent.persistence import Database, ProductionRepository

NOW = datetime(2026, 9, 4, 15, tzinfo=UTC)


class FakeDispatcher:
    def __init__(self, results: list[bool]) -> None:
        self.results = results
        self.calls = 0

    def dispatch_one(self) -> bool:
        self.calls += 1
        return self.results.pop(0) if self.results else False


def _service(
    tmp_path: Path,
    dispatcher: FakeDispatcher,
) -> tuple[Database, ProductionRepository, NotifierService]:
    database = Database(f"sqlite:///{tmp_path / 'notifier.db'}")
    database.initialize()
    repository = ProductionRepository(database)
    service = NotifierService(
        dispatcher,
        repository,
        instance_id="notifier-1",
        poll_seconds=0.01,
        clock=lambda: NOW,
    )
    return database, repository, service


def test_notifier_once_dispatches_and_releases_lock(tmp_path: Path) -> None:
    dispatcher = FakeDispatcher([True])
    database, repository, service = _service(tmp_path, dispatcher)

    assert service.run_once()
    assert dispatcher.calls == 1
    heartbeat = repository.latest_heartbeat("tradeagent-notifier")
    assert heartbeat is not None
    assert heartbeat[2] == {"state": "running", "dispatched": True}
    assert repository.acquire_worker_lock("tradeagent-notifier", "notifier-2")
    database.dispose()


def test_notifier_rejects_second_instance(tmp_path: Path) -> None:
    dispatcher = FakeDispatcher([])
    database, repository, service = _service(tmp_path, dispatcher)
    assert repository.acquire_worker_lock("tradeagent-notifier", "other")

    with pytest.raises(NotifierAlreadyRunningError, match="another notifier"):
        service.run_once()

    database.dispose()


def test_notifier_loop_stops_cleanly(tmp_path: Path) -> None:
    dispatcher = FakeDispatcher([])
    database, repository, service = _service(tmp_path, dispatcher)
    stop_event = asyncio.Event()
    stop_event.set()

    asyncio.run(service.run(stop_event))

    heartbeat = repository.latest_heartbeat("tradeagent-notifier")
    assert heartbeat is not None
    assert heartbeat[2]["state"] == "stopped"
    assert dispatcher.calls == 0
    database.dispose()


def test_notifier_validates_timing(tmp_path: Path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'invalid.db'}")
    database.initialize()
    with pytest.raises(ValueError, match="timing"):
        NotifierService(
            FakeDispatcher([]),
            ProductionRepository(database),
            instance_id="notifier-1",
            poll_seconds=0,
        )
    database.dispose()
