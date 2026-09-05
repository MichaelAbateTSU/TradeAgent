from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Protocol

from tradeagent.persistence import ProductionRepository


class ScheduledReconciliationStatus(Protocol):
    @property
    def healthy(self) -> bool: ...


class ScheduledReconciler(Protocol):
    def reconcile(self, *, observed_at: datetime) -> ScheduledReconciliationStatus: ...


class ReconciliationFailureError(RuntimeError):
    pass


class HeartbeatStaleError(RuntimeError):
    pass


class ReconciliationScheduler:
    def __init__(
        self,
        repository: ProductionRepository,
        reconciler: ScheduledReconciler,
        *,
        interval_seconds: int,
        instance_id: str,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("reconciliation interval must be positive")
        self._repository = repository
        self._reconciler = reconciler
        self._interval_seconds = interval_seconds
        self._instance_id = instance_id
        self._clock = clock

    def run_once(self) -> ScheduledReconciliationStatus:
        observed_at = self._clock()
        result = self._reconciler.reconcile(observed_at=observed_at)
        self._repository.append_event(
            "scheduled_reconciliation",
            {"healthy": result.healthy},
            occurred_at=observed_at,
            trace_id=f"scheduled-reconcile:{observed_at.isoformat()}",
        )
        self._repository.heartbeat(
            "tradeagent-reconciler",
            self._instance_id,
            {"healthy": result.healthy},
            observed_at=observed_at,
        )
        if not result.healthy:
            self._repository.set_control("kill_switch", "active")
            raise ReconciliationFailureError("scheduled broker reconciliation failed")
        return result

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            self.run_once()
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=self._interval_seconds,
                )
            except TimeoutError:
                continue


class HeartbeatWatchdog:
    def __init__(
        self,
        repository: ProductionRepository,
        *,
        service_name: str,
        maximum_age_seconds: int,
    ) -> None:
        if maximum_age_seconds <= 0:
            raise ValueError("maximum heartbeat age must be positive")
        self._repository = repository
        self._service_name = service_name
        self._maximum_age = timedelta(seconds=maximum_age_seconds)

    def check(self, *, observed_at: datetime | None = None) -> None:
        now = observed_at or datetime.now(UTC)
        heartbeat = self._repository.latest_heartbeat(self._service_name)
        if heartbeat is None:
            self._repository.set_control("kill_switch", "active")
            raise HeartbeatStaleError(f"{self._service_name} has not recorded a heartbeat")
        _, heartbeat_at, _ = heartbeat
        if heartbeat_at > now or now - heartbeat_at > self._maximum_age:
            self._repository.set_control("kill_switch", "active")
            raise HeartbeatStaleError(f"{self._service_name} heartbeat is stale")
