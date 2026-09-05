from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Protocol

from tradeagent.notifications import EmailDeliveryError
from tradeagent.persistence import ProductionRepository


class NotifierAlreadyRunningError(RuntimeError):
    pass


class OutboxDispatcher(Protocol):
    def dispatch_one(self) -> bool: ...


class NotifierService:
    def __init__(
        self,
        dispatcher: OutboxDispatcher,
        repository: ProductionRepository,
        *,
        instance_id: str,
        poll_seconds: float = 5,
        maximum_backoff_seconds: float = 60,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if poll_seconds <= 0 or maximum_backoff_seconds <= 0:
            raise ValueError("notifier timing values must be positive")
        self._dispatcher = dispatcher
        self._repository = repository
        self._instance_id = instance_id
        self._poll_seconds = poll_seconds
        self._maximum_backoff_seconds = maximum_backoff_seconds
        self._clock = clock

    def run_once(self) -> bool:
        self._acquire_lock()
        try:
            dispatched = self._dispatcher.dispatch_one()
            self._heartbeat("running", dispatched=dispatched)
            return dispatched
        finally:
            self._repository.release_worker_lock("tradeagent-notifier", self._instance_id)

    async def run(self, stop_event: asyncio.Event | None = None) -> None:
        stop = stop_event or asyncio.Event()
        self._acquire_lock()
        backoff = self._poll_seconds
        try:
            self._heartbeat("starting", dispatched=False)
            while not stop.is_set():
                try:
                    dispatched = await asyncio.to_thread(self._dispatcher.dispatch_one)
                except EmailDeliveryError:
                    self._heartbeat("provider_error", dispatched=False)
                    await self._wait(stop, backoff)
                    backoff = min(backoff * 2, self._maximum_backoff_seconds)
                    continue
                backoff = self._poll_seconds
                self._heartbeat("running", dispatched=dispatched)
                if not dispatched:
                    await self._wait(stop, self._poll_seconds)
        finally:
            self._heartbeat("stopped", dispatched=False)
            self._repository.release_worker_lock("tradeagent-notifier", self._instance_id)

    def _acquire_lock(self) -> None:
        if not self._repository.acquire_worker_lock("tradeagent-notifier", self._instance_id):
            raise NotifierAlreadyRunningError("another notifier owns the delivery lock")

    async def _wait(self, stop_event: asyncio.Event, seconds: float) -> None:
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=seconds)
        except TimeoutError:
            return

    def _heartbeat(self, state: str, *, dispatched: bool) -> None:
        self._repository.heartbeat(
            "tradeagent-notifier",
            self._instance_id,
            {"state": state, "dispatched": dispatched},
            observed_at=self._clock(),
        )
