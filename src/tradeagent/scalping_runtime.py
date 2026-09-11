from __future__ import annotations

import asyncio
import copy
import json
import logging
import math
import os
import socket
import time
from collections import deque
from collections.abc import Callable
from datetime import UTC, datetime
from functools import partial
from threading import RLock
from typing import Any

import httpx
from sqlalchemy.exc import SQLAlchemyError

from tradeagent.alpaca import AlpacaDataSettings
from tradeagent.alpaca_news import AlpacaNewsClient
from tradeagent.alpaca_paper import AlpacaPaperClient, AlpacaPaperSettings
from tradeagent.config import AppConfig
from tradeagent.event_doctor import code_identity
from tradeagent.event_order_stream import AlpacaPaperTradeUpdatesStream
from tradeagent.experimental_policy import reject_live_environment
from tradeagent.news import NewsRepository
from tradeagent.news_worker import NewsWorker, NewsWorkerSettings
from tradeagent.persistence import Database, ProductionRepository
from tradeagent.scalping_config import ScalpingConfig, ScalpSignal
from tradeagent.scalping_execution import ScalpOrderEngine
from tradeagent.scalping_market import BookFeatureEngine, CryptoMarketFeed, MarketEvent
from tradeagent.scalping_notifications import ScalpingNotifications
from tradeagent.scalping_store import ScalpStore
from tradeagent.scalping_strategy import ScalpStrategy

LOGGER = logging.getLogger(__name__)
LOCK_NAME = "tradeagent-event-worker"


async def _blocking[T](function: Callable[[], T]) -> T:
    # Cancelling to_thread does not stop its broker request. Keep ownership until it finishes.
    task = asyncio.create_task(asyncio.to_thread(function))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        await task
        raise


def _percentiles(values: deque[float]) -> dict[str, float | int | None]:
    ordered = sorted(values)
    result: dict[str, float | int | None] = {"samples": len(ordered)}
    for name, fraction in (("p50", 0.5), ("p95", 0.95), ("p99", 0.99)):
        result[name] = ordered[max(0, math.ceil(len(ordered) * fraction) - 1)] if ordered else None
    return result


class ScalpingRuntime:
    def __init__(
        self,
        database: Database,
        broker: AlpacaPaperClient,
        config: ScalpingConfig,
        *,
        owner_id: str,
        code_sha: str,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        order_stream: AlpacaPaperTradeUpdatesStream | None = None,
    ):
        self.database = database
        self.repo = ProductionRepository(database)
        self.config = config
        self.owner_id = owner_id
        self.code_sha = code_sha
        self.clock = clock
        self.order_stream = order_stream
        self.store = ScalpStore(database)
        self.market = BookFeatureEngine(config)
        self.strategy = ScalpStrategy(config)
        self.engine = ScalpOrderEngine(
            database, broker, config, owner_id=owner_id, code_sha=code_sha, clock=clock
        )
        self.notifications = ScalpingNotifications(database, config, code_sha)
        self._market_lock = RLock()
        self._state_lock = RLock()
        self._execution: dict[str, Any] = {}
        self._summary: dict[str, Any] = {}
        self._signals: list[dict[str, Any]] = []
        self._errors: deque[dict[str, Any]] = deque(maxlen=20)
        self._tick_times: deque[float] = deque(maxlen=2000)
        self._write_times: deque[float] = deque(maxlen=2000)
        self._committed_events = 0
        self._committed_batches = 0
        self._last_market_at: datetime | None = None
        self._last_tick_at: datetime | None = None
        self._last_summary_at: datetime | None = None
        self._last_stream_gaps = 0
        self._operator_stop = False
        self._state = "initializing"

    def initialize(self) -> None:
        self.engine.initialize()
        with self._state_lock:
            self._execution = copy.deepcopy(self.engine.status())
            self._summary = copy.deepcopy(self.engine.summary(since=None))
            self._state = "waiting_for_market_data"

    def record_error(self, stage: str, error: Exception) -> None:
        detail = {
            "at": self.clock().isoformat(),
            "stage": stage,
            "error_type": type(error).__name__,
        }
        if isinstance(error, httpx.HTTPStatusError):
            detail["http_status"] = str(error.response.status_code)
        with self._state_lock:
            self._errors.append(detail)
            if stage in {"execution", "market_persistence"}:
                self._state = f"{stage}_unavailable"
        LOGGER.warning("v30 %s failed: %s", stage, type(error).__name__)

    def process_market_batch(self, batch: tuple[MarketEvent, ...]) -> None:
        if not batch:
            raise ValueError("an empty market batch is not progress")
        started = time.monotonic()
        now = self.clock()
        self.store.persist_market_batch([event.model_dump(mode="json") for event in batch], at=now)
        with self._market_lock:
            for event in batch:
                self.market.on_event(event)
        with self._state_lock:
            self._committed_events += len(batch)
            self._committed_batches += 1
            self._last_market_at = now
            self._write_times.append(time.monotonic() - started)

    def tick(self) -> dict[str, Any]:
        started = time.monotonic()
        if self.order_stream is not None:
            updates = self.order_stream.drain()
            gaps = self.order_stream.health_snapshot()["gap_count"]
            if not isinstance(gaps, int) or isinstance(gaps, bool):
                raise ValueError("paper order-stream gap counter is invalid")
            if updates or gaps != self._last_stream_gaps:
                self.engine.reconcile(now=self.clock())
                self._last_stream_gaps = gaps
        now = self.clock()
        inventory = self.engine.inventory()
        stopped = self.repo.get_control(f"scalping:{self.config.cohort_id}:stop") is not None
        signals: list[ScalpSignal] = []
        with self._market_lock:
            quotes = {
                symbol: quote
                for symbol in self.config.symbols
                if (quote := self.market.quote(symbol)) is not None
            }
            for symbol in self.config.symbols:
                features = self.market.features(symbol, now)
                if features is None:
                    continue
                signal = self.strategy.decide(features, inventory=inventory.get(symbol), now=now)
                if signal.action != "buy" or not stopped:
                    signals.append(signal)
        execution = self.engine.step(tuple(signals), quotes, now=self.clock())
        summary = None
        if self._last_summary_at is None or (now - self._last_summary_at).total_seconds() >= 30:
            summary = self.engine.summary(since=None)
            self._last_summary_at = now
        with self._state_lock:
            self._execution = copy.deepcopy(execution)
            if summary is not None:
                self._summary = copy.deepcopy(summary)
            self._signals = [signal.model_dump(mode="json") for signal in signals]
            self._last_tick_at = self.clock()
            self._operator_stop = stopped
            self._state = (
                "operator_stopped"
                if stopped
                else "running"
                if signals or inventory
                else "waiting_for_market_data"
            )
            self._tick_times.append(time.monotonic() - started)
        return execution

    def snapshot(
        self, *, feed: dict[str, Any] | None = None, queued_events: int = 0
    ) -> dict[str, Any]:
        now = self.clock()
        with self._state_lock:
            result = {
                "as_of": now.isoformat(),
                "state": self._state,
                "profile": self.config.profile,
                "mode": "paper",
                "cohort_id": self.config.cohort_id,
                "owner_id": self.owner_id,
                "code_sha": self.code_sha,
                "config_hash": self.config.identity,
                "symbols": list(self.config.symbols),
                "policy": self.config.policy_description(),
                "execution": copy.deepcopy(self._execution),
                "trade_summary": copy.deepcopy(self._summary),
                "last_signals": copy.deepcopy(self._signals),
                "operator_stop": self._operator_stop,
                "errors": list(self._errors),
                "last_execution_tick_at": self._last_tick_at.isoformat()
                if self._last_tick_at
                else None,
                "raw": {
                    "counter_scope": "current process; immutable tape persists across restarts",
                    "committed_events": self._committed_events,
                    "committed_batches": self._committed_batches,
                    "last_batch_at": self._last_market_at.isoformat()
                    if self._last_market_at
                    else None,
                    "queued_events": queued_events,
                },
                "latency": {
                    "units": "seconds",
                    "window": "most recent 2000 runtime samples",
                    "execution_tick": _percentiles(self._tick_times),
                    "raw_persistence_and_features": _percentiles(self._write_times),
                    "basis": "measured runtime samples, not a latency SLA",
                },
            }
        with self._market_lock:
            result["market"] = self.market.health_snapshot()
        result["feed"] = feed
        result["broker_stream"] = self.order_stream.health_snapshot() if self.order_stream else None
        return result

    def heartbeat(self, *, feed: dict[str, Any], queued_events: int) -> None:
        now = self.clock()
        if not self.repo.refresh_worker_lock(LOCK_NAME, self.owner_id, observed_at=now):
            raise RuntimeError("v30 execution lease ownership was lost")
        snapshot = self.snapshot(feed=feed, queued_events=queued_events)
        self.repo.set_control(
            f"scalping:{self.config.cohort_id}:status",
            json.dumps(snapshot, sort_keys=True, default=str),
        )
        self.repo.heartbeat(
            LOCK_NAME,
            self.owner_id,
            {
                "state": snapshot["state"],
                "mode": "paper",
                "purpose": "autonomous-crypto-scalping",
                "execution_feed": "alpaca-crypto-us-l2",
                "entry_policy": self.config.profile,
                "cohort_id": self.config.cohort_id,
                "config_hash": self.config.identity,
                "code_sha": self.code_sha,
                "scalping": {
                    "state": snapshot["state"],
                    "raw": snapshot["raw"],
                    "trade_summary": snapshot["trade_summary"],
                    "operator_stop": snapshot["operator_stop"],
                },
                "paper_policy": self.config.policy_description(),
                "broker_stream": snapshot["broker_stream"],
                "ordinary_entries_enabled": False,
                "global_strategy_kill": self.repo.get_control("kill_switch"),
            },
            observed_at=now,
        )


async def _wait(stop: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except TimeoutError:
        return


async def run_scalping_service(
    config: ScalpingConfig, *, stop_event: asyncio.Event | None = None
) -> None:
    reject_live_environment()
    owner = os.environ.get("RENDER_INSTANCE_ID") or f"{socket.gethostname()}-{os.getpid()}"
    sha = code_identity()
    settings = AlpacaPaperSettings.model_validate({})
    stop = stop_event or asyncio.Event()
    queue: asyncio.Queue[MarketEvent] = asyncio.Queue(maxsize=config.event_queue_capacity)
    with (
        Database(AppConfig().database_url.get_secret_value(), pool_size=3) as database,
        httpx.Client(timeout=5) as transport,
        AlpacaPaperClient(settings, client=transport) as broker,
    ):
        repo = ProductionRepository(database)
        while not await _blocking(
            lambda: repo.acquire_worker_lock(LOCK_NAME, owner, stale_after_seconds=180)
        ):
            LOGGER.info("v30 is waiting for the prior worker's natural lease handoff")
            await asyncio.sleep(5)
        feed = CryptoMarketFeed(config.symbols, settings)
        updates = AlpacaPaperTradeUpdatesStream(settings)
        runtime = ScalpingRuntime(
            database, broker, config, owner_id=owner, code_sha=sha, order_stream=updates
        )
        runtime.market.on_resnapshot = feed.request_resnapshot

        async def receive_market() -> None:
            async for event in feed.stream():
                if stop.is_set():
                    break
                await queue.put(event)

        async def stop_market() -> None:
            await stop.wait()
            feed.stop()

        async def persist_market() -> None:
            pending: tuple[MarketEvent, ...] = ()
            while not stop.is_set():
                if not pending:
                    try:
                        first = await asyncio.wait_for(queue.get(), timeout=0.25)
                    except TimeoutError:
                        continue
                    batch = [first]
                    while len(batch) < config.raw_batch_size:
                        try:
                            batch.append(queue.get_nowait())
                        except asyncio.QueueEmpty:
                            break
                    pending = tuple(batch)
                try:
                    await _blocking(partial(runtime.process_market_batch, pending))
                except (SQLAlchemyError, httpx.HTTPError) as error:
                    runtime.record_error("market_persistence", error)
                    await _wait(stop, 1)
                    continue
                for _ in pending:
                    queue.task_done()
                pending = ()

        async def execute() -> None:
            while not stop.is_set():
                started = time.monotonic()
                try:
                    await _blocking(runtime.tick)
                except (httpx.HTTPError, SQLAlchemyError, ValueError) as error:
                    runtime.record_error("execution", error)
                await _wait(
                    stop, max(0.05, config.decision_interval_seconds - (time.monotonic() - started))
                )

        async def heartbeat() -> None:
            while not stop.is_set():
                await _blocking(
                    lambda: runtime.heartbeat(
                        feed=feed.health_snapshot(), queued_events=queue.qsize()
                    )
                )
                await _wait(stop, config.heartbeat_interval_seconds)

        async def notices() -> None:
            while not stop.is_set():
                try:
                    await _blocking(
                        lambda: runtime.notifications.publish(
                            runtime.snapshot(
                                feed=feed.health_snapshot(), queued_events=queue.qsize()
                            ),
                            runtime.clock(),
                        )
                    )
                except (SQLAlchemyError, ValueError) as error:
                    runtime.record_error("notification_enqueue", error)
                await _wait(stop, 30)

        async def news() -> None:
            news_settings = NewsWorkerSettings.model_validate({})
            news_owner = owner + "-news"
            with AlpacaNewsClient(AlpacaDataSettings.model_validate({})) as source:
                worker = NewsWorker(
                    news_settings, source, NewsRepository(database), repo, instance_id=news_owner
                )
                while not stop.is_set() and not await _blocking(
                    lambda: repo.acquire_worker_lock(
                        "tradeagent-news-worker",
                        news_owner,
                        stale_after_seconds=news_settings.stale_seconds,
                    )
                ):
                    await _wait(stop, 5)
                try:
                    while not stop.is_set():
                        try:
                            await _blocking(worker.poll_once)
                        except (httpx.HTTPError, SQLAlchemyError, ValueError) as error:
                            runtime.record_error("news", error)
                            error_type = type(error).__name__
                            await _blocking(
                                partial(
                                    repo.heartbeat,
                                    "tradeagent-news-worker",
                                    news_owner,
                                    {"state": "provider_error", "error_type": error_type},
                                )
                            )
                        await _wait(stop, news_settings.poll_seconds)
                finally:
                    await _blocking(
                        lambda: repo.release_worker_lock("tradeagent-news-worker", news_owner)
                    )

        try:
            await _blocking(runtime.initialize)
            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(receive_market())
                tasks.create_task(stop_market())
                tasks.create_task(persist_market())
                tasks.create_task(execute())
                tasks.create_task(heartbeat())
                tasks.create_task(notices())
                tasks.create_task(updates.run(stop))
                tasks.create_task(news())
        finally:
            stop.set()
            feed.stop()
            await _blocking(lambda: repo.release_worker_lock(LOCK_NAME, owner))
