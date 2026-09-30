"""Unattended observation service. No broker client, order engine or submit path."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import func, select

from tradeagent.alpaca_paper import AlpacaPaperSettings
from tradeagent.config import AppConfig
from tradeagent.event_doctor import code_identity
from tradeagent.experimental_policy import reject_live_environment
from tradeagent.persistence import Database, ProductionRepository, events
from tradeagent.scalping_market import CryptoMarketFeed, MarketEvent
from tradeagent.shadow_dataset import (
    DATASET_ID,
    LOCK_NAME,
    PROFILE,
    ShadowDatasetCollector,
    ShadowDatasetProtocol,
    ShadowDatasetStore,
    dataset_status,
    persist_daily_quality,
    release_allowed,
    shadow_datasets,
    shadow_evaluations,
    shadow_labels,
)

LOGGER = logging.getLogger(__name__)


def recover_pending(database: Database, collector: ShadowDatasetCollector) -> None:
    with database.begin() as connection:
        label_count = (
            select(func.count())
            .where(shadow_labels.c.evaluation_id == shadow_evaluations.c.evaluation_id)
            .scalar_subquery()
        )
        rows = list(
            connection.execute(
                select(
                    shadow_evaluations.c.evaluation_id,
                    shadow_evaluations.c.evaluated_at,
                    shadow_evaluations.c.payload,
                ).where(
                    shadow_evaluations.c.dataset_id == collector.protocol.dataset_id,
                    label_count < len(collector.protocol.horizons),
                )
            ).mappings()
        )
        ids = [row["evaluation_id"] for row in rows]
        done = (
            connection.execute(
                select(
                    shadow_labels.c.evaluation_id,
                    shadow_labels.c.horizon_seconds,
                ).where(shadow_labels.c.evaluation_id.in_(ids))
            ).all()
            if ids
            else []
        )
        last = connection.scalar(
            select(func.max(shadow_evaluations.c.evaluated_at)).where(
                shadow_evaluations.c.dataset_id == collector.protocol.dataset_id,
            )
        )
    for row in rows:
        collector.pending[row["evaluation_id"]] = row["payload"]
    for identity, horizon in done:
        collector.completed[identity].add(horizon)
    if last:
        collector.last_slot = int(last.replace(tzinfo=UTC).timestamp()) // 10


async def run_shadow_dataset(
    protocol_path: Path | None = None, *, stop_event: asyncio.Event | None = None
) -> None:
    reject_live_environment()
    if protocol_path:
        protocol = ShadowDatasetProtocol.model_validate_json(
            protocol_path.read_text(encoding="utf-8")
        )
    else:
        with Database(AppConfig().database_url.get_secret_value(), pool_size=1) as source_database:
            with source_database.begin() as connection:
                saved = connection.scalar(
                    select(shadow_datasets.c.protocol).where(
                        shadow_datasets.c.dataset_id == DATASET_ID
                    )
                )
            if not saved:
                raise ValueError("observation protocol must be frozen before service startup")
            protocol = ShadowDatasetProtocol.model_validate(saved)
    actual_sha = code_identity()
    with Database(AppConfig().database_url.get_secret_value(), pool_size=1) as release_database:
        if not release_allowed(release_database, protocol, actual_sha):
            raise ValueError("collector code does not match a frozen or approved pre-start release")
    owner = os.environ.get("RENDER_INSTANCE_ID") or f"{socket.gethostname()}-{os.getpid()}"
    stop = stop_event or asyncio.Event()
    credentials = AlpacaPaperSettings.model_validate({})
    collector = ShadowDatasetCollector(protocol)
    collector.capture_code_sha = actual_sha
    feed = CryptoMarketFeed(
        protocol.symbols,
        credentials,
        stale_after_seconds=5,
        queue_capacity=5000,
    )
    collector.features.on_resnapshot = feed.request_resnapshot
    writer_queue: asyncio.Queue[tuple[str, Any]] = asyncio.Queue(maxsize=5000)
    receive_done = asyncio.Event()
    analysis_trigger = asyncio.Event()
    source_batch: list[MarketEvent] = []
    protocol_end = protocol.end + timedelta(seconds=902)
    with Database(AppConfig().database_url.get_secret_value(), pool_size=3) as database:
        repo = ProductionRepository(database)
        while not await asyncio.to_thread(
            repo.acquire_worker_lock, LOCK_NAME, owner, stale_after_seconds=180
        ):
            LOGGER.info("Observation worker waiting for natural prior-owner lease handoff")
            await asyncio.sleep(5)
        store = ShadowDatasetStore(database, protocol, owner)
        await asyncio.to_thread(store.freeze, datetime.now(UTC))
        await asyncio.to_thread(recover_pending, database, collector)
        from tradeagent.shadow_dataset import shadow_daily_reports

        with database.begin() as connection:
            saved_day = connection.scalar(
                select(func.max(shadow_daily_reports.c.report_date)).where(
                    shadow_daily_reports.c.dataset_id == DATASET_ID
                )
            )
        last_day = (
            datetime.fromisoformat(saved_day).date()
            if saved_day
            else protocol.start.date() - timedelta(days=1)
        )

        async def writer() -> None:
            while True:
                kind, body = await writer_queue.get()
                try:
                    if kind == "stop":
                        return
                    if kind == "tape":
                        await asyncio.to_thread(store.persist_tape, body, datetime.now(UTC))
                    elif kind == "rows":
                        await asyncio.to_thread(store.write, body, datetime.now(UTC))
                    elif kind == "quality":
                        quality, now, sealed = body
                        await asyncio.to_thread(
                            store.update_quality, quality, datetime.now(UTC), sealed=sealed
                        )
                    elif kind == "daily":
                        report_date, now = body
                        await asyncio.to_thread(
                            persist_daily_quality, database, report_date, now=now
                        )
                finally:
                    writer_queue.task_done()

        async def receive() -> None:
            try:
                async for event in feed.stream():
                    if stop.is_set():
                        break
                    now = datetime.now(UTC)
                    collector.on_market(event, now)
                    source_batch.append(event)
                    if len(source_batch) >= 500:
                        await writer_queue.put(("tape", tuple(source_batch)))
                        source_batch.clear()
            finally:
                receive_done.set()

        async def clock() -> None:
            nonlocal last_day
            last_source_flush = datetime.now(UTC)
            last_quality = datetime.min.replace(tzinfo=UTC)
            sealed = False
            while not stop.is_set():
                now = datetime.now(UTC)
                if not sealed:
                    rows = collector.evaluate(now) + collector.resolve(now)
                    if rows:
                        await writer_queue.put(("rows", rows))
                    if source_batch and now - last_source_flush >= timedelta(seconds=10):
                        await writer_queue.put(("tape", tuple(source_batch)))
                        source_batch.clear()
                        last_source_flush = now
                    if now - last_quality >= timedelta(seconds=60):
                        await writer_queue.put(("quality", (collector.quality(), now, False)))
                        last_quality = now
                    due_day = now.date() - timedelta(
                        days=1 if now.hour > 0 or now.minute >= 16 else 2
                    )
                    last_collection_day = protocol.end.date() - timedelta(days=1)
                    while last_day < min(due_day, last_collection_day):
                        last_day += timedelta(days=1)
                        await writer_queue.put(("daily", (last_day, now)))
                    if now >= protocol_end and not collector.pending:
                        feed.stop()
                        await receive_done.wait()
                        if source_batch:
                            await writer_queue.put(("tape", tuple(source_batch)))
                            source_batch.clear()
                        await writer_queue.put(("quality", (collector.quality(), now, True)))
                        await writer_queue.join()
                        await writer_queue.put(("daily", (last_collection_day, now)))
                        analysis_trigger.set()
                        sealed = True
                await asyncio.sleep(0.25 if not sealed else 10)

        async def heartbeat() -> None:
            while not stop.is_set():
                now = datetime.now(UTC)
                renewed = await asyncio.to_thread(
                    repo.refresh_worker_lock, LOCK_NAME, owner, observed_at=now
                )
                if not renewed:
                    raise RuntimeError("observation collector lease lost")
                state = (
                    "warming_up"
                    if now < protocol.start
                    else ("collecting" if now < protocol_end else "observation_complete")
                )
                snapshot = {
                    "state": state,
                    "mode": "observation_only",
                    "entry_policy": PROFILE,
                    "dataset_id": DATASET_ID,
                    "cohort_id": DATASET_ID,
                    "code_sha": actual_sha,
                    "config_hash": protocol.identity,
                    "owner_id": owner,
                    "account_digest": protocol.account_digest,
                    "orders_submitted": 0,
                    "trading_authorization": "expired",
                    "economic_entries_enabled": False,
                    "model_state": "no_support",
                    "start": protocol.start.isoformat(),
                    "end": protocol.end.isoformat(),
                    "feed": feed.health_snapshot(),
                    "quality": collector.quality(),
                    "writer_queue": writer_queue.qsize(),
                    "as_of": now.isoformat(),
                }
                await asyncio.to_thread(
                    repo.set_control,
                    f"shadow-dataset:{DATASET_ID}:status",
                    json.dumps(snapshot, sort_keys=True, default=str),
                )
                await asyncio.to_thread(repo.heartbeat, LOCK_NAME, owner, snapshot, observed_at=now)
                await asyncio.sleep(5)

        async def stopper() -> None:
            await stop.wait()
            feed.stop()

        async def terminal_analysis() -> None:
            await analysis_trigger.wait()
            with database.begin() as connection:
                existing = connection.scalar(
                    select(func.count()).where(
                        events.c.event_type == "shadow_dataset_analysis_result",
                        events.c.trace_id == DATASET_ID,
                    )
                )
            if existing:
                return
            # Collection is sealed; release large caches before the isolated bounded child.
            for history in collector.history.values():
                history.clear()
            script = (
                "import json,resource;from pathlib import Path;from uuid import uuid4;"
                "from datetime import UTC,datetime;"
                "from tradeagent.persistence import Database,ProductionRepository;"
                "from tradeagent.config import AppConfig;"
                "from tradeagent.shadow_dataset_analysis import analyze_dataset;"
                "resource.setrlimit(resource.RLIMIT_AS,(402653184,402653184));"
                "d=Database(AppConfig().database_url.get_secret_value(),pool_size=1);"
                "r=analyze_dataset(d,output_dir=Path('.shadow-alpha-screen-'+str(uuid4())));"
                "ProductionRepository(d).append_event('shadow_dataset_analysis_result',r,"
                "occurred_at=datetime.now(UTC),trace_id='shadow-research-20261001-v1');"
                "print('SHADOW_ANALYSIS_ARCHIVED',r.get('state'));d.dispose()"
            )
            process = await asyncio.create_subprocess_exec(sys.executable, "-c", script)
            try:
                result = await asyncio.wait_for(process.wait(), timeout=1800)
            except TimeoutError:
                process.kill()
                await process.wait()
                result = -1
            if result:
                await asyncio.to_thread(
                    repo.append_event,
                    "shadow_dataset_analysis_result",
                    {
                        "state": "analysis_capacity_or_process_failure",
                        "exit_code": result,
                        "profitability_analysis_performed": False,
                        "promotion_allowed": False,
                        "orders_submitted": 0,
                    },
                    occurred_at=datetime.now(UTC),
                    trace_id=DATASET_ID,
                )

        try:
            with database.begin() as connection:
                sealed_at = connection.scalar(
                    select(shadow_datasets.c.sealed_at).where(
                        shadow_datasets.c.dataset_id == DATASET_ID
                    )
                )
            if sealed_at:
                analysis_trigger.set()
                async with asyncio.TaskGroup() as tasks:
                    tasks.create_task(heartbeat())
                    tasks.create_task(terminal_analysis())
                return
            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(writer())
                tasks.create_task(receive())
                tasks.create_task(clock())
                tasks.create_task(heartbeat())
                tasks.create_task(stopper())
                tasks.create_task(terminal_analysis())
        finally:
            stop.set()
            feed.stop()
            await asyncio.to_thread(repo.release_worker_lock, LOCK_NAME, owner)


def shadow_daily_email(database: Database, now: datetime, timezone: str) -> dict[str, Any]:
    report = dataset_status(database)
    from zoneinfo import ZoneInfo

    local = now.astimezone(ZoneInfo(timezone))
    coverage = (
        "; ".join(
            f"{row['symbol']} {row['horizon_seconds']}s: "
            f"{row['complete']}/{row['resolved']} resolved complete"
            for row in report.get("coverage", [])
            if row["horizon_seconds"] == 60
        )
        or "No matured primary-horizon labels yet."
    )
    text = "\n\n".join(
        (
            f"TradeAgent observation-only research update for {local.date()}. "
            f"Dataset {DATASET_ID} is {report['state']}.",
            "No orders are submitted. The prior trading authorization remains expired, "
            "the qualified model remains no_support, and real-money routing is unavailable.",
            f"Primary 60-second label coverage: {coverage}",
            f"Quality gate: {report.get('profitability_analysis_blocker') or 'coverage passed'}. "
            "Missing labels are not replaced with later prices or zero returns.",
            "The fixed collection ends October 15 UTC, with a fifteen-minute label tail. "
            "Profitability analysis and model promotion are not automatic.",
        )
    )
    return {
        "profile": PROFILE,
        "cohort_id": DATASET_ID,
        "text": text,
        "subject": f"TradeAgent observation research - {local.date()}",
        "completed_round_trips": 0,
        "orders_submitted": 0,
        "quality_report": report,
    }
