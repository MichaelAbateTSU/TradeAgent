"""Build versioned report metadata only; never change originals, controls or orders."""

from __future__ import annotations

import argparse
import json
import time
from datetime import UTC, datetime

from sqlalchemy import select, text

from tradeagent.config import AppConfig
from tradeagent.persistence import Database, heartbeats
from tradeagent.reporting_metadata import (
    backfill_reporting_metadata_page,
    missing_reporting_metadata,
)
from tradeagent.reporting_reads import REPORTING_PROJECTION_VERSION


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--pause-seconds", type=float, default=0.1)
    parser.add_argument("--max-seconds", type=float, default=900)
    parser.add_argument("--max-queue-depth", type=int, default=2000)
    args = parser.parse_args()
    if (
        not 1 <= args.batch_size <= 32
        or not 0 <= args.pause_seconds <= 10
        or not 0 < args.max_seconds <= 3600
        or not 0 <= args.max_queue_depth <= 5000
    ):
        parser.error("Invalid bounded backfill limits")
    started = time.monotonic()
    total = 0
    pages = 0
    cursor = None
    last_log = started
    with Database(AppConfig().database_url.get_secret_value(), pool_size=1) as database:
        while True:
            if time.monotonic() - started > args.max_seconds:
                raise RuntimeError("Backfill deadline reached; committed pages are resumable")
            with database.begin() as connection:
                queue = connection.scalar(
                    select(heartbeats.c.details["queue_depth"]).where(
                        heartbeats.c.service_name == "tradeagent-shadow-recorder"
                    )
                )
            if queue is not None and int(queue) > args.max_queue_depth:
                time.sleep(1)
                continue
            page = backfill_reporting_metadata_page(
                database, after_event_id=cursor, batch_size=args.batch_size
            )
            total += page["inserted"]
            pages += 1
            cursor = page["last_event_id"]
            if not page["selected"]:
                remaining = missing_reporting_metadata(database)
                if not remaining:
                    with database.begin() as connection:
                        if connection.dialect.name == "postgresql":
                            connection.execute(text("SET LOCAL statement_timeout = '20s'"))
                            connection.execute(text("ANALYZE event_reporting_metadata"))
                    print(
                        "METADATA_BACKFILL "
                        + json.dumps(
                            {
                                "observed_at": datetime.now(UTC).isoformat(),
                                "projection_version": REPORTING_PROJECTION_VERSION,
                                "inserted": total,
                                "pages": pages,
                                "remaining": remaining,
                                "seconds": time.monotonic() - started,
                                "originals_modified": 0,
                                "controls_modified": 0,
                                "orders_submitted": 0,
                            }
                        ),
                        flush=True,
                    )
                    return
                # Catch originals inserted behind the cursor during a rolling deployment.
                cursor = None
            if time.monotonic() - last_log >= 10:
                print(
                    "METADATA_PROGRESS "
                    + json.dumps({"inserted": total, "pages": pages, "last_event_id": cursor}),
                    flush=True,
                )
                last_log = time.monotonic()
            time.sleep(args.pause_seconds)


if __name__ == "__main__":
    main()
