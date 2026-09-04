from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel


def _json_default(value: object) -> str:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    return str(value)


class SQLiteLedger:
    """Append-only local event ledger; broker truth remains authoritative."""

    def __init__(self, path: Path | str) -> None:
        raw_path = str(path)
        if raw_path != ":memory:":
            Path(raw_path).parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(raw_path)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                occurred_at TEXT NOT NULL,
                recorded_at TEXT NOT NULL,
                event_type TEXT NOT NULL,
                trace_id TEXT NOT NULL,
                payload TEXT NOT NULL
            )
            """
        )
        self._connection.commit()

    def append(
        self,
        event_type: str,
        payload: BaseModel | dict[str, Any],
        *,
        occurred_at: datetime,
        trace_id: str,
    ) -> int:
        value = payload.model_dump(mode="json") if isinstance(payload, BaseModel) else payload
        cursor = self._connection.execute(
            """
            INSERT INTO events (occurred_at, recorded_at, event_type, trace_id, payload)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                occurred_at.astimezone(UTC).isoformat(),
                datetime.now(UTC).isoformat(),
                event_type,
                trace_id,
                json.dumps(value, default=_json_default, sort_keys=True),
            ),
        )
        self._connection.commit()
        if cursor.lastrowid is None:
            raise RuntimeError("ledger insert did not return a sequence")
        return cursor.lastrowid

    def events(self, *, limit: int = 100) -> Iterator[dict[str, Any]]:
        rows = self._connection.execute(
            """
            SELECT sequence, occurred_at, recorded_at, event_type, trace_id, payload
            FROM events ORDER BY sequence DESC LIMIT ?
            """,
            (limit,),
        )
        for row in rows:
            yield {
                "sequence": row["sequence"],
                "occurred_at": row["occurred_at"],
                "recorded_at": row["recorded_at"],
                "event_type": row["event_type"],
                "trace_id": row["trace_id"],
                "payload": json.loads(row["payload"]),
            }

    def event_count(self) -> int:
        row = self._connection.execute("SELECT COUNT(*) AS count FROM events").fetchone()
        return int(row["count"])

    def event_counts(self) -> dict[str, int]:
        rows = self._connection.execute(
            "SELECT event_type, COUNT(*) AS count FROM events GROUP BY event_type"
        )
        return {str(row["event_type"]): int(row["count"]) for row in rows}

    def latest_event(self, event_type: str) -> dict[str, Any] | None:
        row = self._connection.execute(
            """
            SELECT sequence, occurred_at, recorded_at, event_type, trace_id, payload
            FROM events WHERE event_type = ? ORDER BY sequence DESC LIMIT 1
            """,
            (event_type,),
        ).fetchone()
        if row is None:
            return None
        return {
            "sequence": row["sequence"],
            "occurred_at": row["occurred_at"],
            "recorded_at": row["recorded_at"],
            "event_type": row["event_type"],
            "trace_id": row["trace_id"],
            "payload": json.loads(row["payload"]),
        }

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> SQLiteLedger:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
