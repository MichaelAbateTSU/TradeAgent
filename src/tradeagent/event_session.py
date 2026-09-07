from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from typing import Any, Protocol

from pydantic import AwareDatetime, BaseModel, ConfigDict
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Connection
from sqlalchemy.sql.dml import Insert

from tradeagent.alpaca_paper import PaperCalendarSession
from tradeagent.config import IntradayConfig
from tradeagent.intraday import NyseSessionCalendar
from tradeagent.persistence import controls

TUESDAY_PROTOCOL_VERSION = "tuesday-paper-v1"
CANDIDATE_SELECTION_RULE = "latest-primary-receipt_then_symbol_then_evidence-id"


class CalendarBroker(Protocol):
    def calendar(self, *, start: date, end: date) -> tuple[PaperCalendarSession, ...]: ...


class PaperSessionPlan(BaseModel):
    model_config = ConfigDict(frozen=True)
    session_date: date
    session_open: AwareDatetime
    session_close: AwareDatetime
    previous_session_close: AwareDatetime
    verified_at: AwareDatetime
    calendar_source: str = "https://paper-api.alpaca.markets/v2/calendar"


def verify_session_plan(
    broker: CalendarBroker, session_date: date, config: IntradayConfig, now: datetime
) -> PaperSessionPlan:
    sessions = broker.calendar(start=session_date - timedelta(days=10), end=session_date)
    planned = next((item for item in sessions if item.session_date == session_date), None)
    earlier = [item for item in sessions if item.session_date < session_date]
    if planned is None or not earlier:
        raise ValueError("broker calendar lacks the planned session or preceding regular close")
    expected = NyseSessionCalendar(config).session_bounds(session_date)
    if expected != (planned.open_at.astimezone(UTC), planned.close_at.astimezone(UTC)):
        raise ValueError("broker calendar and independent exchange calendar disagree")
    return PaperSessionPlan(
        session_date=session_date,
        session_open=planned.open_at,
        session_close=planned.close_at,
        previous_session_close=max(item.close_at for item in earlier),
        verified_at=now,
    )


def session_identity(account_digest: str, session_date: date) -> str:
    return sha256(f"tradeagent-v20-practice:{session_date}:{account_digest}".encode()).hexdigest()


def equipment_identity(account_digest: str, session_date: date) -> str:
    return sha256(
        f"tradeagent-v20:EQUIPMENT_TEST:{session_date}:AAPL:{account_digest}".encode()
    ).hexdigest()


def empty_session_budget(account_digest: str, session_date: date) -> dict[str, Any]:
    return {
        "session_id": session_identity(account_digest, session_date),
        "planned_session_date": session_date.isoformat(),
        "account_digest": account_digest,
        "total_entries_reserved": 0,
        "news_entries_reserved": 0,
        "equipment_test_id": equipment_identity(account_digest, session_date),
        "equipment_client_order_id": None,
        "equipment_cohort_id": None,
    }


def session_control_key(account_digest: str, session_date: date) -> str:
    return "practice-session:" + session_identity(account_digest, session_date)


def locked_session_budget(
    connection: Connection, account_digest: str, session_date: date, now: datetime
) -> dict[str, Any]:
    key = session_control_key(account_digest, session_date)
    values = {
        "control_key": key,
        "control_value": json.dumps(empty_session_budget(account_digest, session_date)),
        "updated_at": now,
    }
    statement: Insert
    if connection.dialect.name == "postgresql":
        statement = pg_insert(controls).values(**values).on_conflict_do_nothing()
    elif connection.dialect.name == "sqlite":
        statement = sqlite_insert(controls).values(**values).on_conflict_do_nothing()
    else:
        raise ValueError("practice session budgets require PostgreSQL or SQLite")
    connection.execute(statement)
    raw = connection.execute(
        select(controls.c.control_value).where(controls.c.control_key == key).with_for_update()
    ).scalar_one()
    budget = json.loads(raw)
    if budget["account_digest"] != account_digest or budget["planned_session_date"] != str(
        session_date
    ):
        raise ValueError("persisted practice session identity mismatch")
    return dict(budget)


def save_session_budget(connection: Connection, budget: dict[str, Any], now: datetime) -> None:
    key = session_control_key(
        str(budget["account_digest"]), date.fromisoformat(str(budget["planned_session_date"]))
    )
    connection.execute(
        update(controls)
        .where(controls.c.control_key == key)
        .values(control_value=json.dumps(budget, sort_keys=True), updated_at=now)
    )
