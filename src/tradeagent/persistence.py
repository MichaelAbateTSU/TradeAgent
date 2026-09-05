from __future__ import annotations

from contextlib import AbstractContextManager
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    UniqueConstraint,
    create_engine,
    delete,
    func,
    insert,
    select,
    update,
)
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import IntegrityError

metadata = MetaData()

# Table definitions are kept in code so local SQLite and production PostgreSQL share
# identical repository contracts. Alembic owns production upgrades.
events = Table(
    "events_v2",
    metadata,
    Column("event_id", String(36), primary_key=True),
    Column("occurred_at", DateTime(timezone=True), nullable=False),
    Column("recorded_at", DateTime(timezone=True), nullable=False),
    Column("event_type", String(100), nullable=False),
    Column("trace_id", String(128), nullable=False),
    Column("payload", JSON, nullable=False),
)
Index("ix_events_v2_type_time", events.c.event_type, events.c.occurred_at)

controls = Table(
    "controls_v2",
    metadata,
    Column("control_key", String(100), primary_key=True),
    Column("control_value", String(500), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

orders = Table(
    "orders",
    metadata,
    Column("order_id", String(36), primary_key=True),
    Column("client_order_id", String(48), nullable=False, unique=True),
    Column("broker_order_id", String(128), nullable=True, unique=True),
    Column("strategy_version", String(128), nullable=False),
    Column("symbol", String(32), nullable=False),
    Column("side", String(8), nullable=False),
    Column("quantity", Numeric(28, 12), nullable=False),
    Column("filled_quantity", Numeric(28, 12), nullable=False),
    Column("status", String(40), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)
Index("ix_orders_status", orders.c.status)

fills = Table(
    "fills",
    metadata,
    Column("fill_id", String(36), primary_key=True),
    Column("order_id", String(36), ForeignKey("orders.order_id"), nullable=False),
    Column("broker_fill_id", String(128), nullable=False, unique=True),
    Column("quantity", Numeric(28, 12), nullable=False),
    Column("price", Numeric(28, 12), nullable=False),
    Column("fees", Numeric(28, 12), nullable=False),
    Column("filled_at", DateTime(timezone=True), nullable=False),
)

position_cycles = Table(
    "position_cycles",
    metadata,
    Column("cycle_id", String(36), primary_key=True),
    Column("strategy_version", String(128), nullable=False),
    Column("symbol", String(32), nullable=False),
    Column("opened_at", DateTime(timezone=True), nullable=False),
    Column("closed_at", DateTime(timezone=True), nullable=True),
    Column("opening_quantity", Numeric(28, 12), nullable=False),
    Column("opening_vwap", Numeric(28, 12), nullable=False),
    Column("closing_vwap", Numeric(28, 12), nullable=True),
    Column("fees", Numeric(28, 12), nullable=False),
    Column("realized_pnl", Numeric(28, 12), nullable=True),
    Column("outcome", String(10), nullable=True),
    Column("status", String(20), nullable=False),
)
Index("ix_position_cycles_status", position_cycles.c.status)

experiments_v2 = Table(
    "experiments_v2",
    metadata,
    Column("experiment_id", String(36), primary_key=True),
    Column("dataset_hash", String(64), nullable=False),
    Column("config_hash", String(64), nullable=False),
    Column("git_sha", String(64), nullable=False),
    Column("strategy_id", String(128), nullable=False),
    Column("qualified", Boolean, nullable=False),
    Column("report", JSON, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

heartbeats = Table(
    "heartbeats",
    metadata,
    Column("service_name", String(100), primary_key=True),
    Column("instance_id", String(128), nullable=False),
    Column("observed_at", DateTime(timezone=True), nullable=False),
    Column("details", JSON, nullable=False),
)

notification_outbox = Table(
    "notification_outbox",
    metadata,
    Column("notification_id", String(36), primary_key=True),
    Column("cycle_id", String(36), ForeignKey("position_cycles.cycle_id"), nullable=False),
    Column("notification_type", String(50), nullable=False),
    Column("payload", JSON, nullable=False),
    Column("status", String(20), nullable=False),
    Column("attempts", Integer, nullable=False),
    Column("provider_message_id", String(200), nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("sent_at", DateTime(timezone=True), nullable=True),
    UniqueConstraint("cycle_id", "notification_type", name="uq_cycle_notification"),
)
Index("ix_notification_outbox_status", notification_outbox.c.status)

worker_locks = Table(
    "worker_locks",
    metadata,
    Column("lock_name", String(100), primary_key=True),
    Column("owner_id", String(128), nullable=False),
    Column("acquired_at", DateTime(timezone=True), nullable=False),
)

market_bars = Table(
    "market_bars",
    metadata,
    Column("bar_id", String(36), primary_key=True),
    Column("symbol", String(32), nullable=False),
    Column("timeframe", String(20), nullable=False),
    Column("event_at", DateTime(timezone=True), nullable=False),
    Column("received_at", DateTime(timezone=True), nullable=False),
    Column("processed_at", DateTime(timezone=True), nullable=False),
    Column("open", Numeric(28, 12), nullable=False),
    Column("high", Numeric(28, 12), nullable=False),
    Column("low", Numeric(28, 12), nullable=False),
    Column("close", Numeric(28, 12), nullable=False),
    Column("volume", Numeric(28, 12), nullable=False),
    UniqueConstraint(
        "symbol",
        "timeframe",
        "event_at",
        name="uq_market_bar_symbol_timeframe_event",
    ),
)
Index("ix_market_bars_symbol_time", market_bars.c.symbol, market_bars.c.event_at)

market_quotes = Table(
    "market_quotes",
    metadata,
    Column("quote_id", String(36), primary_key=True),
    Column("symbol", String(32), nullable=False),
    Column("event_at", DateTime(timezone=True), nullable=False),
    Column("received_at", DateTime(timezone=True), nullable=False),
    Column("processed_at", DateTime(timezone=True), nullable=False),
    Column("bid_price", Numeric(28, 12), nullable=False),
    Column("ask_price", Numeric(28, 12), nullable=False),
    Column("bid_size", Numeric(28, 12), nullable=False),
    Column("ask_size", Numeric(28, 12), nullable=False),
    UniqueConstraint(
        "symbol",
        "event_at",
        "bid_price",
        "ask_price",
        name="uq_market_quote_event_prices",
    ),
)
Index("ix_market_quotes_symbol_time", market_quotes.c.symbol, market_quotes.c.event_at)


def normalize_database_url(url: str) -> str:
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+psycopg://", 1)
    return url


class Database:
    def __init__(self, url: str) -> None:
        url = normalize_database_url(url)
        self.engine: Engine = create_engine(url, future=True)

    def initialize(self) -> None:
        metadata.create_all(self.engine)

    def begin(self) -> AbstractContextManager[Connection]:
        return self.engine.begin()

    def dispose(self) -> None:
        self.engine.dispose()

    def __enter__(self) -> Database:
        return self

    def __exit__(self, *_: object) -> None:
        self.dispose()


class ProductionRepository:
    def __init__(self, database: Database) -> None:
        self._database = database

    def append_event(
        self,
        event_type: str,
        payload: dict[str, Any],
        *,
        occurred_at: datetime,
        trace_id: str,
    ) -> UUID:
        event_id = uuid4()
        with self._database.begin() as connection:
            connection.execute(
                insert(events).values(
                    event_id=str(event_id),
                    occurred_at=occurred_at,
                    recorded_at=datetime.now(UTC),
                    event_type=event_type,
                    trace_id=trace_id,
                    payload=payload,
                )
            )
        return event_id

    def event_count(self) -> int:
        with self._database.begin() as connection:
            return int(connection.scalar(select(func.count()).select_from(events)) or 0)

    def set_control(self, key: str, value: str) -> None:
        now = datetime.now(UTC)
        with self._database.begin() as connection:
            current = connection.scalar(
                select(controls.c.control_key).where(controls.c.control_key == key)
            )
            if current is None:
                connection.execute(
                    insert(controls).values(
                        control_key=key,
                        control_value=value,
                        updated_at=now,
                    )
                )
            else:
                connection.execute(
                    update(controls)
                    .where(controls.c.control_key == key)
                    .values(control_value=value, updated_at=now)
                )

    def get_control(self, key: str) -> str | None:
        with self._database.begin() as connection:
            value = connection.scalar(
                select(controls.c.control_value).where(controls.c.control_key == key)
            )
            return str(value) if value is not None else None

    def heartbeat(
        self,
        service_name: str,
        instance_id: str,
        details: dict[str, Any],
        *,
        observed_at: datetime | None = None,
    ) -> None:
        timestamp = observed_at or datetime.now(UTC)
        with self._database.begin() as connection:
            current = connection.scalar(
                select(heartbeats.c.service_name).where(heartbeats.c.service_name == service_name)
            )
            values = {
                "instance_id": instance_id,
                "observed_at": timestamp,
                "details": details,
            }
            if current is None:
                connection.execute(insert(heartbeats).values(service_name=service_name, **values))
            else:
                connection.execute(
                    update(heartbeats)
                    .where(heartbeats.c.service_name == service_name)
                    .values(**values)
                )

    def latest_heartbeat(self, service_name: str) -> tuple[str, datetime, dict[str, Any]] | None:
        with self._database.begin() as connection:
            row = (
                connection.execute(
                    select(heartbeats).where(heartbeats.c.service_name == service_name)
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            return None
        observed_at = row["observed_at"]
        if observed_at.tzinfo is None:
            observed_at = observed_at.replace(tzinfo=UTC)
        return (
            str(row["instance_id"]),
            observed_at,
            dict(row["details"]),
        )

    def store_market_bar(
        self,
        *,
        symbol: str,
        timeframe: str,
        event_at: datetime,
        received_at: datetime,
        open_price: Decimal,
        high_price: Decimal,
        low_price: Decimal,
        close_price: Decimal,
        volume: Decimal,
    ) -> bool:
        with self._database.begin() as connection:
            existing = connection.scalar(
                select(market_bars.c.bar_id).where(
                    market_bars.c.symbol == symbol,
                    market_bars.c.timeframe == timeframe,
                    market_bars.c.event_at == event_at,
                )
            )
            if existing is not None:
                return False
            connection.execute(
                insert(market_bars).values(
                    bar_id=str(uuid4()),
                    symbol=symbol,
                    timeframe=timeframe,
                    event_at=event_at,
                    received_at=received_at,
                    processed_at=datetime.now(UTC),
                    open=open_price,
                    high=high_price,
                    low=low_price,
                    close=close_price,
                    volume=volume,
                )
            )
        return True

    def store_market_quote(
        self,
        *,
        symbol: str,
        event_at: datetime,
        received_at: datetime,
        bid_price: Decimal,
        ask_price: Decimal,
        bid_size: Decimal,
        ask_size: Decimal,
    ) -> bool:
        with self._database.begin() as connection:
            existing = connection.scalar(
                select(market_quotes.c.quote_id).where(
                    market_quotes.c.symbol == symbol,
                    market_quotes.c.event_at == event_at,
                    market_quotes.c.bid_price == bid_price,
                    market_quotes.c.ask_price == ask_price,
                )
            )
            if existing is not None:
                return False
            connection.execute(
                insert(market_quotes).values(
                    quote_id=str(uuid4()),
                    symbol=symbol,
                    event_at=event_at,
                    received_at=received_at,
                    processed_at=datetime.now(UTC),
                    bid_price=bid_price,
                    ask_price=ask_price,
                    bid_size=bid_size,
                    ask_size=ask_size,
                )
            )
        return True

    def market_data_counts(self) -> tuple[int, int]:
        with self._database.begin() as connection:
            bars_count = connection.scalar(select(func.count()).select_from(market_bars))
            quotes_count = connection.scalar(select(func.count()).select_from(market_quotes))
        return int(bars_count or 0), int(quotes_count or 0)

    def acquire_worker_lock(self, lock_name: str, owner_id: str) -> bool:
        try:
            with self._database.begin() as connection:
                connection.execute(
                    insert(worker_locks).values(
                        lock_name=lock_name,
                        owner_id=owner_id,
                        acquired_at=datetime.now(UTC),
                    )
                )
            return True
        except IntegrityError:
            return False

    def release_worker_lock(self, lock_name: str, owner_id: str) -> bool:
        with self._database.begin() as connection:
            result = connection.execute(
                delete(worker_locks).where(
                    worker_locks.c.lock_name == lock_name,
                    worker_locks.c.owner_id == owner_id,
                )
            )
            return bool(result.rowcount)
