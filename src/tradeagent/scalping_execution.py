"""v30 paper-only execution. Legacy strategy authorization/risk gates do not apply."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from contextlib import nullcontext, suppress
from datetime import UTC, datetime, timedelta
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from email.utils import parsedate_to_datetime
from hashlib import sha256
from typing import Any
from uuid import NAMESPACE_URL, uuid5

import httpx
from sqlalchemy import func, or_, select, update
from sqlalchemy.engine import Connection

from tradeagent.alpaca_paper import (
    AlpacaPaperOrder,
    PaperCryptoAsset,
    canonical_crypto_symbol,
)
from tradeagent.domain import OrderRequest, OrderType, Side
from tradeagent.persistence import Database, controls, orders, worker_locks
from tradeagent.scalping_config import ScalpingConfig, ScalpInventory, ScalpQuote, ScalpSignal
from tradeagent.scalping_store import (
    ScalpStore,
    canonical,
    insert_once,
    json_value,
    scalping_activities,
    scalping_cycles,
    scalping_order_links,
    utc,
)

FINAL = frozenset({"filled", "canceled", "expired", "rejected"})
CLOSED = frozenset({"closed_owned_flat", "no_fill"})
PAPER_HOST = "https://paper-api.alpaca.markets"


def number(value: Any) -> Decimal:
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError("non-finite broker amount")
    return result


def stamp(value: Any) -> datetime:
    result = (
        value
        if isinstance(value, datetime)
        else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    )
    if result.tzinfo is None:
        raise ValueError("broker timestamp must be aware")
    return result.astimezone(UTC)


def symbol_key(value: str) -> str:
    try:
        return canonical_crypto_symbol(value)
    except ValueError:
        return value.upper()


def floor_quantity(quantity: Decimal, increment: Decimal) -> Decimal:
    return (quantity / increment).to_integral_value(rounding=ROUND_FLOOR) * increment


def ledger_amount(cycle: dict[str, Any], key: str) -> Decimal:
    return number(cycle["payload"].get("ledger", {}).get(key, cycle[key]))


class ScalpOrderEngine:
    def __init__(
        self,
        database: Database,
        broker: Any,
        config: ScalpingConfig,
        *,
        owner_id: str,
        code_sha: str,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ):
        self.database, self.broker, self.config = database, broker, config
        self.owner_id, self.code_sha, self.clock = owner_id, code_sha, clock
        self.store = ScalpStore(database)
        self.run_id: str | None = None
        self.account_key = "v30-account:" + config.account_digest
        self.manual_stop_key = "v30-stop:" + config.account_digest
        self._assets: dict[str, PaperCryptoAsset] = {}
        self._asset_observed_at: dict[str, datetime] = {}
        self._positions: dict[str, Decimal] = {}
        self._available: dict[str, Decimal] = {}
        self._external_open: list[str] = []
        self._positions_at: datetime | None = None
        self._positions_dirty = True
        self._state = "not_initialized"
        self._error: str | None = None
        self._next_reconcile: datetime | None = None
        self._retry_after: datetime | None = None
        self._backoff_seconds = 1

    def _account(self) -> Any:
        if self.broker.broker_host != PAPER_HOST:
            raise ValueError("v30 is structurally paper-only")
        account = self.broker.account()
        if (
            sha256(account.id.encode()).hexdigest() != self.config.account_digest
            or account.currency != "USD"
            or account.status != "ACTIVE"
            or account.account_blocked
            or account.trading_blocked
        ):
            raise ValueError("pinned active paper account required")
        return account

    def _lease(self, connection: Any, now: datetime) -> None:
        row = (
            connection.execute(
                select(worker_locks)
                .where(worker_locks.c.lock_name == "tradeagent-event-worker")
                .with_for_update()
            )
            .mappings()
            .one_or_none()
        )
        if (
            row is None
            or row["owner_id"] != self.owner_id
            or not timedelta(0) <= now - utc(row["acquired_at"]) <= timedelta(seconds=30)
        ):
            raise ValueError("current fresh global event-worker lease required")

    def _account_state(self) -> dict[str, Any]:
        with self.database.begin() as connection:
            raw = connection.scalar(
                select(controls.c.control_value).where(controls.c.control_key == self.account_key)
            )
        return json.loads(raw) if raw else {}

    def _save_account_state(
        self, patch: dict[str, Any], *, connection: Connection | None = None
    ) -> None:
        now = self.clock()
        transaction = self.database.begin() if connection is None else nullcontext(connection)
        with transaction as connection:
            self._lease(connection, now)
            raw = connection.scalar(
                select(controls.c.control_value)
                .where(controls.c.control_key == self.account_key)
                .with_for_update()
            )
            state = json.loads(raw) if raw else {}
            state.update(json_value(patch))
            if raw is None:
                insert_once(
                    connection,
                    controls,
                    {
                        "control_key": self.account_key,
                        "control_value": canonical(state),
                        "updated_at": now,
                    },
                )
            else:
                connection.execute(
                    update(controls)
                    .where(controls.c.control_key == self.account_key)
                    .values(control_value=canonical(state), updated_at=now)
                )

    def _asset(self, symbol: str) -> PaperCryptoAsset:
        if symbol not in self._assets or self.clock() - self._asset_observed_at[
            symbol
        ] >= timedelta(seconds=self.config.reconcile_interval_seconds):
            result = self.broker.crypto_asset(symbol)
            self._assets[symbol] = (
                result
                if isinstance(result, PaperCryptoAsset)
                else PaperCryptoAsset.model_validate(result)
            )
            self._asset_observed_at[symbol] = self.clock()
        asset = self._assets[symbol]
        if asset.symbol != symbol or not asset.tradable or asset.status != "active":
            raise ValueError("broker crypto asset is unavailable")
        return asset

    def _read_positions(self, *, force: bool = False) -> None:
        if (
            not force
            and not self._positions_dirty
            and self._positions_at is not None
            and self.clock() - self._positions_at
            < timedelta(seconds=self.config.reconcile_interval_seconds)
        ):
            return
        self._account()
        positions = self.broker.positions()
        quantities = {symbol_key(row.symbol): number(row.quantity) for row in positions}
        if len(quantities) != len(positions):
            raise ValueError("duplicate normalized broker positions")
        available = {
            symbol_key(row.symbol): min(
                number(row.quantity),
                number(row.available_quantity)
                if getattr(row, "available_quantity", None) is not None
                else number(row.quantity),
            )
            for row in positions
        }
        open_orders = self.broker.open_orders()
        self._account()
        if any(quantity < 0 for symbol, quantity in quantities.items() if "/" in symbol):
            raise ValueError("unexpected negative crypto inventory")
        self._positions, self._available = quantities, available
        with self.database.begin() as connection:
            owned = set(connection.scalars(select(scalping_order_links.c.client_order_id)))
        self._external_open = [
            row.client_order_id for row in open_orders if row.client_order_id not in owned
        ]
        self._positions_at, self._positions_dirty = self.clock(), False

    def initialize(self) -> None:
        if self.config.approved_at > self.clock():
            raise ValueError("the recorded owner approval cannot be in the future")
        prior_state = self._account_state()
        if prior_state.get("retry_after") and stamp(prior_state["retry_after"]) > self.clock():
            with self.database.begin() as connection:
                self._lease(connection, self.clock())
            self.run_id = self.store.freeze_run(self.config, self.code_sha, at=self.clock())
            self._retry_after = stamp(prior_state["retry_after"])
            self._state = "broker_backoff"
            return
        self._account()
        with self.database.begin() as connection:
            self._lease(connection, self.clock())
        self.run_id = self.store.freeze_run(self.config, self.code_sha, at=self.clock())
        self._read_positions()
        self._account()
        state = self._account_state()
        if not state:
            with self.database.begin() as connection:
                existing = connection.scalar(
                    select(scalping_cycles.c.cycle_id)
                    .where(scalping_cycles.c.account_digest == self.config.account_digest)
                    .limit(1)
                )
            if existing is not None:
                raise ValueError(
                    "ownership baseline missing; existing cycles cannot be reclassified"
                )
            now = self.clock()
            after = (now - timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
            self._save_account_state(
                {
                    "account_digest": self.config.account_digest,
                    "captured_at": now.isoformat(),
                    "unowned_positions": {
                        key: str(value) for key, value in self._positions.items()
                    },
                    "initial_unowned_open_orders": self._external_open,
                    "initial_unowned_positions": {
                        key: str(value) for key, value in self._positions.items()
                    },
                    "activity_watermark": after.isoformat(),
                    "activity_initial_complete": False,
                }
            )
            self.store.audit("ownership_baseline", self._account_state(), at=now)
        elif state.get("account_digest") != self.config.account_digest:
            raise ValueError("persisted account identity mismatch")
        if state.get("retry_after"):
            self._retry_after = stamp(state["retry_after"])
        self._state = "running"
        self.reconcile(now=self.clock())

    def _cycles(self, *, active: bool = True) -> list[dict[str, Any]]:
        condition = scalping_cycles.c.account_digest == self.config.account_digest
        if active:
            condition &= scalping_cycles.c.state.not_in(CLOSED)
        with self.database.begin() as connection:
            return [
                dict(row)
                for row in connection.execute(
                    select(scalping_cycles).where(condition).order_by(scalping_cycles.c.created_at)
                ).mappings()
            ]

    def inventory(self) -> dict[str, ScalpInventory]:
        combined: dict[str, dict[str, Any]] = {}
        for cycle in self._cycles():
            qty = ledger_amount(cycle, "owned_quantity")
            bought = ledger_amount(cycle, "entry_quantity")
            if (
                qty <= 0
                or bought <= 0
                or cycle["entry_value"] is None
                or cycle["opened_at"] is None
            ):
                continue
            group = combined.setdefault(
                cycle["symbol"],
                {
                    "quantity": Decimal(0),
                    "cost": Decimal(0),
                    "opened_at": utc(cycle["opened_at"]),
                },
            )
            group["quantity"] += qty
            group["cost"] += qty * ledger_amount(cycle, "entry_value") / bought
            group["opened_at"] = min(group["opened_at"], utc(cycle["opened_at"]))
        return {
            symbol: ScalpInventory(
                symbol=symbol,
                quantity=value["quantity"],
                opened_at=value["opened_at"],
                opening_vwap=value["cost"] / value["quantity"],
            )
            for symbol, value in combined.items()
        }

    def _manual_stop(self) -> bool:
        with self.database.begin() as connection:
            return (
                connection.scalar(
                    select(controls.c.control_value).where(
                        controls.c.control_key == self.manual_stop_key
                    )
                )
                == "active"
            )

    def _unresolved_orders(self) -> int:
        with self.database.begin() as connection:
            return int(
                connection.scalar(
                    select(func.count())
                    .select_from(orders)
                    .join(
                        scalping_order_links,
                        scalping_order_links.c.client_order_id == orders.c.client_order_id,
                    )
                    .join(
                        scalping_cycles,
                        scalping_cycles.c.cycle_id == scalping_order_links.c.cycle_id,
                    )
                    .where(
                        scalping_cycles.c.account_digest == self.config.account_digest,
                        orders.c.status.not_in(FINAL),
                        or_(
                            scalping_order_links.c.dispatch_state.in_(("unknown", "dispatching")),
                            scalping_order_links.c.cancel_requested_at.is_not(None),
                            orders.c.status.in_(
                                ("reconciliation_required", "cancel_pending", "pending_cancel")
                            ),
                        ),
                    )
                )
                or 0
            )

    def _pending_symbols(self) -> set[str]:
        with self.database.begin() as connection:
            return set(
                connection.scalars(
                    select(orders.c.symbol)
                    .join(
                        scalping_order_links,
                        scalping_order_links.c.client_order_id == orders.c.client_order_id,
                    )
                    .join(
                        scalping_cycles,
                        scalping_cycles.c.cycle_id == scalping_order_links.c.cycle_id,
                    )
                    .where(
                        scalping_cycles.c.account_digest == self.config.account_digest,
                        orders.c.status.not_in(FINAL),
                    )
                    .distinct()
                )
            )

    def _unresolved_inventory(self) -> int:
        with self.database.begin() as connection:
            return int(
                connection.scalar(
                    select(func.count())
                    .select_from(scalping_cycles)
                    .where(
                        scalping_cycles.c.account_digest == self.config.account_digest,
                        scalping_cycles.c.state == "ownership_pending",
                    )
                )
                or 0
            )

    def _cash_capacity(
        self, connection: Any, *, exclude_client_id: str | None = None
    ) -> dict[str, Any]:
        pending = (
            connection.execute(
                select(
                    orders.c.client_order_id,
                    orders.c.symbol,
                    scalping_order_links.c.intent,
                    scalping_order_links.c.broker,
                )
                .join(
                    scalping_order_links,
                    scalping_order_links.c.client_order_id == orders.c.client_order_id,
                )
                .join(
                    scalping_cycles, scalping_cycles.c.cycle_id == scalping_order_links.c.cycle_id
                )
                .where(
                    scalping_cycles.c.account_digest == self.config.account_digest,
                    orders.c.side == "buy",
                    orders.c.status.not_in(FINAL),
                )
            )
            .mappings()
            .all()
        )
        commitments: dict[str, Decimal] = {}
        for row in pending:
            if row["client_order_id"] == exclude_client_id:
                continue
            intent = row["intent"]
            if intent.get("limit_price") is None:
                raise ValueError("unbounded pending BUY cash commitment")
            remaining = max(
                Decimal(0),
                number(intent["request"]["quantity"])
                - number((row["broker"] or {}).get("filled_quantity", 0)),
            )
            commitments[row["client_order_id"]] = remaining * number(intent["limit_price"])
        # Unknown external orders cannot be funded using an assumed margin allowance.
        broker_open = self.broker.open_orders()
        open_ids = {item.client_order_id for item in broker_open}
        owned_ids = (
            set(
                connection.scalars(
                    select(scalping_order_links.c.client_order_id)
                    .join(
                        scalping_cycles,
                        scalping_cycles.c.cycle_id == scalping_order_links.c.cycle_id,
                    )
                    .where(
                        scalping_cycles.c.account_digest == self.config.account_digest,
                        scalping_order_links.c.client_order_id.in_(open_ids),
                    )
                )
            )
            if open_ids
            else set()
        )
        external = [
            item.client_order_id
            for item in broker_open
            if item.client_order_id not in owned_ids and item.client_order_id != exclude_client_id
        ]
        if external:
            self._external_open = external
            raise ValueError("unowned broker orders require cash/ownership reconciliation")
        account = self._account()
        cash = number(account.cash)
        reserved = sum(commitments.values(), Decimal(0))
        available = max(Decimal(0), cash - reserved)
        non_marginable = getattr(account, "non_marginable_buying_power", None)
        if non_marginable is not None:
            available = min(available, max(Decimal(0), number(non_marginable)))
        return {
            "cash": str(cash),
            "pending_buy_commitments": str(reserved),
            "non_marginable_buying_power": str(non_marginable)
            if non_marginable is not None
            else None,
            "available_crypto_cash": str(available),
            "leveraged_buying_power_used": False,
        }

    def consume_order_update(self, order: AlpacaPaperOrder, *, observed_at: datetime) -> None:
        """Trading-thread only: apply owned snapshots without broker I/O or order actions.

        Drain queued updates before step(); step refreshes net-coin inventory once,
        rather than issuing position/account requests for every partial-fill message.
        """
        if self.run_id is None:
            raise ValueError("initialize the engine before consuming order updates")
        received_at = stamp(observed_at)
        now = self.clock()
        if received_at > now or self.broker.broker_host != PAPER_HOST:
            raise ValueError("a current paper-stream receipt is required")
        if order.updated_at is not None and stamp(order.updated_at) > received_at:
            raise ValueError("order state cannot postdate its stream receipt")
        with self.database.begin() as connection:
            self._lease(connection, now)
            owned = connection.scalar(
                select(scalping_order_links.c.cycle_id)
                .join(
                    scalping_cycles, scalping_cycles.c.cycle_id == scalping_order_links.c.cycle_id
                )
                .where(
                    scalping_order_links.c.client_order_id == order.client_order_id,
                    scalping_cycles.c.account_digest == self.config.account_digest,
                )
            )
            if owned is None:
                raw = order.model_dump(mode="json")
                self.store.audit(
                    "unowned_stream_update",
                    {
                        "run_id": self.run_id,
                        "stream_received_at": received_at.isoformat(),
                        "broker": raw,
                        "adopted": False,
                    },
                    at=now,
                    connection=connection,
                    identity="scalp-unowned-stream:" + sha256(canonical(raw).encode()).hexdigest(),
                )
                return
            self._update_order(order.client_order_id, order, now, received_at=received_at)

    def _update_order(
        self,
        client_id: str,
        broker_order: AlpacaPaperOrder,
        now: datetime,
        *,
        received_at: datetime | None = None,
    ) -> str:
        raw = broker_order.model_dump(mode="json")
        self.store.audit(
            "broker_order_observation",
            {
                "client_order_id": client_id,
                "broker": raw,
                "source_received_at": (received_at or now).isoformat(),
            },
            at=now,
            identity=f"scalp-response:{client_id}:{sha256(canonical(raw).encode()).hexdigest()}"
            + (f":stream:{received_at.isoformat()}" if received_at else ""),
        )
        with self.database.begin() as connection:
            row = (
                connection.execute(
                    select(
                        orders,
                        scalping_order_links.c.broker,
                        scalping_order_links.c.cycle_id,
                        scalping_order_links.c.first_positive_fill_at,
                        scalping_order_links.c.intent,
                    )
                    .join(
                        scalping_order_links,
                        scalping_order_links.c.client_order_id == orders.c.client_order_id,
                    )
                    .where(orders.c.client_order_id == client_id)
                    .with_for_update()
                )
                .mappings()
                .one()
            )
            if (
                broker_order.client_order_id != client_id
                or symbol_key(broker_order.symbol) != row["symbol"]
                or broker_order.side != row["side"]
                or not number(broker_order.filled_quantity) >= 0
                or broker_order.quantity is None
                or number(broker_order.quantity) != number(row["intent"]["request"]["quantity"])
                or broker_order.filled_quantity > broker_order.quantity
                or (
                    broker_order.filled_average_price is not None
                    and number(broker_order.filled_average_price) <= 0
                )
                or stamp(broker_order.created_at) > now
            ):
                raise ValueError("broker response does not match the durable order intent")
            previous = row["broker"] or {}
            previous_qty = number(previous.get("filled_quantity", 0))
            quantity = number(broker_order.filled_quantity)
            previous_price = previous.get("filled_average_price")
            price = broker_order.filled_average_price
            older_version = (
                broker_order.updated_at is not None
                and previous.get("updated_at") is not None
                and stamp(broker_order.updated_at) < stamp(previous["updated_at"])
                and quantity <= previous_qty
            )
            if (
                older_version
                or quantity < previous_qty
                or (
                    previous_price is not None
                    and price is not None
                    and quantity * price < previous_qty * number(previous_price)
                )
                or (row["status"] in FINAL and broker_order.status.value not in FINAL)
            ):
                self.store.audit(
                    "order_observation_ignored",
                    {
                        "cycle_id": row["cycle_id"],
                        "client_order_id": client_id,
                        "reason": "regressive cumulative fill or state",
                        "broker": raw,
                    },
                    at=now,
                    connection=connection,
                )
                return str(row["cycle_id"])
            previous_first_fill = (
                utc(row["first_positive_fill_at"]) if row["first_positive_fill_at"] else None
            )
            first_fill = previous_first_fill
            if quantity > 0:
                candidate_fill = (
                    stamp(broker_order.filled_at) if broker_order.filled_at else received_at or now
                )
                if candidate_fill > (received_at or now) or candidate_fill < stamp(
                    broker_order.created_at
                ):
                    raise ValueError("future broker fill")
                first_fill = min(utc(first_fill), candidate_fill) if first_fill else candidate_fill
            connection.execute(
                update(orders)
                .where(orders.c.client_order_id == client_id)
                .values(
                    broker_order_id=broker_order.id,
                    filled_quantity=quantity,
                    status=broker_order.status.value,
                    updated_at=now,
                )
            )
            connection.execute(
                update(scalping_order_links)
                .where(scalping_order_links.c.client_order_id == client_id)
                .values(
                    broker=raw,
                    dispatch_state="acknowledged",
                    first_positive_fill_at=first_fill,
                    last_reconciled_at=now,
                )
            )
            if raw != previous or first_fill != previous_first_fill:
                self._positions_dirty |= quantity != previous_qty or (
                    row["side"] == "sell" and row["status"] != broker_order.status.value
                )
                self.store.audit(
                    "order_update",
                    {
                        "cycle_id": row["cycle_id"],
                        "client_order_id": client_id,
                        "previous": previous,
                        "broker": raw,
                        "previous_first_positive_fill_at": previous_first_fill,
                        "first_positive_fill_at": first_fill,
                    },
                    at=now,
                    connection=connection,
                )
            return str(row["cycle_id"])

    def _http_error(self, error: httpx.HTTPError, *, operation: str, now: datetime) -> None:
        delay = self._backoff_seconds
        response = getattr(error, "response", None)
        if response is not None and response.headers.get("Retry-After"):
            value = response.headers["Retry-After"]
            try:
                delay = max(delay, int(value))
            except ValueError:
                with suppress(ValueError, TypeError, OverflowError):
                    delay = max(delay, int((parsedate_to_datetime(value) - now).total_seconds()))
        if (
            response is not None
            and response.headers.get("X-Ratelimit-Reset")
            and (
                response.status_code == 429 or response.headers.get("X-Ratelimit-Remaining") == "0"
            )
        ):
            with suppress(ValueError, TypeError, OverflowError):
                reset = datetime.fromtimestamp(float(response.headers["X-Ratelimit-Reset"]), UTC)
                delay = max(delay, int((reset - now).total_seconds()) + 1)
        self._retry_after = now + timedelta(seconds=max(1, delay))
        self._backoff_seconds = min(60, self._backoff_seconds * 2)
        # A broker directive remains relevant after a slow request outlives lease freshness.
        # Only this monotonic cooldown is merged here; no ownership or authority is changed.
        with self.database.begin() as connection:
            raw = connection.scalar(
                select(controls.c.control_value)
                .where(controls.c.control_key == self.account_key)
                .with_for_update()
            )
            state = json.loads(raw) if raw else {}
            previous = (
                stamp(state["retry_after"]) if state.get("retry_after") else self._retry_after
            )
            state["retry_after"] = max(previous, self._retry_after).isoformat()
            connection.execute(
                update(controls)
                .where(controls.c.control_key == self.account_key)
                .values(control_value=canonical(state), updated_at=now)
            )
        self._error = f"{operation}: {type(error).__name__}"
        self.store.audit(
            "broker_backoff",
            {
                "run_id": self.run_id,
                "operation": operation,
                "status_code": response.status_code if response is not None else None,
                "response": response.text[:20000] if response is not None else None,
                "retry_after": self._retry_after.isoformat(),
            },
            at=now,
        )

    def _dispatch(self, client_id: str) -> None:
        with self.database.begin() as connection:
            row = (
                connection.execute(
                    select(scalping_order_links).where(
                        scalping_order_links.c.client_order_id == client_id
                    )
                )
                .mappings()
                .one()
            )
        if row["dispatch_state"] != "intent":
            return
        intent = row["intent"]
        request = OrderRequest.model_validate(intent["request"])
        asset = self._asset(request.symbol)
        if request.side is Side.BUY:
            self._read_positions(force=True)
            self._refresh_cycles(self.clock(), protect_before_buy=request.symbol)
        # This durable transition precedes the external call, including process death.
        with self.database.begin() as connection:
            self._lease(connection, self.clock())
            changed = connection.execute(
                update(scalping_order_links)
                .where(
                    scalping_order_links.c.client_order_id == client_id,
                    scalping_order_links.c.dispatch_state == "intent",
                )
                .values(dispatch_state="dispatching", submission_started_at=self.clock())
            )
            if changed.rowcount != 1:
                return
            connection.execute(
                update(orders)
                .where(orders.c.client_order_id == client_id)
                .values(status="reconciliation_required", updated_at=self.clock())
            )
            self.store.audit(
                "submission_started",
                {"cycle_id": row["cycle_id"], **intent},
                at=self.clock(),
                connection=connection,
            )
        post_invoked = False
        try:
            self._account()
            with self.database.begin() as connection:
                self._lease(connection, self.clock())
                if request.side is Side.BUY:
                    if row["expires_at"] is not None and self.clock() >= utc(row["expires_at"]):
                        self._expire_unsent(connection, client_id)
                        return
                    quote = ScalpQuote.model_validate(intent["quote"])
                    if (
                        canonical_crypto_symbol(quote.symbol) != request.symbol
                        or not self._quote_valid(quote, self.clock())
                        or self._manual_stop()
                    ):
                        self._expire_unsent(connection, client_id)
                        return
                else:
                    self._read_positions(force=True)
                    current_cycle = dict(
                        connection.execute(
                            select(scalping_cycles).where(
                                scalping_cycles.c.cycle_id == row["cycle_id"]
                            )
                        )
                        .mappings()
                        .one()
                    )
                    protected = number(
                        self._account_state().get("unowned_positions", {}).get(request.symbol, 0)
                    )
                    if request.quantity > min(
                        ledger_amount(current_cycle, "owned_quantity"),
                        max(
                            Decimal(0), self._available.get(request.symbol, Decimal(0)) - protected
                        ),
                    ):
                        self._expire_unsent(connection, client_id)
                        return
                if request.side is Side.BUY:
                    funding = self._cash_capacity(connection, exclude_client_id=client_id)
                    required_cash = request.quantity * number(intent["limit_price"])
                    self.store.audit(
                        "cash_funding_fence",
                        {
                            "cycle_id": row["cycle_id"],
                            "client_order_id": client_id,
                            **funding,
                            "required_cash": str(required_cash),
                        },
                        at=self.clock(),
                        connection=connection,
                    )
                    if required_cash > number(funding["available_crypto_cash"]):
                        self._expire_unsent(connection, client_id)
                        return
                else:
                    self._account()
                self._lease(connection, self.clock())
                if request.side is Side.BUY and (
                    not self._quote_valid(ScalpQuote.model_validate(intent["quote"]), self.clock())
                    or self._manual_stop()
                    or (row["expires_at"] is not None and self.clock() >= utc(row["expires_at"]))
                ):
                    self._expire_unsent(connection, client_id)
                    return
                post_invoked = True
                result = (
                    self.broker.submit_crypto_limit_order(
                        request, number(intent["limit_price"]), asset=asset
                    )
                    if request.order_type is OrderType.LIMIT
                    else self.broker.submit_crypto_market_order(request, asset=asset)
                )
            self._update_order(client_id, result, self.clock())
        except httpx.HTTPError as error:
            definitive = (
                isinstance(error, httpx.HTTPStatusError)
                and 400 <= error.response.status_code < 500
                and error.response.status_code != 409
            )
            with self.database.begin() as connection:
                if not post_invoked:
                    self._expire_unsent(connection, client_id)
                else:
                    connection.execute(
                        update(scalping_order_links)
                        .where(scalping_order_links.c.client_order_id == client_id)
                        .values(
                            dispatch_state="unknown",
                            submission_error={
                                "definitive_rejection": definitive,
                                "status_code": error.response.status_code
                                if isinstance(error, httpx.HTTPStatusError)
                                else None,
                            },
                        )
                    )
            # Even an explicit rejection is looked up under the ORIGINAL client ID first.
            found = None
            rate_limited = isinstance(error, httpx.HTTPStatusError) and (
                error.response.status_code == 429
                or error.response.headers.get("Retry-After") is not None
            )
            if post_invoked and not rate_limited:
                with suppress(httpx.HTTPError):
                    found = self.broker.find_order_by_client_id(client_id)
            if found is not None:
                self._update_order(client_id, found, self.clock())
            elif post_invoked and definitive and not rate_limited:
                with self.database.begin() as connection:
                    connection.execute(
                        update(orders)
                        .where(orders.c.client_order_id == client_id)
                        .values(status="rejected", updated_at=self.clock())
                    )
                    connection.execute(
                        update(scalping_order_links)
                        .where(scalping_order_links.c.client_order_id == client_id)
                        .values(dispatch_state="broker_rejected")
                    )
            self._http_error(error, operation="submit", now=self.clock())
        except ValueError:
            if not post_invoked:
                with self.database.begin() as connection:
                    self._expire_unsent(connection, client_id)
            raise

    def _expire_unsent(self, connection: Any, client_id: str) -> None:
        connection.execute(
            update(orders)
            .where(orders.c.client_order_id == client_id)
            .values(status="expired", updated_at=self.clock())
        )
        connection.execute(
            update(scalping_order_links)
            .where(scalping_order_links.c.client_order_id == client_id)
            .values(dispatch_state="expired_unsent")
        )

    def _reserve_order(
        self,
        cycle: dict[str, Any],
        *,
        side: Side,
        quantity: Decimal,
        now: datetime,
        quote: ScalpQuote | None,
        limit_price: Decimal | None,
        signal: ScalpSignal | None = None,
    ) -> str:
        existing = self.store.cycle_orders(cycle["cycle_id"])
        sequence = sum(row["side"] == side.value for row in existing)
        decision_key = (
            sha256(
                f"{self.config.account_digest}:{cycle['symbol']}:{signal.decision_id}".encode()
            ).hexdigest()
            if signal is not None and side is Side.BUY
            else None
        )
        client_id = (
            "ta30-"
            + (
                decision_key
                or sha256(f"{cycle['cycle_id']}:{side.value}:{sequence}".encode()).hexdigest()
            )[:40]
        )
        request = OrderRequest(
            client_order_id=client_id,
            decision_id=sha256(
                (signal.decision_id if signal else cycle["decision_id"]).encode()
            ).hexdigest(),
            strategy_id="v30:" + cycle["run_id"],
            symbol=cycle["symbol"],
            side=side,
            quantity=quantity,
            order_type=OrderType.LIMIT if limit_price is not None else OrderType.MARKET,
            submitted_at=now,
        )
        intent = json_value(
            {
                "request": request.model_dump(mode="json"),
                "limit_price": limit_price,
                "quote": quote.model_dump(mode="json") if quote else None,
                "exit_reason": cycle["payload"].get("exit_reason") if side is Side.SELL else None,
                "known_quote_at_submission": quote is not None,
                "asset": self._asset(cycle["symbol"]).model_dump(mode="json"),
                "asset_observed_at": self._asset_observed_at[cycle["symbol"]].isoformat(),
                "signal": signal.model_dump(mode="json") if signal else None,
                "execution_run_id": self.run_id,
                "execution_code_sha": self.code_sha,
            }
        )
        with self.database.begin() as connection:
            self._lease(connection, now)
            pending = connection.scalar(
                select(orders.c.client_order_id)
                .join(
                    scalping_order_links,
                    scalping_order_links.c.client_order_id == orders.c.client_order_id,
                )
                .join(
                    scalping_cycles, scalping_cycles.c.cycle_id == scalping_order_links.c.cycle_id
                )
                .where(
                    scalping_cycles.c.account_digest == self.config.account_digest,
                    orders.c.symbol == cycle["symbol"],
                    orders.c.status.not_in(FINAL),
                )
                .limit(1)
            )
            if pending is not None:
                raise ValueError("an unresolved owned order already exists for this symbol")
            insert_once(
                connection,
                orders,
                {
                    "order_id": str(uuid5(NAMESPACE_URL, client_id)),
                    "client_order_id": client_id,
                    "broker_order_id": None,
                    "strategy_version": request.strategy_id,
                    "symbol": request.symbol,
                    "side": side.value,
                    "quantity": quantity,
                    "filled_quantity": Decimal(0),
                    "status": "approved",
                    "created_at": now,
                    "updated_at": now,
                },
            )
            insert_once(
                connection,
                scalping_order_links,
                {
                    "client_order_id": client_id,
                    "cycle_id": cycle["cycle_id"],
                    "run_id": cycle["run_id"],
                    "decision_key": decision_key,
                    "dispatch_state": "intent",
                    "intent": intent,
                    "broker": None,
                    "created_at": now,
                    "expires_at": now + timedelta(seconds=self.config.entry_order_ttl_seconds)
                    if side is Side.BUY
                    else None,
                },
            )
            self.store.audit(
                "order_intent",
                {
                    "cycle_id": cycle["cycle_id"],
                    "client_order_id": client_id,
                    **intent,
                },
                at=now,
                connection=connection,
            )
        return client_id

    def _quote_valid(self, quote: ScalpQuote, now: datetime) -> bool:
        return bool(
            quote.bid > 0
            and quote.ask >= quote.bid
            and quote.bid_size > 0
            and quote.ask_size > 0
            and quote.exchange_at <= quote.received_at <= now
            and now - quote.exchange_at <= timedelta(seconds=self.config.feature_horizon_seconds)
        )

    def _new_cycle(self, signal: ScalpSignal, quote: ScalpQuote, now: datetime) -> None:
        if self.run_id is None:
            raise ValueError("engine not initialized")
        symbol = canonical_crypto_symbol(signal.symbol)
        asset = self._asset(symbol)
        marketable = self.config.entry_style == "marketable"
        reference = quote.ask if marketable else quote.bid
        rounding = ROUND_CEILING if marketable else ROUND_FLOOR
        price = (reference / asset.price_increment).to_integral_value(
            rounding=rounding
        ) * asset.price_increment
        with self.database.begin() as connection:
            self._lease(connection, self.clock())
            funding = self._cash_capacity(connection)
        ticket_cash = min(self.config.order_notional_usd, number(funding["available_crypto_cash"]))
        quantity = floor_quantity(ticket_cash / price, asset.min_trade_increment)
        if quantity < asset.min_order_size:
            raise ValueError("cash-funded strategy ticket is below the broker minimum quantity")
        cycle_id = str(uuid5(NAMESPACE_URL, f"v30:{self.run_id}:{signal.decision_id}:{symbol}"))
        payload = {
            "config": self.config.model_dump(mode="json"),
            "signal": signal.model_dump(mode="json"),
            "exit_requested": False,
            "exit_reason": None,
        }
        decision_key = sha256(
            f"{self.config.account_digest}:{symbol}:{signal.decision_id}".encode()
        ).hexdigest()
        cycle: dict[str, Any] | None = None
        with self.database.begin() as connection:
            self._lease(connection, now)
            if (
                connection.scalar(
                    select(scalping_order_links.c.client_order_id).where(
                        scalping_order_links.c.decision_key == decision_key
                    )
                )
                is not None
            ):
                return
            if (
                connection.scalar(
                    select(scalping_cycles.c.cycle_id).where(
                        scalping_cycles.c.account_digest == self.config.account_digest,
                        scalping_cycles.c.symbol == symbol,
                        scalping_cycles.c.decision_id == signal.decision_id,
                    )
                )
                is not None
            ):
                return
            if (
                connection.scalar(
                    select(scalping_cycles.c.cycle_id)
                    .where(scalping_cycles.c.account_digest == self.config.account_digest)
                    .limit(1)
                )
                is None
            ):
                self._read_positions(force=True)
                if self._external_open:
                    return
                raw_state = connection.scalar(
                    select(controls.c.control_value)
                    .where(controls.c.control_key == self.account_key)
                    .with_for_update()
                )
                if raw_state is None:
                    raise ValueError("ownership baseline unavailable")
                ownership = json.loads(raw_state)
                actual_unowned = {key: str(value) for key, value in self._positions.items()}
                if ownership["unowned_positions"] != actual_unowned:
                    previous_unowned = ownership["unowned_positions"]
                    ownership["unowned_positions"] = actual_unowned
                    ownership["unowned_adjusted_at"] = self.clock().isoformat()
                    connection.execute(
                        update(controls)
                        .where(controls.c.control_key == self.account_key)
                        .values(control_value=canonical(ownership), updated_at=self.clock())
                    )
                    self.store.audit(
                        "unowned_before_first_intent",
                        {
                            "run_id": self.run_id,
                            "before": previous_unowned,
                            "after": actual_unowned,
                        },
                        at=self.clock(),
                        connection=connection,
                    )
            ongoing = (
                connection.execute(
                    select(scalping_cycles)
                    .where(
                        scalping_cycles.c.account_digest == self.config.account_digest,
                        scalping_cycles.c.symbol == symbol,
                        scalping_cycles.c.state.not_in(CLOSED),
                        scalping_cycles.c.payload["exit_requested"].as_boolean().is_(False),
                    )
                    .order_by(scalping_cycles.c.created_at)
                    .limit(1)
                )
                .mappings()
                .one_or_none()
            )
            if ongoing is not None:
                cycle = dict(ongoing)
                cycle_id = str(cycle["cycle_id"])
            if cycle is None and not insert_once(
                connection,
                scalping_cycles,
                {
                    "cycle_id": cycle_id,
                    "run_id": self.run_id,
                    "account_digest": self.config.account_digest,
                    "symbol": symbol,
                    "decision_id": signal.decision_id,
                    "state": "entry_pending",
                    "created_at": now,
                    "updated_at": now,
                    "owned_quantity": 0,
                    "entry_quantity": 0,
                    "exit_quantity": 0,
                    "actual_cash_fees": 0,
                    "actual_base_fees": 0,
                    "fees_pending": True,
                    "payload": payload,
                },
            ):
                return
            self.store.audit(
                "decision", {"cycle_id": cycle_id, **payload}, at=now, connection=connection
            )
        cycle = cycle or {
            "cycle_id": cycle_id,
            "run_id": self.run_id,
            "decision_id": signal.decision_id,
            "symbol": symbol,
            "payload": payload,
        }
        client_id = self._reserve_order(
            cycle,
            side=Side.BUY,
            quantity=quantity,
            now=now,
            quote=quote,
            limit_price=price,
            signal=signal,
        )
        self._dispatch(client_id)

    def _cancel_entries(self, now: datetime) -> None:
        for cycle in self._cycles():
            for row in self.store.cycle_orders(cycle["cycle_id"]):
                if row["side"] != "buy" or row["status"] in FINAL:
                    continue
                due = row["expires_at"] is not None and now >= utc(row["expires_at"])
                if not (due or cycle["payload"].get("exit_requested") or self._manual_stop()):
                    continue
                if row["dispatch_state"] == "intent":
                    with self.database.begin() as connection:
                        self._lease(connection, self.clock())
                        self._expire_unsent(connection, row["client_order_id"])
                    continue
                if row["broker_order_id"] is None:
                    continue
                last = row["last_cancel_attempt_at"]
                if last and now - utc(last) < timedelta(
                    seconds=self.config.reconcile_interval_seconds
                ):
                    continue
                self._account()
                with self.database.begin() as connection:
                    self._lease(connection, self.clock())
                    connection.execute(
                        update(scalping_order_links)
                        .where(scalping_order_links.c.client_order_id == row["client_order_id"])
                        .values(
                            cancel_requested_at=row["cancel_requested_at"] or now,
                            last_cancel_attempt_at=now,
                        )
                    )
                    self.store.audit(
                        "cancel_requested",
                        {
                            "cycle_id": cycle["cycle_id"],
                            "client_order_id": row["client_order_id"],
                            "broker_order_id": row["broker_order_id"],
                        },
                        at=now,
                        connection=connection,
                    )
                with self.database.begin() as connection:
                    self._lease(connection, self.clock())
                    self._account()
                    self._lease(connection, self.clock())
                    self.broker.cancel_order(row["broker_order_id"])
                actual = self.broker.find_order_by_client_id(row["client_order_id"])
                if actual:
                    self._update_order(row["client_order_id"], actual, self.clock())

    def _reconcile_orders(self, now: datetime, *, full: bool = False) -> set[str]:
        affected = set()
        for cycle in self._cycles():
            for row in self.store.cycle_orders(cycle["cycle_id"]):
                if row["dispatch_state"] == "intent":
                    # An intent that survived a restart is never a reason to replay an entry.
                    with self.database.begin() as connection:
                        self._lease(connection, self.clock())
                        self._expire_unsent(connection, row["client_order_id"])
                    affected.add(cycle["cycle_id"])
                elif row["status"] not in FINAL or full:
                    last = row["last_reconciled_at"]
                    cancel_due = (
                        row["broker_order_id"] is not None
                        and row["expires_at"] is not None
                        and now >= utc(row["expires_at"])
                    )
                    if (
                        not full
                        and last is not None
                        and row["cancel_requested_at"] is None
                        and not cycle["payload"].get("exit_requested")
                        and not cancel_due
                        and now - utc(last)
                        < timedelta(seconds=self.config.reconcile_interval_seconds)
                    ):
                        continue
                    self._account()
                    actual = self.broker.find_order_by_client_id(row["client_order_id"])
                    if actual is not None:
                        affected.add(
                            self._update_order(row["client_order_id"], actual, self.clock())
                        )
                    else:
                        with self.database.begin() as connection:
                            connection.execute(
                                update(scalping_order_links)
                                .where(
                                    scalping_order_links.c.client_order_id == row["client_order_id"]
                                )
                                .values(last_reconciled_at=self.clock())
                            )
                    if actual is None and (row.get("submission_error") or {}).get(
                        "definitive_rejection"
                    ):
                        with self.database.begin() as connection:
                            connection.execute(
                                update(orders)
                                .where(orders.c.client_order_id == row["client_order_id"])
                                .values(status="rejected", updated_at=self.clock())
                            )
                            connection.execute(
                                update(scalping_order_links)
                                .where(
                                    scalping_order_links.c.client_order_id == row["client_order_id"]
                                )
                                .values(
                                    dispatch_state="broker_rejected",
                                    last_reconciled_at=self.clock(),
                                )
                            )
        return affected

    def _external_inventory_debits(
        self, account_state: dict[str, Any]
    ) -> tuple[dict[str, Decimal], dict[str, Any], dict[str, Decimal], set[str]]:
        protected = {
            symbol: number(quantity)
            for symbol, quantity in account_state.get("unowned_positions", {}).items()
        }
        prior = account_state.get("ownership_foreign_evidence", {})
        evidence: dict[str, Any] = {}
        disposals: dict[str, Decimal] = {}
        acquisition_symbols: set[str] = set()
        with self.database.begin() as connection:
            rows = (
                connection.execute(
                    select(scalping_activities).where(
                        scalping_activities.c.account_digest == self.config.account_digest,
                        scalping_activities.c.occurred_at > stamp(account_state["captured_at"]),
                        scalping_activities.c.broker_order_id.is_not(None),
                        scalping_activities.c.cycle_id.is_(None),
                        ~select(orders.c.client_order_id)
                        .join(
                            scalping_order_links,
                            scalping_order_links.c.client_order_id == orders.c.client_order_id,
                        )
                        .where(orders.c.broker_order_id == scalping_activities.c.broker_order_id)
                        .exists(),
                    )
                )
                .mappings()
                .all()
            )
        totals: dict[str, dict[str, Decimal]] = {}
        legacy: dict[str, dict[str, Decimal]] = {}
        for row in rows:
            raw = row["payload"]
            symbol = row["symbol"]
            if (
                not symbol
                or self._positions_at is None
                or utc(row["occurred_at"]) > self._positions_at
            ):
                continue
            total = totals.setdefault(
                symbol, {"buys": Decimal(0), "sells": Decimal(0), "fees": Decimal(0)}
            )
            old = legacy.setdefault(
                symbol, {"buys": Decimal(0), "sells": Decimal(0), "fees": Decimal(0)}
            )
            quantity = number(raw.get("qty", 0))
            if row["kind"] == "FILL" and row["side"] in {"buy", "sell"} and quantity > 0:
                field = "buys" if row["side"] == "buy" else "sells"
                total[field] += quantity
                key = (
                    "ownership_external_acquisitions"
                    if field == "buys"
                    else "ownership_external_debits"
                )
                old[field] += number(account_state.get(key, {}).get(row["activity_key"], 0))
            elif (
                row["kind"] in {"CFEE", "FEE"}
                and quantity < 0
                and number(raw.get("net_amount", 0)) == 0
                and raw.get("status") == "executed"
                and raw.get("currency") == "USD"
            ):
                total["fees"] -= quantity
                old["fees"] += number(
                    account_state.get("ownership_external_debits", {}).get(row["activity_key"], 0)
                )
        for symbol in set(protected) | set(totals) | set(prior):
            total = totals.get(
                symbol, {"buys": Decimal(0), "sells": Decimal(0), "fees": Decimal(0)}
            )
            previous = prior.get(symbol, legacy.get(symbol, {}))
            old_buys = number(previous.get("buys", 0))
            old_sells = number(previous.get("sells", 0))
            old_fees = number(previous.get("fees", 0))
            old_provisional = number(
                previous.get(
                    "provisional_reduction",
                    max(
                        Decimal(0),
                        number(account_state.get("initial_unowned_positions", {}).get(symbol, 0))
                        + old_buys
                        - old_sells
                        - old_fees
                        - protected.get(symbol, Decimal(0)),
                    ),
                )
            )
            new_buys = max(Decimal(0), total["buys"] - old_buys)
            new_sells = max(Decimal(0), total["sells"] - old_sells)
            new_fees = max(Decimal(0), total["fees"] - old_fees)
            if new_buys:
                acquisition_symbols.add(symbol)
            fee_overlap = min(old_provisional, new_fees)
            available = protected.get(symbol, Decimal(0)) + new_buys
            debit = new_sells + new_fees - fee_overlap
            disposals[symbol] = max(Decimal(0), debit - available)
            protected[symbol] = max(Decimal(0), available - debit)
            evidence[symbol] = {
                "buys": str(max(old_buys, total["buys"])),
                "sells": str(max(old_sells, total["sells"])),
                "fees": str(max(old_fees, total["fees"])),
                "provisional_reduction": str(old_provisional - fee_overlap),
                "new_buys": str(new_buys),
                "unsettled_external_credit": bool(
                    previous.get("unsettled_external_credit")
                    or (new_fees > 0 and old_provisional > fee_overlap)
                ),
            }
        return protected, evidence, disposals, acquisition_symbols

    def _refresh_cycles(
        self,
        now: datetime,
        extra: set[str] | None = None,
        *,
        protect_before_buy: str | None = None,
        _evidence_current: bool = False,
    ) -> None:
        if self._positions_dirty:
            self._read_positions(force=True)
        cycles = {row["cycle_id"]: row for row in self._cycles()}
        if extra:
            with self.database.begin() as connection:
                for stored_cycle in connection.execute(
                    select(scalping_cycles).where(
                        scalping_cycles.c.cycle_id.in_(extra),
                        scalping_cycles.c.account_digest == self.config.account_digest,
                    )
                ).mappings():
                    cycles[stored_cycle["cycle_id"]] = dict(stored_cycle)
        account_state = self._account_state()
        order_rows = {identity: self.store.cycle_orders(identity) for identity in cycles}
        acknowledged: dict[str, tuple[Decimal, Decimal]] = {}
        changed_fills = False
        for identity, cycle in cycles.items():
            bought = sum(
                (
                    number((row["broker"] or {}).get("filled_quantity", 0))
                    for row in order_rows[identity]
                    if row["side"] == "buy"
                ),
                Decimal(0),
            )
            sold = sum(
                (
                    number((row["broker"] or {}).get("filled_quantity", 0))
                    for row in order_rows[identity]
                    if row["side"] == "sell"
                ),
                Decimal(0),
            )
            acknowledged[identity] = (bought, sold)
            changed_fills |= bought != ledger_amount(
                cycle, "entry_quantity"
            ) or sold != ledger_amount(cycle, "exit_quantity")
        previous_positions = account_state.get("ownership_positions", {})
        position_changed = {
            key: str(value) for key, value in self._positions.items()
        } != previous_positions
        evidence_stale = (
            bool(cycles or protect_before_buy is not None)
            and self._positions_at is not None
            and (
                not account_state.get("activity_watermark")
                or stamp(account_state["activity_watermark"]) < self._positions_at
            )
        )
        if (
            not _evidence_current
            and self._positions_at is not None
            and (changed_fills or position_changed or evidence_stale)
        ):
            refreshed_cycles = self._sync_activities(self.clock(), pages=5)
            account_state = self._account_state()
            self._refresh_cycles(
                self.clock(),
                (extra or set()) | refreshed_cycles,
                protect_before_buy=protect_before_buy,
                _evidence_current=True,
            )
            return
        evidence_ready = bool(
            account_state.get("activity_initial_complete")
            and account_state.get("activity_window_until") is None
            and self._positions_at is not None
            and stamp(account_state["activity_watermark"]) >= self._positions_at
        )
        protected, foreign_evidence, disposal_deltas, acquisition_symbols = (
            self._external_inventory_debits(account_state)
        )
        fee_rows: dict[str, list[dict[str, Any]]] = {identity: [] for identity in cycles}
        with self.database.begin() as connection:
            for fee in connection.execute(
                select(scalping_activities).where(
                    scalping_activities.c.cycle_id.in_(cycles),
                    scalping_activities.c.kind.in_(("CFEE", "FEE")),
                )
            ).mappings():
                fee_rows[fee["cycle_id"]].append(dict(fee))
        facts: dict[str, dict[str, Any]] = {}
        carry_totals: dict[str, Decimal] = {}
        upper_totals: dict[str, Decimal] = {}
        pending_symbols: set[str] = set()
        for identity, cycle in sorted(cycles.items(), key=lambda item: item[1]["created_at"]):
            bought, sold = acknowledged[identity]
            previous = cycle["payload"].get("ownership", {})
            previous_bought = ledger_amount(cycle, "entry_quantity")
            previous_sold = ledger_amount(cycle, "exit_quantity")
            previous_safe = ledger_amount(cycle, "owned_quantity")
            previous_claim = number(previous.get("established_entitlement", previous_safe))
            paid = sum(
                (
                    -number(row["payload"].get("qty", 0))
                    for row in fee_rows[identity]
                    if row["attribution_basis"] == "broker_order_id"
                ),
                Decimal(0),
            )
            proven_fee = max(
                paid,
                number(
                    previous.get(
                        "proven_base_fees",
                        cycle["payload"].get("ledger", {}).get("actual_base_fees", 0),
                    )
                ),
            )
            closed = (
                cycle["state"] in CLOSED and bought == previous_bought and sold == previous_sold
            )
            previous_disposal = number(previous.get("proven_external_disposals", 0))
            disposal = (
                min(
                    disposal_deltas.get(cycle["symbol"], Decimal(0)),
                    max(Decimal(0), bought - sold - proven_fee - previous_disposal),
                )
                if not closed
                else Decimal(0)
            )
            disposal_deltas[cycle["symbol"]] = max(
                Decimal(0), disposal_deltas.get(cycle["symbol"], Decimal(0)) - disposal
            )
            total_disposal = previous_disposal + disposal
            upper = max(Decimal(0), bought - sold - proven_fee - total_disposal)
            acknowledged_upper = upper
            recovery_credit = closed and paid > 0 and upper > 0
            carry = min(upper, max(Decimal(0), previous_claim - (sold - previous_sold) - disposal))
            safe_carry = min(
                carry, max(Decimal(0), previous_safe - (sold - previous_sold) - disposal)
            )
            pending_buy = number(previous.get("unresolved_buy_quantity", 0)) + (
                bought - previous_bought
            )
            if previous.get("version") != 2 and bought > 0 and previous_safe == 0 and sold == 0:
                pending_buy = max(pending_buy, bought)
            if closed:
                carry = safe_carry = pending_buy = Decimal(0)
                if not recovery_credit:
                    upper = Decimal(0)
            mixed = previous.get(
                "mixed_flow_pending", previous.get("settlement_state") == "mixed_unresolved"
            ) or (pending_buy > 0 and cycle["symbol"] in acquisition_symbols)
            unacknowledged_buy = sum(
                (
                    max(
                        Decimal(0),
                        number(row["quantity"])
                        - number((row["broker"] or {}).get("filled_quantity", 0)),
                    )
                    for row in order_rows[identity]
                    if row["side"] == "buy"
                    and row["status"] not in FINAL
                    and row["submission_started_at"] is not None
                ),
                Decimal(0),
            )
            facts[identity] = {
                "upper": upper,
                "claim": carry,
                "safe_carry": safe_carry,
                "pending_buy": pending_buy,
                "mixed": mixed,
                "closed": closed,
                "recovery_credit": recovery_credit,
                "acknowledged_upper": acknowledged_upper,
                "proven_fee": proven_fee,
                "disposal": total_disposal,
                "new_buys": bought - previous_bought,
                "new_sells": sold - previous_sold,
                "unacknowledged_buy": unacknowledged_buy,
            }
            carry_totals[cycle["symbol"]] = (
                carry_totals.get(cycle["symbol"], Decimal(0)) + safe_carry
            )
            upper_totals[cycle["symbol"]] = upper_totals.get(cycle["symbol"], Decimal(0)) + upper
            if pending_buy > 0 or mixed or unacknowledged_buy > 0:
                pending_symbols.add(cycle["symbol"])
        if evidence_ready:
            for symbol, projection in foreign_evidence.items():
                if not projection.get("unsettled_external_credit"):
                    continue
                credit = number(projection["provisional_reduction"])
                restored = protected.get(symbol, Decimal(0)) + credit
                available = self._positions.get(symbol, Decimal(0)) - carry_totals.get(
                    symbol, Decimal(0)
                )
                if available >= restored:
                    protected[symbol] = restored
                    projection["provisional_reduction"] = "0"
                    projection["unsettled_external_credit"] = False
            for symbol in acquisition_symbols | (
                {protect_before_buy} if protect_before_buy else set()
            ):
                observed = self._positions.get(symbol, Decimal(0)) - carry_totals.get(
                    symbol, Decimal(0)
                )
                if symbol in pending_symbols:
                    # Pending owned fills forbid adopting surplus as foreign. Established
                    # safe inventory still bounds how much of the balance can be reserved.
                    if observed >= 0 and observed < protected.get(symbol, Decimal(0)):
                        reduction = protected[symbol] - observed
                        protected[symbol] = observed
                        if symbol in foreign_evidence:
                            foreign_evidence[symbol]["provisional_reduction"] = str(
                                number(foreign_evidence[symbol]["provisional_reduction"])
                                + reduction
                            )
                    continue
                previous_foreign = number(account_state.get("unowned_positions", {}).get(symbol, 0))
                if observed >= 0 and (symbol == protect_before_buy or observed > previous_foreign):
                    if upper_totals.get(symbol, Decimal(0)) > carry_totals.get(symbol, Decimal(0)):
                        observed = min(observed, protected.get(symbol, Decimal(0)))
                    reduction = max(Decimal(0), protected.get(symbol, Decimal(0)) - observed)
                    protected[symbol] = observed
                    if symbol in foreign_evidence:
                        foreign_evidence[symbol]["provisional_reduction"] = str(
                            number(foreign_evidence[symbol]["provisional_reduction"]) + reduction
                        )
        joint_settled = {
            symbol: evidence_ready
            and self._positions.get(symbol, Decimal(0)) == protected.get(symbol, Decimal(0)) + upper
            for symbol, upper in upper_totals.items()
        }
        remaining = {
            key: max(Decimal(0), value - protected.get(key, Decimal(0)))
            for key, value in self._positions.items()
        }
        cycle_writes: list[dict[str, Any]] = []
        ownership_audits: list[tuple[str, dict[str, Any]]] = []
        for cycle in sorted(cycles.values(), key=lambda row: row["created_at"]):
            rows = order_rows[cycle["cycle_id"]]
            bought = sold = buy_value = sell_value = Decimal(0)
            priced = True
            fill_times = []
            for row in rows:
                raw = row["broker"] or {}
                qty = number(raw.get("filled_quantity", 0))
                price = raw.get("filled_average_price")
                if qty > 0 and price is None:
                    priced = False
                value = qty * number(price) if price is not None else Decimal(0)
                if row["side"] == "buy":
                    bought += qty
                    buy_value += value
                    if row["first_positive_fill_at"]:
                        fill_times.append(utc(row["first_positive_fill_at"]))
                else:
                    sold += qty
                    sell_value += value
            fees = fee_rows[cycle["cycle_id"]]
            confirmed = [row for row in fees if row["attribution_basis"] == "broker_order_id"]
            inferred = [row for row in fees if row["attribution_basis"] != "broker_order_id"]
            cash_fees = sum(
                (-number(row["payload"].get("net_amount", 0)) for row in confirmed), Decimal(0)
            )
            base_fees = sum(
                (-number(row["payload"].get("qty", 0)) for row in confirmed), Decimal(0)
            )
            inferred_cash = sum(
                (-number(row["payload"].get("net_amount", 0)) for row in inferred), Decimal(0)
            )
            inferred_base = sum(
                (-number(row["payload"].get("qty", 0)) for row in inferred), Decimal(0)
            )
            previous_bought = ledger_amount(cycle, "entry_quantity")
            previous_sold = ledger_amount(cycle, "exit_quantity")
            previous_owned = ledger_amount(cycle, "owned_quantity")
            if bought < previous_bought or sold < previous_sold:
                raise ValueError("owned cumulative fills regressed")
            previous_ownership = cycle["payload"].get("ownership", {})
            previous_reduction = number(previous_ownership.get("cumulative_reduction", 0))
            fact = facts[cycle["cycle_id"]]
            qty_cap = fact["upper"]
            claim = fact["claim"]
            pending_buy = fact["pending_buy"]
            mixed = fact["mixed"]
            already_closed = fact["closed"] and not (
                fact["recovery_credit"] and joint_settled.get(cycle["symbol"], False)
            )
            observed = remaining.get(cycle["symbol"], Decimal(0))
            grow_from_position = (
                bool(previous_ownership.get("balance_settlement_allowed", previous_owned == 0))
                or fact["new_buys"] > 0
            )
            if cycle["symbol"] in acquisition_symbols or fact["new_sells"] > 0 or mixed:
                grow_from_position = False
            known_buy_ids = {
                row["broker_order_id"]
                for row in rows
                if row["side"] == "buy"
                and number((row["broker"] or {}).get("filled_quantity", 0)) > 0
            }
            exact_fee_ids = {row["broker_order_id"] for row in confirmed}
            buy_fee_evidence = bool(known_buy_ids) and known_buy_ids <= exact_fee_ids
            settlement = "settled"
            if already_closed:
                owned = claim = pending_buy = Decimal(0)
            elif not evidence_ready:
                owned = min(fact["safe_carry"], observed)
                settlement = "evidence_pending"
            elif mixed and not joint_settled.get(cycle["symbol"], False):
                owned = min(fact["safe_carry"], observed)
                settlement = "mixed_unresolved"
            elif buy_fee_evidence and joint_settled.get(cycle["symbol"], False):
                claim = qty_cap
                owned = min(claim, observed)
                pending_buy = max(Decimal(0), qty_cap - owned)
                mixed = False
                settlement = "settled" if pending_buy == 0 else "position_pending"
                grow_from_position = False
            elif mixed or (pending_buy > 0 and joint_settled.get(cycle["symbol"], False)):
                claim = qty_cap
                owned = min(claim, observed)
                pending_buy = Decimal(0) if owned == claim else pending_buy
                mixed = False
                settlement = "settled" if owned == claim else "position_pending"
                grow_from_position = False
            elif grow_from_position or pending_buy > 0:
                if observed <= qty_cap:
                    claim = max(claim, observed)
                    if pending_buy > 0 and buy_fee_evidence:
                        claim = qty_cap
                    owned = min(claim, observed)
                    if owned > 0 and (not buy_fee_evidence or owned == claim):
                        pending_buy = Decimal(0)
                    settlement = (
                        "position_pending"
                        if owned < claim or (pending_buy > 0 and owned == 0)
                        else "provisional_net"
                        if owned < qty_cap
                        else "settled"
                    )
                else:
                    owned = min(fact["safe_carry"], observed)
                    if (
                        not mixed
                        and cycle["symbol"] not in acquisition_symbols
                        and fact["unacknowledged_buy"] > 0
                        and observed <= qty_cap + fact["unacknowledged_buy"]
                    ):
                        settlement = "order_position_pending"
                        grow_from_position = True
                    else:
                        settlement = "mixed_unresolved"
                        mixed = True
                        grow_from_position = False
            else:
                owned = min(claim, observed)
                settlement = (
                    "position_pending"
                    if owned < claim
                    else "provisional_net"
                    if owned < qty_cap
                    else "settled"
                )
            unresolved = settlement in {
                "evidence_pending",
                "mixed_unresolved",
                "position_pending",
                "order_position_pending",
            }
            remaining[cycle["symbol"]] = max(
                Decimal(0), remaining.get(cycle["symbol"], Decimal(0)) - owned
            )
            provisional_debit = max(
                Decimal(0),
                bought - sold - claim - fact["proven_fee"] - fact["disposal"] - pending_buy,
            )
            reduction = fact["proven_fee"] + fact["disposal"] + provisional_debit
            ownership = {
                "version": 2,
                "net_entitlement": str(owned),
                "established_entitlement": str(claim),
                "acknowledged_net_upper_bound": str(qty_cap),
                "unresolved_acknowledged_credit": str(
                    max(Decimal(0), fact["acknowledged_upper"] - claim)
                ),
                "cumulative_owned_buys": str(bought),
                "cumulative_owned_sells": str(sold),
                "cumulative_reduction": str(reduction),
                "proven_base_fees": str(fact["proven_fee"]),
                "proven_external_disposals": str(fact["disposal"]),
                "provisional_reduction": str(max(Decimal(0), provisional_debit)),
                "unresolved_settlement_quantity": str(max(Decimal(0), qty_cap - owned)),
                "unresolved_buy_quantity": str(pending_buy),
                "settlement_state": settlement,
                "mixed_flow_pending": bool(mixed),
                "unresolved": unresolved,
                "balance_settlement_allowed": grow_from_position,
                "position_observed_at": self._positions_at.isoformat()
                if self._positions_at
                else None,
            }
            pending = any(row["status"] not in FINAL for row in rows)
            all_final = bool(rows) and not pending
            if not rows or (all_final and bought == 0):
                state = "no_fill"
            elif unresolved:
                state = "ownership_pending"
            elif (
                all_final
                and owned == 0
                and claim == 0
                and sold > 0
                and (
                    already_closed
                    or qty_cap == 0
                    or self._positions.get(cycle["symbol"], Decimal(0))
                    <= number(protected.get(cycle["symbol"], 0))
                )
            ):
                state = "closed_owned_flat"
            elif cycle["payload"].get("exit_requested"):
                state = "exit_pending" if pending else "residual"
            else:
                state = "entry_pending" if pending else "open"
            config = ScalpingConfig.model_validate(cycle["payload"]["config"])
            opening_candidates = [*fill_times]
            if cycle["opened_at"]:
                opening_candidates.append(utc(cycle["opened_at"]))
            opened = min(opening_candidates) if opening_candidates else None
            due_candidate = (
                opened + timedelta(seconds=config.exit_after_seconds)
                if opened and config.exit_after_seconds is not None
                else None
            )
            exit_due = utc(cycle["exit_due_at"]) if cycle["exit_due_at"] else due_candidate
            if exit_due is not None and due_candidate is not None:
                exit_due = min(exit_due, due_candidate)
            base_pending = max(Decimal(0), reduction - base_fees)
            fee_order_ids = {row["broker_order_id"] for row in confirmed}
            buy_ids = {
                row["broker_order_id"]
                for row in rows
                if row["side"] == "buy"
                and number((row["broker"] or {}).get("filled_quantity", 0)) > 0
            }
            sell_ids = {
                row["broker_order_id"]
                for row in rows
                if row["side"] == "sell"
                and number((row["broker"] or {}).get("filled_quantity", 0)) > 0
            }
            buy_fees_known = bool(buy_ids) and buy_ids <= fee_order_ids
            sell_fees_known = sell_ids <= fee_order_ids
            fee_pending = bought > 0 and (
                base_pending > 0 or not buy_fees_known or not sell_fees_known
            )
            inferred_base_corroborated = (
                inferred_base > 0 and reduction == base_fees + inferred_base and not pending
            )
            gross = sell_value - buy_value if priced else None
            expected_cash_fee = sell_value * config.taker_fee_bps / 10000
            cash_reserve = max(Decimal(0), expected_cash_fee - cash_fees, inferred_cash)
            expected_base = bought * max(config.maker_fee_bps, config.taker_fee_bps) / 10000
            base_reserve = (
                Decimal(0)
                if buy_fees_known
                else max(Decimal(0), expected_base - max(reduction, base_fees))
            )
            modeled = (
                (
                    gross
                    - cash_fees
                    - cash_reserve
                    - (base_reserve * buy_value / bought if bought else 0)
                )
                if gross is not None
                else None
            )
            submission_times = [
                utc(row["submission_started_at"])
                for row in rows
                if row["side"] == "buy" and row["submission_started_at"] is not None
            ]
            broker_fill_times = [
                stamp(row["broker"]["filled_at"])
                for row in rows
                if row["side"] == "buy" and (row["broker"] or {}).get("filled_at")
            ]
            fill_latency = (
                (min(broker_fill_times) - min(submission_times)).total_seconds()
                if broker_fill_times and submission_times
                else None
            )
            closure = cycle["payload"].get("closure_proof") if already_closed else None
            if state in CLOSED and closure is None:
                closure = {
                    "verified_at": now.isoformat(),
                    "all_owned_orders_final": all_final,
                    "owned_quantity": "0",
                    "broker_quantity": str(self._positions.get(cycle["symbol"], 0)),
                    "protected_unowned_quantity": str(protected.get(cycle["symbol"], 0)),
                    "position_observed_at": self._positions_at.isoformat()
                    if self._positions_at
                    else None,
                }
            payload = {
                **cycle["payload"],
                "ownership": ownership,
                "inventory_reduction_not_yet_attributed": str(base_pending),
                "actual_base_fee_quantity": str(base_fees),
                "inferred_account_cash_fee_allocation": str(inferred_cash),
                "inferred_account_base_fee_allocation": str(inferred_base),
                "inferred_base_fee_corroborated_by_position": inferred_base_corroborated,
                "base_fee_cash_deduction_repeated": False,
                "pending_cash_fee_reserve": str(cash_reserve),
                "pending_base_fee_reserve_quantity": str(base_reserve),
                "broker_position": str(self._positions.get(cycle["symbol"], 0)),
                "protected_unowned_position": str(protected.get(cycle["symbol"], 0)),
                "broker_account_flat": not any(self._positions.values()),
                "owned_flat_verified_at": closure["verified_at"] if closure else None,
                "closure_proof": closure,
                "timing": {
                    "entry_submission_started_at": min(submission_times).isoformat()
                    if submission_times
                    else None,
                    "broker_entry_filled_at": min(broker_fill_times).isoformat()
                    if broker_fill_times
                    else None,
                    "actual_entry_fill_latency_seconds": fill_latency
                    if fill_latency is not None and fill_latency >= 0
                    else None,
                    "opening_timestamp_basis": (
                        "broker_fill_if_present_else_first_positive_cumulative_receipt"
                    ),
                    "timer_due_at": exit_due.isoformat() if exit_due else None,
                    "timing_guaranteed": False,
                },
                "quantity_rounding_residual": str(owned) if state == "residual" else "0",
                "buy_base_fee_observed": buy_fees_known,
                "fees_observed_through": now.isoformat(),
                "ledger": {
                    "owned_quantity": str(owned),
                    "entry_quantity": str(bought),
                    "entry_value": str(buy_value) if priced else None,
                    "exit_quantity": str(sold),
                    "exit_value": str(sell_value) if priced else None,
                    "actual_base_fees": str(base_fees),
                    "actual_cash_fees": str(cash_fees),
                },
            }
            closed_at = cycle["closed_at"] or now if state in CLOSED else None
            cycle_writes.append(
                {
                    "cycle_id": cycle["cycle_id"],
                    "previous_updated_at": cycle["updated_at"],
                    "values": {
                        "state": state,
                        "updated_at": now,
                        "opened_at": opened,
                        "exit_due_at": exit_due,
                        "closed_at": closed_at,
                        "owned_quantity": owned,
                        "entry_quantity": bought,
                        "entry_value": buy_value if priced else None,
                        "exit_quantity": sold,
                        "exit_value": sell_value if priced else None,
                        "actual_cash_fees": cash_fees,
                        "actual_base_fees": base_fees,
                        "gross_cash_flow": gross if state in CLOSED else None,
                        "actual_net_pnl": gross - cash_fees
                        if state in CLOSED and not fee_pending and gross is not None
                        else None,
                        "modeled_net_pnl": modeled if state in CLOSED else None,
                        "fees_pending": fee_pending,
                        "payload": payload,
                    },
                }
            )
            if cycle["state"] != state:
                ownership_audits.append(
                    (
                        "cycle_state",
                        {
                            "cycle_id": cycle["cycle_id"],
                            "from": cycle["state"],
                            "state": state,
                            "bought": str(bought),
                            "sold": str(sold),
                            "owned": str(owned),
                            "fees_pending": fee_pending,
                            "gross_cash_flow": str(gross) if gross is not None else None,
                        },
                    )
                )
            if (
                previous_owned != owned
                or previous_reduction != reduction
                or "ownership" not in cycle["payload"]
            ):
                ownership_audits.append(
                    (
                        "net_ownership",
                        {
                            "cycle_id": cycle["cycle_id"],
                            "previous_owned": str(previous_owned),
                            "additional_owned_buys": str(bought - previous_bought),
                            "additional_owned_sells": str(sold - previous_sold),
                            "previous_reduction": str(previous_reduction),
                            "ownership": ownership,
                        },
                    )
                )
        updated_protected = {symbol: str(quantity) for symbol, quantity in protected.items()}
        position_snapshot = {symbol: str(quantity) for symbol, quantity in self._positions.items()}
        projection_changed = (
            updated_protected != account_state.get("unowned_positions", {})
            or foreign_evidence != account_state.get("ownership_foreign_evidence", {})
            or position_snapshot != account_state.get("ownership_positions", {})
        )
        with self.database.begin() as connection:
            self._lease(connection, now)
            current_raw = connection.scalar(
                select(controls.c.control_value)
                .where(controls.c.control_key == self.account_key)
                .with_for_update()
            )
            current_state = json.loads(current_raw) if current_raw else {}
            for key in ("unowned_positions", "ownership_foreign_evidence", "ownership_positions"):
                if current_state.get(key, {}) != account_state.get(key, {}):
                    raise ValueError("ownership projection changed during reconciliation")
            for change in cycle_writes:
                changed = connection.execute(
                    update(scalping_cycles)
                    .where(
                        scalping_cycles.c.cycle_id == change["cycle_id"],
                        scalping_cycles.c.updated_at == change["previous_updated_at"],
                    )
                    .values(**change["values"])
                )
                if changed.rowcount != 1:
                    raise ValueError("cycle changed during ownership reconciliation")
            if projection_changed:
                self._save_account_state(
                    {
                        "unowned_positions": updated_protected,
                        "ownership_foreign_evidence": foreign_evidence,
                        "ownership_positions": position_snapshot,
                    },
                    connection=connection,
                )
            for kind, audit_payload in ownership_audits:
                self.store.audit(kind, audit_payload, at=now, connection=connection)
            if updated_protected != account_state.get("unowned_positions", {}):
                self.store.audit(
                    "unowned_inventory_segregated",
                    {
                        "run_id": self.run_id,
                        "before": account_state.get("unowned_positions", {}),
                        "after": updated_protected,
                        "owned_entitlements_increased": False,
                    },
                    at=now,
                    connection=connection,
                )

    def _request_exits(self, signals: tuple[ScalpSignal, ...], now: datetime) -> None:
        sells = {
            canonical_crypto_symbol(signal.symbol)
            for signal in signals
            if signal.action == "sell"
            and self._quote_valid(signal.quote, now)
            and timedelta(0)
            <= now - signal.observed_at
            <= timedelta(seconds=self.config.feature_horizon_seconds)
        }
        manual = self._manual_stop()
        for cycle in self._cycles():
            due = cycle["exit_due_at"] is not None and now >= utc(cycle["exit_due_at"])
            reason = (
                "manual_stop"
                if manual
                else "signal"
                if cycle["symbol"] in sells
                else "time"
                if due
                else None
            )
            if reason and not cycle["payload"].get("exit_requested"):
                with self.database.begin() as connection:
                    payload = {**cycle["payload"], "exit_requested": True, "exit_reason": reason}
                    connection.execute(
                        update(scalping_cycles)
                        .where(scalping_cycles.c.cycle_id == cycle["cycle_id"])
                        .values(payload=payload, updated_at=now)
                    )
                    self.store.audit(
                        "exit_requested",
                        {
                            "cycle_id": cycle["cycle_id"],
                            "reason": reason,
                        },
                        at=now,
                        connection=connection,
                    )

    def _exit_owned(self, quotes: Mapping[str, ScalpQuote], now: datetime) -> None:
        for cycle in self._cycles():
            if not cycle["payload"].get("exit_requested"):
                continue
            if ledger_amount(cycle, "owned_quantity") <= 0:
                continue
            rows = self.store.cycle_orders(cycle["cycle_id"])
            if any(row["status"] not in FINAL for row in rows):
                continue
            asset = self._asset(cycle["symbol"])
            self._account()
            self._read_positions(force=True)
            self._refresh_cycles(self.clock())
            with self.database.begin() as connection:
                cycle = dict(
                    connection.execute(
                        select(scalping_cycles).where(
                            scalping_cycles.c.cycle_id == cycle["cycle_id"]
                        )
                    )
                    .mappings()
                    .one()
                )
            protected = number(
                self._account_state().get("unowned_positions", {}).get(cycle["symbol"], 0)
            )
            account_available = max(
                Decimal(0), self._available.get(cycle["symbol"], Decimal(0)) - protected
            )
            owned = ledger_amount(cycle, "owned_quantity")
            quantity = floor_quantity(min(owned, account_available), asset.min_trade_increment)
            if quantity < asset.min_order_size:
                continue
            quote = quotes.get(cycle["symbol"])
            if quote is not None and (
                canonical_crypto_symbol(quote.symbol) != cycle["symbol"]
                or not self._quote_valid(quote, now)
            ):
                quote = None
            client_id = self._reserve_order(
                cycle,
                side=Side.SELL,
                quantity=quantity,
                now=self.clock(),
                quote=quote,
                limit_price=None,
            )
            self._dispatch(client_id)
            if self._retry_after and self.clock() < self._retry_after:
                return

    def _sync_activities(
        self, now: datetime, *, pages: int = 5, fee_sweep: bool = False
    ) -> set[str]:
        affected: set[str] = set()
        recheck_orders: dict[str, tuple[str, Decimal]] = {}
        prefix = "fee" if fee_sweep else "activity"
        watermark_key = prefix + "_watermark"
        after_key, until_key, token_key = (
            prefix + "_window_after",
            prefix + "_window_until",
            prefix + "_page_token",
        )
        state = self._account_state()
        if state.get(until_key) is None:
            after = stamp(state.get(watermark_key, state["activity_watermark"])) - (
                timedelta(days=2) if fee_sweep else timedelta(minutes=2)
            )
            self._save_account_state(
                {
                    after_key: after.isoformat(),
                    until_key: now.isoformat(),
                    token_key: None,
                }
            )
        for _ in range(pages):
            state = self._account_state()
            page = self.broker.account_activity_page(
                after=stamp(state[after_key]),
                until=stamp(state[until_key]),
                page_token=state.get(token_key),
                page_size=100,
                **({"activity_types": ("CFEE", "FEE")} if fee_sweep else {}),
            )
            with self.database.begin() as connection:
                self._lease(connection, self.clock())
                for raw in page["activities"]:
                    kind = str(raw["activity_type"])
                    timestamp = raw.get("transaction_time") or raw.get("created_at")
                    precision = "timestamp" if timestamp else "date"
                    if timestamp is None and (kind == "FILL" or not raw.get("date")):
                        raise ValueError(
                            "fill timestamps and non-trade activity dates are required"
                        )
                    at = stamp(timestamp or str(raw["date"]) + "T00:00:00+00:00")
                    if at > self.clock():
                        raise ValueError("future broker activity")
                    key = sha256(f"{self.config.account_digest}:{raw['id']}".encode()).hexdigest()
                    body_hash = sha256(canonical(raw).encode()).hexdigest()
                    current = (
                        connection.execute(
                            select(scalping_activities).where(
                                scalping_activities.c.activity_key == key
                            )
                        )
                        .mappings()
                        .one_or_none()
                    )
                    broker_id = raw.get("order_id")
                    fee_order = (
                        connection.execute(
                            select(
                                scalping_order_links.c.cycle_id,
                                scalping_order_links.c.broker,
                                orders.c.side,
                                orders.c.symbol,
                                orders.c.client_order_id,
                            )
                            .join(
                                orders,
                                orders.c.client_order_id == scalping_order_links.c.client_order_id,
                            )
                            .where(orders.c.broker_order_id == broker_id)
                        )
                        .mappings()
                        .one_or_none()
                        if broker_id
                        else None
                    )
                    cycle_id = fee_order["cycle_id"] if fee_order else None
                    basis = "broker_order_id" if cycle_id else None
                    attributed_id = None
                    if (
                        not broker_id
                        and current is not None
                        and current["payload_hash"] == body_hash
                    ):
                        cycle_id = current["cycle_id"]
                        basis = current["attribution_basis"]
                        attributed_id = current["attributed_order_id"]
                    qty, cash = number(raw.get("qty", 0)), number(raw.get("net_amount", 0))
                    if kind in {"CFEE", "FEE"} and (
                        qty > 0
                        or cash > 0
                        or (qty and cash)
                        or raw.get("status") != "executed"
                        or raw.get("currency") != "USD"
                        or "net_amount" not in raw
                        or (
                            qty
                            and (
                                not raw.get("symbol")
                                or (
                                    fee_order
                                    and symbol_key(str(raw["symbol"])) != fee_order["symbol"]
                                )
                            )
                        )
                        or (not qty and not cash and "qty" not in raw)
                    ):
                        cycle_id = None
                        basis = "invalid_fee_evidence"
                    if fee_order and raw.get("side") and raw["side"] != fee_order["side"]:
                        cycle_id = None
                        basis = "invalid_activity_identity"
                    values = {
                        "activity_key": key,
                        "account_digest": self.config.account_digest,
                        "activity_id": str(raw["id"]),
                        "kind": kind,
                        "symbol": symbol_key(str(raw["symbol"])) if raw.get("symbol") else None,
                        "broker_order_id": broker_id,
                        "side": raw.get("side") or (fee_order["side"] if fee_order else None),
                        "occurred_at": at,
                        "time_precision": precision,
                        "effective_date": str(raw.get("date") or at.date()),
                        "cycle_id": cycle_id,
                        "attribution_basis": basis,
                        "attributed_order_id": attributed_id,
                        "quantity": qty,
                        "net_amount": cash,
                        "payload": raw,
                        "payload_hash": body_hash,
                        "received_at": self.clock(),
                    }
                    if current is None:
                        insert_once(connection, scalping_activities, values)
                    elif current["payload_hash"] != body_hash or current["cycle_id"] != cycle_id:
                        connection.execute(
                            update(scalping_activities)
                            .where(scalping_activities.c.activity_key == key)
                            .values(**values)
                        )
                    if current is None or current["payload_hash"] != body_hash:
                        self.store.audit(
                            "broker_activity",
                            {
                                "cycle_id": cycle_id,
                                "activity": raw,
                            },
                            at=self.clock(),
                            connection=connection,
                            identity="v30-activity:" + key + ":" + body_hash,
                        )
                    if cycle_id and (
                        current is None
                        or current["payload_hash"] != body_hash
                        or current["cycle_id"] != cycle_id
                    ):
                        affected.add(str(cycle_id))
                        if kind == "FILL" and fee_order:
                            recheck_orders[fee_order["client_order_id"]] = (
                                str(broker_id),
                                number((fee_order["broker"] or {}).get("filled_quantity", 0)),
                            )
                    if (
                        current
                        and current["cycle_id"]
                        and (
                            current["payload_hash"] != body_hash or current["cycle_id"] != cycle_id
                        )
                    ):
                        affected.add(str(current["cycle_id"]))
            next_token = page.get("next_page_token")
            if not page.get("complete") and (
                next_token is None or next_token == state.get(token_key)
            ):
                raise ValueError("broker activity cursor did not advance")
            if page.get("complete"):
                self._save_account_state(
                    {
                        watermark_key: state[until_key],
                        until_key: None,
                        token_key: None,
                        prefix + "_initial_complete": True,
                        **(
                            {
                                "activity_coverage_started_at": state.get(
                                    "activity_coverage_started_at", state[after_key]
                                )
                            }
                            if not fee_sweep
                            else {}
                        ),
                    }
                )
                break
            self._save_account_state({token_key: next_token})
        for client_id, (broker_id, known_quantity) in recheck_orders.items():
            with self.database.begin() as connection:
                quantity = sum(
                    (
                        number(value)
                        for value in connection.scalars(
                            select(scalping_activities.c.payload["qty"].as_string()).where(
                                scalping_activities.c.account_digest == self.config.account_digest,
                                scalping_activities.c.kind == "FILL",
                                scalping_activities.c.broker_order_id == broker_id,
                            )
                        )
                    ),
                    Decimal(0),
                )
            if quantity > known_quantity:
                actual = self.broker.find_order_by_client_id(client_id)
                if actual is not None:
                    affected.add(self._update_order(client_id, actual, self.clock()))
        refreshed = self._account_state()
        if (
            refreshed.get("activity_initial_complete")
            and refreshed.get("activity_window_until") is None
        ):
            affected |= self._attribute_unkeyed_fees(now)
        return affected

    def _attribute_unkeyed_fees(self, now: datetime) -> set[str]:
        """Keep non-ID attribution explicitly inferred, even when uniquely corroborated."""
        affected: set[str] = set()
        account_state = self._account_state()
        coverage = account_state.get("activity_coverage_started_at")
        if coverage is None:
            return affected
        with self.database.begin() as connection:
            candidates = (
                connection.execute(
                    select(scalping_activities)
                    .where(
                        scalping_activities.c.account_digest == self.config.account_digest,
                        scalping_activities.c.kind == "CFEE",
                        scalping_activities.c.broker_order_id.is_(None),
                        or_(
                            scalping_activities.c.attribution_basis.is_(None),
                            scalping_activities.c.attribution_basis
                            == "unique_complete_activity_window",
                        ),
                    )
                    .order_by(scalping_activities.c.occurred_at)
                    .limit(50)
                )
                .mappings()
                .all()
            )
            for fee in candidates:
                raw = fee["payload"]
                quantity = number(raw.get("qty", 0))
                cash = number(raw.get("net_amount", 0))
                day_start = stamp(fee["effective_date"] + "T00:00:00+00:00")
                side = (
                    "buy"
                    if quantity < 0 and cash == 0
                    else "sell"
                    if cash < 0 and quantity == 0
                    else None
                )
                valid = (
                    side is not None
                    and fee["time_precision"] == "timestamp"
                    and raw.get("status") == "executed"
                    and raw.get("currency") == "USD"
                    and stamp(coverage) <= day_start
                    and utc(fee["occurred_at"]) <= stamp(account_state["activity_watermark"])
                )
                condition = (
                    (scalping_activities.c.account_digest == self.config.account_digest)
                    & (scalping_activities.c.kind == "FILL")
                    & (scalping_activities.c.side == side)
                    & (scalping_activities.c.effective_date == fee["effective_date"])
                    & (scalping_activities.c.occurred_at <= fee["occurred_at"])
                    & scalping_activities.c.broker_order_id.is_not(None)
                )
                condition &= (
                    scalping_activities.c.symbol == fee["symbol"]
                    if side == "buy"
                    else scalping_activities.c.symbol.endswith("/USD")
                )
                ids = (
                    list(
                        connection.scalars(
                            select(scalping_activities.c.broker_order_id)
                            .where(condition)
                            .distinct()
                            .limit(2)
                        )
                    )
                    if valid
                    else []
                )
                matched = (
                    connection.scalar(
                        select(scalping_order_links.c.cycle_id)
                        .join(
                            orders,
                            orders.c.client_order_id == scalping_order_links.c.client_order_id,
                        )
                        .where(orders.c.broker_order_id == ids[0])
                    )
                    if len(ids) == 1
                    else None
                )
                basis = (
                    "unique_complete_activity_window" if matched else "unattributed_or_ambiguous"
                )
                if fee["cycle_id"] != matched or fee["attribution_basis"] != basis:
                    connection.execute(
                        update(scalping_activities)
                        .where(scalping_activities.c.activity_key == fee["activity_key"])
                        .values(
                            cycle_id=matched,
                            attributed_order_id=ids[0] if matched else None,
                            attribution_basis=basis,
                        )
                    )
                    self.store.audit(
                        "fee_attribution",
                        {
                            "cycle_id": matched,
                            "activity_id": fee["activity_id"],
                            "basis": basis,
                            "broker_order_id_supplied": False,
                            "actual_cycle_fee_confirmed": False,
                        },
                        at=now,
                        connection=connection,
                    )
                    if matched:
                        affected.add(str(matched))
                    if fee["cycle_id"]:
                        affected.add(str(fee["cycle_id"]))
        return affected

    def reconcile(self, *, now: datetime) -> None:
        if self.run_id is None:
            raise ValueError("initialize the engine before reconciliation")
        if self._retry_after and self.clock() < self._retry_after:
            return
        try:
            self._account()
            with self.database.begin() as connection:
                self._lease(connection, self.clock())
            affected = self._reconcile_orders(now, full=True)
            self._read_positions(force=True)
            affected |= self._sync_activities(self.clock(), pages=1 if self._cycles() else 5)
            cursor = self._account_state()
            fee_due = cursor.get("next_fee_sweep_at")
            if cursor.get("activity_initial_complete") and (
                fee_due is None or self.clock() >= stamp(fee_due)
            ):
                affected |= self._sync_activities(self.clock(), pages=1, fee_sweep=True)
                self._save_account_state(
                    {"next_fee_sweep_at": (self.clock() + timedelta(minutes=1)).isoformat()}
                )
            self._refresh_cycles(self.clock(), affected)
            self._next_reconcile = self.clock() + timedelta(
                seconds=self.config.reconcile_interval_seconds
            )
        except httpx.HTTPError as error:
            self._http_error(error, operation="reconcile", now=self.clock())
            self._state = "broker_backoff"

    def step(
        self, signals: tuple[ScalpSignal, ...], quotes: Mapping[str, ScalpQuote], *, now: datetime
    ) -> dict[str, Any]:
        if self.run_id is None:
            raise ValueError("initialize the engine before stepping")
        if self._retry_after is not None and self.clock() < self._retry_after:
            return self.status()
        try:
            with self.database.begin() as connection:
                self._lease(connection, self.clock())
            self._reconcile_orders(now)
            self._read_positions()
            self._refresh_cycles(self.clock())
            self._request_exits(signals, self.clock())
            self._cancel_entries(self.clock())
            self._read_positions()
            self._refresh_cycles(self.clock())
            self._exit_owned(quotes, self.clock())
            if self._retry_after and self.clock() < self._retry_after:
                self._state = "broker_backoff"
                return self.status()
            self._read_positions()
            self._refresh_cycles(self.clock())
            if self._next_reconcile is None or self.clock() >= self._next_reconcile:
                self.reconcile(now=self.clock())
                if self._retry_after and self.clock() < self._retry_after:
                    self._state = "broker_backoff"
                    return self.status()
            if (
                self._account_state().get("activity_initial_complete")
                and not self._external_open
                and self._unresolved_orders() == 0
                and self._unresolved_inventory() == 0
                and not self._manual_stop()
            ):
                pending_symbols = self._pending_symbols()
                for signal in signals:
                    symbol = canonical_crypto_symbol(signal.symbol)
                    quote = quotes.get(symbol)
                    if (
                        signal.action != "buy"
                        or symbol not in self.config.symbols
                        or symbol in pending_symbols
                        or quote is None
                        or canonical_crypto_symbol(quote.symbol) != symbol
                        or not self._quote_valid(quote, self.clock())
                        or not timedelta(0)
                        <= self.clock() - signal.observed_at
                        <= timedelta(seconds=self.config.feature_horizon_seconds)
                    ):
                        continue
                    self._new_cycle(signal, quote, self.clock())
                    if self._retry_after and self.clock() < self._retry_after:
                        self._state = "broker_backoff"
                        return self.status()
                    pending_symbols.add(symbol)
            self._read_positions()
            self._refresh_cycles(self.clock())
            self._state = (
                "order_reconciliation"
                if self._unresolved_orders()
                else "ownership_reconciliation"
                if self._unresolved_inventory()
                else "running"
            )
            self._error = None
            self._backoff_seconds = 1
        except httpx.HTTPError as error:
            self._http_error(error, operation="reconcile", now=self.clock())
            self._state = "broker_backoff"
        except (ValueError, KeyError, ArithmeticError) as error:
            self._state, self._error = "technical_recovery", str(error)[:500]
            self.store.audit(
                "technical_recovery",
                {
                    "run_id": self.run_id,
                    "error": self._error,
                },
                at=self.clock(),
            )
        return self.status()

    def status(self) -> dict[str, Any]:
        with self.database.begin() as connection:
            counts = {
                str(row[0]): int(row[1])
                for row in connection.execute(
                    select(scalping_cycles.c.state, func.count())
                    .where(scalping_cycles.c.account_digest == self.config.account_digest)
                    .group_by(scalping_cycles.c.state)
                ).all()
            }
        return {
            "profile": self.config.profile,
            "mode": "paper",
            "run_id": self.run_id,
            "cohort_id": self.config.cohort_id,
            "account_digest": self.config.account_digest,
            "state": self._state,
            "error": self._error,
            "inventory": {
                symbol: item.model_dump(mode="json") for symbol, item in self.inventory().items()
            },
            "cycle_counts": counts,
            "unresolved_order_count": self._unresolved_orders(),
            "unresolved_ownership_count": self._unresolved_inventory(),
            "unowned_open_order_ids": self._external_open,
            "positions_observed_at": self._positions_at.isoformat() if self._positions_at else None,
            "manual_stop_key": self.manual_stop_key,
            "manual_stop": self._manual_stop(),
            "retry_after": self._retry_after.isoformat()
            if self._retry_after and self.clock() < self._retry_after
            else None,
            "legacy_risk_and_approval_gates_apply": False,
            "profitability_validated": False,
        }

    def summary(self, *, since: datetime | None = None) -> dict[str, Any]:
        return self.store.summary(account_digest=self.config.account_digest, since=since)
