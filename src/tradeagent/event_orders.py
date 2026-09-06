from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime, timedelta
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from hashlib import sha256
from typing import Any, Protocol
from uuid import NAMESPACE_URL, uuid4, uuid5

import httpx
from sqlalchemy import insert, select, update

from tradeagent.alpaca_paper import (
    AlpacaPaperAccount,
    AlpacaPaperOrder,
    AlpacaPaperPosition,
    PaperAsset,
    PaperClock,
)
from tradeagent.config import AppConfig
from tradeagent.domain import OrderRequest, OrderType, Side
from tradeagent.event_performance import allocation_ledgers
from tradeagent.event_store import (
    EventStore,
    event_cluster_claims,
    event_cohorts,
    event_order_links,
)
from tradeagent.experimental_policy import (
    ExperimentalSettings,
    OperationalCertificate,
    reject_live_environment,
)
from tradeagent.intraday import NyseSessionCalendar
from tradeagent.notifications import RoundTripNotificationRepository
from tradeagent.order_state import lifecycle_state_from_alpaca
from tradeagent.persistence import ProductionRepository, orders, position_cycles

FINAL = {"filled", "canceled", "rejected", "expired", "risk_rejected"}
PAPER_HOST = "https://paper-api.alpaca.markets"


class EventLeaseLostError(RuntimeError):
    pass


class EventBroker(Protocol):
    @property
    def broker_host(self) -> str: ...
    def account(self) -> AlpacaPaperAccount: ...
    def positions(self) -> tuple[AlpacaPaperPosition, ...]: ...
    def open_orders(self) -> tuple[AlpacaPaperOrder, ...]: ...
    def clock(self) -> PaperClock: ...
    def asset(self, symbol: str) -> PaperAsset: ...
    def find_order_by_client_id(self, client_order_id: str) -> AlpacaPaperOrder | None: ...
    def submit_limit_order(self, order: OrderRequest, limit_price: Decimal) -> AlpacaPaperOrder: ...
    def submit_market_order(self, order: OrderRequest) -> AlpacaPaperOrder: ...
    def cancel_order(self, order_id: str) -> None: ...


class ExperimentalOrderManager:
    """One durable exposure intent per event; UNKNOWN submissions never auto-resubmit."""

    def __init__(
        self,
        store: EventStore,
        broker: EventBroker,
        settings: ExperimentalSettings,
        app: AppConfig,
        config_hash: str,
        code_sha: str,
        owner_id: str | None = None,
    ):
        self.store, self.broker, self.settings = store, broker, settings
        self.app, self.config_hash, self.code_sha = app, config_hash, code_sha
        self.repo = ProductionRepository(store.database)
        self.calendar = NyseSessionCalendar(app.intraday)
        self.owner_id = owner_id

    def assert_owner(self, now: datetime) -> None:
        if self.owner_id is not None and not self.repo.refresh_worker_lock(
            "tradeagent-event-worker", self.owner_id, observed_at=now
        ):
            raise EventLeaseLostError("event worker lost lease; broker actions fenced")

    def reconcile(self, now: datetime) -> dict[str, Any]:
        if self.broker.broker_host != PAPER_HOST:
            raise ValueError("live or unknown broker endpoint forbidden")
        account = self.broker.account()
        positions = self.broker.positions()
        open_orders = self.broker.open_orders()
        rows = self.store.linked_orders(self.settings.cohort_id)
        mismatches: list[str] = []
        for row in rows:
            if row["status"] in FINAL:
                continue
            broker_order = self.broker.find_order_by_client_id(row["client_order_id"])
            if broker_order is not None:
                self._observe(row["client_order_id"], broker_order, now)
            elif row["status"] not in {"created", "approved"}:
                mismatches.append("SUBMISSION_OUTCOME_UNKNOWN:" + row["client_order_id"])
        rows = self.store.linked_orders(self.settings.cohort_id)
        owned = self.inventory(rows)
        actual = {position.symbol: position.quantity for position in positions}
        if owned != {symbol: quantity for symbol, quantity in actual.items() if quantity}:
            mismatches.append("BROKER_POSITION_MISMATCH")
        local_ids = {row["client_order_id"] for row in rows}
        if any(order.client_order_id not in local_ids for order in open_orders):
            mismatches.append("UNOWNED_BROKER_ORDER")
        if account.account_blocked or account.trading_blocked or account.status != "ACTIVE":
            mismatches.append("BROKER_ACCOUNT_BLOCKED")
        result = {
            "healthy": not mismatches,
            "mismatches": mismatches,
            "account_digest": sha256(account.id.encode()).hexdigest(),
            "account_suffix": account.id[-4:],
            "positions": {s: str(q) for s, q in actual.items()},
            "open_orders": len(open_orders),
            "observed_at": now.isoformat(),
        }
        account_key = f"{self.settings.cohort_id}:broker-account"
        recorded_account = self.repo.get_control(account_key)
        if recorded_account is None:
            self.repo.set_control(account_key, sha256(account.id.encode()).hexdigest())
        elif recorded_account != result["account_digest"]:
            mismatches.append("ACCOUNT_RESET_OR_SWITCH")
            result["healthy"] = False
        self.store.audit("reconciliation", result, now, self.settings.cohort_id)
        if mismatches:
            self.pause("RECONCILIATION_REQUIRED", now)
        else:
            self._notify_completed_cycles(rows, now)
        return result

    def _notify_completed_cycles(self, rows: list[dict[str, Any]], now: datetime) -> None:
        bought = Decimal(0)
        entry_value = Decimal(0)
        sold = Decimal(0)
        exit_value = Decimal(0)
        first: dict[str, Any] | None = None
        for row in rows:
            quantity = Decimal(str(row["filled_quantity"]))
            response = row["link"].get("broker")
            if quantity <= 0 or not response or row["status"] not in FINAL:
                continue
            value = quantity * Decimal(str(response["filled_average_price"]))
            if row["side"] == "buy":
                first = first or row
                bought += quantity
                entry_value += value
            else:
                sold += quantity
                exit_value += value
            if first is not None and sold == bought:
                cycle_id = uuid5(NAMESPACE_URL, "tradeagent-event:" + first["client_order_id"])
                with self.store.database.begin() as connection:
                    if (
                        connection.scalar(
                            select(position_cycles.c.cycle_id).where(
                                position_cycles.c.cycle_id == str(cycle_id)
                            )
                        )
                        is None
                    ):
                        connection.execute(
                            insert(position_cycles).values(
                                cycle_id=str(cycle_id),
                                strategy_version="v20-experimental-edge-unproven",
                                symbol=first["symbol"],
                                opened_at=_utc(first["created_at"]),
                                opening_quantity=bought,
                                opening_vwap=entry_value / bought,
                                fees=0,
                                status="open",
                            )
                        )
                RoundTripNotificationRepository(self.store.database).close_cycle_and_enqueue(
                    cycle_id,
                    closed_at=now,
                    closing_vwap=exit_value / sold,
                    closing_fees=Decimal(0),
                )
                first = None
                bought = sold = entry_value = exit_value = Decimal(0)

    @staticmethod
    def inventory(rows: list[dict[str, Any]]) -> dict[str, Decimal]:
        held: defaultdict[str, Decimal] = defaultdict(Decimal)
        for row in rows:
            held[row["symbol"]] += Decimal(str(row["filled_quantity"])) * (
                1 if row["side"] == "buy" else -1
            )
        return {symbol: quantity for symbol, quantity in held.items() if quantity != 0}

    def pause(self, reason: str, now: datetime) -> None:
        self.repo.set_control(f"{self.settings.cohort_id}:pause", reason)
        self.store.audit("pause", {"reason": reason}, now, self.settings.cohort_id)

    def valuation(self, marks: dict[str, Decimal], now: datetime) -> dict[str, Any]:
        report = allocation_ledgers(
            self.store.linked_orders(self.settings.cohort_id),
            marks,
            self.settings.virtual_equity,
            session_date=now.date(),
        )
        if report["state"] != "valued":
            self.pause("VALUATION_REQUIRED", now)
            return report
        equity = Decimal(report["economic_paper_equity"])
        day_key = f"{self.settings.cohort_id}:day:{now.date()}"
        day_start_value = self.repo.get_control(day_key)
        if day_start_value is None:
            self.repo.set_control(day_key, str(equity))
        day_start = Decimal(day_start_value) if day_start_value else equity
        peak_key = f"{self.settings.cohort_id}:high-watermark"
        peak = max(
            Decimal(self.repo.get_control(peak_key) or str(self.settings.virtual_equity)), equity
        )
        self.repo.set_control(peak_key, str(peak))
        loss = min(self.settings.daily_loss_fraction, self.app.risk.max_daily_loss)
        drawdown = min(self.settings.drawdown_fraction, self.app.risk.max_drawdown)
        if equity <= day_start * (1 - loss) or equity <= peak * (1 - drawdown):
            self.pause("ECONOMIC_LOSS_LIMIT", now)
        self.store.audit("performance", report, now, self.settings.cohort_id)
        return report

    def submit_entry(
        self,
        *,
        symbol: str,
        cluster_key: str,
        decision_id: str,
        eligible_at: datetime,
        expires_at: datetime,
        bid: Decimal,
        ask: Decimal,
        quote_at: datetime,
        median_dollar_volume: Decimal,
        source_valid: bool,
        certificate: OperationalCertificate,
        now: datetime,
    ) -> dict[str, Any]:
        self.assert_owner(now)
        errors: list[str] = []
        if self.settings.mode != "experimental-paper":
            errors.append("SHADOW_NO_ORDERS")
        if self.broker.broker_host != PAPER_HOST:
            raise ValueError("broker host forbidden")
        gate = self.calendar.gate(now)
        broker_clock = self.broker.clock()
        if not gate.can_enter or not broker_clock.is_open:
            errors.append("MARKET_CLOSED_OR_ENTRY_CUTOFF")
        if abs((now - broker_clock.timestamp).total_seconds()) > 60:
            errors.append("BROKER_CLOCK_STALE")
        if not eligible_at <= now <= expires_at:
            errors.append("EVENT_NOT_EXECUTABLE_NOW")
        if not source_valid:
            errors.append("UNVERIFIED_SOURCE_OR_ISSUER")
        if not timedelta(0) <= now - quote_at <= timedelta(seconds=10):
            errors.append("STALE_QUOTE")
        if bid <= 0 or ask < bid or ask < Decimal(5) or (ask - bid) / ask > Decimal("0.001"):
            errors.append("INVALID_PRICE_OR_SPREAD")
        if median_dollar_volume < Decimal("50000000"):
            errors.append("LIQUIDITY_FLOOR")
        if self.repo.get_control(f"{self.settings.cohort_id}:pause"):
            errors.append("OPERATIONAL_PAUSE")
        if self.repo.get_control("kill_switch") == "active":
            errors.append("GLOBAL_KILL_SWITCH")
        account = self.broker.account()
        if (
            not certificate.permits_paper
            or certificate.config_hash != self.config_hash
            or certificate.code_sha != self.code_sha
            or certificate.cohort_id != self.settings.cohort_id
            or certificate.account_digest != sha256(account.id.encode()).hexdigest()
            or not certificate.issued_at <= now < certificate.expires_at
        ):
            errors.append("OPERATIONAL_CERTIFICATE_REQUIRED")
        if account.trading_blocked or account.account_blocked or account.status != "ACTIVE":
            errors.append("BROKER_BLOCKED")
        ledger = self.valuation({symbol: (bid + ask) / 2}, now)
        if ledger["state"] != "valued" or self.repo.get_control(f"{self.settings.cohort_id}:pause"):
            errors.append("ECONOMIC_RISK_OR_PAUSE")
        asset = self.broker.asset(symbol)
        if not (
            asset.symbol == symbol
            and asset.tradable
            and asset.fractionable
            and asset.status == "active"
            and asset.asset_class == "us_equity"
            and asset.exchange in {"NYSE", "NASDAQ", "AMEX", "ARCA"}
        ):
            errors.append("UNSUPPORTED_ASSET")
        # Hard limit, not a market notional estimate: the broker cannot fill above this ceiling.
        limit = ask.quantize(Decimal("0.01"), rounding=ROUND_UP)
        cap = self.settings.effective_notional(self.app)
        quantity = (cap / limit).quantize(Decimal("0.000001"), rounding=ROUND_DOWN)
        if quantity * limit < self.app.intraday.minimum_order_notional or account.cash < cap:
            errors.append("SIZE_OR_CASH_LIMIT")
        if errors:
            result = {"state": "risk_rejected", "reasons": errors}
            self.store.audit(
                "risk_decision", result, now, self.settings.cohort_id + ":" + decision_id
            )
            return result

        claim_key = sha256(f"{self.settings.cohort_id}:{cluster_key}".encode()).hexdigest()
        client_id = "ta20-" + sha256(f"{claim_key}:buy:1".encode()).hexdigest()[:40]
        with self.store.database.begin() as connection:
            # Serializes all entry reservations on PostgreSQL; reservation survives broker timeouts.
            cohort = (
                connection.execute(
                    select(event_cohorts)
                    .where(event_cohorts.c.cohort_id == self.settings.cohort_id)
                    .with_for_update()
                )
                .mappings()
                .one()
            )
            if cohort["config_hash"] != self.config_hash:
                raise ValueError("cohort configuration mismatch")
            if connection.scalar(
                select(event_cluster_claims.c.claim_key).where(
                    event_cluster_claims.c.claim_key == claim_key
                )
            ):
                return {"state": "duplicate_event", "client_order_id": client_id}
            linked = list(
                connection.execute(
                    select(orders)
                    .join(
                        event_order_links,
                        orders.c.client_order_id == event_order_links.c.client_order_id,
                    )
                    .where(event_order_links.c.cohort_id == self.settings.cohort_id)
                ).mappings()
            )
            if (
                any(row["status"] not in FINAL for row in linked)
                or self.inventory([dict(row) for row in linked])
                or self.broker.positions()
                or self.broker.open_orders()
            ):
                return {"state": "risk_rejected", "reasons": ["POSITION_OR_ORDER_RESERVED"]}
            entries_today = sum(
                row["side"] == "buy" and _utc(row["created_at"]).date() == now.date()
                for row in linked
            )
            if entries_today >= min(
                self.settings.max_entries_per_session, self.app.intraday.maximum_round_trips_per_day
            ):
                return {"state": "risk_rejected", "reasons": ["SESSION_ENTRY_LIMIT"]}
            connection.execute(
                insert(event_cluster_claims).values(
                    claim_key=claim_key,
                    cohort_id=self.settings.cohort_id,
                    created_at=now,
                )
            )
            request = OrderRequest(
                client_order_id=client_id,
                decision_id=decision_id,
                strategy_id="v20-event",
                symbol=symbol,
                side=Side.BUY,
                order_type=OrderType.LIMIT,
                quantity=quantity,
                submitted_at=now,
            )
            self._reserve(
                connection,
                request,
                cluster_key,
                {
                    "limit_price": str(limit),
                    "expires_at": expires_at.isoformat(),
                    "exit_at": (
                        now + timedelta(minutes=self.settings.max_holding_minutes)
                    ).isoformat(),
                    "entry_bid": str(bid),
                    "entry_ask": str(ask),
                    "quote_at": quote_at.isoformat(),
                    "certificate_id": certificate.certificate_id,
                    "synthetic": False,
                },
            )
        return self._dispatch(request, limit, now)

    def _reserve(
        self, connection: Any, request: OrderRequest, cluster: str, link: dict[str, Any]
    ) -> None:
        connection.execute(
            insert(orders).values(
                order_id=str(uuid4()),
                client_order_id=request.client_order_id,
                broker_order_id=None,
                strategy_version=request.strategy_id,
                symbol=request.symbol,
                side=request.side.value,
                quantity=request.quantity,
                filled_quantity=0,
                status="approved",
                created_at=request.submitted_at,
                updated_at=request.submitted_at,
            )
        )
        connection.execute(
            insert(event_order_links).values(
                client_order_id=request.client_order_id,
                cohort_id=self.settings.cohort_id,
                cluster_key=cluster,
                payload={**link, "request": request.model_dump(mode="json")},
            )
        )
        self.store.audit(
            "intent_approved",
            request.model_dump(mode="json"),
            request.submitted_at,
            self.settings.cohort_id,
            connection,
        )

    def _dispatch(
        self, request: OrderRequest, limit: Decimal | None, now: datetime
    ) -> dict[str, Any]:
        reject_live_environment()
        self.assert_owner(now)
        existing = self.broker.find_order_by_client_id(request.client_order_id)
        if existing is not None:
            self._observe(request.client_order_id, existing, now)
            return {"state": "recovered", "client_order_id": request.client_order_id}
        if request.side is Side.BUY:
            clock = self.broker.clock()
            if (
                not clock.is_open
                or not self.calendar.gate(clock.timestamp).can_enter
                or not timedelta(0)
                <= clock.timestamp - request.submitted_at
                <= timedelta(seconds=5)
                or self.repo.get_control("kill_switch") == "active"
                or self.repo.get_control(f"{self.settings.cohort_id}:pause")
            ):
                with self.store.database.begin() as connection:
                    connection.execute(
                        update(orders)
                        .where(
                            orders.c.client_order_id == request.client_order_id,
                            orders.c.status == "approved",
                        )
                        .values(status="expired", updated_at=now)
                    )
                return {"state": "expired", "reasons": ["SUBMISSION_REVALIDATION_FAILED"]}
        with self.store.database.begin() as connection:
            claimed = connection.execute(
                update(orders)
                .where(
                    orders.c.client_order_id == request.client_order_id,
                    orders.c.status == "approved",
                )
                .values(status="reconciliation_required", updated_at=now)
            )
            if not claimed.rowcount:
                return {
                    "state": "submission_outcome_unknown",
                    "client_order_id": request.client_order_id,
                }
        try:
            response = (
                self.broker.submit_limit_order(request, limit)
                if limit is not None
                else self.broker.submit_market_order(request)
            )
        except (httpx.TransportError, httpx.HTTPStatusError) as error:
            self.store.audit(
                "submission_unknown",
                {"client_order_id": request.client_order_id, "error_type": type(error).__name__},
                now,
                self.settings.cohort_id,
            )
            self.pause("SUBMISSION_OUTCOME_UNKNOWN", now)
            return {
                "state": "submission_outcome_unknown",
                "client_order_id": request.client_order_id,
            }
        self._observe(request.client_order_id, response, now)
        return {"state": response.status.value, "client_order_id": request.client_order_id}

    def _observe(self, client_id: str, response: AlpacaPaperOrder, now: datetime) -> None:
        with self.store.database.begin() as connection:
            current = (
                connection.execute(
                    select(orders).where(orders.c.client_order_id == client_id).with_for_update()
                )
                .mappings()
                .one()
            )
            if (
                response.client_order_id != client_id
                or response.symbol != current["symbol"]
                or response.side != current["side"]
                or response.filled_quantity < Decimal(str(current["filled_quantity"]))
                or response.filled_quantity > Decimal(str(current["quantity"]))
            ):
                raise ValueError("broker state does not reconcile to durable intent")
            connection.execute(
                update(orders)
                .where(orders.c.client_order_id == client_id)
                .values(
                    broker_order_id=response.id,
                    filled_quantity=response.filled_quantity,
                    status=lifecycle_state_from_alpaca(response).value,
                    updated_at=now,
                )
            )
            link_value = connection.execute(
                select(event_order_links.c.payload).where(
                    event_order_links.c.client_order_id == client_id
                )
            ).scalar_one()
            link = dict(link_value)
            link["broker"] = response.model_dump(mode="json")
            connection.execute(
                update(event_order_links)
                .where(event_order_links.c.client_order_id == client_id)
                .values(payload=link)
            )
            self.store.audit(
                "broker_order",
                response.model_dump(mode="json"),
                now,
                self.settings.cohort_id + ":" + client_id,
                connection,
            )

    def supervise(self, now: datetime, *, feed_healthy: bool) -> None:
        """Always called before event ingestion, including outages and operational pauses."""
        self.assert_owner(now)
        self.reconcile(now)
        rows = self.store.linked_orders(self.settings.cohort_id)
        gate = self.calendar.gate(now)
        for row in rows:
            if row["side"] == "buy" and row["status"] not in FINAL:
                expiry = datetime.fromisoformat(row["link"]["expires_at"])
                paused = self.repo.get_control(f"{self.settings.cohort_id}:pause") or (
                    self.repo.get_control("kill_switch") == "active"
                )
                if not feed_healthy or now >= expiry or gate.must_flatten or paused:
                    if row["broker_order_id"]:
                        self.broker.cancel_order(row["broker_order_id"])
                        with self.store.database.begin() as connection:
                            connection.execute(
                                update(orders)
                                .where(orders.c.client_order_id == row["client_order_id"])
                                .values(status="cancel_pending", updated_at=now)
                            )
                    elif row["status"] == "approved":
                        with self.store.database.begin() as connection:
                            connection.execute(
                                update(orders)
                                .where(
                                    orders.c.client_order_id == row["client_order_id"],
                                    orders.c.status == "approved",
                                )
                                .values(status="expired", updated_at=now)
                            )
        self.reconcile(now)
        rows = self.store.linked_orders(self.settings.cohort_id)
        inventory = self.inventory(rows)
        broker_positions = {
            position.symbol: position.quantity for position in self.broker.positions()
        }
        for symbol, owned in inventory.items():
            buys: list[dict[str, Any]] = []
            running_quantity = Decimal(0)
            for row in rows:
                if row["symbol"] != symbol:
                    continue
                filled = Decimal(str(row["filled_quantity"]))
                running_quantity += filled * (1 if row["side"] == "buy" else -1)
                if row["side"] == "buy" and filled > 0:
                    buys.append(row)
                if running_quantity == 0:
                    buys.clear()
            due = min(datetime.fromisoformat(row["link"]["exit_at"]) for row in buys)
            if not (
                gate.must_flatten
                or now >= due
                or not feed_healthy
                or self.repo.get_control(f"{self.settings.cohort_id}:pause")
                or self.repo.get_control("kill_switch") == "active"
            ):
                continue
            if owned <= 0 or broker_positions.get(symbol) != owned:
                self.pause("EXIT_POSITION_MISMATCH", now)
                continue
            if any(row["symbol"] == symbol and row["status"] not in FINAL for row in rows):
                continue  # cancellation/unknown state must reconcile before the exit can oversell
            if not self.broker.clock().is_open:
                self.pause("POSITION_REQUIRES_NEXT_OPEN_EXIT", now)
                continue
            sequence = len(
                [row for row in rows if row["symbol"] == symbol and row["side"] == "sell"]
            )
            key = f"{self.settings.cohort_id}:{buys[-1]['client_order_id']}:exit:{sequence}"
            request = OrderRequest(
                client_order_id="ta20-" + sha256(key.encode()).hexdigest()[:40],
                decision_id=sha256(key.encode()).hexdigest()[:24],
                strategy_id="v20-risk-exit",
                symbol=symbol,
                side=Side.SELL,
                quantity=owned,
                submitted_at=now,
            )
            with self.store.database.begin() as connection:
                connection.execute(
                    select(event_cohorts)
                    .where(event_cohorts.c.cohort_id == self.settings.cohort_id)
                    .with_for_update()
                ).one()
                active = connection.scalar(
                    select(orders.c.order_id)
                    .join(
                        event_order_links,
                        orders.c.client_order_id == event_order_links.c.client_order_id,
                    )
                    .where(
                        event_order_links.c.cohort_id == self.settings.cohort_id,
                        orders.c.status.not_in(FINAL),
                    )
                )
                if active:
                    continue
                self._reserve(connection, request, key, {"reason": "time_or_risk_exit"})
            self._dispatch(request, None, now)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
