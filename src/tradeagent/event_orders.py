from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from hashlib import sha256
from typing import Any, Literal, Protocol
from uuid import NAMESPACE_URL, uuid4, uuid5
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy import insert, select, update

from tradeagent import event_news_policy
from tradeagent.alpaca_paper import (
    AlpacaPaperAccount,
    AlpacaPaperOrder,
    AlpacaPaperPosition,
    PaperAsset,
    PaperClock,
)
from tradeagent.config import AppConfig
from tradeagent.domain import OrderRequest, OrderType, Side
from tradeagent.event_account_risk import account_risk
from tradeagent.event_demo import demo_authorized, terminate_demo
from tradeagent.event_order_notifications import enqueue_lifecycle
from tradeagent.event_performance import allocation_ledgers
from tradeagent.event_research import DEFAULT_EVENT_POLICY, EventQuote
from tradeagent.event_session import (
    empty_session_budget,
    equipment_identity,
    locked_session_budget,
    save_session_budget,
    session_control_key,
    session_identity,
)
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
        *,
        recovery_only: bool = False,
        quote_provider: Callable[[str], EventQuote] | None = None,
    ):
        self.store, self.broker, self.settings = store, broker, settings
        self.app, self.config_hash, self.code_sha = app, config_hash, code_sha
        self.repo = ProductionRepository(store.database)
        self.calendar = NyseSessionCalendar(app.intraday)
        self.owner_id = owner_id
        self.recovery_only = recovery_only
        self.quote_provider = quote_provider
        self._recoveries: dict[str, ExperimentalOrderManager] = {}
        self._unverified_cohorts: set[str] = set()

    def scoped_orders(self) -> list[dict[str, Any]]:
        if self.settings.purpose != "iex-practice" or self.settings.practice_start_date is None:
            return self.store.linked_orders(self.settings.cohort_id)
        digest = sha256(self.broker.account().id.encode()).hexdigest()
        return self.store.session_orders(
            self.settings.cohort_id, digest, self.settings.practice_start_date
        )

    def _recovery_manager(
        self, cohort_id: str, digest: str, now: datetime
    ) -> ExperimentalOrderManager:
        if self.settings.practice_start_date is None:
            raise ValueError("recovery requires a planned session")
        if self.repo.get_control(f"{cohort_id}:broker-account") != digest:
            raise ValueError("prior cohort account ownership is unverified")
        if cohort_id not in self._recoveries:
            with self.store.database.begin() as connection:
                cohort = (
                    connection.execute(
                        select(event_cohorts).where(event_cohorts.c.cohort_id == cohort_id)
                    )
                    .mappings()
                    .one()
                )
            manifest = cohort["manifest"]
            settings = ExperimentalSettings.model_validate(manifest["settings"])
            if (
                settings.cohort_id != cohort_id
                or settings.purpose != "iex-practice"
                or settings.practice_start_date != self.settings.practice_start_date
                or settings.mode != "experimental-paper"
                or manifest["config_hash"] != cohort["config_hash"]
            ):
                raise ValueError("prior cohort recovery scope mismatch")
            frozen_app = AppConfig(
                intraday=manifest["operational_settings"]["intraday"],
                risk=manifest["operational_settings"]["risk"],
            )
            self._recoveries[cohort_id] = ExperimentalOrderManager(
                self.store,
                self.broker,
                settings,
                frozen_app,
                cohort["config_hash"],
                manifest["code_sha"],
                self.owner_id,
                recovery_only=True,
                quote_provider=self.quote_provider,
            )
        manager = self._recoveries[cohort_id]
        if not self.recovery_only:
            handoff = {
                "from_cohort": cohort_id,
                "to_cohort": self.settings.cohort_id,
                "worker_owner": self.owner_id,
                "account_digest": digest,
                "session_id": session_identity(digest, self.settings.practice_start_date),
                "authority": "recovery_only_under_current_global_worker_lease",
                "old_config_hash": manager.config_hash,
                "old_manifest_unchanged": True,
            }
            key = (
                "recovery-handoff:"
                + sha256(json.dumps(handoff, sort_keys=True).encode()).hexdigest()
            )
            if self.repo.get_control(key) is None:
                self.assert_owner(now)
                self.store.audit("recovery_handoff", handoff, now, self.settings.cohort_id)
                self.repo.set_control(key, now.isoformat())
        return manager

    def assert_owner(self, now: datetime) -> None:
        if self.owner_id is not None and not self.repo.refresh_worker_lock(
            "tradeagent-event-worker", self.owner_id, observed_at=now
        ):
            raise EventLeaseLostError("event worker lost lease; broker actions fenced")

    def reconcile(self, now: datetime) -> dict[str, Any]:
        if self.broker.broker_host != PAPER_HOST:
            raise ValueError("live or unknown broker endpoint forbidden")
        account = self.broker.account()
        digest = sha256(account.id.encode()).hexdigest()
        account_key = f"{self.settings.cohort_id}:broker-account"
        recorded_account = self.repo.get_control(account_key)
        if recorded_account is None:
            self.repo.set_control(account_key, digest)
        elif recorded_account != digest:
            self.pause("ACCOUNT_RESET_OR_SWITCH", now)
            raise ValueError("account reset or switch; recovery forbidden")
        positions = self.broker.positions()
        open_orders = self.broker.open_orders()
        rows = self.scoped_orders()
        mismatches: list[str] = []
        self._unverified_cohorts = set()
        for row in rows:
            manager = self
            if self.settings.purpose == "iex-practice" and (
                self.settings.practice_start_date is None
                or row["link"].get("account_digest") != digest
                or row["link"].get("session_id")
                != session_identity(digest, self.settings.practice_start_date)
            ):
                self._unverified_cohorts.add(row["owning_cohort"])
                mismatches.append("RECOVERY_OWNERSHIP_UNVERIFIED:" + row["client_order_id"])
                continue
            if row["owning_cohort"] != self.settings.cohort_id:
                try:
                    manager = self._recovery_manager(row["owning_cohort"], digest, now)
                except (ValueError, KeyError):
                    self._unverified_cohorts.add(row["owning_cohort"])
                    mismatches.append("RECOVERY_OWNERSHIP_UNVERIFIED:" + row["client_order_id"])
                    continue
            if row["status"] in FINAL and (
                not row["broker_order_id"] or _utc(row["created_at"]).date() != now.date()
            ):
                continue
            broker_order = self.broker.find_order_by_client_id(row["client_order_id"])
            if broker_order is not None:
                manager._observe(row["client_order_id"], broker_order, now)
            elif row["status"] not in {"created", "approved"}:
                mismatches.append("SUBMISSION_OUTCOME_UNKNOWN:" + row["client_order_id"])
        rows = self.scoped_orders()
        owned = self.inventory(rows)
        positions = self.broker.positions()
        open_orders = self.broker.open_orders()
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
            "recovery_cohorts": sorted(
                {
                    row["owning_cohort"]
                    for row in rows
                    if row["owning_cohort"] != self.settings.cohort_id
                }
            ),
            "unconfirmed_local_orders": [
                row["client_order_id"] for row in rows if row["status"] not in FINAL
            ],
        }
        self.store.audit("reconciliation", result, now, self.settings.cohort_id)
        if mismatches:
            self.pause("RECONCILIATION_REQUIRED", now)
        else:
            self._notify_completed_cycles(self.store.linked_orders(self.settings.cohort_id), now)
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
            if response.get("filled_average_price") is None:
                self.pause("FILL_PRICE_UNCONFIRMED", now)
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
                                strategy_version=(
                                    "v20-iex-practice-not-qualification"
                                    if self.settings.purpose == "iex-practice"
                                    else "v20-experimental-edge-unproven"
                                ),
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

    def finish_demo(self, reason: str, now: datetime) -> None:
        if self.settings.entry_policy != "equipment-only-demo":
            return
        terminate_demo(self.store, self.settings.cohort_id, reason, now)

    def demo_authorized(self, now: datetime) -> bool:
        return demo_authorized(
            self.repo,
            self.settings,
            self.config_hash,
            self.code_sha,
            sha256(self.broker.account().id.encode()).hexdigest(),
            now,
            self.calendar,
        )

    def news_authorized(self, now: datetime) -> bool:
        return event_news_policy.authorized(
            self.repo,
            self.settings,
            self.config_hash,
            self.code_sha,
            sha256(self.broker.account().id.encode()).hexdigest(),
            now,
            self.calendar,
        )

    def equipment_completed(self) -> bool:
        budget = self.session_budget() or {}
        client_id = budget.get("equipment_client_order_id")
        rows = self.scoped_orders()
        return bool(
            client_id
            and any(
                row["client_order_id"] == client_id
                and row["side"] == "buy"
                and Decimal(str(row["filled_quantity"])) > 0
                for row in rows
            )
            and all(row["status"] in FINAL for row in rows)
            and not self.inventory(rows)
        )

    def session_budget(self) -> dict[str, Any] | None:
        if self.settings.purpose != "iex-practice" or self.settings.practice_start_date is None:
            return None
        digest = self.repo.get_control(f"{self.settings.cohort_id}:broker-account")
        if digest is None:
            digest = sha256(self.broker.account().id.encode()).hexdigest()
        raw = self.repo.get_control(session_control_key(digest, self.settings.practice_start_date))
        return (
            dict(json.loads(raw))
            if raw is not None
            else empty_session_budget(digest, self.settings.practice_start_date)
        )

    def valuation(self, marks: dict[str, Decimal], now: datetime) -> dict[str, Any]:
        report = allocation_ledgers(
            self.store.linked_orders(self.settings.cohort_id),
            marks,
            self.settings.virtual_equity,
            session_date=now.date(),
            purpose=self.settings.purpose,
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
        if self.settings.entry_policy == "news-paper":
            shared = account_risk(self.store, self.settings, marks, now)
            report["account_risk"] = shared
            if shared["blocked"]:
                self.pause("ACCOUNT_ECONOMIC_LOSS_OR_VALUATION_LIMIT", now)
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
        execution_quote: EventQuote | None = None,
        entry_kind: Literal["strategy", "calibration"] = "strategy",
        decision_ticket: dict[str, Any] | None = None,
        maximum_limit_price: Decimal | None = None,
    ) -> dict[str, Any]:
        self.assert_owner(now)
        errors: list[str] = []
        if self.recovery_only:
            errors.append("RECOVERY_ONLY_NO_ENTRIES")
        if self.settings.entry_policy == "equipment-only-demo":
            if entry_kind != "calibration":
                return {"state": "risk_rejected", "reasons": ["DEMO_NO_STRATEGY_ENTRIES"]}
            if self.owner_id is None or not self.demo_authorized(now):
                return {"state": "risk_rejected", "reasons": ["DEMO_AUTHORIZATION_REQUIRED"]}
        if self.settings.entry_policy == "news-paper":
            if self.owner_id is None or not self.news_authorized(now):
                return {"state": "risk_rejected", "reasons": ["NEWS_AUTHORIZATION_REQUIRED"]}
            if entry_kind == "strategy" and (
                not self.equipment_completed()
                or not event_news_policy.valid_ticket(
                    self.store, self.settings.cohort_id, decision_ticket
                )
            ):
                return {"state": "risk_rejected", "reasons": ["EQUIPMENT_AND_VALID_NEWS_REQUIRED"]}
        reconciliation = self.reconcile(now)
        if not reconciliation["healthy"]:
            errors.append("ACCOUNT_SESSION_RECONCILIATION_REQUIRED")
        if self.settings.mode != "experimental-paper":
            errors.append("SHADOW_NO_ORDERS")
        local_date = now.astimezone(ZoneInfo(self.app.intraday.timezone)).date()
        if self.settings.purpose == "iex-practice" and (
            self.settings.practice_start_date is None
            or local_date < self.settings.practice_start_date
        ):
            errors.append("PRACTICE_NOT_STARTED")
        if (
            self.settings.purpose == "iex-practice"
            and self.settings.practice_start_date is not None
            and local_date > self.settings.practice_start_date
        ):
            errors.append("PRACTICE_SESSION_ENDED")
        if (
            self.settings.purpose == "iex-practice"
            and entry_kind == "strategy"
            and (
                decision_ticket is None
                or not decision_ticket.get("evidence_ids")
                or decision_ticket.get("decision", {}).get("action") != "eligible"
                or decision_ticket.get("decision", {}).get("symbol") != symbol
            )
        ):
            errors.append("DECISION_TICKET_REQUIRED")
        if self.broker.broker_host != PAPER_HOST:
            raise ValueError("broker host forbidden")
        gate = self.calendar.gate(now)
        broker_clock = self.broker.clock()
        if not gate.can_enter or not broker_clock.is_open:
            errors.append("MARKET_CLOSED_OR_ENTRY_CUTOFF")
        if abs((now - broker_clock.timestamp).total_seconds()) > 60:
            errors.append("BROKER_CLOCK_STALE")
        if entry_kind == "calibration" and (
            self.settings.purpose != "iex-practice"
            or local_date != self.settings.practice_start_date
            or symbol != "AAPL"
            or cluster_key != f"opening-calibration:{local_date}:AAPL"
            or gate.session_open is None
            or not gate.session_open
            + timedelta(minutes=self.settings.calibration_window_minutes[0])
            <= now
            < gate.session_open + timedelta(minutes=self.settings.calibration_window_minutes[1])
        ):
            errors.append("CALIBRATION_NOT_AUTHORIZED")
        if self.settings.entry_policy == "news-paper" and (
            gate.session_open is None or now < gate.session_open + timedelta(minutes=40)
        ):
            errors.append("NEWS_SESSION_NOT_STARTED")
        if not eligible_at <= now <= expires_at:
            errors.append("EVENT_NOT_EXECUTABLE_NOW")
        if not source_valid:
            errors.append("UNVERIFIED_SOURCE_OR_ISSUER")
        if not timedelta(0) <= now - quote_at <= timedelta(seconds=10):
            errors.append("STALE_QUOTE")
        if execution_quote is None and self.settings.purpose == "iex-practice":
            errors.append("EXECUTION_QUOTE_REQUIRED")
        if execution_quote is not None and (
            execution_quote.symbol != symbol
            or execution_quote.bid != bid
            or execution_quote.ask != ask
            or execution_quote.timestamp != quote_at
            or execution_quote.feed != self.settings.execution_feed
            or execution_quote.size_unit != "shares"
            or execution_quote.bid_size is None
            or execution_quote.ask_size is None
            or execution_quote.bid_size <= 0
            or execution_quote.ask_size <= 0
            or not quote_at <= execution_quote.received_at <= now
            or not timedelta(0) <= now - quote_at <= timedelta(seconds=5)
            or quote_at < eligible_at
        ):
            errors.append("INVALID_EXECUTION_QUOTE")
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
        if maximum_limit_price is not None and limit > maximum_limit_price:
            errors.append("POST_EVENT_CHASE_LIMIT")
        cap = self.settings.effective_notional(self.app)
        quantity = (cap / limit).quantize(Decimal("0.000001"), rounding=ROUND_DOWN)
        if quantity * limit < self.app.intraday.minimum_order_notional or account.cash < cap:
            errors.append("SIZE_OR_CASH_LIMIT")
        if errors:
            result = {
                "state": "risk_rejected",
                "reasons": errors,
                "observed": {
                    "quote_at": quote_at.isoformat(),
                    "evaluated_at": now.isoformat(),
                    "bid": str(bid),
                    "ask": str(ask),
                    "limit_price": str(limit),
                    "maximum_limit_price": str(maximum_limit_price)
                    if maximum_limit_price is not None
                    else None,
                    "median_daily_dollar_volume": str(median_dollar_volume),
                    "notional": str(quantity * limit),
                    "maximum_notional": str(cap),
                    "market_phase": gate.phase.value,
                    "source_valid": source_valid,
                    "certificate_permits_paper": certificate.permits_paper,
                },
            }
            self.store.audit(
                "risk_decision", result, now, self.settings.cohort_id + ":" + decision_id
            )
            self.finish_demo("ENTRY_RISK_REJECTED", now)
            return result

        account_digest = sha256(account.id.encode()).hexdigest()
        claim_key = (
            equipment_identity(account_digest, local_date)
            if entry_kind == "calibration"
            else sha256(f"{self.settings.cohort_id}:{cluster_key}".encode()).hexdigest()
        )
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
            budget = (
                locked_session_budget(connection, account_digest, local_date, now)
                if self.settings.purpose == "iex-practice"
                else None
            )
            if budget is not None:
                if entry_kind == "calibration" and budget["equipment_client_order_id"] is not None:
                    return {
                        "state": "duplicate_event",
                        "client_order_id": budget["equipment_client_order_id"],
                        "equipment_test_id": budget["equipment_test_id"],
                        "owning_cohort": budget["equipment_cohort_id"],
                    }
                if entry_kind == "strategy" and budget["news_entries_reserved"] >= 1:
                    return {
                        "state": "risk_rejected",
                        "reasons": ["NEWS_ENTRY_LIMIT"],
                        "observed": {"news_entries_reserved": budget["news_entries_reserved"]},
                        "limit": 1,
                    }
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
                    .where(
                        event_order_links.c.payload["session_id"].as_string()
                        == budget["session_id"]
                        if budget is not None
                        else event_order_links.c.cohort_id == self.settings.cohort_id
                    )
                ).mappings()
            )
            if (
                any(row["status"] not in FINAL for row in linked)
                or self.inventory([dict(row) for row in linked])
                or self.broker.positions()
                or self.broker.open_orders()
            ):
                return {"state": "risk_rejected", "reasons": ["POSITION_OR_ORDER_RESERVED"]}
            entries_today = (
                budget["total_entries_reserved"]
                if budget is not None
                else sum(
                    row["side"] == "buy" and _utc(row["created_at"]).date() == now.date()
                    for row in linked
                )
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
            if budget is not None:
                budget["total_entries_reserved"] += 1
                if entry_kind == "strategy":
                    budget["news_entries_reserved"] += 1
                else:
                    budget["equipment_client_order_id"] = client_id
                    budget["equipment_cohort_id"] = self.settings.cohort_id
                save_session_budget(connection, budget, now)
            request = OrderRequest(
                client_order_id=client_id,
                decision_id=decision_id,
                strategy_id=(
                    "v20-calibration"
                    if entry_kind == "calibration"
                    else "v20-iex-practice"
                    if self.settings.purpose == "iex-practice"
                    else "v20-event"
                ),
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
                        now
                        + (
                            timedelta(seconds=60)
                            if entry_kind == "calibration"
                            else timedelta(minutes=self.settings.max_holding_minutes)
                        )
                    ).isoformat(),
                    "entry_bid": str(bid),
                    "entry_ask": str(ask),
                    "quote_at": quote_at.isoformat(),
                    "certificate_id": certificate.certificate_id,
                    "synthetic": False,
                    "entry_kind": entry_kind,
                    "purpose": self.settings.purpose,
                    "qualification_eligible": self.settings.purpose != "iex-practice",
                    "trade_classification": (
                        "EQUIPMENT_TEST" if entry_kind == "calibration" else "NEWS_STRATEGY"
                    ),
                    "account_digest": account_digest,
                    "planned_session_date": local_date.isoformat(),
                    "session_id": budget["session_id"] if budget is not None else None,
                    "equipment_test_id": budget["equipment_test_id"]
                    if entry_kind == "calibration" and budget is not None
                    else None,
                    "decision_ticket": decision_ticket,
                    **(
                        {"protection": event_news_policy.protection(limit)}
                        if self.settings.entry_policy == "news-paper"
                        else {}
                    ),
                },
            )
            self.store.audit(
                "decision_ticket",
                {
                    **(decision_ticket or {}),
                    "trade_classification": (
                        "EQUIPMENT_TEST" if entry_kind == "calibration" else "NEWS_STRATEGY"
                    ),
                    "client_order_id": client_id,
                    "request": request.model_dump(mode="json"),
                    "limit_price": str(limit),
                    "entry_notional": str(quantity * limit),
                    "quote": execution_quote.model_dump(mode="json") if execution_quote else None,
                    "risk_approved": True,
                    "session_budget": budget,
                },
                now,
                self.settings.cohort_id,
                connection,
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
        if self.broker.broker_host != PAPER_HOST:
            raise ValueError("broker dispatch requires the paper host")
        self.assert_owner(now)
        existing = self.broker.find_order_by_client_id(request.client_order_id)
        if existing is not None:
            self._observe(request.client_order_id, existing, now)
            return {"state": "recovered", "client_order_id": request.client_order_id}
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
            link = connection.execute(
                select(event_order_links.c.payload).where(
                    event_order_links.c.client_order_id == request.client_order_id
                )
            ).scalar_one()
        self.store.update_link(
            request.client_order_id, {**link, "submission_prepared_at": now.isoformat()}
        )
        self.store.audit(
            "submission_prepared",
            {"client_order_id": request.client_order_id, "side": request.side.value},
            now,
            self.settings.cohort_id,
        )
        digest = sha256(self.broker.account().id.encode()).hexdigest()
        if self.repo.get_control(f"{self.settings.cohort_id}:broker-account") != digest:
            raise ValueError("account changed before dispatch")
        unauthorized_demo = (
            request.side is Side.BUY
            and self.settings.entry_policy == "equipment-only-demo"
            and (
                self.owner_id is None
                or link.get("entry_kind") != "calibration"
                or request.symbol != "AAPL"
                or not demo_authorized(
                    self.repo,
                    self.settings,
                    self.config_hash,
                    self.code_sha,
                    digest,
                    now,
                    self.calendar,
                )
            )
        )
        unauthorized_news = (
            request.side is Side.BUY
            and self.settings.entry_policy == "news-paper"
            and (
                self.owner_id is None
                or link.get("entry_kind") not in {"calibration", "strategy"}
                or (
                    link.get("entry_kind") == "strategy"
                    and not event_news_policy.valid_ticket(
                        self.store, self.settings.cohort_id, link.get("decision_ticket")
                    )
                )
                or not event_news_policy.authorized(
                    self.repo,
                    self.settings,
                    self.config_hash,
                    self.code_sha,
                    digest,
                    now,
                    self.calendar,
                )
            )
        )
        paused = self.repo.get_control("kill_switch") == "active" or self.repo.get_control(
            f"{self.settings.cohort_id}:pause"
        )
        self.assert_owner(now)
        if request.side is Side.BUY:
            clock = self.broker.clock()
            gate = self.calendar.gate(clock.timestamp)
            quote_at = datetime.fromisoformat(link["quote_at"])
            expired_calibration = link.get("entry_kind") == "calibration" and (
                self.settings.purpose != "iex-practice"
                or clock.timestamp.astimezone(ZoneInfo(self.app.intraday.timezone)).date()
                != self.settings.practice_start_date
                or gate.session_open is None
                or not gate.session_open
                + timedelta(minutes=self.settings.calibration_window_minutes[0])
                <= clock.timestamp
                < gate.session_open + timedelta(minutes=self.settings.calibration_window_minutes[1])
            )
            insufficient_news_horizon = link.get("entry_kind") == "strategy" and (
                gate.session_close is None
                or clock.timestamp
                + timedelta(
                    minutes=DEFAULT_EVENT_POLICY.horizon_minutes
                    + DEFAULT_EVENT_POLICY.flatten_before_close_minutes
                )
                > gate.session_close
            )
            if (
                not clock.is_open
                or not gate.can_enter
                or not timedelta(0)
                <= clock.timestamp - request.submitted_at
                <= timedelta(seconds=5)
                or not timedelta(0) <= clock.timestamp - quote_at <= timedelta(seconds=5)
                or clock.timestamp > datetime.fromisoformat(link["expires_at"])
                or expired_calibration
                or unauthorized_demo
                or unauthorized_news
                or (
                    self.settings.entry_policy == "news-paper"
                    and (
                        gate.session_open is None
                        or clock.timestamp < gate.session_open + timedelta(minutes=40)
                        or clock.timestamp.astimezone(ZoneInfo(self.app.intraday.timezone)).date()
                        != self.settings.practice_start_date
                    )
                )
                or insufficient_news_horizon
                or paused
            ):
                self._expire_unsent(
                    request.client_order_id, clock.timestamp, claimed_in_this_dispatch=True
                )
                return {"state": "expired", "reasons": ["SUBMISSION_REVALIDATION_FAILED"]}
            now = clock.timestamp
        elif self.settings.entry_policy == "news-paper":
            clock = self.broker.clock()
            gate = self.calendar.gate(clock.timestamp)
            if (
                not clock.is_open
                or gate.session_open is None
                or gate.session_close is None
                or not gate.session_open <= clock.timestamp < gate.session_close
                or not timedelta(0)
                <= clock.timestamp - request.submitted_at
                <= timedelta(seconds=5)
            ):
                with self.store.database.begin() as connection:
                    connection.execute(
                        update(orders)
                        .where(
                            orders.c.client_order_id == request.client_order_id,
                            orders.c.status == "reconciliation_required",
                        )
                        .values(status="expired", updated_at=clock.timestamp)
                    )
                    self.store.audit(
                        "exit_expired_unsent",
                        {
                            "client_order_id": request.client_order_id,
                            "reason": "regular_session_or_fresh_dispatch_required",
                        },
                        clock.timestamp,
                        self.settings.cohort_id,
                        connection,
                    )
                return {"state": "expired", "reasons": ["EXIT_SUBMISSION_REVALIDATION_FAILED"]}
            now = clock.timestamp
        # No database/control work may occur between these guards and the broker call.
        # The durable prepared UNKNOWN state covers a crash before acknowledgement.
        try:
            try:
                response = (
                    self.broker.submit_limit_order(request, limit)
                    if limit is not None
                    else self.broker.submit_market_order(request)
                )
            finally:
                with self.store.database.begin() as connection:
                    current_link = connection.execute(
                        select(event_order_links.c.payload).where(
                            event_order_links.c.client_order_id == request.client_order_id
                        )
                    ).scalar_one()
                    connection.execute(
                        update(event_order_links)
                        .where(event_order_links.c.client_order_id == request.client_order_id)
                        .values(
                            payload={**current_link, "submission_attempted_at": now.isoformat()}
                        )
                    )
                    self.store.audit(
                        "submission_attempt",
                        {
                            "client_order_id": request.client_order_id,
                            "side": request.side.value,
                            "request": request.model_dump(mode="json"),
                        },
                        now,
                        self.settings.cohort_id,
                        connection,
                    )
        except (httpx.TransportError, httpx.HTTPStatusError) as error:
            if isinstance(error, httpx.HTTPStatusError) and error.response.status_code in {
                400,
                401,
                403,
                404,
                422,
            }:
                error_code = None
                if "application/json" in error.response.headers.get("content-type", ""):
                    try:
                        payload = error.response.json()
                    except ValueError:
                        payload = None
                    if isinstance(payload, dict) and isinstance(payload.get("code"), (str, int)):
                        error_code = payload["code"]
                if error.response.status_code == 422:
                    existing = self.broker.find_order_by_client_id(request.client_order_id)
                    if existing is not None:
                        self._observe(request.client_order_id, existing, now)
                        return {"state": "recovered", "client_order_id": request.client_order_id}
                rejection = {
                    "http_status": error.response.status_code,
                    "provider_code": error_code,
                    "reason": "broker_rejected_submission",
                }
                with self.store.database.begin() as connection:
                    connection.execute(
                        update(orders)
                        .where(orders.c.client_order_id == request.client_order_id)
                        .values(status="rejected", updated_at=now)
                    )
                    current_link = connection.execute(
                        select(event_order_links.c.payload).where(
                            event_order_links.c.client_order_id == request.client_order_id
                        )
                    ).scalar_one()
                    connection.execute(
                        update(event_order_links)
                        .where(event_order_links.c.client_order_id == request.client_order_id)
                        .values(payload={**current_link, "rejection": rejection})
                    )
                    self.store.audit(
                        "broker_rejection",
                        {"client_order_id": request.client_order_id, **rejection},
                        now,
                        self.settings.cohort_id,
                        connection,
                    )
                    if self.settings.entry_policy == "news-paper":
                        enqueue_lifecycle(
                            connection,
                            cohort=self.settings.cohort_id,
                            client_id=request.client_order_id,
                            symbol=request.symbol,
                            side=request.side.value,
                            status="rejected",
                            link={**current_link, "rejection": rejection},
                            now=now,
                        )
                return {
                    "state": "rejected",
                    "client_order_id": request.client_order_id,
                    **rejection,
                }
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

    def _expire_unsent(
        self, client_id: str, now: datetime, *, claimed_in_this_dispatch: bool = False
    ) -> None:
        """Release only proven local expiry; a restarted prepared UNKNOWN is never releasable."""
        with self.store.database.begin() as connection:
            connection.execute(
                select(event_cohorts)
                .where(event_cohorts.c.cohort_id == self.settings.cohort_id)
                .with_for_update()
            ).one()
            link = connection.execute(
                select(event_order_links.c.payload).where(
                    event_order_links.c.client_order_id == client_id
                )
            ).scalar_one()
            budget = (
                locked_session_budget(
                    connection,
                    link["account_digest"],
                    date.fromisoformat(link["planned_session_date"]),
                    now,
                )
                if link.get("session_id")
                else None
            )
            row = (
                connection.execute(
                    select(orders).where(orders.c.client_order_id == client_id).with_for_update()
                )
                .mappings()
                .one()
            )
            if (
                row["status"]
                != ("reconciliation_required" if claimed_in_this_dispatch else "approved")
                or row["broker_order_id"]
                or row["filled_quantity"]
                or link.get("submission_attempted_at")
                or link.get("reservation_released_at")
            ):
                return
            if budget is not None:
                if budget["session_id"] != link["session_id"]:
                    raise ValueError("unsent reservation session identity mismatch")
                if budget["total_entries_reserved"] < 1 or (
                    link["entry_kind"] == "strategy" and budget["news_entries_reserved"] < 1
                ):
                    raise ValueError("unsent reservation counter mismatch")
                budget["total_entries_reserved"] -= 1
                if link["entry_kind"] == "strategy":
                    budget["news_entries_reserved"] -= 1
                save_session_budget(connection, budget, now)
            connection.execute(
                update(orders)
                .where(orders.c.client_order_id == client_id)
                .values(status="expired", updated_at=now)
            )
            connection.execute(
                update(event_order_links)
                .where(event_order_links.c.client_order_id == client_id)
                .values(
                    payload={
                        **link,
                        "definitively_unsent_at": now.isoformat(),
                        "reservation_released_at": now.isoformat() if budget is not None else None,
                    }
                )
            )
            self.store.audit(
                "unsent_reservation_released",
                {
                    "client_order_id": client_id,
                    "session_budget": budget,
                    "claim_and_intent_preserved": True,
                },
                now,
                self.settings.cohort_id,
                connection,
            )

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
                or (
                    current["broker_order_id"] is not None
                    and response.id != current["broker_order_id"]
                )
                or not response.filled_quantity.is_finite()
                or response.filled_quantity < 0
                or response.filled_quantity > Decimal(str(current["quantity"]))
            ):
                raise ValueError("broker state does not reconcile to durable intent")
            link_value = connection.execute(
                select(event_order_links.c.payload).where(
                    event_order_links.c.client_order_id == client_id
                )
            ).scalar_one()
            previous = link_value.get("broker")
            previous_at = (
                datetime.fromisoformat(previous["updated_at"])
                if previous and previous.get("updated_at")
                else None
            )
            incoming_state = lifecycle_state_from_alpaca(response).value
            stale = (
                previous_at is not None
                and response.updated_at is not None
                and response.updated_at < previous_at
            ) or (
                current["status"] in FINAL
                and incoming_state not in FINAL
                and response.filled_quantity <= Decimal(str(current["filled_quantity"]))
            )
            if stale:
                self.store.audit(
                    "broker_update_ignored",
                    {
                        "client_order_id": client_id,
                        "reason": "out_of_order_update",
                        "received": response.model_dump(mode="json"),
                        "retained_status": current["status"],
                    },
                    now,
                    self.settings.cohort_id,
                    connection,
                )
                return
            if response.filled_quantity < Decimal(str(current["filled_quantity"])):
                raise ValueError("unexplained decrease in broker cumulative filled quantity")
            connection.execute(
                update(orders)
                .where(orders.c.client_order_id == client_id)
                .values(
                    broker_order_id=response.id,
                    filled_quantity=response.filled_quantity,
                    status=incoming_state,
                    updated_at=now,
                )
            )
            link = dict(link_value)
            link["broker"] = response.model_dump(mode="json")
            if (
                self.settings.entry_policy == "news-paper"
                and current["side"] == "buy"
                and response.filled_quantity > 0
                and response.filled_average_price is not None
            ):
                link["protection"] = {
                    **event_news_policy.protection(response.filled_average_price),
                    "basis": "actual_broker_cumulative_filled_vwap",
                }
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
            if self.settings.entry_policy == "news-paper":
                enqueue_lifecycle(
                    connection,
                    cohort=self.settings.cohort_id,
                    client_id=client_id,
                    symbol=current["symbol"],
                    side=current["side"],
                    status=incoming_state,
                    link=link,
                    now=now,
                )

    def reconcile_stream_update(self, payload: dict[str, Any], now: datetime) -> None:
        self.assert_owner(now)
        if self.broker.broker_host != PAPER_HOST:
            raise ValueError("broker stream reconciliation requires the paper host")
        order = payload.get("order") or payload
        client_id = order.get("client_order_id")
        self.reconcile(now)
        row = next(
            (row for row in self.scoped_orders() if row["client_order_id"] == client_id), None
        )
        if row is None or row["owning_cohort"] in self._unverified_cohorts:
            self.store.audit(
                "broker_stream_update",
                {**payload, "owned": False, "reconciliation": "account reconciliation required"},
                now,
                self.settings.cohort_id,
            )
            self.reconcile(now)
            return
        actual = self.broker.find_order_by_client_id(str(client_id))
        self.store.audit(
            "broker_stream_update",
            {
                **payload,
                "owned": True,
                "rest_confirmed": actual is not None,
                "rest_order": actual.model_dump(mode="json") if actual else None,
            },
            now,
            self.settings.cohort_id,
        )
        if actual is None:
            self.pause("STREAM_ORDER_NOT_CONFIRMED_BY_REST", now)
        else:
            manager = (
                self
                if row["owning_cohort"] == self.settings.cohort_id
                else self._recoveries[row["owning_cohort"]]
            )
            manager._observe(str(client_id), actual, now)

    def _cancel(self, row: dict[str, Any], now: datetime) -> None:
        self.assert_owner(now)
        try:
            self.broker.cancel_order(row["broker_order_id"])
        except httpx.HTTPStatusError as error:
            if error.response.status_code not in {404, 422}:
                raise
            actual = self.broker.find_order_by_client_id(row["client_order_id"])
            if actual is None or lifecycle_state_from_alpaca(actual).value not in FINAL:
                raise
            self._observe(row["client_order_id"], actual, now)
            self.store.audit(
                "cancellation_race",
                {
                    "client_order_id": row["client_order_id"],
                    "resolved_order": actual.model_dump(mode="json"),
                },
                now,
                self.settings.cohort_id,
            )
            return
        self.store.update_link(
            row["client_order_id"], {**row["link"], "cancel_requested_at": now.isoformat()}
        )
        with self.store.database.begin() as connection:
            connection.execute(
                update(orders)
                .where(orders.c.client_order_id == row["client_order_id"])
                .values(status="cancel_pending", updated_at=now)
            )
        self.store.audit(
            "cancel_requested",
            {
                "client_order_id": row["client_order_id"],
                "broker_order_id": row["broker_order_id"],
                "side": row["side"],
                "filled_quantity_at_request": str(row["filled_quantity"]),
                "cancellation_confirmed": False,
            },
            now,
            self.settings.cohort_id,
        )

    def _protection_triggers(self, now: datetime) -> dict[str, dict[str, Any]]:
        if self.settings.entry_policy != "news-paper":
            return {}
        rows = self.store.linked_orders(self.settings.cohort_id)
        active: dict[str, list[dict[str, Any]]] = {}
        quantities: defaultdict[str, Decimal] = defaultdict(Decimal)
        for row in rows:
            symbol = row["symbol"]
            filled = Decimal(str(row["filled_quantity"]))
            quantities[symbol] += filled * (1 if row["side"] == "buy" else -1)
            if row["side"] == "buy" and filled > 0:
                active.setdefault(symbol, []).append(row)
            if quantities[symbol] == 0:
                active.pop(symbol, None)
        triggered = {}
        for symbol, buys in active.items():
            row = buys[-1]
            prior = row["link"].get("protection_trigger")
            if prior:
                triggered[symbol] = prior
                continue
            reason = None
            quote = None
            plan = row["link"].get("protection") or {}
            try:
                if (
                    self.quote_provider is None
                    or plan.get("basis") != "actual_broker_cumulative_filled_vwap"
                ):
                    raise ValueError("protective quote provider/fill basis missing")
                quote = self.quote_provider(symbol)
                checked_at = self.broker.clock().timestamp
                if (
                    quote.symbol != symbol
                    or quote.feed != self.settings.execution_feed
                    or not quote.timestamp <= quote.received_at <= checked_at
                    or not timedelta(0) <= checked_at - quote.timestamp <= timedelta(seconds=5)
                    or quote.bid <= 0
                    or quote.ask < quote.bid
                ):
                    raise ValueError("protective quote stale/invalid")
                if quote.bid <= Decimal(plan["stop_price"]):
                    reason = "local_stop_loss"
                elif quote.bid >= Decimal(plan["take_profit_price"]):
                    reason = "local_take_profit"
            except (httpx.HTTPError, ValueError, KeyError, ArithmeticError):
                reason = "local_protection_unavailable_risk_exit"
                self.pause("LOCAL_PROTECTION_UNAVAILABLE", now)
            if reason:
                trigger = {
                    "reason": reason,
                    "observed_at": now.isoformat(),
                    "protection": plan,
                    "quote": quote.model_dump(mode="json") if quote else None,
                    "broker_native": False,
                }
                self.store.update_link(
                    row["client_order_id"], {**row["link"], "protection_trigger": trigger}
                )
                self.store.audit("protection_trigger", trigger, now, self.settings.cohort_id)
                triggered[symbol] = trigger
        return triggered

    def supervise(self, now: datetime, *, feed_healthy: bool) -> None:
        """Always called before event ingestion, including outages and operational pauses."""
        self.assert_owner(now)
        self.reconcile(now)
        if self.settings.entry_policy == "equipment-only-demo":
            bounds = (
                self.calendar.session_bounds(self.settings.practice_start_date)
                if self.settings.practice_start_date
                else None
            )
            rows = self.store.linked_orders(self.settings.cohort_id)
            if bounds and now >= bounds[0] + timedelta(
                minutes=self.settings.calibration_window_minutes[1]
            ):
                self.finish_demo("WINDOW_ENDED_RECOVERY_REMAINS_ACTIVE", now)
            elif (
                rows
                and all(row["status"] in FINAL for row in rows)
                and not self.inventory(rows)
                and not self.broker.positions()
                and not self.broker.open_orders()
            ):
                self.finish_demo("ATTEMPT_FINISHED_BROKER_FLAT", now)
        if not self.recovery_only:
            for manager in tuple(self._recoveries.values()):
                if manager.settings.cohort_id not in self._unverified_cohorts:
                    manager.supervise(now, feed_healthy=False)
            self.reconcile(now)
        if self.settings.cohort_id in self._unverified_cohorts:
            return
        if self.settings.entry_policy == "news-paper":
            bounds = (
                self.calendar.session_bounds(self.settings.practice_start_date)
                if self.settings.practice_start_date
                else None
            )
            own = self.store.linked_orders(self.settings.cohort_id)
            news_attempted = any(
                row["side"] == "buy" and row["link"].get("entry_kind") == "strategy" for row in own
            )
            equipment_failed = any(
                row["side"] == "buy"
                and row["link"].get("entry_kind") == "calibration"
                and row["status"] in FINAL
                and not row["filled_quantity"]
                for row in own
            )
            if (
                (bounds and now >= bounds[1])
                or equipment_failed
                or (
                    news_attempted
                    and all(row["status"] in FINAL for row in own)
                    and not self.inventory(own)
                )
                or (
                    bounds
                    and now >= bounds[0] + timedelta(minutes=60)
                    and not (self.session_budget() or {}).get("equipment_client_order_id")
                )
            ):
                event_news_policy.terminate(
                    self.store, self.settings.cohort_id, "SESSION_FINISHED_OR_EQUIPMENT_FAILED", now
                )
        protection_triggers = self._protection_triggers(now)
        rows = self.store.linked_orders(self.settings.cohort_id)
        gate = self.calendar.gate(now)
        for row in rows:
            if row["side"] == "buy" and row["status"] not in FINAL:
                expiry = datetime.fromisoformat(row["link"]["expires_at"])
                paused = self.repo.get_control(f"{self.settings.cohort_id}:pause") or (
                    self.repo.get_control("kill_switch") == "active"
                )
                if (
                    not feed_healthy
                    or now >= expiry
                    or gate.must_flatten
                    or paused
                    or row["symbol"] in protection_triggers
                ):
                    if row["broker_order_id"]:
                        if row["status"] != "cancel_pending":
                            self._cancel(row, now)
                    elif row["status"] == "approved":
                        self._expire_unsent(row["client_order_id"], now)
            elif (
                row["side"] == "sell"
                and row["status"] not in FINAL
                and row["status"] != "cancel_pending"
                and row["broker_order_id"]
                and now >= _utc(row["created_at"]) + timedelta(seconds=30)
            ):
                self._cancel(row, now)
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
                or symbol in protection_triggers
            ):
                continue
            if owned <= 0 or broker_positions.get(symbol) != owned:
                self.pause("EXIT_POSITION_MISMATCH", now)
                continue
            if any(
                row["symbol"] == symbol and row["status"] not in FINAL
                for row in self.scoped_orders()
            ) or any(order.symbol == symbol for order in self.broker.open_orders()):
                continue  # cancellation/unknown state must reconcile before the exit can oversell
            if (
                not self.broker.clock().is_open
                or gate.session_open is None
                or gate.session_close is None
                or not gate.session_open <= now < gate.session_close
            ):
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
                self._reserve(
                    connection,
                    request,
                    sha256(key.encode()).hexdigest(),
                    {
                        "reason": protection_triggers.get(symbol, {}).get(
                            "reason", "time_or_risk_exit"
                        ),
                        "entry_kind": buys[-1]["link"].get("entry_kind", "strategy"),
                        "purpose": self.settings.purpose,
                        "qualification_eligible": self.settings.purpose != "iex-practice",
                        "entry_client_order_id": buys[-1]["client_order_id"],
                        "trade_classification": buys[-1]["link"].get("trade_classification"),
                        "planned_session_date": buys[-1]["link"].get("planned_session_date"),
                        "account_digest": buys[-1]["link"].get("account_digest"),
                        "session_id": buys[-1]["link"].get("session_id"),
                        "equipment_test_id": buys[-1]["link"].get("equipment_test_id"),
                        "decision_ticket": buys[-1]["link"].get("decision_ticket"),
                        "protection": buys[-1]["link"].get("protection"),
                        "exit_decision": {
                            "decided_at": now.isoformat(),
                            "protective_trigger": protection_triggers.get(symbol),
                            "holding_deadline_reached": now >= due,
                            "flatten_required": gate.must_flatten,
                            "feed_unhealthy": not feed_healthy,
                            "cohort_pause": self.repo.get_control(
                                f"{self.settings.cohort_id}:pause"
                            ),
                            "global_kill": self.repo.get_control("kill_switch") == "active",
                            "owned_quantity": str(owned),
                            "broker_confirmed_quantity": str(broker_positions.get(symbol)),
                        },
                    },
                )
            self._dispatch(request, None, now)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
