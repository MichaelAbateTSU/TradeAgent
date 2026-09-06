from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal

import httpx
from pydantic import ValidationError
from sqlalchemy import select

from tradeagent.alpaca import AlpacaDataSettings
from tradeagent.alpaca_paper import AlpacaPaperClient, AlpacaPaperSettings
from tradeagent.config import AppConfig
from tradeagent.event_context import (
    OfficialContextClient,
    OfficialContextSnapshot,
    latest_rest_quote_size_unit,
)
from tradeagent.event_doctor import code_identity
from tradeagent.event_market import EventMarketClient, EventMarketState
from tradeagent.event_orders import EventLeaseLostError, ExperimentalOrderManager
from tradeagent.event_outcomes import record_quote_paths
from tradeagent.event_research import (
    DEFAULT_EVENT_POLICY,
    EventDecision,
    EventQuote,
    ExtractionResult,
    IssuerEligibility,
    MarketContext,
    SourceEvent,
    decide_event,
    extract_event,
    supported_issuer_mappings,
)
from tradeagent.event_sources import EventSourceClient
from tradeagent.event_store import EventStore, event_evidence
from tradeagent.experimental_policy import (
    ExperimentalSettings,
    OperationalCertificate,
    certificate,
    reject_live_environment,
)
from tradeagent.intraday import NyseSessionCalendar
from tradeagent.news import MarketNewsItem, NewsCategory, NewsRepository, SourceReliability
from tradeagent.persistence import Database, ProductionRepository, events


def cohort_manifest(settings: ExperimentalSettings, code_sha: str) -> tuple[str, dict[str, Any]]:
    policy = DEFAULT_EVENT_POLICY.model_dump(mode="json")
    root = Path(__file__).parent
    modules = sorted(
        {
            *root.glob("event_*.py"),
            *(root / name for name in ("experimental_policy.py", "alpaca_paper.py", "domain.py")),
        }
    )
    module_hashes = {
        path.name: sha256(path.read_text(encoding="utf-8").encode()).hexdigest() for path in modules
    }
    digest = settings.fingerprint({"policy": policy, "module_hashes": module_hashes}, code_sha)
    manifest = {
        "policy_change": (
            "User v20 instruction replaces stop-discovery with bounded experimental paper"
        ),
        "code_sha": code_sha,
        "runtime_module_hashes": module_hashes,
        "config_hash": digest,
        "settings": settings.model_dump(mode="json", exclude={"sec_contact_email"}),
        "sec_contact_configured": bool(
            settings.sec_contact_email or os.getenv("NEWS_CONTACT_EMAIL")
        ),
        "policy": policy,
        "prior_research": "failed and/or superseded; never promoted",
        "evidence_clock": "starts with usable prospective decisions on actual trading sessions",
        "review": "after at least 60 trading sessions and 60 reconciled round trips, not per-trade",
        "primary_outcome": (
            "net economic return after omitted costs and exposure-matched comparison"
        ),
        "hypotheses": [
            "H1 comparable quantitative guidance",
            "H2 valued contract",
            "R1 risk overlay",
        ],
        "attempt_budget": {
            "entry_variants": 2,
            "maximum_per_family": 3,
            "ablations": ["price", "structured", "text", "structured_text", "overlay"],
            "controls": ["shuffled_association", "stale_versions", "delays_1_5_15_next_open"],
        },
        "forecast": (
            "expected net return unknown; explicit experimental rule, not a profitability gate"
        ),
        "service_cost_budget": {
            "new_paid_services": 0,
            "inference_calls_per_day": 0,
            "fixed_monthly_usd": None,
        },
        "statistical_gates": {
            "dsr_min": "0.95",
            "family_pbo_max": "0.20",
            "undefined": "inconclusive, never favorable default",
        },
        "source_limitations": [
            "historical receipt not reconstructed",
            "consensus unavailable",
            "IEX is not NBBO",
            "macro and halt completeness checked independently",
        ],
    }
    return digest, manifest


class EventRuntime:
    def __init__(
        self,
        store: EventStore,
        settings: ExperimentalSettings,
        source: EventSourceClient,
        market: EventMarketClient,
        broker: AlpacaPaperClient,
        *,
        instance_id: str,
        code_sha: str,
    ):
        self.store, self.settings = store, settings
        self.source, self.market, self.broker = source, market, broker
        self.repo = ProductionRepository(store.database)
        self.instance_id, self.code_sha = instance_id, code_sha
        self.app = AppConfig()
        self.calendar = NyseSessionCalendar(self.app.intraday)
        self.config_hash, manifest = cohort_manifest(settings, code_sha)
        store.freeze(
            settings.cohort_id, self.config_hash, manifest, settings.mode, datetime.now(UTC)
        )
        self.oms = ExperimentalOrderManager(
            store, broker, settings, self.app, self.config_hash, code_sha, owner_id=instance_id
        )
        self.market_states: dict[str, EventMarketState] = {}
        self.first_bar_receipts: dict[tuple[str, datetime], datetime] = {}
        self.extractions: dict[str, ExtractionResult] = {}
        self.cert: OperationalCertificate | None = None
        self.last_source_success: datetime | None = None
        self.start_at = datetime.now(UTC)
        self.context_client = OfficialContextClient()
        self.context: OfficialContextSnapshot | None = None

    def operational_preflight(self, now: datetime) -> OperationalCertificate:
        reconciliation = self.oms.reconcile(now)
        account = self.broker.account()
        checks = {
            "paper_host": self.broker.broker_host == "https://paper-api.alpaca.markets",
            "no_live_credentials": not any(
                os.getenv(key)
                for key in ("ALPACA_LIVE_KEY", "ALPACA_LIVE_KEY_ID", "ALPACA_LIVE_SECRET_KEY")
            ),
            "account_reconciled": reconciliation["healthy"],
            "cohort_frozen": True,
            "source_connected": self.last_source_success is not None,
            "frozen_policy_market_feed": bool(self.market_states)
            and all(
                state.feed == "sip" or DEFAULT_EVENT_POLICY.allow_iex_experimental_paper
                for state in self.market_states.values()
            ),
            "primary_source_configured": bool(
                self.source.capabilities["sec_enabled"]
                or self.source.capabilities["primary_urls_configured"]
            ),
            "operational_test_attestation": self.repo.get_control("v20:mechanics_attestation")
            == self.code_sha,
            "current_inputs_required_at_each_entry": True,
        }
        cert = certificate(
            self.settings,
            config_hash=self.config_hash,
            code_sha=self.code_sha,
            account_id=account.id,
            checks=checks,
            now=now,
            limitations=(
                "unqualified, experimental only",
                "macro/halt/quote checked per decision",
                "no configured LLM or consensus",
                "no broker mechanics proof from a market closure",
            ),
        )
        self.store.audit(
            "operational_certificate", cert.model_dump(mode="json"), now, self.settings.cohort_id
        )
        self.cert = cert
        return cert

    def tick(self, now: datetime | None = None) -> dict[str, Any]:
        tick_at = now or datetime.now(UTC)
        # Risk/position supervision is independent of all ingestion and extraction failures.
        self.oms.supervise(
            tick_at,
            feed_healthy=(
                self.last_source_success is not None
                and tick_at - self.last_source_success <= timedelta(seconds=120)
            ),
        )
        self.context = self.context_client.poll(symbols=self.settings.symbols.split(","))
        self.store.audit(
            "official_context",
            self.context.model_dump(mode="json"),
            datetime.now(UTC),
            self.settings.cohort_id,
        )
        marks: dict[str, Decimal] = {}
        market_errors: list[str] = []
        pre_receipt_states = dict(self.market_states)
        for symbol in self.settings.symbols.split(","):
            try:
                state = self.market.state(symbol, datetime.now(UTC))
                self.market_states[symbol] = state
                marks[symbol] = (state.bid + state.ask) / 2
                self.repo.store_market_quote(
                    symbol=symbol,
                    event_at=state.quote_at,
                    received_at=state.observed_at,
                    bid_price=state.bid,
                    ask_price=state.ask,
                    bid_size=state.bid_size,
                    ask_size=state.ask_size,
                    feed_source=state.feed,
                    bid_exchange=str(state.raw_quote.get("bx", "")),
                    ask_exchange=str(state.raw_quote.get("ax", "")),
                )
                if state.raw_trade:
                    raw = state.raw_trade
                    self.repo.store_market_trade(
                        provider_trade_id=str(raw["i"]),
                        symbol=symbol,
                        event_at=datetime.fromisoformat(str(raw["t"]).replace("Z", "+00:00")),
                        received_at=state.observed_at,
                        price=Decimal(str(raw["p"])),
                        size=Decimal(str(raw["s"])),
                        feed_source=state.feed,
                        exchange=str(raw.get("x", "")),
                    )
                if state.completed_bar:
                    bar = state.completed_bar
                    self.repo.store_market_bar(
                        symbol=symbol,
                        timeframe="5Min",
                        event_at=bar.timestamp,
                        received_at=state.observed_at,
                        open_price=bar.open,
                        high_price=bar.high,
                        low_price=bar.low,
                        close_price=bar.close,
                        volume=bar.volume,
                        feed_source=state.feed,
                    )
                    self.first_bar_receipts.setdefault(
                        (symbol, state.completed_bar.timestamp), state.observed_at
                    )
            except (httpx.HTTPError, ValueError) as error:
                market_errors.append(f"{symbol}:{type(error).__name__}")
        self.oms.valuation(marks, datetime.now(UTC))
        record_quote_paths(
            self.store, self.settings.cohort_id, self.market_states, datetime.now(UTC)
        )
        watermark_key = f"{self.settings.cohort_id}:source_watermark"
        watermark = self.repo.get_control(watermark_key)
        start = (
            datetime.fromisoformat(watermark)
            if watermark
            else tick_at - timedelta(minutes=self.settings.initial_lookback_minutes)
        )
        source_events = self.source.poll(
            start=start - timedelta(minutes=2),
            end=datetime.now(UTC),
            symbols=self.settings.symbols.split(","),
        )
        for event in source_events:
            self.store.evidence(
                event.evidence_id, event.model_dump(mode="json"), event.first_received_at
            )
            if event.published_at is not None and event.published_at <= event.first_received_at:
                NewsRepository(self.store.database).store(
                    MarketNewsItem(
                        source=event.source,
                        source_url=event.source_url,
                        category=NewsCategory.ISSUER
                        if event.is_primary_source
                        else NewsCategory.GENERAL,
                        symbols=event.related_instruments or event.provider_symbols,
                        headline=event.headline or "Primary issuer document",
                        published_at=event.published_at,
                        received_at=event.first_received_at,
                        updated_at=event.provider_updated_at,
                        reliability=SourceReliability.OFFICIAL
                        if event.is_primary_source
                        else SourceReliability.LICENSED,
                    )
                )
            trace = f"{self.settings.cohort_id}:{event.evidence_id}:pre_context"
            with self.store.database.begin() as connection:
                exists = connection.scalar(
                    select(events.c.event_id).where(events.c.trace_id == trace)
                )
            if exists is None:
                valid_pre = {
                    symbol: value.model_dump(mode="json")
                    for symbol, value in pre_receipt_states.items()
                    if event.published_at is not None and value.observed_at < event.published_at
                }
                self.store.audit("pre_context", valid_pre, datetime.now(UTC), trace)
        if not self.source.last_errors:
            self.last_source_success = datetime.now(UTC)
            self.repo.set_control(watermark_key, tick_at.isoformat())
            self.repo.heartbeat(
                "tradeagent-news-worker",
                self.instance_id,
                {"state": "healthy", "event_runtime": True},
                observed_at=self.last_source_success,
            )
        else:
            self.store.audit(
                "source_error",
                {"errors": self.source.last_errors},
                tick_at,
                self.settings.cohort_id,
            )
        pending = self.store.pending_evidence(self.settings.cohort_id)
        for row in pending:
            event = SourceEvent.model_validate(row["payload"])
            evaluated_at = datetime.now(UTC)
            try:
                extraction = self._extraction(event, evaluated_at)
                decision = self._decision(event, extraction, evaluated_at, pre_receipt_states)
                if decision.position_review_required and event.is_primary_source:
                    self.oms.pause("R1_EVENT_REQUIRES_POSITION_REVIEW", evaluated_at)
                # Keep waitable, semantically valid events alive, without rewriting past decisions.
                waitable = {
                    "completed_post_availability_observation_missing",
                    "processing_latency_not_elapsed",
                    "quote_not_causal_or_fresh",
                    "outside_verified_regular_session",
                }
                waiting = (
                    decision.action == "abstain"
                    and set(decision.reasons).issubset(waitable)
                    and evaluated_at - event.first_received_at < timedelta(minutes=30)
                )
                self.store.audit(
                    "decision_observation",
                    decision.model_dump(mode="json"),
                    evaluated_at,
                    self.settings.cohort_id + ":" + event.evidence_id,
                )
                if waiting:
                    continue
                self.store.decision(
                    self.settings.cohort_id,
                    event.evidence_id,
                    {
                        **decision.model_dump(mode="json"),
                        "extraction": extraction.model_dump(mode="json"),
                        "source_url": event.source_url,
                        "headline": event.headline,
                        "official_context": self.context.model_dump(mode="json"),
                    },
                    evaluated_at,
                )
                if decision.action == "eligible" and self.settings.mode == "experimental-paper":
                    if self.cert is None or self.cert.expires_at <= evaluated_at:
                        self.operational_preflight(evaluated_at)
                    self._entry(decision, event, evaluated_at)
            except (ValidationError, ArithmeticError) as error:
                self.store.decision(
                    self.settings.cohort_id,
                    event.evidence_id,
                    {
                        "action": "abstain",
                        "reasons": ["EXTRACTION_DEAD_LETTER"],
                        "error_type": type(error).__name__,
                        "source_url": event.source_url,
                    },
                    evaluated_at,
                )
        completed_at = datetime.now(UTC)
        clock = self.broker.clock()
        heartbeat_state = {
            "state": "market_closed" if not clock.is_open else "collecting",
            "mode": self.settings.mode,
            "cohort_id": self.settings.cohort_id,
            "code_sha": self.code_sha,
            "config_hash": self.config_hash,
            "market_phase": self.calendar.gate(completed_at).phase.value,
            "next_open": clock.next_open.isoformat(),
            "source_capabilities": self.source.capabilities,
            "last_successful_source_at": (
                self.last_source_success.isoformat() if self.last_source_success else None
            ),
            "events_received": len(source_events),
            "market_errors": market_errors,
            "tick_latency_seconds": (completed_at - tick_at).total_seconds(),
            "blockers": [
                "NO_CONFIGURED_INFERENCE_PROVIDER_DETERMINISTIC_ONLY",
                *self.context.errors,
                "IEX_SHADOW_ONLY_FROZEN_POLICY",
            ],
            "official_context": self.context_client.capabilities,
            "operational_certificate": self.cert.model_dump(mode="json") if self.cert else None,
            "edge_established": False,
        }
        self.repo.heartbeat(
            "tradeagent-event-worker", self.instance_id, heartbeat_state, observed_at=completed_at
        )
        self.oms.assert_owner(completed_at)
        return heartbeat_state

    def _extraction(self, event: SourceEvent, now: datetime) -> ExtractionResult:
        if event.evidence_id not in self.extractions:
            trace = f"{self.settings.cohort_id}:{event.evidence_id}:extraction"
            with self.store.database.begin() as connection:
                prior = connection.execute(
                    select(events.c.payload)
                    .where(events.c.event_type == "event_extraction", events.c.trace_id == trace)
                    .order_by(events.c.recorded_at)
                    .limit(1)
                ).scalar_one_or_none()
            if prior:
                extraction = ExtractionResult.model_validate(prior)
            else:
                extraction = extract_event(event, now=now)
                self.store.audit("extraction", extraction.model_dump(mode="json"), now, trace)
            self.extractions[event.evidence_id] = extraction
        return self.extractions[event.evidence_id]

    def _decision(
        self,
        event: SourceEvent,
        extraction: ExtractionResult,
        now: datetime,
        pre_states: dict[str, EventMarketState],
    ) -> EventDecision:
        mapping = next(
            (m for m in supported_issuer_mappings() if m.issuer_id == event.issuer_id), None
        )
        mode: Literal["experimental_paper", "shadow"] = (
            "experimental_paper" if self.settings.mode == "experimental-paper" else "shadow"
        )
        if mapping is None or mapping.symbol not in self.market_states:
            return decide_event(
                extraction,
                evidence=(event,),
                now=now,
                quote=None,
                market=MarketContext(mode=mode),
                eligibility=None,
            )
        current = self.market_states[mapping.symbol]
        asset = self.broker.asset(mapping.symbol)
        eligibility = IssuerEligibility(
            symbol=mapping.symbol,
            issuer_id=mapping.issuer_id,
            cik=mapping.cik,
            available_at=now,
            valid_from=mapping.valid_from,
            security_type="common_stock",
            exchange_listed=asset.exchange in {"NYSE", "NASDAQ", "AMEX", "ARCA"},
            tradable=asset.tradable,
            fractionable=asset.fractionable,
            median_daily_dollar_volume=current.median_daily_dollar_volume,
            liquidity_available_at=current.observed_at,
            liquidity_completed_sessions=30,
        )
        quote = EventQuote(
            symbol=mapping.symbol,
            bid=current.bid,
            ask=current.ask,
            bid_size=current.bid_size,
            ask_size=current.ask_size,
            timestamp=current.quote_at,
            received_at=current.observed_at,
            feed=current.feed,
            size_unit=latest_rest_quote_size_unit(feed=current.feed, quote_at=current.quote_at),
        )
        with self.store.database.begin() as connection:
            saved = connection.execute(
                select(events.c.payload)
                .where(
                    events.c.trace_id
                    == f"{self.settings.cohort_id}:{event.evidence_id}:pre_context"
                )
                .order_by(events.c.recorded_at)
                .limit(1)
            ).scalar_one_or_none()
        saved_symbol = saved.get(mapping.symbol) if saved else None
        pre = EventMarketState.model_validate(saved_symbol) if saved_symbol else None
        gate = self.calendar.gate(now)
        bar = current.completed_bar
        return decide_event(
            extraction,
            evidence=(event,),
            now=now,
            quote=quote,
            eligibility=eligibility,
            market=MarketContext(
                symbol=mapping.symbol,
                mode=mode,
                session_open=gate.session_open,
                session_close=gate.session_close,
                observation_start=bar.timestamp - timedelta(minutes=5) if bar else None,
                observation_end=bar.timestamp if bar else None,
                observation_available_at=self.first_bar_receipts.get(
                    (mapping.symbol, bar.timestamp)
                )
                if bar
                else None,
                pre_event_price=(pre.bid + pre.ask) / 2 if pre else None,
                pre_event_volatility_fraction=pre.pre_event_volatility_bps / 10000
                if pre and pre.pre_event_volatility_bps
                else None,
                pre_event_available_at=pre.observed_at if pre else None,
                halted=self.context.halted_for(mapping.symbol, now=now) if self.context else None,
                feed_healthy=self.last_source_success is not None,
                macro_calendar_available_at=self.context.macro_calendar_available_at
                if self.context
                else None,
                macro_calendar_covers_until=self.context.macro_calendar_covers_until
                if self.context
                else None,
                scheduled_macro_events=self.context.scheduled_macro_events if self.context else (),
                contradictions=self.context.blocking_reasons(now=now) if self.context else (),
            ),
        )

    def _entry(self, decision: Any, event: SourceEvent, now: datetime) -> None:
        if self.cert is None or decision.symbol is None:
            return
        market = self.market_states[decision.symbol]
        result = self.oms.submit_entry(
            symbol=decision.symbol,
            cluster_key=decision.event_cluster_id,
            decision_id=decision.decision_id,
            eligible_at=decision.eligible_at,
            expires_at=decision.expires_at,
            bid=market.bid,
            ask=market.ask,
            quote_at=market.quote_at,
            median_dollar_volume=market.median_daily_dollar_volume or Decimal(0),
            source_valid=event.is_primary_source and event.availability_basis == "observed_receipt",
            certificate=self.cert,
            now=now,
        )
        self.store.audit("submission_result", result, now, self.settings.cohort_id)


async def run_event_service(
    settings: ExperimentalSettings, *, once: bool = False
) -> dict[str, Any]:
    reject_live_environment()
    app = AppConfig()
    with Database(app.database_url.get_secret_value()) as database:
        store = EventStore(database)
        repository = ProductionRepository(database)
        instance = os.getenv("RENDER_INSTANCE_ID") or str(os.getpid())
        if not repository.acquire_worker_lock(
            "tradeagent-event-worker", instance, stale_after_seconds=180
        ):
            raise RuntimeError("another event worker owns the allocation")
        with database.begin() as connection:
            seeds = tuple(
                SourceEvent.model_validate(row)
                for row in connection.scalars(select(event_evidence.c.payload))
            )
        contact = settings.sec_contact_email or os.getenv("NEWS_CONTACT_EMAIL")
        with (
            EventSourceClient(
                AlpacaDataSettings.model_validate({}),
                seed_evidence=seeds,
                primary_urls=settings.primary_urls,
                sec_user_agent=f"TradeAgent/20 {contact}" if contact else None,
            ) as source,
            AlpacaPaperClient(AlpacaPaperSettings.model_validate({})) as broker,
        ):
            market = EventMarketClient(AlpacaDataSettings.model_validate({}))
            runtime = EventRuntime(
                store,
                settings,
                source,
                market,
                broker,
                instance_id=instance,
                code_sha=code_identity(),
            )
            try:
                while True:
                    try:
                        result = await asyncio.to_thread(runtime.tick)
                    except EventLeaseLostError:
                        raise
                    except (httpx.HTTPError, ValueError, RuntimeError) as error:
                        runtime.store.audit(
                            "worker_error",
                            {"type": type(error).__name__, "message": str(error)[:200]},
                            datetime.now(UTC),
                            settings.cohort_id,
                        )
                        runtime.oms.pause("WORKER_ERROR_RECONCILIATION_REQUIRED", datetime.now(UTC))
                        if once:
                            raise
                        repository.heartbeat(
                            "tradeagent-event-worker",
                            instance,
                            {
                                "state": "paused",
                                "cohort_id": settings.cohort_id,
                                "mode": settings.mode,
                                "code_sha": runtime.code_sha,
                                "blockers": [type(error).__name__],
                            },
                        )
                    else:
                        if once:
                            return result
                    await asyncio.sleep(settings.poll_seconds)
            finally:
                market.close()
                runtime.context_client.close()
                repository.release_worker_lock("tradeagent-event-worker", instance)
