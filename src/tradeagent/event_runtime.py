from __future__ import annotations

import asyncio
import json
import os
from datetime import UTC, datetime, timedelta
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal
from zoneinfo import ZoneInfo

import httpx
from pydantic import ValidationError
from sqlalchemy import select

from tradeagent.alpaca import AlpacaDataSettings
from tradeagent.alpaca_paper import AlpacaPaperClient, AlpacaPaperSettings
from tradeagent.config import AppConfig
from tradeagent.event_brief import (
    available_prior_evidence,
    load_frozen_evidence_packet,
    persist_premarket_brief,
)
from tradeagent.event_context import (
    OfficialContextClient,
    OfficialContextSnapshot,
    latest_rest_quote_size_unit,
)
from tradeagent.event_doctor import code_identity
from tradeagent.event_market import EventMarketClient, EventMarketState
from tradeagent.event_order_stream import AlpacaPaperTradeUpdatesStream
from tradeagent.event_orders import FINAL, EventLeaseLostError, ExperimentalOrderManager
from tradeagent.event_outcomes import record_quote_paths
from tradeagent.event_performance import paper_cost_assumptions
from tradeagent.event_research import (
    DEFAULT_EVENT_POLICY,
    EventDecision,
    EventPolicy,
    EventQuote,
    ExtractionResult,
    IssuerEligibility,
    MarketContext,
    SourceEvent,
    decide_event,
    extract_event,
    supported_issuer_mappings,
)
from tradeagent.event_session import (
    CANDIDATE_SELECTION_RULE,
    TUESDAY_PROTOCOL_VERSION,
    PaperSessionPlan,
    verify_session_plan,
)
from tradeagent.event_sources import OFFICIAL_ISSUER_FEEDS, EventSourceClient
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


def policy_for(settings: ExperimentalSettings) -> EventPolicy:
    return DEFAULT_EVENT_POLICY.model_copy(
        update={"allow_iex_experimental_paper": settings.purpose == "iex-practice"}
    )


def cohort_manifest(settings: ExperimentalSettings, code_sha: str) -> tuple[str, dict[str, Any]]:
    policy = policy_for(settings).model_dump(mode="json")
    app = AppConfig()
    operational_settings = {
        "intraday": app.intraday.model_dump(mode="json"),
        "risk": app.risk.model_dump(mode="json"),
    }
    root = Path(__file__).parent
    modules = sorted(
        {
            *root.glob("event_*.py"),
            *(
                root / name
                for name in (
                    "experimental_policy.py",
                    "alpaca_paper.py",
                    "domain.py",
                    "config.py",
                    "intraday.py",
                    "order_state.py",
                    "persistence.py",
                )
            ),
        }
    )
    module_hashes = {
        path.name: sha256(path.read_text(encoding="utf-8").encode()).hexdigest() for path in modules
    }
    frozen_inputs: dict[str, object] = {
        "policy": policy,
        "extraction": {
            "provider": "deterministic-only",
            "model_prompt": "source-span numeric grammar; no LLM",
            "implementation_hash": module_hashes["event_research.py"],
            "prior_comparisons": "verified observed pre-event issuer evidence only",
        },
        "cost_assumptions": paper_cost_assumptions(),
        "source_set": {
            "licensed_news": "Alpaca/Benzinga v1beta1 news; metadata retention",
            "official_issuer_feeds": dict(OFFICIAL_ISSUER_FEEDS),
            "configured_primary_urls": settings.primary_urls,
            "SEC": "issuer-verified submissions, filings, EX-99 and older comparison documents",
            "publication_policy": (
                "explicit aware original publication; acceptance/updated times are not substituted"
            ),
            "market_observations": {
                "execution_quotes_and_intraday_bars": settings.execution_feed,
                "daily_liquidity_and_volatility": "historical SIP, raw adjustment",
                "latest_REST_quote_sizes": "shares; verified endpoint schema",
                "paper_fill_basis": "Alpaca NBBO simulation, not observed IEX execution",
            },
            "consensus": "unavailable",
        },
        "module_hashes": module_hashes,
        "operational_settings": operational_settings,
    }
    digest = settings.fingerprint(frozen_inputs, code_sha)
    manifest: dict[str, Any] = {
        "extraction": frozen_inputs["extraction"],
        "cost_assumptions": frozen_inputs["cost_assumptions"],
        "source_set": frozen_inputs["source_set"],
        "policy_change": (
            "Explicit separate IEX paper practice; frozen research remains unchanged"
            if settings.purpose == "iex-practice"
            else "User v20 instruction replaces stop-discovery with bounded experimental paper"
        ),
        "purpose": settings.purpose,
        "execution_feed": settings.execution_feed,
        "operational_settings": operational_settings,
        "qualification_eligible": settings.purpose != "iex-practice",
        "evidence_use": (
            "operational_practice_only"
            if settings.purpose == "iex-practice"
            else "prospective_research"
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
    if settings.purpose == "iex-practice":
        manifest.update(
            {
                "evidence_clock": "excluded from all research qualification clocks",
                "review": "operational practice only; no 60-session/60-round-trip qualification",
                "primary_outcome": "order handling, reconciliation and safety; not economic alpha",
                "calibration": {
                    "session_date": str(settings.practice_start_date),
                    "symbol": "AAPL",
                    "maximum_entry_attempts": 1,
                    "window_minutes_after_open": 30,
                    "existing_entry_warmup_preserved": True,
                    "entry_expiry_seconds": 30,
                    "exit_after_seconds": 60,
                    "maximum_notional_usd": "25",
                    "missed_or_rejected_fill": "record honestly; no forced replacement",
                },
                "session_protocol_version": TUESDAY_PROTOCOL_VERSION,
                "candidate_selection_rule": CANDIDATE_SELECTION_RULE,
                "maximum_news_entries_per_session": 1,
                "entry_session_only": str(settings.practice_start_date),
                "equipment_classification": "EQUIPMENT_TEST",
                "equipment_identity_scope": "paper account and planned session, not deployment",
                "news_scope": "preceding broker regular-session close through session updates",
                "news_age_is_independent_of_collection_window": True,
                "formal_forward_evaluation_started": False,
            }
        )
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
        order_stream: AlpacaPaperTradeUpdatesStream | None = None,
    ):
        self.store, self.settings = store, settings
        self.source, self.market, self.broker = source, market, broker
        self.repo = ProductionRepository(store.database)
        self.instance_id, self.code_sha = instance_id, code_sha
        self.app = AppConfig()
        self.calendar = NyseSessionCalendar(self.app.intraday)
        self.policy = policy_for(settings)
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
        self.session_plan: PaperSessionPlan | None = None
        self.premarket_brief: dict[str, Any] | None = None
        self.order_stream = order_stream

    def operational_preflight(self, now: datetime) -> OperationalCertificate:
        reconciliation = self.oms.reconcile(now)
        account = self.broker.account()
        confirmation = self.repo.get_control(f"{self.settings.cohort_id}:certificate")
        prior = OperationalCertificate.model_validate_json(confirmation) if confirmation else None
        checks = {
            "paper_host": self.broker.broker_host == "https://paper-api.alpaca.markets",
            "no_live_credentials": not any(
                os.getenv(key)
                for key in ("ALPACA_LIVE_KEY", "ALPACA_LIVE_KEY_ID", "ALPACA_LIVE_SECRET_KEY")
            ),
            "account_reconciled": reconciliation["healthy"],
            "cohort_frozen": True,
            "source_connected": self._sources_fresh(now),
            "frozen_policy_market_feed": all(
                symbol in self.market_states
                and self.market_states[symbol].feed == self.settings.execution_feed
                for symbol in self.settings.symbols.split(",")
            ),
            "primary_source_configured": bool(
                self.source.capabilities["sec_enabled"]
                or self.source.capabilities["primary_urls_configured"]
                or self.source.capabilities.get("issuer_feeds_enabled")
            ),
            "operational_test_attestation": self.repo.get_control("v20:mechanics_attestation")
            == self.code_sha,
            "current_inputs_required_at_each_entry": True,
            "operator_confirmation": prior is not None
            and prior.permits_paper
            and prior.checks.get("operator_confirmation") is True
            and prior.config_hash == self.config_hash
            and prior.code_sha == self.code_sha
            and prior.cohort_id == self.settings.cohort_id
            and prior.account_digest == sha256(account.id.encode()).hexdigest(),
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
                *(
                    ("IEX practice only; excluded from strategy qualification",)
                    if self.settings.purpose == "iex-practice"
                    else ()
                ),
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
            feed_healthy=self._sources_fresh(tick_at),
        )
        if self.order_stream is not None:
            for message in self.order_stream.drain():
                self.oms.reconcile_stream_update(message.model_dump(mode="json"), datetime.now(UTC))
        if self.settings.purpose == "iex-practice":
            self._ensure_session_plan(datetime.now(UTC))
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
        if market_errors:
            self.oms.supervise(datetime.now(UTC), feed_healthy=False)
        self.oms.valuation(marks, datetime.now(UTC))
        record_quote_paths(
            self.store, self.settings.cohort_id, self.market_states, datetime.now(UTC)
        )
        watermark_key = f"{self.settings.cohort_id}:source_watermark"
        watermark = self.repo.get_control(watermark_key)
        start = (
            datetime.fromisoformat(watermark)
            if watermark
            else self.session_plan.previous_session_close
            if self.session_plan is not None
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
            watermark_value = (
                self.source.last_poll_stats.get("coverage_watermark")
                if self.settings.purpose == "iex-practice"
                else tick_at.isoformat()
            )
            if watermark_value is not None:
                self.repo.set_control(watermark_key, str(watermark_value))
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
            self.oms.supervise(datetime.now(UTC), feed_healthy=False)
        candidates: list[tuple[SourceEvent, ExtractionResult, EventDecision, str]] = []
        pending = self.store.pending_evidence(self.settings.cohort_id)
        for row in pending:
            event = SourceEvent.model_validate(row["payload"])
            evaluated_at = datetime.now(UTC)
            try:
                extraction = self._extraction(event, evaluated_at)
                metadata = json.loads(event.raw_metadata_json)
                filing = metadata.get("filing") if isinstance(metadata, dict) else None
                context_only = (
                    isinstance(filing, dict) and filing.get("collection_role") == "older_comparison"
                ) or (
                    self.session_plan is not None
                    and event.published_at is not None
                    and event.published_at < self.session_plan.previous_session_close
                )
                if context_only:
                    self.store.decision(
                        self.settings.cohort_id,
                        event.evidence_id,
                        {
                            "action": "abstain",
                            "reasons": ["OLDER_COMPARISON_CONTEXT_ONLY"],
                            "source_url": event.source_url,
                            "extraction": extraction.model_dump(mode="json"),
                            "published_at": event.published_at.isoformat()
                            if event.published_at
                            else None,
                        },
                        evaluated_at,
                    )
                    continue
                decision = self._decision(event, extraction, evaluated_at, pre_receipt_states)
                evaluated_at = decision.decided_at
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
                    {
                        **decision.model_dump(mode="json"),
                        "evidence_id": event.evidence_id,
                        "source_url": event.source_url,
                        "rule_observations": self._rule_observations(decision, event),
                    },
                    evaluated_at,
                    self.settings.cohort_id + ":" + event.evidence_id,
                )
                if waiting:
                    if self.store.was_decided(self.settings.cohort_id, event.evidence_id):
                        stored_id = self.store.decision(
                            self.settings.cohort_id, event.evidence_id, {}, evaluated_at
                        )
                        self.store.candidate_state(
                            stored_id,
                            self.settings.cohort_id,
                            "waiting",
                            {
                                "reasons": list(decision.reasons),
                                "evidence_id": event.evidence_id,
                                "latest_decision": decision.model_dump(mode="json"),
                            },
                            evaluated_at,
                        )
                    continue
                stored_id = self.store.decision(
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
                    self.store.candidate_state(
                        stored_id,
                        self.settings.cohort_id,
                        "waiting",
                        {"reasons": ["awaiting_selection"], "evidence_id": event.evidence_id},
                        evaluated_at,
                    )
                    candidates.append((event, extraction, decision, stored_id))
                else:
                    self.store.candidate_state(
                        stored_id,
                        self.settings.cohort_id,
                        "rejected",
                        {"reasons": list(decision.reasons), "evidence_id": event.evidence_id},
                        evaluated_at,
                    )
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
        calibration = self._practice_calibration(datetime.now(UTC))
        self._select_news_candidate(candidates, pre_receipt_states)
        if self.session_plan is not None:
            self.premarket_brief = persist_premarket_brief(
                self.store.database,
                cohort_id=self.settings.cohort_id,
                session_open=self.session_plan.session_open,
                session_close=self.session_plan.session_close,
                previous_session_close=self.session_plan.previous_session_close,
                now=datetime.now(UTC),
                source_capabilities=self.source.capabilities,
                poll_stats=self.source.last_poll_stats,
            )
        completed_at = datetime.now(UTC)
        clock = self.broker.clock()
        blockers = list(self.context.errors)
        if self.settings.purpose == "research" and any(
            state.feed == "iex" for state in self.market_states.values()
        ):
            blockers.append("IEX_SHADOW_ONLY_FROZEN_POLICY")
        if self.cert is not None and not self.cert.permits_paper:
            blockers.extend(key for key, passed in self.cert.checks.items() if not passed)
        if self.repo.get_control("kill_switch") == "active":
            blockers.append("GLOBAL_KILL_SWITCH")
        heartbeat_state = {
            "state": "market_closed" if not clock.is_open else "collecting",
            "mode": self.settings.mode,
            "purpose": self.settings.purpose,
            "qualification_eligible": self.settings.purpose != "iex-practice",
            "execution_feed": self.settings.execution_feed,
            "practice_start_date": str(self.settings.practice_start_date)
            if self.settings.practice_start_date
            else None,
            "calibration": calibration,
            "session_plan": self.session_plan.model_dump(mode="json")
            if self.session_plan
            else None,
            "session_budget": self.oms.session_budget(),
            "premarket_brief": {
                key: self.premarket_brief.get(key)
                for key in (
                    "snapshot_id",
                    "prepared_before_open",
                    "preparation_status",
                    "coverage",
                    "funnel",
                    "capability_gaps",
                )
            }
            if self.premarket_brief
            else None,
            "broker_stream": self.order_stream.health_snapshot()
            if self.order_stream
            else {
                "state": "not_connected_in_this_run",
                "rest_reconciliation": "active",
            },
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
            "limitations": [
                "NO_CONFIGURED_INFERENCE_PROVIDER_DETERMINISTIC_ONLY",
                *(
                    ["IEX_PRACTICE_ONLY_NOT_QUALIFICATION_EVIDENCE"]
                    if self.settings.purpose == "iex-practice"
                    else []
                ),
            ],
            "blockers": list(dict.fromkeys(blockers)),
            "official_context": self.context_client.capabilities,
            "operational_certificate": self.cert.model_dump(mode="json") if self.cert else None,
            "edge_established": False,
        }
        self.repo.heartbeat(
            "tradeagent-event-worker", self.instance_id, heartbeat_state, observed_at=completed_at
        )
        self.oms.assert_owner(completed_at)
        self._record_session_completion(completed_at)
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
                with self.store.database.begin() as connection:
                    evidence = [
                        SourceEvent.model_validate(value)
                        for value in connection.scalars(select(event_evidence.c.payload))
                    ]
                extraction = extract_event(
                    event,
                    now=now,
                    prior_evidence=available_prior_evidence(event, evidence, now=now),
                )
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
        packet = load_frozen_evidence_packet(self.store.database, extraction, now=now)
        packet = (event, *(item for item in packet if item.evidence_id != event.evidence_id))
        with self.store.database.begin() as connection:
            known_versions = (
                SourceEvent.model_validate(payload)
                for payload in connection.scalars(
                    select(event_evidence.c.payload).where(
                        event_evidence.c.received_at <= now,
                        event_evidence.c.payload["issuer_id"].as_string() == extraction.issuer_id,
                    )
                )
            )
            corrections = tuple(
                item
                for item in known_versions
                if item.evidence_id not in extraction.evidence_ids
                and (item.is_correction or item.is_retraction or item.revision_of is not None)
                and (
                    item.event_cluster_id == extraction.event_cluster_id
                    or item.revision_of in extraction.evidence_ids
                )
            )
        # Later corrections are current veto evidence, never inputs to the frozen extraction.
        packet = (*packet, *corrections)
        mapping = next(
            (m for m in supported_issuer_mappings() if m.issuer_id == event.issuer_id), None
        )
        mode: Literal["experimental_paper", "shadow"] = (
            "experimental_paper" if self.settings.mode == "experimental-paper" else "shadow"
        )
        if mapping is None or mapping.symbol not in self.market_states:
            return decide_event(
                extraction,
                evidence=packet,
                now=now,
                quote=None,
                market=MarketContext(mode=mode),
                eligibility=None,
                policy=self.policy,
            )
        current = self.market_states[mapping.symbol]
        if self.settings.mode == "experimental-paper":
            current = self._refresh_quote(mapping.symbol)
            now = datetime.now(UTC)
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
            liquidity_completed_sessions=current.completed_daily_sessions or 0,
        )
        quote = _execution_quote(current)
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
            evidence=packet,
            now=now,
            quote=quote,
            eligibility=eligibility,
            policy=self.policy,
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
                feed_healthy=self._sources_fresh(now),
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

    def _entry(
        self,
        decision: EventDecision,
        event: SourceEvent,
        now: datetime,
        *,
        ticket: dict[str, Any] | None = None,
        stored_decision_id: str | None = None,
    ) -> dict[str, Any]:
        if self.cert is None or decision.symbol is None:
            return {"state": "risk_rejected", "reasons": ["OPERATIONAL_CERTIFICATE_REQUIRED"]}
        if self._calibration_waiting(now):
            self.store.audit(
                "submission_result",
                {"state": "risk_rejected", "reasons": ["PRACTICE_CALIBRATION_PENDING"]},
                now,
                self.settings.cohort_id,
            )
            return {"state": "risk_rejected", "reasons": ["PRACTICE_CALIBRATION_PENDING"]}
        if decision.eligible_at is None or decision.expires_at is None:
            raise ValueError("eligible decision lacks an execution interval")
        market = self.market_states[decision.symbol]
        result = self.oms.submit_entry(
            symbol=decision.symbol,
            cluster_key=decision.event_cluster_id,
            decision_id=stored_decision_id or decision.decision_id,
            eligible_at=decision.eligible_at,
            expires_at=decision.expires_at,
            bid=market.bid,
            ask=market.ask,
            quote_at=market.quote_at,
            median_dollar_volume=market.median_daily_dollar_volume or Decimal(0),
            source_valid=(
                event.is_primary_source
                and event.availability_basis == "observed_receipt"
                and self._sources_fresh(now)
            ),
            certificate=self.cert,
            now=now,
            execution_quote=_execution_quote(market),
            decision_ticket=ticket,
            maximum_limit_price=decision.permitted_limit_price,
        )
        self.store.audit("submission_result", result, now, self.settings.cohort_id)
        return result

    def _ensure_session_plan(self, now: datetime) -> None:
        if self.session_plan is not None:
            return
        raw = self.repo.get_control(f"{self.settings.cohort_id}:session-plan")
        if raw is not None:
            plan = PaperSessionPlan.model_validate_json(raw)
        else:
            if self.settings.practice_start_date is None:
                raise ValueError("practice session date required")
            plan = verify_session_plan(
                self.broker, self.settings.practice_start_date, self.app.intraday, now
            )
            self.repo.set_control(f"{self.settings.cohort_id}:session-plan", plan.model_dump_json())
            self.store.audit(
                "broker_calendar", plan.model_dump(mode="json"), now, self.settings.cohort_id
            )
        if plan.session_date != self.settings.practice_start_date:
            raise ValueError("persisted broker calendar does not match the frozen session")
        self.session_plan = plan

    def _rule_observations(self, decision: EventDecision, event: SourceEvent) -> dict[str, Any]:
        quote = decision.quote_snapshot
        mid = (quote.bid + quote.ask) / 2 if quote else None
        market = decision.market_snapshot
        return {
            "failed_rules": list(decision.reasons),
            "source": {
                "published_at": event.published_at.isoformat() if event.published_at else None,
                "first_received_at": event.first_received_at.isoformat(),
                "publication_to_receipt_seconds": (
                    (event.first_received_at - event.published_at).total_seconds()
                    if event.published_at
                    else None
                ),
                "receipt_age_seconds": (
                    decision.decided_at - event.first_received_at
                ).total_seconds(),
                "maximum_receipt_age_seconds": self.policy.max_source_age_seconds,
                "primary": event.is_primary_source,
                "revision_of": event.revision_of,
                "retraction": event.is_retraction,
            },
            "quote_age_seconds": (
                (decision.decided_at - quote.timestamp).total_seconds() if quote else None
            ),
            "maximum_quote_age_seconds": self.policy.max_quote_age_seconds,
            "spread_bps": str((quote.ask - quote.bid) / mid * 10000)
            if quote and mid and mid > 0
            else None,
            "maximum_spread_bps": str(self.policy.max_spread_bps),
            "guidance_increase_fraction": str(decision.guidance_revision_fraction)
            if decision.guidance_revision_fraction is not None
            else None,
            "minimum_guidance_increase_fraction": str(
                self.policy.h1_min_guidance_increase_fraction
            ),
            "contract_to_revenue_fraction": str(decision.total_contract_to_annual_revenue)
            if decision.total_contract_to_annual_revenue is not None
            else None,
            "minimum_contract_to_revenue_fraction": str(
                self.policy.h2_min_contract_to_annual_revenue
            ),
            "pre_event_price": str(market.pre_event_price)
            if market.pre_event_price is not None
            else None,
            "price_reaction_fraction": str(mid / market.pre_event_price - 1)
            if mid is not None and market.pre_event_price and market.pre_event_price > 0
            else None,
            "permitted_limit_price": str(decision.permitted_limit_price)
            if decision.permitted_limit_price is not None
            else None,
            "completed_observation_end": market.observation_end.isoformat()
            if market.observation_end
            else None,
            "completed_observation_received_at": market.observation_available_at.isoformat()
            if market.observation_available_at
            else None,
            "halted": market.halted,
            "market_contradictions": list(market.contradictions),
        }

    def _select_news_candidate(
        self,
        candidates: list[tuple[SourceEvent, ExtractionResult, EventDecision, str]],
        pre_states: dict[str, EventMarketState],
    ) -> None:
        if not candidates:
            return
        now = datetime.now(UTC)
        ranked = sorted(
            candidates,
            key=lambda item: (
                -item[0].first_received_at.timestamp(),
                item[2].symbol or "",
                item[0].evidence_id,
            ),
        )
        alternatives = [
            {
                "rank": index + 1,
                "decision_id": stored_id,
                "evidence_id": event.evidence_id,
                "symbol": decision.symbol,
                "first_received_at": event.first_received_at.isoformat(),
                "source_url": event.source_url,
                "decision_time": decision.decided_at.isoformat(),
            }
            for index, (event, _, decision, stored_id) in enumerate(ranked)
        ]
        budget = self.oms.session_budget()
        terminal_reason = (
            "NEWS_ENTRY_LIMIT"
            if budget is not None and budget["news_entries_reserved"] >= 1
            else "PRACTICE_SESSION_ENDED"
            if self.session_plan is not None and now >= self.session_plan.session_close
            else None
        )
        local_rows = self.store.linked_orders(self.settings.cohort_id)
        waiting_reason = (
            "PRACTICE_CALIBRATION_PENDING"
            if self._calibration_waiting(now)
            else "POSITION_OR_ORDER_RESERVED"
            if self.oms.inventory(local_rows)
            or any(row["status"] not in FINAL for row in local_rows)
            else None
        )
        if terminal_reason or waiting_reason:
            reason = terminal_reason or waiting_reason
            for event, _, _, stored_id in ranked:
                self.store.candidate_state(
                    stored_id,
                    self.settings.cohort_id,
                    "rejected" if terminal_reason else "waiting",
                    {
                        "reasons": [reason],
                        "evidence_id": event.evidence_id,
                        "session_budget": budget,
                    },
                    now,
                )
            self.store.audit(
                "candidate_selection",
                {
                    "rule": CANDIDATE_SELECTION_RULE,
                    "alternatives": alternatives,
                    "selected": None,
                    "reasons": [reason],
                    "session_budget": budget,
                },
                now,
                self.settings.cohort_id,
            )
            return
        for event, extraction, _, stored_id in ranked:
            fresh = self._decision(event, extraction, datetime.now(UTC), pre_states)
            selection = {
                "rule": CANDIDATE_SELECTION_RULE,
                "alternatives": alternatives,
                "selected": stored_id if fresh.action == "eligible" else None,
                "selected_symbol": fresh.symbol if fresh.action == "eligible" else None,
                "revalidation_at": fresh.decided_at.isoformat(),
                "reasons": list(fresh.reasons),
                "rule_observations": self._rule_observations(fresh, event),
                "selection_basis": "first ranked candidate still eligible on fresh re-evaluation",
            }
            self.store.audit(
                "candidate_selection", selection, fresh.decided_at, self.settings.cohort_id
            )
            if fresh.action != "eligible":
                waitable = set(fresh.reasons).issubset(
                    {
                        "quote_not_causal_or_fresh",
                        "completed_post_availability_observation_missing",
                        "processing_latency_not_elapsed",
                    }
                )
                self.store.candidate_state(
                    stored_id,
                    self.settings.cohort_id,
                    "waiting" if waitable else "expired",
                    {
                        "reasons": list(fresh.reasons),
                        "evidence_id": event.evidence_id,
                        "latest_decision": fresh.model_dump(mode="json"),
                    },
                    fresh.decided_at,
                )
                continue
            if (
                self.cert is None
                or not self.cert.permits_paper
                or self.cert.expires_at <= fresh.decided_at
            ):
                self.operational_preflight(fresh.decided_at)
            ticket = self._news_ticket(event, extraction, fresh, selection, stored_id)
            result = self._entry(
                fresh, event, datetime.now(UTC), ticket=ticket, stored_decision_id=stored_id
            )
            retryable = result["state"] == "risk_rejected" and set(
                result.get("reasons", [])
            ).issubset(
                {
                    "STALE_QUOTE",
                    "INVALID_EXECUTION_QUOTE",
                    "POSITION_OR_ORDER_RESERVED",
                    "OPERATIONAL_CERTIFICATE_REQUIRED",
                    "OPERATIONAL_PAUSE",
                    "GLOBAL_KILL_SWITCH",
                    "ECONOMIC_RISK_OR_PAUSE",
                    "UNVERIFIED_SOURCE_OR_ISSUER",
                    "PRACTICE_CALIBRATION_PENDING",
                }
            )
            self.store.candidate_state(
                stored_id,
                self.settings.cohort_id,
                "waiting"
                if retryable
                else "rejected"
                if result["state"] == "risk_rejected"
                else "attempted",
                {
                    "evidence_id": event.evidence_id,
                    "submission_result": result,
                    "reasons": result.get("reasons", []),
                    "decision_ticket": ticket,
                },
                datetime.now(UTC),
            )
            break

    def _news_ticket(
        self,
        event: SourceEvent,
        extraction: ExtractionResult,
        decision: EventDecision,
        selection: dict[str, Any],
        stored_id: str,
    ) -> dict[str, Any]:
        if decision.quote_snapshot is None:
            raise ValueError("news decision lacks a quote snapshot")
        if decision.hypothesis not in {"H1", "H2"}:
            raise ValueError("news decision has no supported hypothesis")
        quote = decision.quote_snapshot
        limit = quote.ask.quantize(Decimal("0.01"), rounding=ROUND_UP)
        notional_cap = self.settings.effective_notional(self.app)
        quantity = (notional_cap / limit).quantize(Decimal("0.000001"), rounding=ROUND_DOWN)
        packet = load_frozen_evidence_packet(
            self.store.database, extraction, now=decision.decided_at
        )
        with self.store.database.begin() as connection:
            pre_context = connection.scalar(
                select(events.c.payload)
                .where(
                    events.c.trace_id
                    == f"{self.settings.cohort_id}:{event.evidence_id}:pre_context"
                )
                .order_by(events.c.recorded_at)
                .limit(1)
            )
        gate = self.calendar.gate(decision.decided_at)
        thesis = (
            "Documented comparable company-guidance increase; "
            "this is not a measured consensus surprise."
            if decision.hypothesis == "H1"
            else "Documented binding contract passes the frozen annual-revenue materiality rule."
        )
        return {
            "schema_version": "news-decision-ticket-v1",
            "stored_decision_id": stored_id,
            "trade_classification": "NEWS_STRATEGY",
            "purpose": self.settings.purpose,
            "qualification_eligible": self.settings.purpose != "iex-practice",
            "decision": decision.model_dump(mode="json"),
            "evidence_ids": list(extraction.evidence_ids),
            "event_evidence": [
                {
                    "evidence_id": item.evidence_id,
                    "source_url": item.source_url,
                    "publisher": item.source,
                    "document_id": item.source_event_id,
                    "published_at": item.published_at.isoformat() if item.published_at else None,
                    "provider_received_at": None,
                    "provider_created_at": item.provider_created_at.isoformat()
                    if item.provider_created_at
                    else None,
                    "first_received_at": item.first_received_at.isoformat(),
                    "revision_at": item.provider_updated_at.isoformat()
                    if item.provider_updated_at
                    else None,
                    "revision_of": item.revision_of,
                    "content_sha256": item.content_sha256,
                    "rights_profile": item.rights_profile,
                }
                for item in packet
            ],
            "extraction": extraction.model_dump(mode="json"),
            "facts": [fact.model_dump(mode="json") for fact in extraction.facts],
            "thesis": thesis,
            "contrary_evidence": list(extraction.contradictions),
            "contrary_evidence_limit": (
                "Only supported deterministic checks; "
                "absence is not proof that no adverse facts exist."
            ),
            "missing_information": list(extraction.missing_required_fields),
            "consensus": None,
            "expected_net_return_bps": None,
            "probability_of_profit": None,
            "rule_observations": self._rule_observations(decision, event),
            "pre_event_market_snapshot": pre_context.get(decision.symbol) if pre_context else None,
            "market_data_sources": {
                "quote": quote.feed,
                "completed_intraday_bar": quote.feed,
                "liquidity_and_volatility_history": "SIP historical daily bars, raw adjustment",
                "broker_paper_fills": "Alpaca NBBO simulator, not IEX executions",
                "vwap": "not used by these frozen event rules",
            },
            "selection": selection,
            "entry_conditions": {
                "type": "marketable_limit",
                "limit_price": str(limit),
                "quantity": str(quantity),
                "notional": str(quantity * limit),
                "maximum_notional": str(notional_cap),
                "expires_at": decision.expires_at.isoformat() if decision.expires_at else None,
                "quote_feed": quote.feed,
                "quote_size_unit": quote.size_unit,
            },
            "exit_rules": {
                "maximum_holding_minutes": self.settings.max_holding_minutes,
                "stop_invalidation": (
                    "existing R1 primary-risk review, feed/reconciliation pause, "
                    "allocation loss/drawdown or session flatten"
                ),
                "price_stop": None,
                "profit_target": None,
                "holding_deadline": (
                    decision.decided_at + timedelta(minutes=self.settings.max_holding_minutes)
                ).isoformat(),
                "flatten_start_local": self.app.intraday.flatten_start.isoformat(),
                "session_close": gate.session_close.isoformat() if gate.session_close else None,
            },
            "cost_assumptions": paper_cost_assumptions(),
            "code_sha": self.code_sha,
            "config_hash": self.config_hash,
            "created_at": decision.decided_at.isoformat(),
        }

    def _record_session_completion(self, now: datetime) -> None:
        if self.session_plan is None or now < self.session_plan.session_close:
            return
        result = self.oms.reconcile(now)
        unconfirmed = [
            row["client_order_id"]
            for row in self.store.linked_orders(self.settings.cohort_id)
            if row["status"] not in FINAL
        ]
        complete = (
            result["healthy"]
            and not result["positions"]
            and result["open_orders"] == 0
            and not unconfirmed
        )
        summary = {
            "session_date": self.session_plan.session_date.isoformat(),
            "state": "complete" if complete else "incident_unresolved_exposure",
            "positions": result["positions"],
            "open_orders": result["open_orders"],
            "unconfirmed_local_orders": unconfirmed,
            "mismatches": result["mismatches"],
        }
        key = f"{self.settings.cohort_id}:session-completion"
        value = json.dumps(summary, sort_keys=True)
        if self.repo.get_control(key) != value:
            self.repo.set_control(key, value)
            self.store.audit(
                "session_completion",
                {**summary, "observed_at": now.isoformat()},
                now,
                self.settings.cohort_id,
            )
            from tradeagent.event_session_report import session_report

            session_report(
                self.store.database,
                self.settings.cohort_id,
                self.session_plan.session_date,
                observed_at=now,
                persist=True,
            )

    def _refresh_quote(self, symbol: str) -> EventMarketState:
        state = self.market.refresh_quote(self.market_states[symbol])
        self.market_states[symbol] = state
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
        return state

    def _calibration_waiting(self, now: datetime) -> bool:
        if self.settings.purpose != "iex-practice":
            return False
        local_date = now.astimezone(ZoneInfo(self.app.intraday.timezone)).date()
        gate = self.calendar.gate(now)
        return (
            local_date == self.settings.practice_start_date
            and gate.session_open is not None
            and now < gate.session_open + timedelta(minutes=30)
            and not (self.oms.session_budget() or {}).get("equipment_client_order_id")
        )

    def _practice_calibration(self, now: datetime) -> dict[str, Any] | None:
        if self.settings.purpose != "iex-practice":
            return None
        result = self._calibration_step(now)
        key = f"{self.settings.cohort_id}:calibration_status"
        previous = self.repo.get_control(key)
        if result.get("state") == "missed_window_no_trade":
            old = json.loads(previous) if previous else None
            if old and old.get("outcome") == "MISSED":
                return dict(old)
            result.update(
                outcome="MISSED",
                missed_at=now.isoformat(),
                blocking_evidence=old or {"reason": "no earlier operational observation"},
            )
        value = json.dumps(result, sort_keys=True)
        if self.repo.get_control(key) != value:
            self.repo.set_control(key, value)
            self.store.audit("calibration_status", result, now, self.settings.cohort_id)
        return result

    def _calibration_step(self, now: datetime) -> dict[str, Any]:
        budget = self.oms.session_budget()
        rows = [
            row
            for row in self.store.linked_orders(self.settings.cohort_id)
            if row["link"].get("entry_kind") == "calibration"
        ]
        if rows:
            submitted = any(
                row["broker_order_id"]
                or row["link"].get("submission_attempted_at")
                or row["link"].get("rejection")
                for row in rows
                if row["side"] == "buy"
            )
            if any(row["status"] not in FINAL for row in rows):
                state = "order_pending_or_unknown"
            elif self.oms.inventory(rows):
                state = "awaiting_risk_exit"
            elif any(Decimal(str(row["filled_quantity"])) > 0 for row in rows):
                state = "completed_round_trip"
            else:
                state = "completed_without_fill"
            if state == "completed_without_fill" and not submitted:
                return {
                    "state": "missed_window_no_trade",
                    "entry_attempts": 0,
                    "reserved_attempts": 1,
                    "reason": "reserved_but_never_submitted",
                    "trade_classification": "EQUIPMENT_TEST",
                    "equipment_test_id": budget["equipment_test_id"] if budget else None,
                }
            return {
                "state": state,
                "entry_attempts": 1,
                "qualification_eligible": False,
                "trade_classification": "EQUIPMENT_TEST",
                "equipment_test_id": budget["equipment_test_id"] if budget else None,
                "client_order_id": budget["equipment_client_order_id"] if budget else None,
                "operational_targets": self._equipment_targets(rows, now),
            }
        if budget and budget["equipment_client_order_id"] is not None:
            return {
                "state": "already_consumed_by_prior_cohort",
                "entry_attempts": 1,
                "trade_classification": "EQUIPMENT_TEST",
                "equipment_test_id": budget["equipment_test_id"],
                "client_order_id": budget["equipment_client_order_id"],
                "owning_cohort": budget["equipment_cohort_id"],
                "qualification_eligible": False,
            }
        if self.settings.mode != "experimental-paper":
            return {"state": "shadow_no_orders"}
        local_date = now.astimezone(ZoneInfo(self.app.intraday.timezone)).date()
        start = self.settings.practice_start_date
        if start is None or local_date < start:
            return {"state": "scheduled", "session_date": str(start)}
        gate = self.calendar.gate(now)
        if local_date > start or (
            gate.session_open and now >= gate.session_open + timedelta(minutes=30)
        ):
            return {"state": "missed_window_no_trade", "entry_attempts": 0}
        if not gate.can_enter or not self.broker.clock().is_open:
            return {"state": "waiting_for_regular_entry_window"}
        if self.context is None:
            return {"state": "blocked", "reasons": ["OFFICIAL_CONTEXT_REQUIRED"]}
        reasons = [*self.context.errors, *self.context.blocking_reasons(now=now)]
        if any(
            now - timedelta(minutes=self.policy.macro_blackout_minutes)
            <= event_at
            <= now
            + timedelta(
                minutes=self.settings.max_holding_minutes + self.policy.macro_blackout_minutes
            )
            for event_at in self.context.scheduled_macro_events
        ):
            reasons.append("SCHEDULED_MACRO_BLACKOUT")
        if self.context.halted_for("AAPL", now=now) is not False:
            reasons.append("HALT_STATUS_NOT_CLEAR")
        if not self._sources_fresh(now):
            reasons.append("SOURCE_NOT_FRESH")
        if "AAPL" not in self.market_states:
            reasons.append("MARKET_STATE_REQUIRED")
        opening_state = self.market_states.get("AAPL")
        bar = opening_state.completed_bar if opening_state else None
        received = self.first_bar_receipts.get(("AAPL", bar.timestamp)) if bar else None
        if (
            bar is None
            or gate.session_open is None
            or bar.timestamp < gate.session_open + timedelta(minutes=5)
            or bar.timestamp > now
            or received is None
            or received < bar.timestamp
            or opening_state is None
            or opening_state.pre_event_volatility_bps is None
            or opening_state.completed_daily_sessions is None
            or opening_state.completed_daily_sessions < self.policy.minimum_liquidity_sessions
        ):
            reasons.append("COMPLETED_OPENING_OBSERVATION_REQUIRED")
        elif now < received + timedelta(seconds=self.policy.processing_latency_seconds):
            reasons.append("NEXT_FRAME_PROCESSING_LATENCY")
        if reasons:
            return {
                "state": "blocked",
                "reasons": reasons,
                "observed": {
                    "completed_bar_at": bar.timestamp.isoformat() if bar else None,
                    "bar_received_at": received.isoformat() if received else None,
                    "completed_daily_sessions": opening_state.completed_daily_sessions
                    if opening_state
                    else None,
                    "required_completed_daily_sessions": self.policy.minimum_liquidity_sessions,
                    "sources_fresh": self._sources_fresh(now),
                    "evaluated_at": now.isoformat(),
                },
            }
        if self.cert is None or not self.cert.permits_paper or self.cert.expires_at <= now:
            self.operational_preflight(now)
        if self.cert is None or not self.cert.permits_paper:
            return {"state": "blocked", "reasons": ["OPERATIONAL_CERTIFICATE_REQUIRED"]}
        quote_state = self._refresh_quote("AAPL")
        evaluated_at = datetime.now(UTC)
        if received is None:
            raise ValueError("opening observation receipt required")
        eligible_at = received + timedelta(seconds=self.policy.processing_latency_seconds)
        result = self.oms.submit_entry(
            symbol="AAPL",
            cluster_key=f"opening-calibration:{local_date}:AAPL",
            decision_id=f"opening-calibration:{self.settings.cohort_id}",
            eligible_at=eligible_at,
            expires_at=evaluated_at + timedelta(seconds=30),
            bid=quote_state.bid,
            ask=quote_state.ask,
            quote_at=quote_state.quote_at,
            median_dollar_volume=quote_state.median_daily_dollar_volume or Decimal(0),
            source_valid=True,
            certificate=self.cert,
            now=evaluated_at,
            execution_quote=_execution_quote(quote_state),
            entry_kind="calibration",
            decision_ticket={
                "schema_version": "equipment-test-ticket-v1",
                "trade_classification": "EQUIPMENT_TEST",
                "equipment_test_id": budget["equipment_test_id"] if budget else None,
                "planned_session_date": str(self.settings.practice_start_date),
                "source": "explicit operator equipment test, not a news thesis",
                "completed_opening_bar": bar.model_dump(mode="json") if bar else None,
                "observation_received_at": received.isoformat(),
                "eligible_at": eligible_at.isoformat(),
                "expected_net_return_bps": None,
                "exit_rules": {"cancel_unfilled_after_seconds": 30, "liquidate_after_seconds": 60},
                "cost_assumptions": paper_cost_assumptions(),
            },
        )
        return {**result, "qualification_eligible": False, "purpose": "operator_calibration"}

    def _equipment_targets(self, rows: list[dict[str, Any]], now: datetime) -> dict[str, Any]:
        entry = next(row for row in rows if row["side"] == "buy")
        submitted = entry["link"].get("submission_attempted_at")
        if submitted is None:
            return {"state": "submission_time_unknown", "observed_at": now.isoformat()}
        submitted_at = datetime.fromisoformat(submitted)
        cancel_target = submitted_at + timedelta(seconds=30)
        exit_target = submitted_at + timedelta(seconds=60)
        cancel = entry["link"].get("cancel_requested_at")
        exit_fills = [
            datetime.fromisoformat(row["link"]["broker"]["filled_at"])
            for row in rows
            if row["side"] == "sell"
            and Decimal(str(row["filled_quantity"])) > 0
            and (row["link"].get("broker") or {}).get("filled_at")
        ]
        outstanding_remainder = entry["status"] not in FINAL and Decimal(
            str(entry["filled_quantity"])
        ) < Decimal(str(entry["quantity"]))
        owned = self.oms.inventory(rows)
        return {
            "observed_at": now.isoformat(),
            "original_submission_attempt_at": submitted_at.isoformat(),
            "cancel_remainder_target_at": cancel_target.isoformat(),
            "liquidation_target_at": exit_target.isoformat(),
            "cancel_requested_at": cancel,
            "cancel_request_lateness_seconds": max(
                0, (datetime.fromisoformat(cancel) - cancel_target).total_seconds()
            )
            if cancel
            else None,
            "overdue_unconfirmed_entry_remainder": outstanding_remainder and now > cancel_target,
            "overdue_owned_quantity": {symbol: str(quantity) for symbol, quantity in owned.items()}
            if now > exit_target
            else {},
            "last_broker_exit_fill_at": max(exit_fills).isoformat() if exit_fills else None,
            "last_exit_fill_lateness_seconds": max(
                0, (max(exit_fills) - exit_target).total_seconds()
            )
            if exit_fills
            else None,
            "completion_confirmed": not owned and all(row["status"] in FINAL for row in rows),
            "timing_limitations": (
                "Targets are not guarantees. Missing broker fill times remain unknown; "
                "cancel requests are not cancellation confirmation."
            ),
        }

    def _sources_fresh(self, now: datetime) -> bool:
        return (
            self.last_source_success is not None
            and timedelta(0) <= now - self.last_source_success <= timedelta(seconds=120)
            and not self.source.last_errors
        )


def _execution_quote(state: EventMarketState) -> EventQuote:
    return EventQuote(
        symbol=state.symbol,
        bid=state.bid,
        ask=state.ask,
        bid_size=state.bid_size,
        ask_size=state.ask_size,
        timestamp=state.quote_at,
        received_at=state.observed_at,
        feed=state.feed,
        size_unit=latest_rest_quote_size_unit(feed=state.feed, quote_at=state.quote_at),
    )


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
            order_stream = (
                AlpacaPaperTradeUpdatesStream(AlpacaPaperSettings.model_validate({}))
                if settings.mode == "experimental-paper" and not once
                else None
            )
            runtime = EventRuntime(
                store,
                settings,
                source,
                market,
                broker,
                instance_id=instance,
                code_sha=code_identity(),
                order_stream=order_stream,
            )
            stream_stop = asyncio.Event()
            stream_task = (
                asyncio.create_task(order_stream.run(stream_stop)) if order_stream else None
            )
            try:
                while True:
                    try:
                        if stream_task is not None and stream_task.done():
                            await stream_task
                            stream_task = None
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
                                "purpose": settings.purpose,
                                "qualification_eligible": settings.purpose != "iex-practice",
                                "code_sha": runtime.code_sha,
                                "blockers": [type(error).__name__],
                            },
                        )
                    else:
                        if once:
                            return result
                    await asyncio.sleep(settings.poll_seconds)
            finally:
                stream_stop.set()
                if stream_task is not None:
                    await stream_task
                market.close()
                runtime.context_client.close()
                repository.release_worker_lock("tradeagent-event-worker", instance)
