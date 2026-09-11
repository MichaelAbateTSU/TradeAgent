from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import func, inspect, select

from tradeagent.email_schedule import DAILY_SUMMARY_FORMAT, DailyEmailPolicy, DailyStatusSettings
from tradeagent.event_store import event_order_links
from tradeagent.persistence import Database, ProductionRepository, events, position_cycles
from tradeagent.scalping_reporting import PROFILE, scalping_status
from tradeagent.scalping_store import scalping_cycles, scalping_runs


@dataclass
class DailyTradeFacts:
    completed: int = 0
    no_fill: int = 0
    no_fill_available: bool = False
    symbols: Counter[str] = field(default_factory=Counter)
    families: Counter[str] = field(default_factory=Counter)
    actual_net: Decimal = Decimal(0)
    combined_net: Decimal = Decimal(0)
    actual_known: int = 0
    fees_pending: int = 0
    wins: int = 0
    losses: int = 0
    unchanged: int = 0
    unpriced: int = 0
    holding_seconds: float = 0
    holding_samples: int = 0
    longest_seconds: float = 0
    interruptions: int = 0
    rate_limits: int = 0
    available: bool = True


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _money(value: Decimal) -> str:
    absolute = abs(value)
    return "less than one cent" if 0 < absolute < Decimal(".01") else f"${absolute:,.2f}"


def _outcome(value: Decimal) -> str:
    if value > 0:
        return f"a profit of {_money(value)}"
    if value < 0:
        return f"a loss of {_money(value)}"
    return "a break-even result"


def _name(symbol: str) -> str:
    return {"BTC/USD": "Bitcoin", "ETH/USD": "Ether"}.get(symbol, symbol)


def _plural(count: int, singular: str) -> str:
    return singular if count == 1 else singular + "s"


def _duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f} seconds"
    if seconds < 3600:
        return f"{seconds / 60:.1f} minutes"
    return f"{seconds / 3600:.1f} hours"


def _clock_label(value: datetime) -> str:
    return value.strftime("%I:%M %p").lstrip("0")


def _record_trade(
    facts: DailyTradeFacts, row: Any, *, actual: object, modeled: object, pending: bool
) -> None:
    facts.completed += 1
    facts.symbols[str(row["symbol"])] += 1
    if row.get("family"):
        facts.families[str(row["family"])] += 1
    facts.fees_pending += pending
    actual_value = Decimal(str(actual)) if actual is not None else None
    modeled_value = Decimal(str(modeled)) if modeled is not None else None
    if actual_value is not None and not pending:
        facts.actual_net += actual_value
        facts.actual_known += 1
    value = actual_value if actual_value is not None and not pending else modeled_value
    if value is None:
        facts.unpriced += 1
    else:
        facts.combined_net += value
        if value > 0:
            facts.wins += 1
        elif value < 0:
            facts.losses += 1
        else:
            facts.unchanged += 1
    opened, closed = row.get("opened_at"), row.get("closed_at")
    if opened is not None and closed is not None:
        duration = (_utc(closed) - _utc(opened)).total_seconds()
        if duration >= 0:
            facts.holding_seconds += duration
            facts.holding_samples += 1
            facts.longest_seconds = max(facts.longest_seconds, duration)


def compact_legacy_trades(report: dict[str, Any]) -> list[dict[str, Any]]:
    """Reuse the immutable report's ledgers, not its duplicated strategy views."""
    trades = []
    seen: set[str] = set()
    for version in (report, *report.get("prior_cohort_activity", {}).get("versions", [])):
        ledgers = version.get("all_execution_economics", {}).get("separate_ledgers", {})
        for ledger in ledgers.values():
            for trade in ledger.get("trades", []):
                if trade["state"] != "closed" or trade["trade_id"] in seen:
                    continue
                seen.add(trade["trade_id"])
                trades.append(
                    {
                        "symbol": trade["symbol"],
                        "opened_at": trade.get("entry_at"),
                        "closed_at": trade.get("exit_at"),
                        "modeled_net_pnl": trade.get("economic_paper_pnl"),
                    }
                )
    return trades


def _read_facts(
    database: Database,
    start: datetime,
    end: datetime,
    status: dict[str, Any],
    legacy_trades: list[dict[str, Any]] | None,
) -> DailyTradeFacts:
    facts = DailyTradeFacts()
    account = status.get("execution", {}).get("account_digest")
    is_scalping = status.get("profile") == PROFILE
    if is_scalping and (not account or not inspect(database.engine).has_table("scalping_cycles")):
        facts.available = False
        return facts
    with database.begin() as connection:
        if is_scalping:
            facts.no_fill_available = True
            scope = (
                scalping_cycles.c.account_digest == account,
                scalping_cycles.c.closed_at >= start,
                scalping_cycles.c.closed_at < end,
            )
            rows = connection.execute(
                select(
                    scalping_cycles.c.symbol,
                    scalping_cycles.c.opened_at,
                    scalping_cycles.c.closed_at,
                    scalping_cycles.c.actual_net_pnl,
                    scalping_cycles.c.modeled_net_pnl,
                    scalping_cycles.c.fees_pending,
                    scalping_cycles.c.payload["signal"]["family"].as_string().label("family"),
                )
                .where(
                    *scope,
                    scalping_cycles.c.state == "closed_owned_flat",
                    scalping_cycles.c.entry_quantity > 0,
                    scalping_cycles.c.exit_quantity > 0,
                )
                .execution_options(stream_results=True, yield_per=200)
            )
            for row in rows.mappings():
                _record_trade(
                    facts,
                    row,
                    actual=row["actual_net_pnl"],
                    modeled=row["modeled_net_pnl"],
                    pending=bool(row["fees_pending"]),
                )
            facts.no_fill = int(
                connection.scalar(
                    select(func.count())
                    .select_from(scalping_cycles)
                    .where(*scope, scalping_cycles.c.state == "no_fill")
                )
                or 0
            )
            run_ids = select(scalping_runs.c.run_id).where(
                scalping_runs.c.account_digest == account
            )
            for interruption in connection.execute(
                select(events.c.payload["status_code"].as_integer().label("status_code")).where(
                    events.c.event_type == "scalp_broker_backoff",
                    events.c.trace_id.in_(run_ids),
                    events.c.occurred_at >= start,
                    events.c.occurred_at < end,
                )
            ):
                if interruption.status_code == 429:
                    facts.rate_limits += 1
                elif interruption.status_code != 422:
                    facts.interruptions += 1
        else:
            if legacy_trades is None and connection.scalar(
                select(event_order_links.c.client_order_id).limit(1)
            ):
                facts.available = False
            for trade in legacy_trades or []:
                if not trade["closed_at"]:
                    facts.available = False
                    continue
                closed = _utc(datetime.fromisoformat(trade["closed_at"]))
                if not start <= closed < end:
                    continue
                opened = (
                    _utc(datetime.fromisoformat(trade["opened_at"])) if trade["opened_at"] else None
                )
                _record_trade(
                    facts,
                    {**trade, "opened_at": opened, "closed_at": closed},
                    actual=None,
                    modeled=trade["modeled_net_pnl"],
                    pending=True,
                )
            rows = connection.execute(
                select(position_cycles)
                .where(
                    position_cycles.c.status == "reconciled",
                    position_cycles.c.closed_at >= start,
                    position_cycles.c.closed_at < end,
                )
                .execution_options(stream_results=True, yield_per=200)
            )
            for row in rows.mappings():
                _record_trade(
                    facts,
                    row,
                    actual=row["realized_pnl"],
                    modeled=row["realized_pnl"],
                    pending=False,
                )
    return facts


def build_daily_email_summary(
    database: Database,
    now: datetime,
    settings: DailyStatusSettings,
    *,
    legacy_trades: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    policy = DailyEmailPolicy(settings)
    start, end = policy.reporting_window(now)
    local = policy.local_time(now)
    status = scalping_status(database, now=now)
    if status.get("state") == "not_started":
        worker = ProductionRepository(database).latest_heartbeat("tradeagent-event-worker")
        if worker:
            status = {
                **status,
                "cohort_id": worker[2].get("cohort_id"),
                "reported_state": worker[2].get("state"),
            }
    facts = _read_facts(database, start, end, status, legacy_trades)
    local_start, local_end = policy.local_time(start), policy.local_time(end)
    period = (
        f"from {local_start.strftime('%A')} at {_clock_label(local_start)} "
        f"to {local_end.strftime('%A')} at {_clock_label(local_end)}"
    )
    if not facts.available:
        opening = (
            f"Here is your paper-trading summary for {local.strftime('%A, %B %d')}. "
            f"This update covers the reporting day {period}, including overnight activity. "
            "Some trade records are unavailable, so I cannot give a reliable trade count "
            "or treat missing information as no activity."
        )
    elif facts.completed:
        opening = (
            f"Here is your paper-trading summary for {local.strftime('%A, %B %d')}. "
            f"This update covers the reporting day {period}, including overnight activity. "
            f"The agent completed {facts.completed:,} {_plural(facts.completed, 'trade')}, "
            "meaning positions that were bought and then sold. These were paper trades, "
            "so no real investment money was used."
        )
    else:
        opening = (
            f"Here is your paper-trading summary for {local.strftime('%A, %B %d')}. "
            f"This update covers the reporting day {period}, including overnight activity. "
            "No completed buy-and-sell trades were recorded in that period. That does not "
            "necessarily mean there were no attempted entries or positions still being managed."
        )
    assets = ", ".join(
        f"{_name(symbol)} ({count:,} completed {_plural(count, 'trade')})"
        for symbol, count in facts.symbols.most_common()
    )
    activity = (
        f"Trading activity was spread across {assets}. "
        if assets
        else "There is no completed-trade breakdown to report for the instruments being watched. "
    )
    if facts.families.get("momentum"):
        activity += (
            "Momentum entries looked for short bursts of upward price and buying pressure, "
            "rather than making long-term investments. "
        )
    if facts.families.get("reversion"):
        activity += "Some entries instead looked for a brief recovery after a pullback. "
    activity += (
        f"Another {facts.no_fill:,} entry {_plural(facts.no_fill, 'attempt')} "
        "ended without a fill, so they did not become completed positions. "
        if facts.no_fill
        else "There were no recorded no-fill entry attempts in this reporting period. "
        if facts.no_fill_available
        else "A reliable count of attempts that ended without a fill is unavailable. "
    )
    activity += (
        "The individual order numbers and technical logs stay in the dashboard, not this email."
    )
    if not facts.available:
        activity = (
            "The instrument and entry-outcome breakdown is incomplete in this snapshot. "
            "I cannot reliably separate completed trades from attempts that never filled. "
            "The detailed records can be investigated in the dashboard; missing information "
            "is not being presented as successful trading activity."
        )
    if not facts.available or facts.unpriced:
        finances = (
            "A complete profit or loss figure is not available from the current records. "
            "Some trade values or costs are still missing, so reporting a partial total as "
            "the result for the whole day would be misleading. Open positions are also "
            "separate from the results of completed trades."
        )
    elif not facts.completed:
        finances = (
            "There is no realized profit or loss to report from completed trades in this window. "
            "Any position that has not yet been sold is excluded from that statement. "
            "This is not a claim that the account has no exposure or that a trading strategy "
            "has proved profitable."
        )
    elif facts.actual_known == facts.completed:
        finances = (
            f"The completed trades produced {_outcome(facts.actual_net)} after the costs recorded "
            "for those trades. This figure covers this reporting period rather than the lifetime "
            "of the agent. It excludes positions still open at the end of the period, and "
            "a paper result does not guarantee the same result with real money."
        )
    else:
        pending_results = facts.completed - facts.actual_known
        finances = (
            f"The conservative estimate for the completed trades is {_outcome(facts.combined_net)} "
            "after estimated trading costs. "
            f"Costs for {pending_results:,} {_plural(pending_results, 'trade')} still need "
            "to be fully matched to their records, so the exact final net result is not confirmed. "
            "This estimate is neither guaranteed profit nor the value of positions still open."
        )
    if facts.completed:
        basis = (
            "recorded results"
            if facts.actual_known == facts.completed
            else "current cost estimates"
        )
        performance = (
            f"Using the {basis}, {facts.wins:,} trades finished ahead, {facts.losses:,} finished "
            f"behind and {facts.unchanged:,} were approximately unchanged. "
        )
        if facts.holding_samples:
            performance += (
                f"The average completed position was held for "
                f"{_duration(facts.holding_seconds / facts.holding_samples)}, and the longest "
                f"lasted {_duration(facts.longest_seconds)}. "
            )
        performance += (
            "Small price moves can be outweighed by fees and the difference between buying "
            "and selling prices, so the number of winning trades alone is not enough "
            "to judge success."
        )
        if facts.unpriced:
            performance += (
                f" A further {facts.unpriced:,} completed trades lack a usable valuation."
            )
    else:
        performance = (
            "With no completed trades, there is no meaningful win rate or average holding "
            "time to interpret. I will not turn an empty sample into a performance claim. "
            "The useful distinction today is between watching for opportunities, trying "
            "an entry and actually completing a trade."
        )
    if not facts.available:
        performance = (
            "The reporting gap also prevents a reliable comparison of winning and losing "
            "trades or their holding times. I am leaving those conclusions open rather than "
            "estimating them from incomplete records. This message therefore describes the "
            "limits of the available evidence, not a performance verdict."
        )
    execution = status.get("execution", {})
    inventory = execution.get("inventory")
    unresolved_orders = execution.get("unresolved_order_count")
    unresolved_ownership = execution.get("unresolved_ownership_count", 0)
    if (
        status.get("active")
        and isinstance(inventory, dict)
        and isinstance(unresolved_orders, int)
        and isinstance(unresolved_ownership, int)
    ):
        open_count = len(inventory)
        pending = unresolved_orders + unresolved_ownership
        operation = (
            "new entries were paused"
            if status.get("operator_stop")
            else "the agent was waiting for current market data"
            if status.get("state") == "waiting_for_market_data"
            else "the agent was running automatically"
            if status.get("state") == "running"
            else "the worker was reporting an operational problem"
        )
        ending = (
            "At the latest recorded update, "
            + operation
            + f" and it was managing {open_count:,} {_plural(open_count, 'position')}, "
            f"with {pending:,} unresolved order or ownership {_plural(pending, 'issue')}. "
        )
    else:
        ending = (
            "The current position and order status is unavailable or stale, so I cannot confirm "
            "present positions from this report. The trade totals above are recorded outcomes, "
            "not a substitute for a fresh account update. "
        )
    if facts.rate_limits or facts.interruptions:
        ending += (
            f"The broker delayed {facts.rate_limits + facts.interruptions:,} requests during the "
            "period; those interruptions can make entries or exits take longer than intended. "
        )
    ending += (
        "Trading records continue to be kept automatically. Your next email will be the "
        "next daily summary at 6 p.m. Eastern, rather than an email for each trade."
    )
    paragraphs = [opening, activity, finances, performance, ending]
    text = "\n\n".join(" ".join(paragraph.split()) for paragraph in paragraphs)
    payload = {
        "subject": f"Your paper-trading day in plain English - {local.strftime('%B %d, %Y')}",
        "text": text,
        "summary_format": DAILY_SUMMARY_FORMAT,
        "cohort_id": status.get("cohort_id"),
        "local_date": local.date().isoformat(),
        "timezone": settings.timezone,
        "period_start": start.isoformat(),
        "period_end": end.isoformat(),
        "period_basis": "consecutive reporting days ending at the configured daily email time",
        "completed_round_trips": facts.completed if facts.available else None,
        "fees_pending_cycles": facts.fees_pending,
        "financial_result_final": facts.completed > 0 and facts.actual_known == facts.completed,
        "qualification_eligible": False,
        "live_execution_available": False,
        "profile": status.get("profile"),
        "snapshot": status,
    }
    if not policy.validates_payload(payload, now):
        raise ValueError("daily summary must contain exactly five complete plain-text paragraphs")
    return payload
