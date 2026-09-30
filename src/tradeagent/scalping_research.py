"""Read-only, causal signal research. No broker client, lease or artifact promotion."""

from __future__ import annotations

import heapq
import json
import math
import random
import statistics
import zlib
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import select

from tradeagent.persistence import Database, events
from tradeagent.scalping_config import ScalpingConfig, ScalpQuote
from tradeagent.scalping_market import NS, BookFeatureEngine, MarketEvent, datetime_ns
from tradeagent.scalping_store import canonical, scalping_market_batches, scalping_runs, utc

HORIZONS = (1, 5, 15, 30, 60, 300, 900)
FEATURES = (
    "volatility_5s_bps",
    "return_15s_bps",
    "return_5s_bps",
    "normalized_ofi_5s",
    "trade_count_5s",
    "trade_volume_5s",
    "source_event_id",
)


class ResearchPolicy(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    schema_version: Literal["signal-horizon-research-v1"] = "signal-horizon-research-v1"
    horizons: tuple[int, ...] = HORIZONS
    notional_usd: Decimal = Field(default=Decimal("10.25"), gt=0)
    arrival_delay_ms: int = Field(default=350, ge=0)
    maximum_quote_age_ms: int = Field(default=1000, gt=0, le=2000)
    price_cap_bps: Decimal = Field(default=Decimal("2"), ge=0)
    entry_fee_bps: Decimal = Field(default=Decimal("25"), ge=0, lt=10000)
    exit_fee_bps: Decimal = Field(default=Decimal("25"), ge=0, lt=10000)
    maximum_signals: int = Field(default=10000, ge=1)
    maximum_signal_bytes: int = Field(default=16 * 1024 * 1024, ge=1)
    maximum_tape_events: int = Field(default=10_000_000, ge=1)
    maximum_batch_bytes: int = Field(default=8 * 1024 * 1024, ge=1)

    @model_validator(mode="after")
    def finite_hypotheses(self) -> Self:
        if not self.horizons or tuple(sorted(set(self.horizons))) != self.horizons:
            raise ValueError("horizons must be positive, unique and increasing")
        if self.horizons[0] <= self.arrival_delay_ms / 1000 or self.horizons[-1] > 900:
            raise ValueError("research horizons must follow arrival and not exceed 900 seconds")
        return self


class ResearchSignal(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    event_id: str
    run_id: str
    decision_id: str
    symbol: str
    family: Literal["momentum", "reversion", "none"]
    selected_action: Literal["buy", "sell", "hold"]
    decision_at: AwareDatetime
    score: float
    quote: ScalpQuote
    features: dict[str, float | int | str | None]

    @model_validator(mode="after")
    def causal_quote(self) -> Self:
        if self.symbol != self.quote.symbol or self.quote.received_at > self.decision_at:
            raise ValueError("a research signal requires its already-received own-symbol quote")
        return self


class ResearchTick(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    event_id: str
    symbol: str
    received_at: AwareDatetime
    continuity_id: str
    quote: ScalpQuote | None = None

    @model_validator(mode="after")
    def already_received(self) -> Self:
        if self.quote is not None and (
            self.quote.symbol != self.symbol or self.quote.received_at > self.received_at
        ):
            raise ValueError("a research tick cannot contain future or wrong-symbol evidence")
        return self


def _feature(signal: ResearchSignal, name: str) -> float | None:
    value = signal.features.get(name)
    return (
        float(value)
        if isinstance(value, (float, int)) and not isinstance(value, bool) and math.isfinite(value)
        else None
    )


def regimes(signal: ResearchSignal) -> dict[str, str]:
    volatility = _feature(signal, "volatility_5s_bps")
    spread = float((signal.quote.ask / signal.quote.bid - 1) * 10000)
    trend = _feature(signal, "return_15s_bps")
    volume = _feature(signal, "trade_count_5s")
    return {
        "symbol": signal.symbol,
        "family": signal.family,
        "volatility": (
            "unknown"
            if volatility is None
            else "low_lt2"
            if volatility < 2
            else "medium_2to5"
            if volatility < 5
            else "high_ge5"
        ),
        "spread": "tight_le2" if spread <= 2 else "medium_2to5" if spread <= 5 else "wide_gt5",
        "utc_time_of_day": f"{signal.decision_at.astimezone(UTC).hour // 6 * 6:02d}-"
        f"{signal.decision_at.astimezone(UTC).hour // 6 * 6 + 6:02d}",
        "trend_strength": (
            "unknown"
            if trend is None
            else "nonpositive"
            if trend <= 0
            else "positive_le5"
            if trend <= 5
            else "positive_gt5"
        ),
        "volume_regime": (
            "unknown"
            if volume is None
            else "no_observed_trades"
            if volume == 0
            else "one_to_four"
            if volume < 5
            else "five_or_more"
        ),
    }


def hypothesis_two(signal: ResearchSignal) -> bool:
    volatility = _feature(signal, "volatility_5s_bps")
    trend = _feature(signal, "return_15s_bps")
    flow = _feature(signal, "normalized_ofi_5s")
    spread = float((signal.quote.ask / signal.quote.bid - 1) * 10000)
    return bool(
        signal.family == "momentum"
        and signal.selected_action != "sell"
        and signal.score >= 0.25
        and volatility is not None
        and volatility >= 5
        and trend is not None
        and trend > 5
        and flow is not None
        and flow > 0
        and spread <= 5
    )


def _fresh(tick: ResearchTick | None, at_ns: int, policy: ResearchPolicy) -> bool:
    if tick is None or tick.quote is None:
        return False
    quote = tick.quote
    return (
        0 <= at_ns - datetime_ns(tick.received_at) <= policy.maximum_quote_age_ms * 1_000_000
        and 0 <= at_ns - quote.exchange_time_ns <= policy.maximum_quote_age_ms * 1_000_000
        and 0 <= at_ns - datetime_ns(quote.received_at) <= policy.maximum_quote_age_ms * 1_000_000
        and quote.bid_size > 0
        and quote.ask_size > 0
    )


def analyze_signals(
    signals: Sequence[ResearchSignal],
    ticks: Iterable[ResearchTick],
    policy: ResearchPolicy,
    *,
    split_at: datetime,
    information_cutoff: datetime,
) -> dict[str, Any]:
    """Sample quotes received by each deadline, never the first favorable quote afterward."""
    if split_at.utcoffset() is None or information_cutoff.utcoffset() is None:
        raise ValueError("research split and information cutoff must be timezone-aware")
    if len(signals) > policy.maximum_signals:
        raise ValueError("signal budget exceeded; no truncated research population is allowed")
    if len({signal.event_id for signal in signals}) != len(signals) or len(
        {(signal.run_id, signal.decision_id) for signal in signals}
    ) != len(signals):
        raise ValueError("duplicate recorded signal event")
    deadline_heap: list[tuple[int, int, int]] = []
    entries: dict[int, tuple[ResearchTick | None, str | None]] = {}
    latest: dict[str, ResearchTick] = {}
    rows: list[dict[str, Any]] = []
    maximum_horizon = policy.horizons[-1]
    embargo_start = split_at - timedelta(seconds=maximum_horizon)
    for index, signal in enumerate(signals):
        decision_ns = datetime_ns(signal.decision_at)
        heapq.heappush(deadline_heap, (decision_ns + policy.arrival_delay_ms * 1_000_000, index, 0))
        for horizon in policy.horizons:
            heapq.heappush(deadline_heap, (decision_ns + horizon * NS, index, horizon))
    cutoff_ns = datetime_ns(information_cutoff)
    input_hash = sha256(canonical([signal.model_dump(mode="json") for signal in signals]).encode())

    def evaluate(at_ns: int, index: int, horizon: int) -> None:
        signal = signals[index]
        current = latest.get(signal.symbol)
        if horizon == 0:
            reason = None
            if signal.family == "none" or signal.selected_action == "sell":
                reason = "NOT_A_LONG_ENTRY_HYPOTHESIS"
            elif at_ns > cutoff_ns:
                reason = "ENTRY_AFTER_INFORMATION_CUTOFF"
            elif (
                datetime_ns(signal.decision_at) - signal.quote.exchange_time_ns
                > policy.maximum_quote_age_ms * 1_000_000
            ):
                reason = "DECISION_QUOTE_STALE"
            elif not _fresh(current, at_ns, policy):
                reason = "ENTRY_QUOTE_MISSING_OR_STALE"
            else:
                assert current is not None and current.quote is not None
                quantity = policy.notional_usd / current.quote.ask
                if current.quote.ask > signal.quote.ask * (1 + policy.price_cap_bps / 10000):
                    reason = "ARRIVAL_ASK_EXCEEDS_ORIGINAL_CAP"
                elif current.quote.ask_size < quantity:
                    reason = "INSUFFICIENT_OBSERVED_ENTRY_SIZE"
            entries[index] = (current, reason)
            return
        entry, entry_reason = entries[index]
        reasons = [entry_reason] if entry_reason else []
        if at_ns > cutoff_ns:
            reasons.append("HORIZON_AFTER_INFORMATION_CUTOFF")
        elif not _fresh(current, at_ns, policy):
            reasons.append("EXIT_QUOTE_MISSING_OR_STALE")
        elif entry is not None and current is not None:
            if entry.continuity_id != current.continuity_id:
                reasons.append("LOCAL_CONTINUITY_CHANGED")
            if current.quote is not None and entry.quote is not None:
                retained_quantity = (
                    policy.notional_usd / entry.quote.ask * (1 - policy.entry_fee_bps / 10000)
                )
                if current.quote.bid_size < retained_quantity:
                    reasons.append("INSUFFICIENT_OBSERVED_EXIT_SIZE")
        partition = (
            "discovery"
            if signal.decision_at < embargo_start
            else "later_diagnostic"
            if signal.decision_at >= split_at
            else "embargo"
        )
        gross = net = mid_return = None
        if (
            signal.family != "none"
            and signal.selected_action != "sell"
            and entry_reason
            not in {
                "DECISION_QUOTE_STALE",
                "ENTRY_AFTER_INFORMATION_CUTOFF",
                "NOT_A_LONG_ENTRY_HYPOTHESIS",
            }
            and at_ns <= cutoff_ns
            and _fresh(current, at_ns, policy)
            and entry is not None
            and current is not None
            and entry.continuity_id == current.continuity_id
        ):
            assert current.quote is not None
            mid_return = float(
                (
                    (current.quote.ask + current.quote.bid) / (signal.quote.ask + signal.quote.bid)
                    - 1
                )
                * 10000
            )
        if not reasons:
            assert entry is not None and entry.quote is not None
            assert current is not None and current.quote is not None
            price_ratio = current.quote.bid / entry.quote.ask
            gross = float((price_ratio - 1) * 10000)
            net = float(
                (
                    price_ratio
                    * (1 - policy.entry_fee_bps / 10000)
                    * (1 - policy.exit_fee_bps / 10000)
                    - 1
                )
                * 10000
            )
        rows.append(
            {
                "event_id": signal.event_id,
                "decision_id": signal.decision_id,
                "run_id": signal.run_id,
                "decision_at": signal.decision_at.astimezone(UTC).isoformat(),
                "symbol": signal.symbol,
                "family": signal.family,
                "selected_qualified_action": signal.selected_action,
                "predicted_direction": "long_hypothesis_not_qualified_order",
                "score": signal.score,
                "hypothesis_2_eligible": hypothesis_two(signal),
                "horizon_seconds": horizon,
                "partition": partition,
                "regimes": regimes(signal),
                "source_event_id": signal.features.get("source_event_id"),
                "entry_quote_event_id": entry.event_id if entry is not None else None,
                "exit_quote_event_id": current.event_id if current is not None else None,
                "entry_quote": (
                    entry.quote.model_dump(mode="json")
                    if entry is not None and entry.quote is not None
                    else None
                ),
                "exit_quote": (
                    current.quote.model_dump(mode="json")
                    if current is not None and current.quote is not None
                    else None
                ),
                "complete": not reasons,
                "missing_reasons": reasons,
                "mid_forward_return_bps": mid_return,
                "gross_executable_return_bps": gross,
                "net_executable_return_bps": net,
                "additional_fee_drag_bps": (
                    gross - net if gross is not None and net is not None else None
                ),
                "news_association": "unavailable_no_point_in_time_news_join",
            }
        )

    previous_ns = 0
    event_count = 0
    for tick in ticks:
        at_ns = datetime_ns(tick.received_at)
        if at_ns < previous_ns:
            raise ValueError("tape receive time reversed; cannot silently reorder causal evidence")
        previous_ns = at_ns
        if at_ns > cutoff_ns:
            break
        event_count += 1
        if event_count > policy.maximum_tape_events:
            raise ValueError("tape budget exceeded; no truncated research population is allowed")
        input_hash.update(canonical(tick.model_dump(mode="json")).encode())
        while deadline_heap and deadline_heap[0][0] < at_ns:
            evaluate(*heapq.heappop(deadline_heap))
        latest[tick.symbol] = tick
        # Same-time receipts are sampled before a deadline at that timestamp.
    while deadline_heap:
        evaluate(*heapq.heappop(deadline_heap))
    rows.sort(key=lambda row: (row["decision_at"], row["event_id"], row["horizon_seconds"]))
    return {
        "schema": policy.schema_version,
        "source_sha256": input_hash.hexdigest(),
        "policy": policy.model_dump(mode="json"),
        "signal_count": len(signals),
        "tape_observations": event_count,
        "split_at": split_at.isoformat(),
        "embargo_seconds": maximum_horizon,
        "information_cutoff": information_cutoff.isoformat(),
        "rows": rows,
        **summarize(rows),
        "finite_trial_budget": len(policy.horizons),
        "research_code_sha256": sha256(Path(__file__).read_bytes()).hexdigest(),
        "research_policy_sha256": sha256(
            canonical(policy.model_dump(mode="json")).encode()
        ).hexdigest(),
        "evidence_environment": "offline_replay",
        "return_basis": "hypothetical_fresh_arrival_ask_to_deadline_bid",
        "cost_basis": (
            "multiplicative base-inventory entry fee and cash exit fee; spread and quote "
            "slippage embedded in executable prices, not deducted again"
        ),
        "limitations": [
            "known September data is exploratory, including the later diagnostic split",
            "repeated overlapping signals are not independent trades or portfolio returns",
            "displayed-size markouts are not broker fills or a validated queue model",
            "neutral no-signal observations were aggregated by the recorder, not reconstructed",
            "news effects and unrecorded fees/latency/impact are not invented",
            "no model artifact, broker orders, lease, or promotion",
        ],
    }


def _correlation(x: Sequence[float], y: Sequence[float]) -> float | None:
    if len(x) < 3:
        return None
    mx, my = statistics.mean(x), statistics.mean(y)
    xx = sum((v - mx) ** 2 for v in x)
    yy = sum((v - my) ** 2 for v in y)
    return (
        sum((a - mx) * (b - my) for a, b in zip(x, y, strict=True)) / math.sqrt(xx * yy)
        if xx > 0 and yy > 0
        else None
    )


def _ranks(values: Sequence[float]) -> list[float]:
    ordered = sorted(range(len(values)), key=lambda i: values[i])
    result = [0.0] * len(values)
    offset = 0
    while offset < len(ordered):
        end = offset + 1
        while end < len(ordered) and values[ordered[end]] == values[ordered[offset]]:
            end += 1
        for index in ordered[offset:end]:
            result[index] = (offset + end - 1) / 2
        offset = end
    return result


def _statistics(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    observed = [row for row in rows if row["complete"]]
    values = [float(row["net_executable_return_bps"]) for row in observed]
    gross = [float(row["gross_executable_return_bps"]) for row in observed]
    prediction_rows = [row for row in rows if row["mid_forward_return_bps"] is not None]
    mid = [float(row["mid_forward_return_bps"]) for row in prediction_rows]
    scores = [float(row["score"]) for row in prediction_rows]
    winners = [value for value in values if value > 0]
    losers = [value for value in values if value < 0]
    std = statistics.stdev(values) if len(values) > 1 else None
    daily: dict[str, list[float]] = defaultdict(list)
    for row, value in zip(observed, values, strict=True):
        daily[str(row["decision_at"])[:10]].append(value)
    ci = None
    if len(daily) >= 5:
        rng = random.Random(20260929)
        blocks = [(sum(day), len(day)) for day in daily.values()]
        means = []
        for _ in range(1000):
            chosen = [rng.choice(blocks) for _ in blocks]
            means.append(sum(pair[0] for pair in chosen) / sum(pair[1] for pair in chosen))
        means.sort()
        ci = [means[24], means[974]]
    sorted_values = sorted(values)
    return {
        "attempts": len(rows),
        "complete": len(values),
        "coverage_fraction": len(values) / len(rows) if rows else None,
        "missing_reasons": dict(
            sorted(Counter(reason for row in rows for reason in row["missing_reasons"]).items())
        ),
        "win_rate_after_fees": len(winners) / len(values) if values else None,
        "mean_net_bps": statistics.mean(values) if values else None,
        "mean_gross_bps": statistics.mean(gross) if gross else None,
        "mean_mid_forward_bps": statistics.mean(mid) if mid else None,
        "directional_observations": len(prediction_rows),
        "mean_winner_bps": statistics.mean(winners) if winners else None,
        "mean_loser_bps": statistics.mean(losers) if losers else None,
        "p05_net_bps": sorted_values[int((len(values) - 1) * 0.05)] if values else None,
        "nonannualized_mean_over_std": (
            statistics.mean(values) / std if std is not None and std > 0 else None
        ),
        "ratio_is_not_portfolio_sharpe": True,
        "directional_accuracy_long": sum(value > 0 for value in mid) / len(mid) if mid else None,
        "pearson_score_mid_return": _correlation(scores, mid),
        "spearman_score_mid_return": _correlation(_ranks(scores), _ranks(mid)),
        "utc_day_blocks": len(daily),
        "day_block_bootstrap_95_mean_net_bps": ci,
        "interval_is_exploratory_not_selection_corrected": True,
    }


def summarize(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    groups: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    buckets: dict[tuple[str, int, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        partition, horizon = str(row["partition"]), int(row["horizon_seconds"])
        groups[(partition, horizon)].append(row)
        for dimension, label in row["regimes"].items():
            buckets[(partition, horizon, dimension, label)].append(row)
    top = []
    for (partition, horizon), selected in sorted(groups.items()):
        if partition != "discovery":
            continue
        complete = [row for row in selected if row["complete"]]
        complete.sort(key=lambda row: (-row["net_executable_return_bps"], row["event_id"]))
        best = complete[: math.ceil(len(complete) / 10)]
        top.append(
            {
                "horizon_seconds": horizon,
                "selected_count": len(best),
                "positive_count": sum(row["net_executable_return_bps"] > 0 for row in best),
                "event_ids": [row["event_id"] for row in best],
                "regime_counts": {
                    dimension: dict(Counter(row["regimes"][dimension] for row in best))
                    for dimension in (
                        "symbol",
                        "volatility",
                        "spread",
                        "utc_time_of_day",
                        "trend_strength",
                        "volume_regime",
                    )
                },
                "hindsight_only_not_a_rule_or_holdout": True,
            }
        )
    pairs: dict[str, dict[int, dict[str, Any]]] = defaultdict(dict)
    for row in rows:
        pairs[row["event_id"]][row["horizon_seconds"]] = row
    paired = []
    for horizon in (30, 60, 300, 900):
        for partition in ("discovery", "later_diagnostic"):
            comparable = [
                cell
                for cell in pairs.values()
                if 5 in cell
                and horizon in cell
                and cell[5]["complete"]
                and cell[horizon]["complete"]
                and cell[5]["partition"] == partition
            ]
            improvements = [
                cell[horizon]["net_executable_return_bps"] - cell[5]["net_executable_return_bps"]
                for cell in comparable
            ]
            paired.append(
                {
                    "partition": partition,
                    "horizon_seconds": horizon,
                    "paired_complete_signals": len(comparable),
                    "mean_net_improvement_vs_5s_bps": (
                        statistics.mean(improvements) if improvements else None
                    ),
                }
            )
    return {
        "horizon_results": [
            {"partition": partition, "horizon_seconds": horizon, **_statistics(selected)}
            for (partition, horizon), selected in sorted(groups.items())
        ],
        "regime_results": [
            {
                "partition": p,
                "horizon_seconds": h,
                "dimension": d,
                "bucket": b,
                **_statistics(selected),
            }
            for (p, h, d, b), selected in sorted(buckets.items())
        ],
        "discovery_top_decile": top,
        "paired_horizon_comparison": paired,
        "finite_trial_budget": len({row["horizon_seconds"] for row in rows}),
        "exploratory_regime_comparisons": len(buckets),
        "hypothesis_2_results": [
            {
                "partition": p,
                "horizon_seconds": h,
                **_statistics([row for row in selected if row["hypothesis_2_eligible"]]),
            }
            for (p, h), selected in sorted(groups.items())
            if h in (5, 60)
        ],
        "promotion_allowed": False,
    }


def research_database(
    database: Database,
    *,
    cohort_id: str,
    start: datetime,
    end: datetime,
    split_at: datetime,
    information_cutoff: datetime,
    output_dir: Path,
    policy: ResearchPolicy,
) -> dict[str, Any]:
    """Stream all recorded candidate decisions and hashed tape for an explicit frozen window."""
    if any(value.utcoffset() is None for value in (start, end, split_at, information_cutoff)):
        raise ValueError("research boundaries must be timezone-aware")
    if not start < split_at < end <= information_cutoff:
        raise ValueError("invalid chronological research boundaries")
    if output_dir.exists():
        raise ValueError("refusing to overwrite a research artifact")
    with database.begin() as connection:
        runs = list(
            connection.execute(
                select(scalping_runs).where(scalping_runs.c.cohort_id == cohort_id)
            ).mappings()
        )
        if not runs or len({row["config_hash"] for row in runs}) != 1:
            raise ValueError("a single frozen configuration is required for research")
        config = ScalpingConfig.model_validate(runs[0]["config"])
        if (
            policy.entry_fee_bps < max(config.maker_fee_bps, config.taker_fee_bps)
            or policy.exit_fee_bps < config.taker_fee_bps
        ):
            raise ValueError("research fees cannot underprice the frozen account cost policy")
        signals: list[ResearchSignal] = []
        input_bytes = 0
        raw_signal = events.c.payload["signal"]
        statement = (
            select(
                events.c.event_id,
                events.c.occurred_at,
                events.c.recorded_at,
                events.c.payload["run_id"].as_string().label("run_id"),
                raw_signal["decision_id"].as_string().label("decision_id"),
                raw_signal["symbol"].as_string().label("symbol"),
                raw_signal["family"].as_string().label("family"),
                raw_signal["action"].as_string().label("selected_action"),
                raw_signal["score"].as_float().label("score"),
                raw_signal["quote"].label("quote"),
                *(raw_signal["features"][key].label(key) for key in FEATURES),
            )
            .where(
                events.c.event_type == "scalp_candidate_decision",
                events.c.payload["run_id"].as_string().in_([row["run_id"] for row in runs]),
                events.c.payload["account_digest"].as_string() == config.account_digest,
                events.c.occurred_at >= start,
                events.c.occurred_at < end,
                events.c.recorded_at <= information_cutoff,
            )
            .order_by(events.c.occurred_at, events.c.event_id)
            .execution_options(stream_results=True, yield_per=100)
        )
        for row in connection.execute(statement).mappings():
            compact = {
                key: row[key]
                for key in (
                    "event_id",
                    "run_id",
                    "decision_id",
                    "symbol",
                    "family",
                    "selected_action",
                    "score",
                    "quote",
                )
            }
            compact["decision_at"] = max(utc(row["occurred_at"]), utc(row["recorded_at"]))
            compact["features"] = {key: row[key] for key in FEATURES}
            input_bytes += len(canonical(compact).encode())
            if input_bytes > policy.maximum_signal_bytes or len(signals) >= policy.maximum_signals:
                raise ValueError("signal budget exceeded; no truncated research is allowed")
            signals.append(ResearchSignal.model_validate(compact))
    engine = BookFeatureEngine(config, stale_after_seconds=policy.maximum_quote_age_ms / 1000)
    tape_manifest_hash = sha256()
    tape_batch_count = 0
    tape_event_count = 0

    def ticks() -> Iterator[ResearchTick]:
        nonlocal tape_batch_count, tape_event_count
        previous_connection = ""
        previous_sequence = 0
        continuity = 0
        count = 0
        tape_end = min(end + timedelta(seconds=max(policy.horizons) + 60), information_cutoff)
        with database.begin() as connection:
            query = (
                select(scalping_market_batches)
                .where(
                    scalping_market_batches.c.recorded_at >= start - timedelta(minutes=1),
                    scalping_market_batches.c.recorded_at <= tape_end,
                )
                .order_by(scalping_market_batches.c.recorded_at, scalping_market_batches.c.batch_id)
                .execution_options(stream_results=True, yield_per=8)
            )
            for batch in connection.execute(query).mappings():
                decoder = zlib.decompressobj()
                body = decoder.decompress(batch["raw"], policy.maximum_batch_bytes + 1)
                if len(body) > policy.maximum_batch_bytes or not decoder.eof:
                    raise ValueError("market batch exceeds decompressed input budget")
                if (
                    batch["encoding"] != "zlib-json-v1"
                    or sha256(body).hexdigest() != batch["batch_id"]
                ):
                    raise ValueError("market batch integrity mismatch")
                items = json.loads(body)
                if len(items) != batch["event_count"]:
                    raise ValueError("market batch count mismatch")
                tape_manifest_hash.update(
                    canonical(
                        {
                            "batch_id": batch["batch_id"],
                            "event_count": batch["event_count"],
                            "recorded_at": utc(batch["recorded_at"]).isoformat(),
                        }
                    ).encode()
                )
                tape_batch_count += 1
                for item in items:
                    event = MarketEvent.model_validate(item)
                    count += 1
                    tape_event_count += 1
                    if count > policy.maximum_tape_events:
                        raise ValueError("tape budget exceeded; no truncated research is allowed")
                    connection_id = str(event.connection_id)
                    gap = (
                        connection_id != previous_connection
                        or event.receive_sequence != previous_sequence + 1
                        or event.event_type == "reset"
                        or event.reset
                    )
                    if gap:
                        continuity += 1
                        for symbol in config.symbols:
                            yield ResearchTick(
                                event_id=event.event_id,
                                symbol=symbol,
                                received_at=event.received_at,
                                continuity_id=f"{connection_id}:{continuity}",
                            )
                    previous_connection = connection_id
                    previous_sequence = event.receive_sequence
                    accepted = engine.on_event(event)
                    if accepted and event.event_type == "trade":
                        continue
                    quote = engine.quote(event.symbol)
                    yield ResearchTick(
                        event_id=event.event_id,
                        symbol=event.symbol,
                        received_at=event.received_at,
                        continuity_id=f"{connection_id}:{continuity}",
                        quote=quote if accepted else None,
                    )

    result = analyze_signals(
        signals, ticks(), policy, split_at=split_at, information_cutoff=information_cutoff
    )
    result["manifest"] = {
        "cohort_id": cohort_id,
        "account_digest": config.account_digest,
        "run_ids": sorted(row["run_id"] for row in runs),
        "source_code_shas": sorted({row["code_sha"] for row in runs}),
        "config_hash": runs[0]["config_hash"],
        "start": start.isoformat(),
        "end": end.isoformat(),
        "compact_signal_bytes": input_bytes,
        "market_batch_count": tape_batch_count,
        "market_event_count": tape_event_count,
        "market_manifest_sha256": tape_manifest_hash.hexdigest(),
        "decision_clock": "later of journal occurrence and persisted information receipt",
        "broker_called": False,
        "database_modified": False,
        "worker_config_changed": False,
    }
    return write_research(result, output_dir)


def write_research(result: dict[str, Any], output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=False)
    report = {key: value for key, value in result.items() if key != "rows"}
    digest = sha256()
    with (output_dir / "markouts.jsonl").open("x", encoding="utf-8") as destination:
        for row in result["rows"]:
            encoded = canonical(row) + "\n"
            digest.update(encoded.encode())
            destination.write(encoded)
    report["markouts_sha256"] = digest.hexdigest()
    (output_dir / "report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8"
    )
    return report


def research_files(
    signals_path: Path,
    ticks_path: Path,
    *,
    start: datetime,
    end: datetime,
    split_at: datetime,
    information_cutoff: datetime,
    output_dir: Path,
    policy: ResearchPolicy,
) -> dict[str, Any]:
    if any(value.utcoffset() is None for value in (start, end, split_at, information_cutoff)):
        raise ValueError("research boundaries must be timezone-aware")
    if not start < split_at < end <= information_cutoff:
        raise ValueError("invalid chronological research boundaries")
    if output_dir.exists():
        raise ValueError("refusing to overwrite a research artifact")
    signals: list[ResearchSignal] = []
    input_bytes = 0
    with signals_path.open(encoding="utf-8") as source:
        for line in source:
            if not line.strip():
                continue
            input_bytes += len(line.encode())
            if input_bytes > policy.maximum_signal_bytes or len(signals) >= policy.maximum_signals:
                raise ValueError("signal budget exceeded; no truncated research is allowed")
            signal = ResearchSignal.model_validate_json(line)
            if not start <= signal.decision_at < end:
                raise ValueError("signal outside the declared input population window")
            signals.append(signal)

    def ticks() -> Iterator[ResearchTick]:
        with ticks_path.open(encoding="utf-8") as source:
            for line in source:
                if line.strip():
                    yield ResearchTick.model_validate_json(line)

    result = analyze_signals(
        signals, ticks(), policy, split_at=split_at, information_cutoff=information_cutoff
    )
    result["manifest"] = {
        "input_mode": "frozen_jsonl",
        "start": start.isoformat(),
        "end": end.isoformat(),
        "compact_signal_bytes": input_bytes,
        "broker_called": False,
        "database_modified": False,
        "worker_config_changed": False,
    }
    return write_research(result, output_dir)
