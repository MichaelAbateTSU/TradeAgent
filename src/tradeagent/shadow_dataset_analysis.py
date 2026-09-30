"""Quality-first alpha screening. This module cannot submit orders or promote artifacts."""

from __future__ import annotations

import json
import random
import statistics
import zlib
from collections import defaultdict
from datetime import datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any

from sqlalchemy import select

from tradeagent.persistence import Database
from tradeagent.scalping_store import canonical, scalping_market_batches, utc
from tradeagent.shadow_dataset import (
    DATASET_ID,
    ShadowDatasetProtocol,
    dataset_status,
    shadow_datasets,
    shadow_evaluations,
    shadow_labels,
    shadow_source_links,
)


def verify_source_manifest(database: Database, dataset_id: str = DATASET_ID) -> dict[str, Any]:
    with database.begin() as connection:
        dataset = (
            connection.execute(
                select(shadow_datasets).where(shadow_datasets.c.dataset_id == dataset_id)
            )
            .mappings()
            .one()
        )
        required: set[str] = set()
        evaluation_refs = (
            select(shadow_evaluations.c.payload["decision_observation"]["event_id"].as_string())
            .where(shadow_evaluations.c.dataset_id == dataset_id)
            .execution_options(stream_results=True, yield_per=100)
        )
        for identity in connection.scalars(evaluation_refs):
            if identity:
                required.add(identity)
        label_refs = (
            select(
                shadow_labels.c.payload["exit_observation"]["event_id"].as_string(),
                shadow_labels.c.payload["latency_stress"]["1"]["entry_observation"][
                    "event_id"
                ].as_string(),
                shadow_labels.c.payload["latency_stress"]["3.5"]["entry_observation"][
                    "event_id"
                ].as_string(),
            )
            .join(
                shadow_evaluations,
                shadow_evaluations.c.evaluation_id == shadow_labels.c.evaluation_id,
            )
            .where(shadow_evaluations.c.dataset_id == dataset_id)
            .execution_options(stream_results=True, yield_per=100)
        )
        for refs in connection.execute(label_refs):
            required.update(identity for identity in refs if identity)
            if len(required) > 1_000_000:
                raise ValueError("raw-reference audit budget exceeded; no truncated verification")
        reference_count = len(required)
        root = dataset["protocol_hash"]
        count = 0
        statement = (
            select(
                shadow_source_links,
                scalping_market_batches.c.raw,
                scalping_market_batches.c.event_count,
            )
            .join(
                scalping_market_batches,
                scalping_market_batches.c.batch_id == shadow_source_links.c.batch_id,
            )
            .where(shadow_source_links.c.dataset_id == dataset_id)
            .order_by(shadow_source_links.c.sequence)
            .execution_options(stream_results=True, yield_per=8)
        )
        for row in connection.execute(statement).mappings():
            count += 1
            if row["sequence"] != count:
                raise ValueError("raw manifest sequence gap")
            decoder = zlib.decompressobj()
            raw = decoder.decompress(row["raw"], 8 * 1024**2 + 1)
            if len(raw) > 8 * 1024**2 or not decoder.eof:
                raise ValueError("raw source exceeds integrity-check byte budget")
            if sha256(raw).hexdigest() != row["batch_id"]:
                raise ValueError("raw source content hash mismatch")
            body = json.loads(raw)
            if len(body) != row["event_count"]:
                raise ValueError("raw source event count mismatch")
            for event in body:
                required.discard(event["event_id"])
            root = sha256((root + row["batch_id"]).encode()).hexdigest()
            if root != row["root"]:
                raise ValueError("raw manifest chain mismatch")
        if count != dataset["source_batches"] or root != dataset["source_root"]:
            raise ValueError("sealed raw manifest does not match its ledger")
        if required:
            raise ValueError("dataset quote references missing from immutable raw manifest")
    return {
        "verified": True,
        "batches": count,
        "source_root": root,
        "raw_quote_references_verified": reference_count,
    }


def _mean_interval(values: list[tuple[str, float]]) -> dict[str, Any]:
    days: dict[str, list[float]] = defaultdict(list)
    for day, value in values:
        days[day].append(value)
    mean = statistics.mean(value for _, value in values) if values else None
    ci = None
    if len(days) >= 5:
        blocks = [(sum(day), len(day)) for day in days.values()]
        rng = random.Random(20261001)
        samples = []
        for _ in range(2000):
            chosen = [rng.choice(blocks) for _ in blocks]
            samples.append(sum(value for value, _ in chosen) / sum(count for _, count in chosen))
        samples.sort()
        ci = [samples[49], samples[1949]]
    return {"episodes": len(values), "days": len(days), "mean_bps": mean, "day_block_95_ci": ci}


def analyze_dataset(database: Database, *, output_dir: Path) -> dict[str, Any]:
    status = dataset_status(database)
    if not status.get("profitability_analysis_allowed"):
        return {
            "schema": "shadow-alpha-screen-v1",
            "state": "blocked_on_data_quality",
            "quality": status,
            "profitability_analysis_performed": False,
            "orders_submitted": 0,
            "promotion_allowed": False,
        }
    if output_dir.exists():
        raise ValueError("refusing to overwrite frozen shadow analysis")
    protocol = ShadowDatasetProtocol.model_validate(status["protocol"])
    manifest = verify_source_manifest(database)
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    episode_count = 0
    episode_bytes = 0
    with database.begin() as connection:
        query = (
            select(
                shadow_evaluations.c.evaluated_at,
                shadow_evaluations.c.symbol,
                shadow_evaluations.c.payload.label("evaluation"),
                shadow_labels.c.payload.label("label"),
            )
            .join(
                shadow_labels, shadow_labels.c.evaluation_id == shadow_evaluations.c.evaluation_id
            )
            .where(
                shadow_evaluations.c.dataset_id == DATASET_ID,
                shadow_labels.c.horizon_seconds == protocol.primary_horizon,
                shadow_labels.c.complete.is_(True),
            )
            .order_by(shadow_evaluations.c.evaluated_at, shadow_evaluations.c.symbol)
            .execution_options(stream_results=True, yield_per=100)
        )
        last_episode: dict[tuple[str, str], datetime] = {}
        for stored_row in connection.execute(query).mappings():
            observed = utc(stored_row["evaluated_at"])
            if (
                protocol.validation_start - timedelta(seconds=900)
                <= observed
                < protocol.validation_start
            ):
                continue
            evaluation = stored_row["evaluation"]
            label = stored_row["label"]
            family = evaluation["outputs"]["family"]
            category = family if family != "none" else evaluation["category"]
            key = (stored_row["symbol"], category)
            previous = last_episode.get(key)
            if previous and observed - previous < timedelta(seconds=60):
                continue
            last_episode[key] = observed
            phase = "development" if observed < protocol.validation_start else "validation"
            raw = evaluation.get("raw_features") or {}
            volatility = raw.get("volatility_5s_bps")
            vol_bucket = (
                "unknown"
                if volatility is None
                else "low_lt2"
                if volatility < 2
                else "medium_2to5"
                if volatility < 5
                else "high_ge5"
            )
            depth = evaluation.get("depth") or {}
            compact = {
                "symbol": stored_row["symbol"],
                "at": observed,
                "evaluation": {
                    "directions": evaluation["directions"],
                    "volatility_regime": vol_bucket,
                    "liquidity_regime": (
                        "fresh_l2" if depth.get("book_current") else "l1_only_or_stale_l2"
                    ),
                },
                "label": {
                    key: label[key]
                    for key in (
                        "long_gross_bps",
                        "short_gross_bps",
                        "long_net_bps",
                        "latency_stress",
                    )
                },
            }
            compact["label"]["latency_stress"] = {
                delay: {"complete": value["complete"], "long_net_bps": value["long_net_bps"]}
                for delay, value in label["latency_stress"].items()
            }
            episode_bytes += len(canonical(compact).encode())
            if episode_bytes > 64 * 1024**2:
                raise ValueError("analysis memory budget exceeded; no truncated episodes allowed")
            groups[(stored_row["symbol"], phase, category)].append(compact)
            episode_count += 1
    results = []
    for (symbol, phase, family), selected in sorted(groups.items()):
        gross = [(row["at"].date().isoformat(), row["label"]["long_gross_bps"]) for row in selected]
        net = [(row["at"].date().isoformat(), row["label"]["long_net_bps"]) for row in selected]
        gross_stats, net_stats = _mean_interval(gross), _mean_interval(net)
        benchmarks = {}
        for name in ("random_time_matched", "simple_momentum", "simple_mean_reversion"):
            observations = []
            for row in selected:
                direction = row["evaluation"]["directions"][name]
                value = row["label"]["long_gross_bps" if direction == 1 else "short_gross_bps"]
                if direction and value is not None:
                    observations.append(
                        (
                            row["at"].date().isoformat(),
                            value,
                        )
                    )
            benchmarks[name] = _mean_interval(observations)
        required_friction = float(
            protocol.assumed_entry_fee_bps
            + protocol.assumed_exit_fee_bps
            + protocol.additional_slippage_bps
            + protocol.latency_penalty_bps
        )
        gross_mean = gross_stats["mean_bps"]
        net_ci = net_stats["day_block_95_ci"]
        gross_ci = gross_stats["day_block_95_ci"]
        benchmark_complete = all(
            benchmark["episodes"] == len(selected) for benchmark in benchmarks.values()
        )
        best_benchmark = max(
            (
                benchmark["mean_bps"]
                for benchmark in benchmarks.values()
                if benchmark["mean_bps"] is not None
            ),
            default=None,
        )
        state = (
            "reject_nonpositive_gross"
            if gross_mean is not None and gross_mean <= 0
            else "insufficient_independent_episodes"
            if len(selected) < protocol.minimum_episodes_per_symbol
            else "insufficient_day_blocks"
            if net_ci is None or gross_ci is None
            else "reject_cost_margin"
            if net_ci[0] <= 0
            or gross_ci[0] <= required_friction
            or gross_mean < 1.5 * required_friction
            else "fee_tier_unverified"
            if not protocol.fee_tier_verified
            else "insufficient_time_matched_benchmark_coverage"
            if not benchmark_complete
            else "reject_trivial_benchmark_underperformance"
            if best_benchmark is not None and gross_mean <= best_benchmark
            else "requires_cross_fold_regime_and_execution_validation"
        )
        stability = []
        for days in ((1, 5), (5, 8), (8, 12), (12, 15)):
            subset = [row for row in selected if days[0] <= row["at"].day < days[1]]
            stability.append(
                {
                    "october_day_range": list(days),
                    "gross": _mean_interval(
                        [
                            (row["at"].date().isoformat(), row["label"]["long_gross_bps"])
                            for row in subset
                        ]
                    ),
                }
            )
        latency_stress = {}
        for delay in ("1", "3.5"):
            observations = [
                (
                    row["at"].date().isoformat(),
                    row["label"]["latency_stress"][delay]["long_net_bps"],
                )
                for row in selected
                if row["label"]["latency_stress"][delay]["complete"]
            ]
            latency_stress[delay] = {
                **_mean_interval(observations),
                "coverage": len(observations) / len(selected) if selected else None,
            }
        regime_stability = {}
        for name in ("volatility_regime", "liquidity_regime"):
            categories = sorted({row["evaluation"][name] for row in selected})
            regime_stability[name] = {
                category: _mean_interval(
                    [
                        (row["at"].date().isoformat(), row["label"]["long_net_bps"])
                        for row in selected
                        if row["evaluation"][name] == category
                    ]
                )
                for category in categories
            }
        results.append(
            {
                "symbol": symbol,
                "phase": phase,
                "family_or_category": family,
                "gross_executable_first": gross_stats,
                "conservative_net": net_stats,
                "additional_friction_bps": required_friction,
                "spread_charged_again": False,
                "state": state,
                "time_matched_gross_benchmarks": benchmarks,
                "benchmark_short_leg_is_counterfactual_unavailable_at_broker": True,
                "chronological_folds": stability,
                "regime_stability": regime_stability,
                "latency_stress": latency_stress,
                "extra_slippage_stress_net": _mean_interval(
                    [
                        (row["at"].date().isoformat(), row["label"]["long_net_bps"] - 5)
                        for row in selected
                    ]
                ),
                "extra_fee_stress_net": _mean_interval(
                    [
                        (row["at"].date().isoformat(), row["label"]["long_net_bps"] - 10)
                        for row in selected
                    ]
                ),
            }
        )
    result = {
        "schema": "shadow-alpha-screen-v1",
        "state": "research_screen_not_promotion",
        "protocol_hash": status["protocol_hash"],
        "source_manifest": manifest,
        "primary_horizon": 60,
        "nonoverlapping_episodes_per_symbol_and_group": episode_count,
        "episodes_across_groups_may_overlap": True,
        "quality": status,
        "results": results,
        "orders_submitted": 0,
        "promotion_allowed": False,
        "fees_are_assumptions_unless_independently_verified": not protocol.fee_tier_verified,
    }
    output_dir.mkdir(parents=True, exist_ok=False)
    (output_dir / "analysis.json").write_text(
        json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8"
    )
    return result
