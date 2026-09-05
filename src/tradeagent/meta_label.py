from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from hashlib import sha256
from zoneinfo import ZoneInfo

import numpy as np
from pydantic import BaseModel, ConfigDict, Field
from sklearn.calibration import CalibratedClassifierCV
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss
from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from tradeagent.config import AppConfig
from tradeagent.domain import MarketBar
from tradeagent.features import IntradayFeatureEngine, IntradayFeatureVector
from tradeagent.intraday import NyseSessionCalendar, SessionPhase
from tradeagent.portfolio import PortfolioIntent, PortfolioStrategy
from tradeagent.universe import UniverseFrame

FEATURE_NAMES = (
    "vwap_distance",
    "relative_volume",
    "realized_volatility",
    "momentum_15m",
    "momentum_30m",
    "momentum_60m",
    "candle_body_ratio",
)


class TrendPullbackCandidateStrategy:
    """Broad deterministic setup generator; the model may only reject its events."""

    def __init__(self, config: AppConfig) -> None:
        self._features = IntradayFeatureEngine(config.intraday)
        self._calendar = NyseSessionCalendar(config.intraday)
        self._target = Decimal("0.0025")

    @property
    def strategy_id(self) -> str:
        return "trend-pullback-candidates-v1"

    def on_frame(self, frame: UniverseFrame) -> PortfolioIntent:
        gate = self._calendar.gate(frame.timestamp)
        vectors = {bar.symbol: self._features.on_bar(bar) for bar in frame.bars}
        eligible = []
        if gate.phase is SessionPhase.ENTRY and frame.timestamp.minute % 15 == 0:
            eligible = [
                vector
                for vector in vectors.values()
                if vector.momentum_60m is not None
                and vector.momentum_60m > 0
                and vector.momentum_15m is not None
                and vector.momentum_15m > 0
                and Decimal("-0.005") <= vector.session_vwap_distance <= Decimal("0.002")
            ]
        selected = (
            max(
                eligible,
                key=lambda vector: (vector.momentum_60m or Decimal(0), vector.symbol),
            ).symbol
            if eligible
            else None
        )
        return PortfolioIntent(
            strategy_id=self.strategy_id,
            timestamp=frame.timestamp,
            target_weights={
                bar.symbol: self._target if bar.symbol == selected else Decimal(0)
                for bar in frame.bars
            },
            rationale="positive 60m trend, VWAP pullback, and 15m resumption",
        )


class MetaLabelEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    symbol: str
    decision_at: datetime
    label_end_at: datetime
    features: tuple[Decimal, ...]
    net_forward_return: Decimal
    label: int = Field(ge=0, le=1)


class MetaLabelFold(BaseModel):
    model_config = ConfigDict(frozen=True)

    fold: int
    training_events: int
    testing_events: int
    accepted_events: int
    precision: Decimal | None
    coverage: Decimal
    brier_score: Decimal
    accepted_expectancy: Decimal


class MetaLabelReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    primary_strategy_id: str
    event_count: int
    positive_labels: int
    threshold: Decimal
    folds: tuple[MetaLabelFold, ...]
    accepted_events: int
    accepted_expectancy: Decimal
    model_hash: str
    qualified_filter: bool
    reasons: tuple[str, ...]


def build_meta_label_events(
    frames: Sequence[UniverseFrame],
    strategy: PortfolioStrategy,
    config: AppConfig,
    *,
    horizon_frames: int = 6,
    required_net_edge_bps: Decimal = Decimal("15"),
) -> list[MetaLabelEvent]:
    if horizon_frames < 2:
        raise ValueError("meta-label horizon must be at least two frames")
    feature_engine = IntradayFeatureEngine(config.intraday)
    timezone = ZoneInfo(config.intraday.timezone)
    previous_targets = {bar.symbol: Decimal(0) for bar in frames[0].bars}
    events: list[MetaLabelEvent] = []
    round_trip_cost = (
        (
            config.broker.slippage_bps
            + config.broker.spread_bps / Decimal(2)
            + config.broker.commission_bps
        )
        * Decimal(2)
        * Decimal(3)
        / Decimal(10_000)
    )
    required_edge = required_net_edge_bps / Decimal(10_000)
    for index, frame in enumerate(frames):
        vectors = {bar.symbol: feature_engine.on_bar(bar) for bar in frame.bars}
        intent = strategy.on_frame(frame)
        if intent is None:
            continue
        if index + horizon_frames >= len(frames):
            continue
        entry_frame = frames[index + 1]
        exit_frame = frames[index + horizon_frames]
        if (
            frame.timestamp.astimezone(timezone).date()
            != exit_frame.timestamp.astimezone(timezone).date()
        ):
            previous_targets = dict(intent.target_weights)
            continue
        for bar in frame.bars:
            target = intent.target_weights.get(bar.symbol, Decimal(0))
            if target <= 0 or previous_targets.get(bar.symbol, Decimal(0)) > 0:
                continue
            entry = entry_frame.bar_for(bar.symbol).close
            exit_price = exit_frame.bar_for(bar.symbol).close
            net_return = exit_price / entry - Decimal(1) - round_trip_cost
            events.append(
                MetaLabelEvent(
                    symbol=bar.symbol,
                    decision_at=frame.timestamp,
                    label_end_at=exit_frame.timestamp,
                    features=_features(vectors[bar.symbol], bar),
                    net_forward_return=net_return,
                    label=int(net_return > required_edge),
                )
            )
        previous_targets = dict(intent.target_weights)
    return events


def evaluate_meta_labels(
    events: Sequence[MetaLabelEvent],
    *,
    threshold: Decimal = Decimal("0.65"),
    minimum_events: int = 200,
) -> MetaLabelReport:
    if len(events) < minimum_events:
        raise ValueError(f"meta-labeling requires at least {minimum_events} candidate events")
    labels = np.asarray([event.label for event in events], dtype=int)
    if len(np.unique(labels)) < 2:
        raise ValueError("meta-labeling requires both positive and negative labels")
    if int(labels.sum()) < 20:
        raise ValueError("meta-labeling requires at least 20 positive net-edge labels")
    features = np.asarray(
        [[float(value) for value in event.features] for event in events],
        dtype=float,
    )
    outer_splits = TimeSeriesSplit(n_splits=4, gap=6)
    folds: list[MetaLabelFold] = []
    accepted_returns: list[Decimal] = []
    model_descriptions: list[dict[str, object]] = []
    for fold_index, (train, test) in enumerate(outer_splits.split(features), start=1):
        if len(np.unique(labels[train])) < 2:
            continue
        estimator = Pipeline(
            [
                ("scale", StandardScaler()),
                (
                    "model",
                    LogisticRegression(
                        C=0.1,
                        class_weight="balanced",
                        max_iter=1_000,
                        random_state=7,
                    ),
                ),
            ]
        )
        calibrated = CalibratedClassifierCV(
            estimator,
            method="sigmoid",
            cv=TimeSeriesSplit(n_splits=2, gap=6),
        )
        calibrated.fit(features[train], labels[train])
        probabilities = calibrated.predict_proba(features[test])[:, 1]
        accepted_mask = probabilities >= float(threshold)
        accepted_indices = test[accepted_mask]
        fold_returns = [events[index].net_forward_return for index in accepted_indices]
        accepted_returns.extend(fold_returns)
        accepted_labels = labels[accepted_indices]
        folds.append(
            MetaLabelFold(
                fold=fold_index,
                training_events=len(train),
                testing_events=len(test),
                accepted_events=len(accepted_indices),
                precision=(
                    Decimal(str(float(np.mean(accepted_labels)))) if len(accepted_labels) else None
                ),
                coverage=Decimal(len(accepted_indices)) / Decimal(len(test)),
                brier_score=Decimal(str(brier_score_loss(labels[test], probabilities))),
                accepted_expectancy=(
                    sum(fold_returns, Decimal(0)) / len(fold_returns)
                    if fold_returns
                    else Decimal(0)
                ),
            )
        )
        model_descriptions.append(
            {
                "fold": fold_index,
                "train_end": int(train[-1]),
                "test_start": int(test[0]),
                "threshold": str(threshold),
            }
        )
    expectancy = (
        sum(accepted_returns, Decimal(0)) / len(accepted_returns)
        if accepted_returns
        else Decimal(0)
    )
    reasons: list[str] = []
    if not folds:
        reasons.append("NO_VALID_MODEL_FOLDS")
    if len(accepted_returns) < 50:
        reasons.append("INSUFFICIENT_ACCEPTED_EVENTS")
    if expectancy <= 0:
        reasons.append("NON_POSITIVE_ACCEPTED_EXPECTANCY")
    model_hash = sha256(
        json.dumps(
            {
                "features": FEATURE_NAMES,
                "models": model_descriptions,
            },
            sort_keys=True,
        ).encode()
    ).hexdigest()
    return MetaLabelReport(
        primary_strategy_id="candidate-events",
        event_count=len(events),
        positive_labels=int(labels.sum()),
        threshold=threshold,
        folds=tuple(folds),
        accepted_events=len(accepted_returns),
        accepted_expectancy=expectancy,
        model_hash=model_hash,
        qualified_filter=not reasons,
        reasons=tuple(reasons),
    )


def _features(
    vector: IntradayFeatureVector,
    bar: MarketBar,
) -> tuple[Decimal, ...]:
    candle_range = bar.high - bar.low
    body_ratio = abs(bar.close - bar.open) / candle_range if candle_range > 0 else Decimal(0)
    return (
        vector.session_vwap_distance,
        vector.relative_volume or Decimal(0),
        vector.realized_volatility or Decimal(0),
        vector.momentum_15m or Decimal(0),
        vector.momentum_30m or Decimal(0),
        vector.momentum_60m or Decimal(0),
        body_ratio,
    )
