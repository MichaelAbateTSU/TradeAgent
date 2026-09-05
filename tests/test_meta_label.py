from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from tradeagent.config import AppConfig, IntradayConfig
from tradeagent.domain import MarketBar
from tradeagent.meta_label import (
    MetaLabelEvent,
    TrendPullbackCandidateStrategy,
    evaluate_meta_labels,
)
from tradeagent.universe import UniverseFrame


def _events(count: int = 240) -> list[MetaLabelEvent]:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    return [
        MetaLabelEvent(
            symbol="SPY",
            decision_at=start + timedelta(minutes=index * 10),
            label_end_at=start + timedelta(minutes=index * 10 + 30),
            features=(
                Decimal(index % 7) / 10,
                Decimal(index % 5) / 10,
                Decimal(index % 3) / 10,
                Decimal(index % 2),
                Decimal(index % 11) / 10,
                Decimal(index % 13) / 10,
                Decimal(index % 17) / 10,
            ),
            net_forward_return=Decimal("0.002") if index % 2 else Decimal("-0.001"),
            label=index % 2,
        )
        for index in range(count)
    ]


def test_meta_label_evaluation_is_deterministic() -> None:
    first = evaluate_meta_labels(_events())
    second = evaluate_meta_labels(_events())

    assert first == second
    assert len(first.model_hash) == 64
    assert all(fold.testing_events > fold.accepted_events - 1 for fold in first.folds)


def test_meta_label_evaluation_fails_closed_on_bad_samples() -> None:
    with pytest.raises(ValueError, match="at least"):
        evaluate_meta_labels(_events(20))
    one_class = [event.model_copy(update={"label": 1}) for event in _events()]
    with pytest.raises(ValueError, match="both positive and negative"):
        evaluate_meta_labels(one_class)
    too_few_positive = [
        event.model_copy(update={"label": int(index < 10)}) for index, event in enumerate(_events())
    ]
    with pytest.raises(ValueError, match="20 positive"):
        evaluate_meta_labels(too_few_positive)


def test_trend_pullback_candidate_is_deterministic() -> None:
    config = AppConfig(intraday=IntradayConfig(enabled=True))
    strategy = TrendPullbackCandidateStrategy(config)
    start = datetime(2026, 9, 4, 14, 0, tzinfo=UTC)
    intents = []
    for index in range(15):
        timestamp = start + timedelta(minutes=index * 5)
        price = Decimal("100") + Decimal(index) / 10
        bar = MarketBar(
            symbol="SPY",
            timestamp=timestamp,
            open=price,
            high=price + Decimal("0.1"),
            low=price - Decimal("0.1"),
            close=price,
            volume=Decimal("1000"),
        )
        intents.append(strategy.on_frame(UniverseFrame(timestamp=timestamp, bars=(bar,))))

    assert all(
        sum(intent.target_weights.values(), Decimal(0)) <= Decimal("0.0025") for intent in intents
    )
