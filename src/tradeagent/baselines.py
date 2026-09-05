from __future__ import annotations

import random
from decimal import Decimal

from tradeagent.config import IntradayConfig
from tradeagent.intraday import NyseSessionCalendar, SessionPhase
from tradeagent.portfolio import PortfolioIntent
from tradeagent.universe import UniverseFrame


class RandomTimingBaseline:
    """Deterministic random-entry control with fixed holding time and trade rate."""

    def __init__(
        self,
        intraday: IntradayConfig,
        *,
        seed: int,
        entry_probability: float,
        hold_frames: int,
        target_weight: Decimal = Decimal("0.0025"),
    ) -> None:
        if not 0 <= entry_probability <= 1:
            raise ValueError("entry_probability must be between zero and one")
        if hold_frames < 1:
            raise ValueError("hold_frames must be positive")
        self._calendar = NyseSessionCalendar(intraday)
        self._random = random.Random(seed)
        self._entry_probability = entry_probability
        self._hold_frames = hold_frames
        self._target_weight = target_weight
        self._active: str | None = None
        self._remaining = 0

    @property
    def strategy_id(self) -> str:
        return "random-timing-baseline-v1"

    def on_frame(self, frame: UniverseFrame) -> PortfolioIntent:
        gate = self._calendar.gate(frame.timestamp)
        if gate.phase is SessionPhase.FLATTEN:
            self._active = None
            self._remaining = 0
        elif self._active is not None:
            self._remaining -= 1
            if self._remaining <= 0:
                self._active = None
        elif gate.phase is SessionPhase.ENTRY and self._random.random() < self._entry_probability:
            self._active = self._random.choice(sorted(bar.symbol for bar in frame.bars))
            self._remaining = self._hold_frames
        return PortfolioIntent(
            strategy_id=self.strategy_id,
            timestamp=frame.timestamp,
            target_weights={
                bar.symbol: (self._target_weight if bar.symbol == self._active else Decimal(0))
                for bar in frame.bars
            },
            rationale="deterministic random-entry control",
        )
