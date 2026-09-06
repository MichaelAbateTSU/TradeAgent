from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from tradeagent.alpaca import HistoricalQuote, HistoricalTrade
from tradeagent.diagnostics import TradeDiagnostic
from tradeagent.domain import MarketBar
from tradeagent.lower_execution_evidence import (
    LowerEvidenceAnchor,
    build_lower_evidence_anchors,
    collect_lower_execution_evidence,
    load_lower_evidence_snapshots,
    write_lower_evidence_manifest,
)
from tradeagent.universe import UniverseFrame


class FakeBulkSource:
    def __init__(self) -> None:
        self.calls = 0

    def quotes_many(self, symbols, *, start, end, feed=None):
        self.calls += 1
        for symbol in symbols:
            for timestamp in (start, end):
                yield HistoricalQuote(
                    symbol=symbol,
                    timestamp=timestamp,
                    bid_exchange="P",
                    bid_price=Decimal("99.99"),
                    bid_size=Decimal("100"),
                    ask_exchange="Q",
                    ask_price=Decimal("100.01"),
                    ask_size=Decimal("100"),
                    feed_source="sip",
                )

    def trades_many(self, symbols, *, start, end, feed=None):
        self.calls += 1
        for symbol in symbols:
            for index, timestamp in enumerate((start, end)):
                yield HistoricalTrade(
                    symbol=symbol,
                    timestamp=timestamp,
                    exchange="P",
                    price=Decimal("100"),
                    size=Decimal("10"),
                    trade_id=f"{symbol}-{index}",
                    feed_source="sip",
                )


def _frame(timestamp: datetime) -> UniverseFrame:
    return UniverseFrame(
        timestamp=timestamp,
        bars=(
            MarketBar(
                symbol="SPY",
                timestamp=timestamp,
                open=Decimal("100"),
                high=Decimal("100"),
                low=Decimal("100"),
                close=Decimal("100"),
                volume=Decimal("1000"),
            ),
        ),
    )


def test_lower_anchors_deduplicate_hypotheses_and_preserve_signal_time() -> None:
    first = datetime(2024, 1, 2, tzinfo=UTC)
    second = first + timedelta(days=1)
    trade = TradeDiagnostic(
        symbol="SPY",
        entry_at=second,
        exit_at=second,
        quantity=Decimal("1"),
        gross_pnl=Decimal(0),
        net_pnl=Decimal(0),
        execution_cost=Decimal(0),
        spread_cost=Decimal(0),
        slippage_cost=Decimal(0),
        fees=Decimal(0),
        flattening_cost=Decimal(0),
        mfe=Decimal(0),
        mae=Decimal(0),
        holding_frames=0,
    )
    result = SimpleNamespace(
        configuration_index=1,
        strategy_id="candidate",
        diagnostics=SimpleNamespace(trades=(trade, trade)),
    )
    report = SimpleNamespace(families=(SimpleNamespace(family="momentum", results=(result,)),))

    anchors = build_lower_evidence_anchors(report, (_frame(first), _frame(second)))

    assert len(anchors) == 2
    assert anchors[0].timestamp == first
    assert anchors[0].anchor_types == (
        "benchmark_or_retry_observation",
        "entry_signal",
        "exit_signal",
    )
    assert anchors[1].anchor_types == (
        "benchmark_or_retry_observation",
        "entry_submission",
        "exit_submission",
    )


def test_lower_evidence_shards_are_covered_and_resumable(tmp_path: Path) -> None:
    timestamp = datetime(2024, 1, 2, 21, tzinfo=UTC)
    anchors = (
        LowerEvidenceAnchor(
            symbol="SPY",
            timestamp=timestamp,
            anchor_types=("entry_submission",),
            hypothesis_ids=("momentum:1",),
        ),
        LowerEvidenceAnchor(
            symbol="QQQ",
            timestamp=timestamp,
            anchor_types=("exit_submission",),
            hypothesis_ids=("rotation:1",),
        ),
    )
    source = FakeBulkSource()

    first = collect_lower_execution_evidence(
        source,
        anchors,
        tmp_path / "shards",
        raw_trade_count=2,
        workers=2,
    )
    second = collect_lower_execution_evidence(
        source,
        anchors,
        tmp_path / "shards",
        raw_trade_count=2,
    )

    assert source.calls == 2
    assert first.quote_coverage_ratio == 1
    assert first.trade_coverage_ratio == 1
    assert first.shards == second.shards
    loaded = load_lower_evidence_snapshots(first)
    assert set(loaded) == {("SPY", timestamp), ("QQQ", timestamp)}

    manifest_path = tmp_path / "manifest.json"
    write_lower_evidence_manifest(manifest_path, first)
    assert manifest_path.exists()

    shard_path = Path(first.shards[0].path)
    shard_path.write_text(shard_path.read_text() + " ", encoding="utf-8")
    with pytest.raises(ValueError, match="hash mismatch"):
        load_lower_evidence_snapshots(first)
