from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from tradeagent.alpaca import HistoricalQuote, HistoricalTrade
from tradeagent.execution_evidence import EvidenceAnchor, collect_execution_evidence


class FakeEvidenceSource:
    def quotes(self, symbol: str, *, start, end, feed=None):
        yield HistoricalQuote(
            symbol=symbol,
            timestamp=start,
            bid_exchange="P",
            bid_price=Decimal("100"),
            bid_size=Decimal("10"),
            ask_exchange="Q",
            ask_price=Decimal("100.02"),
            ask_size=Decimal("12"),
            feed_source="sip",
        )
        yield HistoricalQuote(
            symbol=symbol,
            timestamp=end,
            bid_exchange="P",
            bid_price=Decimal("100.01"),
            bid_size=Decimal("10"),
            ask_exchange="Q",
            ask_price=Decimal("100.03"),
            ask_size=Decimal("12"),
            feed_source="sip",
        )

    def trades(self, symbol: str, *, start, end, feed=None):
        yield HistoricalTrade(
            symbol=symbol,
            timestamp=start,
            exchange="P",
            price=Decimal("100.01"),
            size=Decimal("5"),
            trade_id="trade-1",
            feed_source="sip",
        )


def test_execution_evidence_is_hashed_covered_and_append_only(tmp_path: Path) -> None:
    records = tmp_path / "evidence.jsonl"
    anchor = EvidenceAnchor(
        symbol="SPY",
        timeframe="30Min",
        strategy_id="volatility-squeeze-breakout-v1",
        anchor_type="entry_submission",
        timestamp=datetime(2024, 1, 2, 15, tzinfo=UTC),
    )

    manifest = collect_execution_evidence(FakeEvidenceSource(), (anchor,), records)

    assert manifest.quote_coverage_ratio == 1
    assert manifest.quote_records == 2
    assert manifest.trade_records == 1
    assert len(manifest.records_sha256) == 64
    with pytest.raises(FileExistsError, match="append-only"):
        collect_execution_evidence(FakeEvidenceSource(), (anchor,), records)
