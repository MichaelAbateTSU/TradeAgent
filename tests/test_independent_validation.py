from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from tradeagent.alpaca import HistoricalQuote, HistoricalTrade
from tradeagent.candidate_selection import (
    freeze_two_candidates,
    selection_protocol_hash,
)
from tradeagent.domain import MarketBar
from tradeagent.external_validation import evaluate_external_era
from tradeagent.frozen_candidate import FrozenCandidateManifest
from tradeagent.lower_calibration import calibrate_lower_turnover_families
from tradeagent.lower_execution_evidence import (
    LowerEvidenceAnchor,
    PointInTimeSnapshot,
)
from tradeagent.universe import UniverseFrame


def _panel(count: int = 300) -> tuple[UniverseFrame, ...]:
    frames = []
    for index in range(count):
        timestamp = datetime(2020, 1, 2, 21, tzinfo=UTC) + timedelta(days=index)
        bars = []
        for symbol, slope in (("SPY", Decimal("0.05")), ("QQQ", Decimal("0.08"))):
            cycle = Decimal((index % 20) - 10) / Decimal(20)
            price = Decimal("100") + slope * index + cycle
            bars.append(
                MarketBar(
                    symbol=symbol,
                    timestamp=timestamp,
                    open=price,
                    high=price,
                    low=price,
                    close=price,
                    volume=Decimal("1000000"),
                )
            )
        frames.append(UniverseFrame(timestamp=timestamp, bars=tuple(bars)))
    return tuple(frames)


def _snapshots(
    frames: tuple[UniverseFrame, ...],
) -> dict[tuple[str, datetime], PointInTimeSnapshot]:
    snapshots = {}
    for frame in frames:
        for bar in frame.bars:
            anchor = LowerEvidenceAnchor(
                symbol=bar.symbol,
                timestamp=frame.timestamp,
                anchor_types=("test",),
                hypothesis_ids=("all",),
            )
            quote_before = HistoricalQuote(
                symbol=bar.symbol,
                timestamp=frame.timestamp - timedelta(milliseconds=1),
                bid_exchange="P",
                bid_price=bar.close - Decimal("0.01"),
                bid_size=Decimal("10000"),
                ask_exchange="Q",
                ask_price=bar.close + Decimal("0.01"),
                ask_size=Decimal("10000"),
                feed_source="sip",
            )
            quote_after = quote_before.model_copy(
                update={"timestamp": frame.timestamp + timedelta(milliseconds=1)}
            )
            trade_before = HistoricalTrade(
                symbol=bar.symbol,
                timestamp=frame.timestamp - timedelta(milliseconds=1),
                exchange="P",
                price=bar.close,
                size=Decimal("100"),
                trade_id=f"{bar.symbol}-{frame.timestamp}-before",
                feed_source="sip",
            )
            snapshots[(bar.symbol, frame.timestamp)] = PointInTimeSnapshot(
                anchor=anchor,
                quote_before=quote_before,
                quote_after=quote_after,
                trade_before=trade_before,
                trade_after=trade_before.model_copy(
                    update={
                        "timestamp": frame.timestamp + timedelta(milliseconds=1),
                        "trade_id": f"{bar.symbol}-{frame.timestamp}-after",
                    }
                ),
            )
    return snapshots


def test_calibration_freeze_and_external_validation_are_reproducible(
    tmp_path: Path,
) -> None:
    frames = _panel()
    snapshots = _snapshots(frames)
    calibration = calibrate_lower_turnover_families(
        frames,
        snapshots,
        generated_at=datetime(2026, 9, 6, tzinfo=UTC),
        evidence_manifest="test-evidence.json",
    )
    calibration_path = tmp_path / "calibration.json"
    calibration_path.write_text(calibration.model_dump_json(), encoding="utf-8")
    protocol = freeze_two_candidates(
        calibration_path,
        tmp_path / "freezes",
        code_commit="a" * 40,
        development_dataset_hash="b" * 64,
        corrected_reruns=3,
        frozen_at=datetime(2026, 9, 6, tzinfo=UTC),
        external_directory=tmp_path / "unseen-external",
    )
    candidates = tuple(
        FrozenCandidateManifest.model_validate_json(Path(path).read_text(encoding="utf-8"))
        for path in protocol.candidate_files
    )
    report = evaluate_external_era(
        frames,
        snapshots,
        candidates,
        calibration,
        era="synthetic",
        dataset_manifest="synthetic-data.json",
        evidence_manifest="synthetic-evidence.json",
        generated_at=datetime(2026, 9, 6, tzinfo=UTC),
    )

    assert calibration.raw_hypotheses == 30
    assert len(calibration.families) == 3
    assert len(candidates) == 2
    assert not protocol.external_data_present_at_selection
    assert len(report.candidates) == 2
    assert all(not candidate.qualified for candidate in report.candidates)
    assert len(selection_protocol_hash(protocol)) == 64

    external_directory = tmp_path / "already-seen"
    external_directory.mkdir()
    (external_directory / "data.csv").write_text("seen", encoding="utf-8")
    with pytest.raises(RuntimeError, match="external data already exists"):
        freeze_two_candidates(
            calibration_path,
            tmp_path / "second-freeze",
            code_commit="a" * 40,
            development_dataset_hash="b" * 64,
            corrected_reruns=3,
            external_directory=external_directory,
        )
