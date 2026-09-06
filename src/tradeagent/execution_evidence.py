from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Iterator
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Literal, Protocol, TextIO

from pydantic import BaseModel, ConfigDict, Field

from tradeagent.alpaca import HistoricalQuote, HistoricalTrade
from tradeagent.diagnostics import StrategyDiagnostics
from tradeagent.squeeze_external import FrozenSqueezeExternalReport


class EvidenceAnchor(BaseModel):
    model_config = ConfigDict(frozen=True)

    symbol: str
    timeframe: str
    strategy_id: str
    anchor_type: str
    timestamp: datetime


class EvidenceCoverage(BaseModel):
    model_config = ConfigDict(frozen=True)

    anchor: EvidenceAnchor
    quote_count: int = Field(ge=0)
    trade_count: int = Field(ge=0)
    has_quote_before: bool
    has_quote_after: bool


class ExecutionEvidenceManifest(BaseModel):
    model_config = ConfigDict(frozen=True)

    version: str
    provider: str
    feed: str
    created_at: datetime
    window_seconds: int
    records_path: str
    records_sha256: str
    anchor_count: int
    anchors_with_quotes: int
    quote_coverage_ratio: float
    quote_records: int
    trade_records: int
    coverage: tuple[EvidenceCoverage, ...]


class HistoricalEvidenceSource(Protocol):
    def quotes(
        self,
        symbol: str,
        *,
        start: datetime,
        end: datetime,
        feed: Literal["iex", "sip"] | None = None,
    ) -> Iterator[HistoricalQuote]: ...

    def trades(
        self,
        symbol: str,
        *,
        start: datetime,
        end: datetime,
        feed: Literal["iex", "sip"] | None = None,
    ) -> Iterator[HistoricalTrade]: ...


def squeeze_evidence_anchors(
    report: FrozenSqueezeExternalReport,
) -> tuple[EvidenceAnchor, ...]:
    anchors: dict[tuple[str, str, str, datetime], EvidenceAnchor] = {}
    for result in report.results:
        for anchor in diagnostic_evidence_anchors(result.diagnostics, result.timeframe):
            key = (anchor.symbol, anchor.timeframe, anchor.anchor_type, anchor.timestamp)
            anchors[key] = anchor
    return tuple(
        sorted(
            anchors.values(),
            key=lambda item: (
                item.symbol,
                item.timeframe,
                item.timestamp,
                item.anchor_type,
            ),
        )
    )


def diagnostic_evidence_anchors(
    diagnostics: StrategyDiagnostics,
    timeframe: str,
) -> tuple[EvidenceAnchor, ...]:
    interval = timedelta(minutes=_timeframe_minutes(timeframe))
    anchors: dict[tuple[str, str, datetime], EvidenceAnchor] = {}
    for trade in diagnostics.trades:
        for event_type, timestamp in (
            ("entry_signal", trade.entry_at - interval),
            ("entry_submission", trade.entry_at),
            ("exit_signal", trade.exit_at - interval),
            ("exit_submission", trade.exit_at),
        ):
            anchor = EvidenceAnchor(
                symbol=trade.symbol,
                timeframe=timeframe,
                strategy_id=diagnostics.strategy_id,
                anchor_type=event_type,
                timestamp=timestamp,
            )
            anchors[(anchor.symbol, anchor.anchor_type, anchor.timestamp)] = anchor
    return tuple(
        sorted(
            anchors.values(),
            key=lambda item: (item.symbol, item.timestamp, item.anchor_type),
        )
    )


def collect_execution_evidence(
    client: HistoricalEvidenceSource,
    anchors: Iterable[EvidenceAnchor],
    records_path: Path,
    *,
    window_seconds: int = 2,
    on_anchor: Callable[[EvidenceCoverage], None] | None = None,
) -> ExecutionEvidenceManifest:
    if records_path.exists():
        raise FileExistsError(f"{records_path} already exists; evidence archives are append-only")
    normalized_anchors = tuple(anchors)
    if not normalized_anchors:
        raise ValueError("at least one evidence anchor is required")
    records_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = records_path.with_suffix(records_path.suffix + ".partial")
    temporary.unlink(missing_ok=True)
    with temporary.open("x", encoding="utf-8") as output:
        coverage, quote_records, trade_records = _write_anchor_records(
            client,
            normalized_anchors,
            output,
            window_seconds=window_seconds,
            on_anchor=on_anchor,
        )
    temporary.replace(records_path)

    anchors_with_quotes = sum(item.quote_count > 0 for item in coverage)
    return ExecutionEvidenceManifest(
        version="v0.9.0-squeeze-execution-evidence-1",
        provider="alpaca",
        feed="sip",
        created_at=datetime.now(UTC),
        window_seconds=window_seconds,
        records_path=records_path.as_posix(),
        records_sha256=_file_hash(records_path),
        anchor_count=len(coverage),
        anchors_with_quotes=anchors_with_quotes,
        quote_coverage_ratio=anchors_with_quotes / len(coverage),
        quote_records=quote_records,
        trade_records=trade_records,
        coverage=tuple(coverage),
    )


def write_evidence_manifest(path: Path, manifest: ExecutionEvidenceManifest) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".partial")
    temporary.write_text(manifest.model_dump_json(indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def _write_anchor_records(
    client: HistoricalEvidenceSource,
    anchors: tuple[EvidenceAnchor, ...],
    output: TextIO,
    *,
    window_seconds: int,
    on_anchor: Callable[[EvidenceCoverage], None] | None,
) -> tuple[list[EvidenceCoverage], int, int]:
    coverage: list[EvidenceCoverage] = []
    quote_records = 0
    trade_records = 0
    for anchor in anchors:
        start = anchor.timestamp - timedelta(seconds=window_seconds)
        end = anchor.timestamp + timedelta(seconds=window_seconds)
        quotes = tuple(client.quotes(anchor.symbol, start=start, end=end, feed="sip"))
        trades = tuple(client.trades(anchor.symbol, start=start, end=end, feed="sip"))
        anchor_payload = anchor.model_dump(mode="json")
        for record_type, records in (("quote", quotes), ("trade", trades)):
            for record in records:
                output.write(
                    json.dumps(
                        {
                            "record_type": record_type,
                            "anchor": anchor_payload,
                            "record": record.model_dump(mode="json"),
                        },
                        sort_keys=True,
                    )
                    + "\n"
                )
        result = EvidenceCoverage(
            anchor=anchor,
            quote_count=len(quotes),
            trade_count=len(trades),
            has_quote_before=any(quote.timestamp <= anchor.timestamp for quote in quotes),
            has_quote_after=any(quote.timestamp >= anchor.timestamp for quote in quotes),
        )
        coverage.append(result)
        quote_records += len(quotes)
        trade_records += len(trades)
        if on_anchor is not None:
            on_anchor(result)
    return coverage, quote_records, trade_records


def _file_hash(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _timeframe_minutes(timeframe: str) -> int:
    try:
        return {"5Min": 5, "30Min": 30, "1Hour": 60}[timeframe]
    except KeyError as error:
        raise ValueError(f"unsupported evidence timeframe {timeframe}") from error
