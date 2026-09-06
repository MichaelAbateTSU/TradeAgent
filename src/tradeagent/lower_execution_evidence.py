from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field

from tradeagent.alpaca import HistoricalQuote, HistoricalTrade
from tradeagent.lower_turnover_research import LowerTurnoverResearchReport
from tradeagent.universe import UniverseFrame


class BulkEvidenceSource(Protocol):
    def quotes_many(
        self,
        symbols: Sequence[str],
        *,
        start: datetime,
        end: datetime,
        feed: Literal["iex", "sip"] | None = None,
    ) -> Iterator[HistoricalQuote]: ...

    def trades_many(
        self,
        symbols: Sequence[str],
        *,
        start: datetime,
        end: datetime,
        feed: Literal["iex", "sip"] | None = None,
    ) -> Iterator[HistoricalTrade]: ...


class Timestamped(Protocol):
    @property
    def timestamp(self) -> datetime: ...


class LowerEvidenceAnchor(BaseModel):
    model_config = ConfigDict(frozen=True)

    symbol: str
    timestamp: datetime
    anchor_types: tuple[str, ...]
    hypothesis_ids: tuple[str, ...]


class PointInTimeSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)

    anchor: LowerEvidenceAnchor
    quote_before: HistoricalQuote | None
    quote_after: HistoricalQuote | None
    trade_before: HistoricalTrade | None
    trade_after: HistoricalTrade | None


class EvidenceShard(BaseModel):
    model_config = ConfigDict(frozen=True)

    timestamp: datetime
    requested_symbols: tuple[str, ...]
    window_seconds: int
    snapshots: tuple[PointInTimeSnapshot, ...]


class EvidenceShardManifest(BaseModel):
    model_config = ConfigDict(frozen=True)

    timestamp: datetime
    path: str
    sha256: str = Field(min_length=64, max_length=64)
    anchors: int
    quote_complete: int
    trade_complete: int


class LowerExecutionEvidenceManifest(BaseModel):
    model_config = ConfigDict(frozen=True)

    version: str
    provider: str
    feed: str
    created_at: datetime
    window_seconds: int
    raw_trade_count: int
    raw_order_count: int
    unique_anchor_count: int
    unique_timestamp_count: int
    quote_complete_anchors: int
    trade_complete_anchors: int
    quote_coverage_ratio: Decimal
    trade_coverage_ratio: Decimal
    shards: tuple[EvidenceShardManifest, ...]


def load_lower_evidence_snapshots(
    manifest: LowerExecutionEvidenceManifest,
) -> dict[tuple[str, datetime], PointInTimeSnapshot]:
    snapshots: dict[tuple[str, datetime], PointInTimeSnapshot] = {}
    for shard_manifest in manifest.shards:
        path = Path(shard_manifest.path)
        if sha256(path.read_bytes()).hexdigest() != shard_manifest.sha256:
            raise ValueError(f"evidence shard hash mismatch: {path}")
        shard = EvidenceShard.model_validate_json(path.read_text(encoding="utf-8"))
        for snapshot in shard.snapshots:
            key = (snapshot.anchor.symbol, snapshot.anchor.timestamp)
            existing = snapshots.get(key)
            if existing is not None and existing != snapshot:
                raise ValueError(f"conflicting evidence snapshot: {key}")
            snapshots[key] = snapshot
    return snapshots


def write_lower_evidence_manifest(
    path: Path,
    manifest: LowerExecutionEvidenceManifest,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".partial")
    temporary.write_text(manifest.model_dump_json(indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def build_lower_evidence_anchors(
    report: LowerTurnoverResearchReport,
    frames: Sequence[UniverseFrame],
) -> tuple[LowerEvidenceAnchor, ...]:
    if len(frames) < 2:
        raise ValueError("lower execution evidence requires at least two daily frames")
    timestamps = [frame.timestamp for frame in frames]
    previous = {
        timestamp: timestamps[index - 1] for index, timestamp in enumerate(timestamps) if index > 0
    }
    metadata: defaultdict[
        tuple[str, datetime],
        dict[str, set[str]],
    ] = defaultdict(lambda: {"types": set(), "hypotheses": set()})
    for family in report.families:
        for result in family.results:
            hypothesis_id = f"{family.family}:{result.configuration_index}:{result.strategy_id}"
            for trade in result.diagnostics.trades:
                for role, submission_at in (
                    ("entry", trade.entry_at),
                    ("exit", trade.exit_at),
                ):
                    signal_at = previous.get(submission_at)
                    if signal_at is None:
                        raise ValueError(f"missing prior frame for {trade.symbol} {submission_at}")
                    signal = metadata[(trade.symbol, signal_at)]
                    signal["types"].add(f"{role}_signal")
                    signal["hypotheses"].add(hypothesis_id)
                    submission = metadata[(trade.symbol, submission_at)]
                    submission["types"].add(f"{role}_submission")
                    submission["hypotheses"].add(hypothesis_id)
    for frame in frames:
        for bar in frame.bars:
            benchmark = metadata[(bar.symbol, frame.timestamp)]
            benchmark["types"].add("benchmark_or_retry_observation")
            benchmark["hypotheses"].add("equal-weight-benchmarks")
    return tuple(
        LowerEvidenceAnchor(
            symbol=symbol,
            timestamp=timestamp,
            anchor_types=tuple(sorted(values["types"])),
            hypothesis_ids=tuple(sorted(values["hypotheses"])),
        )
        for (symbol, timestamp), values in sorted(
            metadata.items(),
            key=lambda item: (item[0][1], item[0][0]),
        )
    )


def build_frame_observation_anchors(
    frames: Sequence[UniverseFrame],
    *,
    hypothesis_ids: Sequence[str],
) -> tuple[LowerEvidenceAnchor, ...]:
    return tuple(
        LowerEvidenceAnchor(
            symbol=bar.symbol,
            timestamp=frame.timestamp,
            anchor_types=("external_observation",),
            hypothesis_ids=tuple(sorted(hypothesis_ids)),
        )
        for frame in frames
        for bar in frame.bars
    )


def collect_lower_execution_evidence(
    source: BulkEvidenceSource,
    anchors: Sequence[LowerEvidenceAnchor],
    shard_directory: Path,
    *,
    raw_trade_count: int,
    window_seconds: int = 2,
    workers: int = 1,
    on_shard: Callable[[EvidenceShardManifest], None] | None = None,
) -> LowerExecutionEvidenceManifest:
    if not anchors:
        raise ValueError("at least one lower-turnover evidence anchor is required")
    shard_directory.mkdir(parents=True, exist_ok=True)
    by_timestamp: defaultdict[datetime, list[LowerEvidenceAnchor]] = defaultdict(list)
    for anchor in anchors:
        by_timestamp[anchor.timestamp].append(anchor)
    if workers < 1:
        raise ValueError("evidence workers must be positive")
    jobs = [
        (timestamp, tuple(timestamp_anchors))
        for timestamp, timestamp_anchors in sorted(by_timestamp.items())
    ]
    if workers == 1:
        manifests = [
            _collect_or_load_shard(
                source,
                shard_directory,
                timestamp,
                timestamp_anchors,
                window_seconds,
            )
            for timestamp, timestamp_anchors in jobs
        ]
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            manifests = list(
                executor.map(
                    lambda job: _collect_or_load_shard(
                        source,
                        shard_directory,
                        job[0],
                        job[1],
                        window_seconds,
                    ),
                    jobs,
                )
            )
    for manifest in manifests:
        if on_shard is not None:
            on_shard(manifest)
    quote_complete = sum(shard.quote_complete for shard in manifests)
    trade_complete = sum(shard.trade_complete for shard in manifests)
    count = len(anchors)
    return LowerExecutionEvidenceManifest(
        version="v0.10.0-lower-turnover-execution-evidence-1",
        provider="alpaca",
        feed="sip",
        created_at=datetime.now(UTC),
        window_seconds=window_seconds,
        raw_trade_count=raw_trade_count,
        raw_order_count=raw_trade_count * 2,
        unique_anchor_count=count,
        unique_timestamp_count=len(manifests),
        quote_complete_anchors=quote_complete,
        trade_complete_anchors=trade_complete,
        quote_coverage_ratio=Decimal(quote_complete) / Decimal(count),
        trade_coverage_ratio=Decimal(trade_complete) / Decimal(count),
        shards=tuple(manifests),
    )


def _collect_or_load_shard(
    source: BulkEvidenceSource,
    shard_directory: Path,
    timestamp: datetime,
    anchors: tuple[LowerEvidenceAnchor, ...],
    window_seconds: int,
) -> EvidenceShardManifest:
    path = shard_directory / f"{int(timestamp.timestamp() * 1_000_000)}.json"
    if path.exists():
        shard = EvidenceShard.model_validate_json(path.read_text(encoding="utf-8"))
        existing_anchors = tuple(snapshot.anchor for snapshot in shard.snapshots)
        if _anchor_identities(anchors) != _anchor_identities(existing_anchors):
            raise ValueError(f"existing evidence shard does not match {timestamp}")
    else:
        shard = _collect_shard(
            source,
            timestamp,
            anchors,
            window_seconds=window_seconds,
        )
        _write_shard(path, shard)
    return _shard_manifest(path, shard)


def _anchor_identities(
    anchors: Sequence[LowerEvidenceAnchor],
) -> tuple[tuple[str, datetime, tuple[str, ...], tuple[str, ...]], ...]:
    return tuple(
        (
            anchor.symbol,
            anchor.timestamp,
            tuple(sorted(anchor.anchor_types)),
            tuple(sorted(anchor.hypothesis_ids)),
        )
        for anchor in anchors
    )


def _collect_shard(
    source: BulkEvidenceSource,
    timestamp: datetime,
    anchors: Sequence[LowerEvidenceAnchor],
    *,
    window_seconds: int,
) -> EvidenceShard:
    symbols = tuple(sorted({anchor.symbol for anchor in anchors}))
    start = timestamp - timedelta(seconds=window_seconds)
    end = timestamp + timedelta(seconds=window_seconds)
    quotes: defaultdict[str, list[HistoricalQuote]] = defaultdict(list)
    trades: defaultdict[str, list[HistoricalTrade]] = defaultdict(list)
    for quote in source.quotes_many(symbols, start=start, end=end, feed="sip"):
        quotes[quote.symbol].append(quote)
    for trade in source.trades_many(symbols, start=start, end=end, feed="sip"):
        trades[trade.symbol].append(trade)
    return EvidenceShard(
        timestamp=timestamp,
        requested_symbols=symbols,
        window_seconds=window_seconds,
        snapshots=tuple(
            PointInTimeSnapshot(
                anchor=anchor,
                quote_before=_latest_before(quotes[anchor.symbol], timestamp),
                quote_after=_first_after(quotes[anchor.symbol], timestamp),
                trade_before=_latest_before(trades[anchor.symbol], timestamp),
                trade_after=_first_after(trades[anchor.symbol], timestamp),
            )
            for anchor in anchors
        ),
    )


def _latest_before[Value: Timestamped](
    records: Sequence[Value],
    timestamp: datetime,
) -> Value | None:
    eligible = [record for record in records if record.timestamp <= timestamp]
    return max(eligible, key=lambda record: record.timestamp) if eligible else None


def _first_after[Value: Timestamped](
    records: Sequence[Value],
    timestamp: datetime,
) -> Value | None:
    eligible = [record for record in records if record.timestamp >= timestamp]
    return min(eligible, key=lambda record: record.timestamp) if eligible else None


def _write_shard(path: Path, shard: EvidenceShard) -> None:
    temporary = path.with_suffix(".json.partial")
    temporary.write_text(shard.model_dump_json(indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def _shard_manifest(path: Path, shard: EvidenceShard) -> EvidenceShardManifest:
    return EvidenceShardManifest(
        timestamp=shard.timestamp,
        path=path.as_posix(),
        sha256=sha256(path.read_bytes()).hexdigest(),
        anchors=len(shard.snapshots),
        quote_complete=sum(
            snapshot.quote_before is not None and snapshot.quote_after is not None
            for snapshot in shard.snapshots
        ),
        trade_complete=sum(
            snapshot.trade_before is not None and snapshot.trade_after is not None
            for snapshot in shard.snapshots
        ),
    )
