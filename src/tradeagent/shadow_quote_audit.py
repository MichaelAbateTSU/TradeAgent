"""Read-only quote forensics. Never repair, relabel or qualify research evidence."""

from __future__ import annotations

import gzip
import json
import statistics
import zlib
from collections import Counter, defaultdict, deque
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.engine import Connection

from tradeagent.persistence import Database
from tradeagent.scalping_market import MarketEvent, datetime_ns
from tradeagent.scalping_store import canonical, scalping_market_batches, utc
from tradeagent.shadow_dataset import (
    DATASET_ID,
    ShadowDatasetProtocol,
    shadow_datasets,
    shadow_evaluations,
    shadow_labels,
    shadow_source_links,
)

Point = tuple[str, int, int]
MAX_ROWS = 100_000
MAX_POINTS = 200_000
MAX_EVENTS = 2_000_000
MAX_BATCH_BYTES = 8 * 1024**2


def _ns(value: str) -> int:
    return datetime_ns(datetime.fromisoformat(value.replace("Z", "+00:00")))


def _observation(value: dict[str, Any] | None) -> dict[str, Any] | None:
    if value is None:
        return None
    q = value["quote"]
    return {
        "event_id": value["event_id"],
        "source_kind": value["source_kind"],
        "provider_timestamp": q["exchange_at"],
        "provider_timestamp_ns": int(q["exchange_time_ns"]),
        "received_at": q["received_at"],
        "received_at_ns": _ns(q["received_at"]),
        "processed_at": value["processed_at"],
        "epoch": value["epoch"],
        "bid": q["bid"],
        "ask": q["ask"],
        "bid_size": q["bid_size"],
        "ask_size": q["ask_size"],
    }


def _raw_quote(event: MarketEvent, batch: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_id": event.event_id,
        "source_kind": "native_quote",
        "provider_timestamp": event.exchange_timestamp,
        "provider_timestamp_ns": event.exchange_at_ns,
        "received_at": event.received_at.isoformat(),
        "received_at_ns": event.received_at_ns,
        "decoded_at_ns": event.decoded_at_ns,
        "connection_id": str(event.connection_id),
        "receive_sequence": event.receive_sequence,
        "bid": str(event.bids[0].price),
        "ask": str(event.asks[0].price),
        "bid_size": str(event.bids[0].quantity),
        "ask_size": str(event.asks[0].quantity),
        "valid_prices_and_sizes": (
            event.bids[0].price < event.asks[0].price
            and event.bids[0].quantity > 0
            and event.asks[0].quantity > 0
        ),
        "batch_id": batch["batch_id"],
        "persisted_at": utc(batch["recorded_at"]).isoformat(),
    }


def quote_at(value: dict[str, Any] | None, at_ns: int) -> dict[str, Any] | None:
    if value is None:
        return None
    return {
        **value,
        "provider_age_seconds": (at_ns - value["provider_timestamp_ns"]) / 1e9,
        "receive_age_seconds": (at_ns - value["received_at_ns"]) / 1e9,
    }


def _fresh(value: dict[str, Any] | None, at_ns: int, seconds: int) -> bool:
    return value is not None and all(
        0 <= at_ns - value[key] <= seconds * 1_000_000_000
        for key in ("provider_timestamp_ns", "received_at_ns")
    )


def classify_endpoint(
    stored: dict[str, Any] | None,
    point: dict[str, Any],
    *,
    at_ns: int,
    available_ns: int,
    maximum_age_seconds: int,
    accepted: dict[str, dict[str, Any]],
) -> str:
    if stored is not None and any(
        stored[key] > at_ns for key in ("provider_timestamp_ns", "received_at_ns")
    ):
        return "CLOCK_OR_CAUSALITY_VIOLATION"
    if _fresh(stored, at_ns, maximum_age_seconds):
        return "STORED_QUOTE_SATISFIES_FRESHNESS"
    if point.get("last_reset_ns", 0) > (
        point["last_native"]["received_at_ns"] if point["last_native"] else 0
    ):
        return "CONNECTIVITY_RESET_INVALIDATED_QUOTE"
    candidate = point["fresh_native"]
    if candidate is not None:
        proof = accepted.get(candidate["event_id"])
        if (
            proof is not None
            and _ns(proof["processed_at"]) <= available_ns
            and (stored is None or stored["epoch"] == proof["epoch"])
        ):
            return "C_ACCEPTED_FRESH_QUOTE_NOT_SELECTED"
        if _ns(candidate["persisted_at"]) <= available_ns:
            return "B_OR_C_CAPTURED_FRESH_QUOTE_NOT_SELECTED"
        return "PROCESSING_AVAILABILITY_NOT_RECORDED"
    native = point["last_native"]
    if native is None:
        return "A_NO_NATIVE_QUOTE_RECORDED"
    if native["provider_timestamp_ns"] > native["received_at_ns"]:
        return "CLOCK_OR_CAUSALITY_VIOLATION"
    if not native["valid_prices_and_sizes"]:
        return "INVALID_NATIVE_PRICE_OR_SIZE"
    if _fresh(native, at_ns, maximum_age_seconds):
        return "DECODE_AVAILABILITY_BOUNDARY"
    if native["received_at_ns"] + maximum_age_seconds * 1_000_000_000 < at_ns:
        return "D_UPSTREAM_INACTIVITY_STALE_QUOTE"
    return "D_UPSTREAM_PROVIDER_TIMESTAMP_STALE"


def _labels(connection: Connection, start: datetime, end: datetime) -> Any:
    return connection.execute(
        select(
            shadow_evaluations.c.evaluation_id,
            shadow_evaluations.c.symbol,
            shadow_evaluations.c.evaluated_at,
            shadow_evaluations.c.payload["decision_observation"].label("decision"),
            shadow_labels.c.horizon_seconds,
            shadow_labels.c.deadline_at,
            shadow_labels.c.resolved_at,
            shadow_labels.c.complete,
            shadow_labels.c.payload["exit_observation"].label("future"),
            shadow_labels.c.payload["missing_reasons"].label("reasons"),
        )
        .join(shadow_labels, shadow_labels.c.evaluation_id == shadow_evaluations.c.evaluation_id)
        .where(
            shadow_evaluations.c.dataset_id == DATASET_ID,
            shadow_evaluations.c.evaluated_at >= start,
            shadow_evaluations.c.evaluated_at < end,
        )
        .order_by(
            shadow_evaluations.c.evaluated_at,
            shadow_evaluations.c.symbol,
            shadow_labels.c.horizon_seconds,
        )
        .execution_options(stream_results=True, yield_per=100)
    ).mappings()


def _point(row: Any, endpoint: str) -> Point:
    target = utc(row["evaluated_at"] if endpoint == "decision" else row["deadline_at"])
    available = target if endpoint == "decision" else utc(row["resolved_at"])
    return row["symbol"], datetime_ns(target), datetime_ns(available)


def _quantiles(values: list[float]) -> dict[str, Any]:
    ordered = sorted(values)
    return {
        "samples": len(values),
        "p50": statistics.median(values) if values else None,
        "p95": ordered[int((len(values) - 1) * 0.95)] if values else None,
        "maximum": max(values) if values else None,
    }


def audit_quotes(database: Database, *, report_date: date, output_dir: Path) -> dict[str, Any]:
    if output_dir.exists():
        raise ValueError("refusing to overwrite a quote audit")
    output_dir.mkdir(parents=True)
    with database.begin() as connection:
        if connection.dialect.name == "postgresql":
            connection.exec_driver_sql("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        dataset = (
            connection.execute(
                select(shadow_datasets).where(shadow_datasets.c.dataset_id == DATASET_ID)
            )
            .mappings()
            .one()
        )
        protocol = ShadowDatasetProtocol.model_validate(dataset["protocol"])
        start = datetime.combine(report_date, datetime.min.time(), tzinfo=UTC)
        end = start + timedelta(days=1)
        if not protocol.start <= start < end <= protocol.end:
            raise ValueError("audit date is outside the frozen collection window")
        if datetime.now(UTC) < end + timedelta(
            seconds=max(protocol.horizons) + protocol.label_settle_seconds
        ):
            raise ValueError("audit requires the complete day's label tail to have elapsed")
        accepted: dict[str, dict[str, Any]] = {}
        points: dict[Point, dict[str, Any]] = {}
        totals: dict[str, Counter[str]] = defaultdict(Counter)
        failure_reasons: dict[str, Counter[str]] = defaultdict(Counter)
        evaluations: set[str] = set()
        evidence_digest = sha256()
        row_count = 0
        missing_count = 0
        for row in _labels(connection, start, end):
            row_count += 1
            if row_count > MAX_ROWS:
                raise ValueError("label audit row budget exceeded; no truncated audit")
            evaluations.add(row["evaluation_id"])
            evidence_digest.update(canonical(dict(row)).encode())
            key = f"{row['symbol']}:{row['horizon_seconds']}"
            totals[key]["labels"] += 1
            totals[key]["complete" if row["complete"] else "missing"] += 1
            for endpoint in ("decision", "future"):
                observation = _observation(row[endpoint])
                if observation is not None:
                    identity = observation["event_id"]
                    old = accepted.get(identity)
                    if old is not None and canonical(old) != canonical(observation):
                        raise ValueError("conflicting immutable quote observations")
                    accepted[identity] = observation
            if not row["complete"]:
                missing_count += 1
                failure_reasons[key].update(row["reasons"])
                for endpoint in ("decision", "future"):
                    points[_point(row, endpoint)] = {}
            if len(points) > MAX_POINTS:
                raise ValueError("quote audit point budget exceeded; no truncated audit")
        evaluation_count = connection.scalar(
            select(func.count())
            .select_from(shadow_evaluations)
            .where(
                shadow_evaluations.c.dataset_id == DATASET_ID,
                shadow_evaluations.c.evaluated_at >= start,
                shadow_evaluations.c.evaluated_at < end,
            )
        )
        if (
            not row_count
            or len(evaluations) != evaluation_count
            or row_count != len(evaluations) * len(protocol.horizons)
        ):
            raise ValueError("unwritten horizon rows remain; cannot classify every label")
        raw_references: dict[str, dict[str, Any]] = {}
        ordered_points = sorted(points, key=lambda item: item[1])
        point_index = 0
        native: dict[str, deque[dict[str, Any]]] = defaultdict(deque)
        last_native: dict[str, dict[str, Any]] = {}
        last_message: dict[str, Any] | None = None
        period_connection: str | None = None
        period_connections: set[str] = set()
        reconnects = 0
        raw_counts: dict[str, Counter[str]] = defaultdict(Counter)
        receipt_ages: dict[str, list[float]] = defaultdict(list)
        quote_gaps: dict[str, list[float]] = defaultdict(list)
        previous_native: dict[str, dict[str, Any]] = {}
        previous_event: dict[str, Any] | None = None
        last_reset: dict[str, int] = {}
        integrity: Counter[str] = Counter()

        def capture_points(before_ns: int) -> None:
            nonlocal point_index
            while point_index < len(ordered_points) and ordered_points[point_index][1] < before_ns:
                key = ordered_points[point_index]
                symbol, at_ns, available_ns = key
                candidate = None
                for quote in reversed(native[symbol]):
                    if quote["received_at_ns"] < at_ns - protocol.maximum_quote_age_seconds * 10**9:
                        break
                    if (
                        quote["valid_prices_and_sizes"]
                        and _fresh(quote, at_ns, protocol.maximum_quote_age_seconds)
                        and quote["decoded_at_ns"] is not None
                        and quote["decoded_at_ns"] <= available_ns
                        and quote["received_at_ns"] >= last_reset.get(symbol, 0)
                    ):
                        candidate = quote
                        break
                points[key] = {
                    "last_native": last_native.get(symbol),
                    "fresh_native": candidate,
                    "last_reset_ns": last_reset.get(symbol, 0),
                    "websocket": {
                        "connected_at_target": None,
                        "connected_unknown_reason": "per-evaluation socket state was not archived",
                        "last_message_received_at": (
                            last_message["received_at"] if last_message else None
                        ),
                        "last_message_scope": "retained market/reset messages only",
                        "last_quote_received_at": (
                            last_native[symbol]["received_at"] if symbol in last_native else None
                        ),
                        "connection_id": (last_message["connection_id"] if last_message else None),
                        "observed_connection_changes_since_day_start": reconnects,
                        "exchange_sequence_available": False,
                    },
                }
                point_index += 1

        root = dataset["protocol_hash"]
        batches = 0
        event_count = 0
        statement = (
            select(
                shadow_source_links,
                scalping_market_batches.c.raw,
                scalping_market_batches.c.event_count,
                scalping_market_batches.c.encoding,
            )
            .join(
                scalping_market_batches,
                scalping_market_batches.c.batch_id == shadow_source_links.c.batch_id,
            )
            .where(
                shadow_source_links.c.dataset_id == DATASET_ID,
                shadow_source_links.c.sequence <= dataset["source_batches"],
            )
            .order_by(shadow_source_links.c.sequence)
            .execution_options(stream_results=True, yield_per=4)
        )
        for batch_row in connection.execute(statement).mappings():
            batch = dict(batch_row)
            batches += 1
            if batch["sequence"] != batches or batch["encoding"] != "zlib-json-v1":
                raise ValueError("invalid raw manifest sequence or encoding")
            decoder = zlib.decompressobj()
            raw = decoder.decompress(batch["raw"], MAX_BATCH_BYTES + 1)
            if len(raw) > MAX_BATCH_BYTES or not decoder.eof or decoder.unused_data:
                raise ValueError("raw audit batch exceeds decoding contract")
            if sha256(raw).hexdigest() != batch["batch_id"]:
                raise ValueError("raw content hash mismatch")
            root = sha256((root + batch["batch_id"]).encode()).hexdigest()
            if root != batch["root"]:
                raise ValueError("raw hash chain mismatch")
            body = json.loads(raw)
            if len(body) != batch["event_count"]:
                raise ValueError("raw event count mismatch")
            for value in body:
                event_count += 1
                if event_count > MAX_EVENTS:
                    raise ValueError("raw audit event budget exceeded; no truncated audit")
                received_ns = value["received_at_ns"]
                if previous_event is not None and received_ns < previous_event["received_at_ns"]:
                    raise ValueError("raw receipt order regresses; causal audit cannot proceed")
                capture_points(received_ns)
                in_period = datetime_ns(start) <= received_ns < datetime_ns(end)
                if previous_event is not None and (
                    value["connection_id"] == previous_event["connection_id"]
                ):
                    if value["receive_sequence"] != previous_event["receive_sequence"] + 1:
                        integrity["local_sequence_gaps"] += int(in_period)
                    if value["received_monotonic_ns"] < previous_event["received_monotonic_ns"]:
                        integrity["monotonic_clock_reversals"] += int(in_period)
                if in_period:
                    if (
                        period_connection is not None
                        and period_connection != value["connection_id"]
                    ):
                        reconnects += 1
                    period_connection = value["connection_id"]
                    period_connections.add(period_connection)
                    raw_counts[value["symbol"]][value["event_type"]] += 1
                    integrity["provider_future_timestamps"] += int(
                        value["exchange_at_ns"] > received_ns
                    )
                previous_event = last_message = value
                if value["event_type"] == "reset":
                    last_reset[value["symbol"]] = received_ns
                if value["event_id"] in accepted:
                    raw_references[value["event_id"]] = {
                        "batch_id": batch["batch_id"],
                        "persisted_at": utc(batch["recorded_at"]).isoformat(),
                        "provider_timestamp": value["exchange_timestamp"],
                        "provider_timestamp_ns": value["exchange_at_ns"],
                        "received_at_ns": received_ns,
                        "decoded_at_ns": value.get("decoded_at_ns"),
                    }
                    stored = accepted[value["event_id"]]
                    if (
                        stored["provider_timestamp_ns"] != value["exchange_at_ns"]
                        or stored["received_at_ns"] != received_ns // 1000 * 1000
                    ):
                        raise ValueError("stored quote clocks disagree with raw evidence")
                    if value["event_type"] == "quote":
                        event = MarketEvent.model_validate(value)
                        if any(
                            str(number) != stored[name]
                            for name, number in (
                                ("bid", event.bids[0].price),
                                ("ask", event.asks[0].price),
                                ("bid_size", event.bids[0].quantity),
                                ("ask_size", event.asks[0].quantity),
                            )
                        ):
                            raise ValueError("stored native quote prices disagree with raw")
                if value["event_type"] == "quote":
                    event = MarketEvent.model_validate(value)
                    quote = _raw_quote(event, batch)
                    if in_period:
                        receipt_ages[event.symbol].append(
                            (event.received_at_ns - event.exchange_at_ns) / 1e9
                        )
                        old = previous_native.get(event.symbol)
                        if old is not None:
                            quote_gaps[event.symbol].append(
                                (event.received_at_ns - old["received_at_ns"]) / 1e9
                            )
                            raw_counts[event.symbol]["provider_timestamp_regressions"] += int(
                                event.exchange_at_ns < old["provider_timestamp_ns"]
                            )
                        previous_native[event.symbol] = quote
                    native[event.symbol].append(quote)
                    last_native[event.symbol] = quote
                    cutoff = received_ns - 960 * 10**9
                    while (
                        native[event.symbol] and native[event.symbol][0]["received_at_ns"] < cutoff
                    ):
                        native[event.symbol].popleft()
        capture_points(2**63 - 1)
        if batches != dataset["source_batches"] or root != dataset["source_root"]:
            raise ValueError("snapshot source manifest mismatch")
        if set(accepted) != set(raw_references):
            raise ValueError("stored quote references are missing from the raw manifest")

        classifications: dict[str, Counter[str]] = defaultdict(Counter)
        examples: dict[str, list[dict[str, Any]]] = defaultdict(list)
        cases_file = output_dir / "missing-labels.jsonl.gz"
        written = 0
        second_digest = sha256()
        with gzip.open(cases_file, "wt", encoding="utf-8", newline="\n") as cases:
            for row in _labels(connection, start, end):
                second_digest.update(canonical(dict(row)).encode())
                if row["complete"]:
                    continue
                record: dict[str, Any] = {
                    "evaluation_id": row["evaluation_id"],
                    "symbol": row["symbol"],
                    "evaluation_time": utc(row["evaluated_at"]).isoformat(),
                    "horizon_seconds": row["horizon_seconds"],
                    "horizon_time": utc(row["deadline_at"]).isoformat(),
                    "label_resolved_at": utc(row["resolved_at"]).isoformat(),
                    "label_failure_reasons": row["reasons"],
                    "endpoints": {},
                }
                for endpoint in ("decision", "future"):
                    point_key = _point(row, endpoint)
                    point = points[point_key]
                    selected_observation = _observation(row[endpoint])
                    if selected_observation is not None:
                        selected_observation = {
                            **selected_observation,
                            "raw_reference": raw_references[selected_observation["event_id"]],
                        }
                    classification = classify_endpoint(
                        selected_observation,
                        point,
                        at_ns=point_key[1],
                        available_ns=point_key[2],
                        maximum_age_seconds=protocol.maximum_quote_age_seconds,
                        accepted=accepted,
                    )
                    stale_reason = (
                        "DECISION_QUOTE_STALE_OR_MISSING"
                        if endpoint == "decision"
                        else "HORIZON_QUOTE_STALE_OR_MISSING"
                    )
                    if (
                        stale_reason in row["reasons"]
                        and classification == "STORED_QUOTE_SATISFIES_FRESHNESS"
                    ):
                        classification = "C_FRESH_STORED_QUOTE_REJECTED"
                    detail = {
                        "classification": classification,
                        "stored_quote": quote_at(selected_observation, point_key[1]),
                        "last_raw_native_quote": quote_at(point["last_native"], point_key[1]),
                        "fresh_raw_native_candidate": quote_at(point["fresh_native"], point_key[1]),
                        "websocket": point["websocket"],
                        "freshness_seconds_unchanged": protocol.maximum_quote_age_seconds,
                    }
                    record["endpoints"][endpoint] = detail
                    count_key = f"{row['symbol']}:{row['horizon_seconds']}:{endpoint}"
                    classifications[count_key][classification] += 1
                    example_key = f"{row['symbol']}:{endpoint}:{classification}"
                    if len(examples[example_key]) < 2:
                        examples[example_key].append(
                            {
                                "evaluation_id": row["evaluation_id"],
                                "horizon_seconds": row["horizon_seconds"],
                                **detail,
                            }
                        )
                record["other_failure_modes"] = [
                    reason
                    for reason in row["reasons"]
                    if reason
                    not in {"DECISION_QUOTE_STALE_OR_MISSING", "HORIZON_QUOTE_STALE_OR_MISSING"}
                ]
                cases.write(canonical(record) + "\n")
                written += 1
        if written != missing_count:
            raise ValueError("missing-label audit is incomplete")
        if second_digest.digest() != evidence_digest.digest():
            raise ValueError("immutable label input changed during the audit")
        issues = sum(
            count
            for counts in classifications.values()
            for name, count in counts.items()
            if name
            not in {
                "STORED_QUOTE_SATISFIES_FRESHNESS",
                "A_NO_NATIVE_QUOTE_RECORDED",
                "D_UPSTREAM_INACTIVITY_STALE_QUOTE",
                "D_UPSTREAM_PROVIDER_TIMESTAMP_STALE",
            }
        )
        source_evidence_available = all(
            raw_counts[symbol]["quote"] > 0 for symbol in protocol.symbols
        )
        conclusion = (
            "additional_pipeline_or_clock_investigation_required"
            if issues or any(integrity.values())
            else "source_incompatible_with_frozen_freshness"
            if source_evidence_available
            else "insufficient_source_evidence"
        )
        summary = {
            "schema": "shadow-quote-availability-audit-v1",
            "report_date": report_date.isoformat(),
            "dataset_id": DATASET_ID,
            "protocol_hash": protocol.identity,
            "source_snapshot": {"batches": batches, "root": root, "events": event_count},
            "immutable_label_input_sha256": evidence_digest.hexdigest(),
            "evaluations": len(evaluations),
            "labels": row_count,
            "missing_labels_audited": written,
            "all_missing_labels_classified": True,
            "label_counts": {k: dict(v) for k, v in sorted(totals.items())},
            "failure_reason_counts": {k: dict(v) for k, v in sorted(failure_reasons.items())},
            "endpoint_classifications": {k: dict(v) for k, v in sorted(classifications.items())},
            "raw_day_counts": {k: dict(v) for k, v in raw_counts.items()},
            "native_quote_receipt_age_seconds": {k: _quantiles(v) for k, v in receipt_ages.items()},
            "native_quote_receive_gap_seconds": {k: _quantiles(v) for k, v in quote_gaps.items()},
            "integrity": dict(integrity),
            "day_connection_ids": sorted(period_connections),
            "observed_day_connection_changes": reconnects,
            "raw_quote_references_verified": len(raw_references),
            "unexplained_or_pipeline_endpoint_findings": issues,
            "conclusion": conclusion,
            "limitations": [
                "Local continuity cannot prove complete delivery without an exchange sequence.",
                "Historical socket state and unselected processing times were not archived.",
                "Decoding a fresh raw quote does not prove collector availability.",
                "Endpoint counts overlap; they are not additional missing-label denominators.",
            ],
            "examples": dict(examples),
            "artifacts": [
                {
                    "path": cases_file.name,
                    "bytes": cases_file.stat().st_size,
                    "sha256": sha256(cases_file.read_bytes()).hexdigest(),
                    "records": written,
                }
            ],
            "database_read_only": True,
            "v1_modified": False,
            "labels_backfilled": 0,
            "orders_submitted": 0,
            "promotion_allowed": False,
        }
    (output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8", newline="\n"
    )
    return summary
