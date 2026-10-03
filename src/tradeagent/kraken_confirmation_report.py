"""Streaming, read-only confirmation reports on the complete predeclared grid."""

from __future__ import annotations

from collections import Counter, deque
from collections.abc import Iterator
from datetime import UTC, datetime
from hashlib import sha256

from pydantic import JsonValue

from tradeagent.kraken_confirmation import (
    NS,
    SLOTS,
    SYMBOLS,
    ChunkHeader,
    ConfirmationProtocol,
    ConfirmationStore,
    Eligibility,
    Evaluation,
    Evidence,
    Label,
    Quote,
    slot_context,
)
from tradeagent.persistence import Database
from tradeagent.scalping_market import datetime_ns, ns_datetime
from tradeagent.scalping_store import canonical

PASS = "data confirmation pass, v2 blocked until venue cost assumptions"
BTC_ONLY = "consider new BTC-only protocol, not current pass"
NO_GO = "NO_GO"
BAD_SOURCE_COUNTS = (
    "checksum_failures",
    "disconnects",
    "timestamp_errors",
    "protocol_errors",
    "clock_errors",
    "backpressure_failures",
    "cache_budget_failures",
    "unverified_book_updates",
    "persistence_failures",
)


def final_decision(
    btc_complete: int,
    eth_complete: int,
    *,
    clean: bool,
    completed: bool,
    eth_size_only_failures: int,
) -> str:
    if not clean or not completed:
        return NO_GO
    if btc_complete * 100 >= SLOTS * 95 and eth_complete * 100 >= SLOTS * 95:
        return PASS
    if (
        btc_complete * 100 >= SLOTS * 95
        and eth_complete * 100 < SLOTS * 95
        and eth_size_only_failures >= SLOTS - eth_complete
    ):
        return BTC_ONLY
    return NO_GO


def _records(payload: dict[str, JsonValue]) -> list[Evidence]:
    rows = payload.get("records")
    if not isinstance(rows, list):
        raise ValueError("invalid durable evidence records")
    return [Evidence.model_validate(row) for row in rows]


def _evaluation(protocol: ConfirmationProtocol, evaluation: Evaluation) -> None:
    if evaluation.at_ns != protocol.slot_ns(evaluation.slot):
        raise ValueError("durable evaluation differs from the frozen scheduled grid")
    if evaluation.recorded_ns < evaluation.at_ns:
        raise ValueError("evaluation recorded before its scheduled slot")
    if evaluation.missed_scheduled_slot and evaluation.entry is not None:
        raise ValueError("missed scheduled slot cannot be favorably replaced")
    if evaluation.entry is not None and (
        evaluation.entry.symbol != evaluation.symbol
        or evaluation.entry.accepted_ns > evaluation.at_ns
    ):
        raise ValueError("invalid entry quote provenance")


class _ProofRing:
    def __init__(self) -> None:
        self.rows: dict[tuple[int, str], tuple[int, str]] = {}
        self.expiry: deque[tuple[int, tuple[int, str]]] = deque()
        self.last_frame = 0
        self.frames = 0

    def capture(self, record: Evidence, sequence: int) -> None:
        frame = record.payload.get("frame_id")
        quotes = record.payload.get("quotes")
        if not isinstance(frame, int) or isinstance(frame, bool) or frame != self.last_frame + 1:
            raise ValueError("raw frame sequence is missing, duplicated or reordered")
        if not isinstance(quotes, list) or not isinstance(record.payload.get("payload_text"), str):
            raise ValueError("raw frame/quote proof missing")
        for value in quotes:
            quote = Quote.model_validate(value)
            if quote.frame_id != frame:
                raise ValueError("quote does not reference its own raw frame")
            key: tuple[int, str] = (frame, quote.symbol)
            if key in self.rows:
                raise ValueError("duplicate quote proof in one frame")
            digest = sha256(canonical(quote.model_dump(mode="json")).encode()).hexdigest()
            self.rows[key] = (sequence, digest)
            self.expiry.append((quote.accepted_ns, key))
        self.last_frame, self.frames = frame, self.frames + 1
        while self.expiry and self.expiry[0][0] < record.at_ns - 120 * NS:
            _, key = self.expiry.popleft()
            del self.rows[key]
        if len(self.rows) > 120_000:
            raise ValueError("verification quote proof ring budget exceeded")

    def check(self, quote: Quote | None, header: ChunkHeader) -> None:
        if quote is None:
            return
        proof = self.rows.get((quote.frame_id, quote.symbol))
        digest = sha256(canonical(quote.model_dump(mode="json")).encode()).hexdigest()
        if proof is None or proof[0] >= header.sequence or proof[1] != digest:
            raise ValueError("quote raw proof was not durably appended before its consumer")


def build_report(
    store: ConfirmationStore,
    *,
    at_ns: int,
    verify_payloads: bool = False,
    completing: bool = False,
) -> dict[str, JsonValue]:
    protocol = store.load_protocol()
    mature = protocol.matured(at_ns)
    evaluations: dict[tuple[int, str], bool] = {}
    evaluation_hashes: dict[tuple[int, str], str] = {}
    labels: dict[tuple[int, str], Eligibility] = {}
    quality: dict[str, JsonValue] = {}
    terminal: dict[str, JsonValue] | None = None
    latest: ChunkHeader | None = None
    proof = _ProofRing()
    guard_checks = 0
    kinds = frozenset({"protocol", "evaluation", "label", "quality", "guard", "terminal"})
    for header, payload in store.chunks(verify_payloads=verify_payloads, kinds=kinds):
        latest = header
        if header.kind == "capture" and verify_payloads:
            if payload is None:
                raise ValueError("capture verification payload unavailable")
            for row in _records(payload):
                proof.capture(row, header.sequence)
        elif header.kind in {"evaluation", "label"}:
            if payload is None:
                raise ValueError("evaluation/label payload unavailable")
            for row in _records(payload):
                label = Label.model_validate(row.payload) if header.kind == "label" else None
                evaluation = (
                    label.evaluation
                    if label is not None
                    else Evaluation.model_validate(row.payload)
                )
                _evaluation(protocol, evaluation)
                key: tuple[int, str] = (evaluation.slot, evaluation.symbol)
                if header.kind == "evaluation":
                    if key in evaluations:
                        raise ValueError("duplicate durable scheduled evaluation")
                    evaluations[key] = evaluation.missed_scheduled_slot
                    evaluation_hashes[key] = sha256(
                        canonical(evaluation.model_dump(mode="json")).encode(),
                    ).hexdigest()
                    if verify_payloads:
                        proof.check(evaluation.entry, header)
                else:
                    if (
                        label is None
                        or key not in evaluations
                        or key not in evaluation_hashes
                        or key in labels
                    ):
                        raise ValueError("label missing its preceding unique durable evaluation")
                    if (
                        evaluation_hashes.pop(key)
                        != sha256(
                            canonical(evaluation.model_dump(mode="json")).encode(),
                        ).hexdigest()
                    ):
                        raise ValueError(
                            "label entry differs from the preceding durable evaluation"
                        )
                    if label.future is not None and label.future.symbol != evaluation.symbol:
                        raise ValueError("cross-symbol horizon quote")
                    if verify_payloads:
                        proof.check(label.future, header)
                    if label.resolved_ns <= at_ns:
                        labels[key] = label.eligibility
        elif header.kind == "quality" and payload is not None:
            quality = payload
        elif header.kind == "guard":
            if payload is None or any(
                payload.get(name) != value
                for name, value in {
                    "account_digest": protocol.account_digest,
                    "paper_host": "https://paper-api.alpaca.markets",
                    "read_methods": ["GET"],
                    "trading_authorization": "expired",
                    "model_state": "no_support",
                    "order_attempts": 0,
                    "positions": 0,
                    "open_orders": 0,
                    "broker_order_records_since_freeze": 0,
                    "authorization_renewed": False,
                    "v1_runtime_sha": protocol.v1_runtime_sha,
                    "v1_owner_id": protocol.v1_owner_id,
                    "v1_connection_id": protocol.v1_connection_id,
                }.items()
            ):
                raise ValueError("durable observer guard evidence violates frozen safety pins")
            guard_checks += 1
        elif header.kind == "terminal" and payload is not None:
            terminal = payload
    if latest is None:
        raise ValueError("no durable confirmation evidence")
    counts_value = quality.get("counts", {})
    if not isinstance(counts_value, dict):
        raise ValueError("invalid quality counts")
    counts: dict[str, int] = {}
    for name, value in counts_value.items():
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError("invalid durable source counter")
        counts[name] = value
    raw_counts_match = proof.frames == counts.get("frames", 0)
    if verify_payloads and completing and not raw_counts_match:
        raise ValueError("raw frame count disagrees with durable quality counters")
    statistics: dict[tuple[str, str], Counter[str]] = {}
    reasons: dict[tuple[str, str], Counter[str]] = {}
    for slot in range(SLOTS):
        contexts = ("overall", *slot_context(protocol, slot))
        for symbol in SYMBOLS:
            key = (slot, symbol)
            metrics = labels.get(key) if slot < mature else None
            for context in contexts:
                for scope in (symbol, "ALL"):
                    group = (context, scope)
                    stats = statistics.setdefault(group, Counter())
                    taxonomy = reasons.setdefault(group, Counter())
                    stats["scheduled"] += 1
                    if slot >= mature:
                        continue
                    stats["matured"] += 1
                    stats["evaluations_written"] += int(key in evaluations)
                    stats["missed_scheduled_slots"] += int(evaluations.get(key, False))
                    if metrics is None:
                        taxonomy["OVERDUE_UNWRITTEN_LABEL"] += 1
                        continue
                    stats["labels_written"] += 1
                    for name in (
                        "complete",
                        "integrity_eligible",
                        "entry_fresh",
                        "future_fresh",
                        "entry_size",
                        "future_size",
                    ):
                        stats[name] += int(getattr(metrics, name))
                    stats["freshness_eligible"] += int(metrics.entry_fresh and metrics.future_fresh)
                    stats["size_eligible"] += int(metrics.entry_size and metrics.future_size)
                    stats["size_only_missing"] += int(
                        metrics.integrity_eligible
                        and metrics.entry_fresh
                        and metrics.future_fresh
                        and not (metrics.entry_size and metrics.future_size)
                    )
                    taxonomy.update(metrics.reasons)
    blocks: list[JsonValue] = []
    for (context, group_symbol), stats in sorted(statistics.items()):
        denominator = stats["matured"]
        values: dict[str, JsonValue] = {
            "block": context,
            "symbol": group_symbol,
            **{
                name: stats[name]
                for name in (
                    "scheduled",
                    "matured",
                    "evaluations_written",
                    "missed_scheduled_slots",
                    "labels_written",
                    "complete",
                    "integrity_eligible",
                    "entry_fresh",
                    "future_fresh",
                    "freshness_eligible",
                    "entry_size",
                    "future_size",
                    "size_eligible",
                    "size_only_missing",
                )
            },
            "missing": denominator - stats["complete"],
            "coverage": stats["complete"] / denominator if denominator else None,
            "missing_label_records": denominator - stats["labels_written"],
            "not_yet_mature": stats["scheduled"] - denominator,
            "reasons": dict(reasons[(context, group_symbol)]),
        }
        blocks.append(values)
    completed = (
        (completing or (terminal is not None and terminal.get("state") == "completed"))
        and mature == SLOTS
        and at_ns >= protocol.capture_end_ns
    )
    active_store = ConfirmationStore(store.database, latest.owner_id, clock=store.clock)
    state = (
        str(terminal["state"])
        if terminal is not None
        else "completed"
        if completed
        else "collecting"
        if active_store.owner_alive()
        else "failed_incomplete_process_lost_no_restart"
    )
    saved_report = terminal.get("report") if terminal is not None else None
    clean = bool(
        completed
        and guard_checks >= 2
        and not any(counts.get(name, 0) for name in BAD_SOURCE_COUNTS)
        and (not verify_payloads or raw_counts_match)
        and statistics[("overall", "ALL")]["labels_written"] == SLOTS * 2
        and (
            verify_payloads
            or (
                isinstance(saved_report, dict)
                and saved_report.get("full_payload_verification") is True
                and saved_report.get("clean_integrity") is True
            )
        )
    )
    btc, eth = statistics[("overall", SYMBOLS[0])], statistics[("overall", SYMBOLS[1])]
    return {
        "schema": "kraken-book-confirmation-report-v1",
        "protocol": protocol.model_dump(mode="json"),
        "protocol_hash": protocol.identity,
        "as_of": ns_datetime(at_ns).isoformat(),
        "state": state,
        "blocks": blocks,
        "counts": {name: value for name, value in counts.items()},
        "latest_quality": quality,
        "guard_checks": guard_checks,
        "hash_chain_root": latest.chain_hash,
        "journal_chunks": latest.sequence + 1,
        "payload_bytes_charged": latest.stored_bytes,
        "full_payload_verification": verify_payloads,
        "raw_frames_verified": proof.frames if verify_payloads else None,
        "raw_frame_counter_consistent": raw_counts_match if verify_payloads else None,
        "clean_integrity": clean,
        "decision": final_decision(
            btc["complete"],
            eth["complete"],
            clean=clean,
            completed=completed,
            eth_size_only_failures=eth["size_only_missing"],
        ),
        "v2_ready": False,
        "account_specific_venue_costs_verified": False,
        "eligibility_semantics": {
            "denominator": "full predeclared scheduled grid that has reached 60s + 2s settlement",
            "size_eligible": (
                "observed entry ask and horizon bid displayed USD notionals >= 10.25; "
                "independent of freshness, not trading eligibility; missing quotes fail this metric"
            ),
            "complete": (
                "joint primary data-label contract, not permission to trade or evidence of "
                "account-specific venue costs/eligibility"
            ),
        },
        "orders_submitted": 0,
        "private_kraken_requests": 0,
        "terminal_reason": terminal.get("reason")
        if terminal
        else (
            "process loss inferred from expired study-specific lease; "
            "immutable claim prevents resume"
            if state == "failed_incomplete_process_lost_no_restart"
            else None
        ),
        "checksum_scope": "local top-ten reconstruction consistency, not exchange completeness",
        "storage_scope": (
            "bounded payload charge only; database/WAL/index/free capacity not certified"
        ),
    }


def load_report(database: Database, *, verify_payloads: bool = False) -> dict[str, JsonValue]:
    """Read-only terminal/status API; an expired owner never becomes a resumable run."""
    return build_report(
        ConfirmationStore(database, "read-only"),
        at_ns=datetime_ns(datetime.now(UTC)),
        verify_payloads=verify_payloads,
    )


def export_evidence(database: Database) -> Iterator[dict[str, JsonValue]]:
    """Read-only streaming proof: exact compressed bytes, hashes and journal order."""
    yield from ConfirmationStore(database, "read-only").export_chunks()
