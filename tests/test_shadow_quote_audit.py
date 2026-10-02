import argparse
import gzip
import json
import zlib
from datetime import datetime, timedelta

import pytest
from sqlalchemy import select, update
from test_shadow_dataset import event, protocol, refresh
from test_shadow_dataset import store_setup as dataset_setup

from tradeagent.scalping_cli import COMMANDS, register_scalping_commands
from tradeagent.scalping_market import datetime_ns
from tradeagent.scalping_store import canonical, scalping_market_batches
from tradeagent.shadow_dataset import (
    WINDOW_START,
    ShadowDatasetCollector,
    shadow_evaluations,
    shadow_labels,
)
from tradeagent.shadow_quote_audit import audit_quotes, classify_endpoint, quote_at

store_setup = dataset_setup


@pytest.fixture(autouse=True)
def fixed_audit_clock(monkeypatch):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return WINDOW_START + timedelta(days=2)

    monkeypatch.setattr("tradeagent.shadow_quote_audit.datetime", Clock)


def native(at, **changes):
    value = {
        "event_id": "quote",
        "source_kind": "native_quote",
        "provider_timestamp_ns": datetime_ns(at - timedelta(milliseconds=30)),
        "received_at_ns": datetime_ns(at),
        "decoded_at_ns": datetime_ns(at),
        "persisted_at": at.isoformat(),
        "valid_prices_and_sizes": True,
    }
    return {**value, **changes}


def classify(stored, candidate=None, last=None, *, accepted=None, available=None, reset=0):
    at = WINDOW_START + timedelta(seconds=10)
    return classify_endpoint(
        stored,
        {"fresh_native": candidate, "last_native": last, "last_reset_ns": reset},
        at_ns=datetime_ns(at),
        available_ns=datetime_ns(available or at),
        maximum_age_seconds=2,
        accepted=accepted or {},
    )


def test_native_absence_and_true_staleness_are_not_software_failures():
    assert classify(None) == "A_NO_NATIVE_QUOTE_RECORDED"
    old = native(WINDOW_START)
    assert classify(None, last=old) == "D_UPSTREAM_INACTIVITY_STALE_QUOTE"
    late = native(
        WINDOW_START + timedelta(seconds=9), provider_timestamp_ns=datetime_ns(WINDOW_START)
    )
    assert classify(None, last=late) == "D_UPSTREAM_PROVIDER_TIMESTAMP_STALE"
    assert (
        quote_at(old, datetime_ns(WINDOW_START + timedelta(seconds=10)))["receive_age_seconds"]
        == 10
    )


def test_raw_receipt_is_not_assumed_to_prove_collector_availability():
    at = WINDOW_START + timedelta(seconds=9)
    q = native(at, persisted_at=(at + timedelta(seconds=10)).isoformat())
    assert classify(None, q, q) == "PROCESSING_AVAILABILITY_NOT_RECORDED"
    assert classify(None, native(at), native(at)) == "B_OR_C_CAPTURED_FRESH_QUOTE_NOT_SELECTED"
    proof = {"processed_at": at.isoformat(), "epoch": "session:1"}
    assert classify(None, q, q, accepted={"quote": proof}) == "C_ACCEPTED_FRESH_QUOTE_NOT_SELECTED"
    delayed = {"processed_at": (at + timedelta(seconds=3)).isoformat(), "epoch": "session:1"}
    assert (
        classify(None, q, q, accepted={"quote": delayed}) == "PROCESSING_AVAILABILITY_NOT_RECORDED"
    )


def test_reset_and_clock_boundaries_are_explicit():
    q = native(WINDOW_START + timedelta(seconds=9))
    assert classify(None, None, q, reset=datetime_ns(WINDOW_START + timedelta(seconds=9.5))) == (
        "CONNECTIVITY_RESET_INVALIDATED_QUOTE"
    )
    future = native(WINDOW_START + timedelta(seconds=11))
    assert classify(future) == "CLOCK_OR_CAUSALITY_VIOLATION"
    assert classify(q) == "STORED_QUOTE_SATISFIES_FRESHNESS"


def populate(database, repo, store, p):
    collector = ShadowDatasetCollector(p)
    first = event(WINDOW_START, sequence=1)
    collector.on_market(first, first.received_at)
    at = WINDOW_START + timedelta(milliseconds=100)
    evaluations = collector.evaluate(at)
    # The later quote is received after the five-second deadline.
    later = event(at + timedelta(seconds=5.1), sequence=2)
    collector.on_market(later, later.received_at)
    labels = collector.resolve(at + timedelta(seconds=903))
    refresh(repo, first.received_at)
    store.persist_tape((first,), first.received_at)
    refresh(repo, later.received_at)
    store.persist_tape((later,), later.received_at)
    refresh(repo, at + timedelta(seconds=904))
    store.write(evaluations + labels, at + timedelta(seconds=904))


def evidence(database):
    with database.begin() as connection:
        return canonical(
            {
                "evaluations": [
                    dict(r) for r in connection.execute(select(shadow_evaluations)).mappings()
                ],
                "labels": [dict(r) for r in connection.execute(select(shadow_labels)).mappings()],
                "raw": [
                    dict(r) for r in connection.execute(select(scalping_market_batches)).mappings()
                ],
            }
        )


def test_audit_classifies_every_missing_horizon_without_lookahead_or_writes(store_setup, tmp_path):
    database, repo, store, p = store_setup
    populate(database, repo, store, p)
    before = evidence(database)
    output = tmp_path / "audit"
    result = audit_quotes(database, report_date=WINDOW_START.date(), output_dir=output)
    assert result["labels"] == 10 and result["missing_labels_audited"] == 10
    assert result["raw_quote_references_verified"] == 2
    assert result["all_missing_labels_classified"] is True
    assert result["v1_modified"] is False and result["labels_backfilled"] == 0
    assert evidence(database) == before
    with gzip.open(output / "missing-labels.jsonl.gz", "rt", encoding="utf-8") as source:
        records = [json.loads(line) for line in source]
    btc5 = next(r for r in records if r["symbol"] == "BTC/USD" and r["horizon_seconds"] == 5)
    assert btc5["endpoints"]["future"]["fresh_raw_native_candidate"] is None
    assert btc5["endpoints"]["future"]["last_raw_native_quote"]["event_id"].endswith(":1")
    assert btc5["endpoints"]["decision"]["stored_quote"]["raw_reference"]["batch_id"]
    assert btc5["endpoints"]["future"]["websocket"]["connected_at_target"] is None
    with pytest.raises(ValueError, match="overwrite"):
        audit_quotes(database, report_date=WINDOW_START.date(), output_dir=output)


def test_missing_raw_reference_and_content_corruption_never_pass(store_setup, tmp_path):
    database, repo, store, p = store_setup
    populate(database, repo, store, p)
    with database.begin() as connection:
        connection.execute(update(scalping_market_batches).values(raw=b"not-zlib"))
    with pytest.raises(zlib.error):
        audit_quotes(database, report_date=WINDOW_START.date(), output_dir=tmp_path / "audit")
    assert not (tmp_path / "audit" / "summary.json").exists()


def test_cli_registers_explicit_read_only_audit():
    parser = argparse.ArgumentParser()
    register_scalping_commands(parser.add_subparsers(dest="command"))
    args = parser.parse_args(
        [
            "shadow-dataset-quote-audit",
            "--report-date",
            "2026-10-01",
            "--output-dir",
            "audit",
        ]
    )
    assert args.command in COMMANDS
    assert args.report_date == WINDOW_START.date()
    assert protocol().maximum_quote_age_seconds == 2
