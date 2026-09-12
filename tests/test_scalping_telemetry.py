from datetime import timedelta

from sqlalchemy import select
from test_scalping_execution import ACCOUNT, NOW, quote, signal
from test_scalping_market import Tape

from tradeagent.persistence import Database, event_reporting_metadata, events
from tradeagent.scalping_market import datetime_ns
from tradeagent.scalping_telemetry import LatencyWindow, ScalpTelemetry


def test_stage_latency_has_quantiles_and_never_converts_unknown_to_zero():
    window = LatencyWindow()
    for value in range(1, 101):
        window.observe(
            {"t6_submitted_ns": 1_000_000, "t7_acknowledged_ns": 1_000_000 + value * 1_000_000},
            ("send_ack",),
        )
    window.observe({"t8_filled_ns": 10}, ("decision_fill",))
    window.observe({"t7_acknowledged_ns": 20, "t8_filled_ns": 10}, ("ack_fill",))
    stages = window.snapshot()["stages"]
    assert stages["send_ack"]["p50"] == 50
    assert stages["send_ack"]["p95"] == 95
    assert stages["send_ack"]["p99"] == 99
    assert stages["decision_fill"]["p99"] is None
    assert stages["decision_fill"]["missing_samples"] == 1
    assert stages["ack_fill"]["p99"] is None
    assert stages["ack_fill"]["invalid_or_fill_before_ack_samples"] == 1


def test_candidate_timing_and_no_trade_aggregation_are_durable_and_do_not_email(tmp_path):
    with Database(f"sqlite:///{tmp_path / 'telemetry.db'}") as database:
        database.initialize()
        recorder = ScalpTelemetry(database, account_digest=ACCOUNT, started_at=NOW)
        candidate = signal(NOW, "economic-veto").model_copy(
            update={"action": "hold", "reasons": ("non_positive_action_value",)}
        )
        recorder.record_decision(candidate, run_id="run", now=NOW)
        recorder.record_decision(candidate, run_id="run", now=NOW)
        neutral = candidate.model_copy(
            update={"decision_id": "neutral", "family": "none", "reasons": ("uncertain_regime",)}
        )
        recorder.record_decision(neutral, run_id="run", now=NOW + timedelta(seconds=60))
        with database.begin() as connection:
            records = list(connection.execute(select(events)).mappings())
            metadata = list(connection.execute(select(event_reporting_metadata)).mappings())
        assert len(records) == len(metadata) == 2
        by_type = {row["event_type"]: row["payload"] for row in records}
        assert by_type["scalp_candidate_decision"]["signal"]["action"] == "hold"
        assert by_type["scalp_no_trade_observations"]["reason_counts"] == {"uncertain_regime": 1}
        assert recorder.snapshot()["neutral_observations_pending_flush"] == 0


def test_receipt_decoding_and_state_clocks_follow_the_exact_source_event(tmp_path):
    with Database(f"sqlite:///{tmp_path / 'clocks.db'}") as database:
        database.initialize()
        recorder = ScalpTelemetry(database, account_digest=ACCOUNT, started_at=NOW)
        event = Tape().book(0, snapshot=True)
        at = event.received_at
        decoded = event.model_copy(update={"decoded_at_ns": datetime_ns(at) + 1000})
        recorder.on_market(decoded, None, state_updated_at=at + timedelta(microseconds=3))
        times = recorder.decision_timing(
            decoded.event_id,
            features_started_at=at + timedelta(microseconds=5),
            features_at=at + timedelta(microseconds=8),
            model_at=at + timedelta(microseconds=12),
        )
        assert times["t0_received_ns"] == decoded.received_at_ns
        assert times["t1_decoded_ns"] == decoded.decoded_at_ns
        assert recorder.latencies.snapshot()["stages"]["feature_compute"]["p50"] == 0.003
        missing = recorder.decision_timing(
            "not-recorded",
            features_started_at=at,
            features_at=at,
            model_at=at,
        )
        assert missing["t0_received_ns"] is None
        assert recorder.latencies.snapshot()["stages"]["feed_processing"]["missing_samples"] == 1


def test_quote_cache_is_bounded_and_tracks_loss_of_old_samples(tmp_path):
    with Database(f"sqlite:///{tmp_path / 'cache.db'}") as database:
        database.initialize()
        recorder = ScalpTelemetry(database, account_digest=ACCOUNT, started_at=NOW)
        recorder._capacity = 2
        tape = Tape()
        for index in range(3):
            event = tape.event("q", index, bp="100", bs="10", ap="100.01", **{"as": "10"})
            value = quote(event.received_at).model_copy(
                update={
                    "exchange_at": event.exchange_at,
                    "exchange_time_ns": event.exchange_at_ns,
                }
            )
            recorder.on_market(event, value, state_updated_at=event.received_at)
        status = recorder.snapshot()
        assert status["quote_cache_size"] == 2
        assert status["quote_cache_evictions"] == 1


def test_shutdown_flushes_partial_neutral_window(tmp_path):
    with Database(f"sqlite:///{tmp_path / 'flush.db'}") as database:
        database.initialize()
        recorder = ScalpTelemetry(database, account_digest=ACCOUNT, started_at=NOW)
        neutral = signal(NOW, "neutral").model_copy(
            update={"action": "hold", "family": "none", "reasons": ("no_positive_alpha",)}
        )
        recorder.record_decision(neutral, run_id="run", now=NOW)
        recorder.flush(run_id="run", now=NOW + timedelta(seconds=5))
        with database.begin() as connection:
            row = connection.execute(select(events)).mappings().one()
        assert row["event_type"] == "scalp_no_trade_observations"
        assert row["payload"]["reason_counts"] == {"no_positive_alpha": 1}
