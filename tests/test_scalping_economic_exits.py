from datetime import timedelta

import pytest
from sqlalchemy import select, update
from test_scalping_execution import (
    NOW,
    advance,
    quote,
    signal,
)
from test_scalping_execution import (
    setup as execution_setup,
)

from tradeagent.scalping_config import ScalpingConfig
from tradeagent.scalping_store import scalping_cycles

setup = execution_setup


def economic_cycle(setup):
    database, _, _, clock, make = setup
    engine = make()
    engine.initialize()
    engine.step((signal(clock[0], "economic-entry"),), {"BTC/USD": quote(clock[0])}, now=clock[0])
    with database.begin() as connection:
        row = dict(connection.execute(select(scalping_cycles)).mappings().one())
        frozen = ScalpingConfig.model_validate(
            {
                **row["payload"]["config"],
                "decision_policy": "action-value-v1",
                "catastrophic_stop_bps": "100",
                "latency_grace_seconds": 1,
            }
        )
        connection.execute(
            update(scalping_cycles)
            .where(scalping_cycles.c.cycle_id == row["cycle_id"])
            .values(
                payload={**row["payload"], "config": frozen.model_dump(mode="json")},
                exit_due_at=NOW + timedelta(seconds=6),
            )
        )
    return engine


def row(setup):
    with setup[0].begin() as connection:
        return dict(connection.execute(select(scalping_cycles)).mappings().one())


def test_time_stop_is_latched_during_broker_backoff_without_another_request(setup):
    engine = economic_cycle(setup)
    _, broker, _, _, _ = setup
    engine._retry_after = NOW + timedelta(seconds=30)
    now = advance(setup, 7)
    posts = len(broker.posts)
    status = engine.step((), {"BTC/USD": quote(now)}, now=now)
    current = row(setup)
    assert current["payload"]["exit_reason"] == "model_horizon_expired"
    assert current["payload"]["exit_requested"] is True
    assert len(broker.posts) == posts
    assert status["overdue_owned_cycles"][0]["overdue_seconds"] == 1
    assert status["overdue_owned_cycles"][0]["broker_backoff_until"] is not None


def test_economic_execution_gateway_rejects_a_bullish_signal_without_economic_evidence(setup):
    _, broker, _, clock, make = setup
    engine = make(decision_policy="action-value-v1", catastrophic_stop_bps="100")
    engine.initialize()
    engine.step((signal(clock[0], "unpriced-bullish"),), {"BTC/USD": quote(clock[0])}, now=clock[0])
    assert broker.posts == []
    assert row_count(setup) == 0


def row_count(setup):
    with setup[0].begin() as connection:
        return len(list(connection.execute(select(scalping_cycles.c.cycle_id))))


def test_catastrophic_stop_uses_actual_entry_vwap_and_latches_original_evidence(setup):
    engine = economic_cycle(setup)
    now = advance(setup, 1)
    engine._request_exits((), now, {"BTC/USD": quote(now, bid="98.99", ask="99")})
    current = row(setup)
    evidence = current["payload"]["exit_evidence"]
    assert current["payload"]["exit_reason"] == "catastrophic_stop"
    assert evidence["opening_vwap"] == "100"
    assert evidence["hard_stop_price"] == "99.00"
    assert evidence["fresh_quote_available"] is True
    later = advance(setup, 10)
    engine._request_exits((), later, {"BTC/USD": quote(later, bid="101", ask="101.01")})
    assert row(setup)["payload"]["exit_evidence"] == evidence


def test_missing_protection_quote_requests_owned_recovery_without_fake_price(setup):
    engine = economic_cycle(setup)
    now = advance(setup, 1)
    engine._request_exits((), now, {})
    current = row(setup)
    assert current["payload"]["exit_reason"] == "market_data_unavailable"
    assert current["payload"]["exit_evidence"]["quote"] is None
    assert current["payload"]["exit_evidence"]["protection_kind"] == "local_not_guaranteed"


@pytest.mark.parametrize("delay", [0, 1, 3])
def test_economic_deadline_does_not_restart_from_late_fill_receipt(setup, delay):
    engine = economic_cycle(setup)
    now = advance(setup, delay)
    engine._refresh_cycles(now)
    actual = row(setup)["exit_due_at"]
    assert actual.replace(tzinfo=NOW.tzinfo) == NOW + timedelta(seconds=6)
