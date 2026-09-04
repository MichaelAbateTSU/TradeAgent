from __future__ import annotations

from datetime import datetime

from tradeagent.domain import MarketBar
from tradeagent.ledger import SQLiteLedger


def test_ledger_is_append_only_and_returns_latest_first(
    bar: MarketBar, timestamp: datetime
) -> None:
    with SQLiteLedger(":memory:") as ledger:
        first = ledger.append("bar", bar, occurred_at=timestamp, trace_id="trace-1")
        second = ledger.append(
            "health", {"status": "ok"}, occurred_at=timestamp, trace_id="trace-2"
        )
        events = list(ledger.events())

        assert second > first
        assert ledger.event_count() == 2
        assert events[0]["event_type"] == "health"
        assert events[1]["payload"]["symbol"] == "SPY"
        assert ledger.latest_event("health") == events[0]
        assert ledger.latest_event("missing") is None
