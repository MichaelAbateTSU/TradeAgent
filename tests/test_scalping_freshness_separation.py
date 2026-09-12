from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

from tradeagent.persistence import Database, ProductionRepository
from tradeagent.scalping_config import ScalpingConfig
from tradeagent.scalping_runtime import ScalpingRuntime


def test_feature_history_is_broader_than_final_order_freshness(tmp_path):
    now = datetime.now(UTC)
    with Database(f"sqlite:///{tmp_path / 'freshness.db'}") as database:
        database.initialize()
        ProductionRepository(database).acquire_worker_lock(
            "tradeagent-event-worker", "owner", observed_at=now
        )
        config = ScalpingConfig(
            cohort_id="freshness",
            account_digest="a" * 64,
            approved_at=now,
            symbols=("BTC/USD",),
            decision_policy="action-value-v1",
            catastrophic_stop_bps="100",
            feature_horizon_seconds=5,
            decision_interval_seconds=1,
            exit_after_seconds=5,
        )
        runtime = ScalpingRuntime(
            database,
            MagicMock(),
            config,
            owner_id="owner",
            code_sha="c" * 40,
            clock=lambda: now,
        )
        assert runtime.market._stale_ns == 5_000_000_000
        assert config.maximum_quote_age_seconds == 1
        old = MagicMock(
            bid=100,
            ask=101,
            bid_size=1,
            ask_size=1,
            exchange_at=now - timedelta(seconds=2),
            received_at=now - timedelta(seconds=2),
        )
        assert runtime.engine._quote_valid(old, now) is False
