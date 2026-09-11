import asyncio
import json
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, ClassVar
from uuid import UUID

import pytest
from pydantic import SecretStr, ValidationError

from tradeagent.alpaca_paper import AlpacaPaperSettings
from tradeagent.alpaca_stream import StreamProviderError, WebSocketConnection
from tradeagent.scalping_config import ScalpingConfig
from tradeagent.scalping_market import (
    NS,
    BookFeatureEngine,
    CryptoMarketFeed,
    MarketEvent,
    capture_events,
    datetime_ns,
    decode_crypto_message,
    ns_datetime,
    timestamp_ns,
)

NOW = datetime(2026, 9, 11, tzinfo=UTC)
BASE_NS = datetime_ns(NOW)


def config(**overrides: Any) -> ScalpingConfig:
    return ScalpingConfig.model_validate(
        {
            "cohort_id": "v30-market-alpha-replay-test",
            "account_digest": "a" * 64,
            "approved_at": NOW - timedelta(days=100),
            "symbols": ["BTC/USD"],
            **overrides,
        }
    )


def stamp(at_ns: int) -> str:
    return ns_datetime(at_ns).strftime("%Y-%m-%dT%H:%M:%S") + f".{at_ns % NS:09d}Z"


class Tape:
    def __init__(self) -> None:
        self.sequence = 0
        self.connection = UUID(int=1)
        self.previous: dict[str, tuple[Decimal, Decimal]] = {}

    def event(
        self,
        kind: str,
        seconds: float,
        *,
        received_seconds: float | None = None,
        symbol: str = "BTC/USD",
        **payload: Any,
    ) -> MarketEvent:
        self.sequence += 1
        at_ns = BASE_NS + int(Decimal(str(seconds)) * NS)
        received_ns = (
            at_ns + 10_000_000
            if received_seconds is None
            else BASE_NS + int(Decimal(str(received_seconds)) * NS)
        )
        return decode_crypto_message(
            {"T": kind, "S": symbol, "t": stamp(at_ns), **payload},
            connection_id=self.connection,
            receive_sequence=self.sequence,
            received_at=ns_datetime(received_ns),
            received_at_ns=received_ns,
            received_monotonic_ns=received_ns - BASE_NS + NS,
        )

    def book(
        self,
        seconds: float,
        *,
        bid: str = "100",
        ask: str = "101",
        bid_size: str = "10",
        ask_size: str = "10",
        snapshot: bool = False,
        received_seconds: float | None = None,
        symbol: str = "BTC/USD",
    ) -> MarketEvent:
        bids, asks = [{"p": bid, "s": bid_size}], [{"p": ask, "s": ask_size}]
        previous = self.previous.get(symbol)
        if previous is not None and not snapshot:
            if previous[0] != Decimal(bid):
                bids.append({"p": str(previous[0]), "s": "0"})
            if previous[1] != Decimal(ask):
                asks.append({"p": str(previous[1]), "s": "0"})
        self.previous[symbol] = Decimal(bid), Decimal(ask)
        return self.event(
            "o",
            seconds,
            received_seconds=received_seconds,
            symbol=symbol,
            b=bids,
            a=asks,
            r=snapshot,
        )

    def trade(
        self,
        seconds: float,
        *,
        price: str = "100",
        quantity: str = "1",
        trade_id: str = "1",
        side: str | None = "B",
        **kwargs: Any,
    ) -> MarketEvent:
        return self.event("t", seconds, p=price, s=quantity, i=trade_id, tks=side, **kwargs)


def rising(tape: Tape, start: int = 0, end: int = 5, *, size: str = "5") -> list[MarketEvent]:
    return [
        tape.book(
            second,
            bid=str(Decimal(100) + Decimal(second) / 100),
            ask=str(Decimal("100.01") + Decimal(second) / 100),
            bid_size=size,
            ask_size="0.1",
            snapshot=second == 0,
        )
        for second in range(start, end + 1)
    ]


def test_native_nanoseconds_and_immutable_json_round_trip() -> None:
    tape = Tape()
    event = tape.event("t", 0.123456789, p="100.00000000001", s="0.0000001", i=17, tks="S")
    assert event.exchange_at_ns == BASE_NS + 123456789
    assert event.exchange_at.microsecond == 123456
    assert event.trade_price == Decimal("100.00000000001")
    assert event.trade_id == "17" and event.taker_side == "sell"
    assert event.provider_sequence is None
    assert MarketEvent.model_validate_json(json.dumps(event.model_dump(mode="json"))) == event
    assert timestamp_ns("2026-09-10T20:00:00.123456789-04:00") == event.exchange_at_ns
    with pytest.raises(ValidationError):
        event.trade_id = "changed"
    values = event.model_dump()
    values["exchange_at_ns"] += 1
    with pytest.raises(ValidationError, match="disagree"):
        MarketEvent.model_validate(values)


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-1", "0", True])
def test_invalid_prices_are_not_normalized_to_success(value: Any) -> None:
    with pytest.raises(ValueError):
        Tape().event("t", 0, p=value, s="1", i=1)


@pytest.mark.parametrize(
    "payload",
    [
        {"T": "t", "p": 100, "s": 1},
        {"T": "t", "p": 100, "s": 1, "i": True},
        {"T": "t", "p": 100, "s": 1, "i": 1, "tks": "invented"},
        {"T": "o", "b": [{"p": 100, "s": -1}], "a": []},
        {"T": "o", "b": [{"p": 100, "s": 1}, {"p": 100, "s": 2}], "a": []},
        {"T": "o", "r": "false"},
    ],
)
def test_malformed_wire_fields_are_explicit(payload: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        decode_crypto_message(
            {"S": "BTC/USD", "t": stamp(BASE_NS), **payload},
            connection_id=UUID(int=1),
            receive_sequence=1,
            received_at=NOW,
            received_monotonic_ns=0,
        )


def test_snapshot_delta_zero_delete_and_snapshot_replacement() -> None:
    tape, engine = Tape(), BookFeatureEngine(config())
    assert not engine.on_event(tape.book(0))
    assert engine.quote("BTC/USD") is None
    assert engine.on_event(
        tape.event(
            "o",
            1,
            r=True,
            b=[{"p": "100", "s": "2"}, {"p": "99", "s": "4"}],
            a=[{"p": "101", "s": "3"}, {"p": "102", "s": "5"}],
        )
    )
    assert engine.on_event(
        tape.event("o", 2, b=[{"p": "100", "s": "0"}, {"p": "99", "s": "6"}], a=[])
    )
    quote = engine.quote("BTC/USD")
    assert quote is not None and quote.bid == 99 and quote.bid_size == 6
    assert len(engine.levels("BTC/USD", side="ask")) == 2
    assert engine.on_event(tape.book(3, bid="90", ask="91", snapshot=True))
    assert [level.price for level in engine.levels("BTC/USD", side="bid")] == [90]
    features = engine.features("BTC/USD", NOW + timedelta(seconds=3.01))
    assert features is not None and not features.ready
    assert features.ofi_5s == 0 and features.bid_removals_proxy_5s == 0


@pytest.mark.parametrize("failure", ["crossed", "empty", "out_of_order", "stale", "future"])
def test_broken_book_requires_fresh_snapshot_then_recovers(failure: str) -> None:
    tape, engine = Tape(), BookFeatureEngine(config())
    engine.on_event(tape.book(1, snapshot=True))
    if failure == "crossed":
        broken = tape.event("o", 2, b=[{"p": "102", "s": "1"}])
    elif failure == "empty":
        broken = tape.event("o", 2, b=[{"p": "100", "s": "0"}])
    elif failure == "out_of_order":
        broken = tape.event("o", 0.5, received_seconds=2, b=[{"p": "100", "s": "2"}])
    elif failure == "stale":
        broken = tape.book(2, received_seconds=8)
    else:
        broken = tape.book(3, received_seconds=2)
    assert not engine.on_event(broken)
    assert engine.quote("BTC/USD") is None
    assert engine.features("BTC/USD", NOW + timedelta(seconds=10)) is None
    assert not engine.on_event(tape.book(8))
    assert engine.on_event(tape.book(9, snapshot=True))
    assert engine.quote("BTC/USD") is not None


def test_reconnect_does_not_merge_old_depth_or_accept_retired_connection() -> None:
    tape, engine = Tape(), BookFeatureEngine(config())
    engine.on_event(tape.book(0, snapshot=True))
    tape.connection, tape.sequence = UUID(int=2), 0
    assert not engine.on_event(tape.book(1))
    assert engine.quote("BTC/USD") is None
    assert engine.on_event(tape.book(2, bid="90", ask="91", snapshot=True))
    tape.connection = UUID(int=1)
    assert not engine.on_event(tape.book(3, snapshot=True))
    assert engine.quote("BTC/USD") is None
    tape.connection = UUID(int=2)
    assert engine.on_event(tape.book(4, snapshot=True))


def test_book_channel_order_is_independent_of_trade_and_quote_channels() -> None:
    tape, engine = Tape(), BookFeatureEngine(config())
    engine.on_event(tape.book(0, snapshot=True))
    engine.on_event(tape.event("q", 1.3, bp="100", bs="15", ap="101", **{"as": "5"}))
    engine.on_event(tape.trade(1.4, price="101", quantity="2"))
    assert engine.on_event(tape.book(1.2, received_seconds=1.5, bid_size="12", ask_size="8"))
    quote = engine.quote("BTC/USD")
    assert quote is not None and quote.exchange_time_ns == BASE_NS + 1_300_000_000
    assert quote.bid_size == 15
    features = engine.features("BTC/USD", NOW + timedelta(seconds=1.5))
    assert features is not None and features.bid_depth_l5 == 12
    assert features.taker_delta_5s == 2
    assert (
        engine.health_snapshot()["symbols"]["BTC/USD"]["counters"]["cross_channel_bbo_interleaving"]
        == 1
    )
    assert not engine.on_event(tape.book(1.1, received_seconds=1.6))


def test_standard_l1_ofi_microprice_l5_and_aggregate_change_proxies() -> None:
    tape, engine = Tape(), BookFeatureEngine(config())
    engine.on_event(
        tape.event(
            "o",
            0,
            r=True,
            b=[{"p": str(100 - level), "s": "10"} for level in range(5)],
            a=[{"p": str(101 + level), "s": "10"} for level in range(5)],
        )
    )
    engine.on_event(
        tape.event(
            "o",
            1,
            b=[{"p": "100", "s": "14"}],
            a=[{"p": "101", "s": "6"}],
        )
    )
    features = engine.features("BTC/USD", NOW + timedelta(seconds=1.01))
    assert features is not None
    assert features.ofi_1s == features.ofi_5s == 8
    assert features.l1_imbalance == pytest.approx(0.4)
    assert features.l5_imbalance == pytest.approx(0.08)
    assert features.microprice == Decimal("100.7")
    assert features.microprice_displacement_bps > 0
    assert features.bid_additions_5s == 4
    assert features.ask_removals_proxy_5s == 4
    assert features.ask_depletion_fraction_5s == pytest.approx(4 / 50)
    assert features.bid_slope_bps_per_unit is not None
    assert features.aggregate_change_proxies and not features.exact_queue_position_available


def test_native_taker_side_unknowns_vwap_poc_and_cross_day_deduplication() -> None:
    tape, engine = Tape(), BookFeatureEngine(config())
    engine.on_event(tape.book(0, snapshot=True))
    first = tape.trade(0.1, price="100", quantity="2", trade_id="7", side="B")
    assert engine.on_event(first)
    assert not engine.on_event(first)
    assert not engine.on_event(tape.trade(0.1, price="100", quantity="2", trade_id="7", side="B"))
    engine.on_event(tape.trade(0.2, price="101", quantity="1", trade_id="8", side="S"))
    features = engine.features("BTC/USD", NOW + timedelta(seconds=0.21))
    assert features is not None and features.taker_delta_5s == 1
    assert features.session_vwap == Decimal(301) / 3
    assert features.session_poc == 100 and features.session_trade_count == 2
    engine.on_event(tape.trade(0.3, trade_id="9", side=None))
    unknown = engine.features("BTC/USD", NOW + timedelta(seconds=0.31))
    assert unknown is not None and unknown.taker_delta_5s is None
    assert unknown.known_taker_fraction_5s == pytest.approx(2 / 3)
    engine.on_event(tape.book(86400, snapshot=True))
    empty_session = engine.features("BTC/USD", NOW + timedelta(seconds=86400.01))
    assert empty_session is not None and empty_session.session_vwap is None
    assert empty_session.session_poc is None and empty_session.session_trade_count == 0
    assert engine.on_event(tape.trade(86400.1, trade_id="7"))
    next_day = engine.features("BTC/USD", NOW + timedelta(seconds=86400.11))
    assert next_day is not None and next_day.session_trade_count == 1


def test_no_data_sparse_history_staleness_and_five_second_readiness() -> None:
    engine, tape = BookFeatureEngine(config()), Tape()
    assert engine.features("BTC/USD", NOW) is None
    events = rising(tape)
    for event in events[:-1]:
        engine.on_event(event)
    before = engine.features("BTC/USD", events[-2].received_at)
    assert before is not None and not before.ready and before.return_5s_bps is None
    engine.on_event(events[-1])
    features = engine.features("BTC/USD", events[-1].received_at)
    assert features is not None and features.ready and features.history_seconds == 5
    assert features.return_5s_bps is not None and features.return_5s_bps > 0
    assert features.return_1s_bps is not None and features.return_1s_bps > 0
    assert features.volatility_5s_bps is not None and features.volatility_5s_bps > 0
    assert features.return_15s_bps is None and features.atr_1m is None
    assert features.session_vwap is None and features.session_poc is None
    assert engine.features("BTC/USD", NOW + timedelta(seconds=11)) is None
    assert engine.features("BTC/USD", NOW + timedelta(seconds=4)) is None
    json.dumps(features.model_dump(mode="json"), allow_nan=False)


def test_nanosecond_future_data_cannot_be_made_current_by_datetime_truncation() -> None:
    tape, engine = Tape(), BookFeatureEngine(config())
    event = tape.book(0.123456789, snapshot=True, received_seconds=0.123456780)
    assert event.exchange_at == event.received_at
    assert not engine.on_event(event)
    assert engine.features("BTC/USD", NOW + timedelta(seconds=1)) is None


def test_receive_order_errors_and_late_trades_are_exposed_without_cross_channel_poison() -> None:
    tape, engine = Tape(), BookFeatureEngine(config())
    first = tape.book(0, snapshot=True)
    engine.on_event(first)
    engine.on_event(tape.trade(1, trade_id="new"))
    assert not engine.on_event(tape.trade(0.5, received_seconds=1.1, trade_id="old"))
    assert engine.quote("BTC/USD") is not None
    assert not engine.on_event(first)
    assert engine.quote("BTC/USD") is None
    assert engine.on_event(tape.book(2, snapshot=True))


def test_bounded_history_capacity_and_automatic_rewarming() -> None:
    tape, engine = Tape(), BookFeatureEngine(config(), max_events=3)
    for event in rising(tape, end=3):
        engine.on_event(event)
    features = engine.features("BTC/USD", NOW + timedelta(seconds=3.01))
    assert features is not None and not features.ready
    state = engine.health_snapshot()["symbols"]["BTC/USD"]
    assert state["counters"]["feature_window_overflow"] == 1
    assert max(state["bbo_samples"], state["event_samples"], state["change_samples"]) <= 3
    engine.on_event(tape.book(4, snapshot=True))
    engine.on_event(tape.book(6.5))
    engine.on_event(tape.book(9))
    recovered = engine.features("BTC/USD", NOW + timedelta(seconds=9.01))
    assert recovered is not None and recovered.ready


def test_dedup_capacity_cannot_reaccept_evicted_identity_at_same_nanosecond() -> None:
    engine, tape = BookFeatureEngine(config(), max_events=2), Tape()
    engine.on_event(tape.book(0, snapshot=True))
    assert engine.on_event(tape.trade(1, trade_id="one"))
    assert engine.on_event(tape.trade(1, trade_id="two"))
    assert not engine.on_event(tape.trade(1, trade_id="three"))
    assert not engine.on_event(tape.trade(1, trade_id="one"))
    assert engine.health_snapshot()["symbols"]["BTC/USD"]["dedup_identities"] == 2
    assert engine.on_event(tape.trade(2, trade_id="one"))


def test_minute_atr_requires_observed_completed_minutes_not_warmup_gate() -> None:
    tape, engine = Tape(), BookFeatureEngine(config())
    for second in range(0, 16 * 60 + 1, 5):
        engine.on_event(tape.book(second, snapshot=second == 0))
        engine.on_event(
            tape.trade(second + 0.02, price=str(100 + (second // 5) % 3), trade_id=str(second))
        )
        if second == 5:
            early = engine.features("BTC/USD", NOW + timedelta(seconds=second + 0.03))
            assert early is not None and early.ready and early.atr_1m is None
    mature = engine.features("BTC/USD", NOW + timedelta(seconds=960.03))
    assert mature is not None and mature.atr_1m is not None and mature.atr_1m > 0


def test_explicit_reset_and_book_capacity_do_not_reuse_a_good_quote() -> None:
    tape, engine = Tape(), BookFeatureEngine(config(), max_levels=1)
    engine.on_event(tape.book(0, snapshot=True))
    engine.reset(reason="queue_overflow")
    assert engine.quote("BTC/USD") is None
    assert not engine.on_event(tape.book(1))
    assert engine.on_event(tape.book(2, snapshot=True))
    assert not engine.on_event(tape.event("o", 3, b=[{"p": "99", "s": "1"}]))
    assert engine.quote("BTC/USD") is None
    with pytest.raises(ValueError, match="universe"):
        engine.features("ETH/USD", NOW)


def test_conflicting_repeated_raw_identity_is_not_silently_deduplicated() -> None:
    tape, engine = Tape(), BookFeatureEngine(config())
    first = tape.book(0, snapshot=True)
    assert engine.on_event(first)
    altered = MarketEvent.model_validate(
        {
            **first.model_dump(),
            "bids": [{"price": "99", "quantity": "10"}],
        }
    )
    assert not engine.on_event(altered)
    assert engine.quote("BTC/USD") is None


def test_trade_identity_is_scoped_to_symbol_as_well_as_day_and_venue() -> None:
    tape, engine = Tape(), BookFeatureEngine(config(symbols=["BTC/USD", "ETH/USD"]))
    for symbol in ("BTC/USD", "ETH/USD"):
        engine.on_event(tape.book(0, symbol=symbol, snapshot=True))
    for symbol in ("BTC/USD", "ETH/USD"):
        assert engine.on_event(tape.trade(0.1, trade_id="same", symbol=symbol))
    for symbol in ("BTC/USD", "ETH/USD"):
        value = engine.features(symbol, NOW + timedelta(seconds=0.11))
        assert value is not None and value.session_trade_count == 1


def test_exact_nanosecond_ordering_and_quote_currentness() -> None:
    tape, engine = Tape(), BookFeatureEngine(config())
    event = tape.book(0.123456790, snapshot=True, received_seconds=0.13)
    assert engine.on_event(event)
    assert engine.quote_at_ns("BTC/USD", event.received_at_ns - 1) is None
    assert engine.quote_at_ns("BTC/USD", event.received_at_ns) is not None
    assert not engine.on_event(tape.book(0.123456789, received_seconds=0.14))
    assert engine.health_snapshot()["symbols"]["BTC/USD"]["reason"] == "out_of_order_book"


@pytest.mark.parametrize("age,bid_count,ask_count", [(34, 88, 53), (67, 53, 44)])
def test_old_provider_snapshot_is_only_reconstruction_until_a_current_book_touch(
    age: int, bid_count: int, ask_count: int
) -> None:
    tape, engine = Tape(), BookFeatureEngine(config())
    initial = tape.event(
        "o",
        -age,
        received_seconds=0.01,
        r=True,
        b=[
            {"p": str(Decimal(100) - Decimal(index) / 100), "s": "10"} for index in range(bid_count)
        ],
        a=[
            {"p": str(Decimal(101) + Decimal(index) / 100), "s": "10"} for index in range(ask_count)
        ],
    )
    assert engine.on_event(initial)
    assert initial.exchange_at_ns == BASE_NS - age * NS
    assert engine.quote("BTC/USD") is None
    assert engine.features("BTC/USD", initial.received_at) is None
    health = engine.health_snapshot()["symbols"]["BTC/USD"]
    assert health["reconstruction_valid"] and not health["book_valid"]
    assert health["bid_levels"] == bid_count and health["ask_levels"] == ask_count
    assert health["bbo_samples"] == 0
    engine.on_event(tape.event("q", 0.2, bp="100", bs="15", ap="101", **{"as": "5"}))
    assert engine.quote("BTC/USD") is None
    for second in range(1, 7):
        update = tape.event("o", second, b=[{"p": "100", "s": str(10 + second)}])
        assert engine.on_event(update)
        value = engine.features("BTC/USD", update.received_at)
        assert value is not None and value.quote.exchange_time_ns == update.exchange_at_ns
        assert value.history_seconds == second - 1
        assert value.ready == (second == 6)
        assert value.trade_count_5s == value.native_taker_trade_count_5s == 0
        assert value.trade_volume_5s == value.trade_intensity_5s == 0
        assert value.taker_delta_5s is None and value.session_vwap is None
        if second == 1:
            assert value.ofi_5s == 0 and value.bid_additions_5s == 0
    assert value.ofi_5s > 0 and value.l1_imbalance > 0
    assert value.microprice_displacement_bps > 0
    assert not engine.health_snapshot()["symbols"]["BTC/USD"]["native_taker_side_observed"]


def test_first_current_delta_after_a_delayed_initial_snapshot_uses_same_connection_baseline() -> (
    None
):
    tape, engine = Tape(), BookFeatureEngine(config())
    assert engine.on_event(tape.book(-67, received_seconds=0.01, snapshot=True))
    assert engine.features("BTC/USD", NOW + timedelta(seconds=10)) is None
    assert engine.on_event(tape.book(11, bid_size="20"))
    value = engine.features("BTC/USD", NOW + timedelta(seconds=11.01))
    assert value is not None and value.history_seconds == 0 and not value.ready
    assert value.quote.exchange_time_ns == BASE_NS + 11 * NS


def test_lost_local_receive_sequence_invalidates_all_symbols_not_an_exchange_gap_claim() -> None:
    tape, engine = Tape(), BookFeatureEngine(config(symbols=["BTC/USD", "ETH/USD"]))
    for symbol in ("BTC/USD", "ETH/USD"):
        engine.on_event(tape.book(0, symbol=symbol, snapshot=True))
    tape.sequence += 1
    assert not engine.on_event(tape.book(1))
    assert engine.quote("BTC/USD") is None and engine.quote("ETH/USD") is None
    assert engine.health_snapshot()["local_receive_gaps"] == 1
    assert engine.health_snapshot()["exchange_gap_coverage"] == "unprovable"
    assert engine.on_event(tape.book(2, snapshot=True))
    assert engine.quote("ETH/USD") is None
    assert engine.on_event(tape.book(2, symbol="ETH/USD", snapshot=True))


class FeedClock:
    def __init__(self) -> None:
        self.now = NOW

    def clock(self) -> datetime:
        return self.now

    def monotonic(self) -> int:
        return datetime_ns(self.now) - BASE_NS + NS


class FakeSocket:
    def __init__(self, frames: Sequence[Any], clock: FeedClock) -> None:
        self.frames, self.clock = iter(frames), clock
        self.sent: list[dict[str, Any]] = []

    async def send(self, message: str) -> None:
        self.sent.append(json.loads(message))

    async def recv(self, decode: bool | None = None) -> str | bytes:
        await asyncio.sleep(0)
        frame = next(self.frames, None)
        if frame is None:
            await asyncio.Future()
            raise AssertionError("unreachable")
        if isinstance(frame, Exception):
            raise frame
        if isinstance(frame, (str, bytes)):
            return frame
        for message in frame:
            if isinstance(message.get("t"), str):
                at = ns_datetime(timestamp_ns(message["t"])) + timedelta(milliseconds=10)
                self.clock.now = max(self.clock.now, at)
        return json.dumps(frame)


def handshake() -> list[Any]:
    return [
        [{"T": "success", "msg": "connected"}],
        [{"T": "success", "msg": "authenticated"}],
        [
            {
                "T": "subscription",
                "trades": ["BTC/USD"],
                "quotes": ["BTC/USD"],
                "orderbooks": ["BTC/USD"],
            }
        ],
    ]


def wire_book(second: int, *, reset: bool = True) -> dict[str, Any]:
    return {
        "T": "o",
        "S": "BTC/USD",
        "t": stamp(BASE_NS + second * NS),
        "b": [{"p": 100, "s": 10}],
        "a": [{"p": 101, "s": 10}],
        "r": reset,
    }


def credentials() -> AlpacaPaperSettings:
    return AlpacaPaperSettings(
        key_id=SecretStr("only-a-test-key"),
        secret_key=SecretStr("only-a-test-secret"),
        _env_file=None,
    )


def test_feed_auth_subscription_batched_frames_stop_and_no_secret_health() -> None:
    async def exercise() -> None:
        clock = FeedClock()
        frames = handshake()
        frames[-1].extend(
            [
                wire_book(0),
                {
                    "T": "t",
                    "S": "BTC/USD",
                    "p": 100,
                    "s": 1,
                    "i": 1,
                    "tks": "S",
                    "t": stamp(BASE_NS),
                },
            ]
        )
        socket = FakeSocket(frames, clock)

        @asynccontextmanager
        async def transport() -> AsyncIterator[FakeSocket]:
            yield socket

        feed = CryptoMarketFeed(
            ["BTC/USD"],
            credentials(),
            transport=transport,
            clock=clock.clock,
            monotonic_ns=clock.monotonic,
            connection_ids=lambda: UUID(int=1),
        )
        received = []
        async for event in feed.stream():
            received.append(event)
            if event.event_type == "trade":
                feed.stop()
        assert [event.event_type for event in received[:3]] == ["reset", "book", "trade"]
        assert received[2].taker_side == "sell"
        assert [event.receive_sequence for event in received] == sorted(
            event.receive_sequence for event in received
        )
        assert socket.sent[0]["action"] == "auth"
        assert socket.sent[1] == {
            "action": "subscribe",
            "trades": ["BTC/USD"],
            "quotes": ["BTC/USD"],
            "orderbooks": ["BTC/USD"],
        }
        health = feed.health_snapshot()
        assert health["state"] == "stopped" and not health["authenticated"]
        assert not health["exchange_sequence_available"]
        assert "only-a-test-secret" not in json.dumps(health)
        assert "only-a-test-key" not in json.dumps(health)

    asyncio.run(asyncio.wait_for(exercise(), timeout=3))


def test_feed_reconnect_emits_resets_and_backoff_without_merging_delta() -> None:
    async def exercise() -> None:
        clock = FeedClock()
        sockets = [
            FakeSocket([*handshake(), [wire_book(0)], OSError("private transport details")], clock),
            FakeSocket([*handshake(), [wire_book(1, reset=False)]], clock),
            FakeSocket([*handshake(), [wire_book(2)]], clock),
        ]
        available = iter(sockets)
        identifiers = iter([UUID(int=1), UUID(int=2), UUID(int=3)])
        delays = []

        @asynccontextmanager
        async def transport() -> AsyncIterator[FakeSocket]:
            yield next(available)

        async def sleep(seconds: float) -> None:
            delays.append(seconds)
            await asyncio.sleep(0)

        feed = CryptoMarketFeed(
            ["BTC/USD"],
            credentials(),
            transport=transport,
            clock=clock.clock,
            monotonic_ns=clock.monotonic,
            connection_ids=lambda: next(identifiers),
            sleep=sleep,
        )
        engine = BookFeatureEngine(config())
        saw_last_snapshot = False
        async for event in feed.stream():
            accepted = engine.on_event(event)
            if event.event_type == "reset":
                assert engine.quote("BTC/USD") is None
            if event.connection_id == UUID(int=2) and event.event_type == "book":
                assert not accepted
            if event.connection_id == UUID(int=3) and event.event_type == "book":
                saw_last_snapshot = True
                assert accepted
                feed.stop()
        assert saw_last_snapshot and delays == [0.25, 0.5]
        assert feed.health_snapshot()["reconnects"] == 2
        assert "private transport details" not in json.dumps(feed.health_snapshot())

    asyncio.run(asyncio.wait_for(exercise(), timeout=3))


def test_feed_overflow_surfaces_gap_resets_and_never_a_truncated_book() -> None:
    async def exercise() -> None:
        clock = FeedClock()
        socket = FakeSocket([*handshake(), [wire_book(0), wire_book(1, reset=False)]], clock)

        @asynccontextmanager
        async def transport() -> AsyncIterator[FakeSocket]:
            yield socket

        feed = CryptoMarketFeed(
            ["BTC/USD"],
            credentials(),
            transport=transport,
            clock=clock.clock,
            monotonic_ns=clock.monotonic,
            queue_capacity=1,
        )
        engine = BookFeatureEngine(config())
        async for event in feed.stream():
            engine.on_event(event)
            if feed.health_snapshot()["queue_overflows"]:
                assert event.event_type == "reset"
                assert engine.quote("BTC/USD") is None
                assert feed.health_snapshot()["state"] in ("reconnecting", "stopped")
                assert not feed.health_snapshot()["book_state"]["BTC/USD"]["current"]
                feed.stop()
        assert feed.health_snapshot()["dropped_events"] >= 1
        assert feed.health_snapshot()["gap_count"] >= 1

    asyncio.run(asyncio.wait_for(exercise(), timeout=3))


def test_feed_fatal_provider_error_never_retries_or_echoes_authentication() -> None:
    async def exercise() -> None:
        clock = FeedClock()
        socket = FakeSocket(
            [
                [{"T": "success", "msg": "connected"}],
                [{"T": "error", "code": 402, "msg": "only-a-test-secret"}],
            ],
            clock,
        )

        @asynccontextmanager
        async def transport() -> AsyncIterator[FakeSocket]:
            yield socket

        feed = CryptoMarketFeed(
            ["BTC/USD"],
            credentials(),
            transport=transport,
            clock=clock.clock,
        )
        with pytest.raises(StreamProviderError) as error:
            async for _ in feed.stream():
                pass
        assert "only-a-test-secret" not in str(error.value)
        assert feed.health_snapshot()["state"] == "failed"
        assert feed.health_snapshot()["reconnects"] == 0

    asyncio.run(asyncio.wait_for(exercise(), timeout=3))


def test_feed_missing_snapshot_times_out_despite_other_active_channels() -> None:
    async def exercise() -> None:
        clock = FeedClock()
        sockets = iter(
            [
                FakeSocket(
                    [
                        *handshake(),
                        [
                            {
                                "T": "t",
                                "S": "BTC/USD",
                                "p": 100,
                                "s": 1,
                                "i": 1,
                                "tks": "S",
                                "t": stamp(BASE_NS + 6 * NS),
                            }
                        ],
                    ],
                    clock,
                ),
                FakeSocket([*handshake(), [wire_book(7)]], clock),
            ]
        )
        identifiers = iter([UUID(int=1), UUID(int=2)])
        reasons = []

        @asynccontextmanager
        async def transport() -> AsyncIterator[FakeSocket]:
            yield next(sockets)

        async def sleep(seconds: float) -> None:
            await asyncio.sleep(0)

        feed = CryptoMarketFeed(
            ["BTC/USD"],
            credentials(),
            transport=transport,
            clock=clock.clock,
            monotonic_ns=clock.monotonic,
            connection_ids=lambda: next(identifiers),
            sleep=sleep,
        )
        async for event in feed.stream():
            if event.event_type == "reset":
                reasons.append(event.reset_reason)
            if event.event_type == "book":
                feed.stop()
        assert "snapshot_timeout" in reasons
        assert feed.health_snapshot()["reconnects"] == 1

    asyncio.run(asyncio.wait_for(exercise(), timeout=3))


def test_stop_before_stream_does_not_open_a_connection() -> None:
    async def exercise() -> None:
        feed = CryptoMarketFeed(["BTC/USD"], credentials())
        feed.stop()
        assert [event async for event in feed.stream()] == []
        assert feed.health_snapshot()["state"] == "stopped"

    asyncio.run(exercise())


def test_feed_keeps_old_resets_without_reporting_instant_freshness_or_observed_trades() -> None:
    async def exercise() -> None:
        clock = FeedClock()
        release = asyncio.Event()
        frames = handshake()
        for channel in ("trades", "quotes", "orderbooks"):
            frames[-1][0][channel] = ["BTC/USD", "ETH/USD"]
        frames.append([wire_book(-34), {**wire_book(-67), "S": "ETH/USD"}])
        frames.extend([[wire_book(second, reset=False)] for second in range(1, 8)])
        frames.append([{**wire_book(8, reset=False), "S": "ETH/USD"}])

        class PausingSocket(FakeSocket):
            calls = 0

            async def recv(self, decode: bool | None = None) -> str | bytes:
                self.calls += 1
                if self.calls == 5:
                    await release.wait()
                return await super().recv(decode)

        socket = PausingSocket(frames, clock)

        @asynccontextmanager
        async def transport() -> AsyncIterator[PausingSocket]:
            yield socket

        feed = CryptoMarketFeed(
            ["BTC/USD", "ETH/USD"],
            credentials(),
            transport=transport,
            clock=clock.clock,
            monotonic_ns=clock.monotonic,
        )
        initial = 0
        async for event in feed.stream():
            if event.event_type == "book" and event.reset:
                initial += 1
                if initial == 2:
                    health = feed.health_snapshot()
                    assert health["state"] == "awaiting_current_book_update"
                    for state in health["book_state"].values():
                        assert state["snapshot_received"] and not state["current"]
                        assert state["observed_trades"] == 0
                        assert not state["native_taker_side_observed"]
                    release.set()
            if event.event_type == "book" and not event.reset and event.symbol == "ETH/USD":
                health = feed.health_snapshot()
                assert health["state"] == "streaming" and health["reconnects"] == 0
                assert all(state["current"] for state in health["book_state"].values())
                feed.stop()
        assert initial == 2
        assert feed.health_snapshot()["reconnects"] == 0

    asyncio.run(asyncio.wait_for(exercise(), timeout=3))


def test_data_failures_notify_once_and_cannot_rearm_with_a_delta(
    caplog: pytest.LogCaptureFixture,
) -> None:
    tape, engine = Tape(), BookFeatureEngine(config())
    requests = []
    engine.on_resnapshot = lambda reason, connection: requests.append((reason, connection))
    assert engine.on_event(tape.book(0, snapshot=True))
    assert requests == []
    assert not engine.on_event(tape.book(1, bid_size="1e400"))
    health = engine.health_snapshot()
    assert health["resnapshot_required"]
    assert health["resnapshot_reasons"] == {"BTC/USD": "invalid_market_numeric_domain"}
    assert requests == [("invalid_market_numeric_domain", UUID(int=1))]
    assert engine.quote("BTC/USD") is None
    assert engine.features("BTC/USD", NOW + timedelta(seconds=1.01)) is None
    assert not engine.on_event(tape.book(2))
    assert len(requests) == 1
    assert engine.on_event(tape.book(3, snapshot=True))
    assert not engine.health_snapshot()["resnapshot_required"]
    assert engine.quote("BTC/USD") is not None
    assert "fresh snapshot" in caplog.text and "1e400" not in caplog.text


def test_nonfinite_derived_features_return_none_and_request_a_new_snapshot() -> None:
    tape, engine = Tape(), BookFeatureEngine(config())
    requests = []
    engine.on_resnapshot = lambda reason, connection: requests.append((reason, connection))
    assert engine.on_event(
        tape.event(
            "o",
            0,
            r=True,
            b=[{"p": "100", "s": "1e308"}, {"p": "99", "s": "1e308"}],
            a=[{"p": "101", "s": "1"}],
        )
    )
    assert engine.features("BTC/USD", NOW + timedelta(seconds=0.01)) is None
    assert requests == [("nonfinite_feature", UUID(int=1))]
    assert engine.quote("BTC/USD") is None
    assert engine.health_snapshot()["resnapshot_required"]
    assert engine.on_event(tape.book(1, snapshot=True))
    assert engine.features("BTC/USD", NOW + timedelta(seconds=1.01)) is not None
    assert not engine.health_snapshot()["resnapshot_required"]


def test_unknown_market_symbol_is_observable_data_failure_not_task_termination() -> None:
    engine, tape = BookFeatureEngine(config()), Tape()
    assert not engine.on_event(tape.book(0, symbol="ETH/USD", snapshot=True))
    assert engine.health_snapshot()["resnapshot_required"]
    assert engine.health_snapshot()["resnapshot_reasons"] == {
        "BTC/USD": "unconfigured_market_symbol"
    }


@pytest.mark.parametrize(
    "broken",
    [
        '{"not valid json',
        "[1]",
        '[{"T":"o","S":"BTC/USD","t":"not-a-timestamp","b":[],"a":[]}]',
        [{**wire_book(1, reset=False), "b": [{"p": 102, "s": 1}]}],
        [{**wire_book(1, reset=False), "b": [{"p": 100, "s": "NaN"}]}],
    ],
)
def test_invalid_wire_frames_reconnect_instead_of_terminating_stream(broken: Any) -> None:
    async def exercise() -> None:
        clock = FeedClock()
        sockets = iter(
            [
                FakeSocket([*handshake(), [wire_book(0)], broken], clock),
                FakeSocket([*handshake(), [wire_book(2)]], clock),
            ]
        )
        identifiers = iter([UUID(int=1), UUID(int=2)])

        @asynccontextmanager
        async def transport() -> AsyncIterator[FakeSocket]:
            yield next(sockets)

        async def retry_sleep(seconds: float) -> None:
            await asyncio.sleep(0)

        feed = CryptoMarketFeed(
            ["BTC/USD"],
            credentials(),
            transport=transport,
            clock=clock.clock,
            monotonic_ns=clock.monotonic,
            connection_ids=lambda: next(identifiers),
            sleep=retry_sleep,
        )
        recovered = False
        async for event in feed.stream():
            if event.connection_id == UUID(int=2) and event.event_type == "book":
                recovered = True
                feed.stop()
        assert recovered and feed.health_snapshot()["reconnects"] == 1

    asyncio.run(asyncio.wait_for(exercise(), timeout=3))


def test_worker_thread_requests_coalesce_and_old_connection_requests_are_ignored() -> None:
    async def exercise() -> None:
        clock = FeedClock()
        sockets = iter(
            [
                FakeSocket([*handshake(), [wire_book(0)]], clock),
                FakeSocket([*handshake(), [wire_book(1)]], clock),
            ]
        )
        identifiers = iter([UUID(int=1), UUID(int=2)])

        @asynccontextmanager
        async def transport() -> AsyncIterator[FakeSocket]:
            yield next(sockets)

        async def retry_sleep(seconds: float) -> None:
            await asyncio.sleep(0)

        feed = CryptoMarketFeed(
            ["BTC/USD"],
            credentials(),
            transport=transport,
            clock=clock.clock,
            monotonic_ns=clock.monotonic,
            connection_ids=lambda: next(identifiers),
            sleep=retry_sleep,
        )

        def request_batch() -> None:
            for _ in range(3):
                feed.request_resnapshot("feature_integrity", UUID(int=1))

        reasons = []
        async for event in feed.stream():
            if event.event_type == "reset":
                reasons.append(event.reset_reason)
            if event.event_type == "book" and event.connection_id == UUID(int=1):
                await asyncio.to_thread(request_batch)
            if event.event_type == "book" and event.connection_id == UUID(int=2):
                await asyncio.to_thread(feed.request_resnapshot, "old_writer_batch", UUID(int=1))
                await asyncio.sleep(0)
                health = feed.health_snapshot()
                assert health["state"] == "streaming"
                assert health["stale_resnapshot_requests"] >= 1
                feed.stop()
        assert reasons.count("feature_integrity") == 1
        assert feed.health_snapshot()["connections"] == 2
        health = feed.health_snapshot()
        assert (health["coalesced_resnapshot_requests"] + health["stale_resnapshot_requests"]) >= 3

    asyncio.run(asyncio.wait_for(exercise(), timeout=3))


def test_feature_writer_callback_recovers_while_owned_supervision_keeps_running() -> None:
    async def exercise() -> None:
        clock = FeedClock()
        oversized = {
            **wire_book(0),
            "b": [{"p": 100, "s": "1e308"}, {"p": 99, "s": "1e308"}],
        }
        sockets = iter(
            [
                FakeSocket([*handshake(), [oversized]], clock),
                FakeSocket([*handshake(), [wire_book(1)]], clock),
            ]
        )
        identifiers = iter([UUID(int=1), UUID(int=2)])
        complete = asyncio.Event()
        heartbeats = []

        @asynccontextmanager
        async def transport() -> AsyncIterator[FakeSocket]:
            yield next(sockets)

        async def retry_sleep(seconds: float) -> None:
            await asyncio.sleep(0)

        feed = CryptoMarketFeed(
            ["BTC/USD"],
            credentials(),
            transport=transport,
            clock=clock.clock,
            monotonic_ns=clock.monotonic,
            connection_ids=lambda: next(identifiers),
            sleep=retry_sleep,
        )
        market = BookFeatureEngine(config())
        market.on_resnapshot = feed.request_resnapshot

        async def receive() -> None:
            async for event in feed.stream():
                await asyncio.to_thread(market.on_event, event)
                if event.event_type == "book":
                    value = await asyncio.to_thread(
                        market.features, event.symbol, event.received_at
                    )
                    if event.connection_id == UUID(int=1):
                        assert value is None
                    else:
                        assert value is not None
                        assert not market.health_snapshot()["resnapshot_required"]
                        complete.set()
                        await asyncio.to_thread(feed.stop)

        async def supervise() -> None:
            while not complete.is_set():
                heartbeats.append("supervising")
                await asyncio.sleep(0)

        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(receive())
            tasks.create_task(supervise())
        assert heartbeats and complete.is_set()
        assert feed.health_snapshot()["connections"] == 2
        assert feed.health_snapshot()["last_resnapshot_reason"] == "nonfinite_feature"

    asyncio.run(asyncio.wait_for(exercise(), timeout=3))


def test_resnapshot_before_start_and_after_stop_are_safe_and_do_not_open_extra_connections() -> (
    None
):
    async def exercise() -> None:
        clock = FeedClock()
        socket = FakeSocket([*handshake(), [wire_book(0)]], clock)

        @asynccontextmanager
        async def transport() -> AsyncIterator[FakeSocket]:
            yield socket

        feed = CryptoMarketFeed(
            ["BTC/USD"],
            credentials(),
            transport=transport,
            clock=clock.clock,
        )
        feed.request_resnapshot("startup_recovery")
        async for event in feed.stream():
            if event.event_type == "book":
                feed.stop()
        feed.request_resnapshot("stopped_recovery")
        assert feed.health_snapshot()["connections"] == 1
        assert feed.health_snapshot()["coalesced_resnapshot_requests"] == 1
        assert feed.health_snapshot()["ignored_resnapshot_requests"] == 1
        with pytest.raises(ValueError, match="resnapshot reason"):
            feed.request_resnapshot("")

    asyncio.run(asyncio.wait_for(exercise(), timeout=3))


def captured_row(
    payload: dict[str, Any],
    sequence: int,
    *,
    seconds: float = 0,
    connection: UUID | None = None,
) -> dict[str, Any]:
    return {
        "connection_id": str(connection if connection is not None else UUID(int=1)),
        "receive_sequence": sequence,
        "received_at": (NOW + timedelta(seconds=seconds)).isoformat(),
        "received_monotonic_ns": NS + int(seconds * NS),
        "payload": payload,
    }


def test_capture_conversion_preserves_original_receipts_sequence_and_large_trade_identity() -> None:
    records = [
        captured_row(handshake()[-1][0], 1),
        captured_row(wire_book(-34), 2),
        captured_row(
            {
                "T": "t",
                "S": "BTC/USD",
                "p": Decimal("77275.399000001"),
                "s": Decimal("0.0010089"),
                "i": 8073166941685564994,
                "tks": "S",
                "t": stamp(BASE_NS + 123456789),
            },
            3,
            seconds=0.2,
        ),
    ]
    before = deepcopy(records)
    events = list(capture_events(records))
    assert records == before
    assert len(events) == 2 and [event.receive_sequence for event in events] == [2, 3]
    assert all(event.connection_id == UUID(int=1) for event in events)
    assert events[0].reset and events[0].exchange_at_ns == BASE_NS - 34 * NS
    assert events[1].trade_id == "8073166941685564994"
    assert events[1].trade_price == Decimal("77275.399000001")
    assert events[1].taker_side == "sell" and events[1].provider_sequence is None
    assert events[1].received_monotonic_ns == records[2]["received_monotonic_ns"]
    assert events[1].exchange_at_ns == BASE_NS + 123456789
    assert [MarketEvent.model_validate_json(event.model_dump_json()) for event in events] == events


def test_capture_new_connections_keep_their_snapshot_boundary_without_synthetic_records() -> None:
    records = [
        captured_row(wire_book(0), 12),
        captured_row(handshake()[-1][0], 1, seconds=1, connection=UUID(int=2)),
        captured_row(wire_book(1), 2, seconds=1.1, connection=UUID(int=2)),
    ]
    events = list(capture_events(records))
    assert [event.receive_sequence for event in events] == [12, 2]
    assert [event.event_type for event in events] == ["book", "book"]
    engine = BookFeatureEngine(config())
    assert all(engine.on_event(event) for event in events)
    assert not engine.health_snapshot()["resnapshot_required"]


@pytest.mark.parametrize("fault", ["sequence", "receive_time", "monotonic", "midstream_control"])
def test_capture_bad_ordering_and_unmodeled_controls_are_not_silently_fixed(fault: str) -> None:
    first = captured_row(wire_book(0), 2, seconds=1)
    second = captured_row(wire_book(1), 3, seconds=2)
    if fault == "sequence":
        second["receive_sequence"] = 1
    elif fault == "receive_time":
        second["received_at"] = NOW.isoformat()
    elif fault == "monotonic":
        second["received_monotonic_ns"] = 0
    else:
        second["payload"] = handshake()[-1][0]
    with pytest.raises(ValueError):
        list(capture_events([first, second]))


def test_capture_provider_error_cannot_disclose_raw_authentication_message() -> None:
    record = captured_row({"T": "error", "code": 402, "msg": "private-key-input"}, 1)
    with pytest.raises(StreamProviderError) as error:
        list(capture_events([record]))
    assert "private-key-input" not in str(error.value)


def test_quiet_same_connection_book_is_stale_for_decisions_but_not_a_lost_delta() -> None:
    tape, engine = Tape(), BookFeatureEngine(config())
    assert engine.on_event(tape.book(0, snapshot=True))
    assert engine.features("BTC/USD", NOW + timedelta(seconds=10)) is None
    assert engine.quote_at_ns("BTC/USD", BASE_NS + 10 * NS) is None
    assert not engine.health_snapshot()["resnapshot_required"]
    assert engine.on_event(tape.book(41, bid_size="20"))
    quote = engine.quote_at_ns("BTC/USD", BASE_NS + 41_010_000_000)
    assert quote is not None and quote.bid_size == 20
    assert engine.features("BTC/USD", NOW + timedelta(seconds=41.01)) is not None
    assert engine.health_snapshot()["local_receive_gaps"] == 0
    assert not engine.health_snapshot()["resnapshot_required"]
    tape.sequence += 1
    assert not engine.on_event(tape.book(42))
    assert engine.quote("BTC/USD") is None and engine.health_snapshot()["resnapshot_required"]


def test_feed_silence_does_not_disconnect_a_healthy_contiguous_capture() -> None:
    async def exercise() -> None:
        clock = FeedClock()
        socket = FakeSocket([*handshake(), [wire_book(0)], [wire_book(41, reset=False)]], clock)

        @asynccontextmanager
        async def transport() -> AsyncIterator[FakeSocket]:
            yield socket

        class RecordingFeed(CryptoMarketFeed):
            read_timeouts: ClassVar[list[float | None]] = []

            async def _messages(
                self, websocket: WebSocketConnection, *, timeout: float | None
            ) -> tuple[list[dict[str, Any]], datetime, int]:
                self.read_timeouts.append(timeout)
                return await super()._messages(websocket, timeout=timeout)

        feed = RecordingFeed(
            ["BTC/USD"],
            credentials(),
            transport=transport,
            clock=clock.clock,
            monotonic_ns=clock.monotonic,
        )
        seen = 0
        async for event in feed.stream():
            if event.event_type == "book":
                seen += 1
                if seen == 2:
                    assert feed.health_snapshot()["state"] == "streaming"
                    feed.stop()
        assert None in feed.read_timeouts
        assert feed.health_snapshot()["connections"] == 1
        assert feed.health_snapshot()["gap_count"] == 0

    asyncio.run(asyncio.wait_for(exercise(), timeout=3))
