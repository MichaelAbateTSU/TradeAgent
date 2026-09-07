from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal
from types import TracebackType
from typing import Any

import pytest
from pydantic import SecretStr, ValidationError
from websockets.datastructures import Headers
from websockets.exceptions import ConnectionClosedOK, InvalidHandshake, InvalidStatus
from websockets.frames import Close
from websockets.http11 import Response

from tradeagent import event_order_stream as stream_module
from tradeagent.alpaca_paper import AlpacaOrderStatus, AlpacaPaperSettings
from tradeagent.event_order_stream import (
    PAPER_TRADE_UPDATES_URL,
    AlpacaPaperTradeUpdatesStream,
    PaperTradeUpdate,
)

AUTH = {
    "stream": "authorization",
    "data": {"status": "authorized", "action": "authenticate"},
}
LISTENING = {"stream": "listening", "data": {"streams": ["trade_updates"]}}
NOW = datetime(2026, 9, 8, 13, 35, 1, tzinfo=UTC)


def _settings() -> AlpacaPaperSettings:
    return AlpacaPaperSettings(
        key_id=SecretStr("test-paper-key"),
        secret_key=SecretStr("test-paper-secret"),
    )


def _trade(
    *,
    event: str = "partial_fill",
    timestamp: str = "2026-09-08T13:35:01Z",
    filled: str = "0.05",
    status: str = "partially_filled",
) -> dict[str, Any]:
    return {
        "stream": "trade_updates",
        "data": {
            "event": event,
            "timestamp": timestamp,
            "execution_id": "execution-1",
            "event_id": "event-1",
            "qty": "0.05",
            "price": "200.001",
            "position_qty": filled,
            "unused_auth": {"key": "test-paper-key", "secret": "test-paper-secret"},
            "order": {
                "id": "order-1",
                "client_order_id": "v20-equipment-20260908",
                "symbol": "AAPL",
                "side": "buy",
                "status": status,
                "qty": "0.1",
                "filled_qty": filled,
                "filled_avg_price": "200.0005",
                "created_at": "2026-09-08T09:35:00-04:00",
                "updated_at": timestamp,
                "unused": "discard",
            },
        },
    }


def _frame(value: object, *, binary: bool = False) -> str | bytes:
    text = json.dumps(value)
    return text.encode("utf-8") if binary else text


class FakeWebSocket:
    def __init__(
        self, messages: Sequence[str | bytes | Exception], *, block_connect: bool = False
    ) -> None:
        self.messages = list(messages)
        self.block_connect = block_connect
        self.connect_started = asyncio.Event()
        self.connect_cancelled = False
        self.idle = asyncio.Event()
        self.sent: list[dict[str, Any]] = []
        self.closed = False
        self.receive_cancelled = False
        self.logger: logging.Logger | None = None
        self.before_recv: Callable[[], None] | None = None

    async def __aenter__(self) -> FakeWebSocket:
        self.connect_started.set()
        if self.block_connect:
            try:
                await asyncio.Future[None]()
            except asyncio.CancelledError:
                self.connect_cancelled = True
                raise
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.closed = True

    async def send(self, message: str) -> None:
        self.sent.append(json.loads(message))
        if self.logger is not None:
            self.logger.debug("outbound frame: %s", message)

    async def recv(self, decode: bool | None = None) -> str | bytes:
        if self.before_recv is not None:
            self.before_recv()
        if self.messages:
            await asyncio.sleep(0)
            message = self.messages.pop(0)
            if isinstance(message, Exception):
                raise message
            return message
        self.idle.set()
        try:
            await asyncio.Future[None]()
        except asyncio.CancelledError:
            self.receive_cancelled = True
            raise
        raise AssertionError("unreachable")


class FakeConnector:
    def __init__(self, attempts: list[FakeWebSocket | Exception]) -> None:
        self.attempts = attempts
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def __call__(self, url: str, **kwargs: Any) -> FakeWebSocket:
        self.calls.append((url, kwargs))
        assert self.attempts, "unexpected connection attempt"
        attempt = self.attempts.pop(0)
        if isinstance(attempt, Exception):
            raise attempt
        attempt.logger = kwargs["logger"]
        return attempt


@asynccontextmanager
async def _running(
    adapter: AlpacaPaperTradeUpdatesStream,
) -> AsyncIterator[tuple[asyncio.Event, asyncio.Task[None]]]:
    stop = asyncio.Event()
    task = asyncio.create_task(adapter.run(stop))
    try:
        yield stop, task
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=2)


def _hold_retries(
    monkeypatch: pytest.MonkeyPatch, adapter: AlpacaPaperTradeUpdatesStream
) -> tuple[asyncio.Event, list[float]]:
    retrying = asyncio.Event()
    delays: list[float] = []

    async def pause(delay: float) -> None:
        delays.append(delay)
        retrying.set()
        await asyncio.Future[None]()

    monkeypatch.setattr(adapter, "_retry_pause", pause)
    return retrying, delays


@pytest.mark.parametrize("binary", [False, True])
def test_fixed_paper_protocol_health_and_normalization(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, binary: bool
) -> None:
    caplog.set_level(logging.DEBUG)

    async def scenario() -> None:
        socket = FakeWebSocket(
            [_frame(value, binary=binary) for value in (AUTH, LISTENING, LISTENING, _trade())]
        )
        connector = FakeConnector([socket])
        monkeypatch.setattr(stream_module, "_PaperConnect", connector)
        adapter = AlpacaPaperTradeUpdatesStream(_settings())
        stages: list[dict[str, object]] = []
        socket.before_recv = lambda: stages.append(adapter.health_snapshot())
        initial = adapter.health_snapshot()
        assert (
            not initial["connected"] and not initial["authenticated"] and not initial["subscribed"]
        )
        async with _running(adapter):
            await asyncio.wait_for(socket.idle.wait(), 2)
            health = adapter.health_snapshot()
            assert health["running"] and health["connected"] and health["authenticated"]
            assert health["subscribed"] and health["state"] == "subscribed"
            assert health["gap_count"] == 0 and health["gap_started_at"] is None
            assert health["received_updates"] == 1 and health["messages_received"] == 4
            assert health["last_message_at"] and health["last_update_at"]
            assert stages[0]["connected"] and not stages[0]["authenticated"]
            assert stages[1]["authenticated"] and not stages[1]["subscribed"]
            assert stages[2]["subscribed"]
            updates = await asyncio.to_thread(adapter.drain)
            assert len(updates) == 1 and adapter.drain() == ()
            update = updates[0]
            assert update.timestamp == NOW
            assert update.received_at.tzinfo is UTC
            assert update.order_created_at == NOW.replace(second=0)
            assert update.order_updated_at == NOW
            assert update.order_id == "order-1"
            assert update.client_order_id == "v20-equipment-20260908"
            assert update.sequence == 1 and update.connection_id == 1
            assert update.status is AlpacaOrderStatus.PARTIALLY_FILLED
            assert update.filled_quantity == Decimal("0.05")
            assert update.filled_average_price == Decimal("200.0005")
            assert update.fill_price == Decimal("200.001")
            assert update.position_quantity == Decimal("0.05")
            assert update.execution_id == "execution-1" and update.event_id == "event-1"
            assert "unused" not in update.model_dump_json()
            with pytest.raises(ValidationError):
                update.sequence = 2
            assert socket.sent == [
                {"action": "auth", "key": "test-paper-key", "secret": "test-paper-secret"},
                {"action": "listen", "data": {"streams": ["trade_updates"]}},
            ]
        assert socket.closed and socket.receive_cancelled
        assert not adapter.health_snapshot()["subscribed"]
        assert "test-paper-key" not in json.dumps(health)
        assert "test-paper-secret" not in update.model_dump_json()
        url, options = connector.calls[0]
        assert url == PAPER_TRADE_UPDATES_URL == "wss://paper-api.alpaca.markets/stream"
        assert options["additional_headers"] == {"Content-Type": "application/json"}
        assert options["proxy"] is None
        assert options["open_timeout"] == 10
        assert options["max_size"] == 262144 and options["max_queue"] == 16
        assert options["ping_interval"] == options["ping_timeout"] == 20
        assert not options["logger"].isEnabledFor(logging.DEBUG)

    asyncio.run(scenario())
    assert "test-paper-key" not in caplog.text and "test-paper-secret" not in caplog.text


def test_duplicate_out_of_order_and_fractional_updates_are_not_applied_or_deduplicated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        latest = _trade(event="fill", filled="0.1", status="filled")
        earlier = _trade(timestamp="2026-09-08T13:35:00.123456789Z", filled="0.000000001")
        socket = FakeWebSocket([_frame(row) for row in (AUTH, LISTENING, latest, latest, earlier)])
        monkeypatch.setattr(stream_module, "_PaperConnect", FakeConnector([socket]))
        adapter = AlpacaPaperTradeUpdatesStream(_settings())
        async with _running(adapter):
            await asyncio.wait_for(socket.idle.wait(), 2)
        updates = adapter.drain()
        assert [row.sequence for row in updates] == [1, 2, 3]
        assert [row.event for row in updates] == ["fill", "fill", "partial_fill"]
        assert updates[0].timestamp == updates[1].timestamp
        assert updates[2].timestamp is not None and updates[0].timestamp is not None
        assert updates[2].timestamp < updates[0].timestamp
        assert updates[2].filled_quantity == Decimal("0.000000001")
        assert adapter.health_snapshot()["dropped_updates"] == 0

    asyncio.run(scenario())


def test_nonfill_can_omit_event_time_and_notional_order_quantity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        row = _trade(event="pending_cancel", filled="0", status="pending_cancel")
        for key in ("timestamp", "qty", "price", "position_qty"):
            row["data"].pop(key)
        row["data"]["order"].update(qty=None, notional="25", filled_avg_price=None)
        socket = FakeWebSocket([_frame(value) for value in (AUTH, LISTENING, row)])
        monkeypatch.setattr(stream_module, "_PaperConnect", FakeConnector([socket]))
        adapter = AlpacaPaperTradeUpdatesStream(_settings())
        async with _running(adapter):
            await asyncio.wait_for(socket.idle.wait(), 2)
            update = adapter.drain()[0]
            assert update.timestamp is None and update.quantity is None
            assert update.notional == Decimal("25")
            assert update.filled_average_price is None
            assert update.event == "pending_cancel"

    asyncio.run(scenario())


def _invalid_trade(path: str, value: object) -> str:
    row = _trade()
    target = row["data"]
    if path.startswith("order."):
        target = target["order"]
        path = path.removeprefix("order.")
    target[path] = value
    return json.dumps(row)


@pytest.mark.parametrize(
    ("payload", "code"),
    [
        ("not json test-paper-secret", "invalid_json"),
        (b"\xff", "invalid_json"),
        ("[]", "invalid_envelope"),
        ("null", "invalid_envelope"),
        ('{"stream":"trade_updates","data":[]}', "invalid_envelope"),
        ('{"data":{}}', "invalid_envelope"),
        ('{"stream":"account_updates","data":{}}', "unexpected_stream"),
        ('{"stream":"trade_updates","data":{}}', "invalid_trade_update"),
        (_invalid_trade("event", ""), "invalid_trade_update"),
        (_invalid_trade("event", {}), "invalid_trade_update"),
        (_invalid_trade("order.id", None), "invalid_trade_update"),
        (_invalid_trade("order.client_order_id", ""), "invalid_trade_update"),
        (_invalid_trade("order.symbol", " "), "invalid_trade_update"),
        (_invalid_trade("order.side", "short"), "invalid_trade_update"),
        (_invalid_trade("order.status", "unknown"), "invalid_trade_update"),
        (_invalid_trade("order.filled_qty", "-1"), "invalid_trade_update"),
        (_invalid_trade("order.filled_qty", "NaN"), "invalid_trade_update"),
        (_invalid_trade("order.filled_qty", True), "invalid_trade_update"),
        (_invalid_trade("order.filled_avg_price", None), "invalid_trade_update"),
        (_invalid_trade("order.filled_avg_price", "Infinity"), "invalid_trade_update"),
        (_invalid_trade("order.qty", "-1"), "invalid_trade_update"),
        (_invalid_trade("order.created_at", "2026-09-08T13:35:00"), "invalid_trade_update"),
        (_invalid_trade("order.updated_at", 1), "invalid_trade_update"),
        (_invalid_trade("timestamp", None), "invalid_trade_update"),
        (_invalid_trade("timestamp", "bad date"), "invalid_trade_update"),
        (_invalid_trade("price", None), "invalid_trade_update"),
        (_invalid_trade("qty", "0"), "invalid_trade_update"),
        (_invalid_trade("position_qty", None), "invalid_trade_update"),
    ],
)
def test_invalid_frames_leave_explicit_gap_and_retry_without_raw_payloads(
    monkeypatch: pytest.MonkeyPatch,
    payload: str | bytes,
    code: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def scenario() -> None:
        socket = FakeWebSocket([_frame(AUTH), _frame(LISTENING), payload])
        connector = FakeConnector([socket])
        monkeypatch.setattr(stream_module, "_PaperConnect", connector)
        adapter = AlpacaPaperTradeUpdatesStream(_settings())
        retrying, delays = _hold_retries(monkeypatch, adapter)
        async with _running(adapter):
            await asyncio.wait_for(retrying.wait(), 2)
            health = adapter.health_snapshot()
            assert health["last_error"] == code
            assert health["invalid_messages"] == 1 and health["gap_count"] == 1
            assert health["last_gap_at"] and health["gap_started_at"]
            assert health["state"] == "backoff"
            assert (
                not health["connected"] and not health["authenticated"] and not health["subscribed"]
            )
            assert adapter.drain() == ()
            assert socket.closed
        assert delays == [1.0] and len(connector.calls) == 1
        assert "test-paper-secret" not in json.dumps(health)

    asyncio.run(scenario())
    assert "test-paper-secret" not in caplog.text


@pytest.mark.parametrize("phase", ["auth", "listen", "subscribed"])
def test_unauthorized_is_terminal_and_never_claims_subscription(
    monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    async def scenario() -> None:
        rejected = {
            "stream": "authorization",
            "data": {
                "action": "authenticate" if phase == "auth" else "listen",
                "status": "unauthorized",
                "message": "test-paper-key test-paper-secret",
            },
        }
        rows = [] if phase == "auth" else [AUTH]
        if phase == "subscribed":
            rows.append(LISTENING)
        socket = FakeWebSocket([_frame(value, binary=True) for value in (*rows, rejected)])
        connector = FakeConnector([socket])
        monkeypatch.setattr(stream_module, "_PaperConnect", connector)
        adapter = AlpacaPaperTradeUpdatesStream(_settings())
        await asyncio.wait_for(adapter.run(asyncio.Event()), 2)
        health = adapter.health_snapshot()
        assert health["state"] == "failed" and health["last_error"] == "authentication_rejected"
        assert not health["running"] and not health["connected"]
        assert not health["authenticated"] and not health["subscribed"]
        assert health["gap_count"] == 1 and health["error_count"] == 1
        assert socket.closed and len(connector.calls) == 1
        assert len(socket.sent) == (1 if phase == "auth" else 2)
        assert adapter.drain() == ()
        assert "test-paper-key" not in json.dumps(health)
        assert "test-paper-secret" not in json.dumps(health)

    asyncio.run(scenario())


@pytest.mark.parametrize("streams", [[], ["other"], "trade_updates", None])
def test_missing_subscription_is_terminal(monkeypatch: pytest.MonkeyPatch, streams: object) -> None:
    async def scenario() -> None:
        socket = FakeWebSocket(
            [_frame(AUTH), _frame({"stream": "listening", "data": {"streams": streams}})]
        )
        monkeypatch.setattr(stream_module, "_PaperConnect", FakeConnector([socket]))
        adapter = AlpacaPaperTradeUpdatesStream(_settings())
        await asyncio.wait_for(adapter.run(asyncio.Event()), 2)
        health = adapter.health_snapshot()
        assert health["last_error"] == "subscription_rejected" and health["state"] == "failed"
        assert health["subscribed_at"] is None and not health["subscribed"]
        assert socket.closed

    asyncio.run(scenario())


def test_unvalidated_authentication_never_sends_listen(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> None:
        socket = FakeWebSocket(
            [_frame({"stream": "authorization", "data": {"status": "authorized"}})]
        )
        monkeypatch.setattr(stream_module, "_PaperConnect", FakeConnector([socket]))
        adapter = AlpacaPaperTradeUpdatesStream(_settings())
        retrying, _ = _hold_retries(monkeypatch, adapter)
        async with _running(adapter):
            await asyncio.wait_for(retrying.wait(), 2)
            assert adapter.health_snapshot()["last_error"] == "invalid_authorization"
            assert not adapter.health_snapshot()["authenticated"]
            assert len(socket.sent) == 1

    asyncio.run(scenario())


def test_server_error_is_sanitized_and_subscription_revocation_is_not_ignored(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        error = {"action": "error", "data": {"error_message": "test-paper-secret"}}
        first = FakeWebSocket([_frame(value) for value in (AUTH, LISTENING, error)])
        second = FakeWebSocket(
            [
                _frame(value)
                for value in (AUTH, LISTENING, {"stream": "listening", "data": {"streams": []}})
            ]
        )
        monkeypatch.setattr(stream_module, "_PaperConnect", FakeConnector([first, second]))
        adapter = AlpacaPaperTradeUpdatesStream(_settings())
        failures: list[dict[str, object]] = []

        async def retry(delay: float) -> None:
            failures.append(adapter.health_snapshot())
            await asyncio.sleep(0)

        monkeypatch.setattr(adapter, "_retry_pause", retry)
        await asyncio.wait_for(adapter.run(asyncio.Event()), 2)
        assert failures[0]["last_error"] == "server_error"
        assert "test-paper-secret" not in json.dumps(failures)
        health = adapter.health_snapshot()
        assert health["state"] == "failed" and health["last_error"] == "subscription_rejected"
        assert health["gap_count"] == 2 and not health["subscribed"]
        assert first.closed and second.closed

    asyncio.run(scenario())


def test_reconnect_backoff_is_capped_and_quick_subscription_does_not_reset_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        close = ConnectionClosedOK(Close(1000, "test-paper-secret"), Close(1000, ""), True)
        first = FakeWebSocket([_frame(AUTH), _frame(LISTENING), _frame(_trade()), close])
        last = FakeWebSocket([_frame(AUTH), _frame(LISTENING), _frame(_trade())])
        connector = FakeConnector(
            [
                OSError("test-paper-secret"),
                first,
                TimeoutError("test-paper-secret"),
                InvalidHandshake("test-paper-secret"),
                last,
            ]
        )
        monkeypatch.setattr(stream_module, "_PaperConnect", connector)
        adapter = AlpacaPaperTradeUpdatesStream(
            _settings(), reconnect_initial_seconds=0.5, reconnect_max_seconds=2
        )
        delays: list[float] = []

        async def retry(delay: float) -> None:
            delays.append(delay)
            assert not adapter.health_snapshot()["subscribed"]
            await asyncio.sleep(0)

        monkeypatch.setattr(adapter, "_retry_pause", retry)
        async with _running(adapter):
            await asyncio.wait_for(last.idle.wait(), 2)
            health = adapter.health_snapshot()
            assert health["connected"] and health["authenticated"] and health["subscribed"]
            assert health["connection_attempts"] == 5 and health["reconnects"] == 4
            assert health["connections"] == 2 and health["disconnects"] == 1
            assert health["gap_count"] == 4 and health["has_gaps"]
            assert health["gap_started_at"] is None
            assert health["last_error"] == "transport_lost"
            assert health["retry_delay_seconds"] == 0
            updates = adapter.drain()
            assert [row.sequence for row in updates] == [1, 2]
            assert [row.connection_id for row in updates] == [1, 2]
            assert updates[0].execution_id == updates[1].execution_id
        assert delays == [0.5, 1, 2, 2]
        assert all(url == PAPER_TRADE_UPDATES_URL for url, _ in connector.calls)
        assert first.closed and last.closed
        assert "test-paper-secret" not in json.dumps(health)

    asyncio.run(scenario())


def test_overflow_drops_oldest_and_retains_explicit_loss_after_drain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        socket = FakeWebSocket([_frame(AUTH), _frame(LISTENING), *[_frame(_trade())] * 5])
        monkeypatch.setattr(stream_module, "_PaperConnect", FakeConnector([socket]))
        adapter = AlpacaPaperTradeUpdatesStream(_settings(), queue_capacity=2)
        async with _running(adapter):
            await asyncio.wait_for(socket.idle.wait(), 2)
            health = adapter.health_snapshot()
            assert health["queue_depth"] == health["queue_capacity"] == 2
            assert health["received_updates"] == 5 and health["dropped_updates"] == 3
            assert health["gap_count"] == 3 and health["last_error"] == "queue_overflow"
            assert health["subscribed"] and health["state"] == "subscribed"
            assert [row.sequence for row in await asyncio.to_thread(adapter.drain)] == [4, 5]
            assert adapter.health_snapshot()["queue_depth"] == 0
            assert adapter.health_snapshot()["dropped_updates"] == 3
        assert adapter.health_snapshot()["has_gaps"]

    asyncio.run(scenario())


def test_owner_thread_can_drain_while_reader_produces(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> None:
        socket = FakeWebSocket([_frame(AUTH), _frame(LISTENING), *[_frame(_trade())] * 200])
        monkeypatch.setattr(stream_module, "_PaperConnect", FakeConnector([socket]))
        adapter = AlpacaPaperTradeUpdatesStream(_settings(), queue_capacity=16)
        collected: list[PaperTradeUpdate] = []
        async with _running(adapter):
            while not socket.idle.is_set():
                collected.extend(await asyncio.to_thread(adapter.drain))
                health = await asyncio.to_thread(adapter.health_snapshot)
                assert isinstance(health["queue_depth"], int) and 0 <= health["queue_depth"] <= 16
            collected.extend(await asyncio.to_thread(adapter.drain))
            final = adapter.health_snapshot()
            assert len(collected) + int(str(final["dropped_updates"])) == 200
            sequences = [row.sequence for row in collected]
            assert sequences == sorted(set(sequences))

    asyncio.run(asyncio.wait_for(scenario(), 5))


@pytest.mark.parametrize("stage", ["connect", "authenticate", "subscribe", "receive"])
def test_stop_interrupts_connection_handshake_or_idle_receive_and_leaks_no_tasks(
    monkeypatch: pytest.MonkeyPatch, stage: str
) -> None:
    async def scenario() -> None:
        frames = [] if stage in {"connect", "authenticate"} else [_frame(AUTH)]
        if stage == "receive":
            frames.append(_frame(LISTENING))
        socket = FakeWebSocket(frames, block_connect=stage == "connect")
        connector = FakeConnector([socket])
        monkeypatch.setattr(stream_module, "_PaperConnect", connector)
        adapter = AlpacaPaperTradeUpdatesStream(_settings())
        before = asyncio.all_tasks()
        async with _running(adapter):
            ready = socket.connect_started if stage == "connect" else socket.idle
            await asyncio.wait_for(ready.wait(), 2)
        health = adapter.health_snapshot()
        assert health["state"] == "stopped" and not health["running"]
        assert not health["connected"] and not health["authenticated"] and not health["subscribed"]
        assert health["stopped_at"] and health["gap_started_at"]
        assert len(connector.calls) == 1
        if stage == "connect":
            assert socket.connect_cancelled
        else:
            assert socket.closed and socket.receive_cancelled
        assert asyncio.all_tasks() == before

    asyncio.run(scenario())


def test_cancellation_propagates_after_cleanup_and_pre_set_stop_never_connects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        socket = FakeWebSocket([_frame(AUTH), _frame(LISTENING), _frame(_trade())])
        connector = FakeConnector([socket])
        monkeypatch.setattr(stream_module, "_PaperConnect", connector)
        adapter = AlpacaPaperTradeUpdatesStream(_settings())
        stop = asyncio.Event()
        stop.set()
        await adapter.run(stop)
        assert connector.calls == []
        task = asyncio.create_task(adapter.run(asyncio.Event()))
        await asyncio.wait_for(socket.idle.wait(), 2)
        with pytest.raises(RuntimeError, match="already running"):
            await adapter.run(asyncio.Event())
        assert adapter.health_snapshot()["subscribed"]
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert socket.closed and socket.receive_cancelled
        assert not adapter.health_snapshot()["running"]
        assert len(adapter.drain()) == 1

    asyncio.run(scenario())


@pytest.mark.parametrize("frames", [[], [json.dumps(AUTH)]])
def test_handshake_timeout_is_retried_and_stop_interrupts_long_backoff(
    monkeypatch: pytest.MonkeyPatch, frames: list[str]
) -> None:
    async def scenario() -> None:
        socket = FakeWebSocket(list(frames))
        connector = FakeConnector([socket])
        monkeypatch.setattr(stream_module, "_PaperConnect", connector)
        adapter = AlpacaPaperTradeUpdatesStream(
            _settings(), handshake_timeout_seconds=0.01, reconnect_initial_seconds=30
        )
        retrying, delays = _hold_retries(monkeypatch, adapter)
        async with _running(adapter):
            await asyncio.wait_for(retrying.wait(), 2)
            assert adapter.health_snapshot()["last_error"] == "handshake_timeout"
            assert not adapter.health_snapshot()["subscribed"]
        assert socket.closed and socket.receive_cancelled
        assert delays == [30] and len(connector.calls) == 1

    asyncio.run(scenario())


def test_unexpected_failure_is_terminal_and_sanitized(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> None:
        socket = FakeWebSocket([RuntimeError("test-paper-secret")])
        monkeypatch.setattr(stream_module, "_PaperConnect", FakeConnector([socket]))
        adapter = AlpacaPaperTradeUpdatesStream(_settings())
        await asyncio.wait_for(adapter.run(asyncio.Event()), 2)
        health = adapter.health_snapshot()
        assert health["state"] == "failed" and health["last_error"] == "reader_failed"
        assert not health["running"] and socket.closed
        assert "test-paper-secret" not in json.dumps(health)

    asyncio.run(scenario())


def test_stop_interrupts_real_retry_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> None:
        connector = FakeConnector([OSError("offline")])
        monkeypatch.setattr(stream_module, "_PaperConnect", connector)
        adapter = AlpacaPaperTradeUpdatesStream(_settings(), reconnect_initial_seconds=30)
        retrying = asyncio.Event()
        real_pause = adapter._retry_pause

        async def pause(delay: float) -> None:
            retrying.set()
            await real_pause(delay)

        monkeypatch.setattr(adapter, "_retry_pause", pause)
        async with _running(adapter):
            await asyncio.wait_for(retrying.wait(), 2)
            assert adapter.health_snapshot()["retry_delay_seconds"] == 30
        assert len(connector.calls) == 1
        assert adapter.health_snapshot()["state"] == "stopped"

    asyncio.run(scenario())


def test_restarting_reader_preserves_evidence_and_marks_missing_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        first = FakeWebSocket([_frame(value) for value in (AUTH, LISTENING, _trade())])
        second = FakeWebSocket([_frame(value) for value in (AUTH, LISTENING, _trade())])
        monkeypatch.setattr(stream_module, "_PaperConnect", FakeConnector([first, second]))
        adapter = AlpacaPaperTradeUpdatesStream(_settings())
        async with _running(adapter):
            await asyncio.wait_for(first.idle.wait(), 2)
        assert adapter.health_snapshot()["gap_started_at"]
        async with _running(adapter):
            await asyncio.wait_for(second.idle.wait(), 2)
            health = adapter.health_snapshot()
            assert health["last_gap_reason"] == "reader_restarted"
            assert health["gap_count"] == 1 and health["gap_started_at"] is None
            updates = adapter.drain()
            assert [row.sequence for row in updates] == [1, 2]
            assert [row.connection_id for row in updates] == [1, 2]

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "kwargs",
    [
        {"queue_capacity": 0},
        {"queue_capacity": -1},
        {"queue_capacity": True},
        {"queue_capacity": 1.5},
        {"reconnect_initial_seconds": 0},
        {"reconnect_initial_seconds": float("nan")},
        {"reconnect_max_seconds": float("inf")},
        {"reconnect_initial_seconds": 2, "reconnect_max_seconds": 1},
        {"handshake_timeout_seconds": -1},
    ],
)
def test_invalid_limits_are_rejected(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        AlpacaPaperTradeUpdatesStream(_settings(), **kwargs)


def test_empty_credentials_and_bypassed_live_settings_are_rejected() -> None:
    with pytest.raises(ValueError, match="PAPER host"):
        AlpacaPaperTradeUpdatesStream(
            _settings().model_copy(update={"paper_url": "https://api.alpaca.markets"})
        )
    with pytest.raises(ValueError, match="nonempty"):
        AlpacaPaperTradeUpdatesStream(_settings().model_copy(update={"key_id": SecretStr(" ")}))


@pytest.mark.parametrize(
    "location",
    [
        "wss://api.alpaca.markets/stream",
        "wss://another.invalid/stream",
        "wss://paper-api.alpaca.markets/another-path",
    ],
)
def test_websocket_redirects_are_never_followed(location: str) -> None:
    async def scenario() -> None:
        connector = stream_module._PaperConnect(PAPER_TRADE_UPDATES_URL)
        redirect = InvalidStatus(Response(302, "Found", Headers({"Location": location})))
        assert connector.process_redirect(redirect) is redirect

    asyncio.run(scenario())
