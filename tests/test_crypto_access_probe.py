from __future__ import annotations

import asyncio
import hashlib
import json
from typing import Any

import httpx
import pytest
from pydantic import SecretStr

from infra.render import crypto_access_probe as probe
from tradeagent.alpaca_paper import AlpacaPaperSettings


def settings() -> AlpacaPaperSettings:
    return AlpacaPaperSettings(
        key_id=SecretStr("fixture-key"),
        secret_key=SecretStr("fixture-secret"),
        paper_url="https://paper-api.alpaca.markets",
    )


def guard() -> dict[str, Any]:
    return {
        "state": "paused_invalid",
        "entry_policy": "shadow-research-dataset-v1",
        "trading_authorization": "expired",
        "model_state": "no_support",
        "orders_submitted": 0,
        "economic_entries_enabled": False,
        "owner_id": probe.OWNER,
        "account_digest": probe.ACCOUNT_DIGEST,
        "code_sha": probe.SOURCE_SHA,
        "config_hash": "frozen-config",
        "dataset_id": "original-v1",
        "active": True,
        "feed": {
            "state": "stopped",
            "authenticated": False,
            "subscribed": False,
            "connection_id": probe.CONNECTION,
        },
    }


class Socket:
    def __init__(self, messages: list[Any]) -> None:
        self.messages = iter(messages)
        self.sent: list[dict[str, Any]] = []
        self.closed = False

    async def __aenter__(self) -> Socket:
        return self

    async def __aexit__(self, *args: object) -> None:
        self.closed = True

    async def send(self, raw: str) -> None:
        self.sent.append(json.loads(raw))

    async def recv(self) -> str:
        try:
            message = next(self.messages)
        except StopIteration:
            raise TimeoutError from None
        return message if isinstance(message, str) else json.dumps(message)


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("state", "running"),
        ("entry_policy", "other"),
        ("trading_authorization", "active"),
        ("model_state", "supported"),
        ("orders_submitted", 1),
        ("economic_entries_enabled", True),
        ("owner_id", "new-owner"),
        ("account_digest", "different-account"),
        ("code_sha", "different-source"),
        ("active", False),
        ("config_hash", None),
    ],
)
def test_guard_rejects_unsafe_or_unpinned_state(key: str, value: Any) -> None:
    snapshot = guard()
    snapshot[key] = value
    with pytest.raises(ValueError, match="safety proof"):
        probe.require_stopped_guard(snapshot)


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("state", "running"),
        ("authenticated", True),
        ("subscribed", True),
        ("connection_id", "new-connection"),
    ],
)
def test_guard_rejects_changed_feed(key: str, value: Any) -> None:
    snapshot = guard()
    snapshot["feed"][key] = value
    with pytest.raises(ValueError, match="safety proof"):
        probe.require_stopped_guard(snapshot)


def test_discovery_captures_exact_native_payload_but_not_sent_secrets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    quote = '[{"T":"q","S":"BTC/USD","bp":123.1234567890123456789,"t":"provider-clock"}]'
    socket = Socket(
        [
            [{"T": "success", "msg": "connected"}],
            [{"T": "success", "msg": "authenticated"}],
            [{"T": "subscription", "quotes": ["BTC/USD", "ETH/USD"]}],
            quote,
        ]
    )
    endpoints = []

    def connect(endpoint: str, **kwargs: Any) -> Socket:
        endpoints.append(endpoint)
        return socket

    monkeypatch.setattr(probe, "connect", connect)
    report = asyncio.run(probe.probe_location(settings(), "us-1"))
    assert report["status"] == "access_observed"
    assert report["quote_counts"] == {"BTC/USD": 1, "ETH/USD": 0}
    assert report["frames"][-1]["raw"] == quote
    assert socket.sent[0]["action"] == "auth"
    assert socket.sent[1] == {"action": "subscribe", "quotes": ["BTC/USD", "ETH/USD"]}
    assert socket.closed
    assert "fixture-secret" not in json.dumps(report)
    assert endpoints == ["wss://stream.data.alpaca.markets/v1beta3/crypto/us-1"]


def test_connection_limit_is_retained_before_sending_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    socket = Socket([[{"T": "error", "code": 406, "msg": "connection limit exceeded"}]])
    monkeypatch.setattr(probe, "connect", lambda *args, **kwargs: socket)
    report = asyncio.run(probe.probe_location(settings(), "eu-1"))
    assert report["status"] == "provider_rejected"
    assert report["provider_errors"][0]["code"] == 406
    assert not socket.sent
    assert socket.closed


def test_unsolicited_authentication_does_not_establish_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    socket = Socket(
        [
            [{"T": "success", "msg": "authenticated"}],
            [{"T": "subscription", "quotes": ["BTC/USD", "ETH/USD"]}],
        ]
    )
    monkeypatch.setattr(probe, "connect", lambda *args, **kwargs: socket)
    report = asyncio.run(probe.probe_location(settings(), "us"))
    assert report["status"] == "authentication_or_subscription_not_established"
    assert not socket.sent


def test_malformed_provider_payload_is_not_success(monkeypatch: pytest.MonkeyPatch) -> None:
    socket = Socket([{"T": "success", "msg": "connected"}])
    monkeypatch.setattr(probe, "connect", lambda *args, **kwargs: socket)
    with pytest.raises(ValueError, match="list of objects"):
        asyncio.run(probe.probe_location(settings(), "us"))
    assert socket.closed


def test_undocumented_endpoint_is_rejected() -> None:
    with pytest.raises(ValueError, match="documented"):
        asyncio.run(probe.probe_location(settings(), "arbitrary-host"))


def test_guard_changes_stop_before_opening_another_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = probe.require_stopped_guard(guard())
    calls: list[str] = []
    guards = iter([first, first, ("changed",)])
    monkeypatch.setattr(probe, "current_guard", lambda: next(guards))
    monkeypatch.setattr(probe, "verify_paper_identity", lambda supplied: None)

    async def location(supplied: AlpacaPaperSettings, name: str) -> dict[str, Any]:
        calls.append(name)
        return {"location": name}

    monkeypatch.setattr(probe, "probe_location", location)
    with pytest.raises(ValueError, match="identity changed"):
        asyncio.run(probe.collect_access(settings()))
    assert calls == ["us-1"]


def test_paper_identity_uses_only_pinned_gets(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    class Response:
        def raise_for_status(self) -> None:
            pass

        def json(self) -> Any:
            return {"id": "fixture-account"} if calls[-1] == "/v2/account" else []

    class Client:
        def __init__(self, **kwargs: Any) -> None:
            assert kwargs["base_url"] == "https://paper-api.alpaca.markets"

        def __enter__(self) -> Client:
            return self

        def __exit__(self, *args: object) -> None:
            pass

        def get(self, path: str) -> Response:
            calls.append(path)
            return Response()

    monkeypatch.setattr(httpx, "Client", Client)
    monkeypatch.setattr(
        probe, "ACCOUNT_DIGEST", hashlib.sha256(b"fixture-account").hexdigest()
    )
    probe.verify_paper_identity(settings())
    assert calls == ["/v2/account", "/v2/positions", "/v2/orders?status=open&limit=500"]


def test_transport_failure_is_explicit(monkeypatch: pytest.MonkeyPatch) -> None:
    def connect(*args: Any, **kwargs: Any) -> Socket:
        raise OSError("fixture failure")

    monkeypatch.setattr(probe, "connect", connect)
    report = asyncio.run(probe.probe_location(settings(), "us"))
    assert report["status"] == "transport_failure"
    assert report["transport_error_type"] == "OSError"
    assert not report["authenticated"]


def test_capture_limit_does_not_look_like_a_quality_pass(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    socket = Socket(["x" * (1024 * 1024 + 1)])
    monkeypatch.setattr(probe, "connect", lambda *args, **kwargs: socket)
    report = asyncio.run(probe.probe_location(settings(), "us"))
    assert report["status"] == "bounded_capture_limit"
    assert not report["frames"]
    assert socket.closed
