"""Explicit one-time operator crypto PAPER test; never a news strategy or live route."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from typing import Any
from uuid import NAMESPACE_URL, uuid5

import httpx

from tradeagent.domain import OrderRequest, Side
from tradeagent.experimental_policy import reject_live_environment
from tradeagent.notifications import RoundTripNotificationRepository

TEST_ID = "operator-btc-paper-20260909-0151"
COMMAND = "operator-btc-test:20260909:request"
STATE = "operator-btc-test:20260909:state"
BUY_ID = "ta-crypto-test-20260909-0151-buy"
ACCOUNT = "b3ddf697092ad516c7e68024af06916f7c5dc921c8dfd44ff35ea6618b44947f"
ENTRY_DEADLINE = datetime(2026, 9, 9, 7, tzinfo=UTC)
FINAL = {"filled", "canceled", "expired", "rejected"}
FEE_RATE = Decimal("0.0025")


def _stamp(value: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError("timestamp must be a string")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp must be timezone aware")
    return parsed.astimezone(UTC)


def _btc(symbol: str) -> bool:
    return symbol.replace("/", "") == "BTCUSD"


def _quote(market: Any) -> tuple[dict[str, Any], Decimal, Decimal]:
    payload = market.get("/v1beta3/crypto/us/latest/quotes", {"symbols": "BTC/USD"})
    quotes = payload.get("quotes")
    quote = quotes.get("BTC/USD") if isinstance(quotes, dict) else None
    if not isinstance(quote, dict) or not all(
        key in quote for key in ("bp", "ap", "bs", "as", "t")
    ):
        raise ValueError("crypto quote fields unavailable")
    try:
        bid, ask, bid_size, ask_size = (
            Decimal(str(quote[key])) for key in ("bp", "ap", "bs", "as")
        )
        stamp = _stamp(quote["t"])
    except (ArithmeticError, TypeError, ValueError) as error:
        raise ValueError("malformed crypto quote") from error
    if (
        not all(value.is_finite() for value in (bid, ask, bid_size, ask_size))
        or bid <= 0
        or ask < bid
        or bid_size <= 0
        or ask_size <= 0
        or (ask - bid) / ask > Decimal("0.005")
        or not timedelta(0) <= datetime.now(UTC) - stamp <= timedelta(seconds=5)
    ):
        raise ValueError("fresh valid crypto quote/spread required")
    return quote, bid, ask


def _notify(store: Any, stage: str, value: dict[str, Any]) -> None:
    identity = uuid5(NAMESPACE_URL, f"tradeagent:{TEST_ID}:{stage}")
    RoundTripNotificationRepository(store.database).enqueue_status(
        identity,
        {
            "subject": f"[TradeAgent PAPER BTC TEST] {stage}",
            "text": (
                "Actual running Render worker crypto PAPER test. No real money.\n"
                "This is the owner's explicit equipment test, NOT a qualified news signal.\n\n"
                + json.dumps(value, indent=2, default=str)
                + "\n\nProfit is not guaranteed. Fee-adjusted figures are estimates until "
                "broker fee activities reconcile. Stop/time exits can realize a loss."
            ),
            "test_id": TEST_ID,
            "crypto_paper_test": True,
            "qualification_eligible": False,
        },
        created_at=datetime.now(UTC),
    )


def _save(repo: Any, store: Any, state: dict[str, Any], kind: str = "crypto_test") -> None:
    state["observed_at"] = datetime.now(UTC).isoformat()
    repo.set_control(STATE, json.dumps(state, default=str))
    store.audit(kind, state, datetime.now(UTC), TEST_ID)
    print("CRYPTO_PAPER_TEST " + json.dumps(state, default=str), flush=True)


def _observe(broker: Any, state: dict[str, Any]) -> None:
    for key, side in [
        ("buy", "buy"),
        *[(f"sell_{i}", "sell") for i in range(state.get("exit_count", 0))],
    ]:
        intent = state.get(key)
        if intent is None:
            continue
        current = broker.find_order_by_client_id(intent["client_order_id"])
        if current is None:
            if intent.get("submission_status") == "rejection_requires_lookup":
                intent["submission_status"] = "rejected"
            continue
        if (
            current.client_order_id != intent["client_order_id"]
            or not _btc(current.symbol)
            or current.side != side
            or current.filled_quantity < 0
        ):
            raise ValueError("broker order does not match the crypto test identity")
        prior = intent.get("broker")
        if prior and current.filled_quantity < Decimal(prior["filled_quantity"]):
            raise ValueError("broker crypto cumulative fill decreased")
        intent["broker"] = current.model_dump(mode="json")


def _dispatch(
    broker: Any,
    repo: Any,
    store: Any,
    state: dict[str, Any],
    key: str,
    owner_check: Any,
    *,
    quantity: Decimal | None = None,
) -> None:
    owner_check(datetime.now(UTC))
    _save(repo, store, state, "crypto_test_intent")
    intent = state[key]
    if key == "buy":
        owner_check(datetime.now(UTC))
        if repo.get_control("kill_switch") != "active":
            raise ValueError("ordinary trading pause changed before crypto entry")
        now = datetime.now(UTC)
        if now >= ENTRY_DEADLINE or not timedelta(0) <= now - _stamp(
            state["entry_quote"]["t"]
        ) <= timedelta(seconds=5):
            intent["submission_status"] = "rejected"
            intent["reason"] = "final_crypto_quote_or_entry_window_expired_before_post"
            _save(repo, store, state)
            return
    try:
        if key == "buy":
            response = broker.submit_bitcoin_test_buy(intent["client_order_id"])
        else:
            if quantity is None or quantity <= 0:
                raise ValueError("confirmed owned quantity required for crypto exit")
            response = broker.submit_market_order(
                OrderRequest(
                    client_order_id=intent["client_order_id"],
                    decision_id=TEST_ID,
                    strategy_id="operator-crypto-paper-test",
                    symbol="BTC/USD",
                    side=Side.SELL,
                    quantity=quantity,
                    submitted_at=datetime.now(UTC),
                )
            )
        intent["broker"] = response.model_dump(mode="json")
        intent["submission_status"] = "acknowledged"
    except httpx.HTTPStatusError as error:
        intent["submission_status"] = "outcome_unknown"
        intent["http_status"] = error.response.status_code
        if error.response.status_code in {400, 401, 403, 422}:
            intent["submission_status"] = "rejection_requires_lookup"
            _save(repo, store, state, "crypto_test_rejection")
            existing = broker.find_order_by_client_id(intent["client_order_id"])
            if existing is not None:
                intent["broker"] = existing.model_dump(mode="json")
                intent["submission_status"] = "recovered"
            else:
                intent["submission_status"] = "rejected"
    except httpx.TransportError:
        intent["submission_status"] = "outcome_unknown"
    _save(repo, store, state, "crypto_test_submission")
    _notify(
        store,
        f"{key} {intent['submission_status']}",
        {
            "worker": state["worker_owner"],
            "test_id": TEST_ID,
            "order": intent,
        },
    )


def step_crypto_test(
    broker: Any,
    market: Any,
    repo: Any,
    store: Any,
    *,
    owner_id: str,
    code_sha: str,
    config_hash: str,
    cohort_id: str,
    assert_owner: Any,
) -> dict[str, Any] | None:
    raw = repo.get_control(f"{COMMAND}:{cohort_id}")
    if raw is None:
        return None
    command = json.loads(raw)
    if any(
        command.get(k) != v
        for k, v in {
            "test_id": TEST_ID,
            "account_digest": ACCOUNT,
            "code_sha": code_sha,
            "config_hash": config_hash,
            "cohort_id": cohort_id,
            "action": "one_btc_paper_round_trip",
        }.items()
    ):
        raise ValueError("crypto command differs from the explicit running-worker scope")
    reject_live_environment()
    assert_owner(datetime.now(UTC))
    if broker.broker_host != "https://paper-api.alpaca.markets":
        raise ValueError("crypto test requires the paper host")
    account = broker.account()
    if sha256(account.id.encode()).hexdigest() != ACCOUNT:
        raise ValueError("crypto paper account identity changed")
    raw_state = repo.get_control(STATE)
    state: dict[str, Any] | None = json.loads(raw_state) if raw_state else None
    if state and state.get("state") in {"completed", "rejected_without_fill"}:
        _notify(store, state["state"], state)
        return dict(state)
    now = datetime.now(UTC)
    if state is None:
        if not datetime(2026, 9, 9, 5, tzinfo=UTC) <= now < ENTRY_DEADLINE:
            raise ValueError("new crypto test window is closed")
        if account.status != "ACTIVE" or account.account_blocked or account.trading_blocked:
            raise ValueError("paper account blocked")
        if account.cash < Decimal(25) or broker.positions() or broker.open_orders():
            raise ValueError("flat unreserved funded paper account required")
        if repo.get_control("kill_switch") != "active":
            raise ValueError("ordinary strategy trading must stay paused")
        asset = broker.bitcoin_asset()
        if (
            asset.get("class") != "crypto"
            or asset.get("symbol") != "BTC/USD"
            or not asset.get("tradable")
            or asset.get("status") != "active"
        ):
            raise ValueError("BTC/USD crypto asset unavailable")
        news = market.get(
            "/v1beta1/news",
            {
                "symbols": "BTCUSD,BTC",
                "start": (now - timedelta(hours=24)).isoformat(),
                "end": now.isoformat(),
                "limit": 20,
                "sort": "desc",
                "include_content": "false",
            },
        )
        if not isinstance(news.get("news"), list):
            raise ValueError("crypto news response missing")
        stories = [
            {
                k: row.get(k)
                for k in ("id", "headline", "url", "source", "created_at", "updated_at", "symbols")
            }
            for row in news["news"]
        ]
        for story in stories:
            if not story.get("created_at") or _stamp(story["created_at"]) > datetime.now(UTC):
                raise ValueError("crypto news publication timestamp invalid")
        quote, _, ask = _quote(market)
        now = datetime.now(UTC)
        minimum = Decimal(str(asset.get("min_order_size", "0")))
        if minimum * ask > Decimal(20):
            raise ValueError("asset minimum exceeds the $20 paper test budget")
        state = {
            "test_id": TEST_ID,
            "state": "buy_submitted",
            "worker_owner": owner_id,
            "code_sha": code_sha,
            "cohort_id": cohort_id,
            "account_digest": ACCOUNT,
            "started_at": now.isoformat(),
            "exit_due_at": (now + timedelta(minutes=5)).isoformat(),
            "maximum_notional_usd": "20",
            "symbol": "BTC/USD",
            "asset": asset,
            "entry_quote": quote,
            "news": stories,
            "news_assessment": (
                "Context recorded by worker; no validated crypto news strategy. "
                "Explicit operator test."
            ),
            "confidence": None,
            "qualification_eligible": False,
            "exit_count": 0,
            "exit_policy": (
                "Sell on estimated positive net >$0.01; "
                "otherwise 1% gross loss or 5-minute deadline."
            ),
            "fee_assumption_each_side": str(FEE_RATE),
            "buy": {
                "client_order_id": BUY_ID,
                "prepared_at": now.isoformat(),
                "submission_status": "outcome_unknown",
            },
        }
        _dispatch(broker, repo, store, state, "buy", assert_owner)
        return state

    _observe(broker, state)
    buy = state["buy"].get("broker")
    if buy is None:
        if state["buy"].get("submission_status") == "rejected":
            state["state"] = "rejected_without_fill"
            _save(repo, store, state)
            _notify(store, "rejected_without_fill", state)
            return state
        state["state"] = "entry_outcome_unknown"
        _save(repo, store, state)
        return state
    buy_qty = Decimal(buy["filled_quantity"])
    if buy["status"] not in FINAL:
        if now - _stamp(state["started_at"]) >= timedelta(seconds=30):
            assert_owner(datetime.now(UTC))
            broker.cancel_order(buy["id"])
            state["buy"]["cancel_requested_at"] = datetime.now(UTC).isoformat()
        _save(repo, store, state)
        return state
    if buy_qty > 0:
        _notify(
            store,
            "buy filled",
            {
                "test_id": TEST_ID,
                "worker_owner": state["worker_owner"],
                "order": buy,
                "exit_due_at": state["exit_due_at"],
                "exit_policy": state["exit_policy"],
            },
        )
    sell_keys = [f"sell_{i}" for i in range(state["exit_count"])]
    for key in sell_keys:
        intent = state[key]
        order = intent.get("broker")
        if order is None and intent.get("submission_status") != "rejected":
            state["state"] = "exit_outcome_unknown"
            _save(repo, store, state)
            return state
        if order is not None and order["status"] not in FINAL:
            if now - _stamp(intent["prepared_at"]) >= timedelta(seconds=30):
                assert_owner(datetime.now(UTC))
                broker.cancel_order(order["id"])
                intent["cancel_requested_at"] = datetime.now(UTC).isoformat()
            _save(repo, store, state)
            return state
    positions = broker.positions()
    position = next((p for p in positions if _btc(p.symbol)), None)
    pending = broker.open_orders()
    own_ids = {BUY_ID, *(state[key]["client_order_id"] for key in sell_keys)}
    if any(o.client_order_id not in own_ids for o in pending):
        state["incident"] = (
            "unrelated open order observed; only original owned BTC may be recovered"
        )
    if position is None:
        if pending:
            state["state"] = "waiting_for_no_open_orders"
        elif buy_qty > 0 and any(
            Decimal((state[key].get("broker") or {}).get("filled_quantity", "0")) > 0
            for key in sell_keys
        ):
            sell_value = sum(
                (
                    Decimal(order["filled_quantity"]) * Decimal(order["filled_average_price"])
                    for key in sell_keys
                    if (order := state[key].get("broker")) and Decimal(order["filled_quantity"]) > 0
                ),
                Decimal(0),
            )
            buy_value = buy_qty * Decimal(buy["filled_average_price"])
            state.update(
                state="completed",
                broker_buy_fill_value=str(buy_value),
                broker_sell_fill_value=str(sell_value),
                fill_value_difference=str(sell_value - buy_value),
                conservative_net_estimate=str(
                    sell_value * (1 - FEE_RATE)
                    - buy_value
                    - Decimal(state.get("entry_fee_reserve_not_in_quantity", "0"))
                ),
                completed_at=now.isoformat(),
                ending_positions=[p.model_dump(mode="json") for p in positions],
                ending_open_orders=[],
                profit_guaranteed=False,
            )
            _notify(store, "completed", state)
        elif buy_qty == 0:
            state["state"] = "rejected_without_fill"
        else:
            state["state"] = "incident_position_missing_without_exit"
        _save(repo, store, state)
        return state
    sold = sum(
        (
            Decimal((state[key].get("broker") or {}).get("filled_quantity", "0"))
            for key in sell_keys
        ),
        Decimal(0),
    )
    if position.quantity <= 0 or position.quantity > buy_qty - sold:
        raise ValueError("BTC position quantity exceeds the recorded owned purchase")
    if not buy.get("filled_average_price"):
        raise ValueError("filled entry price unavailable")
    if not sell_keys:
        state["credited_quantity_observed"] = str(position.quantity)
        entry_price = Decimal(buy["filled_average_price"])
        state["entry_fee_reserve_not_in_quantity"] = str(
            max(
                Decimal(0),
                buy_qty * entry_price * FEE_RATE - (buy_qty - position.quantity) * entry_price,
            )
        )
    due = now >= _stamp(state["exit_due_at"])
    reason = state.get("exit_reason") or ("five_minute_time_limit" if due else None)
    if reason is None:
        try:
            quote, bid, _ = _quote(market)
            price = Decimal(buy["filled_average_price"])
            net = (
                position.quantity * bid * (1 - FEE_RATE)
                - buy_qty * price
                - Decimal(state["entry_fee_reserve_not_in_quantity"])
            )
            state["latest_net_estimate"] = str(net)
            state["latest_quote"] = quote
            if net > Decimal("0.01"):
                reason = "positive_net_estimate"
            elif bid <= price * Decimal("0.99"):
                reason = "one_percent_loss_limit"
        except (httpx.HTTPError, ValueError):
            reason = "protective_quote_unavailable"
    if reason:
        if state["exit_count"] >= 5:
            state["state"] = "incident_exit_recovery_limit"
            _save(repo, store, state)
            return state
        state["exit_reason"] = reason
        index = state["exit_count"]
        key = f"sell_{index}"
        state["exit_count"] += 1
        state[key] = {
            "client_order_id": f"ta-crypto-test-20260909-0151-s{index}",
            "prepared_at": datetime.now(UTC).isoformat(),
            "quantity": str(position.quantity),
            "submission_status": "outcome_unknown",
        }
        state["state"] = "exiting"
        _dispatch(broker, repo, store, state, key, assert_owner, quantity=position.quantity)
    else:
        state["state"] = "monitoring"
        _save(repo, store, state)
    return state
