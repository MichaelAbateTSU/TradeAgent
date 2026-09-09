"""One explicitly authorized closed-market PAPER order/cancel test, not a fill test."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from typing import Any
from uuid import NAMESPACE_URL, uuid5

import httpx

from tradeagent.domain import OrderRequest, OrderType, Side
from tradeagent.experimental_policy import reject_live_environment
from tradeagent.notifications import RoundTripNotificationRepository

TEST_ID = "operator-paper-submit-cancel-20260909"
CLIENT_ID = "ta-probe-20260909-0020-aapl"
CONTROL_KEY = "operator-paper-probe:20260909:0020"
ACCOUNT = "b3ddf697092ad516c7e68024af06916f7c5dc921c8dfd44ff35ea6618b44947f"
COMMAND_KEY = "operator-paper-probe:20260909:request"
DEADLINE = datetime(2026, 9, 9, 5, tzinfo=UTC)
TERMINAL = {"canceled", "expired", "rejected", "filled"}
LIMIT = Decimal("100")
QUANTITY = Decimal("0.1")


def run_probe(
    broker: Any,
    repository: Any,
    store: Any,
    *,
    owner_id: str,
    code_sha: str,
    config_hash: str,
    cohort_id: str,
    assert_owner: Any,
    sleep: Any,
    attempts: int = 3,
) -> dict[str, Any]:
    reject_live_environment()
    raw_command = repository.get_control(f"{COMMAND_KEY}:{cohort_id}")
    if raw_command is None:
        raise ValueError("explicit operator command required")
    command = json.loads(raw_command)
    if (
        command.get("test_id") != TEST_ID
        or command.get("code_sha") != code_sha
        or command.get("config_hash") != config_hash
        or command.get("cohort_id") != cohort_id
        or command.get("account_digest") != ACCOUNT
        or command.get("action") != "submit_cancel_paper_limit"
    ):
        raise ValueError("operator command does not match the running worker")
    assert_owner(datetime.now(UTC))
    if broker.broker_host != "https://paper-api.alpaca.markets":
        raise ValueError("paper host required")
    account = broker.account()
    if sha256(account.id.encode()).hexdigest() != ACCOUNT:
        raise ValueError("paper account identity changed")
    saved = repository.get_control(CONTROL_KEY)
    state = json.loads(saved) if saved else None
    if state is not None and state.get("state") == "canceled_and_flat":
        enqueue_result_email(store.database, state)
        return dict(state)
    existing = broker.find_order_by_client_id(CLIENT_ID)
    post_result = "not_repeated"
    if state is None and existing is None:
        if account.status != "ACTIVE" or account.trading_blocked or account.account_blocked:
            raise ValueError("paper account is blocked")
        if repository.get_control("kill_switch") != "active":
            raise ValueError("normal strategy entry must remain paused")
        if broker.open_orders() or broker.positions():
            raise ValueError("account must be flat with no unrelated open orders")
        clock = broker.clock()
        if (
            clock.is_open
            or not datetime(2026, 9, 9, 4, tzinfo=UTC) <= clock.timestamp < DEADLINE
            or clock.next_open - clock.timestamp < timedelta(minutes=30)
        ):
            raise ValueError("new probe restricted to the explicit closed-market window")
        asset = broker.asset("AAPL")
        if not (
            asset.symbol == "AAPL"
            and asset.tradable
            and asset.fractionable
            and asset.status == "active"
            and asset.asset_class == "us_equity"
        ):
            raise ValueError("AAPL fractional equity unavailable")
        if account.cash < LIMIT * QUANTITY:
            raise ValueError("insufficient paper cash")
        request = OrderRequest(
            client_order_id=CLIENT_ID,
            decision_id=TEST_ID,
            strategy_id="operator-api-cancel-test-not-strategy",
            symbol="AAPL",
            side=Side.BUY,
            quantity=QUANTITY,
            order_type=OrderType.LIMIT,
            submitted_at=clock.timestamp,
        )
        state = {
            "test_id": TEST_ID,
            "client_order_id": CLIENT_ID,
            "state": "submission_outcome_unconfirmed",
            "authority": "owner explicitly requested immediate Render paper test Sep9 00:20 ET",
            "scope": "submit and cancel only; not a filled trade or strategy signal",
            "request": request.model_dump(mode="json"),
            "limit_price": str(LIMIT),
            "maximum_notional_usd": str(LIMIT * QUANTITY),
            "price_basis": "fixed non-marketable diagnostic ceiling; no fresh-price claim",
            "extended_hours": False,
            "time_in_force": "day",
            "normal_trading_pauses_unchanged": True,
            "deployed_build": os.getenv("RENDER_GIT_COMMIT"),
            "worker_owner": owner_id,
            "cohort_id": cohort_id,
            "config_hash": config_hash,
            "prepared_at": datetime.now(UTC).isoformat(),
        }
        repository.set_control(CONTROL_KEY, json.dumps(state))
        store.audit("operator_order_probe", state, datetime.now(UTC), TEST_ID)
        assert_owner(datetime.now(UTC))
        if repository.get_control("kill_switch") != "active":
            raise ValueError("normal trading pause changed before diagnostic submission")
        final_clock = broker.clock()
        if final_clock.is_open or not final_clock.timestamp < DEADLINE:
            raise ValueError("closed-market operator test window ended")
        try:
            existing = broker.submit_limit_order(request, LIMIT)
            post_result = "broker_acknowledged"
        except httpx.HTTPStatusError as error:
            if error.response.status_code in {400, 401, 403, 422}:
                post_result = f"broker_rejected_http_{error.response.status_code}"
            else:
                post_result = f"submission_unconfirmed_http_{error.response.status_code}"
        except httpx.TransportError:
            post_result = "submission_unconfirmed_transport"

    observed: list[dict[str, Any]] = []
    errors: list[str] = []
    for _ in range(attempts):
        try:
            order = broker.find_order_by_client_id(CLIENT_ID)
            if order is None:
                if post_result.startswith("broker_rejected_"):
                    break
            else:
                existing = order
                if (
                    order.client_order_id != CLIENT_ID
                    or order.symbol != "AAPL"
                    or order.side != "buy"
                    or order.quantity != QUANTITY
                ):
                    raise ValueError("returned order differs from the recorded probe identity")
                value = order.model_dump(mode="json")
                if not observed or observed[-1] != value:
                    observed.append(value)
                if order.status.value in TERMINAL:
                    break
                if order.status.value != "pending_cancel":
                    try:
                        assert_owner(datetime.now(UTC))
                        broker.cancel_order(order.id)
                    except httpx.HTTPStatusError as error:
                        if error.response.status_code not in {404, 422}:
                            raise
                        errors.append(f"cancel_race_http_{error.response.status_code}")
        except httpx.HTTPError as error:
            errors.append(type(error).__name__)
        sleep(2)

    final = broker.find_order_by_client_id(CLIENT_ID)
    positions = broker.positions()
    pending = broker.open_orders()
    canceled_flat = (
        final is not None
        and final.status.value in {"canceled", "expired"}
        and final.filled_quantity == 0
        and not positions
        and not pending
    )
    result = {
        "test_id": TEST_ID,
        "client_order_id": CLIENT_ID,
        "post_result": post_result,
        "state": "canceled_and_flat" if canceled_flat else "not_proven",
        "actual_filled_trade_proven": False,
        "broker_host": broker.broker_host,
        "deployed_build": os.getenv("RENDER_GIT_COMMIT"),
        "worker_owner": owner_id,
        "cohort_id": cohort_id,
        "config_hash": config_hash,
        "order": final.model_dump(mode="json") if final else None,
        "observations": observed,
        "errors": errors,
        "positions": [p.model_dump(mode="json") for p in positions],
        "open_orders": [o.model_dump(mode="json") for o in pending],
        "kill_switch": repository.get_control("kill_switch"),
        "completed_at": datetime.now(UTC).isoformat(),
        "prepared": state.get("prepared", state) if state else None,
    }
    print("PAPER_ORDER_PROBE " + json.dumps(result, default=str), flush=True)
    repository.set_control(CONTROL_KEY, json.dumps(result, default=str))
    store.audit("operator_order_probe", result, datetime.now(UTC), TEST_ID)
    if canceled_flat:
        enqueue_result_email(store.database, result)
    return result


def enqueue_result_email(database: Any, result: dict[str, Any]) -> None:
    order = result["order"]
    identity = uuid5(NAMESPACE_URL, f"tradeagent:{TEST_ID}:verified-result")
    RoundTripNotificationRepository(database).enqueue_status(
        identity,
        {
            "subject": "[TradeAgent PAPER TEST] Render worker order accepted and canceled",
            "text": "\n".join(
                [
                    "ACTUAL RENDER WORKER / ALPACA PAPER API TEST",
                    f"Test: {TEST_ID}",
                    f"Worker instance: {result['worker_owner']}",
                    f"Worker code: {result['deployed_build']}",
                    f"Broker: {result['broker_host']}",
                    f"Alpaca order ID: {order['id']}",
                    f"Client order ID: {CLIENT_ID}",
                    f"Symbol/side: {order['symbol']} / {order['side']}",
                    f"Quantity: {order['quantity']}; limit: $100; maximum notional: $10",
                    f"Created: {order['created_at']}",
                    f"Canceled: {order.get('canceled_at')}",
                    f"Final broker status: {order['status']}",
                    f"Filled quantity: {order['filled_quantity']}",
                    "Final positions: none. Final open orders: none.",
                    "Normal trading remains paused.",
                    "",
                    "This proves the running Render worker submitted a real PAPER order,",
                    "read it back, canceled it, and confirmed a flat account.",
                    "It does NOT prove a completed buy/sell round trip or profitability:",
                    "the regular market was closed and the order did not fill.",
                    "No real money, live account, or trading limits were changed.",
                ]
            ),
            "test_id": TEST_ID,
            "operator_probe": True,
            "actual_filled_trade_proven": False,
        },
        created_at=datetime.now(UTC),
    )
