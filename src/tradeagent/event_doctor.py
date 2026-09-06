from __future__ import annotations

import os
import subprocess
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any

import httpx

from tradeagent.alpaca import AlpacaDataClient, AlpacaDataSettings
from tradeagent.alpaca_paper import AlpacaPaperClient, AlpacaPaperSettings
from tradeagent.config import AppConfig
from tradeagent.event_market import EventMarketClient
from tradeagent.experimental_policy import ExperimentalSettings


def code_identity() -> str:
    if os.getenv("RENDER_GIT_COMMIT"):
        return str(os.environ["RENDER_GIT_COMMIT"])
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True
    )
    return result.stdout.strip()


def source_capabilities() -> dict[str, Any]:
    now = datetime.now(UTC)
    settings = ExperimentalSettings()
    data = AlpacaDataSettings.model_validate({})
    paper_settings = AlpacaPaperSettings.model_validate({})
    with AlpacaPaperClient(paper_settings) as paper:
        account = paper.account()
        clock = paper.clock()
        assets = [paper.asset(symbol.strip()) for symbol in settings.symbols.split(",")]
        positions, pending = paper.positions(), paper.open_orders()
    market = EventMarketClient(data)
    capabilities: dict[str, Any] = {}
    try:
        for feed in ("sip", "iex"):
            try:
                payload = market.get("/v2/stocks/AAPL/quotes/latest", {"feed": feed})
                quote = payload["quote"]
                capabilities[f"{feed}_latest_quote"] = {
                    "accessible": True,
                    "quote_at": quote["t"],
                    "real_time_freshness": "not established outside market hours",
                    "nbbo": feed == "sip",
                }
            except httpx.HTTPStatusError as error:
                if error.response.status_code not in {401, 403}:
                    raise
                capabilities[f"{feed}_latest_quote"] = {
                    "accessible": False,
                    "http_status": error.response.status_code,
                    "reason": "entitlement_or_authentication_denied",
                }
        with AlpacaDataClient(data) as historical:
            probe = historical.probe_historical_sip(
                "AAPL",
                start=now - timedelta(days=10),
                end=now - timedelta(days=10, minutes=-1),
            )
        capabilities["historical_sip"] = probe.model_dump(mode="json")
    finally:
        market.close()
    app = AppConfig()
    return {
        "code_sha": code_identity(),
        "observed_at": now.isoformat(),
        "mode": settings.mode,
        "paper_host": paper_settings.paper_url,
        "account_suffix": account.id[-4:],
        "account_digest": sha256(account.id.encode()).hexdigest(),
        "account_product": "Alpaca paper API; not inferred from live-account credentials",
        "broker_healthy": account.status == "ACTIVE"
        and not account.trading_blocked
        and not account.account_blocked,
        "cash_positive": account.cash > 0,
        "broker_positions": len(positions),
        "broker_open_orders": len(pending),
        "clock": clock.model_dump(mode="json"),
        "market_data": capabilities,
        "assets": [asset.model_dump(mode="json") for asset in assets],
        "news": {
            "provider": "Alpaca/Benzinga",
            "poll": "enabled; source worker reports actual receipt",
            "original_historical_revisions": "unknown",
            "stream": "not separately probed",
        },
        "consensus": {
            "state": "unavailable",
            "reason": "no earnings/consensus entitlement configured",
        },
        "inference": {
            "provider": "none configured",
            "fallback": "deterministic evidence extractor",
            "calls_daily_cap": 0,
            "variable_cost_usd": "0",
        },
        "virtual_equity": str(settings.virtual_equity),
        "effective_entry_cap_usd": str(settings.effective_notional(app)),
        "max_positions": 1,
        "max_entries_per_session": settings.max_entries_per_session,
        "daily_loss_dollars": str(settings.virtual_equity * settings.daily_loss_fraction),
        "drawdown_stop_dollars": str(settings.virtual_equity * settings.drawdown_fraction),
        "live_execution_available": False,
        "historical_alpha_required_for_experiment": False,
        "statistical_qualification": "not established",
        "fee_rounding": "conservative reserve; account-activity reconciliation required",
        "size_units": "raw provider quantities retained; historical schema not assumed",
        "live_credential_environment_present": any(
            name in os.environ
            for name in ("ALPACA_LIVE_KEY", "ALPACA_LIVE_KEY_ID", "ALPACA_LIVE_SECRET_KEY")
        ),
    }
