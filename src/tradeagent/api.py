from __future__ import annotations

# ruff: noqa: E501
from datetime import UTC, datetime, timedelta
from pathlib import Path

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, PlainTextResponse

from tradeagent.broker import PaperBroker
from tradeagent.config import BrokerConfig
from tradeagent.domain import AccountSnapshot, PaperBrokerState
from tradeagent.ledger import SQLiteLedger
from tradeagent.news import NewsRepository
from tradeagent.persistence import Database, ProductionRepository
from tradeagent.research import ExperimentRegistry

DASHBOARD = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>TradeAgent Paper Console</title>
  <style>
    :root { color-scheme: dark; font-family: Inter, system-ui, sans-serif; }
    body { max-width: 1100px; margin: 0 auto; padding: 2rem; background: #09111f; color: #dce7f7; }
    header { display: flex; align-items: center; justify-content: space-between; gap: 1rem; }
    h1 { margin-bottom: .25rem; }
    .badge { background: #123d2b; color: #71e2a7; padding: .4rem .7rem; border-radius: 999px; }
    .warning { background: #392b11; border: 1px solid #765f24; padding: 1rem; border-radius: .6rem; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(210px, 1fr)); gap: 1rem; margin: 1rem 0; }
    .card { background: #101c2f; border: 1px solid #233653; border-radius: .7rem; padding: 1rem; }
    .value { font-size: 1.8rem; font-weight: 700; margin-top: .4rem; }
    table { width: 100%; border-collapse: collapse; font-size: .9rem; }
    th, td { padding: .65rem; border-bottom: 1px solid #233653; text-align: left; }
    code { color: #9bc4ff; }
  </style>
</head>
<body>
  <header><div><h1>TradeAgent</h1><div>Local paper-trading console</div></div><span class="badge">PAPER ONLY</span></header>
  <p class="warning">No live broker is connected. Qualification means a research gate passed, not that profit is guaranteed.</p>
  <section class="grid">
    <div class="card"><div>NAV</div><div id="nav" class="value">-</div></div>
    <div class="card"><div>Gross exposure</div><div id="exposure" class="value">-</div></div>
    <div class="card"><div>Kill switch</div><div id="kill-switch" class="value">-</div></div>
    <div class="card"><div>Audit events</div><div id="events" class="value">-</div></div>
    <div class="card"><div>Experiments</div><div id="experiments" class="value">-</div></div>
    <div class="card"><div>Qualified trials</div><div id="qualified" class="value">-</div></div>
    <div class="card"><div>Hosted bars</div><div id="hosted-bars" class="value">-</div></div>
    <div class="card"><div>Hosted quotes</div><div id="hosted-quotes" class="value">-</div></div>
    <div class="card"><div>Shadow NAV</div><div id="shadow-nav" class="value">-</div></div>
    <div class="card"><div>Recent news</div><div id="news-count" class="value">-</div></div>
  </section>
  <section class="card"><h2>Recent experiments</h2>
    <table><thead><tr><th>ID</th><th>Strategy</th><th>Seed</th><th>Qualified</th><th>Git SHA</th></tr></thead>
    <tbody id="experiment-rows"></tbody></table>
  </section>
  <script>
    function escapeHtml(value) {
      const element = document.createElement('span');
      element.textContent = String(value);
      return element.innerHTML;
    }
    async function refresh() {
      const [status, experiments, runtime, news] = await Promise.all([
        fetch('/api/status').then(r => r.json()),
        fetch('/api/experiments?limit=10').then(r => r.json()),
        fetch('/api/runtime').then(r => r.json()),
        fetch('/api/news?limit=20').then(r => r.json())
      ]);
      document.querySelector('#events').textContent = status.event_count;
      document.querySelector('#nav').textContent =
        status.account ? `$${Number(status.account.equity).toLocaleString()}` : 'No run';
      document.querySelector('#exposure').textContent =
        status.account ? `${(Number(status.account.gross_exposure_ratio) * 100).toFixed(2)}%` : '-';
      document.querySelector('#kill-switch').textContent = status.kill_switch;
      document.querySelector('#experiments').textContent = experiments.total;
      document.querySelector('#qualified').textContent = experiments.qualified_total;
      document.querySelector('#hosted-bars').textContent = runtime.market_bars ?? '-';
      document.querySelector('#hosted-quotes').textContent = runtime.market_quotes ?? '-';
      document.querySelector('#shadow-nav').textContent =
        runtime.shadow_nav ? `$${Number(runtime.shadow_nav).toLocaleString()}` : '-';
      document.querySelector('#news-count').textContent = news.items.length;
      document.querySelector('#experiment-rows').innerHTML = experiments.items.map(item =>
        `<tr><td>${escapeHtml(item.experiment_id)}</td><td>${escapeHtml(item.strategy_id)}</td>` +
        `<td>${escapeHtml(item.random_seed)}</td><td>${item.qualified ? 'yes' : 'no'}</td>` +
        `<td><code>${escapeHtml(item.git_sha.slice(0, 8))}</code></td></tr>`
      ).join('');
    }
    refresh();
    setInterval(refresh, 10000);
  </script>
</body>
</html>
"""


def _latest_account(ledger: SQLiteLedger) -> AccountSnapshot | None:
    checkpoint = ledger.latest_event("broker_checkpoint")
    if checkpoint is None:
        return None
    state = PaperBrokerState.model_validate(checkpoint["payload"])
    broker = PaperBroker.from_state(BrokerConfig(), state)
    return broker.account(datetime.fromisoformat(str(checkpoint["occurred_at"])))


def create_app(
    *,
    ledger_path: Path = Path("data/tradeagent.db"),
    experiments_path: Path = Path("data/experiments.db"),
    production_database_url: str | None = None,
) -> FastAPI:
    app = FastAPI(
        title="TradeAgent Paper Console",
        version="0.7.0",
        description="Read-only local observability for fake-money trading.",
    )

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    def dashboard() -> str:
        return DASHBOARD

    @app.get("/health")
    def health() -> dict[str, object]:
        return {
            "status": "ok",
            "mode": "paper",
            "live_trading_available": False,
        }

    @app.get("/api/status")
    def status() -> dict[str, object]:
        with SQLiteLedger(ledger_path) as ledger:
            account = _latest_account(ledger)
            return {
                "mode": "paper",
                "trading_enabled": False,
                "live_trading_available": False,
                "kill_switch": ledger.get_control("kill_switch", default="inactive"),
                "event_count": ledger.event_count(),
                "event_counts": ledger.event_counts(),
                "account": (
                    {
                        **account.model_dump(mode="json"),
                        "gross_exposure_ratio": str(account.gross_exposure / account.equity),
                    }
                    if account is not None
                    else None
                ),
            }

    @app.get("/api/events")
    def events(limit: int = Query(default=50, ge=1, le=500)) -> dict[str, object]:
        with SQLiteLedger(ledger_path) as ledger:
            return {"items": list(ledger.events(limit=limit))}

    @app.get("/api/experiments")
    def experiments(limit: int = Query(default=20, ge=1, le=100)) -> dict[str, object]:
        with ExperimentRegistry(experiments_path) as registry:
            return {
                "total": registry.count(),
                "qualified_total": registry.qualified_count(),
                "items": registry.recent(limit=limit),
            }

    @app.get("/api/runtime")
    def runtime() -> dict[str, object]:
        if production_database_url is None:
            return {
                "connected": False,
                "market_bars": None,
                "market_quotes": None,
                "shadow_nav": None,
                "worker_heartbeat": None,
                "notifier_heartbeat": None,
                "news_heartbeat": None,
            }
        with Database(production_database_url) as database:
            repository = ProductionRepository(database)
            bars, quotes = repository.market_data_counts()
            outcome = repository.latest_event_payload("shadow_outcome")
            worker = repository.latest_heartbeat("tradeagent-worker")
            notifier = repository.latest_heartbeat("tradeagent-notifier")
            news = repository.latest_heartbeat("tradeagent-news-worker")
            return {
                "connected": True,
                "market_bars": bars,
                "market_quotes": quotes,
                "shadow_nav": outcome.get("shadow_nav") if outcome else None,
                "worker_heartbeat": worker[1].isoformat() if worker else None,
                "notifier_heartbeat": notifier[1].isoformat() if notifier else None,
                "news_heartbeat": news[1].isoformat() if news else None,
            }

    @app.get("/api/news")
    def news(limit: int = Query(default=20, ge=1, le=100)) -> dict[str, object]:
        if production_database_url is None:
            return {"items": [], "feed_heartbeat": None}
        with Database(production_database_url) as database:
            repository = ProductionRepository(database)
            heartbeat = repository.latest_heartbeat("tradeagent-news-worker")
            items = NewsRepository(database).recent(
                since=datetime.now(UTC) - timedelta(hours=24),
                until=datetime.now(UTC),
            )[:limit]
            return {
                "feed_heartbeat": heartbeat[1].isoformat() if heartbeat else None,
                "items": [
                    {
                        "headline": item.headline,
                        "source": item.source,
                        "source_url": item.source_url,
                        "symbols": item.symbols,
                        "category": item.category.value,
                        "published_at": item.published_at.isoformat(),
                        "received_at": item.received_at.isoformat(),
                    }
                    for item in items
                ],
            }

    @app.get("/metrics", response_class=PlainTextResponse)
    def metrics() -> str:
        with SQLiteLedger(ledger_path) as ledger:
            counts = ledger.event_counts()
            lines = [
                "# HELP tradeagent_events_total Audit events recorded by type.",
                "# TYPE tradeagent_events_total counter",
                *[
                    f'tradeagent_events_total{{event_type="{event_type}"}} {count}'
                    for event_type, count in sorted(counts.items())
                ],
            ]
            account = _latest_account(ledger)
            if account is not None:
                lines.extend(
                    [
                        "# HELP tradeagent_nav Paper account net asset value.",
                        "# TYPE tradeagent_nav gauge",
                        f"tradeagent_nav {account.equity}",
                        "# HELP tradeagent_gross_exposure_dollars Paper gross exposure.",
                        "# TYPE tradeagent_gross_exposure_dollars gauge",
                        f"tradeagent_gross_exposure_dollars {account.gross_exposure}",
                    ]
                )
        with ExperimentRegistry(experiments_path) as registry:
            lines.extend(
                [
                    "# HELP tradeagent_experiments_total Research experiments recorded.",
                    "# TYPE tradeagent_experiments_total counter",
                    f"tradeagent_experiments_total {registry.count()}",
                ]
            )
        return "\n".join(lines) + "\n"

    return app
