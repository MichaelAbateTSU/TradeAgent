from __future__ import annotations

# ruff: noqa: E501
from pathlib import Path

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, PlainTextResponse

from tradeagent.ledger import SQLiteLedger
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
    <div class="card"><div>Audit events</div><div id="events" class="value">-</div></div>
    <div class="card"><div>Experiments</div><div id="experiments" class="value">-</div></div>
    <div class="card"><div>Qualified trials</div><div id="qualified" class="value">-</div></div>
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
      const [status, experiments] = await Promise.all([
        fetch('/api/status').then(r => r.json()),
        fetch('/api/experiments?limit=10').then(r => r.json())
      ]);
      document.querySelector('#events').textContent = status.event_count;
      document.querySelector('#experiments').textContent = experiments.total;
      document.querySelector('#qualified').textContent =
        experiments.items.filter(item => item.qualified).length;
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


def create_app(
    *,
    ledger_path: Path = Path("data/tradeagent.db"),
    experiments_path: Path = Path("data/experiments.db"),
) -> FastAPI:
    app = FastAPI(
        title="TradeAgent Paper Console",
        version="0.1.0",
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
            return {
                "mode": "paper",
                "trading_enabled": False,
                "live_trading_available": False,
                "event_count": ledger.event_count(),
                "event_counts": ledger.event_counts(),
            }

    @app.get("/api/events")
    def events(limit: int = Query(default=50, ge=1, le=500)) -> dict[str, object]:
        with SQLiteLedger(ledger_path) as ledger:
            return {"items": list(ledger.events(limit=limit))}

    @app.get("/api/experiments")
    def experiments(limit: int = Query(default=20, ge=1, le=100)) -> dict[str, object]:
        with ExperimentRegistry(experiments_path) as registry:
            return {"total": registry.count(), "items": registry.recent(limit=limit)}

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
