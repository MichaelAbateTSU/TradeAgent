# TradeAgent

A safety-first autonomous trading system that starts with **fake money only**. The first
release is an auditable paper-trading kernel: deterministic strategies propose target
allocations, a separate risk engine approves or rejects every order, and a paper broker
models slippage and commission before recording decisions and fills in an append-only
SQLite ledger.

> [!WARNING]
> TradeAgent is research software, not financial advice. It does not guarantee profit.
> Paper results do not reproduce live liquidity, queue position, market impact, outages,
> taxes, or regulatory constraints. There is intentionally no live-trading adapter.

## Current capabilities

- Paper-only configuration; `mode` rejects values other than `paper`
- UTC-aware OHLCV validation and deterministic synthetic market data
- Long-only SMA crossover baseline producing target weights, not raw orders
- Independent, fail-closed risk checks for stale data, shorts, leverage, exposure,
  concentration, loss, drawdown, order rate, and kill-switch state
- Idempotent paper fills with configurable slippage and commission
- Cash, positions, realized/unrealized P&L, NAV, and drawdown accounting
- Durable checkpoints and progress markers that safely resume interrupted paper runs
- Append-only SQLite events linking intent, risk decision, order, fill, and state by trace ID
- CSV replay and synthetic backtests through a local CLI
- Rolling walk-forward folds, 1x/2x/3x cost stress, and buy-and-hold comparison
- Dataset/configuration hashes and an append-only experiment registry
- Read-only local dashboard, JSON endpoints, health check, and Prometheus-style metrics
- Strict typing, linting, and a high-coverage test suite

## Quick start

TradeAgent requires Python 3.12 or newer.

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\tradeagent.exe backtest --symbol SPY --bars 500 --seed 7
```

Run a persistent fake-money simulation and inspect its event ledger:

```powershell
.\.venv\Scripts\tradeagent.exe paper --synthetic-bars 500
.\.venv\Scripts\tradeagent.exe status --limit 10
```

Replay canonical CSV bars:

```powershell
.\.venv\Scripts\tradeagent.exe paper --csv .\data\bars.csv --symbol SPY
```

CSV columns are `timestamp,symbol,open,high,low,close,volume`. Timestamps must include a
UTC offset. Historical inputs should be point-in-time correct and include delisted assets
and corporate-action handling where applicable; this repository does not supply market
data.

Download adjusted Alpaca historical data after setting `ALPACA_KEY_ID` and
`ALPACA_SECRET_KEY`:

```powershell
.\.venv\Scripts\tradeagent.exe download-alpaca `
  --symbol SPY --start 2020-01-01 --end 2026-01-01 `
  --timeframe 1Day --output .\data\spy.csv
```

Run the promotion-gated research suite:

```powershell
.\.venv\Scripts\tradeagent.exe evaluate --strategy sma --csv .\data\spy.csv
.\.venv\Scripts\tradeagent.exe evaluate --strategy volatility-trend --csv .\data\spy.csv
```

Start the read-only console at <http://127.0.0.1:8000>:

```powershell
.\.venv\Scripts\tradeagent.exe serve
```

Or run it in a container, published only on the host loopback interface:

```powershell
docker compose up --build
```

## Qualification status

**No candidate strategy is qualified.** On the deterministic synthetic validation set,
both current trend candidates failed to beat the equal-risk buy-and-hold benchmark
consistently across rolling out-of-sample folds. This blocks promotion exactly as
designed. Synthetic data validates the machinery, not market alpha; real,
point-in-time market data is required before strategy conclusions are meaningful.

## Development

```powershell
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe -m ruff format --check .
.\.venv\Scripts\python.exe -m mypy src
.\.venv\Scripts\python.exe -m pytest --cov=tradeagent
```

The design is grounded in the four research reports committed at the repository root.
See the implementation guides:

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)
- [`docs/DATA.md`](docs/DATA.md)
- [`docs/RESEARCH.md`](docs/RESEARCH.md)
- [`docs/RISK.md`](docs/RISK.md)
- [`docs/OPERATIONS.md`](docs/OPERATIONS.md)
- [`docs/ROADMAP.md`](docs/ROADMAP.md)
