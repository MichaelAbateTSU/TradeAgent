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
- Durable audited kill switch with explicit reconciliation confirmation before reset
- Idempotent paper fills with configurable slippage and commission
- Bid-ask spread, bar-volume participation limits, and deterministic partial fills
- Cash, positions, realized/unrealized P&L, NAV, and drawdown accounting
- Durable checkpoints and progress markers that safely resume interrupted paper runs
- Append-only SQLite events linking intent, risk decision, order, fill, and state by trace ID
- CSV replay and synthetic backtests through a local CLI
- Rolling walk-forward folds, 1x/2x/3x cost stress, and buy-and-hold comparison
- One-bar ordinary paper delay plus one- and two-bar research delay stress
- Deterministic 95% bootstrap confidence bounds on benchmark excess return
- Dataset/configuration hashes and an append-only experiment registry
- Read-only local dashboard, JSON endpoints, health check, and Prometheus-style metrics
- Typed Alpaca paper account, position, and order client fixed to the paper endpoint
- Risk-gated paper OMS with idempotent recovery and fail-closed reconciliation
- Strict typing, linting, and a high-coverage test suite
- Aligned multi-symbol panels with dropped-row accounting and provenance hashes
- Synchronized portfolio execution and cross-sectional momentum qualification
- Validated low-turnover 5/15-minute intraday paper mandate
- NYSE holiday/early-close gates and fail-closed minute-bar aggregation
- PostgreSQL-compatible persistence, worker locks, heartbeats, and Alembic migrations
- Persisted OMS transition state machine for partial fills, cancels, and recovery
- Transactional exactly-once round-trip profit/loss email outbox
- Typed Alpaca IEX websocket ingestion with reconnect backoff
- Single-instance shadow worker with scheduled reconciliation and watchdogs
- Always-on notifier service and Azure Container Apps deployment template
- $10–$25 fractional intraday backtesting with correct 5-minute annualization
- Opening-range breakout and session-VWAP mean-reversion qualification
- Normalized hosted bars/quotes with event, receive, and process timestamps
- Point-in-time spread, volume, volatility, VWAP, momentum, and regime features
- Regime-filtered intraday momentum with DSR/PBO and sealed-holdout gates
- Hosted synchronized shadow decisions, hypothetical costs/P&L, and shadow NAV
- PostgreSQL-backed runtime dashboard and immutable signed strategy promotions

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

Close-derived signals are delayed by one bar by default; use
`--execution-delay-bars` on an offline backtest only when testing a different explicit
assumption.

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

Verify the fake-money brokerage account without submitting an order:

```powershell
.\.venv\Scripts\tradeagent.exe alpaca-paper-status
.\.venv\Scripts\tradeagent.exe alpaca-paper-reconcile
```

Monitor an existing BTC paper position and sell the full quantity after unrealized P&L
becomes positive:

```powershell
.\.venv\Scripts\tradeagent.exe alpaca-paper-take-profit `
  --symbol BTC/USD --minimum-profit 0 --poll-seconds 15 --confirm-paper
```

Reconciliation journals broker-authoritative account, position, and open-order state.
Missing or duplicate orders and blocked accounts activate the durable kill switch. The
OMS contains a submission path, but new exposure fails closed unless the strategy's
latest registered experiment is qualified; risk-reducing exits remain available. No CLI
or autonomous loop invokes submission yet.

Run the promotion-gated research suite:

```powershell
.\.venv\Scripts\tradeagent.exe evaluate --strategy sma --csv .\data\spy.csv
.\.venv\Scripts\tradeagent.exe evaluate --strategy volatility-trend --csv .\data\spy.csv
```

Download and evaluate the default diversified ETF universe:

```powershell
.\.venv\Scripts\tradeagent.exe download-universe `
  --symbols SPY,QQQ,IWM,TLT,GLD --start 2018-01-01 --end 2026-09-04 `
  --output-directory .\data\universe
.\.venv\Scripts\tradeagent.exe portfolio-evaluate `
  --symbols SPY,QQQ,IWM,TLT,GLD --universe-directory .\data\universe
```

Download and qualify real five-minute candidates:

```powershell
.\.venv\Scripts\tradeagent.exe download-universe `
  --symbols SPY,QQQ --start 2025-01-01 --end 2026-09-04 `
  --timeframe 5Min --output-directory .\data\intraday
.\.venv\Scripts\tradeagent.exe intraday-evaluate `
  --strategy opening-range --symbols SPY,QQQ `
  --universe-directory .\data\intraday
.\.venv\Scripts\tradeagent.exe intraday-evaluate `
  --strategy vwap --symbols SPY,QQQ `
  --universe-directory .\data\intraday
```

Seal development data before a fresh candidate:

```powershell
.\.venv\Scripts\tradeagent.exe seal-intraday-holdout `
  --symbols SPY,QQQ --universe-directory .\data\intraday `
  --manifest .\data\intraday-holdout.json
.\.venv\Scripts\tradeagent.exe intraday-evaluate `
  --strategy regime-momentum --symbols SPY,QQQ `
  --universe-directory .\data\intraday `
  --holdout-manifest .\data\intraday-holdout.json
```

Start the read-only console at <http://127.0.0.1:8000>:

```powershell
.\.venv\Scripts\tradeagent.exe serve
```

Emergency-stop new exposure and inspect the durable state:

```powershell
.\.venv\Scripts\tradeagent.exe kill-switch activate
.\.venv\Scripts\tradeagent.exe kill-switch status
```

Or run it in a container, published only on the host loopback interface:

```powershell
docker compose up --build
```

## Qualification status

**No candidate strategy is qualified.** On the deterministic synthetic validation set,
the SMA, volatility-targeted trend, and z-score mean-reversion candidates failed to beat
the equal-risk buy-and-hold benchmark consistently across rolling out-of-sample folds.
The same three candidates also failed on 1,536 adjusted Alpaca IEX SPY daily bars through
September 3, 2026. This blocks promotion exactly as designed; broader point-in-time data
and stronger strategies are required before continuous autonomous paper operation. A
five-ETF cross-sectional momentum portfolio also failed its equal-risk benchmark gate.
The initial real five-minute opening-range and VWAP candidates also failed and remain
unauthorized.

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
- [`docs/AUTONOMOUS_DAY_TRADING_PLAN.md`](docs/AUTONOMOUS_DAY_TRADING_PLAN.md)
- [`docs/DATA.md`](docs/DATA.md)
- [`docs/RESEARCH.md`](docs/RESEARCH.md)
- [`docs/RISK.md`](docs/RISK.md)
- [`docs/OPERATIONS.md`](docs/OPERATIONS.md)
- [`docs/ROADMAP.md`](docs/ROADMAP.md)
- [`infra/azure/README.md`](infra/azure/README.md)
- [`infra/render/README.md`](infra/render/README.md)

Initialize the production-compatible schema locally:

```powershell
.\.venv\Scripts\alembic.exe upgrade head
```

Run the PostgreSQL-backed shadow services with Docker:

```powershell
docker compose up -d postgres migrate api
docker compose --profile runtime up -d shadow-worker
docker compose --profile notifications up -d notifier
```
