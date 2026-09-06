# Paper operations

## Production-compatible database

Set `TRADEAGENT_DATABASE_URL` to SQLite locally or PostgreSQL in an always-on deployment,
then apply migrations:

```powershell
tradeagent --help
.\.venv\Scripts\alembic.exe upgrade head
```

The v2 schema includes events, controls, orders, fills, position cycles, experiments,
heartbeats, notification outbox, an exactly-one-worker lock, and normalized bars, quotes,
and trades with feed provenance and exchange/receipt timestamps.

## Always-on local shadow stack

Start PostgreSQL, apply migrations, and run the dashboard:

```powershell
docker compose up -d postgres migrate api
```

Add live paper-data shadow ingestion:

```powershell
docker compose --profile runtime up -d shadow-worker
```

Add email delivery after configuring the `EMAIL_*` variables:

```powershell
docker compose --profile notifications up -d notifier
```

The shadow worker consumes Alpaca IEX bars, quotes, and trades, applies NYSE calendar and
freshness gates, reconciles the paper broker on startup and every 60 seconds, and records
heartbeats. New theoretical signals also record their expected cost/return floors and
one-, three-, and six-frame decay. It cannot place orders. Both worker and notifier have
one-replica locks and fail closed.

For laptop-independent operation, build and deploy
[`infra/azure/main.bicep`](../infra/azure/main.bicep) using the adjacent Azure guide.

## Round-trip email outbox

Configure a Resend sender and recipient:

```dotenv
EMAIL_PROVIDER=resend
EMAIL_API_KEY=...
EMAIL_SENDER=TradeAgent <paper@example.com>
EMAIL_RECIPIENT=owner@example.com
```

A reconciled cycle closure and its `round_trip_closed` outbox row are committed in one
transaction. The notifier claims one row with locking, sends with the notification UUID
as the provider idempotency key, and marks it sent. Failed delivery is retried from the
same row, preserving one logical email per round trip.

## Local workflow

Create or extend a durable fake-money run:

```powershell
tradeagent paper --synthetic-bars 500 --seed 7 --database data\tradeagent.db
```

Running the same command again reports `up_to_date`. Increasing `--synthetic-bars` or
appending newer CSV rows restores the last broker checkpoint, replays prior bars only to
reconstruct marks and indicator state, and processes unseen timestamps. Use a separate
database for an intentionally independent account.

The complete application configuration is fingerprinted in every progress event. A
resume with changed strategy, delay, risk, cost, or database configuration fails instead
of combining incompatible account histories. Start a separate database for a changed
policy.

Paper fills cross half the configured spread, add directional slippage, and cannot
consume more than the configured fraction of bar volume. Oversized fills are partial at
the broker layer; the risk gate rejects orders above its participation limit before
submission.

Inspect events:

```powershell
tradeagent status --database data\tradeagent.db --limit 20
```

Block all new exposure immediately:

```powershell
tradeagent kill-switch activate --database data\tradeagent.db
tradeagent kill-switch status --database data\tradeagent.db
```

Risk-reducing orders remain eligible. Reset only after checking the data source, latest
broker checkpoint, positions, cash, and event sequence:

```powershell
tradeagent kill-switch reset --confirm-reconciled --database data\tradeagent.db
```

Activation and reset are appended as `control_changed` events. The control value is
stored independently of process memory and enforced when a paper session resumes.

Start the read-only local console:

```powershell
tradeagent serve --database data\tradeagent.db --experiments data\experiments.db
```

Verify configured Alpaca fake-money brokerage access:

```powershell
tradeagent alpaca-paper-status
tradeagent alpaca-paper-reconcile --database data\tradeagent.db
```

This reads account and positions from `https://paper-api.alpaca.markets`. The endpoint is
a literal validated setting and cannot be changed to Alpaca's live URL. Reconciliation
records account, positions, open orders, and recovered order states in the audit ledger.
Missing local orders, duplicate broker client IDs, or a blocked broker account make the
result unhealthy and activate the durable kill switch.

The OMS submission method always:

1. reads the durable kill switch;
2. runs the independent risk engine;
3. verifies the strategy's latest experiment is qualified for new exposure;
4. queries Alpaca by client order ID to recover a lost acknowledgement;
5. submits only when no broker order exists;
6. journals the returned broker state.

Missing or failed qualification rejects new exposure as `STRATEGY_NOT_QUALIFIED`.
New-exposure submission remains unavailable from the CLI and autonomous engine.
Risk-reducing exits remain eligible through the explicitly confirmed monitor below.

## v0.9 research and execution evidence

The v0.9 research universe and date range are fixed in code before evaluation. Rebuild the
local, ignored market-data files and verify them against the tracked manifest:

```powershell
tradeagent build-v09-dataset
tradeagent squeeze-external --workers 4
tradeagent lower-turnover-research
tradeagent economic-ml-research
```

Historical quote access is probed explicitly with `feed=sip`. A denied entitlement stops
the historical evidence command; it never fabricates quotes or silently substitutes an
estimated feed. Historical bars, quote/trade archives, and experiment databases remain
local under `data\`; tracked manifests and reports contain their hashes and provenance.

Reproduce the frozen v0.8 squeeze cost calibration:

```powershell
tradeagent intraday-diagnostics --strategy volatility-squeeze `
  --output data\v09\execution-evidence\v080-squeeze-diagnostics.json
tradeagent collect-diagnostic-evidence `
  --diagnostics data\v09\execution-evidence\v080-squeeze-diagnostics.json `
  --timeframe 5Min `
  --records data\v09\execution-evidence\v080-squeeze-quotes-trades.jsonl `
  --manifest research\datasets\v0.9.0-v080-squeeze-execution-evidence.json
tradeagent calibrate-diagnostic-execution `
  --diagnostics data\v09\execution-evidence\v080-squeeze-diagnostics.json `
  --timeframe 5Min `
  --evidence data\v09\execution-evidence\v080-squeeze-quotes-trades.jsonl `
  --output research\results\v0.9.0-v080-squeeze-cost-calibration.json
```

The market-order simulation uses the first SIP quote at or after submission and caps
fills at displayed opposite-side size. The marketable-limit simulation fixes its limit at
the decision-time opposite quote and records a miss unless the order is still marketable
and fully covered at submission. A bar touching a price never creates a fill.

## Manual paper take-profit monitor

An explicitly confirmed operator command can monitor an existing paper position and
submit a full risk-reducing exit once Alpaca reports unrealized profit above a threshold:

```powershell
tradeagent alpaca-paper-take-profit `
  --symbol BTC/USD `
  --minimum-profit 0 `
  --poll-seconds 15 `
  --database data\alpaca-paper.db `
  --confirm-paper
```

The terminal must remain running. Each observation is printed without account balances.
The trigger and broker order states are audited, the fill is polled, and broker
reconciliation runs before the command exits. A positive observed unrealized value does
not guarantee positive realized proceeds because price can move before the market order
fills.

## Portfolio qualification

```powershell
tradeagent portfolio-evaluate `
  --symbols SPY,QQQ,IWM,TLT,GLD `
  --universe-directory data\universe `
  --lookback-frames 63 `
  --top-n 2 `
  --gross-target 0.04 `
  --database data\experiments.db
```

Every timestamp is marked across all assets before a decision. The candidate holds up to
the two strongest positive-momentum assets at 2% NAV each and is compared with a 4%
equal-weight passive portfolio. The same cost, delay, fold-stability, and bootstrap gates
used by single-asset research apply.

Endpoints:

| Path | Purpose |
| --- | --- |
| `/` | Paper-only operator dashboard |
| `/health` | Process health and mode |
| `/api/status` | Kill switch, latest account/NAV, exposure, and event counts |
| `/api/events` | Recent audit events |
| `/api/experiments` | Recent qualification trials |
| `/metrics` | Prometheus text-format event, experiment, NAV, and exposure metrics |

The default server binds to `127.0.0.1`. The container binds internally to `0.0.0.0`,
but Compose publishes it only to host loopback.

## Event sequence

For an executed order, the ledger records `trade_intent`, `risk_decision`,
`order_submitted`, `order_filled`, `broker_checkpoint`, and `engine_progress` under one
trace ID. Bars without orders still record progress so restart never silently replays
completed data.

## Incident response

1. Stop the process and preserve the SQLite database.
2. Activate the durable kill switch if it is not already active.
3. Do not delete events or manually manufacture a checkpoint.
4. Inspect the last progress, risk, order, fill, and checkpoint events.
5. Validate source data ordering and timestamps.
6. Restart only with the same configuration and dataset, or use a separate database.
7. Confirm `up_to_date` or a strictly newer `started_at` before trusting resumed output.

For a future external broker, this becomes **freeze, broker-authoritative reconcile,
repair, validate, and canary resume**. Local checkpoint recovery is not a substitute for
broker reconciliation.
