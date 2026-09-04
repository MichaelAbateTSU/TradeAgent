# Paper operations

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
