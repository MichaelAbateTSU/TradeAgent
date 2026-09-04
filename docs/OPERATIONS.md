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

Inspect events:

```powershell
tradeagent status --database data\tradeagent.db --limit 20
```

Start the read-only local console:

```powershell
tradeagent serve --database data\tradeagent.db --experiments data\experiments.db
```

Endpoints:

| Path | Purpose |
| --- | --- |
| `/` | Paper-only operator dashboard |
| `/health` | Process health and mode |
| `/api/status` | Event counts and execution availability |
| `/api/events` | Recent audit events |
| `/api/experiments` | Recent qualification trials |
| `/metrics` | Prometheus text-format counters |

The default server binds to `127.0.0.1`. The container binds internally to `0.0.0.0`,
but Compose publishes it only to host loopback.

## Event sequence

For an executed order, the ledger records `trade_intent`, `risk_decision`,
`order_submitted`, `order_filled`, `broker_checkpoint`, and `engine_progress` under one
trace ID. Bars without orders still record progress so restart never silently replays
completed data.

## Incident response

1. Stop the process and preserve the SQLite database.
2. Do not delete events or manually manufacture a checkpoint.
3. Inspect the last progress, risk, order, fill, and checkpoint events.
4. Validate source data ordering and timestamps.
5. Restart only with the same configuration and dataset, or use a separate database.
6. Confirm `up_to_date` or a strictly newer `started_at` before trusting resumed output.

For a future external broker, this becomes **freeze, broker-authoritative reconcile,
repair, validate, and canary resume**. Local checkpoint recovery is not a substitute for
broker reconciliation.

