# Active Render deployment

Deployed September 4, 2026 in the `InSight AI` Render workspace.

| Resource | Region | Plan | Status |
| --- | --- | --- | --- |
| `tradeagent-postgres` | Oregon | 0.1 CPU / 256 MB | Available |
| `tradeagent-dashboard` | Oregon | Starter | Live |
| `tradeagent-shadow-worker` | Oregon | Starter | Live |
| `tradeagent-notifier` | Oregon | Starter | Not created; email secrets missing |

Dashboard: <https://tradeagent-dashboard-wnu1.onrender.com>

The dashboard `/health` endpoint reports `mode: paper`. The shadow worker completed its
Alembic migration, authenticated to the Alpaca IEX stream after the production handshake
fix, and has no errors in the current instance.

The deployed worker:

- remains running when the laptop is off;
- has one instance;
- holds a database worker lock;
- reconciles the Alpaca paper account;
- consumes and audits IEX bars and quotes;
- cannot place orders.

The PostgreSQL instance rejects public inbound connections. Render injects its internal
connection string directly into the worker.

## Pending notifier activation

Do not create `tradeagent-notifier` until these are configured:

```text
EMAIL_API_KEY
EMAIL_SENDER
EMAIL_RECIPIENT
```

`EMAIL_SENDER` must be verified by Resend. After configuration, create the notifier from
the root `render.yaml` or with the documented Render CLI command and verify its heartbeat
before relying on trade-result email.

