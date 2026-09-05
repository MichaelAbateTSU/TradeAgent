# Active Render deployment

Deployed September 4, 2026 in the `InSight AI` Render workspace.

| Resource | Region | Plan | Status |
| --- | --- | --- | --- |
| `tradeagent-postgres` | Oregon | 0.1 CPU / 256 MB | Available |
| `tradeagent-dashboard` | Oregon | Starter | Live |
| `tradeagent-shadow-worker` | Oregon | Starter | Live |
| `tradeagent-notifier` | Oregon | Starter | Live |

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

## Notification verification

The private Resend channel was verified with a configuration test. The notifier is
running as one locked instance with no startup errors. It uses the Render-injected
PostgreSQL connection and the notification UUID as the provider idempotency key.

The current sender uses Resend's single-recipient testing domain. If delivery ever needs
to reach a different recipient, verify a private domain first.
