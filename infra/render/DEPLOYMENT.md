# Active Render deployment

## v20 event workflow (September 6, 2026)

The existing `tradeagent-news-worker` now runs
`tradeagent run --mode shadow --cohort-id v20-forward-shadow-001`.
It retains licensed-news storage/heartbeat and adds SEC discovery, immutable event
extraction, official risk context, decisions and sampled market evidence.
The original shadow-stream recorder and notifier are unchanged.

Worker and dashboard deployment commit: `79076cdf3366949734845ae68d4a1dcfb72975e2`.
PostgreSQL migration `0006_event_experiments` completed successfully.
The deployed `/api/event-product` confirms mode `shadow`, cohort config hash
`7e8c61066a29e9f66ab1ef9df3e477a986652e74af0c95a1d5d07fcc448f84dc`,
and state `market_closed`. No experimental certificate or order was created.
No new paid resource was added. See [v20 operations](../../docs/V20_OPERATIONS.md).
Automatic deploys are disabled on the event worker and dashboard so later commits
cannot silently change a frozen cohort's code identity.

## Original deployment record

Deployed September 4, 2026 in the `InSight AI` Render workspace.

| Resource | Region | Plan | Status |
| --- | --- | --- | --- |
| `tradeagent-postgres` | Oregon | 0.1 CPU / 256 MB | Available |
| `tradeagent-dashboard` | Oregon | Starter | Live |
| `tradeagent-shadow-worker` | Oregon | Starter | Live |
| `tradeagent-notifier` | Oregon | Starter | Live |

Dashboard: <https://tradeagent-runtime-dashboard.onrender.com>

The PostgreSQL-connected dashboard `/health` endpoint reports `mode: paper` and exposes
runtime heartbeats, normalized data counts, and shadow NAV. The shadow worker completed its
Alembic migration, authenticated to the Alpaca IEX stream after the production handshake
fix, and has no errors in the current instance.

The deployed worker:

- remains running when the laptop is off;
- has one instance;
- holds a database worker lock;
- reconciles the Alpaca paper account;
- consumes and audits IEX bars and quotes;
- persists normalized bars and quotes with event/receive/process timestamps;
- cannot place orders.

The PostgreSQL instance rejects public inbound connections. Render injects its internal
connection string directly into the worker.

## Notification verification

The private Resend channel was verified with a configuration test. The notifier is
running as one locked instance with no startup errors. It uses the Render-injected
PostgreSQL connection and the notification UUID as the provider idempotency key.

The current sender uses Resend's single-recipient testing domain. If delivery ever needs
to reach a different recipient, verify a private domain first.
