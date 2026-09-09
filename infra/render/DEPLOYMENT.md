# Active Render deployment

## September 8 incident

The earlier point-in-time readiness statements below are historical, not current
health guarantees. See [the incident record](../../docs/RENDER_INCIDENT_20260908.md)
and the [mandatory post-deployment acceptance procedure](POSTDEPLOY_ACCEPTANCE.md).
Every corrective deployment requires sustained dependency, data-progress, memory
and failure-event tests; a successful `/health` response is not sufficient.

### Subsequent one-demo preparation (September 8 evening)

The separately requested equipment-only demo is documented in
[its September 9 protocol](../../docs/PAPER_DEMO_20260909.md). Only the event
worker was deployed to reviewed `b354bf8f925d76ee33205735736ec4cf36ff54ce`;
recorder/dashboard remain `5e728c5b29fb9330df61d8f3bae95ca8a34ca53b` and notifier
remains `3bbe3eb589ba08ee7f57e30b63c45318f6f8ef8f`. Auto-deploy is still disabled.
The new `v20-manual-demo-20260909-r1` cohort permits at most one conditional
AAPL paper equipment entry and zero strategy entries. It has **no approval or
orders**, global kill remains active, and an actual
`R1_EVENT_REQUIRES_POSITION_REVIEW` pause blocks activation. Prior pauses and
the September 8 MISSED record are preserved. The original 09:35 September 9
read-only incident acceptance remains required before any separate demo action.
After-hours deployment checks are not proof of an executed trade.

## Project organization

All TradeAgent resources are grouped under **Trade agent**
(`prj-daf34jn40ujc739c852g`) in the existing `InSight AI` workspace.
The environment is **Production** (`evm-daf34jn40ujc739c8530`).
This is an organizational label, not permission for live or autonomous trading.

| Resource | Render ID |
| --- | --- |
| `tradeagent-runtime-dashboard` | `srv-dadoa3740ujc73cb09c0` |
| `tradeagent-shadow-worker` | `srv-dadn8son74is73apqcc0` |
| `tradeagent-news-worker` | `srv-dae4tr7qj5pc73a9e0k0` |
| `tradeagent-notifier` | `srv-dadnn6mq1p3s73ef7ef0` |
| `tradeagent-postgres` | `dpg-dadn7nht0dsc73f9jja0-a` |

Grouping completed September 6, 2026 Eastern without changing worker start
commands or requesting redeployments. PostgreSQL remains available with an empty
public IP allowlist; the dashboard remains healthy with live trading unavailable.
The unrelated `My project` / `InSightAI` deployment was left unchanged.

## Current IEX paper-practice release (September 7, 2026)

Worker, dashboard and notifier are deployed at
`fb4a7b438e0427ed48a72d89833a2380b2b4b21e`. The event worker now runs:

```text
tradeagent run --mode experimental-paper --purpose iex-practice --practice-start-date 2026-09-08 --cohort-id v20-iex-practice-20260908-r2
```

The worker's pre-deploy command applies `alembic upgrade head`, including
`0008_control_values`. The exact new cohort's paper preflight completed successfully in
job `job-daf4ck8n74is7389dbhg`, with permission saved at `2026-09-07T05:06:50.278756Z`.
The live heartbeat confirms the new code and configuration, `market_closed`, no blockers,
and a calibration scheduled for **September 8**. No orders have been submitted.

The earliest possible calibration entry is **09:35 Eastern**, retaining the existing
five-minute opening warm-up. The one-attempt window ends at 10:00; quotes, account,
liquidity, context and risk checks must all pass. Real-time IEX is used, never 15-minute-old
execution quotes. Practice activity cannot qualify a strategy or establish profitability.
See [practice operations](../../docs/V20_OPERATIONS.md#free-iex-paper-practice).

Full deployment IDs, the frozen config hash, feed permissions, limits, initial zero-order
state and validation are in the
[practice deployment record](../../research/results/v20-iex-practice-deployment.json).
Latest SIP remains denied; no market-data or inference subscription was purchased.

The original research cohort remains unchanged. The first practice setup failed to save
its certificate in the old 500-character control field and submitted no orders. It is
preserved under its original ID; the corrected code uses a new `-r2` cohort.

All four services now have automatic deploys disabled, including the retained
shadow-stream recorder. That recorder remains at `2a1db33e7cf4f596e8fce9aac47c4e1e8c854630`;
it was not restarted or given order permission. This avoids accidental recorder restarts
and global safety pauses from documentation pushes. The daily email schedule is still
**18:00 America/New_York**. The notifier completed its normal lease handoff.

## Initial v20 event workflow (September 6, 2026)

The existing `tradeagent-news-worker` was deployed with
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

The existing `tradeagent-notifier` also schedules daily status/next-step emails at
18:00 Eastern every calendar day using the existing Resend sender and recipient.
Its pre-deploy command applies `0007_daily_status_email`. This change does not
redeploy or alter the frozen trading/event cohort.

Daily notifier release: `1c061c7` (Render deploy `dep-daf2unqd0e5s73aiulig`).
The first digest, dated September 6, 2026 Eastern, was accepted by Resend at
`2026-09-07T03:34:09Z` after the old notifier lease expired. Provider message ID:
`c9cbdf72-924d-40a6-aed2-574cd7b686c4`. This confirms provider acceptance, not
mailbox delivery. The notifier is pinned with automatic deploys disabled;
future changes use explicit deployment.

### One-off example email

At the owner's request, job `job-daf3578u01pc738l89sg` reused the notifier's deployed
code and environment to build the current daily status and enqueue a separate
`[TEST]` message. The job succeeded; the running notifier accepted the new outbox
item without a second delivery daemon or a change to the scheduled digest.

- Subject: `[TEST] [TradeAgent PAPER] Daily agent status - 2026-09-06`
- Notification ID: `58a0f479-b52f-5b01-ba13-133ae01f1559`
- Resend acceptance: `2026-09-07T03:42:41Z`
- Provider message ID: `450be5aa-3d83-468d-be1a-9e88fc823576`

The job confirmed the schedule remains enabled at **18:00 America/New_York**.
It did not replace the daily notification ID or consume a future scheduled send.
Provider acceptance is recorded; inbox delivery was not independently confirmed.

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
