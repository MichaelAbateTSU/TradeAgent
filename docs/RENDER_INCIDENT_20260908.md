# Render incident — September 8, 2026

## Observed failure and preserved evidence

The 09:35–10:00 Eastern equipment window was missed. Its durable status is
`MISSED`, recorded at **14:00:34.987478 UTC**, with zero submitted attempts.
The last evaluation at 13:59:57 UTC retained both `INVALID_PRICE_OR_SPREAD`
and `GLOBAL_KILL_SWITCH`: bid 316.23 / ask 316.85 was approximately 19.6 bp,
above the unchanged 10 bp ceiling. This is not an executed trade or a
successful morning test; the window is not backdated or replayed.

At 15:01:18 UTC, a same-service read-only probe verified PostgreSQL migration
`0009_candidate_states`, successful `SELECT 1`, an ACTIVE/unblocked/flat
paper account and zero open orders. The event/news worker was collecting
fresh data, while the legacy recorder was stopped. Not every resource was dead.

Confirmed causes:

- Dashboard Render events explicitly reported repeated **512 MiB OOM kills**.
  Hot full-report rebuilding and overlapping page requests materialized repeated
  heavy historical context/brief payloads.
- The notifier also recorded 512 MiB OOM kills while generating its daily report.
  Its apparent daytime recovery did not test the next 18:00 report.
- The recorder repeatedly terminated on quotes roughly 10 seconds old after
  synchronous persistence/audit work blocked ingestion. Shadow startup, stale-feed
  and stop paths also changed the shared operator kill switch. Its separate feed
  monitor could report “healthy” from a heartbeat even when ingestion had stopped.
- Operational CLI imports unnecessarily loaded offline machine-learning libraries.
  Changing source-query windows also accumulated unbounded response-cache entries.
- The reported generic Alpaca authentication error lacked sufficient provider
  detail to establish its original provider code. A connection-limit explanation
  is possible during overlap, not a proven cause of that particular traceback.

Entries were paused by job `job-dag1pl9594qs73e07fp0`, preserving
`v20-tuesday-20260908-r3:pause=OPERATOR_INCIDENT_REPAIR`; risk recovery remains active.
No account reset, live trade, forced paper test order, subscription purchase,
plan upgrade, database public-access change or holdout opening is authorized.

## Implemented repair and local validation

- The IEX socket now validates authentication and complete subscription acknowledgments,
  retains safe provider error codes, and separates receiving from bounded batch persistence.
  Exchange, receipt and processing times remain distinct. Overflow/restart/missing-frame
  gaps are recorded explicitly, never fabricated away.
- The read-only recorder has its own lease, heartbeat and feed monitor. It cannot
  start/stop the operator kill switch or run the trading reconciler. Autonomous-paper
  stale-data, authorization and ownership checks remain fail-closed.
- Only complete synchronized five-minute frames enter derived shadow analysis;
  incomplete or gapped frames reset contiguous-evidence assumptions.
- Hot dashboard polling uses a small singleflight-cached overview, independent section
  failures and non-overlapping refreshes. Current recorder/feed roles, actual PostgreSQL
  controls and durable progress are exposed; obsolete legacy heartbeats are not substituted.
- Full reports stream/project history before JSON decoding, select repeated heavy
  latest states in SQL and retain complete counts, losses, incidents and immutable
  references. The notifier persists a full report without duplicating its JSON in outbox.
- Offline ML imports are lazy; source response storage is bounded by bytes and entries,
  and oversized downloads stop before materializing a whole response.

Final integrated local validation: **779 tests passed, 86.96% coverage**, unchanged
85% threshold; Ruff check/format passed (177 files); mypy passed (82 source files).
Regression evidence includes an actual local WebSocket/SQLite pipeline, burst/blocked
writer/backpressure/commit-retry cases, 8,000 × 64 KiB historical report fixtures, 48
concurrent overview callers, and explicit release-email idempotency. Dashboard JavaScript
syntax passed Node checking. The design detector's existing Inter-font preference warning
was not restyled during incident recovery. These tests do not replace live PostgreSQL,
Render RSS, real IEX progress or delivery acceptance.

## Evidence files

- [Pre-repair Render events](../research/results/render-incident-20260908-before.json)
- [Pre-repair memory samples](../research/results/render-incident-20260908-before-memory.json)
- [Actual database/broker/MISSED baseline](../research/results/render-incident-20260908-baseline.json)
- [Prior frozen r3 record](../research/results/v20-tuesday-20260908-r3-deployment.json)
- [Mandatory repeatable post-deploy acceptance](../infra/render/POSTDEPLOY_ACCEPTANCE.md)

The historical r3 record and user-owned Tuesday-readiness edits are not rewritten.
Incident recovery acceptance is recorded separately after actual sustained tests.
