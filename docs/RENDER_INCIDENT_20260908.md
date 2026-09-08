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

## First release acceptance caught additional defects

Reviewed commit `37c262b7419bed8c1ecaf2db561c4f3bc4bce04d` reached all four
application roles at 15:28 UTC. Recorder data began advancing and application
memory fell substantially, but this candidate was **not accepted**:

- The actual full report exceeded 120 seconds. Two read-only test cursors remained
  CPU-active for more than six minutes; they were specifically canceled at
  15:36:55 UTC. No release email had been enqueued.
- PostgreSQL retained about 570 MB of audit storage, including approximately
  399 MB of repeated official context. Repeated JSON-text extraction imposed
  database CPU costs not represented by the SQLite/Python-allocation stress result.
- The first 663-second concurrent soak captured 12 HTTP failures. Database memory
  sampled 252,280,830 bytes on the unchanged 256 MiB plan. At 15:49:00 a PostgreSQL
  backend was killed by signal 9; the database reinitialized and resumed at
  15:49:07. The notifier restarted after the interrupted connection. No data reset
  or paid upgrade was performed.

The follow-up repair uses single-pass PostgreSQL JSON read models, the missing
concurrent audit lookup index, and one bounded dashboard connection pool.
Readiness timestamps are evaluated after the read, preventing fresh concurrently
updated heartbeats from being falsely labeled future-dated. Acceptance now checks
database memory, current role/lease/code identity and actual recorded progress,
not only successful HTTP responses. These changes require a fresh pinned review,
deployment and sustained acceptance; the failed first run is retained separately.

Follow-up integrated validation: **786 tests passed, 86.98% coverage**, unchanged
85% requirement; Ruff check/format passed (179 files) and mypy passed (82 source
files). Pool concurrency, read-completion freshness, PostgreSQL one-pass SQL shape,
index-history preservation and real-progress acceptance guards are covered.

## Second candidate: additional live failures preserved

Reviewed `eb566718b89e4cd6f4dc9ead9266842d2933b056` was deployed to all four
applications. Migration `0010_event_audit_lookup` was applied through the notifier
first and its validity was confirmed on PostgreSQL. This release is also **not
accepted**:

- A bounded, read-only historical metadata query still exceeded its 45-second
  statement limit. The native JSON-value semantic test passed. Three measured
  official-context originals were approximately 107 KB compressed but 440 KB
  decoded each; single-pass extraction still reparsed all historical bodies.
- The actual notifier report was built but its **27.5 MB JSON INSERT** coincided
  with PostgreSQL backend PID 441371 being killed by signal 9 and database recovery
  around 16:26 UTC. No release email was enqueued. A subsequent strictly read-only
  reproduction completed in 14.68 seconds, peaking at 265,232,384 bytes RSS:
  **25.8 MB was repeated `news_decisions.original_decision` bodies**.
- The 16:37:38–16:48:43 UTC observation lasted 664.7 seconds and issued 476
  requests. One request failed after 20.8 seconds; several others waited 17–20
  seconds. Application peaks were 137.4/193.1/109.1/153.4 MiB
  (dashboard/event/notifier/recorder), but PostgreSQL reached 243.89 MiB.
  Recorder committed-data age briefly reached 12.3 seconds. The run failed.
- `/api/runtime` took 3.15 seconds versus 0.16 seconds for `/ready` when measured
  separately. Repeating full-table counters under concurrent polling contributed
  database work and bounded-pool contention.

The next correction implements compact original-decision references, a versioned
immutable reporting sidecar with bounded historical backfill, and timestamped
singleflight counters. Fresh controls and committed-data health are not cached
as totals. Reports reject missing projections and reject an oversized serialized
snapshot before PostgreSQL receives the INSERT. Original bodies, losses, decisions,
and unknown states remain retained. Recorder batches add sidecars only for newly
inserted immutable audit identities; ambiguous retries never replace original facts.

Current integrated validation: **813 tests passed, 87.11% coverage**, with the
unchanged 85% requirement; Ruff check/format passed for 183 files, mypy passed for
83 source files, and dashboard JavaScript syntax passed. A fixture duplicating a
4 MiB source body across context and decisions produces a report below 256 KiB
without altering the originals. Transactional sidecar writes and bounded/resumable
backfill, missing-read-model errors, immutable retry identities, and uncached
control changes during cached counting are covered. This candidate still requires
exact review, deployment, backfill, and repeated actual acceptance before rearm.

Original r3 MISSED evidence was re-read unchanged:
14:00:34.987478 UTC, with the original spread and global-kill blocking reasons.

## Evidence files

- [Pre-repair Render events](../research/results/render-incident-20260908-before.json)
- [Pre-repair memory samples](../research/results/render-incident-20260908-before-memory.json)
- [Actual database/broker/MISSED baseline](../research/results/render-incident-20260908-baseline.json)
- [Prior frozen r3 record](../research/results/v20-tuesday-20260908-r3-deployment.json)
- [Mandatory repeatable post-deploy acceptance](../infra/render/POSTDEPLOY_ACCEPTANCE.md)
- [Failed first 663-second soak](../research/results/render-incident-20260908-first-soak-summary.json)
- [Complete compressed first-soak observations](../research/results/render-incident-20260908-first-soak.json.gz)
- [Actual PostgreSQL audit sizes](../research/results/render-incident-20260908-distribution.json)
- [Database interruption and memory](../research/results/render-incident-20260908-database-interruption.json)
- [Failed second sustained observation](../research/results/render-incident-20260908-r2-soak-summary.json)
- [Complete compressed second-soak observations](../research/results/render-incident-20260908-r2-soak.json.gz)
- [Measured report expansion](../research/results/render-incident-20260908-report-size.json)
- [Second database interruption](../research/results/render-incident-20260908-r2-database-interruption.json)
- [Decoded storage sizes and original MISSED preservation](../research/results/render-incident-20260908-storage-samples.json)

The historical r3 record and user-owned Tuesday-readiness edits are not rewritten.
Incident recovery acceptance is recorded separately after actual sustained tests.
