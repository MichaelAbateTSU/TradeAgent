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

## Third candidate: report recovery verified; concurrency still rejected

`b9e6d146a3ac85914ad6aaa7b50b3c9e41545636` was independently reviewed and deployed
to all four applications. Schema `0011` and its audit index were verified in
PostgreSQL. The metadata job stopped safely at its deadline after 56,512 committed
projections; a checkpointed continuation completed the remainder, with zero
missing projections at 17:52:18 UTC. Originals and trading controls were not rewritten.

- The deployed notifier **persisted a 3,708,920-byte report** in 16.93 seconds.
  Its job peaked at 202,924,032 bytes RSS. One explicitly labeled release-test email
  was sent in one attempt, provider ID `17b63f74-0750-400a-a28e-581823f6f7cb`.
  Resend's existing key is sending-only: delivery lookup returned
  `401 restricted_api_key`. Provider acceptance is verified, not recipient inbox
  delivery. No key permissions or recipients were changed.
- The previously failing original-cohort report returned HTTP 200 in 42.53 seconds
  under concurrent load, preserving all 44 decisions, original MISSED evidence,
  zero actual submissions, and unknown stale broker confirmation rather than
  relabeling it as fresh.
- The 662.46-second, 700-request stress run still failed: concurrent notifier and
  API full-report generation produced one HTTP 500 and brief recorder lag up to
  14.48 seconds. PostgreSQL's 235.00 MiB peak coincided with an extra full-history
  validation scan over 1.6 million quotes. There were no dropped packets in those
  recorder observations. This result is retained, not accepted.

The fourth correction coordinates full-report admission across processes using a
dedicated PostgreSQL advisory-lock session; it does not hold a normal data-pool
slot or mutate trading leases/controls. Busy API generation returns explicit 429;
scheduled reporting can defer and retry safely. Verification now counts actual
stored rows in a fixed, indexed recent window, excluding old and future data,
instead of repeatedly scanning all retained market history. Application and
database acceptance bounds are unchanged. Current validation: **831 tests passed,
87.16% coverage**; Ruff passes for 184 files and mypy for 83 source files. The
corrective release still requires pinned review, deployment and sustained tests.

Pinned review of `eb112dd` found an EOD integration defect before deployment:
broker completion was recorded before a busy report attempt, so later ticks could
skip that report permanently. The correction preserves the broker completion fact
but records successful report completion independently. Busy reporting defers
without triggering an execution-worker fault; a later tick or restart retries
until persistence succeeds. Other reporting failures still propagate and retain
retry eligibility. Changed exposure summaries produce new evidence instead of
reusing an old completion result. Regression tests cover busy and storage failure,
restart retry, no duplicate successful snapshot, changed exposure and unchanged
kill/pause controls. **833 tests passed, 87.17% coverage** after this correction.

### Fourth deployed release: no HTTP failures, acceptance still blocked

The corrected EOD retry was independently reviewed clean at
`3bbe3eb589ba08ee7f57e30b63c45318f6f8ef8f`; all four applications deployed that
pin around 18:56–18:58 UTC. Entries stayed paused after the news cutoff.

- Actual cross-process PostgreSQL report admission held the slot for 45 seconds.
  The overlapping API request returned 429 in 0.441 seconds while health, ready,
  and status remained HTTP 200. After release, the archived report returned HTTP
  200 in 23.21 seconds, preserving the original morning MISSED evidence.
- The notifier persisted a **3,984,810-byte report** in 15.22 seconds. Its job RSS
  was 205,918,208 bytes. The release email was sent in one attempt at
  `19:08:24.871302 UTC`, provider ID `b75744f6-c1b3-4e94-a6ed-dea5d2a170c1`.
  The sending-only key still cannot confirm inbox delivery; no resend or expanded
  permission was used.
- Indexed physical-data snapshots from 19:05:54 to 19:08:13 UTC, with a fixed
  19:04:54 cutoff, increased quotes 11,991→39,652, trades 88→380, and bars 4→14.
  Missing reporting projections were zero. The actual paper account was ACTIVE,
  unblocked, flat, and had zero open orders at 19:08:19 UTC.
- The **662.24-second soak had zero HTTP errors**, stable recorder ownership,
  healthy recorder observations, and no dropped packets. Nevertheless it failed:
  one PostgreSQL memory sample was **249.51 MiB**, above the unchanged 230 MiB
  acceptance bound, and feed-monitor snapshots were intermittently stale.
  Subsequent PostgreSQL samples were 184.80–225.82 MiB; this does not establish
  the cause of the first peak. Hot requests still reached 23.07 seconds.

The feed monitor was aging an earlier heartbeat's exchange watermark while a
newer durable recorder batch already existed, and sampled its clock before its
database read. Its repair must retain the ten-second market freshness bound,
live-owner checks, and original timestamp meanings. Recurring all-history market
counts also require removal from the dashboard request path. Neither corrective
work nor this failed soak is final production acceptance.

### Fifth correction: validation and remaining live gate

The candidate replaces repeated PostgreSQL market-history counts with three
transactionally maintained exact totals. Migration `0012_market_data_totals`
initializes under brief write-excluding locks compatible with ordinary reads and installs nine
statement-level transition-table triggers. Inserts count only genuinely inserted
rows; deletes, truncation, conflicts, and rollback retain exact accounting. Missing
or invalid totals fail explicitly with HTTP 503, never a fabricated zero or fallback
history scan. Existing and new Python writers are both covered by the database.

At **19:52:20 UTC**, the exact candidate migration, table model, and standalone
fixture passed on real PostgreSQL in a uniquely isolated schema. All seven test
stages passed, including 1,000-row insertion, conflict/update semantics, rollback,
deletion, truncation, missing/negative-counter rejection, and all nine actual
statement triggers. The outer transaction was rolled back and schema removal
verified. This fixture did **not** migrate or modify the production market tables.
Source hashes and the job command are retained in the r5 counter-fixture evidence.

The final local suite reached **869 passed, one optional local PostgreSQL test
skipped, 87.22% coverage**; the real PostgreSQL proof above is separate from that
skip. Ruff checks 188 files; mypy checks 84 source files. Final scoped ownership,
including 87 recorder-focused checks, is released. Exact-pin review, migration,
deployment, and post-deployment acceptance are still required before calling this
correction accepted.

The recorder fix uses a bounded, single-connection heartbeat/lease/batch read and
evaluates time only after that read. Future batches carry the immutable recorder
owner; old or foreign batches cannot rescue freshness. Positive health requires
the same live healthy owner, no reported loss/fault, genuine nonzero market counts,
ordered exchange/receipt/processing timestamps, and the unchanged ten-second
exchange-age bound. Database visibility proves durability, but processing start
is never relabeled as COMMIT time. Original sampled heartbeat/commit timestamps
remain visible alongside the explicit durable proof. Both observed double-sampling
failures have regression tests.

The regular session has closed; a new eleven-minute regular-session flow test
cannot now be completed for September 8. A same-session continuation is scheduled
for **18:10 Eastern** to inspect the normal daily notifier run, with explicit
instructions to schedule the remaining read-only market-open verification for
**September 9 at 09:35 Eastern**. This is not permission to rearm entries, replay
the MISSED window, or relax freshness or memory checks.

Pinned review rejected `395a15c` before migration or deployment: choosing the latest
processed batch can hide a still-fresh durable quote when a later batch contains
only a minute bar timestamped at that minute's start. The correction must preserve
valid committed exchange progress across the bounded recent same-owner batches,
not confuse per-batch event time with a cumulative watermark. The reviewer
reproduced this against actual persistence and monitor code in RAM SQLite.

The correction now streams the complete bounded recent same-owner interval with
one-row buffering, rejects invalid/nonmarket/future summaries, and selects the
maximum valid exchange progress using aware datetimes rather than timestamp-string
ordering. It does not truncate the interval with an arbitrary row limit. Liveness
and the unchanged ten-second age gate are still evaluated after the reads; original
timestamps remain unchanged. The corrected full tree passed **903 tests, one
optional local PostgreSQL skip, 87.24% coverage** with identical source hashes
before and after the run. Ruff188/mypy84 and whitespace checks pass. A new exact-pin
review is required; the rejected pin is not being reconsidered unchanged.

Additional pre-deployment PostgreSQL logs show SIG9 backend termination and
recovery at **19:45:11 and 19:57:18 UTC**, while all applications still ran `3bbe3eb`.
The victim statements were COMMIT and a multirow quote INSERT. Those last
statements alone do not identify the allocation source. These failures are
preserved and remain part of the unresolved database acceptance gate.

#### Defensive prepared-statement retention bound

Read-only inspection identified another plausible memory contributor, not a proven
SIG9 cause: variable-size recorder INSERTs produce distinct prepared-statement
keys, and psycopg normally retains 100 per pooled connection across COMMIT/idle
periods. A 500-quote shape has 6,000 parameters and approximately 104 KB of wire SQL,
plus server parse/plan structures. Bound values are not 100 retained payload batches.

The candidate adds only a PostgreSQL/psycopg connection hook, preserving SQL,
transactions, pool settings, SQLite behavior, and preparation threshold 5.
Actual testing caught a difference from the initial assumption: Render runs
psycopg **3.3.5**, and configuration 8 retained nine statements even after a second
diagnostic query. That failed test and both diagnostics are preserved.

Configuration **7** reserves one entry for the observed behavior while keeping the
original verification budget at **eight**. At **21:25:37 UTC**, the exact final
candidate passed both the isolated counter rollback fixture and the transaction-
enforced READ ONLY cache fixture on real PostgreSQL. Sixteen distinct small SELECT
shapes, seven executions each, retained 1→8 then eight statements, with correct
parameter results and unchanged threshold 5. This is a measured sequential-query
workload, not a universal byte-memory guarantee or proof of the SIG9 allocation
source. Real recorder throughput, RSS, and PostgreSQL headroom remain acceptance
requirements.

Final local integration: **908 passed, two explicitly optional local PostgreSQL
tests skipped, 87.25% coverage**; both PostgreSQL paths were separately exercised
on the real service. Ruff190/mypy84 and whitespace checks pass; source hashes stayed
identical throughout the full suite. The minute-bar correction at `70c78f6` received
an explicit clean delta review; the final cache supplement still needs its own
exact-pin review before deployment.

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
- [Third stress run and failure analysis](../research/results/render-incident-20260908-r3-soak-summary.json)
- [Actual report persistence and email enqueue](../research/results/render-incident-20260908-r3-report-email.json)
- [Preserved original report truth](../research/results/render-incident-20260908-r3-archive-truth.json)

The historical r3 record and user-owned Tuesday-readiness edits are not rewritten.
Incident recovery acceptance is recorded separately after actual sustained tests.
