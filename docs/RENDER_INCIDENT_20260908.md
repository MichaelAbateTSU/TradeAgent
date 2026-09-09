# Render incident — September 8, 2026

## Current deployment and remaining gate

The final reviewed release `5e728c5b29fb9330df61d8f3bae95ca8a34ca53b` is deployed
to **recorder and dashboard**. Event/news and notifier deliberately remain at
`3bbe3eb589ba08ee7f57e30b63c45318f6f8ef8f`; their immutable execution cohort is not
rewritten. PostgreSQL migration `0012_market_data_totals` and all nine enabled
statement-level triggers are verified. Plans and the database's empty public
allowlist are unchanged.

**Full acceptance remains open.** The post-deployment after-hours observation
completed 840 requests over 662.66 seconds with zero HTTP failures, stable owners,
no new gaps/losses/restarts, and sampled memory below every unchanged bound.
The unmodified full gate still fails on absent market progress and truthful
closed-market/degraded recorder states; this is **not** regular-session acceptance.
A direct same-session
continuation is scheduled for **September 9 at 09:35 Eastern**, for a thirty-minute
read-only market-load test with unchanged freshness, ownership, loss and memory
gates. All entries remain paused; no MISSED reset or account reset occurred.

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

The deeper promotion path supersedes that sequential-only candidate: already-known
query keys can be promoted without rotation. An independent **non-perturbing**
PostgreSQL experiment at 21:51:58 UTC reproduced configuration 7 growing to
**14** retained statements; configuration 4 peaked at **eight**. An earlier
diagnostic queried between warmup/promotions and disturbed that cache state; it
is retained as an experiment, not acceptance. `66b11fc` was never deployed.

The stronger candidate therefore configures **4**, without changing threshold 5,
SQL, transactions, pools, permissions, or the eight-statement verification budget.
Its exact real-PostgreSQL fixtures passed at **21:54:29 UTC**: sequential traffic
peaked at five; four prepared plus four warmed known-key promotions peaked at
eight, then explicit new-key traffic reduced retained counts to four. No inspection
SQL is inserted between warmup and promotion. The isolated counter fixture again
passed all seven stages and rolled back. This still does not establish the SIG9
allocation source or replace actual throughput/RSS verification.

Final strengthened integration: **909 passed, two optional local PostgreSQL skips,
87.25% coverage**, identical input hashes throughout the full run, Ruff190/mypy84
and whitespace checks clean. Independent final review explicitly approved
`5e728c5` with no significant issues; earlier findings remain closed.

### Actual final rollout and immediate tests

- Recorder deployment `dep-dag8ecm7bikc7394cfgg` finished at 22:09:31 UTC.
  Its pre-deploy step applied migration0012 once. The actual deployed image then
  passed both PostgreSQL fixtures, including the unperturbed eight-statement
  burst gate. The O(1) totals read took **0.035 seconds**, returning 1,442 bars,
  2,930,890 quotes and 28,814 trades.
- The new recorder briefly exited during rolling overlap because the old owner
  still held its lease. Those startup failures are preserved. The old owner stopped
  and released naturally; no operator lease deletion/stealing or manual restart was issued.
  The replacement `srv-dadn8son74is73apqcc0-c67d8f6c5-l5w9b` then held the matching
  fresh lease and authenticated/subscribed successfully. The first fixture's
  ownership snapshot caught the old stopped heartbeat and no lease; this was not
  misrepresented as a completed handoff.
- Only after schema and actual recorder handoff verification was the dashboard
  deployed: `dep-dag8gd6k1f9s738f7hpg`, finished 22:13:10 UTC. All fourteen immediate
  hot requests returned HTTP200 in **0.095–0.411 seconds**.
- Cross-process report admission returned expected HTTP429 in **1.24 seconds**
  while health/ready/status remained HTTP200. After release, the original-cohort
  full report returned HTTP200 in **7.51 seconds**, preserving the exact MISSED
  status, timestamp, blockers, stable test/session identities, zero submissions
  and all 44 decisions.
- During sustained hot traffic, the notifier environment persisted another real
  report in **8.02 seconds**, without enqueueing or sending another email. Its
  report-email wrapper was 984,688 bytes and job RSS was 213,864,448 bytes; wrapper
  serialization is not mislabeled as the stored report's size.
- The actual broker at 22:12:50 UTC was ACTIVE/unblocked, flat, with zero open
  orders; the exchange clock reported closed. Global kill and operator pauses
  remained active. Missing reporting projections were zero.

### Actual normal 18:00 notification

The ordinary scheduler—not a manual test—persisted report
`171fe7d5-e6a9-5778-b4e8-b40fbffbb962` (4,438,083 bytes) and its metadata projection.
Notification `c13027db-13b7-5242-9f8f-f52489841f7a` was sent in **one attempt** at
22:00:20 UTC, provider ID `9277329c-97ee-4e86-a1c6-70db3509fc26`. There was no
manual enqueue/resend. Provider acceptance is verified; the sending-only key still
cannot prove inbox delivery.

Across the observed normal-report window, sampled notifier and PostgreSQL peaks
were **192.43 and 204.51 MiB**, respectively, with no matching PG termination,
interruption or out-of-memory log entries. This completes the planned 18:10 check;
its automation was advanced directly to the September9 market-open continuation.
It does not replace final deployment or regular-market acceptance.

### Completed after-hours sustained observation

From **22:15:20 to 22:26:23 UTC**, two page-equivalent clients issued **840 requests**
over **662.66 seconds**. There were **zero HTTP errors**, no deployment/owner
changes or service restarts during the observation, all role heartbeats were fresh,
and actual leases matched. Recorder drop/notice/persistence/decision fault
observations were zero; its initially recorded gap count of one stayed unchanged.

Median request time was **0.168 seconds**, p95 **0.889 seconds**, maximum
**9.551 seconds**. The maximum and all individual requests are retained rather
than reporting only the fastest initial probes.

| Resource | Sampled peak MiB |
|---|---:|
| Event/news | 272.94 |
| Recorder | 112.02 |
| Notifier | 192.94 |
| Dashboard | 205.25 |
| PostgreSQL | 192.40 |

All samples stayed below the existing 400 MiB application and 230 MiB PostgreSQL
bounds. A separate database log check from rollout start through the final check
found no SIG9/other termination, interruption, or out-of-memory messages; the
database remained available with its empty public allowlist.

The unchanged acceptance script exited nonzero because committed exchange time
and raw quotes/trades/bars did not advance, the recorder correctly reported
`degraded` without a first market batch, and the feed reported `market_closed`.
These failures are preserved verbatim with `accepted: false`. An authenticated
subscription is not substituted for market data or entry authority. The thirty-
minute September9 regular-session test must pass before full acceptance.

The final 22:28:24 UTC same-service snapshot confirmed migration0012, zero missing
projections, unchanged pauses/global kill, ACTIVE/unblocked/flat broker and zero
open orders. The recorder remained authenticated/subscribed with no reconnects
or drops. Actual report `52222c2b-53f4-5513-b1c3-403f7e06756c` was present, recorded
at 22:16:42 UTC during the load test. One Render log retrieval transiently failed
with Loki504/HTTP503; only the log read was retried, not the job or email.

## September 9 regular-session continuation

The requested 1,800-second **read-only** observation started at approximately
13:36:30 UTC (09:36:30 Eastern), using event `29cf232`, recorder/dashboard
`5e728c5`, and notifier `3bbe3eb`. Entry pauses remained active; neither the
overnight completed BTC operator test nor the canceled AAPL diagnostic is
substituted for this acceptance or for the morning equipment experiment.

The initial live recorder was already degraded: at 13:36:23 UTC it had received
245,803 events, committed 234,509, queued 10,600 with 500 in flight, and recorded
194 drops and four gaps. Durable commit lag was approximately 20.3 seconds, despite
receive lag of approximately 0.035 seconds. The 194 losses/four gaps predated that
sample and must not be described as newly occurring later in the observation.
The stale/degraded initial observations nevertheless fail full acceptance.

An independently measured 143.409-second interval received 470.326 events/second
and committed 474.168/second while carrying substantial backlog. A 500-row batch
had taken approximately 1.094 seconds. PostgreSQL was repeatedly near its
0.1-CPU allocation. These observations identify insufficient effective service
headroom during the opening burst, not a proven universal database-memory cause.
The original full run is retained rather than replaced with its later healthy
HTTP samples or lower arrival rate.

A narrow corrective writer patch replaces per-batch SQL expression trees with
parameterized SQLAlchemy inserts, explicitly paged at 500 rows. Raw records,
audit originals and metadata still commit atomically; conflict/RETURNING and
ambiguous-commit retry semantics, receipt times, queue limits, gaps and freshness
rules are unchanged. No schema or trading-policy change is included.

Local file-SQLite alternating 500-quote measurements improved median wall time
from 92.83 to 33.68 milliseconds. Separate phase measurements locate most savings
in application SQL construction/compilation (72.317 to 11.569 milliseconds),
not driver execution (5.840 to 5.268 milliseconds). Those local measurements
are **not** proof of deployed PostgreSQL throughput or memory. Paging, conflicts,
partial-page rollback, immutable retry and existing bounded-queue regressions
passed in the 124-test focused validation. Actual corrective deployment and a
new complete market-load acceptance are still required.

The real report-admission job `job-dagm34u1egvs73bkjlbg` returned HTTP429 in
0.404 seconds while health/ready/status returned HTTP200. After slot release,
the full new-cohort report returned HTTP200 in 9.947 seconds (3,367,678 bytes).
The job did not persist/email another report. Transient log-read failures were
retried as reads, not as duplicate jobs or email sends.

No approval window is extended to accommodate the repair. The September 9
news-paper policy remains conditional on passing the complete incident gate
before its original 10:30 Eastern deadline.

### Completed original morning gate and scoped corrective rollout

The unchanged original run completed **13:36:27.643701-14:06:31.296953 UTC**,
covering **1,803.653 seconds and 2,156 requests**, with zero HTTP errors. It
**failed**: the recorder accumulated **22,292 additional dropped events and
25 additional gaps**, reached its unchanged 20,000-item queue capacity, and
recorded a maximum commit lag of **94.386 seconds**. PostgreSQL peaked at
**234.703 MiB**, above the original 230 MiB gate. Application peaks were recorder
170.08, event 159.72, notifier 192.38 and dashboard 236.39 MiB. Nothing was
rearmed or ordered.

An exhaustive 2,189-record PostgreSQL log retrieval for that complete interval
found no backend termination/recovery/FATAL/PANIC records, but did contain
89 known `event_evidence_pkey` duplicate errors. These are not hidden behind a
claim that every SQL operation succeeded. Complete logs are retained in session
artifacts; the committed projection records counts and the raw-log hash.

Both physical endpoint snapshots found actual market rows. The second had
quotes, trades and 15 bars for every one of SPY/QQQ/IWM/TLT/GLD in the fixed
window, but the snapshot took approximately 56 seconds overall and concurrent
commits again lagged substantially. Increasing counts alone did not pass
freshness, loss, memory or capacity acceptance.

The exact recorder correction **`645714bf4c21aa8a9cbd73849f94119dc0b563a6`**
passed independent review and the mandatory validation (1,016 passed, two skipped,
87.32% coverage, Ruff/format/mypy). It was pushed and deployed only after the
original read-only observation finished. Deployment
`dep-dagmh2mk1f9s73diissg` finished at **14:10:48 UTC**. By **14:12:47 UTC**,
new recorder owner `srv-dadn8son74is73apqcc0-846d9b5d44-97mvv` held its matching
fresh lease and had real committed data; no lease was stolen. Its new
instance-local counters do not erase the prior owner's lost data.

The actual-image job `job-dagmj3qjnfac73e76jkg` confirmed Python 3.13.7,
psycopg 3.3.5, the exact Git writer-source hash, prepared-cache configuration four
with observed peak eight, and rollback-only isolated counter correctness.
The actual raw data stayed untouched by that counter fixture. A later test-only
commit strengthens all-conflict-page coverage but does not replace the deployed
`645714b` pin.

Event/news remains `29cf232`, dashboard `5e728c5`, notifier `3bbe3eb`, all
auto-deploys remain disabled, and all compute plans are unchanged. The
[included Pro protections](RENDER_PRO_OPERATIONS.md) were applied separately
after the first read-only interval. A **new complete 30-minute read-only run**
is in progress on these exact pins. Its physical probes use a fixed recent
window with three-minute endpoint separation to bound diagnostic query work;
the original long-window failure is retained. No threshold or trading window
is relaxed, and the required repeat cannot finish before today's 10:30
authorization deadline.

### Corrected run also failed: remaining PostgreSQL headroom

The complete repeat on recorder `645714b` ran **14:16:07.217560-14:46:10.120474
UTC**, covering **1,802.903 seconds and 2,184 requests**. All hot HTTP requests
returned 200; maximum request time was 5.132 seconds. Nevertheless acceptance
**failed again**: **1,842 new drops, six new gaps, a full 20,000-item queue,
56.092-second maximum commit lag, and PostgreSQL at 234.449 MiB**. The unchanged
230 MiB database limit was not raised.

Current-owner heartbeat deltas averaged **376.216 received versus 375.320
committed events/second** over the observed period, not spare sustainable
capacity. Supplemental 14:16-14:25 minute samples showed recorder CPU averaging
0.081 cores while PostgreSQL averaged 0.086 and reached its 0.1-core allocation.
The application optimization therefore has a real measured benefit but is not
sufficient proof of database throughput or memory headroom.

Both small-window physical samples contained increasing actual quotes, trades
and bars for all five symbols. Real report admission returned 429, ordinary
sections stayed 200, and the subsequent 3,678,757-byte report returned 200 in
15.319 seconds without an email. Neither substitutes for the failed loss,
freshness and memory requirements.

The exhaustive corrected-window database log read retained 2,279 records, with
no backend termination/recovery/FATAL/PANIC, and 87 known duplicate-evidence
errors. A fresh event-image broker read at **10:47:45 Eastern** remained
ACTIVE/unblocked and flat, with no open orders. Schema 0012 and zero missing
reporting metadata were verified.

Meanwhile the event worker independently recorded the expired equipment window
as **MISSED with zero attempts**, latched a durable news terminal, and retained
the original entry pauses. See the [actual scoped outcome](PAPER_NEWS_20260909.md#actual-september-9-expiry-no-authorized-entries).
No late preflight, replay or replacement cohort is permitted.

The live writer remains the reviewed `645714b`. The test-only, isolated,
rollback-only PostgreSQL ingestion fixture at `c3a8e39` has passed independent
review and local checks; **it has not been executed on PostgreSQL or deployed**.
Its default is only a guarded, throttled 50-row smoke, always explicitly
inconclusive rather than a performance acceptance. Execution is deferred until
after the 15:45-16:05 Eastern read-only close watch, and only with freshly verified
closed/flat broker state and spare database capacity. See the
[execution restrictions](../infra/render/SHADOW_COPY_BENCHMARK.md) and
[exact review/hash record](../research/results/render-incident-20260909-copy-review.json).
No production path will be adopted merely because a local prototype looks faster.

At **11:38:19 Eastern**, a further light, event-image readback (no market-history
scan) again verified the pinned broker account ACTIVE/unblocked, zero positions,
zero open orders and zero unresolved local intents. The original global/pause
timestamps, worker-owned news terminal and zero-attempt MISSED result persisted.
See [handoff evidence](../research/results/news-paper-20260909-handoff-readback.json).

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
- [Actual final per-role deployment pins](../research/results/render-incident-20260908-r5-deployed.json)
- [Final after-hours observation and remaining failures](../research/results/render-incident-20260908-r5-afterhours-soak-summary.json)
- [Complete compressed after-hours observations](../research/results/render-incident-20260908-r5-afterhours-soak.json.gz)
- [Actual deployed recorder/schema/cache tests](../research/results/render-incident-20260908-r5-recorder-postdeploy.json)
- [Actual deployed dashboard/pool/cache tests](../research/results/render-incident-20260908-r5-dashboard-postdeploy.json)
- [Actual normal 18:00 report and send](../research/results/render-incident-20260908-normal-daily-verification.json)
- [Final broker, pauses, stream, report and schema snapshot](../research/results/render-incident-20260908-r5-final-snapshot.json)
- [Post-rollout database recovery-log check](../research/results/render-incident-20260908-r5-pg-final-check.json)
- [September 9 actual report admission](../research/results/render-incident-20260909-report-admission.json)
- [September 9 local writer measurements](../research/results/render-incident-20260909-writer-local-measurements.json)
- [September 9 local writer phase measurements](../research/results/render-incident-20260909-writer-local-phases.json)
- [September 9 original full market-open observation](../research/results/render-incident-20260909-market-open-soak.json)
- [September 9 failed gate summary](../research/results/render-incident-20260909-market-open-summary.json)
- [September 9 original fixed-window physical snapshots](../research/results/render-incident-20260909-physical-snapshots.json)
- [September 9 complete PostgreSQL log check](../research/results/render-incident-20260909-pg-log-check.json)
- [September 9 exact corrective release and owner handoff](../research/results/render-incident-20260909-writer-deployed.json)
- [September 9 actual corrective-image PostgreSQL fixtures](../research/results/render-incident-20260909-writer-pg-fixtures.json)
- [September 9 complete corrected observation](../research/results/render-incident-20260909-corrected-soak.json)
- [September 9 corrected failure summary](../research/results/render-incident-20260909-corrected-summary.json)
- [September 9 corrected physical progression](../research/results/render-incident-20260909-corrected-physical.json)
- [September 9 corrected report admission](../research/results/render-incident-20260909-corrected-report-admission.json)
- [September 9 corrected database logs](../research/results/render-incident-20260909-corrected-pg-log-check.json)
- [September 9 post-run broker and metadata readback](../research/results/render-incident-20260909-corrected-final-probe.json)

The historical r3 record and user-owned Tuesday-readiness edits are not rewritten.
Incident recovery acceptance is recorded separately after actual sustained tests.
