# Test-only PostgreSQL ingestion comparison

`tests/shadow_copy_pg_fixture.py` is an **optional diagnostic**, not a production
writer. It compares the exact `645714b` parameterized writer with a private
COPY/staging/ordered-merge adapter. Do not deploy the adapter.

## Execution policy

The September 9 original and corrected live observations both failed. The
database is already losing recorder headroom during ordinary operation. **Do not
add this synthetic write workload while the stock market is open.** Local
fixture checks do not authorize a production-database execution.

The same-session 15:45 Eastern close follow-up must first complete its separate
read-only account/order/recovery watch through 16:05. Only afterward may the
operator run one reviewed comparison if all of these are freshly established:

- Alpaca's actual clock is closed; the pinned paper account is flat, with no
  open orders or unresolved owned intents. Existing entry pauses and the expired
  cohort's terminal remain intact.
- Actual recorder image is `645714bf4c21aa8a9cbd73849f94119dc0b563a6`, with its
  matching current owner/lease. Its installed writer's normalized SHA256 is
  `96679b8a6d8db325b9742de785790543139a409a3e5badc4e7ced767bfd326c0`.
- The fixture's exact committed contents have received independent review.
  Verify the payload hash before execution; do not substitute unreviewed HEAD.
- The single shared database has spare capacity: recent observed memory below
  200 MiB, CPU below 0.04 cores, no recorder queue/in-flight backlog or recent
  persistence error, and no other full report, migration or diagnostic job.
  These are conservative **benchmark admission** conditions, not changes to the
  original live-acceptance or trading thresholds. Missing evidence means defer.

If these conditions are not met, record **DEFERRED/INCONCLUSIVE** and stop.
Do not stop the recorder, reset counters, terminate a database backend, add
compute, open another IEX socket, relax a risk gate or repeatedly start jobs to
obtain a faster result.

## Invocation

Use **one existing recorder-service Render job**, not another recorder process.
Jobs inherit the deployed image, not repository HEAD. The new fixture is not in
the deployed `645714b` checkout: transport the reviewed fixture as a compressed,
hash-checked inline payload, leaving its definitions separate from production
code. Run from the image's repository root so the existing migration module is
available. Do not deploy a test-only commit just to make the file accessible.

The explicitly loaded module exposes:

```python
with Database(AppConfig().database_url.get_secret_value(), pool_size=1) as db:
    result = fixture.compare_shadow_copy(db.engine, guard=parent_live_guard)
```

The default is **one pair of 25-row quote pages: 50 raw attempts**, with two
seconds between actual raw pages. It does not run the extended fault sweep.
`parent_live_guard` is mandatory and must perform finite-timeout, read-only
checks of current owner/lease, backlog, loss counters and controls. It must
reject an open market and return the built-in `True` only when the admission
conditions still hold. Never replace it with a constant-true callback or silently
use stale/missing observations. A false result, callback exception or timeout
aborts further work. Keep callback HTTP timeouts at two seconds and bound
database connection establishment too.

The caller must assert actual image/service identity, record the fixture hash,
and print the returned result as an unambiguous benchmark JSON marker. Never
print the database URL or other secrets. Record the job ID immediately.
If job creation or log retrieval is ambiguous, inspect/retry **reads**, not the
job submission.

## Guarantees and limits

The fixture requires the real psycopg engine with the existing prepared-cache
limit four. It creates one random private schema and connection-local staging
tables, with schema translation and a search path excluding `public`.
Original raw/audit/metadata/counter definitions are used only in that private
schema. One outer write transaction always rolls back; individual writer calls
use savepoints, and actual COMMIT is rejected. Successful completion also checks
that schema/staging objects are gone in an explicitly read-only transaction.

Test pages are 25-500 rows; the production 500-row page cap is unchanged.
Options are bounded to one-three pairs, one-five seconds between pages,
a 15-90-second work deadline, and at most 2,000 raw attempts. The default raw
budget is 750, with only 50 needed for the default smoke. Every attempted raw
page, including failures/retries, consumes budget. Do not escalate these
parameters during the scheduled initial smoke.

Statement/lock limits are **two seconds/500 milliseconds**. Idle transactions
are limited to ten seconds; PostgreSQL 17+ also has a server transaction
deadline. These server safety limits can disconnect the benchmark session if
triggered; that is an inconclusive failure, not evidence of acceptable runtime
behavior. No database backend is to be manually terminated.

The COPY variant uses an ordinal and ordered conflict handling rather than
unsafe pre-deduplication. Basic packet fidelity, exact IDs, sidecars and counters
are checked in the default smoke. The optional extended fault sweep requires
separate review and 25-50-row test pages; it is not authorized by the initial
smoke. It exercises actual overflow and metadata foreign-key failures against
private data. Aborts, unexpected SQL errors and failed cleanup are explicitly
returned as `inconclusive`, with sanitized error information and cleanup status.
Do not treat the Render job's generic success status as a passing comparison.

**Data isolation is not CPU or memory isolation.** Stop on unexpected resource
pressure; an interrupted/incomplete run is not a passing result. After the job,
verify its terminal state, cleanup, database availability, unchanged original
controls/owners and absence of new recorder loss or backend recovery.

Timings cover small private indexes and SAVEPOINT release/rollback, **not**
production-sized index I/O, actual COMMIT/fsync, cross-connection visibility,
global PostgreSQL RSS or sustained live-market capacity. Backend context bytes
are not total memory. The default smoke records only one observation per
variant. Its status is always `inconclusive`,
even when all smoke checks pass; more-pair completion is still explicitly
`comparison_complete_unaccepted`. This is not statistical proof. COPY may be
slower because staging and deletion add work. Report guard/throttle time
separately from active execution time, and retain both.

Even a favorable result requires a separate reviewed production design for
committing/pooled temporary-table lifecycle, complete existing validation, exact
deployment and another genuine 30-minute regular-session acceptance. It never
reopens the missed September 9 equipment/news window or qualifies a strategy.
