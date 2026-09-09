# Mandatory post-deployment acceptance

A Render `live` deployment and HTTP `/health` are **not** acceptance. September 8
showed dashboard OOMs between healthy probes, a stopped recorder with a misleading
feed heartbeat, and a notifier failure that appeared only while generating a report.

## Explicit capacity profiles

The default `pg-256mb-v1` retains the original PostgreSQL **230 MiB** ceiling.
Historical evidence without a profile keeps that same ceiling; never reclassify
an old failed run against a larger instance.

On September 9 at **12:23:39 Eastern**, the owner explicitly approved upgrading
only PostgreSQL from `0.1c-256mb` ($6/month) to `0.5c-1g` ($19/month), an additional
$13/month excluding unchanged storage/workspace charges. For that actual approved
capacity, `--database-profile pg-1gb-v1` requires **at least 1,800 seconds** and
PostgreSQL memory **below 800 MiB** (at least 224 MiB allocation headroom).
The exact plan, availability and empty public allowlist are checked both before
and after the observation. This is a new capacity experiment, not a relaxation
of the failed 256 MiB experiment.

Application memory remains below 400 MiB. Every original HTTP, owner/lease,
committed exchange-progress, physical-data, loss/gap, schema, report, broker and
trading-risk gate remains in force. Record the planned database-resize outage
and any resulting worker reconnections separately; begin a new acceptance only
after they settle. Never delete old loss records, reset account/day budgets or
clear an expired cohort's terminal. A larger database does not authorize entries.

## Before changing a release

1. Preserve the account/session order budget, immutable cohorts, existing incident
   evidence and operator pauses. Never reset a missed equipment window, place an
   arbitrary test order, open a second IEX socket, or relax entry gates.
2. Run the targeted regressions, then the unchanged full validation:

   ```powershell
   .venv\Scripts\python.exe -m pytest --cov=tradeagent --cov-report=term:skip-covered
   .venv\Scripts\python.exe -m ruff check --no-cache src tests migrations infra\render
   .venv\Scripts\python.exe -m ruff format --check --no-cache src tests migrations infra\render
   .venv\Scripts\python.exe -m mypy src\tradeagent
   ```

3. Review the **exact commit**, push only task-owned files, then deploy that SHA
   explicitly. Keep automatic deployments disabled. Changed event runtime code
   requires a new immutable cohort, not rewriting the prior manifest/certificate.
4. Apply required migrations before dependent processes. Do not change PostgreSQL
   public access, recreate its data, steal leases, or increase plans to hide leaks.

## After every deploy, including a corrective redeploy

1. Confirm the actual service ID, live deployment SHA, start command, plan and
   migration. Observe normal shutdown/lease expiry and the new lease owner.
   A starting process or old heartbeat is not a successful handoff.
   Serialize a new migration through one application deployment before launching
   the others. After `0011_reporting_metadata` and all new writers are live, run
   the one-time, versioned historical projection backfill as a same-service job:

   ```text
   python -u -m infra.render.backfill_reporting_metadata
   ```

   Require `METADATA_BACKFILL` with `remaining: 0`. This reads originals one at a
   time in at-most-32-ID pages, throttles for recorder backlog, and writes only
   the compact sidecar. It never rewrites original events, controls, or orders.
   Interrupted pages roll back; reruns safely skip completed projections.
   Historical PostgreSQL reports intentionally return unavailable until their
   metadata is complete, rather than silently omitting rows or reparsing gigabytes.
   Migration `0012_market_data_totals` must finish before deploying its readers.
   It initializes three exact totals while briefly excluding raw-table writers,
   then maintains them through nine statement-level triggers. Verify the three
   totals and enabled triggers; do not reintroduce full-history polling scans.
   Run the isolated, rollback-only counter fixture before migration and the
   standalone `tests\prepared_cache_pg_fixture.py` against the candidate/deployed
   `Database.engine`. Its small-query test enforces READ ONLY and does not send
   orders. Configuration 4 preserves the tested eight-statement server budget
   on psycopg3.3.5, including warmed-key burst promotion; a sequential-only
   test missed that configuration 7 can retain 14. Do not insert inspection SQL
   between burst warmup and promotion or assume a configured value equals
   retained count. Record the actual library version and counts. This fixture does not
   replace live throughput, memory, or dependency verification.
2. Run the combined probe as a Render job on the broker-equipped event service
   using the deployed environment:

   ```text
   python -u -m infra.render.acceptance_probe --report --samples 12 --interval 60
   ```

   It opens no stream and submits no orders. Retain the `ACCEPTANCE_REPORT` and
   `ACCEPTANCE_SNAPSHOT` JSON lines from the job's logs. Use `--resources JOB_ID`,
   not `--instance`, when retrieving those logs. A transient log-read failure
   is not permission to repeat the job.
   The least-privilege notifier intentionally has no Alpaca keys; do not copy
   broker credentials into it just to run the combined probe. Its own report
   and delivery helpers can be exercised without the broker snapshot, below.
   Raw-market verification uses the five configured recorder symbols and one
   fixed recent exchange-time cutoff across all samples, using the existing
   `(symbol,event_at)` indexes. These are exact window counts, not all-history
   totals. Full-history `GROUP BY` scans over millions of quotes are not a polling
   health check: the September 8 validation itself caused unnecessary database load.
   Validate the two decoded snapshot objects with `physical_progress_failures`
   from `infra.render.acceptance_probe`. It requires one unchanged recent cutoff,
   endpoint separation below 25 minutes, and increasing physical counts/exchange
   timestamps for **each** SPY/QQQ/IWM/TLT/GLD quote/trade/bar series. Aggregate
   counter growth is insufficient: September 9 exposed missing current QQQ
   trades despite healthy HTTP and growing total trade counts. Retain any
   missing-symbol result and its provider comparison; never substitute a wider
   historical count or fabricated bar. A generic load-observer pass plus a
   physical-probe failure is **not** full incident acceptance.
3. For a notifier release, explicitly exercise production report generation and
   the existing approved recipient once:

   ```text
   python -u -m infra.render.acceptance_probe --report --email-release EXACT_RELEASE_SHA
   ```

   The stable UUID deduplicates this clearly labeled release-test message. The
   running notifier, not a competing dispatcher, sends it. Verify `sent_at`,
   provider ID, attempts and Resend acceptance. Do not claim inbox delivery without
   actual delivery evidence. Do not change the daily 18:00 Eastern schedule.
   A sending-only Resend key can confirm send acceptance but cannot read delivery
   state (`401 restricted_api_key`). Record that limitation; do not resend the
   message or widen permissions merely to obtain an inbox claim.
   The combined command above runs on the event service. To exercise the notifier
   environment itself, call `report_test(database, release, email=False)` from
   `infra.render.acceptance_probe` in a notifier job; call
   `email_status(database, release)` there for provider verification. These helpers
   deliberately separate the report/provider checks from the broker-only snapshot.
4. After initial handoff, exercise concurrent dashboard sections continuously for
   at least eleven minutes (longer than the prior approximately five-minute crash
   cycle). For a coordinated release with all four roles at the same SHA:

   ```powershell
   .venv\Scripts\python.exe infra\render\observe_release.py --commit EXACT_RELEASE_SHA --seconds 660 --output research\results\release-EXACT_RELEASE_SHA-live.json
   ```

   This observes `/health`, `/ready`, PostgreSQL controls, runtime, news,
   experiments and the bounded event overview using two simultaneous page-equivalent
   clients. It records Render memory and service events, and fails on HTTP errors,
   restarts, changed commits, missing metrics, changed public database access or
   any application sample at/above 400 MiB on the unchanged 512 MiB plans, or
   PostgreSQL at/above 230 MiB on its unchanged 256 MiB plan. It also rejects stale
   or replaced role owners, missing/mismatched leases, old event code, recorder
   drops/new gaps, and non-advancing committed exchange timestamps or raw data counts.
   Inspect database logs for backend termination/recovery too; memory sampling can
   miss a peak. The dashboard uses one bounded two-connection pool, not one pool
   per request. Migration `0011_reporting_metadata`, the valid audit lookup
   index, and zero `reporting_metadata_missing` in same-service snapshots are
   required. Hosted totals are timestamped, singleflight snapshots cached for
   60 seconds; `/ready`, role progress, and trading controls are not that cache.
   Also load the full session report on demand; it is not a hot polling endpoint.
   Full generation is serialized across API/notifier processes by a dedicated
   PostgreSQL advisory-lock connection. Verify an overlapping API request returns
   prompt, explicit 429 while ordinary dashboard sections remain available, and
   verify the full report succeeds after the slot is released. A 429 is admission
   control, not a completed report; it must not be counted as report success.
   For a single-role revision, explicitly verify each role's own expected SHA rather
   than pretending the unchanged roles received that revision.
   Keep `--commit` as the unchanged base pin and supply `--role-commit recorder=FULL_SHA`
   and/or `--role-commit dashboard=FULL_SHA` for deliberately scoped deployments.
   All roles, including the unchanged event heartbeat, remain strictly checked.
5. Inspect **all** recorded evidence, including any failure. Require:
   - Stable current worker owners and advancing heartbeats throughout the window.
   - Actual increasing committed IEX quotes, trades and bars, with fresh exchange
     and receipt timestamps. A socket subscription or heartbeat alone is insufficient.
   - Visible backpressure/stale/incomplete-frame gaps; never fabricate missing bars.
   - Fresh event market/source processing, bounded cache/report memory, and no worker
     errors. Legitimate missing-source/quote/spread gates remain distinct from uptime.
   - Database `SELECT 1`, correct schema, and unchanged empty public IP allowlist.
   - Actual authenticated paper-account status/positions/orders; fixture-only tests
     for broker rejection, timeout, unknown submission, partial fill and risk exits.
6. Only after review, actual flat/unreserved account and healthy roles may the
   operator-confirmed preflight authorize the new exact cohort. Do not reactivate
   the prior incident cohort or replay its MISSED morning equipment identity.
   September 8 news must still satisfy the existing day, evidence, quote, source,
   budget and 60-minute holding rules before the 15:50 flatten deadline; no late
   entry after the effective 14:50 Eastern cutoff.
7. A failure blocks acceptance. Fix it, pin/review/test the corrective commit,
   redeploy affected roles and repeat this entire post-deployment observation.
   Save measured results and limitations, not simply “healthy.”
