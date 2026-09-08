# Mandatory post-deployment acceptance

A Render `live` deployment and HTTP `/health` are **not** acceptance. September 8
showed dashboard OOMs between healthy probes, a stopped recorder with a misleading
feed heartbeat, and a notifier failure that appeared only while generating a report.

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
2. Run the same-service probe as a Render job using the deployed environment:

   ```text
   python -u -m infra.render.acceptance_probe --report --samples 12 --interval 60
   ```

   It opens no stream and submits no orders. Retain the `ACCEPTANCE_REPORT` and
   `ACCEPTANCE_SNAPSHOT` JSON lines from the job's logs. Use `--resources JOB_ID`,
   not `--instance`, when retrieving those logs. A transient log-read failure
   is not permission to repeat the job.
3. For a notifier release, explicitly exercise production report generation and
   the existing approved recipient once:

   ```text
   python -u -m infra.render.acceptance_probe --report --email-release EXACT_RELEASE_SHA
   ```

   The stable UUID deduplicates this clearly labeled release-test message. The
   running notifier, not a competing dispatcher, sends it. Verify `sent_at`,
   provider ID, attempts and Resend acceptance. Do not claim inbox delivery without
   actual delivery evidence. Do not change the daily 18:00 Eastern schedule.
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
   For a single-role revision, explicitly verify each role's own expected SHA rather
   than pretending the unchanged roles received that revision.
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
