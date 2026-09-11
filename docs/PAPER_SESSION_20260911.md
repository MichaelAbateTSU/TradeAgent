# September 11 autonomous paper session

**Status: armed at 20:59 Eastern on September 10.** The running Render worker
has consumed the durable schedule and reports `scheduled_preopen` in actual
experimental-paper mode. No additional chat approval is needed for the one
bounded September 11 operation; genuine morning readiness and risk gates still
must pass. No September 11 trade has occurred yet.

## Owner instruction

At **17:53:44 Eastern on September 10**, the owner explicitly requested that the
agent be ready to trade tomorrow, without shadow mode or another last-minute
setup blocker. This authorizes preparation of **one bounded September 11 paper
equipment round trip**, not live money, an indefinite daily trading mandate,
fabricated news confidence or suspension of genuine risk checks.

The objective is a self-starting **Render worker** operation. A Copilot wake-up
or an operator remembering to publish a command tomorrow must not be required.
The active session must use experimental-paper mode with a dedicated scheduled
policy, not a shadow host described as automatic trading.

## Planned finite operation

- Date: September 11, 2026, verified with the actual paper broker's calendar.
- Regular session: **09:30-16:00 America/New_York**.
- Morning readiness: at least **30 continuous minutes** of real regular-session
  worker/lease, durable market progress, configured-source and broker evidence.
- Intended entry interval: **10:05 inclusive to 10:30 exclusive Eastern**.
- One owner-requested AAPL paper equipment entry, **at most $25**; account-day
  total BUY-attempt cap **two**, including manual/diagnostic history.
- At most one position including pending reservations. Existing $50 daily,
  $50 weekly and $150 drawdown limits at $10,000 virtual capital stay intact.
- Unfilled remainder cancel target: 30 seconds. Actual owned-quantity exit
  target: 60 seconds, or the existing local 0.5% stop/1% target sooner.

The schedule must carry the **actual September 10 approval time** separately
from tomorrow's readiness/issuance time. No future manual confirmation may be
invented. It must bind the reviewed release, account, immutable cohort,
session date, original control versions and exact permitted action. It expires
without replay if the window cannot be met.

The schedule must not accidentally activate normal news-strategy entries.
This equipment test is explicitly requested by the owner; it is not an eligible
H1/H2 news signal and is excluded from strategy qualification.

## Protections that remain real

No account switch, unresolved order, unknown/unvalued history, stale quote,
excess spread, insufficient liquidity, active halt or macro blackout may be
treated as passing. A new emergency/manual-kill update or unreviewed risk change
revokes the pinned entry authority. Older cohorts, their MISSED/terminal records
and acknowledgements are not overwritten or transferred.

Protection remains local and worker-dependent, not a guaranteed broker-native
bracket or fill price. Cancellation acknowledgement is not confirmation.
Recovery must use immutable order ownership and original client IDs, including
after malformed schedule/command pointers or restart.

The historical accounting bridge must include every prior broker order and
fee once, without restoring consumed attempts or increasing virtual capital
from external funding. Its conservative fee allowances are not reported as
known actual fees.

## Verification contract

Tonight's reviewed deployment must pass real after-hours HTTP, database,
owner/lease, report and resource checks. That evidence is explicitly
**after-hours resource/deployment evidence**, not tomorrow's market acceptance.

Tomorrow's worker must collect and persist its own fresh regular-session
readiness before issuing the one delegated order request. It must not claim to
have measured cloud-wide RSS if that metric is not available to it. Nightly
resource evidence and morning market/risk evidence retain distinct scopes.
No Render control-plane credentials or separately billed preview/cron services
are added merely to obtain a misleading pass label.

The existing Render notifier must send idempotent readiness, blocked/missed and
actual order/exit outcomes. An unavailable genuine input produces a visible
reason, not silent abstention or forced execution. Provider acceptance is
different from inbox delivery.

## Initial actual checks

The **17:56:37 Eastern September 10** broker read verified the September 11
calendar, ACTIVE/unblocked flat account, no open/unresolved orders and no active
operator request. Complete history still contains the two earlier BTC cycles
and canceled diagnostics; no September 10 trade occurred. Conservative history
valuation is `-$0.7399174463133780`, with actual and estimated fees kept distinct.

Preparation, deployment, durable schedule and final verification evidence are
still required before claiming the agent is ready. Tomorrow's fill cannot be
promised in advance.

## Initial release review

Candidate `71f248306e00d2d4aed2357b1bd42a80d9615545` passed 1,224 tests
(two skipped), but independent review reproduced two timing defects and
**blocked deployment and arming**:

- Synchronous source collection could delay local protection of an already
  filled operator position. In the reproduction, a stop crossing during a
  60-second source poll was not acted on until that poll returned.
- Continuity was checked before readiness collection, but not against its
  completed observation time. A 120-second actual gap could incorrectly retain
  the original 30-minute readiness interval.

Both require targeted regression coverage, a corrected reviewed release and
actual deployment before tonight's authority can be published. The initial
passing suite is not proof that these timing scenarios were safe.

## Corrected deployment and accounting

Corrected release `21a3d4186824a85746e10b5fb36f4060a09f3515` passed the
follow-up independent review and 1,237 tests (two skipped, 87.49% coverage).
Existing-position supervision now avoids full source ingestion while durable
orders/inventory remain unsettled. Completed observation gaps over 90 seconds
discard the old readiness interval and physical proof chain. Broker/quote I/O
and the local protection mechanism still cannot guarantee a two-second exit.

The event and dashboard services were deployed explicitly to that release:

| Role | Deployment |
|---|---|
| Event | `dep-dahkfnad0e5s73fp38pg` |
| Dashboard | `dep-dahkfn942hec73992ckg` |

The event deployment became live before ownership transferred. Startup
attempts correctly refused the prior owner's unexpired lease; the old owner's
last renewal was **20:16:48 Eastern**. Render's restart backoff added delay.
At **20:23:12 Eastern**, the current worker's fresh heartbeat and matching lease
were observed as owner
`srv-dae4tr7qj5pc73a9e0k0-58dd55b49f-v4k4t`.
No lease was stolen or deleted.

Actual immutable host `paper-scheduled-20260911` is **experimental-paper**,
policy `scheduled-operator`, AAPL only, with config
`0ea47bb4a8239deacb550b496f2edfa849324bea269e1dccf805042b22eee8d1`.
Recomputation matched the stored manifest. Ordinary strategy entries remain
disabled; the initial scheduled status is `not_authorized` until publication.
The real paper-order stream was connected, authenticated and subscribed,
with zero observed disconnects, dropped updates or errors.

Recorder `68c2e8fe086af06c548e7064014c8f1d927ff952` and notifier
`37e7a72c792a0398324ac0c5bee5cd50cdbb5077` were not redeployed. Application
plans, singleton replicas, disabled automatic deployments, environment-value
fingerprints, PostgreSQL capacity/storage/private access and existing
notification recipients were unchanged.

Same-image read-only baseline preview was followed by guarded import job
`job-dahkj4942hec7399k40g` at **20:21:56 Eastern**. It imported complete valued
history and corrected missing external BUY counts **upward to two each on
September 4 and September 9**. Correction audits are
`839edcb0-1696-4822-abaf-2e585de5f36e` and
`830ed3a5-bc19-48a8-acd2-ec7fda953687`; pre-arm import audit is
`f646356b-db35-44bb-98ef-f13f5167b892`.
The second preview proposed no further additions. No budget was reset,
no September 11 entry was reserved, and no order or authority was created.
Global kill and all pause values/timestamps were preserved.

All 16 prior cohort row hashes matched the pre-rollout snapshot. The original
September 9 cohort still matches historical hash
`6633a0006d6b6e8a11d6cd3555f0c8a061bbe9c9a696e3566d3a9a1f809203fa`.
Schema 0013, nine enabled statement-level counter triggers, three exact totals,
valid audit lookup index and zero missing reporting metadata were observed.
The existing read-only psycopg 3.3.5 sequential/burst fixture reached the
reviewed eight-statement budget; this is not universal RSS proof.

The actual managed full-report request returned **429** while its slot was
held, with ordinary sections returning 200; after release, the full September
11 report returned **200**, 190,182 bytes, state `NOT_STARTED`. No report email
was sent by this exercise and the already-sent daily report was not resent.

## Exact new-host source-risk review

The new host latched its own R1 pause at
`2026-09-11T00:23:00.972903Z`. It is **not cleared** and no older
acknowledgement is transferred. The fresh projection contains seven
position-review decisions: three historical Pusheen records, two iPhone Duo
records and two health/fitness records. All seven remain rejected/abstaining,
unclassified, with no extracted facts, null hypotheses and unknown publication
times. No explicit correction/retraction or waiting candidate was observed.

Exact current content and raw-document hashes are retained in the identity
evidence. A separate read-only comparison verified that the Pusheen revisions
have identical content; the iPhone and health/fitness differences only reorder
or replace links in **More from Apple Newsroom**, after press contacts. These
are not new quantitative trading signals. Publication uncertainty and the
rejected decisions are preserved.

The intended equipment authority may bind this **exact reviewed pause
version**, not a whitelist for future revisions. A subsequent risk/control
update invalidates the pinned exception. Configured AAPL feed, news and SEC
coverage was healthy; this does not imply complete corporate-source coverage.

## Nighttime resource evidence, not market acceptance

The actual **20:23:29-20:53:32 Eastern** observation lasted **1,802.58 seconds**:
2,380 dashboard requests, zero HTTP errors, no owner change or restart during
the observation, and no new recorder gaps or drops. The historical gap count
of one and prior incomplete derived frames remain intact.

| Resource | Peak memory |
|---|---|
| Event worker | 184.35 MiB |
| Dashboard | 153.31 MiB |
| Notifier | 197.11 MiB |
| Recorder | 131.74 MiB |
| PostgreSQL | 440.88 MiB |

Each resource had 31 memory samples. All applications stayed below 400 MiB and
the approved 1 GB PostgreSQL stayed below 800 MiB. The complete two-page,
1,384-record database log window contained no backend termination, recovery,
out-of-memory or other searched failure indicator.

The generic observer **failed seven live-market checks**, preserved verbatim
in its raw evidence: quotes/trades/bars and committed exchange time did not
advance; the recorder was degraded/unhealthy and the market-feed health check
was not satisfied. The actual market was closed, durable persistence was
drained, and exchange freshness/derived frames were stale. A fresh owner read
confirmed the recorder still reported its subscribed, authenticated transport
without persistence errors. That transport state is **not fresh exchange
delivery proof**. These failures were not relabeled as live-market passes.

The separate resource/deployment attestation was completed at
`2026-09-11T00:58:28.358091Z`, hash
`261ef0aa5336198f53a4db6b5e31097cfbc724109ae3b98e21805a49f08c4ae7`.
It explicitly records `full_market_acceptance_passed: false`.
Tomorrow's worker must obtain its own real regular-session proof. It does not
claim to measure morning Render-wide or PostgreSQL RSS itself.

## Durable arming and actual notification

Same-image publication job `job-dahl4i942hec739c83kg` committed the immutable
schedule and host pointer with approval audit
`43d443cb-ff6f-4ccf-b74b-a59e478348fc` at
**20:59:00.508858 Eastern**. Actual owner approval remains
**September 10, 17:53:44.767 Eastern**, separate from publication and any
future morning issuance.

| Identity | Value |
|---|---|
| Scheduled session | `7b922299-817c-5655-98e8-261bf947a1cb` |
| Nonce | `a7123df5-5f2d-5908-8014-0bb5f9fd1524` |
| Authority SHA256 | `4bbe94c98a6dde03e658947d92fb577b3630549ed3cadd1e96a5296d9ea35e74` |
| Deterministic future request | `f88ec4f2-18ef-5b84-bd0b-39babe1e914a` |

The worker reported **`scheduled_preopen` at 20:59:34 Eastern**, with
`operator_exception: scheduled` and `market_readiness: false`.
Independent job `job-dahl50qfngtc73cvkur0` read back the same authority hash,
one approval audit, unchanged pinned controls and all old cohort hashes,
zero missing reporting metadata, and the ACTIVE/unblocked broker account
flat with no open or unresolved orders. History remains the same six orders.
There is no delegated request or entry reservation tonight.

The existing notifier sent the worker's idempotent `scheduled_ready` message
**once at 20:59:36.263635 Eastern**:

- Notification: `ce1304d1-8536-5729-b21f-c0d27eaee7e2`
- Provider acceptance: `bdd7bbf7-af97-4b65-ba93-11f9dab97850`
- Outbox status: `sent`, attempts: **1**

This is provider acceptance, not inbox delivery proof. It is a readiness
notification, not a filled-trade notification. No prior email was resent.

The **September 11, 09:00 Eastern same-session oversight wake** was updated
with the actual release, schedule, source-risk and accounting pins. It is
read-only oversight, not the execution scheduler or another approval step.
At 09:00 the market is still closed; incomplete morning readiness is expected.
The worker alone may delegate and execute during **10:05-10:30 Eastern** after
the required continuous proof. Missing that window produces an honest
terminal outcome, not a replay or a later unapproved entry.

The retained official calendar lists a September 11 **08:30 Eastern** timed
release and no September 11 date-precision macro-risk window. That known
release is outside the proposed readiness/entry buffer. The worker still
refreshes official context tomorrow; this is not a guarantee that no new
halt, release, source revision or other genuine blocker can appear.

## Evidence

- [Actual calendar, account and full history precheck](../research/results/paper-sep11-precheck.json)
- [Corrected release review](../research/results/paper-sep11-release-review.json)
- [Complete pre-rollout state](../research/results/paper-sep11-prerollout-chunked.json)
- [Actual host identity and source risks](../research/results/paper-sep11-host-identity.json)
- [Exact source revision comparison](../research/results/paper-sep11-source-review.json)
- [Committed history import](../research/results/paper-sep11-baseline-import.json)
- [Managed full-report exercise](../research/results/paper-sep11-report.json)
- [Nighttime resource attestation](../research/results/paper-sep11-night-attestation.json)
- [Lossless raw nighttime observation](../research/results/paper-sep11-night-soak.json.gz)
- [Published authority](../research/results/paper-sep11-authority.json)
- [Independent final worker and email proof](../research/results/paper-sep11-final-proof.json)
