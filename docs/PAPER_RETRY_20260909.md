# September 9 afternoon workflow retry

## New owner request and capacity approval

At 12:18 Eastern the owner requested another workflow run while the market was
open. At **12:23:39 Eastern** the owner explicitly approved the quoted PostgreSQL
upgrade from **0.1 CPU / 256 MB, $6/month**, to **0.5 CPU / 1 GB, $19/month**.
The incremental compute charge is **$13/month**, separate from unchanged storage,
application services and Pro workspace charges.

Only database `dpg-dadn7nht0dsc73f9jja0-a` was resized. Its identity, 15 GB disk,
private connectivity, empty public allowlist and Production environment were
preserved. No worker replica, other compute plan, credential or recipient was
changed. The database was temporarily unavailable during the resize; application
restarts briefly rejected still-current leases and later recovered naturally.
No lease was stolen and no old data, order budget or experiment terminal was reset.

The new capacity experiment uses the reviewed `pg-1gb-v1` acceptance profile:
at least 30 minutes, PostgreSQL below 800 MiB, applications below 400 MiB, actual
plan verified before and after, and every original loss/freshness/HTTP/owner/
physical-data/report/broker gate unchanged. Historical 256 MB failures retain
their 230 MiB threshold and failed outcomes.

## Separate account-day limit blocks another entry

The broker's complete September 9 Eastern-day order read returned these actual
BUY submissions:

| Order | Purpose | Outcome |
| --- | --- | --- |
| `dd9ba5c8-8319-4466-83c0-af3cc9911c73` | Overnight AAPL diagnostic | Accepted, then canceled; zero fills |
| `385947be-92ac-4215-98f4-54380075a145` | Overnight BTC/USD operator test | Filled; subsequently sold |

The configured owner policy permits **two total account-day BUY entries/attempts**.
Canceled-after-submission orders do not restore a slot, and the separately
implemented operator paths are not an authorized exemption from an account-wide
limit. Therefore **zero additional BUY attempts are available today under the
unchanged limit**. A fresh infrastructure run or another cohort cannot reset it.

The operator tests use controls/audit rather than normal `event_order_links`.
The current shared OMS/economic ledger therefore does not automatically include
all their history. This is a known accounting integration gap, **not permission
to treat their orders or losses as absent**. No afternoon entry is authorized
using that incomplete ledger. The morning cohort's MISSED record, terminal,
original global kill and operator pause remain intact.

At the **12:28:45 Eastern** broker read, positions and open orders were both zero.
The canceled AAPL diagnostic is still not a filled equipment test, and the BTC
operator test is still not a qualifying strategy result.

## Posted crypto fees now visible

The same read returned the genuine BUY/SELL FILL activities, a **$0.05 USD CFEE**,
and a **0.00000062 BTC fee quantity** at the original BUY price. The BTC fee explains
the difference between purchased and subsequently sellable quantity.

The fill-value difference was `-$0.07723217556`. After the posted USD fee, the
cash-net round-trip result was **`-$0.12723217556`**. Do not subtract the BTC fee's
dollar value again: its effect is already reflected in the reduced sell quantity.
This new fee evidence supplements, rather than overwrites, the earlier explicitly
provisional fee estimate.

## Notifications and completion

The retry outcome must be sent once through the existing Render notifier and
recipient, including a blocked outcome. No previously accepted diagnostic,
crypto lifecycle or daily summary message may be resent. Provider acceptance
must be recorded separately from inbox delivery.

The notifier now includes a lightweight observer for the fresh, current
experimental-paper cohort's same-day MISSED/terminal/completion controls. It
checks the frozen date/code/config identity and exact control versions, then
atomically inserts a stable-ID outbox entry. Pending and sent entries deduplicate
across polling, restarts and competing observations. A terminal/MISSED pair
shares one result identity rather than producing two alerts. It does not scan
old cohorts, change controls, query broker/market history, or generate a full
report. The email explicitly distinguishes cohort attempts from account-wide
usage and does not claim that broker flatness or fills were checked.

The independently reviewed notifier release
`37e7a72c792a0398324ac0c5bee5cd50cdbb5077` passed full validation (1,076 passed,
two skipped; 87.41% coverage) and was deployed only to the notifier as
`dep-dagoqf8n74is739dkj7g`. Its new owner
`srv-dadnn6mq1p3s73ef7ef0-867d694f49-fqbql` obtained the delivery lease naturally;
temporary lease-contention starts are preserved separately.

The **running notifier**, not an email-sending job, automatically enqueued and
sent the current cohort's MISSED result at **12:50:56 Eastern**, exactly once.
Notification `3c12a339-00a7-52e5-97aa-2d4aae49a5d9` has status `sent`, one attempt,
and provider ID **`8028c160-8139-4211-bc35-3c59e278d066`**.
The independent proof job only read the outbox. This establishes provider
acceptance, not inbox delivery. A separate owner-requested retry summary follows
the full upgraded-capacity observation; it is not a resend of this MISSED alert.

Capacity acceptance, trade authorization and email acceptance remain separate.
The 15:45 Eastern close watch still must observe actual flatness through close.

## Physical-data defect found during the rerun

The larger database kept raw ingestion fast, but the independent physical probe
found **no QQQ trade rows** in its recent window despite advancing QQQ quotes and
bars. A bounded, read-only Alpaca IEX historical request then returned 20 actual
QQQ trades with IDs 4118-4137 at approximately 16:49 UTC on September 9.
Every sampled ID matched a stored **September 8** trade instead, with a different
timestamp and price.

The existing unique key `(symbol, feed_source, provider_trade_id)` incorrectly
treats provider trade IDs as globally unique across sessions. `ON CONFLICT`
therefore silently classified these genuine new trades as duplicates. Zero queue
drops and successful HTTP requests do not establish complete persistence.

This evidence blocks full data-pipeline acceptance even if the generic load
observer passes. A narrow, data-preserving trade-identity correction is being
implemented and must receive its own migration, exact review and full live
acceptance. Old rows and all failed observations remain; no historical REST
backfill or replay is being used to repair evidence into a pass.

The full upgraded-capacity run completed **16:51:21.705129-17:21:25.160627 UTC**:
**1,803.455 seconds, 2,366 HTTP requests, zero HTTP errors, zero new queue drops
or recorder gaps**, and maximum commit lag **0.519497 seconds**. PostgreSQL
peaked at **392.781 MiB**, below the reviewed 800 MiB bound. The highest
application peak was dashboard **252.895 MiB**, below 400 MiB.

The generic load observer passed, but the stricter physical validator returned
`physical:market_trades:QQQ:not_advancing`; **full incident acceptance is false**.
Incomplete GLD/TLT minute buckets also remain visible in derived-frame evidence,
not filled with invented bars. The exhaustive 2,145-record PostgreSQL log read
found no backend termination/recovery/FATAL/PANIC during this interval; it retained
114 known duplicate-evidence-key SQL errors rather than claiming all SQL succeeded.

The separate report-admission exercise returned 429 in **0.098 seconds** while
ordinary sections remained 200, followed by a real **4,413,743-byte report** in
**2.541 seconds**. That report was not resent as a daily email. The full raw
observation is retained as lossless gzip with its uncompressed SHA256 in the
summary.

## Data-preserving trade-identity correction

Candidate `68c2e8fe086af06c548e7064014c8f1d927ff952` changes identity to
`(symbol, feed_source, provider_trade_id, exchange, event_at)`. Price, quantity,
receipt and processing fields remain outside the key, so replaying the same
packet retains its first original. The schema retains the old three-column
index prefix for existing lookups. Existing microsecond timestamp precision is
unchanged; no nanoseconds or missing records are invented.

Migration `0013_trade_event_identity` builds the replacement PostgreSQL index
concurrently, then swaps constraints in a short transaction. It does not rewrite
or delete historical rows, rebuild the PostgreSQL table, change the nine counter
triggers or repair old audit results. Index-build lock/statement limits are
two/300 seconds; final-swap limits are two/ten seconds. Unsafe downgrade refuses
to collapse newly distinct records. Real PostgreSQL fixture and migration results
must be recorded separately from local validation.

The exact candidate passed independent review and full validation (1,102 passed,
two skipped, 87.41% coverage). The real PostgreSQL private-schema fixture
`job-dagpqndg1s2s73fa3t60` then passed and rolled back, including the reproduced
old loss, cross-day/venue writes, original-record retention, late-batch rollback
and unsafe downgrade refusal. That fixture did not claim to test a concurrent
index build or a durable commit.

Recorder deployment `dep-dagps3dbedkc739omglg` applied the actual migration through
its normal pre-deploy command. A separate **18:00:53 UTC** readback verified
schema 0013, the exact five-field constraint, unchanged table OID **16549**,
all nine unchanged trigger OIDs, the unchanged hash of all 20 sampled historical
QQQ rows, and unchanged selected trading controls. No REST backfill was performed.
The replacement recorder owner `srv-dadn8son74is73apqcc0-58d84df74b-r47rd`
acquired its lease naturally and demonstrated healthy durable progress.
A fresh complete post-migration observation is required and is being retained
separately from the earlier physical failure.

**Remaining frozen-news-worker limitation:** the old event image `29cf232` also
calls the generic trade writer, whose application-side three-field precheck is
defective. A schema/recorder correction cannot fix that old application method.
The new method is fixed in the candidate library, but deploying it under the
existing morning cohort would violate its immutable code/config fingerprint.
Permission for a new shadow-only collection cohort was requested; the owner was
unavailable. The conservative choice is to preserve the old event image and
its existing pauses/terminal, not silently replace the cohort.

Therefore a corrected five-symbol recorder must **not** be described as fixing
every active trade-history writer or qualifying the complete news-paper product.
A separately reviewed event-role transition remains required. No trading cohort,
budget reset, old R1 acknowledgement transfer or new entry authority is created.

After schema 0013 is applied, older images can still read the unchanged columns
and continue running, but their old `alembic upgrade head` pre-deploy command
cannot resolve a revision absent from their checkout. Do not redeploy an old
image blindly or downgrade the schema to accommodate it; review a compatible
release transition first.

## Final retry outcome

**Subsequent update:** at 14:43 Eastern the owner authorized fixing the frozen
news worker. The separate [shadow-only repair](NEWS_WORKER_REPAIR_20260909.md)
updates that producer while preserving the old cohort and trading restrictions.
The statements below describe the earlier retry outcome; they are not permission
to reopen its terminal or erase its history.

The corrected recorder's complete **18:01:53.468854-18:31:56.921440 UTC**
observation passed the reviewed capacity/load and per-symbol physical checks:
**1,803.453 seconds, 2,366 HTTP requests, zero HTTP errors, zero new queue drops
or recorder gaps, maximum commit lag 0.614 seconds**, and PostgreSQL peak
**382.211 MiB**. Every application stayed below 400 MiB. QQQ trade counts in the
fixed physical window advanced **5 to 251**; quotes, trades and bars advanced
for all five required symbols. No historical backfill was used.

The exhaustive 2,073-record database log check found no termination/recovery/
FATAL/PANIC and retained 112 known duplicate-evidence-key SQL errors. The real
post-migration full report returned 200 in 2.750 seconds after explicit 429
admission control; ordinary sections stayed 200. Incomplete sparse IEX minute
buckets remain visible, so no complete derived-frame or qualification claim is made.

**No additional order was authorized or placed.** At broker clock
**14:32:57 Eastern**, the account was ACTIVE/unblocked and flat, with no open
orders or unresolved local intents. Its complete account-day list still contained
only the two prior BUY submissions and the BTC exit. The morning MISSED/terminal
and original entry pauses remained intact. The frozen event-writer transition
and operator-history accounting gap remain blockers to full news-paper readiness.

The existing Render notifier sent the separate requested retry summary once at
**14:36:16 Eastern**. Notification
`42474d68-9355-566c-be98-ecb23463ca41` is `sent`, attempts **1**, provider ID
**`18cef6dd-bcac-4659-8df4-f01f6501e844`**. Transient log-read failures were retried
as reads only; neither email nor job was resubmitted. Provider acceptance is
verified, not inbox delivery. The 15:45 Eastern read-only close watch remains
scheduled; through-close flatness is not yet claimed.

## Evidence

- [Approved resize request and response](../research/results/render-db-upgrade-20260909.json)
- [Pre-resize flat account and original controls](../research/results/render-db-upgrade-20260909-before-broker.json)
- [Planned outage and reconnection observations](../research/results/render-db-upgrade-20260909-reconnection.json)
- [Resize-related process events](../research/results/render-db-upgrade-20260909-process-events.json)
- [Complete account-day broker orders and posted fees](../research/results/news-paper-20260909-afternoon-broker-budget.json)
- [Automatic outcome-email provider acceptance](../research/results/render-notifier-outcomes-20260909-email-proof.json)
- [Actual notifier deployment](../research/results/render-notifier-outcomes-20260909-deploy.json)
- [Natural notifier handoff](../research/results/render-notifier-outcomes-20260909-handoff.json)
- [Actual QQQ provider-ID collisions across days](../research/results/render-db-upgrade-20260909-qqq-investigation.json)
- [Upgraded observation summary: capacity pass, physical failure](../research/results/render-db-upgrade-20260909-summary.json)
- [Complete compressed upgraded observation](../research/results/render-db-upgrade-20260909-full-soak.json.gz)
- [Upgraded interval database log check](../research/results/render-db-upgrade-20260909-pg-logs.json)
- [Actual private PostgreSQL migration fixture](../research/results/render-trade-identity-20260909-pg-fixture.json)
- [Pre-migration historical/constraint snapshot](../research/results/render-trade-identity-20260909-before.json)
- [Actual migrated schema and preserved originals](../research/results/render-trade-identity-20260909-after.json)
- [Recorder migration deployment](../research/results/render-trade-identity-20260909-deploy.json)
- [Final corrected-recorder summary and remaining product blockers](../research/results/render-trade-identity-20260909-summary.json)
- [Complete compressed post-migration observation](../research/results/render-trade-identity-20260909-soak.json.gz)
- [All-symbol post-migration physical progression](../research/results/render-trade-identity-20260909-physical-proof.json)
- [Final broker and account-day orders](../research/results/news-paper-20260909-retry-final-broker.json)
- [Final retry-summary email acceptance](../research/results/news-paper-20260909-retry-result-email-proof.json)
