# September 10 owner-requested paper calibration

The owner requested a Render-worker paper trade at **12:17 Eastern** and asked
why no automatic morning trade occurred. The deployed worker had deliberately
remained in **shadow mode with a September 9 practice date**, a global kill and
an R1 pause. No September 10 trading/session activation had been configured.
This was an incomplete automation handoff, not an attempted or failed fill.

## Explicit current-session scope

The new `owner-paper-calibration-v1` path is a single, finite **operator equipment
test**, not a fabricated news signal, qualified strategy or profitability claim.
Only the existing global lease-owning Render event worker consumes a request.
An operator job publishes the scoped request; it does not call a broker order
endpoint or start another worker.

An `OperatorPaperRequest` binds its nonce, host cohort/config, reviewed code,
paper account, current session date, actual acceptance hash, complete fresh
broker-history hash and explicit owner confirmation. Its entry deadline is at
most 30 minutes after approval and is created only after review, deployment and
actual live gates. The request also pins exact versions of the global kill,
host-cohort pause and dedicated operator-cohort pause. A later kill/pause update,
including active-to-active with a new timestamp, invalidates entry.

The normal strategy kill and old risk pauses remain active. No old cohort,
terminal, acknowledgement or authorization is overwritten or transferred.
The operator uses the existing OMS, durable intent, original client ID,
reconciliation and owned-quantity exit paths. Maximum entry is $25 AAPL, one
position including pending reservations, and the account-day two-BUY budget
includes prior diagnostic/manual submissions.

Entries require an ACTIVE/unblocked flat paper account with no open/unresolved
orders, complete valued history, current market hours, completed observation,
liquidity, a quote no older than five seconds, spread no wider than ten basis
points, fresh worker news collection and valid official halt/macro context.
Daily/weekly loss limits remain $50 and drawdown $150 at $10,000 virtual capital,
or stricter existing configured limits.

Protection is local, not broker-native: a 0.5% stop and 1% target are rebased to
actual filled VWAP. Unfilled remainder cancellation targets 30 seconds; owned
exposure liquidation targets 60 seconds. The worker supervises an active
operator request on a two-second cadence. Network/halts/partial fills can delay
targets; actual timestamps must be reported. Cancellation must be confirmed
before selling only the actual owned remainder. UNKNOWN submissions retain
their original client ID and reservation; no replacement BUY is inferred.

Lifecycle and terminal outcomes use the existing notifier/recipient with
idempotent IDs. Missing broker outcomes never become invented fills or profits.

## Historical risk bridge

Fresh complete broker reads found **six historical orders**, not only the three
from September 9: the earlier September 4 SPY diagnostic and BTC round trip
also belong to this account. No September 10 order existed at the initial check.
Both BTC inventory differences are explained by actual coin-fee quantities.
Funding is an identified $100,000 initial cash journal, not trading profit.

The bridge keeps actual fill cash flows, posted fees and modeled fee reserves
separate, counts external BUY attempts once per actual account/date/client ID,
and combines the baseline with subsequent OMS fills without double counting.
It refuses incomplete orders/activities, unvalued instruments, short/unexplained
inventory, rewritten historical records and unexplained positive cash flows.

The broker reported flat cash/equity of $99,999.27. Identified fills and posted
cash fees alone give a $0.52809467556 loss; the remaining cash deficit is not
misrepresented as a known fee. The conservative calculation includes a $0.20
unposted older crypto fee allowance and the residual unattributed debit.
Its initial modeled economic loss is $0.739917446313378, not a claim of exact
net fees or profitability.

## Automation boundary

This explicit request is not an indefinitely recurring authorization and does
not silently clear a manual kill or future R1 review. Daily automatic trading
requires a separately coherent standing policy, current-date session rollover
and risk-gated activation; a scheduled readiness check alone must not be called
automatic trading. Today's actual fill/exit/email proof is still required.

## September 10 continuation

The 14:55 Eastern continuation found the live service still on the previously
reviewed `68c2e8f` shadow release. No operator request or order had been published.
The final implementation review and complete validation had not both finished
before the session interruption; an incomplete test log is not a passing result.
The resumed full run on `7431393` passed 1,158 tests with two skips and 87.47%
coverage. Independent review remains a separate release gate.

The actual **14:57 Eastern** broker read was ACTIVE/unblocked and flat, with
zero open or unresolved orders and **zero September 10 orders**. The proposed
current-date shadow host and its operator request keys were absent.

The existing entry cutoff is **15:30 Eastern**, and a newly deployed execution
path requires a complete 30-minute live acceptance after deployment and natural
lease handoff. Once those prerequisites cannot finish before the cutoff, the
request must remain unissued even though the exchange itself stays open until
16:00. The cutoff is not moved to compensate for development or review delays.
This is an incomplete operational handoff, not an exhausted September 10 budget
or a rejected/filled trade.

Prior reviews found and corrected account-switch and owned-recovery defects
before deployment. The final code must preserve ordinary owned-position
supervision even when an unrelated operator recovery is invalid. These fixes,
the actual acceptance result and the final email outcome must be recorded
honestly; preparation alone is not execution.

The final review found no significant issues in `7431393`. The event service
was explicitly deployed at **19:09 UTC** with the new **shadow-only**
`iex-operator-host-20260910` cohort; other service pins and compute plans were
unchanged. Natural lease overlap/startup delays are retained in the handoff
evidence rather than classified as a successful live interval.

The new host's immutable config hash is
`f9da25a504f1c5920446555a548f5d6f5906f67b8082b48bb5bad5b25cce5dbd`.
The actual installed code recomputed its manifest and conservatively valued
the complete broker history, without persisting an operator request or
baseline import. Both prior cohort row hashes matched the pre-rollout snapshot.
Fresh processing/lease ownership was observed at **19:17:58 UTC**.

The full 30-minute post-deployment observation began at approximately **19:18
UTC / 15:18 Eastern**, so it cannot authorize an entry by the unchanged
15:30 cutoff. The order request remains **unpublished**. This deployment prepares
a safer one-off path; it does not implement standing daily authorization or
automatic session rollover and must not be described as doing so.

## Actual final outcome: no trade

The new shadow host completed its live observation from **19:18:10.780914 to
19:48:13.331665 UTC**: **1,802.551 seconds, 2,352 HTTP requests, zero HTTP errors,
zero new recorder drops/gaps**, and maximum recorder commit lag 0.783 seconds.
All required recorder series and AAPL/MSFT/NVDA trade rows advanced. PostgreSQL
peaked at 489.87 MiB and every application stayed below 400 MiB. Real report
admission returned 429, ordinary sections stayed 200, and the full report
returned 200 in 0.977 seconds.

The complete 2,176-record database log check found no termination/recovery/
FATAL/PANIC, while retaining 104 known duplicate-evidence SQL errors. This
establishes operational **shadow-host** behavior, not an executed operator order.
The actual broker check at **15:49:28 Eastern** remained ACTIVE/unblocked, flat,
with no open/unresolved orders and **zero September 10 orders**. Old cohort
hashes and selected controls were preserved. No operator request, active pointer
or new trading certificate was published.

**The owner's requested trade was not completed.** Preparation, required review,
the session interruption and deployment/verification took too long. Acceptance
finished after the unchanged 15:30 entry cutoff; it was not moved to fit the
request. Today's allowance was unused, not exhausted. No fill, rejection or
positive-return evidence is claimed.

The existing Render notifier sent one explicit no-trade status email at
**15:50:54 Eastern**, notification `28e61f50-8c53-55fa-96c9-fb69fabe62d6`,
attempts **1**, provider ID `ddfc6113-49e5-488e-b4ad-a7fa22bd9094`. A transient
log-read timeout was retried as a read only, not as another job or send.
Provider acceptance does not prove inbox delivery.

A **September 11 09:00 Eastern read-only pre-open readiness and status-email
follow-up** is scheduled to make unresolved blockers visible before the open.
It does not authorize a trade, manufacture daily confirmation, clear R1/manual
kills, or solve the still-unimplemented standing session-rollover policy.
The expired September 10 request must not be replayed on a later date.

## Initial evidence

- [Actual deployments, stale session date and pauses](../research/results/paper-sep10-start.json)
- [Complete broker order/activity baseline](../research/results/paper-sep10-broker-baseline.json)
- [All activity types and cash reconciliation](../research/results/paper-sep10-economics.json)
- [Actual AAPL liquidity and official context](../research/results/paper-sep10-context.json)
- [Final exact release review](../research/results/paper-sep10-release-review.json)
- [Actual shadow-host settings change](../research/results/paper-sep10-host-settings.json)
- [New manifest and complete history readback](../research/results/paper-sep10-host-identity.json)
- [Actual deployed shadow observation](../research/results/paper-sep10-host-summary.json)
- [Complete lossless observation archive](../research/results/paper-sep10-host-soak.json.gz)
- [Final unchanged zero-order broker state](../research/results/paper-sep10-final-proof.json)
- [Once-only no-trade status email](../research/results/paper-sep10-final-email-proof.json)
