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

## Initial evidence

- [Actual deployments, stale session date and pauses](../research/results/paper-sep10-start.json)
- [Complete broker order/activity baseline](../research/results/paper-sep10-broker-baseline.json)
- [All activity types and cash reconciliation](../research/results/paper-sep10-economics.json)
- [Actual AAPL liquidity and official context](../research/results/paper-sep10-context.json)
