# September 2026 paper-scalping cohort: findings

This is a closeout of the **paper-only** `signal-scalp-experiment-20260921-r1`
cohort. Its frozen entry cutoff was September 28, 2026, 14:40:07 UTC. The
read-only production report was retrieved on September 29 at 20:27:38 UTC
from `/api/scalping-experiments`; the independent calibration audit was run
without writing or selecting a model in Render job
`job-dau1vjlg1s2s73b5926g`. It is not a claim about live-market profitability.

## Cohort result

| Measure | Observed result |
|---|---:|
| Experimental entry intents | 19 (17 BTC/USD, 2 ETH/USD) |
| Experimental broker POST entry attempts | 15 (13 BTC/USD, 2 ETH/USD) |
| Experimental entry intents expired before broker POST | 4 |
| Experimental broker-accepted entry with no fill | 1 |
| Experimental no-fill cycles | 5 (four unsent, one broker-accepted) |
| Broker-confirmed completed and flat round trips | 14 (13 BTC/USD, 1 ETH/USD) |
| Independently accepted calibration samples | 14, with no sample exclusions |
| Required chronological training / later held-out | 20 / 10 |
| Available held-out after required training allocation | 0 |
| Actual net P&L with final fee attribution | unavailable for all 14 |
| Entry value deployed across completed cycles | $141.322490248054938400 |
| Fill-price P&L before inventory/fee effects | -$0.050213653289102991 |
| Broker gross cash-flow difference | -$0.383578015531997137 |
| Conservative modeled net P&L | -$0.810174335156147828 |
| Modeled net return on aggregate deployed entry value | -57.3281 bps |

The fill-price P&L is already negative. The difference between that measure
and cash flow includes reduced sellable base inventory and must **not** be
relabeled entirely as an observed fee. The model deducts an additional
conservative $0.426596319624150691 from cash flow. Observed broker fills
already reflect spread and paper execution prices; this bridge does not deduct
them again. The exact final net remains unknown until attributable Alpaca fee
activities arrive. These are small, simulator-only observations; they do not
estimate real queue position, market impact or latency slippage.

**Decision:** This bounded cohort expired with **14/30** required independent
cycles, no later held-out population, and negative observed economic results.
It failed the predeclared support floor and did not demonstrate a profitable
edge. No model was promoted or artifact selected, and the qualified strategy
remains `no_support`. This does not prove that every possible scalping strategy
is unprofitable. Do not retroactively extend this cohort or increase its order
frequency to fill the shortfall.

The actual-cycle calibration audit reports an additional compatibility
boundary: its maximum decision-to-exit contract is **10 seconds** (five-second
entry TTL plus five-second owned hold); the current qualified worker expects a
**five-second** artifact. Even a positive 10-second result could not simply be
loaded under that five-second configuration.

## One traceable paper round trip

The September 28 BTC/USD cycle
`c7adbbeb-ac06-513e-a970-56eb01257f43` has a matching candidate-check
event `64e8835f-3a0b-4f73-87c4-14ac957a3b01` at 08:01:00.409531 UTC.
The frozen experimental checks passed: momentum score **0.26927** against
**0.25**, quote age **0.347 seconds** against **2 seconds**, and the daily
submission, fill, loss and entry-cutoff gates. Its decision and feature
snapshot, original quote and policy hash are retained with the cycle.

The existing OMS submitted BUY client ID
`ta30e-0abf3a8c5bdd7565d7f1d8c0b79424bd7cbfba21`, broker ID
`cf8e0d8c-d087-47b6-9761-9858b6a0c791`, filled at
08:01:01.573371 UTC for 0.000121600 BTC at $82,996.24. It then submitted
the owned-quantity SELL client ID
`ta30e-afbc7a062b4a92d8b56cc414d695040df52fc9db`, broker ID
`93edc584-8040-42a3-8937-48724cb9c4eb`, filled at
08:01:03.958986 UTC for 0.000121296 BTC at $82,957.96. Reconciliation
closed the cycle at 08:01:05.089245 UTC with zero owned quantity. Gross
cash flow was -$0.02987406784 and conservative modeled net
-$0.05987406784; actual final net remains pending.

This was an **experimental heuristic** entry, not a qualified-model action:
the stored qualified signal selected `NO_TRADE` / `hold` while the separate
bounded experiment independently checked its momentum family and score.
The broker order IDs, fills, ownership and P&L are linked to the same cycle.
The matched candidate check is verified by cohort, symbol and decision
timestamp; historical candidate events did not carry a durable cycle ID.

## Four reported defect repairs and production limits

| Defect | Evidence after repair |
|---|---|
| Unconditional passive-probe broker polling | Regression test proves ten idle ticks defer nine redundant reconciliations. The passive probe cohort was already superseded before this deployment, so there is **no prospective production request-rate comparison** for that retired loop. |
| Probe payload crashes historical strategy loader | Read-only production job `job-dau21fmk1f9s73a25sk0` loaded September 18-20 without crashing: 12 incompatible probe cycles were excluded explicitly, with zero strategy candidates in that window. No missing signal features were invented. |
| Mis-scoped evidence reports | The restored report requires account, exact cohort, policy hash, and entry cutoff for cycle evidence. Fixture tests inject wrong-cohort, wrong-policy and out-of-window rows. Account-specific candidate totals exclude and disclose **2,746 legacy events** lacking an account digest. |
| BTC-first passive selection | Persisted least-submitted-symbol selection and restart-safe tests prevent tuple-order starvation. The 16 old passive no-fill cycles were **13 BTC / 3 ETH** and predate this repair; they cannot prove its effect in production. The active signal cohort has its own score-based choice (13 BTC / 1 ETH completed), not the passive round-robin. |

The first report attempt timed out/returned 502 as the unbounded journal
grew. A bounded aggregation initially returned 503 on PostgreSQL because
separately bound JSON paths appeared in `SELECT` and `GROUP BY`; job
`job-dau1sevlk1mc73dc4e70` exposed that exact database error. Grouping
over one projected subquery restored HTTP 200. The database contained over
116,000 redundant blocked acceptance checks after the acceptance trade;
their writer is now disabled when acceptance is complete or expired. Existing
records were not deleted.

The original report also called all **51 reserved order intents**
"submissions," despite **20 expiring before any broker POST**. The repaired
funnel reports 51 intents, 31 identifiable broker POST attempts (including
16 BUYs), 20 expired-unsent and zero unknown dispatch outcomes. These are
account/cohort-scoped records across passive probes, acceptance and the signal
experiment, **not** 51 trades or 31 independent payoff samples.

A September 28 runtime snapshot recorded one `ValueError` at 09:15 Eastern.
The available worker log identifies the exception class but not the cause;
subsequent worker ownership and position/order reconciliation were healthy.
Treat recurrence as an operational incident rather than claiming this
historical error was explained.
