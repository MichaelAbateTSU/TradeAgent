# Experiment #2 Proposal: volatility-conditioned 60-second momentum

**Status: proposed shadow-only, not activated. Retrospective screening did
not meet economic or coverage requirements. No broker orders authorized.**

The proposed strategy trial is superseded by the
[trade-free Shadow Research Dataset v1](../2026-10-shadow-dataset-v1/protocol.md).
No broker-paper or model-promotion run follows from this proposal.
Prepared September 30, 2026 UTC. The September cohort is complete and remains
closed. This is a new hypothesis and a shadow-first research protocol, not an
extension, requalification or reset of its results.

The completed [full-population retrospective](retrospective-findings.md)
does **not** support activating this candidate as a broker-paper strategy.
Its known-data complete-case outcomes remain negative after fees and
later diagnostic price coverage is insufficient. The dates below remain
a proposed, unstarted shadow window, not a trading commitment.

## 1. What failed and what was learned

`signal-scalp-experiment-20260921-r1` failed its promotion objective:
**14 qualifying round trips versus 30 required**, negative modeled net P&L
and a calibration audit that refused promotion. It did not statistically
disprove every momentum strategy. With fourteen observations and no final
fee attribution, neither a universal profitability claim nor a definitive
absence-of-alpha claim is justified.

The infrastructure provided useful evidence: broker-confirmed entries/exits,
flat ownership, compatible readers, cohort scoping, immutable decision facts,
and conservative accounting. Reuse it. Do not alter the finished cohort,
reduce fees, increase ticket size, manufacture more trades, open sealed
holdouts, activate live money, or run a new broker experiment automatically.

### Why only fourteen completed trades?

The account-tagged production candidate journal contained:

| Funnel | Count |
|---|---:|
| Minute-level experimental evaluations | 9,721 |
| No momentum/reversion family | 9,683 |
| Named symbol/family candidates | 38 (22 BTC, 16 ETH) |
| Passed all recorded candidate checks | 17 (14 BTC, 3 ETH) |
| Failed quote check among named candidates | 21 |
| All experimental entry intents in the cohort ledger | 19 |
| Intents stopped before POST | 4 (two stale quotes, two original-cap violations) |
| Broker POST entry attempts | 15 |
| Broker-accepted entry with no fill | 1 |
| Completed experimental round trips | 14 (13 BTC, 1 ETH) |

The gate counts were independently read in Render job
`job-dau73gou01pc73ft0or0`. `fresh_quote` failed on 9,704 evaluations,
but 9,683 of those had **no candidate quote because there was no signal**;
these are correlated checks, not 9,704 independent feed outages.
All named candidates passed their score check. Daily loss, submission,
filled-cycle caps and acceptance checks had no recorded failures in this
account-tagged subset.

There are 2,746 earlier cohort candidate events without an account digest.
They are explicitly excluded from account-scoped counts. Therefore the
17 eligible checks cannot be forced into an exact one-to-one match with
all nineteen entry intents. Never fill this gap with synthetic approvals.

**Supported diagnosis:** under the frozen heuristic/cadence, signal families
were rare; quote freshness/caps further reduced entries. Increasing the
holding horizon alone would not produce more entry signals. The observation
does not establish that thresholds were "too strict," that unobserved
opportunities did not exist, or that tuning them would create positive edge.
All fourteen completions were momentum; this is not a validation of reversion.

## 2. Where the economic edge was lost

| Component over 14 completed trades | USD | bps of $141.32249 deployed entry value |
|---|---:|---:|
| Fill-price P&L before inventory/cost effects | -0.050214 | -3.5531 |
| Reduced-quantity/cash-flow difference | -0.333364 | -23.5889 |
| Gross broker cash-flow difference | -0.383578 | -27.1420 |
| Further conservative modeled reserve | -0.426596 | -30.1860 |
| Conservative modeled net P&L | **-0.810174** | **-57.3281** |

All fourteen modeled outcomes were negative. BTC contributed thirteen cycles
and -$0.754987 modeled net; ETH contributed one and -$0.055187. Entry spreads
were 1.177-3.621 bps (median 2.689). Decision-to-first-fill delay was
0.764-3.417 seconds (median 1.075); decision-to-flat closure was
4.230-9.072 seconds (median 5.325). These are observed clocks, not exact
exchange latency or evidence that a five-second forecast lasted ten seconds.

The individual cost bridge is below (USD rounded to six decimals).
`Quantity/cash effect` and `extra reserve` are diagnostic accounting
differences, **not independently confirmed commission receipts**.

| Cycle prefix | Symbol | Fill-price P&L | Quantity/cash effect | Extra reserve | Modeled net |
|---|---|---:|---:|---:|---:|
| d9c28383 | BTC | -0.010065 | -0.025249 | -0.025161 | -0.060475 |
| 3266e387 | BTC | -0.009460 | -0.025257 | -0.025170 | -0.059887 |
| f7d9ee0b | BTC | -0.004996 | -0.015174 | -0.035239 | -0.055408 |
| edc45269 | BTC | 0.000062 | -0.025252 | -0.040000 | -0.065190 |
| 635fa112 | BTC | -0.000077 | -0.025217 | -0.040000 | -0.065294 |
| 4e54aa3b | BTC | -0.002116 | -0.025334 | -0.025181 | -0.052631 |
| e3c354dc | BTC | -0.002448 | -0.025263 | -0.025194 | -0.052906 |
| 2e1e0aea | BTC | -0.001634 | -0.025320 | -0.040000 | -0.066953 |
| 3fba756f | BTC | -0.001517 | -0.025196 | -0.025130 | -0.051843 |
| d8eb0438 | BTC | -0.003389 | -0.025266 | -0.030000 | -0.058655 |
| bd7e6a87 | BTC | -0.002760 | -0.015163 | -0.035223 | -0.053146 |
| cf8c4022 | ETH | -0.004897 | -0.025183 | -0.025108 | -0.055187 |
| 21b675d4 | BTC | -0.002274 | -0.025260 | -0.025191 | -0.052725 |
| c7adbbeb | BTC | -0.004643 | -0.025231 | -0.030000 | -0.059874 |

**Fees:** final attributable fee activities are pending on all fourteen.
The reduced-quantity difference and conservative reserve are not confirmed
commissions. Keep actual net unknown.

**Spread/slippage:** both affect observed fill prices and are already
embedded in fill-price P&L. Decision spread is a diagnostic, not a second
cash deduction. A separate dollars-only slippage attribution requires
matched arrival/dispatch quote provenance; it is not identified from this
aggregate report. Additional live market impact/queue costs are unmeasured.
Do not fabricate a clean independent fee/spread/slippage decomposition.

The existing worst-case entry fee and taker exit fee are each 25 bps.
Multiplicative inventory/cash charging needs approximately **50.1881 bps
gross ask-to-bid price gain merely to break even**. A research target of
**+5 bps net** needs approximately **55.2132 bps gross**, before any
additional unembedded friction. The fourteen trades were already negative
on the fill-price measure, so fees alone do not explain the entire failure.

## 3. New hypothesis and frozen candidate definition

**H2-60:** With otherwise unchanged crypto universe, fees and price-capped
execution assumptions, high-volatility positive-order-flow momentum
expansions have a larger **cost-adjusted 60-second payoff than the identical
candidate's five-second payoff**, and the 60-second payoff remains positive
on later prospective observations.

This is the user's proposed explanation to falsify, **not a pattern proven
by the fourteen trades**. It is a momentum-expansion proxy, not a claim
that recorded features identify an actual resistance breakout.

Predecision eligibility is fixed before prospective data:

- BTC/USD and ETH/USD; momentum family; score at least 0.25.
- Recorded five-second volatility at least 5 bps.
- Recorded 15-second midprice change greater than 5 bps.
- Positive recorded `normalized_ofi_5s`.
- Spread at most 5 bps; otherwise the same two-second paper entry fence,
  original two-bps price cap and account/ownership safeguards.
- Missing features mean ineligible, never zero-filled substitutes.

Only entry conditions and the proposed 60-second deadline change.
Universe, fee assumptions, broker isolation, caps and accounting do not.
Selection should remain deterministic with no profitable-outcome-based
symbol rotation. A later paper phase would retain at most one pending/owned
position, $10.25 cap, existing twelve-submission/six-fill daily caps and
$5 modeled daily loss stop. The existing OMS would remain the only broker
writer. No such phase is selected by this proposal.

The shadow payoff deadline is **60 seconds from original decision**, not
60 seconds after a delayed fill. Arrival allowance is fixed at 350 ms; no
entry is modeled at/after the exit deadline. The fixed entry, cancellation,
payoff and recovery semantics must be represented in any future policy and
artifact. Never load the old ten-second artifact into a sixty-second worker.

## 4. Fixed research dates and trial budget

| Stage | Window (UTC) | Status |
|---|---|---|
| Existing evidence exploration | September 21 to September 28, 2026, 14:40:07 | Known data, retrospective only |
| Proposed prospective shadow development | October 1 00:00 to October 8 00:00 | Not scheduled or started |
| Proposed prospective shadow validation | October 8 00:00 to October 15 00:00 | Not scheduled or started |
| Fixed review | October 15 00:00 | Pass, fail or inconclusive; no automatic extension |

If the implementation is not frozen and prospectively recording by October 1,
mark this proposed window **missed**, rather than backdating later collection.
A replacement date/policy requires a new proposal revision before outcomes.

The retrospective pipeline inspects exactly **seven** horizons: 1, 5, 15,
30, 60, 300 and 900 seconds. All fixed regime buckets and all failures remain
visible. These comparisons and discovery-only top-decile review count as
exploration, not seven independent confirmatory tests. No horizon or
threshold is optimized against the later diagnostic split.

The sole prospective primary comparison is **60 versus 5 seconds** on the
same H2-60 candidates. The 30-, 300- and 900-second scenarios are diagnostics,
not fallbacks from which to choose a winning live policy.

## 5. Required evidence and success criteria

Thirty completed independent broker-paper cycles (20 training, 10 later)
remain a minimum for any subsequent paper promotion path, **not a sufficient
statistical sample size and not an order quota**. The shadow phase cannot
satisfy a broker-fill requirement.

For research screening require at least 100 complete, non-overlapping primary
candidate observations in each chronological half, at least five represented
UTC days per half, and at least 95% price/size/continuity coverage of the
declared eligible population. Sparse or censored data is **inconclusive**.
Select non-overlapping primary observations using earliest eligible receipt
first, a 60-second exclusion window per symbol; do not discard overlapping
raw rows or call adjacent events independent.

At the single fixed review:

1. Later 60-second mean net return exceeds the +5 bps economic target.
2. The later day-block uncertainty interval for mean net is above zero.
3. The paired 60-minus-five-second improvement is positive, including its
   day-block uncertainty interval.
4. The conclusion is not a single-asset/day outlier: report leave-one-day-out
   sensitivity, both symbols, the losing tail and data coverage.
5. Repeat at fixed 3.5-second arrival latency and +5 bps **additional**
   residual-cost stress (without removing/reducing fees). Net must remain
   positive under both; these are research stresses, not live-fill validation.

The 3.5-second stress exceeds the observed 3.417-second maximum in the
fourteen completed cycles; it is a fixed conservative scenario, not a claim
about the entire venue's latency distribution. The base 350-ms replay
allowance comes from the existing shadow convention and is more optimistic
than the cohort's median decision-to-fill delay. Its results must not be
presented as achieved deployment latency. At 3.5 seconds, a one-second
entry/exit horizon is inapplicable, not assigned an invented fill.

These requirements do not replace existing model validation, execution
certification, account provenance, calendar coverage or forward qualification
floors. Crossing a bootstrap bound is not license to deploy a model.

Illustrative sizing: with independent return standard deviation of 30 bps,
a normal-approximation 95% mean half-width of 2 bps needs roughly 865
independent observations. This is planning arithmetic, **not measured
volatility or a promised sample count**. Freeze any revised power calculation
after discovery-only variance estimation, before prospective validation.
The fixed end date does not move if signals are rare.

## 6. Expected edge after fees

**Not established.** Neither a high score nor a longer horizon establishes
expected value. +5 bps net is the minimum economically meaningful target,
not the expected return. The existing fourteen-cycle average was
approximately -$0.05787 modeled net per completed cycle.

To distinguish "predictive but uneconomic" from "no demonstrated prediction,"
compare midpoint directional outcomes and score/return correlations with
fresh arrival-ask/deadline-bid returns after unchanged fees. A positive
midpoint direction rate with negative executable net suggests a cost/
execution hurdle; it does not prove profitable predictability. Near-zero
correlation or accuracy cannot establish randomness with sparse,
dependent observations. Neither case can be resolved by loosening gates.

## 7. Deliverables, reuse and next decision

`tradeagent scalp-signal-research` is an offline, read-only evidence layer
over all retained candidate decisions, not another trading engine. It emits
source-hashed per-signal markouts, coverage/missingness, fixed regimes,
predictive diagnostics, paired horizons and discovery-only top-decile
descriptions. See [the research runbook](research-pipeline.md).

The old neutral no-signal ticks were aggregated, so their exact features
cannot be reconstructed. No news correlation is invented without a
point-in-time join. Existing replay results are neither broker fills nor
untouched forward validation. The old cohort and sealed holdouts remain
unchanged.

**Decision sequence:** inspect retrospective feasibility; reject H2-60 if
costs dominate even credible longer-horizon marks; otherwise implement and
freeze a new shadow-only protocol for the proposed dates. Only a later
approved paper phase may test broker execution at the new horizon.
No automatic rearm, promotion, paid feed or new service is created.
