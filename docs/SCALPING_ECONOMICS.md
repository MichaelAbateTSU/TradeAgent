# Scalping action economics

The September 11, 2026, 18:11 Eastern instruction keeps TradeAgent a
short-horizon paper scalper. It replaces the earlier permission to ignore
economic entry gates, not the paper endpoint, ownership ledger, existing
recipient, or daily email schedule. It does not add a daily trade quota,
minimum five-minute hold, news requirement, or live-money permission.

## Audit of the observed release

The observed event worker was
`211347c82b2f4b04aa0dd22e042dfa3eae434ee1`. Its two September 11 runs supplied
62 completed cycles and 167 no-fill cycles in the reporting window from
September 10 at 18:00 Eastern to September 11 at 18:00 Eastern. The completed
cycles' estimated net result was `-31.987050091879617240` USD. This is not
confirmed final net P&L: fee attribution was pending.

The original implementation was **not a trained directional or economic
model**. It used transparent, uncalibrated heuristic scores. An 11-second
average hold does not by itself diagnose a scalping defect, and the
filled/unfilled pattern alone does not prove adverse selection.

The completed reconstruction found:

| Fact | Actual result |
|---|---:|
| Candidates reconstructed | 229 |
| Completed / no fill | 62 / 167 |
| Momentum / reversion completions | 59 / 3 |
| Raw fill-price P&L | `-$3.619737530813030699` |
| Cash-flow difference | `-$13.141326540867069736` |
| Modeled net after reserves | `-$31.9870500918796172393025` |
| Confirmed actual net | unavailable for all 62 |
| Entry and exit orders independently cross-checked | 124 / 124 |
| Trades cross-checked by both broker history and FILL activity | 62 / 62 |

No accounting failure was proven. The gap between raw price P&L and cash flow
includes `9.521589010054039037` USD of reduced-quantity principal already
reflected in cash flow. The remaining model used `14.1671251421457625033025`
USD of cash reserves and `4.678598408866785000000` USD of additional
base-fee reserves. These figures must not be blended into "actual fees."

The evidence classifier assigned 12 `ADVERSE_SELECTION`, seven
`COST_FAILURE`, and one `LATENCY_FAILURE` labels. Labels can overlap. All 62
also retain `UNKNOWN` because historical prediction, trained regime and
complete price-curve evidence were unavailable. `ALPHA_FAILURE`,
`EXECUTION_FAILURE`, `EXIT_FAILURE`, `REGIME_FAILURE`, `STALE_DATA_FAILURE`
and `ACCOUNTING_FAILURE` were not proven.

Markout coverage was sparse: 9/62 at 100 ms, 24/62 at 250 ms, 20/62 at
500 ms, 21/62 at one second, 19/62 at two seconds, 24/62 at five seconds,
and 20/62 at ten seconds. No trade had a complete freshness-qualified holding
curve. Filled-vs-unfilled comparisons are therefore diagnostics, not proof of
causality or an executable counterfactual.

The longest trade lasted `422.150523` seconds. Its exit request was recorded
0.896357 seconds after its timer deadline. Five broker HTTP 429 cooldowns
overlapped 300 seconds after the deadline; another 107.140415 seconds of
overrun cannot be assigned to a specific unrecorded cause. The repair latches
time, missing-data and catastrophic exit reasons even while broker backoff is
active, so recovery evidence no longer waits for the cooldown to expire.

| Stage | Implementation and actual behavior | Repair requirement |
|---|---|---|
| Receipt and book | `scalping_market.py`: timestamped aggregate L2, native quotes/trades, local receive-order and snapshot integrity | Preserve native/proxy distinctions; expose decode and channel clocks; bound freshness by the decision horizon |
| Features | L1/L5 imbalance, OFI, microprice, additions/removals, volatility, returns and native taker delta when available | Missing taker side or exact cancellations remain unknown; do not invent MBO or queue position |
| Candidate and regime | `scalping_strategy.py`: separate momentum/reversion hypotheses, momentum precedence | Preserve each family and the original owned thesis; model support and uncertainty decide economic eligibility |
| Action choice | Fixed `entry_style`, not a fill-conditioned optimizer | Compare passive, aggressive and NO_TRADE values; unsupported short entries cannot be selected |
| Entry costs | Fee/spread diagnostics; `expected_net_edge_bps` was null and did not veto orders | Prospective economic evidence must be present and positive before reservation and final submission |
| OMS | `scalping_execution.py`: pinned account, one lease, durable client IDs, cash/ownership reconciliation, cancel confirmation | Preserve these fences; revalidate economics and remaining signal life after slow broker calls |
| Exits | Any positive family could maintain a position; first-fill 15-second timer; backoff deferred supervision | Keep the original family and prediction deadline; latch overdue/protection exits even during broker backoff |
| Accounting | Cumulative broker VWAP/quantities; confirmed cash fees and base-coin ownership debits; conservative pending reserves | Reconstruct from original orders/activities, separate price P&L from cash flow, and never double-charge spread or base fees |
| Replay | Same features/heuristics, conservative visible queues; cash-fee convention and late markout sampling | Use the same economic policy, base-coin fee convention, seven bounded markout horizons and holding-bounded excursions |
| Journal | Order intents retained signals, but no complete stage clock or future markout journal | Keep every candidate, aggregate neutral observations, persist diagnostic revisions, expose missing observations |
| Daily report | `daily_email_summary.py`: five paragraphs, consecutive 18:00 reporting windows, all account runs | Preserve cadence, honest losses and pending-cost labels; keep technical detail in the journal |

One concrete recording defect was found independently of any strategy claim:
`persist_market_batch()` looked for `exchange_time_ns`, whereas canonical
market events use `exchange_at_ns`. Raw compressed events were intact, but the
batch's exchange-range columns were null. New writes use the canonical field;
historical compressed evidence is not rewritten. SQLite uses a 64-bit integer
variant for these ranges so nanoseconds are not rounded through a float.

## Measurement and accounting rules

Reconstruction uses original client and broker IDs, positive cumulative fills,
and separately keyed fill/fee activities. Canceled, rejected and unfilled
orders are not completed trades. Entry/exit VWAP uses actual fills, not quotes.

For crypto, a BUY fee can be paid in base coins. Fewer coins then reach the
SELL. The resulting cash-flow difference already contains the value of those
missing coins; charging the same base fee again is incorrect. Price-only P&L,
cash flow, confirmed cash/base fees, inferred allocations and pending cost
reserves therefore have separate meanings.

A passive limit's actual liquidity role is not established merely by its
order type. Until attribution supports a lower rate, prospective entry costs
use the worse configured maker/taker fee. With the observed 15/25-bps
configuration, this is a 25-bps entry allowance plus a 25-bps exit allowance,
not an optimistic 40-bps round trip. Neither allowance is an actual fee receipt.

Markouts cover 100, 250 and 500 milliseconds, and 1, 2, 5 and 10 seconds.
Each sample retains its source timestamp and age. Sparse observations are
censored, not interpolated or borrowed from a later favorable quote. MFE/MAE
ends with the actual position lifetime. Unknown forecasts, regimes, queue
positions and cost components remain explicitly unknown.

## Runtime contract

The new command path selects `action-value-v1`. Low-level legacy configuration
remains readable so old cohorts can be reconstructed and recovered without
rewriting their frozen rules. Missing or rejected economic evidence produces
NO_TRADE, not an automatic fallback to legacy buying.

The local catastrophic stop defaults to an explicitly recorded **100 bps**
in the new command. This is a protective operating boundary, not a fitted
profit target or a claim that 100 bps is optimal. It is rebased to actual entry
VWAP. Broker backoff, gaps, halts and slippage can prevent execution at the
threshold. Missing fresh protection data requests owned recovery without
pretending a current price is available.

The time deadline is the original decision's prediction horizon plus declared
latency grace. Grace cannot exceed that horizon, and the default is zero.
Neither a delayed fill nor a restart starts a fresh multi-minute thesis.
Owned exits remain possible when new-entry economic evidence is unavailable.

Recorded stage clocks cover receipt, decode, state update, features, model,
risk approval, submit, acknowledgement and broker fill. Stage percentiles
include missing/reversed-clock counts; an absent timestamp is not zero latency.
The diagnostic writer does not own execution, send orders or deliver email.
Its bounded quote cache identifies evictions; immutable raw tape supports
offline reconstruction when a fine-grained live sample was unavailable.

The read-only `/api/scalping/diagnostics` endpoint serves a bounded page of the
latest diagnostic revision for each cycle, not a disguised daily P&L total.
`tradeagent scalp-diagnose --snapshot ... --output ...` reconstructs recorded
evidence without initializing a trading engine.

`tradeagent scalp-calibrate` consumes the raw calibration envelope, not a
hand-selected subset. If any candidate in a symbol/family/action group lacks a
required input, the entire group remains unsupported. The September 11 export
has **zero model-ready candidates** at both one and five seconds because exact
decision-to-send latency was absent for all 229. It also lacks some
fresh-horizon returns and independently priced spread/slippage/impact/adverse
selection components.

The resulting pinned five-second artifact is intentionally `no_support`:

- Model ID: `adb76e65b74528a8f8e5d017b4da7e675d63de2ce724da912d1572f84052837b`
- File SHA-256:
  `db755d4064f8cde5461d1295a0e1b418443b423d4034439bba0b926109617d52`
- Raw candidates: 229
- Accepted calibration samples: 0
- Reason: `NO_SAMPLES`

Loading that artifact produces NO_TRADE. It does not fall back to the old
heuristic policy, create aggressive counterfactuals, or claim validation.

The execution worker loads an artifact only when its file SHA-256, internal
model ID, paper-account digest, fee schedule, horizon, cancellation horizon and
symbol support agree with the frozen run. The initial decision chooses among
supported passive/aggressive actions and NO_TRADE. Immediately before the
broker POST, after prerequisite account/position/asset reads, the OMS
revalidates the **original** feature fingerprint, model, action, expiry,
elapsed latency, current spread and current midpoint. A fresh quote cannot
reset an old prediction. A passive limit that has left the current bid or an
aggressive limit that no longer reaches the ask is rejected without a POST.

Feature construction may use the configured five-second observation window so
the worker can retain abstention and calibration candidates during quieter
crypto periods. This does not weaken execution: the final order fence still
requires a quote no older than the one-second decision interval, plus the
model's own observed support and remaining-horizon checks.

The paper order stream is applied to cumulative owned order state before the
normal execution step. A stream gap still triggers REST reconciliation, and
REST remains authoritative at startup and periodically; an ordinary valid
stream update no longer forces a full REST reconciliation by itself.

## Release evidence

Implementation, chronological economic validation and the new deployment
must be verified separately. The observed September 11 source export is
`research/results/v30-economics-20260911-source.json.gz`; it was collected by a
read-only same-image Render job with no order, lease, policy or email mutation.
The source evidence and failed checks are retained. No profitability or
successful new-model fill is established merely by this repair.

The final local implementation summary is
[`v30-economics-20260911-implementation-summary.json`](../research/results/v30-economics-20260911-implementation-summary.json).
The full repository validation completed with 1,823 tests passed, two skipped,
and 88.33% coverage; whole-project Ruff, formatting and strict mypy also pass.

## Current deployment

The owner explicitly authorized deployment in the September 11, 22:20 Eastern
prompt. Release `42901a99c19b46624aaa0625bfea509fa0482732` is running on the
existing event worker as cohort `v30-action-value-20260911-r1`, configuration
`cc56a298ade989d7997728cb777616b796abb9ae9147f0e9a6c6fcc9216c61b4`.
The dashboard uses the same release. The notifier and recorder remain
suspended because the prompt authorized starting the new trading method, not
restoring every stopped process.

The worker loaded the pinned `no_support` model and is current, but it has
created **zero cycles and zero orders** for the new cohort. The paper account
was independently observed ACTIVE, unblocked, flat and without open orders.
This is an active evaluation/telemetry deployment, not a claim that a trade
occurred. Starting the new method cannot truthfully mean bypassing its
NO_TRADE decision.

The release and postdeploy evidence are stored under
`research/results/v30-action-value-20260911-*`. No automation or follow-up wake
was created.
