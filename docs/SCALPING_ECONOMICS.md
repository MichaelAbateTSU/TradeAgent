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
offline reconstruction when a fine-grained live sample was unavailable. The
live cache retains at most two minutes and 10,000 quotes: this covers the
five-second pre-entry and ten-second post-close diagnostic window with ample
scheduling margin, while avoiding an unnecessary fifteen-minute memory cache.

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

## No-order model bootstrap

## Controlled paper execution-validation probes

The action-value strategy remains `no_support` and must continue to submit no
strategy entries. Separately, `execution-validation-probe-20260916-r1` is an
immutable, paper-only BTC/USD and ETH/USD cohort that collects prospective
broker fill, cancel, partial-fill, and owned-exit labels. A probe is a small
passive BUY at the current bid with a short TTL; there is one global outstanding
probe order or owned probe exposure, plus finite daily submission and filled
cycle caps. Its client IDs, durable payload classification, and reporting are
separate from strategy P&L and cannot promote a model.

The cohort may not initiate at or after `2026-09-21T15:22:58.996-04:00`.
After that deadline, cancellation, reconciliation, and broker market-sale
recovery of already proven owned probe inventory remain allowed. It uses the
same pinned paper-account checks, singleton lease, native asset validation,
idempotent client-ID lookup after ambiguous acknowledgements, and
never-sell-foreign-inventory fence as the regular OMS. A held-out report uses
only chronologically partitioned, actually resolved broker labels and reports
`not eligible` until enough exist; it makes no profitability or model-validity
claim.

## Marketable acceptance and bounded experimental scalping

The passive probe cohort is superseded for new submissions. Its historical
orders remain immutable evidence, but a closed no-fill attempt is not an
economic label. The report now separates candidates, blocked candidates,
submissions, broker acceptances, partial fills, entry fills, completed exits,
flat reconciliations, and independent completed round trips. Multiple broker
status updates for one cycle never increase the independent-observation count.

`execution-acceptance-20260921-r4` first sends one separately classified,
price-capped marketable BTC/USD paper limit through the existing OMS. The
entry uses a fresh ask, a hard $10.25 cap, a five-second TTL, fee-compatible
quantity rounding, and a five-second owned exit. Sizing targets Alpaca's $10
minimum retained exit value plus a 50-basis-point buffer, because a $5 entry
is rejected and an exact $10 gross buy can fall below the sell minimum after
base-currency fees. Up to three attempts are allowed, but the cohort stops
after one broker-confirmed entry, exit, recorded P&L, and operationally flat
reconciliation. This verifies the Alpaca paper integration only; it is not
passive-fill or profitability evidence.

The quote that establishes the immutable price cap must be fresh when the
intent is reserved. Account and reconciliation calls may age that quote before
broker dispatch, so a marketable experiment obtains another quote immediately
before POST. That dispatch quote must itself be fresh, must not predate the
original quote, and its ask must remain inside the original cap. Pre-submit
rejections are persisted with the actual quote and threshold.

After acceptance, `signal-scalp-experiment-20260921-r1` may collect bounded
BTC/USD and ETH/USD paper scalps through September 28, 2026 at 10:40:07
Eastern. It requires an existing positive momentum or reversion candidate,
a quote no older than two seconds, a marketable price-capped limit, at most
$10.25 per order, one global order or owned position, no more than twelve
submissions and six filled cycles per UTC day, a five-second maximum hold, and
a five-dollar modeled daily loss stop. Account pinning, lease ownership,
stale-data checks, durable intent-before-submit, duplicate IDs, partial-fill
handling, cancellation reconciliation, and never-sell-foreign-inventory rules
remain active.

Acceptance and experimental cycles are excluded from qualified-strategy P&L
and cannot flip `no_support`. `/api/scalping-experiments` and
`tradeagent scalp-experiment-report` expose the detailed funnel, each order's
quote, limit, broker ID, broker timestamps, cancellation state, fill evidence,
P&L, flatness, and exclusion reason.

Actual model transition requires at least 30 independent completed
experimental round trips: 20 chronological training observations and 10 later
held-out observations. `tradeagent scalp-actual-calibrate` converts only
broker-confirmed experimental cycles into `EconomicSample` rows, applies
fill-to-fill returns plus the conservative configured fee allowance, runs the
existing purged chronological calibration, and writes an artifact only when
the sample count and model status are both validated. The worker's existing
file hash, account, horizon, cancellation horizon, fee, and universe checks
must then load that artifact. Insufficient or nonpositive evidence remains
`no_support` or `failed_validation`; it never falls back to heuristic trading.

Alpaca paper fills omit real queue position, latency slippage, and market
impact. Marketable acceptance is therefore kept separate from passive
execution-quality claims and from any future live-money decision.

Waiting without labels cannot turn `no_support` into a valid model. The
action-value worker therefore records a calibration-only outcome for each
positive momentum or reversion state while continuing to submit **no broker
order**.

For each candidate, the evaluator freezes both actions:

- `PASSIVE_BUY` joins the recorded bid after a conservative 350 ms arrival
  allowance. The allowance exceeds the historical 331.554 ms p95 from
  dispatch start to first broker observation. Visible bid size remains ahead;
  cancellations never grant queue priority; only native sell-aggressor prints
  at or through the limit reduce the queue.
- `AGGRESSIVE_BUY` uses the frozen decision ask as a marketable limit. It may
  consume only already-observed BBO/L2 depth at or below that limit.

Both actions use the same requested notional, three-second entry expiry and
five-second prediction horizon. The exit is valued from already-received bid
depth at the horizon. A horizon quote older than 250 ms, insufficient exit
depth, changed arrival touch or missing native trade evidence produces an
explicit incomplete outcome rather than a zero return.

The runtime records the exact model-to-shadow-send boundary, entry fill/no-fill,
fill delay, fill fraction, decision/horizon midpoint, embedded spread/slippage/
impact/adverse-selection result, and a separate worst-configured entry plus
taker-exit fee allowance. These records never call Alpaca's order endpoint.

Historical passive outcomes validate the simulator before its samples can be
treated as `validated_simulation`. The predeclared validation requires at least
100 candidates, precision of at least 80%, and a false-positive rate no higher
than 10%. Aggregate L2 cannot validate aggressive execution against the old
passive-only broker history, so aggressive outcomes remain diagnostic until
separate execution evidence exists.

Shadow calibration requires at least 95% complete outcomes per cell and seven
calendar days of observation. It then uses the same purged chronological
fit/validation and positive conservative held-out net-edge requirements as the
main model. A complete but unprofitable dataset still produces NO_TRADE.

The read-only `/api/scalping/shadow` endpoint reports candidate and outcome
coverage. `tradeagent scalp-shadow-calibrate` freezes a reviewed artifact from
a validated report. The collection-time estimate targets 200 candidates and
30 simulated fills per cell plus seven calendar days; it estimates statistical
support, never guaranteed profitability.

The collector keeps completed outcomes in an acknowledgement buffer until the
deterministic audit record is committed. On restart, mature candidates without
outcomes become explicit incomplete records, so database errors and process
replacement cannot make the denominator look better. Per-outcome provenance is
bounded to first/last event identity, count and digest; the full tape remains in
compressed market batches.

Execution-validation hashes are recomputed and bound to one simulation policy
and per-symbol candidate manifests. A calibration subset must contain
non-empty comparable candidates that belong to the validated manifest for the
same symbol. Version-one validation artifacts remain valid only for their exact
original symbol and candidate population. Only historically passive entry
orders filled within the same cancellation window contribute ground truth.
Aggressive simulation is never promoted using passive broker outcomes.

### September 14 collection result

The reviewed collector release is commit
`455760b0e04b643d6b703724fa2740c71ad9f818`. Render deployed it as event
deployment `dep-dak3svad0e5s738jui5g` and dashboard deployment
`dep-dak3sv8jo6nc73bddrq0`. The worker returned with a fresh matching lease,
the same paper-only cohort and the pinned `no_support` model. Its execution
inventory was empty with no unresolved or unowned orders.

The corrected one-off backfill `job-dak3uijm8hqs739amoa0` succeeded without
calling the broker or acquiring the execution lease. It reconstructed 229
historical candidates into 458 counterfactual outcomes and 595 current
candidates into 1,190 outcomes, all of which were persisted idempotently.

The historical passive simulator did **not** validate. Of 227 comparable
passive candidates, it predicted no fill for every case while 52 actual orders
filled inside the comparable window: 39 false negatives among 168 BTC/USD
candidates and 13 among 59 ETH/USD candidates. Aggregate precision was zero;
the zero false-positive rate does not offset the absence of any predicted
positive. These outcomes therefore remain unsupported counterfactuals.

The current outcome population also failed completeness requirements. BTC/USD
momentum had 22 complete aggressive outcomes out of 344 and zero complete
passive outcomes; ETH/USD momentum had zero complete outcomes out of 249 for
either action. Most failures were caused by horizon quote freshness, a changed
passive touch before simulated arrival, action expiry, or missing observed exit
depth. Sparse reversion cells had one candidate per symbol.

Consequently the model remains `no_support` with zero accepted calibration
samples. The estimator returned no finite support date for the required passive
cells: merely leaving this exact no-order collector running cannot validate its
own fill model. After a separately reviewed source of exact-policy execution
ground truth exists, the predeclared calendar gate still imposes a seven-day
minimum; a practical planning range is 14-30 calendar days for enough complete
multi-regime evidence, with no guarantee that held-out net edge will be
positive.

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
prompt. Release `25cd90acb538b3ac83c435b748acf2b11ea7946f` is running on the
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

At the final observation the authenticated market stream had received its
initial BTC and ETH books, but both snapshots were older than the five-second
feature freshness bound and awaited a current update. The order-update stream
was authenticated and subscribed with no gaps. The runtime therefore reported
`waiting_for_market_data`, correctly created no decision, cycle or order, and
did not substitute stale snapshot prices.

The release and postdeploy evidence are stored under
`research/results/v30-action-value-20260911-*`. No automation or follow-up wake
was created.
