# Shadow Research Dataset v1: prospective observation protocol

**No orders. No model promotion. Prior trading authorization remains expired.**
This protocol is an observation dataset, not a replacement trading cohort.
Its machine-readable, source-release-bound freeze is archived alongside this
document after administrative registration and deployment.

## Fixed window and hypotheses

Dataset: `shadow-research-20261001-v1`. Evaluation window:
**October 1, 2026 00:00:00 UTC to October 15 00:00:00 UTC** (fourteen days).
That is September 30 at 20:00 to October 14 at 20:00 America/New_York.
The last fifteen-minute markouts mature during a fixed tail ending
October 15 at 00:15:02 UTC. Warm-up beforehand is identified separately.
No missed dates are backfilled or counted as evaluated days.

Freeze the protocol hash, code release, schema, account fingerprint,
universe, hypotheses, cost assumptions and source-manifest genesis **before**
the first evaluation. Future raw data cannot be hashed before it exists:
each immutable compressed batch extends the predeclared hash chain, and
daily reports record its current root. The terminal manifest is sealed
after the label tail; analysis independently verifies batch content,
sequence, event counts and referenced quote IDs.

Pre-start safety repairs may be approved only while there are zero
evaluations and the collection clock has not begun. Each approval is an
append-only release event bound to the original protocol hash; the
original code reference and protocol are never rewritten. Captured
evaluations and process-quality snapshots identify the actual approved
source release. No such repair can change thresholds or renew trading.

BTC/USD and ETH/USD only. Every ten-second evaluation for both symbols is
retained, not just positive signals or fills. The existing momentum (0.25)
and liquidity-shock reversion (0.65) rules remain frozen. Each record
contains both scores, the selected family, raw feature values or explicit
unavailability, rejection reasons, quality flags, synchronized bid/ask/
midpoint/spread, L5 depth and its independent clock, and source IDs.
Neutral/no-supported-family and near-miss observations are not aggregated
away. Near-miss is a research description of a score between 80% and 100%
of the existing threshold, not an entry permission.

## Price and missingness contract

Native L1 and valid reconstructed L2 are separately identified. A failed
L2 delta must not destroy an already received native quote. Its original
exchange, receipt and processing times are preserved: repeating a cached
quote does **not** make it fresh. Depth can be stale while native L1 is
current; this never grants trading permission to the L2 strategy.

Quote history is bounded to sixteen minutes and 60,000 records per symbol.
Evaluation inputs must already be available at decision time. Forward
labels use the most recent quote **received at or before** decision plus
5, 30, 60, 300 or 900 seconds. Exchange and receipt ages must be at most
two seconds, the predeclared current paper data fence. Outcomes settle
after two seconds, without using a quote received after the deadline.

Primary returns are long **ask-to-future-bid**, with short
**bid-to-future-ask** recorded only as a research counterfactual.
Crypto short execution, borrowing and funding are unavailable and are not
authorized. Positive sizes, displayed notional and continuity are checked.
Missing, stale, undersized, reset-spanning and restart-unavailable labels
remain missing with reasons; they are never interpolated or replaced by
zero. A restarted collector resumes unresolved immutable evaluations,
never replays or invents their decisions.

## Costs and analysis order

The actual account fee tier is **unverified unless an independent evidence
reference is supplied before freeze**. Record that unknown separately from
the assumptions: maker 15 bps, conservative entry 25 bps, exit 25 bps,
assumed marketable GTC limit and $10.25 notional. There is no actual order.
The configured paper adapter does not establish actual fee-role attribution
or tier merely by exposing account buying power.

Base-inventory entry and cash exit allowances are multiplicative. Report
spread as embedded in ask-to-bid pricing, never deduct it again.
Predeclared additional execution-slippage reserve is 5 bps and latency
penalty is 3 bps. One- and 3.5-second arrival-price sensitivities use
causal delayed quotes inside the original two-bps cap; their observed
price movement replaces, rather than duplicates, the scalar latency penalty.
Extra 5-bps slippage and 10-bps fee stress are separate diagnostics.

Profitability analysis is blocked while the dataset is open or if either
symbol lacks **95% primary 60-second coverage** over the entire expected
grid, including missed evaluations, or has unexplained timestamp reversals.
The manifest/reference integrity audit must also pass. Other horizons,
duplicate/stale rates, gaps and latency distributions remain visible even
when profitability is blocked. Coverage is a target, **not a promised result**.

After quality passes:

1. Evaluate gross executable outcomes first. Reject nonpositive gross mean.
2. Compare fixed families and rejected/no-family groups with deterministic
   time-matched random directions, simple momentum and simple mean reversion.
   Unavailable inputs remain unavailable; a short baseline is not a
   broker-available strategy.
3. Require enough non-overlapping episodes per symbol/group (at least 100),
   day-block uncertainty support, a positive lower net bound, gross lower
   bound above additional friction, and gross mean at least 1.5 times that
   additional friction. Embedded spread is not counted twice.
4. Report development versus later validation split at October 8 UTC,
   a 900-second boundary embargo, fixed chronological folds, both symbols,
   volatility/liquidity regimes and cost/latency stresses. No thresholds
   are tuned using validation.

An unverified tier, insufficient episode/day/regime support or failure to
beat trivial baselines is not economic qualification. The screening
command always returns `promotion_allowed: false`. No generated analysis
file can become a worker model or a live-money authorization.

After sealing, the service invokes screening in a separate child process
with a 384-MiB address-space limit and a thirty-minute time budget. Its
quality-blocked result, completed research report, or explicit capacity
failure is archived as an immutable dataset analysis event. No failed
child is treated as successful evaluation or retried until it becomes
favorable. This uses existing compute, not a new paid service.

## Operations and persistence

Use the existing Render event worker, PostgreSQL, dashboard and notifier.
There is one fenced worker lease. The observation service contains no
broker client, order engine or submission path. A separate read-only
deployment preflight confirms the pinned paper account is flat; collector
status does not pretend to query broker positions continuously.

The prior model remains `no_support`. Original cohort controls and
archived evidence are not rewritten. The legacy shadow worker stays
suspended; no new paid resource, subscription or recipient is created.
Quality reports are archived daily in UTC after markout settlement, and
the existing once-daily 18:00 Eastern email switches to an honest
five-paragraph observation/coverage update.

Raw and dataset payloads have a hard 3-GiB storage budget. This is not a
guarantee of filesystem free space: indexes, WAL and maintenance headroom
must be monitored. Budget failure stops the collector visibly and makes
the window incomplete; it does not delete evidence or truncate an
analysis population to obtain a favorable result.

Commands:

```powershell
tradeagent shadow-dataset-freeze --protocol docs\experiments\2026-10-shadow-dataset-v1\frozen-protocol.json
tradeagent shadow-dataset-run
tradeagent shadow-dataset-status
tradeagent shadow-dataset-analyze --output-dir research\results\shadow-alpha-20261015-v1
```

`/api/shadow-dataset` reports the frozen protocol and quality; the dashboard
shows a separate observation section. `/api/shadow-dataset/daily?report_date=YYYY-MM-DD`
serves archived quality reports. Statistical analysis cannot truthfully be
finished before the fixed future window and label tail have elapsed.
