# Offline signal/horizon research

`tradeagent scalp-signal-research` reads recorded signals and market tape.
It never constructs a broker client, takes an execution lease, changes
configuration, writes database evidence or selects a model. Run it in an
isolated research process, not inside the active execution worker.

## Inputs and bounded execution

Database mode requires one immutable run configuration across the explicit
cohort. It includes **all retained `scalp_candidate_decision` events** in
the declared period, not just filled orders or winning signals. Non-entry
signals remain present with exclusions. Exact neutral features cannot be
recovered because the runtime aggregated neutral hold ticks.

The reader projects compact predecision fields instead of loading complete
economic-model JSON repeatedly. Tape is streamed in small batches; encoding,
content hash and event count are checked before reconstruction with the
existing book engine. Input budgets fail explicitly and do not authorize
partial-population training. Local continuity changes censor spanning labels.

Example for the completed September cohort (dates in UTC):

```powershell
tradeagent scalp-signal-research `
  --cohort-id v30-action-value-20260921-experiment-r1 `
  --start 2026-09-21T00:00:00+00:00 `
  --end 2026-09-28T14:40:07+00:00 `
  --split-at 2026-09-25T00:00:00+00:00 `
  --at 2026-09-30T00:00:00+00:00 `
  --output-dir research\results\signal-horizon-20260930-r1
```

This is an explicit **post hoc diagnostic split of known data**, not a
sealed test or prospective validation. A 900-second boundary embargo prevents
cross-half horizon overlap. Adjacent observations within halves can still
overlap and are not independent portfolio trades.

Offline file mode replaces `--cohort-id` with `--signals-jsonl FILE` and
`--quotes-jsonl FILE`. Each signal is a strict `ResearchSignal` (event/run/
decision identity, symbol, family, original selected action, decision time,
score, predecision quote and feature snapshot); each quote record is a
strict `ResearchTick` (event identity, symbol, receipt time, continuity ID,
and an optional `ScalpQuote`). A null quote denotes missing/invalid evidence.
Never forge quote times or dummy features to fit the schema.

Defaults cap the population at 10,000 signals, 16 MiB of compact signals,
10 million tape records and 8 MiB decompressed per market batch. A larger
population needs an explicit reviewed isolated-process budget, not truncation.
Existing output directories are refused to preserve previous attempts.

## Label and cost rules

- The arrival deadline is decision + 350 ms, with the original ask + two-bps
  cap. The latest quote already received by that deadline establishes a
  hypothetical ask entry only with sufficient displayed size.
- For each 1/5/15/30/60/300/900-second decision-relative horizon, use the
  latest quote already received at/before the deadline, never a later
  favorable print. Exchange and receipt ages must be at most one second;
  missing, stale, capped-out, undersized or continuity-invalid labels are
  censored with a reason, not zero returns.
- Entry notional is fixed at $10.25. Entry fees withhold 25 bps of base
  inventory and exit fees withhold 25 bps of sale cash multiplicatively.
  They cannot underprice the frozen account fee configuration. Spread and
  quote-price slippage are embedded in ask-to-bid returns, not charged twice.
- Midpoint forward returns and direction metrics have their own coverage;
  they are not executable payoffs. Exit signals do not become crypto shorts.
  No OHLC touch, hidden liquidity, queue fill or news correlation is invented.

## Outputs and interpretation

`markouts.jsonl` contains all signal/horizon rows, including missing labels,
with source IDs, times, original qualified action, hypothetical long family,
entry/exit evidence, regime buckets and gross/net outcomes. `report.json`
contains the input/policy manifest, hashes, missingness and:

- Win rate after costs, mean return/winner/loser and losing tail.
- Fixed symbol/family, volatility, spread, UTC time, trend and observed-
  trade-volume buckets; absent features occupy an `unknown` bucket.
- Midpoint long-direction accuracy, Pearson and tie-aware Spearman score/
  return correlations; constant scores or insufficient samples return null.
- A descriptive **nonannualized mean/std ratio**, explicitly not a portfolio
  Sharpe. Overlapping hypothetical outcomes are never summed into a
  fictional trading portfolio.
- UTC-day-block bootstrap intervals where five days exist; these are
  exploratory, not selection/dependence-corrected qualification.
- Paired 30/60/300/900 versus five-second results using only the same complete
  signals. Population/coverage changes remain visible.
- Discovery-only top-decile descriptions for each horizon, even when all
  top-decile net outcomes are negative. Later diagnostic winners are never
  used to select the apparent best entry filter.
- Fixed H2-60 filter results for the proposed high-volatility momentum
  expansion; eligibility is a diagnostic, not an authorization.

The seven horizons and all regime comparisons count as exploration. A
positive cell does not automatically identify a profitable strategy. Freeze
the new prospective population and uncertainty protocol before attempting a
confirmatory conclusion; do not reuse this known September period as an
untouched test. The command always returns `promotion_allowed: false`.
