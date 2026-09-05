# Profitability strategy development plan

## Objective

Develop a selective, low-turnover intraday strategy with positive expected paper return
after spread, slippage, fees, latency, and missed fills.

Profitability is an empirical target, not a promise. If no candidate satisfies every
gate, TradeAgent remains in cash and continues shadow research.

## What the failed baselines taught us

The initial opening-range, VWAP-reversion, and regime-momentum rules all failed. Their
main lesson is not that every intraday strategy is impossible; it is that unconditional
rules trade too often when their expected edge is smaller than realistic execution cost.

The next strategy must:

- predict **whether a specific setup is worth taking**, not predict every bar;
- trade at most one ETF at a time and no more than two round trips daily;
- combine slow market regime with short-horizon setup confirmation;
- estimate net expected return before entry;
- use calibrated probabilities and remain in cash most of the time;
- be trained and evaluated only on the sealed development segment;
- consume a fixed, recorded trial budget.

## Primary hypothesis

Build a **regime-filtered trend-pullback strategy with classical ML meta-labeling**.

The deterministic setup identifies a liquid ETF that:

1. is in a positive daily and 60-minute regime;
2. leads the ETF universe on volatility-adjusted relative momentum;
3. pulls back toward session VWAP without breaking the trend;
4. resumes short-horizon positive momentum;
5. has acceptable spread, volatility, and relative volume.

A calibrated model then predicts whether the setup is likely to produce a positive
30-minute return after 3× modeled round-trip cost. The model can reject a setup; it
cannot create one, size an order, or bypass risk.

This is a meta-labeling problem:

```text
deterministic setup -> candidate event -> probability of net-positive outcome
                    -> take or skip
```

It is deliberately narrower than raw price prediction.

## Universe and cadence

- SPY, QQQ, IWM, TLT, GLD.
- 5-minute bars and point-in-time quotes.
- Decisions every 15 minutes from 10:00 AM through 3:30 PM ET.
- Regular NYSE sessions only.
- Long-only.
- One active position initially.
- Maximum two closed round trips per day.
- Mandatory flatten by 3:50 PM ET.

## Entry setup

A symbol becomes a candidate only when every deterministic condition passes:

| Filter | Initial rule |
| --- | --- |
| Daily regime | Previous close above 20-day moving average |
| 60-minute momentum | Positive and top two in the universe |
| 30-minute momentum | Positive |
| Pullback | Price touched or approached session VWAP in previous 3 bars |
| Resumption | Latest 15-minute momentum is positive |
| Relative volume | At least 1.0× prior-session time-slot average |
| Spread | At most 5 bps |
| Realized volatility | Between 8% and 40% annualized |
| Time | 10:00 AM–3:30 PM ET |
| Expected net edge | Greater of 15 bps or 3× expected round-trip cost |

If several symbols qualify, select the highest model probability and then the strongest
volatility-adjusted relative momentum.

## Meta-label and model

### Label

For each deterministic candidate at time `t`:

```text
forward_return = close[t + 6 bars] / executable_price[t + 1 bar] - 1
net_return = forward_return - stressed_round_trip_cost
label = 1 if net_return > required_edge else 0
```

The entry price uses the next bar plus spread and slippage. Labels never cross session
boundaries.

### Point-in-time features

- 15-, 30-, and 60-minute returns;
- cross-sectional momentum ranks;
- distance from session VWAP;
- pullback depth and recovery strength;
- current and lagged relative volume;
- current spread and spread percentile;
- 30- and 60-minute realized volatility;
- market breadth across the ETF universe;
- SPY market-regime features;
- time of day;
- distance from opening range;
- previous-session gap;
- candidate's modeled round-trip cost.

No feature may use a revised future value, incomplete bar, current-day close, or future
universe membership.

## Recent news awareness

The hosted agent must continuously ingest recent market news so its decisions have
current context. News begins as a **risk and setup filter**, not an unconstrained
directional trading signal.

### Initial sources

- SEC EDGAR filings and trading-suspension notices;
- issuer investor-relations releases;
- exchange halt/status feeds;
- Federal Reserve, BLS, BEA, and Treasury calendars/releases;
- a licensed timestamped market-news feed before production paper promotion.

Every item stores source, source URL, symbol/entity mapping, original publication time,
first received time, update time, content hash, and revision lineage. Duplicate,
backfilled, revised, or future-dated items are quarantined.

### Decision-time news features

- minutes since latest relevant headline;
- scheduled high-impact event proximity;
- earnings/filing/event category;
- source reliability class;
- novelty versus recent headlines;
- point-in-time sentiment and uncertainty;
- cross-source confirmation count;
- whether the item arrived before the decision cutoff.

The first behavior is conservative:

- disable new entries around FOMC, CPI, payroll, and other configured macro releases;
- disable affected symbols around halts, unresolved filings, and breaking issuer events;
- widen the required expected-edge threshold during high news uncertainty;
- attach relevant headline references to shadow decisions and round-trip reports.

### LLM boundary

An LLM may summarize retrieved news for the dashboard and explain why a deterministic
filter blocked or allowed a setup. It cannot:

- generate an order;
- alter a target or probability;
- disable a news blackout;
- change risk limits;
- promote a strategy;
- execute instructions embedded in article text.

All external content is untrusted data. Strip active content, ignore embedded
instructions, limit retrieved text, preserve source citations, and expose only a typed
summary schema to the rest of the system.

### Point-in-time validation

Historical testing must use the first publication/receipt timestamp, not the latest
revised article. Evaluation adds delayed receipt, missing-feed, duplicate-headline,
incorrect-symbol, and sentiment-failure scenarios. A news-derived feature is eligible
only if the same feed and latency are available in hosted paper operation.

If the news feed is stale, disconnected, or reports an unresolved high-impact event, the
agent fails closed for new entries while preserving risk-reducing exits.

### Models

Champion candidate:

- regularized logistic regression;
- standardized numeric features;
- class weights fixed from training data;
- isotonic or sigmoid probability calibration fitted inside each training fold.

Single challenger:

- histogram gradient-boosted trees;
- shallow depth and explicit regularization;
- calibrated probabilities.

Deep learning and reinforcement learning remain out of scope.

## Trial budget

The full experiment budget is fixed before results:

- 4 deterministic setup variants;
- 2 model families;
- 3 probability thresholds;
- maximum 24 candidate configurations.

Every trial is recorded. Deflated Sharpe uses at least 24 trials, even if fewer runs
finish. No additional variants may be added after inspecting the terminal holdout.

## Development data

1. Use the existing 8,000-frame development partition.
2. Leave the 2,000-frame terminal holdout unopened.
3. Add earlier historical 5-minute data for model training where point-in-time quality is
   acceptable.
4. Keep Render's new normalized live bars and quotes as forward-only evidence.
5. Cross-check corporate actions and sessions against an independent vendor before any
   final promotion.
6. Do not combine hosted forward observations into historical training until their
   collection period is complete and versioned.

## Validation design

### Nested chronological evaluation

- Outer rolling walk-forward folds estimate performance.
- Inner purged/embargoed folds select regularization and probability threshold.
- Purge at least the 30-minute label horizon.
- Embargo at least one full decision interval.
- Fit scalers, imputers, calibration, and model only inside each training fold.
- Freeze the selected configuration before forward shadow or holdout evaluation.

### Execution simulation

Every result includes:

- next-bar execution;
- current bid/ask spread;
- directional slippage;
- 1% volume participation cap;
- partial fills;
- missed fills;
- cancel latency;
- one- and two-frame decision delays;
- 1×, 2×, and 3× total costs;
- market halts, early closes, missing bars, and stale quotes.

### Required metrics

- net total and annualized return;
- Sharpe, Sortino, and Calmar;
- maximum drawdown;
- profit factor;
- win rate and payoff ratio;
- turnover and holding period;
- expected versus realized slippage;
- probability calibration and Brier score;
- benchmark-relative fold return;
- bootstrap confidence interval;
- Deflated Sharpe probability;
- Probability of Backtest Overfitting;
- trade and profit concentration.

## Development promotion gates

All gates are mandatory:

| Gate | Requirement |
| --- | --- |
| Closed out-of-sample trades | At least 200 |
| Positive outer folds | At least 60% |
| Net Sharpe | At least 1.0 |
| Sortino | At least 1.25 |
| Profit factor | At least 1.20 |
| Maximum drawdown | At most 5% |
| Benchmark wins | At least 60% of folds |
| Average excess return | Positive |
| 95% bootstrap excess lower bound | Greater than zero |
| Deflated Sharpe probability | At least 95% using 24 trials |
| PBO | At most 20% |
| 3× cost scenario | Positive |
| Two-frame delay scenario | Positive |
| Largest trade contribution | Less than 10% of total profit |
| Probability calibration | No severe fold-level degradation |

Failure of any gate means rejection.

## Exit logic

The first fixed exit policy:

- profit target: 1.0 ATR from executable entry;
- protective stop: 0.6 ATR;
- thesis failure: 30-minute momentum becomes non-positive;
- time stop: 30 minutes;
- hard session exit: 3:50 PM ET;
- stale data or reconciliation failure: risk-reducing market exit and kill switch.

Exit parameters are part of the fixed 24-trial budget.

## Position sizing

During initial autonomous paper:

- $10 minimum;
- $25 maximum;
- maximum 0.25% NAV target;
- one position;
- no leverage or shorting;
- maximum two round trips per day;
- 30-minute symbol cooldown;
- no scale-in;
- no averaging down.

Sizing remains deterministic and outside the model.

## Shadow qualification

After one development candidate passes:

1. sign the exact code, dataset, configuration, and model artifact;
2. deploy it to Render shadow mode;
3. record every candidate, probability, target, risk result, hypothetical fill, and
   outcome;
4. run for at least 20 trading days;
5. require no duplicate orders, no unresolved gaps, and no reconciliation failures;
6. compare shadow performance with backtest confidence intervals;
7. reject on material drift or cost underestimation.

No paper order is submitted during this stage.

## One-time terminal holdout

Open the sealed holdout only if shadow qualification passes.

The one-time evaluation must:

- use the already-frozen strategy and probability threshold;
- make no parameter changes;
- run the complete cost/delay suite;
- satisfy every development gate;
- write the holdout audit marker;
- produce an immutable report.

Failure retires that strategy version. The holdout cannot be used to repair it.

## Autonomous paper qualification

Only after holdout success and explicit promotion:

- enable the Render worker's autonomous-paper mode;
- begin with $10–$25 entries;
- keep the durable kill switch and one-worker lease;
- reconcile on startup and every 60 seconds;
- send exactly one email after each reconciled round trip;
- run for at least 60 trading days and 60 closed trades.

Final paper gates:

- positive net realized P&L;
- paper Sharpe at least 0.75;
- drawdown below 3%;
- profit factor above 1.10;
- no duplicate entry;
- no unresolved reconciliation event;
- no overnight position;
- 100% exactly-once result-email delivery;
- successful restart, disconnect, gap, and provider-failure drills.

## Retraining policy

- No online or intraday self-modification.
- Retrain no more than monthly.
- New data creates a challenger, never silently modifies the champion.
- Champion remains active until challenger passes the full process.
- Feature drift can demote the champion to shadow/cash automatically.
- Only explicit human approval can promote a new strategy version.

## Ordered implementation steps

1. Add a timestamped news/event schema, source provenance, deduplication, and revision
   lineage.
2. Add SEC, issuer-release, exchange-halt, and macro-calendar ingestion.
3. Add freshness monitoring and deterministic news blackout rules.
4. Add typed cited LLM summaries for dashboard context only.
5. Add point-in-time candidate-event and label generation.
6. Add dataset manifests for model features, labels, and news snapshots.
7. Add purged nested walk-forward splitters.
8. Add logistic-regression pipeline and calibration.
9. Add gradient-boosted challenger.
10. Add Brier/calibration and trade-concentration metrics.
11. Add missed-fill, halt, news-delay, and variable-spread replay.
12. Register the fixed 24-trial experiment budget.
13. Run development experiments.
14. Reject or freeze the best passing candidate.
15. Add signed model-artifact storage.
16. Deploy frozen candidate to hosted forward shadow.
17. Run 20-day shadow qualification.
18. Open the terminal holdout once if shadow passes.
19. Promote exact evidence-bound version.
20. Enable minimum-size autonomous paper entries.
21. Run 60-day paper qualification.

## Stop conditions

Stop new research and remain in cash if:

- no candidate beats the benchmark after the fixed trial budget;
- the independent data cross-check fails;
- costs erase the lower confidence bound;
- DSR or PBO fails;
- forward shadow materially underperforms backtest;
- the holdout fails;
- operational failures prevent reliable reconciliation.

The objective is not to force a trading strategy into existence. It is to discover
whether a defensible net edge exists and automate it only if the evidence survives.
