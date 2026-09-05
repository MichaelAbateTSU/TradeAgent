# Backtest and strategy audit report

## Executive conclusion

The original “no qualified strategy” decision was directionally safe, but several
reported metrics were not scientifically reliable because of shared timestamp,
fold-boundary, liquidation, and accounting defects.

Those defects are fixed. After repair:

- noise-area and Donchian strategies show small positive gross P&L but execution costs
  overwhelm it;
- volatility squeeze shows positive gross P&L in the full development replay but has too
  few trades and negative net P&L;
- the intended one-minute academic noise-area replication is negative before costs;
- the meta-label dataset contains too few positive net-edge examples to train safely;
- no strategy qualifies;
- both sealed terminal holdouts remain unopened.

## Baseline audit

The audit covered data ingestion, timestamps, sessions, features, signals, delayed
execution, fills, costs, accounting, benchmarks, walk-forward folds, bootstrap, DSR/PBO,
holdouts, OMS, persistence, worker leases, and deployment.

## Confirmed bugs fixed

### Historical/live timestamp mismatch

Alpaca historical intraday bars were start-labeled while live aggregated bars were
end-labeled. Historical 5-minute decisions therefore appeared five minutes before their
closing information was available.

**Fix:** `1Min`, `5Min`, and `1Hour` historical bars are normalized to interval-close
time. Daily bars preserve their session label.

### Intraday folds split sessions

Walk-forward frame counts began and ended folds in the middle of sessions. Session open,
VWAP, and intraday state could initialize from a partial day.

**Fix:** intraday walk-forward windows use complete exchange sessions. Incomplete fold
configurations fail explicitly.

### Positions remained open at fold end

Some folds reported final positions and omitted liquidation costs.

**Fix:** portfolio runs force exactly one terminal liquidation and include its costs.
Every repaired intraday fold ends flat.

### Delayed intents could enter after the cutoff

A delayed buy could become actionable during manage-only or flatten windows.

**Fix:** delayed strategies suppress risk-increasing intents outside entry time and emit
an immediate zero-target intent during flatten.

### Realized P&L omitted entry commission

Cash/equity were correct, but position-level realized P&L deducted only exit commission.

**Fix:** entry commission is allocated through partial exits and included in realized
P&L. State export/recovery preserves the remaining entry commission.

### Session-close bar was dropped

After normalizing to close timestamps, the bar ending exactly at 4:00 PM was considered
outside the session.

**Fix:** an event timestamp equal to official session close is a flatten event. Later
timestamps are closed.

### Worker leases could become orphaned

Rolling deployments could leave a durable lock owned by a terminated instance, and an
old process did not stop if lease refresh failed.

**Fix:** unique platform instance IDs, renewable leases, stale takeover, and fail-closed
lease-loss handling.

### Statistical trial accounting was incomplete

PBO used interleaved modulo partitions, and DSR was configured for three trials despite
a larger strategy family.

**Fix:** PBO uses contiguous temporal blocks. Intraday DSR uses the preregistered
24-trial budget.

## Verified-correct components

- UTC normalization and official XNYS holidays/early closes.
- Aligned universe timestamps and duplicate/out-of-order rejection.
- Current observation excluded from historical noise estimates.
- Donchian channel excludes the signal bar.
- Spread, slippage, and commission each enter once.
- 2×/3× costs are separate scenarios, not stacked onto the base result.
- Cash plus marked positions equals equity.
- Risk-reducing exits remain available under kill switch.
- Broker reconciliation and lost-ack lookup are idempotent.
- Holdout hashes are verified; no holdout-open audit marker exists.
- Hosted order execution remains disabled.

## Data-quality report

Corrected close-labeled SPY/QQQ five-minute intersection:

| Measure | SPY | QQQ |
| --- | ---: | ---: |
| Observations | 32,564 | 32,564 |
| Sessions | 419 | 419 |
| Expected observations | 32,574 | 32,574 |
| Missing bars | 10 | 10 |
| Missing rate | 0.0307% | 0.0307% |
| Incomplete sessions | 14 | 14 |
| Median bars/session | 78 | 78 |
| Duplicate timestamps | 0 | 0 |
| Out-of-order rows | 0 | 0 |
| Zero-volume rows | 0 | 0 |
| Greater-than-10% jumps | 0 | 0 |

Limitations:

- no historical quote/spread coverage in the bar files;
- one adjusted vendor, without independent corporate-action validation;
- no delisted-universe coverage;
- no queue-position or market-impact observations.

## Gross versus net attribution

Full 8,000-frame development replay after repairs:

| Strategy | Trades | Gross P&L | Execution cost | Net P&L | Net expectancy | Profit factor |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Noise area | 78 | +$0.177 | $1.365 | -$1.188 | -$0.0152 | 0.563 |
| Donchian/ATR | 79 | +$0.227 | $1.383 | -$1.156 | -$0.0146 | 0.567 |
| Volatility squeeze | 15 | +$0.134 | $0.263 | -$0.129 | -$0.0086 | 0.470 |

The model charges proportional basis-point costs, so increasing paper size does not turn
negative expectancy positive. Noise area and Donchian are **execution/capacity
failures** under the current conservative cost assumptions. Squeeze is too sparse to
support an inference.

Ten seeded random-timing controls with comparable trade frequency averaged:

- 71.8 trades;
- gross P&L: -$0.143;
- net P&L: -$1.399;
- positive net runs: 0/10.

This indicates the repaired primary signals contain more gross information than random
timing, but not enough to monetize under current costs.

## Repaired walk-forward results

All results use complete sessions, terminal liquidation, development data only, one-frame
delay, and the expanded 0×/1×/1.5×/2×/3× attribution suite.

| Strategy | Gross Sharpe | Net Sharpe | Positive folds | Est. closed trades | DSR probability | Qualified |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Noise area | 0.660 | -4.277 | 25.0% | 67 | 9.4% | No |
| Donchian/ATR | 0.982 | -3.320 | 37.5% | 65 | 9.3% | No |
| Volatility squeeze | 3.827 | -1.601 | 12.5% | 13 | 16.6% | No |

Gross Sharpe alone is not usable edge. None has positive net expectancy, enough trades,
or acceptable DSR.

## Faithful one-minute noise-area replication

The original strategy was only an approximation. The repaired replication now uses:

- one-minute close-labeled SPY bars;
- 14 prior completed sessions for each minute slot;
- gap-adjusted upper boundary using session open and prior close;
- 30-minute decision checkpoints;
- session-reset VWAP;
- boundary/VWAP failed-breakout exit;
- complete-session folds and terminal liquidation.

A new one-minute holdout was sealed after timestamp repair:

- total prospective frames: 50,000;
- development: 40,000;
- unopened holdout: 10,000;
- holdout begins July 31, 2026 at 15:47 UTC;
- dataset hash:
  `0c33c86dbb001ef02c15a0ee4becde399d98f051f1b09cc178d3b8c5b187fa5b`.

Development result:

| Gross Sharpe | Net Sharpe | Positive folds | Est. trades | DSR | Qualified |
| ---: | ---: | ---: | ---: | ---: | --- |
| -0.671 | -6.844 | 31.3% | 66 | 3.3% | No |

The cited strategy does not replicate positively in this long-only, fractional,
IEX-data implementation. The holdout remains unopened.

## Meta-labeling result

The deterministic trend-pullback generator produced:

- 406 point-in-time candidate events;
- only 11 positive labels after next-frame execution, 3× round-trip costs, and the
  required net edge;
- 395 negative labels.

The calibrated logistic pipeline refuses to train unless it has at least 200 events,
both classes, and at least 20 positive net-edge examples. Result:

```text
qualified_filter = false
reason = meta-labeling requires at least 20 positive net-edge labels
```

This is a signal-generation failure, not a model-selection problem. A classifier cannot
recover an edge when positive post-cost outcomes are almost absent.

## Qualification decisions

No tested strategy or meta-label filter qualifies. No promotion record should be
created, and autonomous paper entry must remain disabled.

Earlier intraday performance tables in `RESEARCH.md` are retained as experiment history
but are superseded by this audit because they used start-labeled bars and frame-split
folds.

## Reproduction

```powershell
tradeagent data-quality `
  --symbols SPY,QQQ `
  --universe-directory data\intraday

tradeagent intraday-evaluate `
  --strategy noise-area `
  --symbols SPY,QQQ `
  --universe-directory data\intraday `
  --holdout-manifest data\intraday-holdout.json

tradeagent intraday-diagnostics `
  --strategy noise-area `
  --symbols SPY,QQQ `
  --universe-directory data\intraday `
  --holdout-manifest data\intraday-holdout.json

tradeagent meta-label-evaluate `
  --symbols SPY,QQQ `
  --universe-directory data\intraday `
  --holdout-manifest data\intraday-holdout.json
```

## Next highest-value experiment

Do not add another indicator. Collect live quote spreads and normalized one-minute bars
until historical spread coverage is adequate, then test whether the small gross
Donchian/noise edge survives **observed** execution rather than a generic 7-bps round
trip assumption.

In parallel, broaden the economically distinct candidate-event generator to create at
least 20 positive net-edge labels without changing the frozen label threshold. Examples
must come from new development data or a preregistered hypothesis, not holdout-guided
tuning.

