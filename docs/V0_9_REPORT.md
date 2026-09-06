# TradeAgent v0.9.0 final report

## Decision

**D. No demonstrated edge.**

The original squeeze's provisional execution model was materially too expensive for the
tested $10-$25 order size, but that did not rescue the strategy family. The unchanged
five-minute rules failed on five additional years and 21 ETFs, the unchanged 20-bar
session-reset rules generated no 30-minute or hourly signals, no lower-turnover
configuration passed the existing gates, and ML was worse than its simple economic
baseline. Autonomous entry remains disabled and both sealed holdouts remain unopened.

## Research freeze

`research/freezes/v0.8.0.json` binds the v0.8 code commit, strategy set, parameters,
datasets, holdout hashes, reports, and six-row experiment ledger. Neither holdout has an
opening marker. This milestone did not tune a frozen configuration or use either holdout.

## Data acquired

| Data | Source | Coverage | Identity |
| --- | --- | --- | --- |
| Adjusted bars | Alpaca Market Data API, `feed=sip` | 2020-01-02 through 2024-12-31; 21 ETFs; daily, hourly, 30-minute, and five-minute | 84 files, 4,092,548 rows; manifest `415c11cea239745e7fe34f98ee99534fd5751cf518b425aff5085b25e15cb004` |
| Historical quotes | Alpaca SIP | +/-2 seconds around all 60 signal/submission anchors for the 15 full-development squeeze trades | 46,404 records |
| Historical trades | Alpaca SIP | Same 60 anchors | 10,915 records |
| Forward bars, quotes, and trades | Alpaca IEX WebSocket | Continuous hosted shadow recorder | PostgreSQL migration `0005_market_evidence` |
| Paper order lifecycle | Alpaca paper API and PostgreSQL audit records | Submission, fill, partial fill, reject, reconciliation | Shadow only; no autonomous entries |

The predefined universe is SPY, QQQ, IWM, DIA, XLB, XLC, XLE, XLF, XLI, XLK, XLP,
XLRE, XLU, XLV, XLY, SHY, IEF, TLT, GLD, EFA, and EEM. Every ETF passed the preregistered
$5 minimum price and $50 million median daily dollar-volume requirements. The lowest
observed median was $184.86 million (XLRE), and the lowest close was $9.179 (XLE).
Individual stocks were not added, so point-in-time index membership was not needed.

## Quote coverage and cost model

Historical SIP entitlement was explicitly tested and available. All 60 original squeeze
anchors have at least one quote before and after the timestamp; the minimum anchor had 108
quotes and 24 trades. No quote was fabricated.

| Full-development squeeze, 15 trades | Provisional bar model | SIP top-of-book market model |
| --- | ---: | ---: |
| Gross edge | $0.133693 | $0.139046 |
| Spread | $0.037506 | $0.009768 |
| Slippage | $0.150025 | $0.000000 beyond displayed best quote |
| Broker commission | $0.075000 | $0.000000 |
| Total execution cost | $0.262531 | $0.009768 |
| Net edge | -$0.128838 | $0.129277 |
| Date-clustered Sharpe | not the walk-forward statistic | 4.631 on only 15 trades |

The estimated model charged 7 bps per round trip: 1 bp spread, 4 bps slippage, and 2 bps
commission. Its $0.262531 charge was 196.37% of gross edge. Slippage contributed 57.15%
of estimated cost, commission 28.57%, and spread 14.29%. Forced-flatten execution was
$0.043765, a subset of those costs rather than an additional charge.

For the tiny tested quantities, displayed SIP size covered every market simulation.
Observed spread cost was 96.28% below the provisional total. Signal-to-submission price
movement was favorable by $0.101594 in aggregate, but it is reported separately and is
not assumed repeatable. Historical quotes cannot establish hidden depth or market impact.
Alpaca's direct stock/ETF commission is zero; future regulatory fees still need statement
reconciliation.

The conservative marketable-limit model fixed each limit to the decision-time opposite
quote. Only 5 of 15 round trips filled completely; 10 missed, and missed trades contributed
zero. Filled limit trades produced $0.036405. A bar touching a limit never caused a fill.

### Why Sharpe collapsed from 3.827 gross to -1.601 net

The 13-trade walk-forward result had less gross edge than the fixed bar-cost model charged.
The full-development analogue shows the mechanism exactly: $0.133693 gross minus
$0.262531 estimated execution cost equals -$0.128838 net. The loss was therefore driven
by the assumed 4 bps slippage and 2 bps commission, not an accounting discrepancy.
SIP evidence shows that model was too conservative at this order size, but 15 trades are
far too few to promote, and the external test below rejects the underlying edge.

## Frozen squeeze external validation

The complete preregistered matrix contains 63 cells: 21 instruments times five-minute,
30-minute, and hourly decisions. It used the exact frozen strategy and 63-trial
multiple-testing penalty.

| Timeframe | Cells | Walk-forward trades | Positive full gross P&L | Positive full net P&L | Gross P&L | Net P&L | Result |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 5-minute | 21 | 1,331 | 14 | 0 | $6.858729 | -$34.582793 | Failed |
| 30-minute | 21 | 0 | 0 | 0 | $0 | $0 | Failed |
| Hourly | 21 | 0 | 0 | 0 | $0 | $0 | Failed |

SPY's five-minute gross walk-forward Sharpe was -0.339 and net Sharpe was -2.303 over
115 closed trades. The best descriptive cell was XLB, with gross Sharpe 1.600 and net
Sharpe 0.432, but it did not qualify after the complete matrix and unchanged gates. Net
P&L was negative in every five-minute instrument and in every calendar year from 2020
through 2024.

The exact strategy clears its 20-bar state at every session boundary. A regular session
contains only 13 30-minute bars or seven hourly/partial-hour bars, so those frozen rules
cannot form a signal at the longer cadences. Changing that lookback would be retuning and
was prohibited. The top 13 external five-minute trades contributed 68.64% of aggregate
gross edge, confirming material trade concentration. The family is retired.

## Lower-turnover results

All 30 configurations were fixed before results: ten per family. Entries required
projected movement to exceed the 7 bps round-trip estimate plus a 3 bps uncertainty
buffer. Results used temporal folds, a 21-day embargo, date clustering, contiguous-block
bootstrap, DSR, and PBO.

| Family | Net-positive configs | Best descriptive configuration | Full trades | Gross P&L | Net P&L | Edge/cost | DSR | Candidate PBO | Family PBO | Qualified |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Time-series momentum | 8/10 | 126-day lookback, 21-day skip/rebalance | 676 | $958.83 | $873.89 | 11.29 | 0.551 | 1.000 | 0.667 | No |
| Relative-strength rotation | 9/10 | 252-day lookback, top five monthly | 441 | $635.89 | $592.73 | 14.73 | 0.355 | 1.000 | 0.167 | No |
| Regime swing mean reversion | 8/10 | 40-day, -1.0 z, 10-day maximum | 292 | $1,071.87 | $915.87 | 6.87 | 0.163 | 1.000 | 0.167 | No |

These are descriptive best rows, not selected candidates. Momentum and relative strength
failed DSR/PBO; swing mean reversion also failed its benchmark, execution stress, and
walk-forward trade-count gates. Their costs remain provisional because SIP event-window
calibration was completed for the frozen squeeze, not thousands of lower-turnover fills.
No configuration was frozen as a paper candidate.

## Experiment count

- Durable experiment ledger: 66 rows total: six frozen v0.8 rows plus 60 v0.9 execution
  records. The 60 comprise the same 30 predefined configurations before and after the
  temporal-block bootstrap correction; no extra configuration was introduced.
- Unique v0.9 research cells/configurations: 97: 63 frozen squeeze cells, 30
  lower-turnover configurations, three Ridge hyperparameters, and one simple ML baseline.
- Execution calibration additionally compared two predefined fill styles: market and
  decision-price marketable limit.

The one historical `qualified` ledger row is an old buy-and-hold benchmark, not a candidate
promotion. No current candidate is qualified.

## ML eligibility

The expanded panel generated 14,202 candidate events, 7,564 positive net-edge events, and
five independent calendar years. ML therefore passed the minimum eligibility counts.
Three preregistered Ridge regressions predicted five-day net return in basis points after
7 bps estimated cost and traded only above a 3 bps uncertainty buffer.

The simple cost-aware threshold averaged -4.146 bps. The best Ridge attempt averaged
-14.127 bps, its improvement confidence lower bound was -23.620 bps, and model-family PBO
was 0.333. It failed baseline improvement, confidence, DSR, and PBO gates. **ML remains
disabled.**

## Shadow collector status

The Render shadow worker remains one-instance, kill-switch active, and incapable of
autonomous entry. It now records IEX bars, quotes, and trades with feed source, exchange
timestamps, local receipt/processing timestamps, bid/ask sizes, and trade identifiers.
The PostgreSQL migration completed successfully. A separate feed heartbeat labels the
stream `healthy`, `stale`, or `market_closed`; an open-session stale feed activates the
kill switch. Each theoretical signal records expected cost/return floors and one-, three-,
and six-frame decay. The dashboard exposes the worker and feed status.

At release time the market was closed, so the newly migrated live tables had zero new
records while the worker, reconciler, notifier, and news heartbeats were current. Paper
submission/fill fields, partial fills, rejects, and reconciliation remain wired but no
autonomous strategy order is allowed.

## Qualification decisions

| Item | Decision |
| --- | --- |
| Frozen volatility squeeze | Retire family |
| Multi-day time-series momentum | Reject; DSR/PBO failed |
| Relative-strength rotation | Reject; DSR/PBO failed |
| Regime-conditioned swing mean reversion | Reject; benchmark/stress/count/DSR/PBO failed |
| Economic ML | Eligible to evaluate, rejected and disabled |
| Autonomous paper entry | Disabled |
| Sealed holdouts | Unopened |
| Final project outcome | **D. No demonstrated edge** |

## Files changed

- Freeze and evidence: `research/freezes/*`, `research/datasets/*`,
  `research/results/*`
- Market data/runtime: `alpaca.py`, `alpaca_stream.py`, `persistence.py`,
  `runtime.py`, `worker.py`, `live_shadow.py`, `api.py`, `cli.py`,
  `migrations/versions/0002_normalized_market_data.py`, and
  `migrations/versions/0005_market_evidence.py`
- Research: `research_dataset.py`, `diagnostics.py`, `research.py`,
  `portfolio_research.py`, `squeeze_external.py`, `lower_turnover.py`,
  `lower_turnover_research.py`, `economic_ml.py`, `execution_evidence.py`, and
  `execution_calibration.py`
- Tests: the corresponding Alpaca, stream, persistence, worker, runtime, shadow,
  dataset, freeze, squeeze, lower-turnover, economic-ML, evidence, calibration, API,
  diagnostics, and research tests
- Documentation/release: `README.md`, `docs/OPERATIONS.md`, `docs/ROADMAP.md`,
  this report, `pyproject.toml`, `src/tradeagent/__init__.py`, and API metadata

## Commands run

```powershell
tradeagent build-v09-dataset
tradeagent lower-turnover-research
tradeagent economic-ml-research
tradeagent squeeze-external --workers 12
tradeagent intraday-diagnostics --strategy volatility-squeeze --output ...
tradeagent collect-diagnostic-evidence --diagnostics ... --timeframe 5Min ...
tradeagent calibrate-diagnostic-execution --diagnostics ... --timeframe 5Min ...
alembic upgrade head
alembic downgrade base
alembic upgrade head
python -m ruff check .
python -m ruff format --check .
python -m mypy src\tradeagent
python -m pytest --cov=tradeagent
```

Render deployments and `/health` plus `/api/runtime` checks were also run. Every generated
dataset, evidence archive, and result has a tracked hash or tracked report. The final suite
passed 183 tests with 85.51% branch-aware coverage; Ruff, formatting, strict mypy, and the
full upgrade/downgrade/upgrade migration chain passed.

## Remaining blockers

1. No strategy has passed all unchanged qualification gates.
2. Event-window SIP calibration covers the original squeeze completely, but the
   lower-turnover and expanded external fills still use provisional costs.
3. Hidden depth, market impact, regulatory fees, and live adverse selection require
   forward observations and controlled paper calibration; historical top-of-book alone
   cannot prove executable profitability.
4. The 60-trading-day and 60-reconciled-round-trip forward gate cannot start without a
   qualified frozen candidate.

## Exact next action

Keep autonomous entry disabled and run the deployed quote/trade/bar shadow recorder for
60 trading days to build an independent observed-cost dataset. Do not reopen these
families or holdouts; preregister a genuinely new hypothesis only after that dataset is
available.
