# Research and qualification

TradeAgent separates pipeline validation from claims of market alpha. Synthetic data is
useful for deterministic tests, cost sensitivity, accounting, and promotion logic. It
cannot establish profitability because it does not contain real price formation,
survivorship effects, corporate actions, queue dynamics, or market impact.

## Reproducibility contract

Every recorded experiment includes:

- SHA-256 of the canonical ordered bars;
- SHA-256 of application, walk-forward, and strategy configuration;
- Git commit SHA;
- random seed;
- complete fold and cost-scenario reports.

The experiment registry is append-only. Re-running an experiment creates a new record
rather than mutating prior evidence.

## Walk-forward protocol

The default evaluation uses 252 training bars, a 5-bar embargo, 63 test bars, and a
63-bar step. The deterministic strategies do not fit parameters in the training window;
its trailing bars warm indicator state without opening positions. Every test fold starts
with a fresh fake-money broker.

The suite delays every close-derived signal by one bar and then two bars. At each delay
it repeats every fold at baseline, 2x, and 3x commission, spread, and slippage. Orders
are limited to 1% of bar volume. The benchmark is subject to the same assumptions. The
suite computes total and annualized return, annualized volatility, Sharpe, Sortino,
Calmar, maximum drawdown, turnover, positive-fold ratio, and relative results against
equal-risk buy-and-hold.

Each relative comparison also uses 2,000 deterministic bootstrap resamples of fold
excess returns. Promotion requires the lower bound of the 95% interval to remain above
zero in every delay-and-cost scenario.

## Current evidence

The following pipeline smoke test used 1,000 synthetic daily bars per seed and commit
`f7dd207`. Values shown are the one-bar-delay, baseline-cost scenario. Every candidate
also failed at two-bar delay and higher costs.

| Candidate | Seed | Positive folds | Avg. Sharpe | Benchmark wins | Avg. excess | 95% excess interval | Qualified |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| SMA crossover | 7 | 54.5% | 1.256 | 36.4% | -0.0079% | [-0.0677%, +0.0554%] | No |
| SMA crossover | 17 | 63.6% | 1.331 | 18.2% | +0.0010% | [-0.0367%, +0.0414%] | No |
| SMA crossover | 29 | 81.8% | 2.672 | 27.3% | -0.0072% | [-0.0399%, +0.0199%] | No |
| Volatility trend | 7 | 54.5% | 1.134 | 18.2% | -0.0322% | [-0.0939%, +0.0329%] | No |
| Volatility trend | 17 | 63.6% | 1.321 | 27.3% | -0.0231% | [-0.0725%, +0.0263%] | No |
| Volatility trend | 29 | 81.8% | 2.734 | 27.3% | -0.0493% | [-0.0863%, -0.0113%] | No |
| Z-score mean reversion | 7 | 45.5% | -0.190 | 27.3% | -0.0885% | [-0.1752%, -0.0068%] | No |
| Z-score mean reversion | 17 | 45.5% | 0.518 | 27.3% | -0.0911% | [-0.1928%, -0.0134%] | No |
| Z-score mean reversion | 29 | 45.5% | 1.681 | 27.3% | -0.1512% | [-0.2503%, -0.0342%] | No |

High absolute Sharpe values here must not be interpreted as market evidence. The
synthetic generator contains designed regimes. Seed 17's SMA has slightly positive
average excess return, but its confidence interval crosses zero and therefore blocks
promotion.

## Required next evidence

1. Acquire independent, licensed, point-in-time U.S. equity bars with corporate actions,
   exchange calendars, symbol history, and delisted instruments.
2. Lock an untouched terminal holdout before strategy iteration.
3. Add missing-bar, halt, and variable-spread stress.
4. Compare against cash, buy-and-hold, and volatility-matched benchmarks.
5. Add Deflated Sharpe Ratio and Probability of Backtest Overfitting.
6. Only promote a candidate to continuous paper operation after all gates pass.
