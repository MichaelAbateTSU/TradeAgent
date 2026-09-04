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
it repeats every fold at baseline, 2x, and 3x commission and slippage. The benchmark is
subject to the same delay. The suite computes total and annualized return, annualized
volatility, Sharpe, Sortino, Calmar, maximum drawdown, turnover, positive-fold ratio, and
relative results against equal-risk buy-and-hold.

## Current evidence

The following pipeline smoke test used 1,000 synthetic daily bars per seed and commit
`547861f`. Values shown are the one-bar-delay, baseline-cost scenario; all candidates
also failed at one- and two-bar delays under 2x and 3x costs.

| Candidate | Seed | Positive folds | Average Sharpe | Folds beating benchmark | Average excess return | Qualified |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| SMA crossover | 7 | 54.5% | 1.259 | 36.4% | -0.0079% | No |
| SMA crossover | 17 | 63.6% | 1.335 | 18.2% | +0.0011% | No |
| SMA crossover | 29 | 81.8% | 2.675 | 27.3% | -0.0072% | No |
| Volatility trend | 7 | 54.5% | 1.139 | 18.2% | -0.0321% | No |
| Volatility trend | 17 | 63.6% | 1.327 | 27.3% | -0.0230% | No |
| Volatility trend | 29 | 81.8% | 2.740 | 27.3% | -0.0492% | No |

High absolute Sharpe values here must not be interpreted as market evidence. The
synthetic generator contains designed regimes, and both candidates still underperform
buy-and-hold at the same small allocation.

The mean-reversion challenger was added at commit `5d43365` and failed every seed:

| Candidate | Seed | Positive folds | Average Sharpe | Folds beating benchmark | Average excess return | Qualified |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Z-score mean reversion | 7 | 45.5% | -0.179 | 27.3% | -0.0883% | No |
| Z-score mean reversion | 17 | 45.5% | 0.529 | 27.3% | -0.0911% | No |
| Z-score mean reversion | 29 | 45.5% | 1.692 | 27.3% | -0.1512% | No |

## Required next evidence

1. Acquire independent, licensed, point-in-time U.S. equity bars with corporate actions,
   exchange calendars, symbol history, and delisted instruments.
2. Lock an untouched terminal holdout before strategy iteration.
3. Add execution-delay, spread, missing-bar, and higher-cost stress.
4. Compare against cash, buy-and-hold, and volatility-matched benchmarks.
5. Add bootstrap confidence intervals, Deflated Sharpe Ratio, and Probability of
   Backtest Overfitting.
6. Only promote a candidate to continuous paper operation after all gates pass.
