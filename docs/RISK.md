# Risk policy

TradeAgent starts with capital-preservation defaults adapted from the committed research:
no live mode, no leverage, no shorts, maximum 50% gross exposure, maximum 5% in one
position, and maximum 2% added by one order.

| Control | Default | Behavior |
| --- | ---: | --- |
| Gross exposure | 50% NAV | Reject new risk above the limit |
| Position exposure | 5% NAV | Reject concentration above the limit |
| Order exposure | 2% NAV | Reject an oversized risk-increasing order |
| Open positions | 10 | Reject an additional symbol |
| Daily loss | 1% | Reject new risk after the threshold |
| Drawdown | 1.5% | Reject new risk after the threshold |
| Order rate | 20/hour | Reject new risk after the threshold |
| Market-data age | 120 seconds | Reject the order; stale data fails closed |
| Shorting | Disabled | Reject a projected negative position |
| Leverage | Disabled | Reject purchases exceeding available cash |

Risk-reducing orders may proceed while trading is disabled, the kill switch is active, or
loss limits are breached. They still require valid fresh data and cannot create a short.
This preserves a path to flatten without permitting the strategy to add exposure.

## Kill-switch operations

`RiskEngine.activate_kill_switch()` blocks new risk in-process. A later persistent
operator control will make activation durable across restarts. Reset must only happen
after data health, account state, and the event ledger have been reconciled.

## Research promotion gates

A strategy cannot qualify on total return alone. Each evaluation uses rolling
out-of-sample folds with an embargo and requires:

- at least 60% positive folds;
- positive average Sharpe ratio;
- the same absolute tests under 2x and 3x simulated costs;
- positive average excess return versus equal-risk buy-and-hold;
- buy-and-hold outperformance in at least 60% of folds at every cost level.

Passing these gates is evidence for more paper testing, never authorization for live
capital.

## What paper controls do not cover

The paper broker cannot reproduce queue position, venue rejects, halts, borrow
availability, spread dynamics, market impact, latency, corporate actions, margin calls,
or broker outages. Passing these limits is necessary but not sufficient for live
trading. Any future live path requires a written regulatory assessment, separate
credentials, broker reconciliation, shadow operation, a multi-month paper qualification,
and an explicit human approval gate.
