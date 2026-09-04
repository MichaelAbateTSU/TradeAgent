# Roadmap

The roadmap follows the common sequence in the four research reports: establish safety
and deterministic infrastructure first, validate strategies second, and consider tightly
bounded live capital only after prolonged forward evidence.

## Phase 1: paper-trading kernel

- [x] Immutable domain models and UTC market bars
- [x] Deterministic target-weight strategy boundary
- [x] Independent hard-risk gateway and kill switch
- [x] Idempotent fake-money broker with costs
- [x] Portfolio accounting and drawdown
- [x] Append-only event ledger
- [x] Synthetic backtest and CSV replay CLI
- [x] Strict lint, typing, and automated tests

## Phase 2: research validity

- [ ] Dataset manifests and point-in-time provenance hashes
- [ ] Buy-and-hold and cash/no-trade benchmarks
- [ ] Rolling and expanding walk-forward evaluation
- [ ] Cost and decision-delay stress tests
- [ ] Sharpe, Sortino, Calmar, turnover, capacity, and fold stability
- [ ] Append-only experiment registry with immutable configurations
- [ ] Locked holdout and explicit promotion gates

## Phase 3: production paper operation

- [ ] Alpaca paper adapter behind the existing broker protocol
- [ ] Startup and periodic broker reconciliation
- [ ] Partial fills, cancels, rejects, and deterministic order state machine
- [ ] Read-only operator API and NAV/drawdown dashboard
- [ ] Structured logs, metrics, traces, health checks, and alerting
- [ ] Durable kill switch and incident runbooks
- [ ] Failure injection for stale data, disconnects, duplicates, and lost acknowledgements

## Phase 4: strategy challengers

- [ ] Volatility-targeted trend and simple mean-reversion baselines
- [ ] Purged/embargoed cross-validation and bootstrap confidence intervals
- [ ] Multiple-testing controls such as Deflated Sharpe Ratio and PBO
- [ ] Classical ML challenger only after deterministic baselines pass

## Explicitly deferred

Reinforcement learning, unrestricted LLM execution, market making, crypto, colocation,
microservices, and Kubernetes are out of scope until the simpler system demonstrates a
stable, reproducible advantage after realistic costs.

No calendar date authorizes live trading. A future canary would require at least 60
paper-trading days, user-defined minimum trade counts, all operational gates passing,
legal/broker review, and explicit human approval.

