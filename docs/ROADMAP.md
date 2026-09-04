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

- [x] Canonical, strictly chronological CSV ingestion
- [x] Credential-safe Alpaca historical bar downloader
- [x] Dataset manifests and provenance hashes
- [x] Buy-and-hold and cash/no-trade benchmarks
- [x] Rolling walk-forward evaluation with embargo
- [x] 1x/2x/3x transaction-cost stress tests
- [x] Sharpe, Sortino, Calmar, turnover, drawdown, and fold stability
- [x] Append-only experiment registry with immutable configuration hashes
- [x] Relative outperformance gate against equal-risk buy-and-hold
- [x] One- and two-bar decision-delay stress tests
- [ ] Capacity estimates
- [ ] Corporate-action and symbol-history cross-checks against a second vendor
- [ ] Locked terminal holdout

## Phase 3: production paper operation

- [ ] Alpaca paper adapter behind the existing broker protocol
- [ ] Startup and periodic broker reconciliation
- [x] Local paper-broker checkpoints and restart recovery
- [ ] Partial fills, cancels, rejects, and deterministic order state machine
- [x] Read-only local operator API and research dashboard
- [x] Health checks and Prometheus-style counters
- [ ] Structured logs, NAV metrics, traces, and alerting
- [x] Durable audited kill switch and incident runbook
- [ ] Failure injection for stale data, disconnects, duplicates, and lost acknowledgements

## Phase 4: strategy challengers

- [x] Volatility-targeted trend challenger
- [x] Simple mean-reversion challenger
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
