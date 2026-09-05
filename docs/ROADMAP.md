# Roadmap

The roadmap follows the common sequence in the four research reports: establish safety
and deterministic infrastructure first, validate strategies second, and consider tightly
bounded live capital only after prolonged forward evidence.

The detailed always-on intraday build and deployment sequence is in
[`AUTONOMOUS_DAY_TRADING_PLAN.md`](AUTONOMOUS_DAY_TRADING_PLAN.md).

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

- [x] Aligned five-ETF panel with provenance and missing-row accounting
- [x] Canonical, strictly chronological CSV ingestion
- [x] Credential-safe Alpaca historical bar downloader
- [x] Initial SPY real-data qualification on Alpaca IEX history
- [x] Dataset manifests and provenance hashes
- [x] Buy-and-hold and cash/no-trade benchmarks
- [x] Rolling walk-forward evaluation with embargo
- [x] 1x/2x/3x transaction-cost stress tests
- [x] Sharpe, Sortino, Calmar, turnover, drawdown, and fold stability
- [x] Append-only experiment registry with immutable configuration hashes
- [x] Relative outperformance gate against equal-risk buy-and-hold
- [x] Fail-closed OMS qualification enforcement
- [x] One- and two-bar decision-delay stress tests
- [x] Spread and bar-volume participation constraints
- [x] Bootstrap confidence interval on benchmark excess return
- [ ] Capacity estimates
- [ ] Corporate-action and symbol-history cross-checks against a second vendor
- [ ] Locked terminal holdout

## Phase 3: production paper operation

- [x] Typed Alpaca paper account, position, and order client
- [x] Risk-gated Alpaca client integration behind the OMS protocol
- [x] On-demand broker-authoritative reconciliation
- [x] Audited manual paper take-profit monitor
- [ ] Scheduled startup and periodic reconciliation
- [x] Local paper-broker checkpoints and restart recovery
- [ ] Partial fills, cancels, rejects, and deterministic order state machine
- [x] Read-only local operator API and research dashboard
- [x] Health checks and Prometheus-style counters
- [ ] Structured logs, NAV metrics, traces, and alerting
- [x] Durable audited kill switch and incident runbook
- [ ] Failure injection for stale data, disconnects, duplicates, and lost acknowledgements

## Phase 5: always-on intraday foundations

- [x] Strict 5/15-minute low-turnover paper mandate
- [x] NYSE holiday, early-close, entry, and flatten gates
- [x] Complete-bar aggregation with minute-gap rejection
- [x] PostgreSQL-compatible production schema and Alembic migration
- [x] Exactly-one-worker lock and heartbeat persistence
- [x] Persisted order transition state machine
- [x] Transactional position-cycle and notification outbox
- [x] Idempotent Resend email provider adapter
- [x] Live Alpaca IEX websocket data ingestion
- [x] Calendar-aware fail-closed shadow worker
- [x] Scheduled reconciliation and heartbeat watchdogs
- [x] Single-instance always-on notifier service
- [x] Docker Compose PostgreSQL runtime profile
- [x] Azure Container Apps always-on shadow template
- [ ] Qualified strategy integration and autonomous order entry

## Phase 4: strategy challengers

- [x] Cross-sectional momentum portfolio and equal-risk benchmark
- [x] Portfolio walk-forward cost, delay, and bootstrap qualification
- [x] Volatility-targeted trend challenger
- [x] Simple mean-reversion challenger
- [ ] Purged cross-validation
- [ ] Multiple-testing controls such as Deflated Sharpe Ratio and PBO
- [ ] Classical ML challenger only after deterministic baselines pass

## Explicitly deferred

Reinforcement learning, unrestricted LLM execution, market making, crypto, colocation,
microservices, and Kubernetes are out of scope until the simpler system demonstrates a
stable, reproducible advantage after realistic costs.

No calendar date authorizes live trading. A future canary would require at least 60
paper-trading days, user-defined minimum trade counts, all operational gates passing,
legal/broker review, and explicit human approval.
