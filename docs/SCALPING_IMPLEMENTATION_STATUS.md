# Scalping implementation status

This status maps the September 21, 2026 scalping implementation report to the
current paper-only system. The report reviewed commit `9194a6f`; the current
release includes the later execution-acceptance and bounded signal-experiment
work.

## Implemented operational path

- The paper-only Alpaca OMS persists intents before broker submission,
  reconciles unknown outcomes by client ID, tracks partial fills cumulatively,
  exits only confirmed owned inventory, and keeps a singleton account lease.
- A separately classified marketable acceptance test has completed a
  broker-confirmed BTC/USD entry and exit. It is excluded from strategy
  performance.
- The signal-driven experiment remains bounded to BTC/USD and ETH/USD,
  price-capped marketable limits, one account-wide exposure, daily submission
  and fill caps, a five-second exit target, stale-data fences, and a modeled
  daily-loss stop.
- Idle retained passive-probe ticks defer broker reconciliation until a
  scheduling boundary; unresolved exposure still receives immediate
  supervision. Probe scheduling selects the least-submitted available symbol,
  so a valid BTC quote cannot indefinitely starve ETH.
- `ProbeObservationV2` is a typed, read-only broker-paper evidence projection.
  It carries the account, cohort, exact policy hash, decision and quote clocks,
  order IDs, broker states, fills, resolution time, and explicit exclusion
  reasons. It is not a strategy training row.
- Probe reports include only records matching the account, cohort, exact
  stored policy hash, authorization window, and requested as-of time.
  Historical shadow loading excludes probe, acceptance, and experimental
  payloads from strategy candidates with explicit audit reasons.
- `/api/scalping-experiments` and `tradeagent scalp-experiment-report` expose
  candidate-to-reconciliation funnel counts, broker/order evidence, P&L
  states, flatness, and evidence exclusions. `tradeagent
  scalp-actual-calibrate` performs chronological calibration only from
  completed broker-confirmed experimental round trips.

## Intentionally not claimed

The execution system works in **experimental paper mode**, not as a proven
profitable or qualified strategy. The qualified action-value model remains
`no_support` until 20 chronological training and 10 later held-out
experimental round trips produce a cost-aware validated artifact. A paper fill
does not establish live queue quality, and no live-money route exists.

The only completed acceptance round trip was integration evidence and had a
negative modeled result. The experiment may honestly make no trade when no
fresh signal reaches its frozen threshold; it must not increase activity,
relax thresholds, or fabricate an edge to meet a collection target.
