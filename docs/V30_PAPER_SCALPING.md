# v30 autonomous paper scalping

## Operating decision

The owner's September 11, 2026, **02:18:28.634 Eastern** instruction authorizes
an autonomous, unrestricted **paper** scalper that runs while the owner is
asleep. This is a new standing experiment, not another dated equipment test.
The preference is preserved in [OPERATOR_PREFERENCES.md](OPERATOR_PREFERENCES.md).

No paper loss ceiling, drawdown ceiling, gross-exposure ceiling, daily trade
quota, news/R1 approval requirement, macro veto, fixed morning entry window,
research qualification or shadow period governs this profile. There is no
automatic catastrophic stop based on paper P&L. Order size, entry selection
and short signal/time exits are strategy parameters.

This does not remove the paper-only broker/account boundary, broker order
constraints, valid-data requirements, ownership, duplicate prevention,
reconciliation, or the ability to request a stop. Technical failures recover
automatically where possible; they are not converted into invented market
data, fills, or permission to sell someone else's holdings.

## What the available infrastructure can support

The supplied research recommends CME MES/MBO as a preferred research route.
No futures broker, CME entitlement, MBO archive or direct exchange connection
is configured here. v30 therefore implements the available **Alpaca paper
crypto** route, initially BTC/USD and ETH/USD, using the existing Render
resources and credentials. No new paid provider, compute plan or recipient is
introduced.

The actual read-only Render probe `job-dahpvih42hec73a0haa0` observed:

- ACTIVE, unblocked paper account, cash $99,999.27, flat, no open or unresolved
  orders before the transition.
- BTC/USD and ETH/USD tradable and fractional, but not marginable or shortable.
  Account-wide displayed buying power is not a crypto leverage entitlement.
- Authenticated subscriptions to quotes, trades and order books on
  `wss://stream.data.alpaca.markets/v1beta3/crypto/us`.
- In 45.4 seconds: 49 BTC book messages and 24 quotes; 21 ETH book messages and
  five quotes. **No trades were received during this sample.**
- Initial reset snapshots can describe an older book; receiving a snapshot
  does not magically make its exchange timestamp current.

This feed supplies **aggregated L2**, not exchange order IDs or sequence
numbers. Exact queue position, complete venue gap detection, global crypto
liquidity and MBO/L3 reconstruction are unavailable. Locally numbered receipt
events are not exchange sequence numbers.

Actual broker order increments and minimum sizes are read from the asset
endpoint rather than assumed from exchange examples. The observed price and
quantity increments were `0.000000001`; observed minimum quantities were
`0.00001299` BTC and `0.000407688` ETH.

## Data, signals and replay

The deterministic fast path uses book pressure and event flow, not an LLM
guessing candle colors. Features cover queue imbalance, order-flow imbalance,
microprice, spread/depth, short returns, volatility and event intensity.
Trade delta, VWAP and volume-profile information require actual observed
trades. Sparse or missing trades are visible as missing information, not
fabricated aggressor flow.

Momentum and liquidity-shock reversion are separate rule families. Scores
are not calibrated probabilities. The initial rule model is unqualified:
expected net edge and profitability remain unknown. Fee and spread
break-even diagnostics remain visible but are not an additional qualification
veto on this unrestricted paper experiment.

The initial strategy configuration is one-second decisions, a five-second
feature horizon, $100 order tickets, passive entry limits with a three-second
order lifetime, and a 15-second signal/time horizon for owned positions.
The ticket is a configurable sizing parameter, **not a hard maximum**.
Crypto shorting is unavailable because the broker does not support it.

Replay uses the same feature and strategy definitions, with explicit latency,
fees and conservative aggregate-queue assumptions. Touching a passive limit
is not sufficient proof of a fill. L2-derived queue estimates are not exact
exchange queue positions, and simulated results are not broker fills.

## Runtime and observation

The cloud worker owns one durable execution lease. Market ingestion and
persisted event batching are separate from order supervision; slow news
collection must not stall an open position's execution lifecycle.

Original client order IDs survive timeouts and restart. An unknown submission
is reconciled using that ID, not retried as a new order. Partial fills remain
cumulative; cancellation must be confirmed before an owned-quantity exit.
Crypto base-asset fees can reduce available inventory and must not be counted
twice in cash proceeds. Unposted or estimated fees remain labeled as such.

The existing notifier sends a once-only startup message and periodic
execution/accounting digests, initially every 30 minutes. Every order and
cycle remains in the ledger; high-frequency trading does not require an email
for every quote or state change. The normal 18:00 Eastern daily report remains
on its existing schedule and gains a v30-specific view.

The read-only dashboard exposes `/api/scalping`, clearly separating the v30
profile from retained legacy global/R1 controls. Stale or mismatched-owner
snapshots are not shown as current execution.

## Commands

The daemon uses existing `ALPACA_KEY_ID`, `ALPACA_SECRET_KEY` and
`TRADEAGENT_DATABASE_URL` configuration. Broker endpoints remain fixed to paper.
Run the normal Alembic upgrade before a schema-dependent release.

```powershell
tradeagent scalp-run `
  --cohort-id v30-paper-scalper-20260911 `
  --account-digest YOUR_PAPER_ACCOUNT_SHA256 `
  --approved-at 2026-09-11T02:18:28.634-04:00 `
  --symbols BTC/USD,ETH/USD `
  --notional 100 `
  --confirm-paper-unrestricted

tradeagent scalp-status
tradeagent scalp-stop --cohort-id v30-paper-scalper-20260911 --reason "Owner requested stop"
tradeagent scalp-replay --events .\data\crypto-events.jsonl --latency-ms 25 --output .\research\results\scalping-replay.json
```

The startup flag selects the unrestricted paper profile explicitly; it is not
a daily approval prompt. Do not start another worker against the same account.

## Release status

Implementation and integration are in progress. The currently deployed
September 11 AAPL schedule is not silently overwritten by writing this
document. A reviewed v30 rollout must explicitly supersede it, preserve its
authority and evidence, and record actual new ownership, data, order
reconciliation and notification outcomes before this document claims v30 is
operational.

Paper trading has no real investment loss, but APIs, compute, storage,
credentials and operational failures still have real consequences. Neither
the research attachment, a paper fill nor a passing software test establishes
profitability.

## References

- [Actual read-only capabilities](../research/results/v30-capabilities.json)
- [Alpaca real-time crypto data](https://docs.alpaca.markets/docs/real-time-crypto-pricing-data)
- The owner-supplied 1,234-line scalping research attachment, received September
  11, 2026. Its CME/MBO, neural-model and co-location recommendations are not
  represented as services or capabilities currently configured in TradeAgent.
