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

**v30 is deployed in paper mode; new entries are temporarily paused for
corrective maintenance.** Recovery, data recording and notifications remain
active. The current reviewed release is
`cfc0c275ccb78e32e04f98449e58cf40cd449542`; its full validation passed 1,466
tests, with two skipped and 87.92% coverage. Earlier passing suites did not
catch several execution defects: all original findings and failed regression
evidence remain preserved. Focused review cleared the final corrections to
marketable replay limits, foreign inventory segregation, delayed settlement
credits and atomic ownership/activity projection updates.

Actual rollout identities:

| Role | Deployment / code |
|---|---|
| Event/scalper | `dep-dahusfid0e5s7391qbj0` / `cfc0c27` |
| Dashboard | `dep-dahut69594qs738mlad0` / `cfc0c27` |
| Notifier | `dep-dahut6ajnfac73a88cmg` / `cfc0c27` |
| Recorder, unchanged | `dep-dagps3dbedkc739omglg` / `68c2e8f` |

The running execution owner is
`srv-dae4tr7qj5pc73a9e0k0-756df49c96-khf2f`, observed with its matching
fresh lease at **08:09:36 Eastern**. The process waited for the previous
owner's natural lease handoff; no lease was stolen. The immutable run is
`v30-paper-scalper-20260911`, config
`c5652d4939cfc1072f1725866c6c6d7f746503e2cfc52d9414f5c39edd12302d`.
Migration `0014_scalping_runtime` is installed.

The old one-day AAPL authority was explicitly superseded at
**08:10:35.499689 Eastern**, only after observing the new v30 owner.
Its immutable authority and history were not overwritten. A separate
`:revoked` sidecar and audit record link the owner's new instruction and
replacement release. Existing global/pause values and all legacy cohort
hashes were preserved. This is not a replay or reset of a missed test.

At the **08:16:46 Eastern** readback, the worker was processing actual crypto
book/quote data, the compressed persisted tape decoded with matching hashes,
and reporting metadata had no missing rows. One genuine strategy-generated
paper BUY had reached Alpaca and then been canceled without a fill:

- Client: `ta30-da0c0060d10ebe2ed58125256cd57cab5fce15f5`
- Broker: `bf2cbaea-9f42-47ea-8145-16d3ead18f40`
- Status: `canceled`, cumulative filled quantity: **0**

The account was ACTIVE/unblocked, flat and without open orders at that
readback. **This is submission/cancellation proof, not a completed trade or
profitability proof.** The unrestricted worker remains active; it does not
force a BUY when its alpha conditions are absent.

The existing notifier accepted the once-only v30 startup email at
**08:11:27.805030 Eastern**, outbox attempts **1**:
notification `c5e09693-f346-5768-9bfb-ae95934581e2`,
provider `d912a284-72a9-4387-aedf-47f22449b09f`.
Provider acceptance is not inbox delivery proof. The startup message is not
a fill notification.

Application plans, replicas, disabled auto-deployment, environment
fingerprints, PostgreSQL capacity/storage/private allowlist and recipients
were not expanded. A 30-minute resource/flow observation is in progress at
this checkpoint; its outcome and later actual order/fee observations must
remain separate from the deployment and startup-email facts above.

### September 11, 09:00 read-only oversight

The **09:08:42 Eastern** broker read confirmed ACTIVE/unblocked status,
**zero positions and zero open orders**. The same deployed `cfc0c27` worker
still held its fresh matching lease; no unresolved order or ownership state
was reported. Its state was `operator_stopped` because the earlier maintenance
command pauses new entries while preserving recovery.

The ledger then contained **11 completed paper round trips** and 24 no-fill
cycles. Recorded gross cash flow was `-$1.822114911887358160`; modeled net
P&L was `-$4.622181484248164765`. All 11 completed cycles still had pending
fee attribution, so these are **not final confirmed strategy net results**.
The broker cash balance was $99,995.33.

The initial 30-minute observation retained a failed
`runtime_reported_error` verdict: five actual broker HTTP 429 responses caused
backoff, and one exit was delayed approximately seven minutes. The worker
recovered and closed that position. Source/data and request-cadence
corrections are separate unfinished implementation work; the newer local Git
candidate is not the deployed release.

The 08:30 and 09:00 digest messages were each sent once through the existing
notifier. The 09:00 provider acceptance ID is
`11d627b3-45e9-4b3f-b451-147cabc4f0b3`; acceptance is not inbox proof.
No additional email, order, rearm, policy or configuration change was made
by this read-only oversight. The old AAPL authority remains revoked and all
legacy cohort hashes remain unchanged.

Evidence: [09:00 read-only summary](../research/results/v30-0900-readonly-summary.json),
[broker and ledger snapshot](../research/results/v30-0900-readonly-proof.json),
and [existing digest acceptance](../research/results/v30-digest-proof.json).

Paper trading has no real investment loss, but APIs, compute, storage,
credentials and operational failures still have real consequences. Neither
the research attachment, a paper fill nor a passing software test establishes
profitability.

## References

- [Actual read-only capabilities](../research/results/v30-capabilities.json)
- [Cumulative release review](../research/results/v30-review-final.json)
- [Actual initial deployed state](../research/results/v30-initial-proof.json)
- [Legacy authority supersession](../research/results/v30-legacy-supersession.json)
- [Actual intermediate order and email proof](../research/results/v30-interim-proof.json)
- [Alpaca real-time crypto data](https://docs.alpaca.markets/docs/real-time-crypto-pricing-data)
- The owner-supplied 1,234-line scalping research attachment, received September
  11, 2026. Its CME/MBO, neural-model and co-location recommendations are not
  represented as services or capabilities currently configured in TradeAgent.
