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

**Email policy update:** the owner's 11:50 Eastern instruction now requires
only the 18:00 Eastern five-paragraph daily summary. The original startup and
30-minute digest behavior described in the rollout history below is
superseded; see [DAILY_EMAIL.md](DAILY_EMAIL.md).

The cloud worker owns one durable execution lease. Market ingestion and
persisted event batching are separate from order supervision; slow news
collection must not stall an open position's execution lifecycle.

Original client order IDs survive timeouts and restart. An unknown submission
is reconciled using that ID, not retried as a new order. Partial fills remain
cumulative; cancellation must be confirmed before an owned-quantity exit.
Crypto base-asset fees can reduce available inventory and must not be counted
twice in cash proceeds. Unposted or estimated fees remain labeled as such.

The existing notifier sends one five-paragraph plain-English summary at
18:00 Eastern. Every order and cycle remains in the ledger; no separate trade,
startup or intraday digest email is sent under the updated policy.

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

**The corrected v30 run is active in paper mode**, with autonomous entries
resumed at 09:48 Eastern. Current execution code is
`211347c82b2f4b04aa0dd22e042dfa3eae434ee1`; its final observation is described
below. It remains enabled without another chat or daily approval. The first deployed release was
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

### Corrective run, unchanged paper mandate

The request-load correction and its settlement regressions passed 1,475 tests
(two skipped, 87.92% coverage) and focused independent review. A targeted
reproduction dropped from **260 broker reads over ten unchanged unresolved
ticks to at most four**. This is broker-I/O efficiency, not a trade quota or a
paper loss/approval gate.

Only the event service was redeployed:
`dep-dai0almk1f9s73fii4fg`, code
`211347c82b2f4b04aa0dd22e042dfa3eae434ee1`.
Dashboard/notifier remain on reviewed `cfc0c27`; recorder remains `68c2e8f`.
Plans, instances, environment fingerprints, database schema/capacity and
strategy parameters were unchanged.

The immutable replacement is `v30-paper-scalper-20260911-r2`, run ID
`45b62cc787d4558a76262b5bbf3ad701aa4445b7b2651ce1a78e97fa58d6b432`,
config
`80604de3d2012f9944bb560c73158304b7ac7fb7271bfd10ba7c8aeab3e1cb97`.
It preserves the actual 02:18 owner approval and all prior runs, orders and
fees. The old run's maintenance stop remains historical; the new run reports
`operator_stop: false` and does not reset the account's economic history.

At **09:48:24 Eastern**, fresh ownership was verified for
`srv-dae4tr7qj5pc73a9e0k0-7cd574cbb5-lz6d7`.
The new startup notice
`1dc806b6-5343-52ad-9597-cd234cd18469` was accepted once at
09:48:20.638375 Eastern, provider
`cc1c2f02-a2a6-446c-a04a-f4f8b67bd995`.
This is startup acceptance, not proof of new fills.

A new observation ran from **09:48:54 to 10:18:58 Eastern**, lasting
**1,803.83 seconds**. All 604 dashboard requests returned 200; the sampled
runtime/resource evaluator reported no failures. Persisted market-event
progress advanced from 1,254 to 88,128 under one unchanged current owner.
The complete four-page PostgreSQL log window contained 3,646 records and no
searched backend termination/recovery or out-of-memory indicator.

Peak memory was 155.11 MiB for the event worker, 138.50 MiB for the dashboard,
114.50 MiB for the notifier, 142.75 MiB for the recorder and 757.49 MiB for
PostgreSQL, within the unchanged reviewed capacity bounds.

The **10:20:52 Eastern** broker read confirmed ACTIVE/unblocked status, no
positions and no open orders. The current run had **26 completed paper round
trips**, no unresolved order/ownership state and no reported runtime error.
Its maximum recorded holding interval was **17.83 seconds**, versus the
approximately seven-minute delayed exit in the retained first-run incident.
The strategy's 15-second exit remains a target rather than a fill guarantee.

There were **no broker 429 records in the corrected run** at that read.
Two broker 422 responses reported that an order was already filled during a
cancel/fill race; both recovered through the original order identity with a
one-second retry. These audit facts are retained even though the ten-second
HTTP sampler did not observe a failing runtime snapshot. This is not a claim
that every broker request returned success.

Corrected-run gross cash flow was `-$6.063478242665711576`; modeled net P&L
was `-$14.456783356942432475`. All 26 cycles still had pending/inferred fee
attribution, so **final actual per-cycle net P&L is not confirmed**. The
broker's cash balance was $99,982.84 at the readback. These initial results
are negative, not proof of a profitable strategy.

The 10:00 digest was accepted once by the existing notifier:
notification `db2cb6eb-8eb3-5b7b-b846-58745e59ee57`, provider
`cc288871-c30e-401e-be9d-56af740fadbe`.
The corrected worker remains running with its original unrestricted paper
policy, not stopped after the observation. Prior code, orders, failed
observations, maintenance records and the old AAPL revocation remain intact.
An independent **12:00 Eastern read-only follow-up** is scheduled in this
session; execution does not depend on that wake or another chat message.

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
- [Corrective release review](../research/results/v30-r2-review-final.json)
- [Corrective deployed run](../research/results/v30-r2-initial-proof.json)
- [Final corrected-run summary](../research/results/v30-final-summary.json)
- [Final broker and ledger proof](../research/results/v30-r2-final-proof.json)
- [Lossless corrected observation](../research/results/v30-r2-live-observation.json.gz)
- [Final notifier evidence](../research/results/v30-r2-final-notifier.json)
- [Alpaca real-time crypto data](https://docs.alpaca.markets/docs/real-time-crypto-pricing-data)
- The owner-supplied 1,234-line scalping research attachment, received September
  11, 2026. Its CME/MBO, neural-model and co-location recommendations are not
  represented as services or capabilities currently configured in TradeAgent.
