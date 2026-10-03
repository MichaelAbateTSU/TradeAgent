# Native-book confirmation: startup NO-GO

The ETH investigation and isolated durable receiver are implemented. The
single frozen 72-hour confirmation **did not start successfully**. Do not
describe it as running, completed, or a coverage pass.

## Findings retained from the completed short trial

ETH's eight displayed-size failures are genuine positive base quantities,
not missing fields or unit errors. Six failed at the entry ask and two at
the future bid; the displayed notionals were $1.9572-$9.3366 against the
unchanged $10.25 requirement. The five freshness failures had ETH update
ages exceeding two seconds while BTC messages continued on the same
socket, with approximately 80 ms provider-to-receive delay.

| Short-trial metric | BTC/USD | ETH/USD |
|---|---:|---:|
| Full primary coverage | 178/180, 98.89% | 167/180, 92.78% |
| Fresh paired endpoints | 178/180, 98.89% | 175/180, 97.22% |
| Size eligible among fresh paired endpoints | 178/178, 100% | 167/175, 95.43% |

These observations distinguish reconstruction, freshness and size
eligibility; they do not replace the original full denominator or the
two-symbol NO-GO. A correct small best-side quantity is valid market data,
not evidence that the collector needs fixing or that deeper liquidity is
insufficient. See `eth-availability-audit.md` and
`eth-thirteen-failures.json`.

## The frozen attempt and its failure

| Item | Immutable value |
|---|---|
| Study | `kraken-book-confirmation-v1` |
| Protocol identity | `4a232f3b8ad5e1a02db909f4cae86cc90f6ea25e01578491cab8f5e3839dc982` |
| Receiver source | `255e50a9f51298b55ca3c20bcf0534cb7cb7735f` |
| Frozen at | October 3, 2026, 18:52:41.482681 UTC |
| Planned window | October 3, 20:00 UTC to October 6, 20:00 UTC |
| Planned evaluations | 25,920 per symbol, not a count collected |
| Receiver job | `job-db0kv4ou01pc73aquhmg` |
| Receiver outcome | Failed October 3, 18:55:45 UTC, before the planned start |

The receiver sealed itself failed/incomplete with:

```text
ValueError:v1 pinned source, ownership, connection or freshness changed
```

The guard's generic message alone does not identify every predicate at
the original failure instant. A subsequent independent read-only probe
at 19:48:34.899424 UTC verified unchanged code, owner and connection pins
and a fresh heartbeat/lease, but **`feed.subscribed` was false** and the
feed state was `stopped`.

The existing v1 dashboard reported `paused_invalid` and a persistent
`BROKER_SAFETY_MONITOR_UNAVAILABLE` stop recorded at **11:56:55.016924 UTC**.
This is real safety-monitor containment, not merely a low-quality-data
result. The probe demonstrates a failed subscription prerequisite; it
does not diagnose the original broker-monitor outage or prove current
broker safety from heartbeat freshness.

No guard was weakened, no collector/lookup defect was invented, and no
v1 rearm or successor confirmation was launched. The sealed claim,
original dates and frozen protocol remain evidence of a failed attempt,
not a window to reuse or backdate.

Read-only production export independently confirmed **zero native raw
frames**, no evaluation-count records, zero successful guard checks, and
three journal chunks: protocol, final quality, and terminal failure.
`sealed-status.json` reports `failed_incomplete`, `NO_GO`, and
`v2_ready: false`. `sealed-journal.jsonl` preserves the exact compressed
payloads and journal headers. Full payload verification passed; the
export's compressed payload hashes, decoded lengths, previous links, and reported root were
also checked locally. This verifies the failure archive, not feed
integrity or a coverage result.

A separate GET-only paper-account proof at **20:04:13.759813 UTC**
verified the pinned account, zero positions, zero open orders, zero
broker order records since the original shadow freeze, expired
authorization and `no_support`. See `current-broker-proof.json`.
That later clean snapshot does **not** explain the earlier monitor
failure, clear persistent containment, satisfy the stopped subscription
predicate, or authorize another study.

## Unchanged prospective contract

`frozen-protocol.json` specifies BTC/USD and ETH/USD, ten-second cadence,
60-second primary horizon, two-second settlement, two-second provider
and receive freshness, and $10.25 entry-ask/future-bid best-side size.
The intended duration is exactly 72 hours plus a 75-second capture tail.
No threshold or ticket was optimized for either symbol.

The receiver validates every native depth-ten snapshot/update checksum,
applies updates in wire order using decimal precision, and invalidates
books after disconnect or reconstruction/clock failure until fresh valid
snapshots. Book-state, price-change, quantity-change and receipt clocks
remain distinct; heartbeats and BTC activity do not refresh ETH.

The scheduled/mature denominator retains absent slots and overdue
unwritten outcomes. Reports separate integrity, freshness and conditional
size eligibility while preserving the combined 95% requirement for each
symbol. Fixed summaries include twelve six-hour blocks, UTC dates, and
weekday/weekend classifications. Larger correlated counts do not alone
establish independent reliability.

## Durable storage and safe operations

The implementation has an isolated single-claim namespace, append-only
compressed hash-chain evidence, bounded queues/rings/storage, persisted
hourly/block/final reports, and fail-closed terminal states. A dead
receiver is failed/incomplete, not automatically resumed.

Read-only inspection of the original claim:

```powershell
python -m tradeagent.kraken_confirmation_runtime status --verify
python -m tradeagent.kraken_confirmation_runtime export
```

The production auxiliary installer preserves the global v1 Alembic
revision `0015_shadow_research_dataset`. **Do not run `alembic upgrade
head` or redeploy existing services while their v1 readiness contract
expects 0015.** Migration 0016 is present for later coordinated deployment,
not permission to alter the pinned observer.

Deployment preparation exposed two bootstrap errors, both retained:
an oversized uncompressed command failed before execution; the compressed
replacement installed the auxiliary schema but then called nonexistent
`db.close()` during cleanup. The database interface is `db.dispose()`.
Neither error is a native-book decoder defect or a reason to alter v1.

All four relevant Render services had auto-deploy disabled at the
October 3 inspection; the legacy shadow service remained suspended.
Source publication is not a redeployment or a study restart.

## Costs, eligibility and decision rules

No eligible authenticated Kraken account or personal fee tier has been
established. Public pair minimums are 0.00005 BTC and 0.001 ETH, with
$0.50 minimum cost. The independently retrieved official lowest-volume
spot fee scenario is 0.40% maker / 0.80% taker: 1.60% taker/taker
round-trip fees before spread, slippage and latency. These are scenarios,
not verified user rates. See `fees-and-eligibility.md`.

The personal-mail Alpaca inquiry remains unsent; no corporate mailbox,
notification service, account/key creation, purchase or private Kraken
request was used. Account eligibility, API fee proof and Alpaca
paper-fill reference remain explicit external uncertainties.

| Future fixed confirmation result | Permitted next decision |
|---|---|
| Both symbols reach 95%, integrity clean | Consider v2 research only after venue/account/cost assumptions are resolved |
| BTC passes; ETH genuinely fails size availability | Consider a separately frozen BTC-only protocol with fresh validation; preserve the two-symbol failure |
| Integrity/freshness fails | Repair a demonstrated defect before another strategy dataset |
| Costs overwhelm plausible returns | Reconsider horizon/execution/strategy, not the historical coverage contract |

**Current decision: startup NO-GO.** Resolve and independently verify
the safety-monitor prerequisites before proposing any explicitly versioned
successor. Do not restart this ID, alter its dates, lower size/freshness,
drop ETH, begin v2, or enable paper/live orders. Trading authorization
remains `expired` and the model remains `no_support`.

## Validation

The unchanged implementation passed **2,000 tests, two skipped**, including
75 focused confirmation tests; Ruff and strict mypy over 125 source files
passed. All four frozen source-byte hashes match the receiver commit.
The live guard stopped the launch before collection and the sealed
production journal was successfully exported and verified. A successful
72-hour deployment smoke/endurance test and mature coverage results do
not exist.

`validation.json` records these boundaries. Database size was measured
at 16.1326 GiB before installation; true disk/WAL/index headroom and
72-hour storage endurance were not certified. A storage budget is a
stop condition, not a capacity guarantee.

Commit `dc00e4e`, the entire original v1 dataset, and its
`quote-availability-audit` archive are unchanged. No historical quote,
label, coverage, profitability or model result was rewritten.
