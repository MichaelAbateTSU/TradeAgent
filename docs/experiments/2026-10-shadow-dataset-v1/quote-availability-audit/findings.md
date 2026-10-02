# Day 1 quote-availability root-cause investigation

**Decision: Alpaca US observation availability does not satisfy the frozen
v1 contract. No ingestion, persistence or label-selection defect has been
demonstrated that could rescue the primary-horizon result. Do not start v2
or change the trading strategy.**

This is a read-only investigation of evaluations on October 1 UTC,
September 30 at 20:00 through October 1 at 20:00 Eastern. Existing v1 rows,
missing labels, source links, protocol, thresholds and authority were not
edited. The existing observer remains on approved source `94e80ec`.
Neither a new cohort nor a new observation window was started.

## Complete failure attribution

The audit read all **17,280 evaluations and 86,400 horizon labels** for the
day, including all **78,759 missing labels** across 5/30/60/300/900 seconds.
Each missing label has a separate decision and horizon correlation in
`missing-labels.jsonl.gz`; endpoint counts are not additional labels.

At the primary 60-second horizon:

| Symbol | Mature evaluations | Complete | Missing | Coverage | Upstream quote failure | Displayed-size failure |
|---|---:|---:|---:|---:|---:|---:|
| BTC/USD | 8,640 | 1,425 | 7,215 | 16.49% | 7,129 | 86 |
| ETH/USD | 8,640 | 95 | 8,545 | 1.10% | 8,541 | 4 |

The primary-horizon missing labels are fully explained by the absence of
usable fresh executable quotes or the existing $10.25 displayed-size
contract. Fixing a lookup would not make those quotes younger or larger.
The original 95% denominator is not changed.

Across all horizons, attribution is:

| Horizon | BTC upstream / size / boundary-only | ETH upstream / size / boundary-only |
|---|---|---|
| 5 seconds | 7,042 / 98 / 1 | 8,523 / 9 / 1 |
| 30 seconds | 7,100 / 84 / 1 | 8,540 / 10 / 0 |
| 60 seconds | 7,129 / 86 / 0 | 8,541 / 4 / 0 |
| 300 seconds | 7,150 / 79 / 1 | 8,529 / 5 / 0 |
| 900 seconds | 7,198 / 68 / 0 | 8,557 / 3 / 0 |

`adjudication.json` derives these non-overlapping label-level counts from
the immutable forensic artifact. It does not overwrite `summary.json`.
The original summary conservatively requests further investigation when
any endpoint lacks definitive historical processing evidence.

## Why many quote messages still produce poor coverage

| Native quote evidence, Day 1 | BTC/USD | ETH/USD |
|---|---:|---:|
| Retained quote messages | 124,899 | 8,990 |
| Receipt age p50 | 30.19 ms | 30.71 ms |
| Receipt age p95 | 33.40 ms | 79.67 ms |
| Receipt-gap p95 | 3.605 seconds | 55.900 seconds |
| Largest receipt gap | 137.884 seconds | 852.401 seconds |
| Already stale at receipt | 0 | 0 |
| Provider timestamp regressions | 0 | 0 |

Quotes arrived promptly when they arrived, but in bursts rather than with
reliable two-second coverage. Counting messages is therefore a poor proxy
for time coverage. An ETH stream with thousands of messages can still
spend most evaluation times waiting for its next usable quote.

The audit found one Day 1 connection identity, no recorded local sequence
gaps, no monotonic clock reversals, no future provider timestamps and no
connection changes during the day. Formal sampling had no missing slots;
there were no overdue unwritten Day 1 labels. These facts argue against a
stalled writer, gross clock error or a reconnect storm as the dominant cause.
They do not establish an exchange-side delivery guarantee: this stream
does not provide an exchange sequence number.

Native quotes and stored reconstructed-L2 quotes are distinguished.
Unusable L2 updates are not treated as native quote arrivals, and repeating
a retained quote does not refresh its provider or receive timestamp.
The two independent age fences are applied exactly as frozen.

## Correlation and causal boundaries

Read-only production job `job-davrm0gu01pc73ft2ivg` used a PostgreSQL
**repeatable-read, read-only transaction**. It verified:

- 14,914 source batches and 938,009 events in the captured manifest prefix;
- every batch content hash, event count, sequence and chained root;
- 14,315 distinct stored quote references against raw content and clocks;
- native stored prices/sizes against their original raw quote messages;
- the same immutable label-input digest before and after classification.

The manifest snapshot root is
`0f61a94386ad438b344249792d8480191220782d86183c875981eb8efb213346`.
This is a verified prefix of the still-running dataset, not a fabricated
final seal. The dataset protocol hash remains
`b408f5444145d5e8dfe40751c7eb1f9b2bb4b1bda64736f941406f6be7fe8ab5`.

The executed audit module's SHA-256 is
`68d799bc87e3158acd82cb980ca5805ac6eef5cc541dbfd264dd94b1d338c70f`.
It ran in an isolated diagnostic job using the existing service image;
it did not replace the active collector or approve a post-start source.

Every missing case includes evaluation/horizon/resolution times, the
original selected observations, last causal raw native quote, any fresh
native candidate, provider/receipt ages, decoded clock, raw batch and
persistence references, connection identity, observed connection changes,
last retained market-message time and exact original failure reasons.

The original selector searches history backwards. Decision quotes must
have `processed_at <= evaluation_time`; horizon quotes must have
`received_at <= deadline` and `processed_at <= label_resolution_time`.
Provider and receive ages must both be nonnegative and at most two seconds.
The audit never borrows a quote received after a deadline.

### Two unresolved sub-millisecond decisions

Two decision endpoints have a raw native quote immediately before the
evaluation but lack definitive historical consumer-availability proof:

| Symbol | Evaluation ID | Raw receipt before evaluation |
|---|---|---:|
| BTC/USD | `98bdc5ca-5521-5876-93b6-657754617118` | 0.329 ms |
| ETH/USD | `90fea298-d48d-5e2b-a4bd-71090cb768f0` | 0.493 ms |

These produce ten repeated endpoint findings across the five horizons,
not ten independent incidents. At 60 seconds, both labels also have a
genuinely stale future quote, so neither uncertainty can rescue primary
coverage. Four non-primary labels have this boundary as their only
unresolved cause.

Decoding or later persistence does not prove the consumer had accepted a
quote before evaluation. No accepted-before-cutoff reference was found in
the every-missing-label artifact for those candidate identities. Their
unknown processing times remain unknown; they are not classified as
demonstrated B/C collector bugs or silently reassigned to upstream failure.

Dedicated read-only follow-up `job-davrv0ugekts73f87a0g` searched the
complete retained decision/exit quote-reference inventory for both
candidate identities and inspected neighboring evaluations. It found no
candidate consumer-processing proof; the boundary evaluations reported
features unavailable. `boundary-readback.json` preserves that actual
result. It does not turn a likely scheduling boundary into a proven cause.

Exact historical socket-open state was also not archived at every
evaluation. It is explicitly null, with its reason, rather than invented
from the presence of one recent message. Connection/reconnection and
last-message evidence are reported only to the extent supported by raw
records and actual acceptance checks.

## Bounded source comparison: useful candidates, no approved replacement

GET-only job `job-davrp8rncjis73fh6vhg` sampled all three documented
locations sixty times at five-second intervals, October 2 approximately
14:16-14:21 UTC. It opened **zero additional WebSocket connections**, to
avoid disturbing the existing observer's connection allowance.

| Location / symbol | Fresh valid snapshots | Fresh and $10.25-sized snapshots |
|---|---:|---:|
| Alpaca US / BTC | 47/60 (78.33%) | 47/60 (78.33%) |
| Alpaca US / ETH | 7/60 (11.67%) | 7/60 (11.67%) |
| Kraken US / BTC | 60/60 (100%) | 56/60 (93.33%) |
| Kraken US / ETH | 59/60 (98.33%) | 40/60 (66.67%) |
| Kraken EU / BTC | 60/60 (100%) | 55/60 (91.67%) |
| Kraken EU / ETH | 59/60 (98.33%) | 41/60 (68.33%) |

The Kraken locations were fresher in this short snapshot study, but their
displayed size did not demonstrate the complete frozen contract. More
importantly, data access is not proof that this account executes there.
Read-only BTC/ETH asset metadata reported exchange `CRYPTO`, not a
verified account-specific Alpaca/Kraken route.

These are **REST snapshot rates**, not streaming forward-label coverage,
profitability, an independent market-episode count or prospective
validation. Poll receipt times did not replace provider timestamps.
Different periods are not compared as if they were the same experiment.

Before and after the probe, the pinned paper account was ACTIVE with zero
positions, open orders and broker order records since freeze. The probe
submitted no orders and changed neither v1 nor trading permission.

### Source recommendation

Do not replace Alpaca US executable prices with Kraken prices merely
because they update faster. First obtain an account-specific execution
route/venue mapping. If the intended venue is Alpaca US, require an
appropriate same-venue source or explicit provider clarification of
executable quote semantics; an unrelated exchange is only a research
comparison. If Kraken is demonstrably the eligible intended execution
venue, evaluate its matching region's native stream, not an assumed route.
The current WebSocket URL and `MarketEvent.venue` are Alpaca-US-only.
Supporting another venue requires an explicit new venue/schema contract;
swapping a URL while retaining the Alpaca US provenance tag is invalid.

Official references checked October 2:

- [Real-time crypto data](https://docs.alpaca.markets/us/docs/real-time-crypto-pricing-data):
  quote bid/ask/sizes/RFC3339 timestamps and `us`, `us-1`, `eu-1` locations.
- [Latest crypto quotes](https://docs.alpaca.markets/us/reference/cryptolatestquotes-1):
  location-specific bid/ask/sizes and native quote timestamps.
- [Crypto spot trading](https://docs.alpaca.markets/us/docs/crypto-trading):
  execution and market-data context; generic asset metadata does not
  independently verify this account's route.

## Requirements before a new v2 window

1. Verify execution-venue equivalence, entitlements, quote semantics and
   symbol/size units. A faster feed alone is insufficient.
2. Run a separate bounded, trade-free feasibility observer on that matched
   source. Use the unchanged two-second clocks, ten-second cadence,
   $10.25 size contract and all five horizons, including the full
   fifteen-minute maturity tail. Require at least 95% primary mature-label
   coverage **for both symbols**, with clean integrity and zero orders.
3. Predeclare a schema retaining each quote's exact consumer-processing
   time and per-decision socket/queue diagnostics. The four historical
   non-primary boundary labels cannot be repaired retroactively.
4. Only after feasibility passes, freeze a distinct v2 identity, future
   start/end, source/venue, schema, source manifest and unchanged research
   gates. Do not reuse v1 dates, labels or denominators, and do not tune
   signals using these validation failures.

No tested replacement is approved, so v2 remains unstarted. The completed
investigation is not a trading authorization or an alpha qualification.
If a future audit proves a concrete observer defect, repair it with
regressions and a bounded independent smoke before freezing v2. There is
no justification for a speculative active-v1 patch now.

## Reproduction and archived artifacts

```powershell
tradeagent shadow-dataset-quote-audit --report-date 2026-10-01 --output-dir research\results\day1-quote-audit
```

The command refuses an existing output directory, unelapsed label tails,
missing horizon rows, conflicting stored observations, missing raw
references, corrupt hashes and exceeded resource budgets. It performs no
broker requests, database writes, relabeling, model promotion or economics
analysis. `summary.json` is written only after complete verification.
Run it from the updated checkout. Do not redeploy the frozen v1 collector
merely to obtain the new CLI; production execution here used an isolated
read-only diagnostic job.

Archived alongside this report:

- `summary.json`: exact machine audit and manifest/reference evidence.
- `missing-labels.jsonl.gz`: all 78,759 individually correlated failures.
- `adjudication.json`: non-overlapping label attribution and boundary IDs.
- `boundary-readback.json`: targeted, read-only processing-proof lookup.
- `source-probe.json.gz`: all bounded location samples and broker proof.

The full repository suite passed **1,925 tests, two skipped**; the 35
targeted observer/audit tests, Ruff and strict mypy across 121 source files
passed. Synthetic regressions are not prospective market evidence.
