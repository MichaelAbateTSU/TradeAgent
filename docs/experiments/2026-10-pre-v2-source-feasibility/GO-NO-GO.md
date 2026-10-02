# Pre-v2 readiness: NO-GO

**No source satisfies both the unchanged data-quality gate and the
account-specific venue gate. Stop here: no source is selected, no observer
change or deployment is prepared, and no v2 window is frozen or started.**

Two separately predeclared, prospective thirty-minute studies were
completed on October 2, 2026. This report does not replace the September
cohort, the failed v1 experiment or commit `dc00e4e`.

## Execution venue evidence

The configured, pinned account uses the **Alpaca paper simulator**.
Authoritative provider documentation explicitly says paper orders are
not routed to a live exchange. The inspected account, configuration,
asset and historical-order responses do not identify a BTC/USD or
ETH/USD paper quote-reference location or a live execution route.

Assets reporting exchange `CRYPTO` and successful market-data access
are not evidence of Alpaca-US/Kraken routing. Published descriptions of
generic real-time/NBBO paper fills do not establish a crypto-location
binding. Paper fills also do not enforce displayed NBBO size, so they
cannot validate the unchanged liquidity requirement.

See [execution-venue-evidence.md](execution-venue-evidence.md) and
`routing-evidence.json` for the actual evidence and authoritative URLs.
**Venue match remains unproven for every candidate.**

## Exact study contract

Both studies retained:

- BTC/USD and ETH/USD, evaluated independently on ten-second sampling slots.
- Original actual v1 evaluation and primary-resolution clocks as shared
  anchors for every candidate in each predeclared interval.
- Primary horizon 60 seconds plus two-second settlement.
- Nonnegative provider and local-receive ages, each at most two seconds.
- Inputs known by decision time; future quotes received by the horizon
  and available by the original resolution time.
- Positive executable bid/ask prices and base-asset sizes.
- At least **$10.25 at the entry ask and future bid**, exactly as v1.
- All actual mature evaluations in the denominator, including missing,
  stale, undersized and unavailable-feed cases. No young evaluations
  are counted; after the fixed tails there were none.

Each source/symbol has **180 mature evaluations** per study. A 95% pass
would require at least 171 completed labels independently for BTC and ETH.
No bad interval or rejection category is excluded to improve that number.
No signal or profitability analysis was performed.

## Study 1: existing native stream and conditional alternate streams

Window **16:00-16:30 UTC**, capture tail to **16:31:05 UTC**.
Freeze hash:
`87cd386563d51b04972b767afea917fcb660f78c23fa80338d001f05b39a96cd`.
Read-only job `job-davt8tfavr4c73dbhcf0` completed successfully.

| Source | BTC complete / mature | BTC coverage | ETH complete / mature | ETH coverage | Venue matched | Result |
|---|---:|---:|---:|---:|---|---|
| Existing Alpaca US (`us`) | 77 / 180 | 42.78% | 2 / 180 | 1.11% | Unproven | Fails quality and venue gates |
| Kraken US native (`us-1`) | 0 / 180 captured | Access blocked | 0 / 180 captured | Access blocked | Unproven | Provider error 406 |
| Kraken EU native (`eu-1`) | 0 / 180 captured | Access blocked | 0 / 180 captured | Access blocked | Unproven | Provider error 406 |

Both alternate connections received **406, connection limit exceeded**.
Their raw server responses and connection histories are archived. The
observed zero captured labels are an availability/access failure under
the current credentials, **not a claim that Kraken's native quote
generation has zero coverage**.

The documentation describes limits per endpoint, but these actual
attempts were rejected while v1 held the existing crypto stream. The
precise entitlement/limit scope is not proven by a public account field.
No existing connection was disconnected, no subscription was changed and
no new key or plan was acquired to bypass the limit.

## Study 2: legitimate read-only REST acquisition fallback

The already-accessible location-specific native-quote REST endpoints
provided a safe alternative to opening more sockets. The fallback was
frozen separately, **before its own future window**, with no threshold
changes.

Window **16:45-17:15 UTC**, capture tail to **17:16:05 UTC**.
Freeze hash:
`ea8d5b3cfb635f9c6eac14be1cec66611324c159c50f00898af8e30c481ebd99`.
Read-only job `job-davtu6dg1s2s73bs4k2g` completed successfully.

| Source | BTC complete / mature | BTC coverage | ETH complete / mature | ETH coverage | Venue matched | Result |
|---|---:|---:|---:|---:|---|---|
| Existing Alpaca US (`us`) | 39 / 180 | 21.67% | 0 / 180 | 0.00% | Unproven | NO-GO |
| Kraken US native REST (`us-1`) | 150 / 180 | 83.33% | 125 / 180 | 69.44% | Unproven | NO-GO |
| Kraken EU native REST (`eu-1`) | 151 / 180 | 83.89% | 128 / 180 | 71.11% | Unproven | NO-GO |

Every fallback percentage is **mature paired-label coverage**, not the
fraction of individual snapshots that looked fresh.
Quote acquisition used a fixed 1.5-second interval for each alternate
source, approximately 80 combined requests/minute; the observed API
headers reported a 200-request/minute limit. Both sources used the same
acquisition setting, and neither was tuned after seeing outcomes.

There were 1,297 GET responses and 2,594 accepted quote observations for
each alternate source, including warm-up and the fixed tail. No HTTP
rate-limit or capture failure was recorded. Provider timestamps were
preserved; a recent poll receipt did not make an old quote fresh.
The underlying source generation and polling phase cannot be disentangled
from this run alone, so these results describe this measured REST
acquisition method, not an upper bound on native Kraken performance.

The two different study windows are not pooled or compared as if market
conditions were identical. Their individual frozen results remain intact.

## Complete rejection attribution for the fallback

These are non-overlapping **label-level** counts:

| Source / symbol | Complete | Quote freshness failures | Displayed-size failures | Other label failures | Mature denominator |
|---|---:|---:|---:|---:|---:|
| Alpaca US / BTC | 39 | 140 | 1 | 0 | 180 |
| Alpaca US / ETH | 0 | 180 | 0 | 0 | 180 |
| Kraken US REST / BTC | 150 | 24 | 6 | 0 | 180 |
| Kraken US REST / ETH | 125 | 25 | 30 | 0 | 180 |
| Kraken EU REST / BTC | 151 | 24 | 5 | 0 | 180 |
| Kraken EU REST / ETH | 128 | 23 | 29 | 0 | 180 |

`label-failure-attribution.json` derives this table from the archived
per-label records. Original entry/horizon reason counts remain separately
visible and overlap; they are not added to the denominator.

The native-study alternate-source rejections are categorized as
**connectivity/entitlement**, not quote inactivity. In the fallback,
provider age and receive age failures remain separate, as do displayed
size, continuity, timestamps, capture integrity and persistence categories.
No timestamp/capture/selector defect was demonstrated in the completed
fallback. That statement does not claim that every possible native
transport or future market interval has been validated.

The original machine summaries omit a `complete` counter when its
value is zero. They are preserved byte-for-byte; the tables above and
independent verification explicitly derive the zero from the actual
per-label records rather than rewriting the original summaries.

## Candidate inventory completeness

- `us`: tested through the unmodified existing native/L2 observation path.
- `us-1`, `eu-1`: published native providers; attempted streaming access
  and completed full-contract REST fallback studies.
- `us-2`, `bs-1`: additional latest-quote schema enums, but both discovery
  responses contained no BTC/ETH quotes and no authoritative provider,
  native-stream or account-routing mapping was established. They remain
  explicitly excluded/unqualified, not assigned synthetic price outcomes.
- Other unconfigured exchanges, sandbox data and test streams have no
  established execution relationship to this account and were not
  introduced as substitutes.

## Source selection and proposed v2 configuration

**Selected source: none.**

**Exact proposed v2 configuration: null/blocked.** There is no approved
source/venue, new schema/release, dataset identity, future start/end or
deployment to configure. Supplying a complete-looking v2 configuration
would incorrectly imply readiness.

The constraints that must remain unchanged when a source is eventually
qualified are BTC/USD + ETH/USD; ten-second evaluations; both two-second
age fences; 60-second primary horizon + two-second settlement; $10.25
entry-ask/exit-bid displayed size; all mature evaluations in the denominator;
95% coverage per symbol; zero orders; expired authorization; `no_support`;
and no automatic promotion. Any eventual full v2 protocol must additionally
predeclare its other horizons, schema, source provenance, cost assumptions,
holdout and fixed future dates before collection.

No application code, quote-source configuration or model was changed in
this work. Since no candidate passed all gates, **no observer deployment
or deployment smoke was attempted**. The bounded studies themselves
validate diagnostic acquisition, not a newly approved v2 worker.

## Remaining prerequisites

1. Obtain account-specific provider evidence identifying the paper
   simulator's BTC/ETH quote-reference location and size semantics;
   separately establish the corresponding eligible live route before
   any later live-risk stage. Generic `CRYPTO`/ACTIVE fields do not suffice.
2. Clarify native crypto stream connection-limit/entitlement scope without
   stopping v1, bypassing limits, purchasing access or creating credentials
   automatically. Native alternate-stream coverage remains unmeasured
   because access was denied.
3. If a defensible matched source becomes available, predeclare another
   bounded feasibility study under the unchanged contract. A different
   acquisition method can be investigated, but not by weakening freshness,
   size or the denominator or by treating the earlier snapshot rates as
   successful forward labels.
4. Only after venue and both-symbol quality gates pass, prepare the smallest
   observation-layer change, run regression/lint/type checks and an isolated
   bounded deployment smoke, then request a distinct v2 freeze/start.

These prerequisites are **not satisfied**. Stop here rather than selecting
the highest percentage or quietly switching to Kraken.

## Preservation, reproduction and validation

- Both studies' raw source chains, quote journals, connection/HTTP state,
  source identities, exact clock/size fields, per-label cases, fixed plans
  and broker safety proofs are archived in `native-stream-study` and
  `rest-study`.
- Independent verification reproduced **all 2,160 mature source/symbol
  primary-label records**, checked raw chains/artifact hashes and ruled out
  forward receipt/consumer-availability joins in those records.
- The frozen diagnostic kernel passed **2,118 v1-equivalence/causality
  checks**, and the REST adapter passed mocked stateless identity and
  receipt/acceptance clock checks.
- The unchanged application passed **1,925 tests, two skipped**; Ruff and
  strict mypy across 121 source files passed.
- Before/after broker checks found zero positions, open orders and order
  records since freeze. Worker authorization stayed expired, model stayed
  `no_support`, and reported order attempts remained zero.
- V1 stayed on approved `94e80ec`, using the same existing connection
  identity, without a restart or observer-source change.
- `dc00e4e`, application/migration files, the entire v1 protocol and the
  `quote-availability-audit` artifacts remain unchanged. Original v1
  collection continues only under its existing frozen policy; it is not
  retrospectively promoted or made successful by these studies.
