# Observation run readiness: September 30, 2026

## Verified implementation and actual deployment

- Existing Render worker `srv-dae4tr7qj5pc73a9e0k0` runs
  **`tradeagent shadow-dataset-run`**, not `scalp-run`.
- Worker, dashboard and notifier are live on source release
  `7a4f68e4b3863385bcdc045bc4a1fdad8d8dbb52`.
- Deployment IDs: worker `dep-dauifo67bikc73aoru20`, dashboard
  `dep-dauifo7lk1mc73f8884g`, notifier `dep-dauifoe7bikc73aorusg`.
- Worker plan remains starter, one replica, automatic deployment off.
  The old shadow worker remains suspended; no new service or subscription
  was created.
- Migration `0015_shadow_research_dataset` was explicitly applied via
  successful job `job-dauhqd60tbcc73fg8kbg`.
- The frozen protocol was registered before collection with hash
  `b408f5444145d5e8dfe40751c7eb1f9b2bb4b1bda64736f941406f6be7fe8ab5`;
  [frozen-protocol.json](frozen-protocol.json) preserves its original source
  reference and fields.

Read-only broker preflight `job-dauhdp60tbcc73ferblg` confirmed an ACTIVE
paper account with the pinned digest, **zero positions and zero open orders**.
The collector itself never instantiates a broker client or queries positions;
its order counter is its own verified zero-order path, not a claim that other
account users cannot act manually.

## Actual warm-up, not simulated collection

The published `/api/scalping` snapshot reports `active: true`,
`state: warming_up`, `mode: observation_only`, source `7a4f68e`,
`orders_submitted: 0`, `trading_authorization: expired` and `no_support`.
The crypto feed was authenticated/subscribed/streaming, with 4,425 market
events and an empty writer queue at the final readback.

`/api/shadow-dataset` returned HTTP 200 with the same immutable protocol
hash, **166 persisted warm-up raw batches** and 1,019,039 charged payload
bytes. There were **zero formal evaluations**, as required before
October 1 00:00 UTC. Profitability analysis and promotion were false.
`/api/shadow-dataset/analysis` reports `waiting_for_sealed_dataset`.
These are point-in-time readiness facts, not evidence of fourteen elapsed
days, 95% forward coverage, a profitable signal or completed validation.

The fixed collection is October 1-15 UTC (September 30 at 20:00 through
October 14 at 20:00 Eastern), with the predeclared fifteen-minute label tail.
All evaluations—including rejected and no-family cases—will be retained.
Daily quality reporting and the existing 18:00 Eastern email continue under
the observation-only policy. Terminal quality-gated research runs separately
and archives results or explicit failure; no model is promoted automatically.

## Pre-start repair lineage

The original protocol references `84d03fa`, not a rewritten release.
Its first registration attempt failed safely because the dashboard did not
automatically migrate new tables. The explicit migration/freeze succeeded.

Initial observer startup then exposed a lease-time race: a queued quality
snapshot carried a timestamp older than a concurrent heartbeat renewal.
Ownership is now checked against the live execution clock **after** locking
the lease row. The snapshot's original observation times are not altered.
Regressions cover the queue race, lost ownership and immutable registration.

Release `6e6df3d` was approved by append-only pre-start event via job
`job-daui4p6gekts73eiaheg`; `7a4f68e` was likewise approved via
`job-dauieu6gekts73ejebgg` for bounded terminal screening/archival.
Both approvals required zero evaluations, occurred before start, and were
bound to the original protocol hash. Dates, thresholds, cost assumptions and
zero-order authority were unchanged.

## Limits that remain explicit

- Actual crypto fee tier remains unverified, separate from conservative
  25/25-bps assumptions. No account-specific fee claim is fabricated.
- ETH L2 may be stale during quiet periods. Fresh native L1 survives failed
  L2 reconstruction, but neither old quotes nor unavailable depth are
  retimestamped. Actual primary coverage must be measured.
- PostgreSQL database size was about 16.64 GB at preflight. The 3-GiB payload
  cap does not represent filesystem free space; WAL/index headroom remains
  an operational monitoring requirement.
- Full repository tests passed with two expected skips; targeted new
  regressions, Ruff and strict mypy across 119 source files passed. Synthetic
  tests are not prospective market evidence.

The September paper cohort and archived broker losses remain untouched.
The earlier quote-only retrospective is retained with a coverage-superseded
notice, not silently recomputed or presented as unbiased validation.
