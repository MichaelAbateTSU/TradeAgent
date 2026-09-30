# Acceptance automation deployment evidence

Recorded September 30, 2026. This supplements the earlier
[readiness record](readiness.md); it does not replace that historical snapshot
or modify [the frozen protocol](frozen-protocol.json).

## Final monitoring release

The existing worker, dashboard and notifier deployed source
`94e80ec1e75390a284bb0dbe89f38977a3a5dd1b` successfully:

| Service | Render deployment |
|---|---|
| Observation worker | `dep-daulmp3ncjis73fela5g` |
| Dashboard | `dep-daulmpe7bikc73ct1psg` |
| Notifier | `dep-daulmp942hec73ev6cig` |

Pre-start approval `job-daulkik1nsns73epeigg` succeeded before any formal
evaluations. Its append-only approval is bound to protocol hash
`b408f5444145d5e8dfe40751c7eb1f9b2bb4b1bda64736f941406f6be7fe8ab5`.
The original frozen source reference remains `84d03fa`; protocol dates,
signals, sampling, horizons, thresholds and costs are unchanged.

Render's deployment status alone was not treated as active ownership.
The outgoing worker's heartbeat became stale during the natural lease
handoff. No lease was forcibly stolen. Subsequent `/api/scalping` readbacks
confirmed the new source, active `warming_up` observation-only state,
authenticated/subscribed streaming feed, empty writer queue, expired
trading authorization, `no_support`, and zero submitted orders.

At 19:06:21 UTC the manifest had 1,364 raw batches and root
`43c3bc8bf88d82ad5347cbf4ca49b13d3eab3ec0273dae5a90803164ed493095`.
At 19:07:38 UTC it had 1,368 batches, 5,099,080 charged payload bytes and
root `24fb94f9e8699955cc31445c8d541ff2b1ff8ea7de6e8641ad0e99e150c94d94`.
Formal evaluations remained zero before the frozen start; analysis and
promotion remained disabled. Both symbols produced market events.
The legacy shadow service remained suspended.

## Independent read-only readiness

Worker-environment job `job-daulppc1nsns73epvgcg` succeeded on the approved
source. Its actual snapshot ran at 19:03:25.839272 UTC and completed at
19:03:26.329886 UTC. This was a separate release verification, **not**
tonight's final-readiness, startup-smoke or tomorrow's Day 1 check.

| Evidence | Observed result |
|---|---|
| Deployed source | `94e80ec1e75390a284bb0dbe89f38977a3a5dd1b` |
| Lease owner | `srv-dae4tr7qj5pc73a9e0k0-6467cc5744-4txxf` |
| Lease matches heartbeat | Yes |
| Latest heartbeat | September 30, 19:03:21.569968 UTC |
| Latest persisted raw batch | September 30, 19:03:21.341418 UTC |
| Migration | `0015_shadow_research_dataset` |
| Paper account | Pinned digest, `ACTIVE` |
| Positions / open orders | 0 / 0 |
| Broker order records since freeze | 0 |
| Local order attempts since freeze | 0 |
| Renewed trading authorization | False |
| Model / authorization | `no_support` / expired |
| Persistent stop command | Absent |
| Readiness verdict | `verified_clean` |
| Broker request methods | GET only |

The feed had subscribed and was awaiting current book updates immediately
after handoff. BTC's retained book was not current; ETH was explicitly
`awaiting_current_book_update`. Later readbacks showed streaming. The
readiness verdict establishes infrastructure ownership, persistence and
zero-order controls; it does **not** assert usable quotes for every symbol,
95% forward-label coverage or successful future labels. Sparse/stale ETH
quotes remain an explicit collection risk, not a reason to retimestamp data.

## Preserved initial transition and scheduled decisions

The immutable `readiness_initial` record from the previous release captured
a deployment transition: its heartbeat referenced `7a4f68e` while the lease
had moved to the next worker, and the feed was connecting. That record is
retained unchanged and is **not** described as a passed readiness check.
The separate release readback above does not overwrite it.

The deployed `/api/shadow-dataset/checks` endpoint retained initial readiness
and fee evidence and exposed these fixed future checks:

| Check | Eastern time | UTC |
|---|---|---|
| Final immutable readiness | September 30, 19:50 | September 30, 23:50 |
| Bounded startup smoke | September 30, 20:25 | October 1, 00:25 |
| First full-day quality gate | October 1, 20:20 | October 2, 00:20 |

These checks run inside the existing Render observer, independently of this
chat session. Final unverified readiness persists a stop command. Order
attempts, exposure or renewed authority also stop collection. Observation
infrastructure defects pause collection without automatic rearm; legitimate
unusable quotes remain explicit missingness. Missed bounded checks are
recorded as missed, never backdated as successful.

BTC and ETH are assessed independently at every frozen horizon. Primary
coverage divides completed mature 60-second labels by all actual evaluations
old enough for that horizon plus the frozen settlement delay. Overdue
unwritten labels remain in the denominator; young evaluations do not.
Missed grid slots, timestamps, cadence, latency, categories, missing reasons
and maximum backlog are reported separately.

See [acceptance-schedule.md](acceptance-schedule.md) for the decision rules.
There is no new email destination, notification cadence, paid service,
trading cohort, broker-write capability or model promotion.

## Account fee evidence, separate from the protocol

The same read-only job queried account metadata, account configuration and
the latest ten CFEE/FEE activities. At 19:03:26.552019 UTC:

- The account reported `crypto_tier: 1`, `accrued_fees: "0"` and
  `pending_reg_taf_fees: "0"`.
- Configuration exposed no explicit fee fields; ten fee activities were
  returned. None of these facts establishes the effective role, charged
  rate, effective date, asset units or rounding for future executions.
- The [published crypto fee schedule](https://docs.alpaca.markets/us/docs/crypto-fees)
  lists a tier-1 scenario of 15 bps maker / 25 bps taker for 0-100,000 USD
  executed crypto over 30 days.
- The reported field's source is the account's GET
  `https://paper-api.alpaca.markets/v2/account`. The
  [published account reference](https://docs.alpaca.markets/us/reference/getaccount-1)
  does not independently establish the account's effective fee basis.

`reported_account_crypto_tier` is therefore 1 while
`actual_crypto_fee_tier` remains unknown and `verified` remains false.
Fee-sensitive outputs remain scenarios. Frozen conservative assumptions
are not replaced with a favorable rate, and zero accrued fees is not
interpreted as zero crypto trading cost.

No future startup, Day 1 coverage, fourteen-day result or profitable edge is
claimed by this deployment record.

## Validation

On the final monitoring source, the full repository suite passed:
**1,919 tests passed, two skipped**. The 29 targeted dataset/acceptance
regressions, Ruff and strict mypy across 120 source files also passed.
These checks cover software behavior, not future market-data coverage.
