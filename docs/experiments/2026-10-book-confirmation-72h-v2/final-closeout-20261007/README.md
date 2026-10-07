# Final fixed-window closeout: NO-GO

**The study is failed/incomplete, not a completed quality pass.** Its
original receiver failed on October 4 at 15:01:39 UTC with lease expiry.
No collector was restarted, no replacement observations were collected,
and no protocol, label, threshold, denominator or trading state changed.

The frozen window is October 4, 2026, 00:00 UTC through October 7, 00:00
UTC. Its fixed capture tail ended at 00:01:15 UTC. All **25,920 scheduled
primary outcomes per symbol** are now mature.

| Final result | BTC/USD | ETH/USD |
|---|---:|---:|
| Scheduled and mature denominator | 25,920 | 25,920 |
| Original complete primary labels | 4,616 | 4,689 |
| Full-window coverage | **17.81%** | **18.09%** |
| Written rejected primary outcomes | 778 | 705 |
| Unwritten mature outcomes | 20,526 | 20,526 |
| Required coverage | 95% | 95% |

Unwritten outcomes comprise 20,519 absent evaluations after the
coordinator stopped, plus seven recorded evaluations whose outcomes were
not written. This process-loss missingness remains in the frozen
denominator; it is **not evidence that Kraken was inactive for those
remaining hours**.

## Immutable-data and terminal-state verification

The dedicated read-only closeout ran `status --verify --study-id
kraken-book-confirmation-v2` using the exact original committed LF
four-module bundle from `d5578551e4bb675675b567f9219f4079c85c89af`.
PostgreSQL transactions were enforced read-only, with mutation attempts
denied. The successful closeout job was `job-db2oshid0e5s73dsh9l0`.
It verified every compressed raw payload/header and independently
checked frame continuity, evaluation/label identities, and fixed-block
arithmetic. No production writes or new claims occurred.

Protocol identity:
`e05f3c559cfc3841c8492500aeebdecbcb2812e0b2f4bc743c549ee9ed560f3a`.

Verified unchanged journal root:
`54782507ee32d1d6a4c4b80561aa9d045975c96f6ddc0f8bf5aec3d3a8f03ab2`.

| Durable evidence | Verified count |
|---|---:|
| Journal chunks | 168,121 |
| Contiguous native frames | 1,688,284 |
| Native quotes | 1,632,695 |
| Scheduled evaluation records per symbol | 5,401 |
| Written primary outcomes per symbol | 5,394 |

The earlier [complete native replay and reliability audit](../../2026-10-book-confirmation-reliability-audit/README.md)
reconstructed all 1,632,695 native book updates and found no
price/quantity/provider-clock/checksum discrepancy. The unchanged journal
root binds this closeout to that same corpus; no historical data was
replaced after the repair.

**Exact drained final counters cannot be certified.** There is no
`quality:final` record and no successful terminal seal. The last quality
snapshot matches its own durable prefix exactly at 1,686,537 frames and
1,631,037 quotes, but 1,747 additional frames and 1,658 additional quotes
were persisted afterward. This is an incomplete terminal drain, not a
reason to loosen the final-counter gate. `clean_integrity` and
`raw_frame_counter_consistent` remain false.

The report's exact state is
`failed_incomplete_process_lost_no_restart`, with `decision=NO_GO`.
Its terminal explanation infers process loss from the expired
study-specific lease; the immutable claim prevents continuation.

The first failed confirmation remains independently verified at its
original three-chunk journal root
`1ec4d51937c9761c3f5692c643d8431861cbeefa2ebfb8412865f04fe776b463`
and protocol identity
`4a232f3b8ad5e1a02db909f4cae86cc90f6ea25e01578491cab8f5e3839dc982`.
The entire failed Shadow Research Dataset v1, commit `dc00e4e` and its
quote-availability audit are preserved.

## Failure attribution

These counts classify written rejected outcomes using the previously
verified immutable-tape forensic audit. They are mutually exclusive per
label; raw decision/horizon endpoint-reason counts in the final report
overlap and must not be added as mutually exclusive label totals.

| Written rejection cause | BTC/USD | ETH/USD |
|---|---:|---:|
| No fresh native book update under the frozen clock | 648 | 402 |
| Fresh provider-time quote received after cutoff | 69 | 28 |
| Genuine insufficient displayed best-side notional | 12 | 226 |
| Explicitly missed scheduled evaluation | 45 | 45 |
| Fresh stored horizon quote lost to delayed ring lookup | 4 | 4 |
| Total written rejected outcomes | 778 | 705 |

All raw reconstruction/CRC/timestamp-regression rejection counters are
zero, with one native connection and no reconnects during the captured
interval. The absence of a reconnect cannot establish connectivity after
the receiver failed. Source-update inactivity, late local delivery and
genuinely small displayed size remain distinct from the demonstrated
reporting/cadence/ring defects. A small or unchanged quote remains valid
market data even when it fails this frozen trading-eligibility contract.

The later infrastructure repair and bounded diagnostic do not alter these
outcomes, retime old quotes, backfill labels or convert this failed study
into a successful one.

## Fixed time breakdowns

Every six-hour block retains 2,160 mature scheduled outcomes per symbol.
The third block is incomplete because the receiver failed halfway
through it. Zero-coverage later blocks are retained rather than excluded.

| UTC block | BTC complete / coverage | ETH complete / coverage | Unwritten per symbol |
|---|---:|---:|---:|
| Oct 4 00:00-06:00 | 1,727 / 79.95% | 1,797 / 83.19% | 0 |
| Oct 4 06:00-12:00 | 1,939 / 89.77% | 1,923 / 89.03% | 0 |
| Oct 4 12:00-18:00 | 950 / 43.98% | 969 / 44.86% | 1,086 |
| Oct 4 18:00-Oct 5 00:00 | 0 / 0% | 0 / 0% | 2,160 |
| Oct 5 00:00-06:00 | 0 / 0% | 0 / 0% | 2,160 |
| Oct 5 06:00-12:00 | 0 / 0% | 0 / 0% | 2,160 |
| Oct 5 12:00-18:00 | 0 / 0% | 0 / 0% | 2,160 |
| Oct 5 18:00-Oct 6 00:00 | 0 / 0% | 0 / 0% | 2,160 |
| Oct 6 00:00-06:00 | 0 / 0% | 0 / 0% | 2,160 |
| Oct 6 06:00-12:00 | 0 / 0% | 0 / 0% | 2,160 |
| Oct 6 12:00-18:00 | 0 / 0% | 0 / 0% | 2,160 |
| Oct 6 18:00-Oct 7 00:00 | 0 / 0% | 0 / 0% | 2,160 |

UTC October 4, also the weekend category, has denominator 8,640 per
symbol: BTC 4,616 complete (53.43%), ETH 4,689 (54.27%), and 3,246 unwritten
each. October 5 and October 6 each retain denominator 8,640 and have no
complete labels. The combined weekday category retains denominator
17,280 per symbol, all unwritten.

All twelve fixed blocks, three UTC dates and weekday/weekend categories
were independently checked to sum to the unchanged overall denominator
and counts. `fixed-blocks.json` preserves every original diagnostic,
including aggregate rows and overlapping rejection reasons.

## Current safety and next permitted decision

A fresh GET-only broker guard at October 7 00:21:41 UTC verified zero
positions, open orders, local order attempts and broker order records
since freeze. Authorization remains **expired**, the model remains
**`no_support`**, and the original v1 remains `paused_invalid`, stopped,
unauthenticated and unsubscribed under the exact original stop-control
hash. There was no rearm or protocol change.

**Do not start v2 or another confirmation automatically.** Resolve the
remaining venue-matched executable-price source feasibility, real
PostgreSQL locking/pool/endurance requirements, personal venue eligibility
and actual maker/taker fees before separately proposing a new frozen
prospective study. A bounded component repair pass is not a 72-hour
market-quality pass, profitability result or execution authorization.

The first read-only closeout wrapper failed at import time because it
looked for `validate_guard` in the store module rather than the frozen
runtime module. Correcting only that operational import allowed the
read-only retry to succeed. It created no claim, changed no source or
protocol, and wrote no production evidence; both attempt receipts are
preserved.

`final-status-readback.json`, `immutable-journal-and-safety-proof.json`,
`closeout-verdict.json`, `fixed-blocks.json`, the verification script and
artifact hashes provide the complete reproducible closeout evidence.
