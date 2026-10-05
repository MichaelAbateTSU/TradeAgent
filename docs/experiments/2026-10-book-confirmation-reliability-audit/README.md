# Collector reliability and missing-label investigation

**Observation and research remain NO-GO; orders remain disabled.** This
investigation preserves both failed confirmation claims, their original
labels, all frozen thresholds, and the stopped Shadow Research Dataset v1.
It neither restarts a failed receiver nor begins research v2.

## What failed

The second fixed confirmation receiver, `job-db0p1ovavr4c738nlkg0`,
stopped on October 4, 2026, at approximately 15:01 UTC with
`RuntimeError: confirmation_lease_expired`. Its attempt to seal also failed
because ownership had expired. The reader therefore reports
`failed_incomplete_process_lost_no_restart`; this is not a successfully
sealed study.

Periodic report completion lag increased from 9.03 seconds at hour one
to 82.743 seconds at hour fourteen, approaching the 90-second ownership
lease. No hour-fifteen report was persisted. The coordinator's last
evaluation was at 15:00:00, its last label at 14:59:52, while raw reception
and persistence continued through 15:01:23. Independent native-frame
replay found fresh horizon quotes that were absent from eight recorded
labels after delayed resolution. These facts distinguish an observation
scheduling/liveness failure from a broken native order-book reconstruction.

Source tracing demonstrated that the coordinator advanced cadence, renewed
ownership, persisted quality/safety checks and then awaited the entire
hourly report in the same loop. The independent receiver/writer could
continue while that coordinator stopped scheduling and renewing.
A deterministic 110-second report reproduced legacy lease expiry.
A longer lease or a more permissive freshness rule is not a substitute
for independent, timely renewal and label resolution.

## Complete immutable-corpus verification

The bounded, read-only audit used the exact frozen `d557855` four-module
bundle, not the edited working tree. PostgreSQL sessions were read-only.
The audit made zero database writes and completed in approximately
547 seconds, with maximum resident memory below 239 MiB.

| Verification | Result |
|---|---:|
| Hash-chain and compressed-payload-verified journal chunks | 168,121 |
| Contiguous native frames | 1,688,284 |
| Independently replayed native book updates/checksums | 1,632,695 |
| Stored quotes matching reconstructed prices, quantities, clocks and CRCs | 1,632,695 |
| Selected quote references matching immutable raw persistence | 10,814 |
| Price/quantity/clock/checksum discrepancies or capture rejections | 0 |
| Stored scheduled evaluations per symbol | 5,401 |
| Written primary outcomes per symbol | 5,394 |

Verified journal root:
`54782507ee32d1d6a4c4b80561aa9d045975c96f6ddc0f8bf5aec3d3a8f03ab2`.

All evaluations through slot 5,400 are present. Forty-five per symbol were
explicitly marked as missed scheduled evaluations; they are not absent
database rows. Seven evaluations per symbol have no written primary
outcome when the process stops progressing. Later scheduled outcomes
missing after process loss are retained by the original full denominator;
they are **not evidence of upstream quote unavailability**.

## Attribution of every observed missing outcome

The following buckets are mutually exclusive per label. Where endpoints
have multiple failures, scheduling is attributed first, followed by a
demonstrated lookup miss, late local receipt, native-update inactivity,
and displayed size. Endpoint-level reasons in the case archive can overlap
and must not be summed as mutually exclusive labels.

| Original observed result / failure bucket | BTC/USD | ETH/USD |
|---|---:|---:|
| Original complete primary labels, unchanged | 4,616 | 4,689 |
| No fresh native book update under the frozen provider/receive clock | 648 | 402 |
| Fresh provider-time quote received locally after cutoff | 69 | 28 |
| Genuine insufficient best-side displayed notional | 12 | 226 |
| Explicitly missed scheduled evaluation | 45 | 45 |
| Fresh stored horizon quote missed after reporting delay | 4 | 4 |
| Evaluation present, primary outcome unwritten at process loss | 7 | 7 |
| Total observed scheduled evaluations | 5,401 | 5,401 |

All forty-five declared scheduling misses overlap periodic-report lag
intervals. All four lookup misses per symbol occur after the hour-twelve,
hour-thirteen or hour-fourteen reporting delay. At the relevant horizon,
the independently reconstructed and stored native quote was fresh; later
resolution found no retained future quote. The reference quotes are
**forensic witnesses, not replacement labels or backfills**.

The dominant remaining failures are native-update freshness and genuine
displayed size, not quote loss, CRC corruption, unit interpretation, or
timestamp regression. Correctly observed small quantities and unchanged
books remain valid market data even when the frozen experiment rejects
their trading eligibility. The 95% gate is not rescued by excluding them.

Provider-to-local-receive delay reached 2.930 seconds for BTC and 2.944
seconds for ETH. A provider-time quote arriving too late establishes local
unavailability at the cutoff, but does not alone identify network,
provider batching, or receiver scheduling as the cause.

## Timestamp-method correction

The first independent reference sweep additionally required acceptance
before the exact cutoff. That was stronger than the frozen provider and
receive-time rule. Its preliminary output is preserved and explicitly
superseded by `label-audit-summary.json` and `failed-label-cases.jsonl.gz`.

One BTC future quote, frame 1,347,498 at slot 4,374, was received 93,911
nanoseconds before the horizon and accepted 30,732 nanoseconds afterward.
The original future eligibility was valid; the label failed only on its
stale decision quote. This is not a ninth lookup defect.

The exact frozen selection rule searches backward in acceptance order.
Entry acceptance must be no later than the scheduled evaluation. Future
reception must be no later than the 60-second horizon, while acceptance
may occur through the frozen two-second settlement, at evaluation plus
62 seconds. Freshness uses provider-book and receive ages at the endpoint;
provider time is not a selection-sort key. An eligible-clock invalidation
stops selection. The final attribution preserves these distinctions.
No historical protocol or label was edited to make this correction.

## Surgical prospective repair

Confirmation infrastructure revision 3 separates periodic reporting from
scheduled evaluations and mature-label resolution. Dedicated, bounded
runtime I/O lanes keep expensive historical reporting from occupying the
writer, lease/safety checks or cadence work. Ownership checks sample the
current wall clock after acquiring the ownership row lock; an old quote
or report timestamp cannot extend an expired lease. Ownership loss and
unsafe/missing current safety proof still fail closed.

The lease remains 90 seconds and the quote ring remains 70 seconds.
Deterministic 71-, 82- and 110-second reporting delays reproduce the
legacy horizon-quote eviction; the repaired independent resolution loop
writes the outcome at its normal 60-second horizon plus two-second
settlement while the report is still blocked. Finalization and ownership
loss are also covered. All 173 focused regressions, repository lint and
strict source/test typing pass.

The supported prospective schema and study ID are
`kraken-book-confirmation-v3`. No v3 protocol has been frozen or claimed.
If a future study is justified independently, its bounded warm-up is
60 seconds and its launch must be admitted within that interval before
the fixed start, not days early. The formal `run` command is **not a
bounded smoke command**.

The deployment diagnostic is a separate component harness: public native
book data, fixed cadence/maturity, isolated scratch SQLite evidence, current
GET-only safety proof and concurrent read-only historical reporting.
It must write no production claims, evidence, worker locks, schema or
trading state. Its results cannot certify PostgreSQL contention behavior,
72-hour endurance or market-source feasibility. Deployment readbacks are
archived only after that bounded diagnostic has completed.

## Decision

Repair the demonstrated collector scheduling/renewal defect and validate
that repair separately. **Do not automatically create another 72-hour
confirmation or research-v2 window.** The observed native freshness and
size failures already materially exceed the permitted missingness,
independently of the demonstrated reporting defects.

A reliability smoke is not a source-feasibility pass. Further research
requires a defensible venue-matched source that passes the unchanged
two-symbol contract in a separately preregistered prospective study,
plus resolved personal venue eligibility and actual cost assumptions.
Neither successful reconstruction nor repaired lease handling establishes
fills, execution eligibility, economic edge or profitability.

The existing October 7 read-only closeout remains responsible for archiving
the original fixed window honestly. No failed claim may be resumed,
extended, reseeded, backfilled or relabeled successful.
