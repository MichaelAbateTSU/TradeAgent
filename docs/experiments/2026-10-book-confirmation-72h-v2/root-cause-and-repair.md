# Confirmation startup diagnosis and narrowly scoped repair

This is a separately versioned **infrastructure confirmation**, not Shadow
Research Dataset v2, a trading cohort, or a replacement for any failed
coverage result. The user explicitly requested diagnosing the remaining
blocker and becoming unblocked on October 3, 2026.

## What actually stopped the old observer

The pinned v1 runtime's `acceptance_checks` calls the GET-only paper
monitor with a five-second HTTP timeout. It records exception class and
consecutive failure count. After three consecutive failures it writes
the persistent safety stop; the observer then stops its market feed.
Successful later requests do not automatically clear that stop.

The durable events show:

| Event time, UTC October 3 | Exception | Consecutive failures |
|---|---|---:|
| 11:54:49.543928 | `ReadTimeout` | 1 |
| 11:55:54.736374 | `HTTPStatusError` | 2 |
| 11:56:55.016924 | `HTTPStatusError` | 3 |

The third event produced `BROKER_SAFETY_MONITOR_UNAVAILABLE`. The
ReadTimeout incident was recorded approximately 5.18 seconds after its
sampled start time. The observer correctly failed closed when it could
not establish broker safety.

**Historical limitation:** the old incident payload did not record the
failed endpoint or HTTP response code. Neither specific status codes nor
an authentication, rate-limit, provider-outage or network root cause can
be reconstructed from these records. We do not guess, increase old
timeouts, edit the events, clear the stop, or restart v1.

The provider's public status-page incident and scheduled-maintenance
feeds each returned fifty records without an October 3 match. The page
displayed a September 26 maintenance notice, not proof of October 3
maintenance. Those bounded public listings do not rule out unreported
or unlisted disruption and do not establish the cause of these requests.

## Current safety is independently available

A read-only diagnostic on October 3 at 22:39 UTC made three complete
paper-account snapshots, twelve GETs in total, using the same five-second
timeout. Each verified the pinned account as ACTIVE, no positions, no
open orders, and no broker order records since the original shadow
freeze. All calls completed in approximately 0.058-0.208 seconds.

Local order attempts remained zero, authorization remained expired, and
the model remained `no_support`. The observer heartbeat and lease were
fresh, with unchanged source, owner and connection pins. The global
schema remained `0015_shadow_research_dataset`.

The stop-control bytes were unchanged before and after inspection:

```text
b5e4e4348054ec0d8f97df2b4d186e406ad8603c38a3eb276ec5c27fa0a94d7e
```

These fresh checks establish present safety at their timestamps, not
continuous past availability or permission to clear persistent
containment. The complete durable incident and current diagnostic proof
is retained in `safety-diagnosis.json`.

An additional bounded check used the original one-minute cadence and
five-second timeout: complete snapshots at **22:51:15.348469**,
**22:52:15.784602**, and **22:53:16.214734 UTC** all verified the same
zero-exposure account. The twelve GETs completed within 0.269 seconds
each. `broker-availability-confirmation.json` preserves this check and
the unchanged stop control. It is API-safety evidence, not a quote-source
study or proof of uninterrupted future availability.

## Demonstrated confirmation defect

The first confirmation receiver incorrectly depended on the unrelated
Alpaca observer being **subscribed**, despite obtaining all candidate
prices from Kraken's public native book and performing its own direct
broker safety checks. A contained v1 therefore blocked independent
observation indefinitely even when current broker safety was verified.

The fix is not to pretend v1 is healthy or ignore broker safety. It is to
version the confirmation's prerequisite as **preserve this exact paused
v1**. The original confirmation ID, guard semantics, failed claim,
protocol, zero-frame journal and terminal decision remain unchanged.

The new confirmation requires the exact stopped state, stop-control
hash/reason and no-rearm flags, the original protocol/global schema,
unchanged source/owner/connection, fresh heartbeat/lease, expired
authorization, `no_support`, and fresh direct GET-only broker/local
zero-exposure and zero-order proof. Any unavailable proof or unexpected
state/pin change fails closed. An active/restarted v1 does not satisfy
this prerequisite.

A read-only preflight verifies those conditions, schema and exact
receiver source bytes before a new durable claim. Study selection is
explicit and isolated so inspecting the new study cannot overwrite,
resume or relabel the old one.

This is a guard-dependency repair supported by concrete evidence, not a
diagnosed quote collector bug, a relaxation of measurement requirements,
or a repair to the historical paper safety monitor.

## Measurement and authorization remain unchanged

BTC/USD and ETH/USD retain ten-second evaluations, a sixty-second
primary horizon, two-second settlement, two-second provider and receive
freshness, and $10.25 best-side entry/future size. Native checksum-valid
book-state timestamps remain distinct from price/quantity change and
receipt clocks. The scheduled/mature denominator includes missing slots
and rejected labels; no thresholds or sizes are optimized per symbol.

Only an entirely future, fixed 72-hour window can be frozen under the
new confirmation ID. It includes the original label tail and fixed
time-block analysis. No restarting until coverage looks favorable, no
ETH removal, and no real v2 research window are authorized here.

The read-only capacity check measured **17,322,333,875 database bytes**,
six database connections against a maximum of 103, and a published
30 GB Render disk. The unchanged receiver payload budget is 3 GiB.
Nominal disk-minus-database size is not true free space: WAL, indexes,
temporary data and 72-hour endurance remain uncertified. No storage
plan or paid service was changed.

Kraken account eligibility and account-specific fees remain unverified.
Previously documented public fee scenarios remain scenarios. No
authenticated Kraken requests, account/key creation, orders, purchases,
model promotion, or Alpaca observer redeployment accompany this repair.
