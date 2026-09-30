# Automatic readiness, startup smoke and Day 1 acceptance

This monitoring schedule is a pre-start, observation-only addition. It does
not modify the frozen source window, signal thresholds, horizon set,
sampling cadence, fees, or protocol hash.

| Check | Fixed time | Behavior |
|---|---|---|
| Initial readiness and fee evidence | Monitor startup, before the window | Immutable actual-time snapshot, no backdating |
| Final readiness | September 30 at 19:50 Eastern / 23:50 UTC | Release/hash, lease, feed, raw timestamp, migration and read-only broker proof |
| Startup smoke | September 30 at 20:25 Eastern / October 1 00:25 UTC | Two bounded observations fifteen seconds apart; after fifteen-minute maturity |
| Day 1 gate | October 1 at 20:20 Eastern / October 2 00:20 UTC | First complete UTC collection day, including its label tail |

Smoke checks can run only before 20:30 Eastern; a missed scheduler
window is recorded as missed, never replayed as a past success. Day 1 has a
two-hour reporting grace. Each check has a deterministic event identity
bound to dataset/protocol/kind, survives restarts and cannot be silently
overwritten or retried until a favorable answer appears.

The collector performs no broker writes. Monitoring uses only allowlisted
**GET** requests on the fixed Alpaca paper host. A separate minute-level
safety watcher verifies the pinned account, positions, open orders,
broker-created order records since protocol freeze, local submission
states, and authorization flags. Reported zero is not merely inferred
from a healthy web endpoint.

## Smoke proof

The stored report includes both symbols, observed cadence, matured
60/900-second labels, explicit missingness, evaluation growth and raw
manifest advancement. Fresh-native-L1/stale-L2 counts demonstrate the
separate observation contract where such cases occur; synthetic unit
regressions validate the failure case without deliberately disrupting
the live feed.

## Per-symbol Day 1 report

Every BTC/ETH horizon shows:

- formal/eligible evaluations and evaluations old enough for that horizon;
- completed and missing labels, missingness reasons, overdue unwritten
  labels and not-yet-mature evaluations;
- mature coverage, signal/rejection-category counts and stale-decision
  quote counts;
- observed cadence, per-symbol duplicate/reversal/restart counters,
  feed/processing latency p50/p95, labeling latency and maximum backlog.

The primary live coverage denominator is **all recorded evaluations old
enough for the 60-second label plus its frozen two-second settlement**.
Overdue unwritten rows count against coverage. Young evaluations do not.
For safety classification only, a ten-second persistence grace avoids
declaring a normal in-flight write to be a broken label worker; this grace
does not alter labels, their deadlines or the strict reported coverage.
Missed evaluation-grid slots are separately counted. The final fourteen-day
quality gate additionally retains its full expected-grid requirement.

## Decisions and zero-order containment

- Both symbols at least 95%, clean integrity: continue unchanged.
- Complete/explicit stale-price or size missingness with healthy source
  processing: retain honestly; no synthesis or relaxed freshness.
- Stalled evaluation/manifest, overdue labeling, missing grid or unexplained
  timestamp integrity: persist a pause/invalidity command and stop the feed.
  There is no automatic rearm; repair is administrative and a new versioned
  window is required if comparability changes.
- Any observed order/exposure or renewed authority: stop the observer and
  archive the incident. Read failures are never reported as zero positions.
  Three consecutive inability-to-verify incidents also pause the observer.
- Signal/threshold/horizon/cadence adjustments remain outside this run.

The account's actual crypto fee tier is investigated **separately** through
account/configuration metadata and CFEE/FEE activity evidence. Published
30-day-volume tiers are not proof of this account's current tier/fee role.
Unknown effective basis, missing sources or unverified units stay explicit;
fee-sensitive returns remain scenarios. Frozen assumptions are not changed.

Checks are exposed at `/api/shadow-dataset/checks`, with the exact UTC
schedule and immutable snapshots. The existing daily email cadence and
recipients are unchanged; no extra test email or new alert destination is
created. The schedule records genuine future results when they occur—it
does not claim that tonight's start or tomorrow's gate already passed.
