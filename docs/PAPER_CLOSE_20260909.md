# September 9 read-only close watch

The scheduled 15:45 Eastern continuation ran against the actual corrected
**shadow-only** event worker, not the superseded frozen event image. No entry
authorization, deployment, configuration change, order, cancellation, lease
mutation or historical-data operation is part of this watch.

## Actual start and scope

The control-plane check at **15:46:18 Eastern** verified:

- Event/news and recorder release `68c2e8fe086af06c548e7064014c8f1d927ff952`.
- Event cohort `iex-trade-repair-20260909`, config
  `607deddd1a7b05178830fa4cd966de148e266e020905359555686a64042dcad8`,
  shadow mode and fresh matching ownership.
- Dashboard `5e728c5b29fb9330df61d8f3bae95ca8a34ca53b` and notifier
  `37e7a72c792a0398324ac0c5bee5cd50cdbb5077`.
- PostgreSQL `0.5c-1g`, 15 GB, empty public allowlist, protected/isolated
  Production, unchanged Starter application plans and disabled auto-deploys.
- Original global kill and the current R1 pause still active.

One existing event-image Render job,
`job-dagrg69t0dsc73feo7q0`, performs bounded read-only account, clock, position,
open-order, local-intent, diagnostic and selected-control observations. Its
database transactions explicitly enforce READ ONLY and a five-second statement
limit. It opens no IEX socket and no competing worker.

**Actual broker sampling began at 15:49:01 Eastern**, after identity checks and
job startup. This is not retroactive proof of flatness for the preceding minutes.
The job samples approximately once per minute, including the 15:50 flatten and
15:55 hard-deadline periods, and must take a fresh broker-closed sample at or
after **16:05 Eastern**. A successful HTTP response or an old flat snapshot is
not substituted for that final observation.

Each sample retains the actual account status, positions/open orders, unresolved
local intents, diagnostic terminal states, immutable old-cohort hash, current
shadow identity and event heartbeat/lease. Account-day orders are read at both
endpoints. Any unexpected exposure, identity/authority drift or stopped job is
surfaced for read-only reconciliation, not silently cleared.

## Preserved restrictions

The old morning cohort's MISSED/terminal and original pauses remain immutable.
The completed AAPL diagnostic and BTC test are not repeated or reclassified.
Their two submitted BUY attempts still consume the current account-day cap,
irrespective of the separate operator-history accounting gap. No R1
acknowledgement, certificate or operator command is transferred to the new cohort.

The lack of a paper order-update websocket is intentional in shadow mode; REST
reconciliation remains active. Sparse IEX frames and configured-source-only
coverage are not altered. The unexecuted COPY benchmark remains canceled.

Only after the actual close watch finishes may one distinct, idempotent close
summary enter the existing notifier outbox. Earlier accepted emails must not be
resent; provider acceptance is not inbox-delivery proof. The normal 18:00 Eastern
daily schedule is unchanged.

## Completed result

The Render job completed successfully with **17 sequential broker snapshots**
from **15:49:01.627036 through 16:05:00.473252 Eastern**. Every snapshot showed
zero positions, zero open orders and no unresolved local intents. Maximum spacing
between broker timestamps was **61.369 seconds**. The first closed-market
observation was **16:00:07.250529 Eastern**.

Both diagnostic states stayed terminal and their hashes were unchanged. The old
cohort hash, selected controls, original global-kill timestamp, new R1 pause and
shared budget were unchanged across every sample. The complete account-day order
lists at the two endpoints matched: the prior AAPL canceled BUY, BTC filled BUY
and BTC filled SELL only. No new order, cancellation or rearm was performed.

One local control-plane log read timed out at 16:03; the existing job continued
sampling. The complete sequential sample set was subsequently retrieved, with no
missing broker sample or replacement job. This timeout remains in the evidence.
Final runtime readback at 16:06 confirmed unchanged reviewed pins, plans, private
database settings and fresh roles; the event worker correctly reported
`market_closed`.

Only after the watch completed, the existing notifier sent the distinct close
summary **once at 16:08:16 Eastern**. Notification
`59211edb-9c6a-5a58-b5fe-6524e7fa71d7` is `sent`, attempts **1**, provider ID
`46e17c07-5c77-40b9-b2c5-7d15bba9bdd5`. The enqueue job verified that the normal
18:00 Eastern daily schedule remained enabled and unchanged. This is provider
acceptance, not inbox-delivery proof.

The result is sampled flatness through the required post-close check, not
continuous exposure proof, profitability, qualification or renewed entry authority.
All trading restrictions remain in force.

## Evidence

- [Actual starting pins and infrastructure](../research/results/paper-close-20260909-start.json)
- [Read-only job request](../research/results/paper-close-20260909-job.json)
- [Actual sampled account/clock/control evidence](../research/results/paper-close-20260909-samples.json)
- [Exact-byte lossless evidence archive](../research/results/paper-close-20260909-samples.json.gz)
- [Verified close-watch summary](../research/results/paper-close-20260909-summary.json)
- [Final runtime pins and closed-market state](../research/results/paper-close-20260909-final-runtime.json)
- [Retained control-plane read timeout](../research/results/paper-close-20260909-log-read-errors.json)
- [Once-only close-summary email acceptance](../research/results/paper-close-20260909-email-proof.json)
