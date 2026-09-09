# Frozen news-worker repair: shadow-only transition

At **14:43:26 Eastern on September 9**, the owner requested that the frozen news
worker be fixed. The correction already exists in reviewed release
`68c2e8fe086af06c548e7064014c8f1d927ff952`: trade identity includes venue and
event time, and the generic writer no longer rejects a new day's trade merely
because its provider ID appeared on an earlier day.

## Scope and protections

The existing morning cohort `v20-news-paper-20260909-r1` is immutable. Its
code/config fingerprint cannot be overwritten to make a different package appear
approved. Its MISSED outcome, terminal marker, original pause and prior risk
acknowledgement are preserved.

The same Render event service is therefore updated to the corrected release with
a new, explicitly **shadow-only, nonqualifying collection cohort**:

```text
tradeagent run --mode shadow --purpose iex-practice --practice-start-date 2026-09-09 --entry-policy event-strategy --cohort-id iex-trade-repair-20260909 --symbols AAPL,MSFT,NVDA --max-entries-per-session 2
```

This is not another paper-trading attempt. Shadow mode blocks new entries and
calibration and does not open the paper order-update stream. Existing
same-account/day owned-position recovery remains available, so actual flatness,
zero open orders and no unresolved local/diagnostic orders are required before
the zero-order observation. No certificate, command, risk acknowledgement or
entry authorization is transferred.

The account's two prior submitted BUYs remain consumed under the unchanged
account-day limit. No budget, historical equity record, old cohort or terminal is
reset. New shadow-local diagnostic rows are not a fresh trading allocation.
The global kill switch remains active at its original timestamp.

## Actual transition

The **14:46:47 Eastern** read-only preflight found the new ID and command keys
absent, the pinned paper account ACTIVE/unblocked and flat, no open or unresolved
orders, and both old diagnostics terminal. It recorded the old cohort row hash
`6633a0006d6b6e8a11d6cd3555f0c8a061bbe9c9a696e3566d3a9a1f809203fa`
for independent post-transition comparison.

The exact operational configuration received independent review. Forty-four
targeted shadow/runtime/risk tests passed; the unchanged candidate's previous
complete validation passed 1,102 tests with two skips. Only the event service's
start command was patched. All environment values were verified unchanged;
there were no obsolete news/demo digest overrides to remove. No plan, replica,
recipient, source subscription or permission was added.

Deployment `dep-dagqkimk1f9s73ckdtog` explicitly targets `68c2e8f`. Its normal
pre-deploy migration command is compatible with the already applied schema 0013.
Actual owner handoff, new immutable manifest, fresh source processing, fixed
stock-trade persistence, a complete live observation and result-email acceptance
are required before this transition is called operationally complete.

At **18:56:44 UTC**, the actual new owner
`srv-dae4tr7qj5pc73a9e0k0-866dc8455f-kqghg` was fresh, collecting in shadow mode,
and matched the global event lease. Its immutable config hash is
`607deddd1a7b05178830fa4cd966de148e266e020905359555686a64042dcad8`.
An independent job recomputed the manifest/config from the actual installed
image and verified the corrected persistence source hash. The entire old cohort
row and selected old terminal/pause/shared-budget controls matched their
preflight values exactly.

The new cohort has no certificate, trading authorization or operator command.
Its calibration status is `shadow_no_orders`. Its new R1 risk pause remains
visible and is **not** cleared or borrowed from the old cohort's acknowledgement;
data collection continues in shadow mode. The absent paper order-update socket
is intentional in this mode; REST reconciliation remains active.

The real new-cohort report-admission test returned 429 while ordinary sections
stayed 200, then generated the full 5,309,674-byte report successfully in
1.227 seconds. No daily report or accepted prior notification was resent.

## Completed live verification

The full **18:59:52.985346-19:29:55.852013 UTC** run completed
**1,802.867 seconds and 2,366 HTTP requests**, with zero HTTP errors, zero new
recorder queue drops/gaps, stable matching owners and no deployment changes.
The event worker stayed in shadow mode, and successful configured-source polling
advanced from 18:59:28 to 19:29:42 UTC.

Independent indexed stock-trade reads showed **AAPL 2 to 36**, **MSFT 2 to 35**,
and **NVDA 2 to 36** actual persisted rows in the fixed window. All required
SPY/QQQ/IWM/TLT/GLD quote/trade/bar series also advanced. This demonstrates the
running event worker's corrected write path, not a test job inserting evidence.

Peak application memory stayed below 400 MiB: event 212.25, dashboard 292.07,
notifier 111.34 and recorder 123.35 MiB. PostgreSQL peaked at 374.25 MiB, below
the reviewed 800 MiB ceiling. Its complete 2,024-record log check found no
termination/recovery/FATAL/PANIC; 104 known duplicate-evidence-key SQL errors
remain explicitly recorded.

At **15:31:23 Eastern**, the actual paper account was ACTIVE and flat with no
open or unresolved orders. The account-day order list, old cohort row hash,
selected old controls and shared budget matched preflight exactly. The new
cohort still had no trading certificate or authorization.

**The frozen-worker operational defect is resolved.** Both active raw-trade
producers now run the corrected library. This is not a trading rearm, complete
market/source coverage, strategy qualification or profitability proof. The
new R1 pause, global kill and consumed BUY budget remain in force; operator
history must still be integrated into risk accounting before future entry
authority. The 15:45 Eastern read-only close watch remains scheduled.

The existing Render notifier sent the repair-completion email **once at
15:34:15 Eastern**. Notification `1ca73746-fb64-5a5b-90e2-7375b4e4cc3c` is
`sent`, attempts **1**, provider ID
`7a64b8e8-2397-4b45-889e-45a4e9ff0ec3`. This is provider acceptance, not proof
of inbox delivery. No earlier accepted message was resent.

The subsequent [read-only close watch](PAPER_CLOSE_20260909.md) completed with
17 flat broker observations through **16:05 Eastern**, preserved all selected
controls and account-day orders, and sent its distinct close summary once.
No trading permissions were enabled.

## Evidence

- [Fresh transition guards](../research/results/news-worker-repair-20260909-preflight.json)
- [Start-command-only change and environment preservation](../research/results/news-worker-repair-20260909-settings.json)
- [Exact deployment request](../research/results/news-worker-repair-20260909-deploy.json)
- [Actual owner handoff](../research/results/news-worker-repair-20260909-handoff.json)
- [Recomputed manifest and preservation proof](../research/results/news-worker-repair-20260909-identity.json)
- [Actual report admission and generation](../research/results/news-worker-repair-20260909-report-proof.json)
- [Complete live summary](../research/results/news-worker-repair-20260909-summary.json)
- [Lossless full observation](../research/results/news-worker-repair-20260909-soak.json.gz)
- [Actual stock/recorder progression](../research/results/news-worker-repair-20260909-physical-proof.json)
- [Final old-record, budget and broker preservation](../research/results/news-worker-repair-20260909-final-proof.json)
- [Complete database-log check](../research/results/news-worker-repair-20260909-pg-logs.json)
- [Repair-completion email acceptance](../research/results/news-worker-repair-20260909-email-proof.json)
