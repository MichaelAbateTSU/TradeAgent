# Investigation: disappearing later-period forward labels

The September broker lifecycle and fourteen completed paper cycles are
not invalidated by this investigation. The previous retrospective coverage
claim, however, was affected by a separate quote-replay defect.

The read-only Render job `job-dauh041srm7s73c7r7t0` inspected September 28,
2026 at 08:01 UTC. It found correctly aligned signal and raw quote clocks:
the signal journal recorded 08:01:00.307513 UTC; its native quote had
exchange time 08:01:00.062785 and receipt time 08:01:00.095371.
The nearby ten-second tape slice contained 27 batches / 181 events.
Native quotes were interleaved with L2 deltas, including deletes and
single-sided updates.

The retrospective reader could enter `snapshot_required` or reject a
stale L2 delta, return a null observation, and overwrite its latest
usable native quote. The later-period contract consequently treated
available L1 evidence as missing. Requiring L2 recovery was not justified
for the independently declared L1 markout contract.

The repair preserves an already-received native quote across L2 failure,
preserving its original event ID, exchange and receipt clocks. It never
retimestamps stale data, invents depth, uses future quotes or grants L2
strategy permission. Genuine resets, transport gaps and native quote
invalidity remain explicit quality failures.

The previous `2026-10-scalping-research/retrospective-results.json` is
retained as an immutable, **superseded coverage result**. Its low coverage,
zero later labels and complete-case horizon means must not be interpreted
as unbiased proof of signal quality. The independently confirmed September
broker losses and failed 30-cycle promotion floor still stand.

The prospective dataset fixes the other upstream gap: neutral/no-family
ticks were previously aggregated and could not be reconstructed into full
feature rows. The new observation service persists every frozen ten-second
symbol evaluation, its available features/scores and reasons, with causal
forward labels and quality accounting. Whether this feed meets the
predeclared 95% primary target will be measured, not assumed.
