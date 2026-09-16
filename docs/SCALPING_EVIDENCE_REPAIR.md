# September 16, 2026: scalping evidence repair

The active paper collector uses a pinned `no_support` economic model with
`NO_SAMPLES`. A running worker therefore does not imply an authorized trade.
The September 14 cohort has no validated profitable model; collecting more
unvalidated counterfactuals alone cannot establish one.

## Corrected implementation

- Resolve simulated arrival and horizon deadlines from evidence already
  received by those deadlines. A later quote cannot supply an earlier fill.
- Retain native book events during database replay. Seed bounded replay only
  from recorded candidate observations and retain the depth's own timestamps.
  Untimed historical depth is reduced to the recorded top of book.
- Keep current L1 quotes independently of stale L2. Strategy eligibility still
  requires current depth; old initial snapshots still require a current update.
- Remove obsolete better levels after the top of book changes. Recheck depth
  freshness at the simulated action deadline, including gaps between events.
- Do not let a delayed pre-arrival trade fill a simulated passive order. Local
  continuity loss makes the affected outcome incomplete.
- Mature online outcomes through processed market time. Serialize persistence
  and acknowledgment, count each committed outcome once, and release completed
  source-event buffers.
- Preserve the configured cancellation horizon in economic samples.
- Keep independent historical execution validation separate from the newer
  calibration population. Recompute its proof, reject overlapping/duplicate
  evidence, and require the same simulation policy and account. All existing
  completeness, calendar, sample, fee and positive holdout requirements remain.
- Show model blockers and collection diagnostics on the dashboard. Historical
  account totals are explicitly distinguished from current-model activity.

New simulations use `shadow-action-policy-v2`; archived v1 outcomes remain
readable but are not silently relabeled with the corrected semantics.

## Reproducible audit

`tradeagent scalp-shadow-audit` reads immutable candidates, historical paper
execution evidence and recorded native market data. It writes `report.json`,
`summary.json` and `model.json` to a new directory. It does not use the broker,
take an execution lease, modify database evidence or deploy the model.

Example inside the existing Render service, with explicit UTC boundaries:

```sh
tradeagent scalp-shadow-audit \
  --cohort-id v30-action-value-20260914-seven-day-r1 \
  --historical-start 2026-09-10T22:00:00+00:00 \
  --historical-end 2026-09-11T22:00:00+00:00 \
  --at 2026-09-16T16:00:00+00:00 \
  --valid-until 2026-09-21T19:22:58.996+00:00 \
  --output-dir /tmp/scalping-evidence-audit-20260916
```

Use the actual audit cutoff and a unique output directory. The command rejects
overwriting an earlier experiment or silently truncating a large population.

## What must happen before trades

The corrected simulator must pass its independent execution validation; enough
complete chronological samples must satisfy the predeclared seven-day calendar
and sample requirements; the held-out action return must remain positive after
fees, execution costs and uncertainty. A qualifying artifact must then be
pinned by hash and deployed. Each subsequent entry still needs fresh data and
positive supported action value.

A failed audit remains a failed experiment. If the recorded venue evidence is
insufficient to validate execution, or costs exceed the strategy's edge, waiting
or bypassing the gate cannot fix that. The data/strategy needs further research.
Paper simulation is not evidence of live profitability.

The current Render authorization ends September 21 at 15:22:58.996 Eastern.
This repair does not extend it or create any assistant follow-up automation.

## Local release validation

Ruff formatting and lint, strict mypy (113 source files), the full repository
suite (1,849 passed, two skipped), distribution build and the seeded offline
demo pass. The final dashboard wording also passes all 14 API tests. The SPY
synthetic demo produced 41 simulated fills and a negative return; it checks the
offline execution path and is not evidence that the crypto strategy is profitable.

## Stored-data experiment at 2026-09-16 15:00:14 UTC

The first Render audit stopped on a JSON decoding error: strict nested economic
decisions cannot be parsed as arbitrary Python dictionaries containing JSON
strings/lists. Both the audit and restart-recovery readers now use JSON model
validation. The regression fixture includes a persisted economic decision.
The follow-up full test run exposed a pre-existing reporting fixture race when
Windows recording timestamps tied. That fixture now supplies explicit
chronological occurrence times; reporting behavior is unchanged. All 25 focused
loader, shadow and reporting regression tests pass.

A read-only retry with that input-decoding correction completed:

- Historical population: 229 candidates, 227 comparable passive orders.
- Passive validation: 52 false negatives, 175 true negatives, no predicted
  fills; precision zero and validation failed.
- Current population: 1,013 candidates / 2,026 action outcomes.
- All 1,013 passive outcomes were incomplete. Only 27 aggressive outcomes were
  complete, and aggressive execution has no qualifying historical validation.
- Dominant gaps: stale horizon quotes, no fresh quote at simulated arrival,
  and a changed passive touch. Local continuity loss also affects some windows.
- The largest cells span about 1.8 days, below the unchanged seven-day minimum.
- Zero samples accepted; resulting model `no_support`, reason `NO_SAMPLES`.

The failed model artifact has SHA-256
`d20eea4258735184010fbda07dc4fb3b2f65e9205f0ea6a4d29129a0ba0bc9fa`.
It was not deployed. The existing pinned model remains unchanged.

This experiment does not show an improvement in passive fill validation.
Historical missing data cannot be reconstructed by fixing the collector.
Before a trade-capable model is possible, an independently validated execution
model and sufficiently complete prospective evidence are still required.
The existing collector alone has no demonstrated path to a qualifying model.
