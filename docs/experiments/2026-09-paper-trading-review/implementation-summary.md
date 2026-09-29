# Paper-scalping implementation and release summary

The September 21 independent report reviewed an older `9194a6f` revision.
The repair and closeout retain the existing Alpaca **paper-only** execution
engine and its qualified `no_support` gate. No new strategy, live adapter,
order-size increase, or retrospective cohort extension was introduced.

## What changed

- `scalping_probes.py` defers idle passive-probe broker reconciliation,
  persists least-tested-symbol allocation through recorded cycle counts,
  and exposes typed `ProbeObservationV2` broker-paper evidence.
- `scalping_shadow.py` distinguishes incompatible probes from strategy
  candidates rather than parsing a neutral probe as a momentum signal.
  `scalping_audit.py` records exclusions.
- `scalping_reporting.py` now scopes cycles by exact frozen policy, bounds
  candidate payload reads to a latest-200 page, aggregates totals in SQL,
  discloses legacy unscoped events, distinguishes reserved intents from
  broker POST attempts, and reports a separate cohort P&L bridge.
- `api.py` binds the public experiment report to the pinned paper account.
  `scalping_policy.py` only calibrates cycles of the exact experimental
  cohort/policy before the entry cutoff, and states the ten-versus-five
  second artifact mismatch.
- `scalping_experiments.py` stops writing five-second acceptance checks
  after a completed round trip or the immutable deadline; neither safety
  supervision nor the experimental entry policy changed.

The two v20 prompts in this folder are **verbatim historical inputs**.
The Tuesday US-equity exercise they describe is not the current crypto
authorization. [The measured result and remaining limits](findings.md)
supersede speculative assertions about this cohort, not its immutable data.

## Verification and operations

The source/report changes were covered by targeted probe, experiment,
reporting and runtime regressions; the complete repository suite passed
with two expected skips. Ruff and strict mypy across 115 source files
passed. A PostgreSQL-dialect regression prevents the JSON-path grouping
failure missed by SQLite-only fixtures.

The final report responded HTTP 200 after deployment, reporting 51 reserved
intents separately from 31 identifiable broker POST attempts. A separate read-only
production calibration audit accepted 14 actual samples, correctly refused
promotion for insufficient support, and wrote no model artifact. A second
read-only job proved production probe payloads no longer crash the
historical reader. The live runtime snapshot reported `autonomy_expired`,
no inventory, no unresolved orders and no unresolved ownership; the
qualified model remained `no_support`.

The existing Render event/scalping worker is live on `7f958c4`
(`dep-dau20hvlot8c739c971g`); the dashboard is live on `4147ac7`
(`dep-dau22kflot8c739cfvr0`). The dashboard contains the later read-only
submission-count correction; its schema is compatible with the worker. The
legacy shadow worker remains suspended. The original v20
equity prompts are archived here for traceability, not a reauthorization.
The next research decision, if any, requires a separately frozen hypothesis
and a compatible cost/evidence contract. There is no automatic promotion
or real-money readiness claim.
