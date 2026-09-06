# v20 implementation and evidence index

## Status

**Shadow collection works; experimental brokerage operation is not yet certified.
No strategy edge or profitable forward period is established.**

This is an explicit replacement of the v0.10 stop-search policy, not a promotion of
failed configurations. The release introduces only two bounded event hypotheses and one
risk overlay. Operational paper permission is separate from statistical qualification.
The existing holdouts and historical result files were not opened or overwritten.

## Evidence

| Area | Actual result | Artifact |
| --- | --- | --- |
| Independent execution reference | 20 labelled synthetic cases; four confirmed defects and two unresolved provenance categories | [Audit](../research/results/v20-execution-audit.json) |
| Frozen event rules | One H1 guidance variant, one H2 contract variant, R1 overlay; forecasts unknown | [Protocol](../research/protocols/event-v20.json) |
| Extraction benchmark | 18 synthetic cases, correct event type/field counts in all; numeric spans, units, basis, contradictions and injection have dedicated regressions | [Gold](../research/fixtures/event-v20-extraction-gold.json), [evaluation](../research/results/v20-extraction-evaluation.json) |
| Deterministic vertical slice | Evidence → extraction → eligible replay decision → risk approval → synthetic limit fill → exit → reconciliation | [Replay](../research/results/v20-synthetic-replay.json) |
| Real-source demonstration | Three actual licensed news items received; three abstentions; no orders | [Receipt status](../research/results/v20-live-source-shadow.json), [decisions](../research/results/v20-live-source-decisions.json) |
| Primary-source follow-up | Official NVIDIA release acquired with CIK/domain verification; old or unsupported facts abstain, never credited as a fresh trade | [Primary-source status](../research/results/v20-primary-source-shadow.json) |
| Current service | Paper-only shadow event worker and read-only dashboard; actual deployed revision and config in `/api/event-product` | [Operations](V20_OPERATIONS.md) |
| Deployment verification | Worker and dashboard run commit `79076cd`, package `20.0.0`; production migration succeeded; no new paid services | [Deployment record](../research/results/v20-deployment.json) |
| Real preflight | User-authorized preflight still denies paper permission: latest SIP denied and configured IEX fails the frozen feed requirement | [Preflight](../research/results/v20-paper-preflight.json) |

Development demonstration artifacts explicitly retain their then-current commit identity.
The frozen runtime cohort additionally records module hashes, so uncommitted development
code is distinguishable from the final deployed build.

## Confirmed defects and corrections

| Issue | Reproduced before | Now |
| --- | --- | --- |
| Prior quote used as arrival fill | A pre-arrival quote produced a fill | Missing arrival evidence remains unavailable |
| Future quote used in decision | First later quote substituted when no prior quote existed | No future-quote substitution |
| Regular-session order after close | Synthetic 0.25-share order filled $25.25 after close | Rejected there; independent reference schedules next permitted session |
| Fee boundary | $1,000 sale charged $0.02060 Section 31 before its effective charge date | Effective-dated reference gives zero through April 3, 2026 |
| Delay double count | Arrival movement charged again as slippage | Distinct residual assumption; no second charge for that same movement |
| Current cycle exit supervision | Old trade deadline could close the next cycle immediately | Deadline derives from the current owned cycle |
| Pause with partial entry | Pending buy could remain active and prevent a safe exit | Cancel, reconcile actual quantity, then risk-reduce |
| Lease loss | Failed renewal could be ignored | Stale owner broker actions are fenced |
| Future diagnostic receipt | Quote timestamp checked but later local receipt ignored | Receipt must also be available at evaluation time |

Raw/adjusted-price mixing, historical SIP normalization, corporate-action basis and
account-specific historical fee rounding are not resolved by relabeling timestamps.
Legacy economic simulation now raises an explicit unavailable-evidence error.
Archived v0.9/v0.10 numerical outputs remain visible but are superseded for these specific
execution reasons. This is **not** a corrected profitability claim or a fresh external test.

The reported 7,229 partial fills were aggregated over alternative configurations and
unverified historical size semantics. They are not 7,229 observed brokerage fills.
Current latest REST IEX/SIP size schema is separately verified as shares; no blanket
historical multiplication by 100 is applied.

## Sources, latency and extraction limits

- Existing paper account was active and flat, with zero open orders; no new account,
  account reset, subscription or paid service was created.
- Historical SIP retrieval succeeds. Latest SIP quotes return HTTP 403. Latest IEX
  works, but is not NBBO and remains shadow-only in the frozen protocol.
- Existing Alpaca/Benzinga news is accessible. The real demonstration took approximately
  1.48 seconds for its recorded tick; this is one observation, **not latency percentiles**.
- SEC submission discovery, verified CIK-scoped documents and allowlisted issuer URLs
  are supported. Acceptance time is not relabeled as publication time.
- Official Fed, BLS and Nasdaq endpoints returned HTTP 200 in the bounded probe.
  Calendar coverage reached December 30, 2026; unhalted status is freshness-limited.
- Consensus and inference-provider credentials are absent. No model calls were made.
  The deterministic parser handles a narrow documented numerical grammar and abstains
  outside it. Synthetic accuracy is not production extraction accuracy.
- Licensed-news full-text retention is not assumed. Metadata-only items cannot become
  primary-source quantitative trade evidence.

All three real demonstration items were secondary/reaction stories without the required
retained primary quantitative evidence. Abstention was correct. Their arrival on Sunday
does not count as a forward trading session.

## Orders, ledgers and qualification

**Actual new broker-paper orders: 0. Actual calibration orders: 0.**
The replay made zero HTTP calls. Its small positive artificial P&L is excluded from
strategy performance. Read-only broker reconciliation was healthy and flat.

The event allocation starts from $10,000 virtual equity with a $25 entry ceiling,
one position, two entries/session and stricter existing risk limits. The API distinguishes:
broker-paper P&L, economic-paper reserves, cash baseline, diagnostic quote paths,
and unmeasured text/overlay/passive comparisons. No alternative strategy profits are added.

Real event-cohort broker P&L: $0; economic P&L: $0 before unallocated fixed service costs.
Inference cost: $0. Exact fixed hosting/data invoices and net product economics remain
unknown, not zero. No forward alpha statistics are reported.

Usable forward trading sessions: 0. Independent eligible event clusters: 0.
Reconciled real event round trips: 0. DSR/PBO and comparative confidence: not applicable
yet. Later qualification retains DSR ≥ 0.95 and family PBO ≤ 0.20 where valid.

The code includes operational certificates and a durable, quantity-bounded order path;
it cannot clear failed preflight gates. Broker mechanics still require demonstration
during an eligible market session, and the frozen feed gate requires unavailable latest
SIP access. The shadow service must not be advertised as “v20 operational paper trading.”

## Implementation and verification

New modules cover event evidence/sources, deterministic extraction/policy, official risk
context, cohort/certificate policy, market snapshots, durable order reservations and
recovery, allocation ledgers, diagnostic outcomes, replay, commands and dashboard state.
Migration `0006_event_experiments` adds the durable event/cohort tables.

Focused tests exercise causal future revisions, numeric/basis errors, wrong issuer,
injection, duplicate clusters, stale quotes, event expiry, live-host refusal, lost broker
acknowledgements, partial cancellation/exits, cycle deadlines, ownership fencing, paused
risk supervision, immutable cohorts and mocked end-to-end CLI/API behavior.

The final functional suite passed 412 tests and the unchanged 85% coverage gate
(85.02% measured). Ruff, formatting, strict mypy and the full migration
upgrade/downgrade/upgrade cycle passed. No coverage or statistical threshold was lowered.
The prior historical engine's unavailable provenance is not bypassed for test coverage.

Logical implementation commits include `48b5b4a` (paper boundary and durable orders),
`0bdc9ba` (independent reference/quarantine), `574a628` (event/source/context policies)
and `15470de` (integrated pipeline and evidence). Final deployment evidence is reported
separately by the running service's actual commit/configuration fields.

## Remaining limits / next evaluation

Continue acquisition in the existing services at the next regular open:
**September 8, 2026, 09:30 America/New_York** (Labor Day is closed).
Experimental entries remain blocked pending the frozen protocol's usable execution feed,
an operational preflight and a valid primary-source event. No automatic purchase or
unapproved feed-policy relaxation is performed.

Complete event-level ablation/forecast calibration, real extraction accuracy, fixed-cost
economics and forward statistical conclusions only after sufficient recorded events.
No elapsed-time loop can manufacture those observations.
