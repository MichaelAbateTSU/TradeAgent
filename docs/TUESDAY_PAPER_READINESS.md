# Tuesday, September 8, 2026 — paper readiness

## Safety closeout — supersedes the earlier armed snapshot

The `-r2` cohort was paused for four bounded safety repairs before Tuesday:
cross-cohort recovery/completion, database latency after final dispatch guards,
provably-unsent reservation accounting, and feed-only revision availability.
The corrected release uses a **new immutable `v20-tuesday-20260908-r3` cohort**.
The earlier deployment JSON and all cohort manifests remain historical evidence,
not permission to rely on the old completion matrix.

**Corrected release verified September 7, 2026, 09:00–09:06 UTC (05:00–05:06 Eastern).**
No Tuesday result or preparation trade is claimed. See the corrected
[protocol](TUESDAY_PAPER_PROTOCOL.md) and immutable
[r3 deployment evidence](../research/results/v20-tuesday-20260908-r3-deployment.json).

| Current release | Verified value |
|---|---|
| Exact code on worker, dashboard and notifier | `40f3ee2a6d2dc103e2eccf8faa71872277a491bd` |
| Immutable cohort | `v20-tuesday-20260908-r3` |
| Effective configuration | `a7ab993c246d353797a001809e398e09a578e17e96f98d8a41172d9cb5832bd5` |
| Worker / dashboard / notifier deployments | `dep-daf7ni0n74is738nt4h0` / `dep-daf7nhlg1s2s73dcpua0` / `dep-daf7nhlg1s2s73dcpu00`, all live |
| Same-service paper preflight | `job-daf7oc0u01pc7397j9ug`, succeeded; all certificate checks true |
| Certificate | `de736f29d917289329cf70dc29b3734952b52b1c06161b16bbab3b140fb51f23`, issued 08:56:45 UTC |
| Saved account/lease/report verification | `job-daf7qcn40ujc73a1g3mg`, succeeded |
| Saved notifier/report verification | `job-daf7qcon74is738o9vqg`, succeeded |
| Paper account | Same fingerprint below; active/unblocked, **zero positions, open orders and event-order history** |
| Account/session budget | **0 of 2 total, 0 of 1 news**; original stable session/equipment IDs unchanged |
| Migration | `0009_candidate_states`; no schema change required |
| Full validation | **723 passed**, **86.51% coverage** above unchanged 85%; Ruff check/format and mypy pass |
| Independent final review | `practice-review`: **no significant remaining issues**, 22 independently executed memory-only checks; confirmed before deployment |

The parent-issued pause `job-daf7c9qd0e5s73b57frg` succeeded at **08:31:09 UTC**.
The r2 `OPERATOR_PAUSE` is still persisted. Only the new r3 preflight restored the
global operational permission; the old cohort was not unpaused or rewritten.
The current cohort has no pause/blockers. Normal worker/notifier lease expiry and
restarts completed without manually stealing or deleting locks.

### What the safety closeout established

- Prior-cohort full/partial equipment and partial-exit recovery use verified account/
  session links, original frozen settings and current global worker ownership.
  Unverified quantity is never liquidated; old UNKNOWNs remain EOD incidents.
- Slow audit/link writes occur **before** final equipment/news cutoff and quote-age
  checks. Prepared is not submitted. A crash after broker acceptance but before
  attempt logging remains UNKNOWN, reserved, and recoverable by its original ID.
- Only **definitively never-submitted** local expiry releases budget under the same
  account/session lock. Release is idempotent; rejected/submitted/UNKNOWN attempts
  stay consumed. Claims, tickets and equipment identity remain preserved.
- A new feed-only revision becomes available no earlier than observation of that
  changed feed evidence. Identical/restarted versions retain original receipt times.
  Publication and provider revision are not substituted for receipt.
- The older-denominator correction veto and immutable extraction-packet hash
  regression remain intact. No source permissions, thresholds or strategy scope changed.

### Actual current collection and reporting

The worker lease owner is `srv-dae4tr7qj5pc73a9e0k0-597b8489bd-tx5gx`,
matching the saved heartbeat owner. The final sampled heartbeat at **09:05:52 UTC**
has no blockers or market errors; actual configured-source acquisition is fresh,
and the paper trade-update stream is authenticated and subscribed.

Premarket brief `cc6c556e-a2ca-40bf-9804-139ee5f05ef0` covers the configured sources
from **September 4, 20:00 UTC through September 7, 09:05:14 UTC**. Its roughly
37-second observation tail is explicitly uncovered, not claimed as complete news.
This r3 snapshot has **521 raw receptions, 70 source-health checks, 23 immutable
evidence versions / 21 events, two revision versions and two older context documents**.
It has **zero valid quantitative events or strategy candidates**; no news trade is owed.
IEX, AAPL publication uncertainty, Microsoft Cloud-only coverage/optional >5 MB context
gap, metadata-only licensed news and other finite-source limitations below remain.

Saved report **`f9fe03ef-0e44-5f6e-81a9-dd72201c9eff`**, **09:00:59 UTC**, is
`NOT_STARTED`; two independent API reads returned identical immutable content.
[Open the corrected readiness report](https://tradeagent-runtime-dashboard.onrender.com/api/event-session-report?report_id=f9fe03ef-0e44-5f6e-81a9-dd72201c9eff).
Monday's flat account is explicitly **not** Tuesday's broker-confirmed ending state.

The notifier heartbeat was fresh at the **09:01:01 UTC** probe. It persists reports,
retains the approved Resend sender/recipient and **18:00 America/New_York** schedule,
and retains exactly two previously sent daily messages. Verification sent/enqueued
no test email. Next daily update is Monday September 7 at 18:00 ET; Tuesday's report
and provider acceptance remain pending. Original shadow deployment, empty database
public allowlist and both sealed manifest hashes are unchanged.

**Pending Tuesday:** actual completed opening observations, conditional one-time
equipment result, any genuine rule-supported news opportunity, verified recovery/
ending exposure and 18:00 report delivery. Formal qualification remains excluded.
The historical r2 matrix below is not evidence of any future Tuesday outcome.

## Historical r2 verification (superseded; retained for audit)

**Verified September 7, 2026, 08:11–08:13 UTC (04:11–04:13 Eastern).**
The existing Render environment is armed for a **conditional, paper-only operational
and news experiment**. Tuesday has not started. No preparation orders, simulated
forward days, Tuesday fills, profits or completed session are claimed.

## Release and permission

| Item | Verified value |
|---|---|
| Deployed code, event worker/dashboard/notifier | `c01aa6af532e73982704ddb062832ff52b2ff96f` |
| Immutable cohort | `v20-tuesday-20260908-r2` |
| Effective configuration hash | `e5973412c8d687523c6442efffbfd84ea1bbea57f61ae1c1972866779385393b` |
| Purpose / feed | `iex-practice` / real-time IEX; **not qualification evidence** |
| News-worker deployment | `dep-daf702lg1s2s73da7p20` |
| Dashboard deployment | `dep-daf70rqd0e5s73b3p580` |
| Notifier deployment | `dep-daf70ruq1p3s73bv9qag` |
| PostgreSQL migration | `0009_candidate_states`, after lossless `0008_control_values` |
| Same-service preflight job | `job-daf70rpt0dsc73crgh90`: passed; every check true |
| Certificate | `f3c10821b5d64f6d80a1aa20cfd90e81dc116869c6c0d361cdb159d5ea6b3e58` |
| Certificate issue time | September 7, 08:06:36 UTC |
| Paper host | `https://paper-api.alpaca.markets` |
| Account fingerprint, not a credential | `b3ddf697092ad516c7e68024af06916f7c5dc921c8dfd44ff35ea6618b44947f` |
| Actual account | Active, unblocked, **zero positions and zero open orders** |
| Entry budget | Durable account/session record: **0 of 2 total, 0 of 1 news reserved** |
| Equipment order | Not submitted; scheduled, conditional |

The certificate lasts 24 hours; the worker renews operational permission from the
persisted matching operator confirmation and fresh safety checks before an eligible
action. Expiry does not fabricate consent, bypass current checks or require an
unattended job to manufacture a signal.

The code/configuration/source/extraction/cost manifest, runtime module hashes,
service commands, deployment IDs and actual probe results are preserved in
[`v20-tuesday-20260908-deployment.json`](../research/results/v20-tuesday-20260908-deployment.json).
The short frozen rules are in [TUESDAY_PAPER_PROTOCOL.md](TUESDAY_PAPER_PROTOCOL.md).

The initial checklist release `540fe42` / `v20-tuesday-20260908` remains immutable.
A final report fallback for missing-worker calendar evidence required the separately
labeled `-r2` release. The initial setup had no orders. The earlier
`v20-iex-practice-20260908-r2` and all other old cohorts remain intact. No thresholds
were adjusted to generate activity.

## Effective limits and Tuesday clocks

- Universe: **AAPL, MSFT, NVDA** only; long common equities, fractional when supported.
  No leverage, shorts, options, crypto, live endpoint or extended-hours entries.
- Virtual capital **$10,000**; entry at most **$25**; minimum order **$10**.
  One position **including pending exposure**; two total reservations, one news entry.
  Broker-submitted rejections consume attempts; local checks do not. Conservative
  pre-dispatch reservations remain consumed but are not mislabeled as broker attempts.
- Daily allocation loss stop **$50**; drawdown stop **$150**. Existing stricter limits win.
- Broker calendar confirms **September 8, 09:30–16:00 ET** (13:30–20:00 UTC).
  Previous regular close is **September 4, 16:00 ET**, not Monday's holiday.
- Equipment window **09:35–10:00 ET**: completed opening bar, at least 20 observed
  completed daily sessions, two-second processing delay, fresh quote and all gates.
  Cancellation target about **30 seconds**; liquidation target about **60 seconds**
  from original submission. Missed targets and unresolved remainder remain explicit.
- News uses its existing **60-minute** maximum holding horizon, not the equipment
  minute. It must fit before **15:50**, making its effective latest entry **14:50 ET**.
  Final dispatch repeats this horizon check. Thus **no news entry after 15:00** is
  possible despite the broader intraday configuration's **15:30** cutoff.
- Flatten **15:50**, hard deadline **15:55**, actual broker session close **16:00**.
  Recovery continues if broker confirmation is missing; a deadline is not proof of exit.
- News receipt age at most **30 minutes**; publication-to-receipt at most **15 minutes**.
  Quote age at most **5 seconds**; spread at most **10 bps**; minimum price **$5**;
  median daily dollar volume at least **$50 million**.
- H1 guidance increase at least **1%**, comparable metrics/periods; H2 binding-contract
  value at least **5%** of observed annual revenue, with duration/economic-share inputs.
  Unknown reference price, annual comparison, consensus or forecast is never invented.

Persistent equipment identity:
`56d5a4fe2bc837c2d7d5753cb38a109507ca654cf569aa7310372a6da117b06d`.

Persistent account/session identity:
`f31657d996309a19bf68884023e8f168e9ff098f0c3111f9953b4770323ef5c9`.

Preparation initialized the **absent** budget with the normal locked
`INSERT ... ON CONFLICT DO NOTHING` path only after confirming an entirely empty
event-order history and flat paper account. No existing counter, account or test
was reset. Job `job-daf73muq1p3s73bvmslg` recorded this and persisted the report.

## Worker and data evidence — not merely dashboard health

- Event owner `srv-dae4tr7qj5pc73a9e0k0-7679cb6845-dxr7x`; its lease and heartbeat
  matched at **08:10:44 UTC**. Latest sampled heartbeat **08:13:39 UTC**:
  `market_closed`, no blockers, no market errors.
- Actual source success **08:13:39 UTC**. Licensed news, all three fixed official
  feeds and all three SEC interval checks reported healthy.
- Actual paper trade-update stream subscribed at **08:09:49 UTC**:
  one authenticated connection, two handshake messages, zero order updates,
  zero errors/gaps. REST remains the fill/position authority.
- Premarket brief `2a5bc423-90f0-4dc9-a3e0-c01c2652ac23` was prepared before open.
  Configured-source coverage starts **September 4, 20:00 UTC**, through the last
  verified poll **September 7, 08:13:36 UTC**. The approximately two-second
  observation tail remains explicitly uncovered; no “all news through now” claim.
- That snapshot contains **371 raw item receptions**, **49 source-health
  checks**, **23 unique evidence versions / 21 events**, two revision versions and
  two older context documents. Re-polls/cache hits are not independent news.
  The final cohort reused observed immutable receipts rather than backdating new ones.
  **Zero valid quantitative events or strategy candidates**; no pending extractions.
- Order evidence separately confirms **zero entry attempts, filled orders and round
  trips**. News-brief execution fields intentionally stay uncounted there. Individual
  execution-count and risk-approval fields without observations remain unknown,
  rather than invented favorable zeros.

### Exact coverage limitations

These are visible capability gaps, not secretly loosened entry conditions:

1. **Apple:** the allowed Atom feed supplies `updated`, not exact original publication.
   Date-only article metadata cannot authorize a timestamp-sensitive trade.
2. **Microsoft:** verified Cloud Blog feed is **not full corporate/IR coverage**.
   The old investor RSS URL returned 404; current IR links outside the existing
   allowlist were not silently enabled.
3. **Microsoft older SEC context:** the attempted optional comparison document exceeded
   the existing **5 MB** size bound. Its annual denominator is unavailable from that
   attempt. A missing figure blocks that contract rule. Do not raise the bound or
   invent a denominator during Tuesday; obtain an appropriately bounded, permitted,
   timestamped comparison for a separately reviewed follow-up if needed.
4. Licensed Alpaca/Benzinga content remains **metadata-only** under current permissions.
   No body-retention or paid entitlement was added. SEC acceptance is not publication.
5. Finite feeds and recent SEC submissions do not establish complete archives or
   historical revisions. Older documents downloaded now do not establish pre-event
   knowledge. Deterministic extraction recognizes only the frozen explicit numerical
   grammar; consensus and configured inference are unavailable.
6. IEX observations are one venue; paper fills use Alpaca's NBBO simulation.
   Live SIP remains unavailable to the original SIP-gated research cohort.

The correct current source conclusion is **healthy bounded coverage**, not complete
company-news coverage or proof that a qualifying Tuesday event will exist.

## Checklist completion matrix

| Section | Implemented and verified now | Pending Tuesday / limitations |
|---|---|---|
| 1. Preserve system | Exact pinned services, account, leases, flat state, historical cohorts and sealed hashes checked | Original shadow recorder is untouched; its closed-market heartbeat is not current market evidence |
| 2. Small scope | Paper-only host/account guards, $25 cap, one slot, account/session atomic budgets and stable IDs; failure/restart tests | Actual broker submissions/fills |
| 3. Calendar/news | Broker/NYSE agreement; holiday-weekend backfill and durable premarket brief; real deployed source checks | Continuous updates; specific coverage gaps above |
| 4. Frozen protocol | Code/config/module/source/extractor/cost identities, ranking, completed-bar/next-frame gates frozen | Actual opening observations; no tuning to force entry |
| 5. Structured evidence | Immutable source versions, timestamps/references, issuer mapping, quantitative extraction, comparisons and missing fields | No validated fresh numerical event yet |
| 6. Educated trade | Ranked alternatives, fresh re-evaluation, current-revision veto, persisted ticket and unchanged news exits | Optional genuine news trade; expected net edge remains unknown |
| 7. Equipment check | Account/session identity, finite window, classification, cancel/exit targets and lateness visibility tested | Conditional one-time check or honest `MISSED`/rejected outcome |
| 8. Recovery | Unknown submissions, cancellation races, late/partial fills, partial exits, out-of-order stream hints and final flat-plus-no-orders checks tested | Real broker behavior and any incident recovery |
| 9. Economics | Separate equipment/news/combined ledgers; unchanged base costs and 1.5x/2x/3x stresses; no double spread; explicit denominators | Actual fills/cost estimates; passive benchmark unavailable |
| 10. No-trade evidence | Source/decision funnel, observed failed rules, health distinction and diagnostic markouts implemented | Timestamped real markouts and end-of-day explanation |
| 11. Failure verification | **709 tests**, **86.55%** coverage above unchanged 85% gate; Ruff and mypy pass | Fixtures are not Tuesday fills or elapsed forward days |
| 12. Readiness/report | Durable report and immutable API verified; actual notifier settings, existing outbox and fresh heartbeat verified | Tuesday 18:00 email acceptance and broker-confirmed ending state |
| 13. Usable evidence | Equipment excluded, all genuine news outcomes retained/versioned, no qualification promotion | Formal evaluation not started; existing eligibility/statistical gates remain |

Validation used existing tools:

```powershell
.\.venv\Scripts\python.exe -m pytest --cov=tradeagent --cov-report=term:skip-covered
.\.venv\Scripts\python.exe -m ruff check --no-cache src tests migrations
.\.venv\Scripts\python.exe -m ruff format --check --no-cache src tests migrations
.\.venv\Scripts\python.exe -m mypy src\tradeagent
```

The full run had 16 non-failing dependency-deprecation/SQLite resource warnings.
No threshold was lowered or dependency added. Direct final review added regressions
for insufficient real history, fixed-feed configuration, newly known corrections,
equipment target lateness and final news-horizon expiry.

## Report and notifications

Persisted report **`4b3c138a-2045-5b9c-b223-411b7543625c`**, snapshot **08:12:26 UTC**:
`NOT_STARTED`, recorded budget, healthy bounded coverage. Two independent API reads
returned identical immutable report content:

[Open the readiness snapshot](https://tradeagent-runtime-dashboard.onrender.com/api/event-session-report?report_id=4b3c138a-2045-5b9c-b223-411b7543625c).

The report intentionally does **not** call Monday's flat account Tuesday's ending
state. Tuesday's final quantities/orders remain unconfirmed until actually observed.

The existing notifier is enabled at **18:00 America/New_York**, unchanged approved
sender/recipient and Resend provider. A same-notifier-service job rendered and
persisted a real current report without enqueueing or sending an email. The existing
outbox retained **two previously sent daily messages**, no new test dispatches.
Fresh notifier heartbeat: **08:13:07 UTC**, checked at **08:13:10 UTC**.
Round-trip notifications remain on the original idempotent outbox path.

Next daily update: **Monday September 7, 18:00 ET**; requested session report:
**Tuesday September 8, 18:00 ET**. Report persistence precedes delivery; provider
failures and ambiguous sends remain visible rather than generating blind duplicates.
Tuesday provider acceptance is pending, not claimed.

## Preservation and next actions

All four existing services have automatic deploys **off**. PostgreSQL remains available
with its public IP allowlist empty; no public database access was enabled. No unrelated
project, plan, recipient, paid subscription or resource was changed. The original
shadow service remains on `2a1db33e7cf4f596e8fce9aac47c4e1e8c854630`.
Lease handoffs waited for normal expiry/restarts; no leases were manually stolen/deleted.
One preparation audit initially assumed notifier-only email settings existed on the
news service; corrected role-specific probes succeeded without sending mail or orders.

Both sealed manifests retain their original SHA-256 values; underlying holdouts were
not opened:

- `data\intraday-holdout.json`:
  `1D0AB69F5FF21BA57F292802C89C819459DFEE8CBF831FC72DF55C28FE8882F9`
- `data\intraday-1m-holdout.json`:
  `BAD71D82604333493185C9084C40D2E52514AAFE44704FED6FF6E20DC6B410B0`

Let the frozen worker continue collecting. At Tuesday's opening, observe the actual
bar/history/quote checks, conditional equipment lifecycle and any real news decision.
Do not create a news trade because the funnel is empty. Reconcile any uncertainty and
continue permitted exits. Review the persisted 18:00 report before proposing a
separately labeled correction.

Formal qualification remains blocked on the original eligible-feed/protocol/evaluation
requirements and genuine inclusive evidence: at least **60 sessions / 60 reconciled
round trips**, plus the existing statistical gates. Practice contributes **zero**.
