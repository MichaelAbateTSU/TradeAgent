# v20 event-paper operations

## Visible policy change

The September 6, 2026 v20 assignment replaces the v0.10 stop-discovery direction.
Only H1 (comparable numerical guidance revision), H2 (quantified binding contract), and
R1 (verified event/macro risk) enter this bounded program. Previous strategies are not
promoted. Both sealed holdouts and all archived failures remain unchanged.

Operational permission and statistical qualification are separate. A signed-in paper
account, deterministic mechanics, a frozen cohort, and current valid inputs can permit
a small experimental paper order without historical alpha. No code enables live money.

**The original research cohort remains SIP-gated and unqualified.** Latest SIP access
is denied; its frozen protocol keeps the non-NBBO IEX feed shadow-only. The September 7
operator request adds a separate, opt-in **IEX paper-practice** purpose, described below.
It does not change that research cohort or purchase a subscription.

## Commands

```powershell
tradeagent doctor
tradeagent source-capabilities
tradeagent audit-execution --output research\results\v20-execution-audit.json
tradeagent evaluate-extraction
tradeagent event-replay
tradeagent experiment-freeze --cohort-id <new-cohort>
tradeagent news-record --once --cohort-id <shadow-cohort>
tradeagent run --mode shadow --cohort-id <shadow-cohort>
tradeagent paper-preflight --cohort-id <new-paper-cohort>
tradeagent run --mode experimental-paper --cohort-id <new-paper-cohort>
tradeagent reconcile --cohort-id <cohort>
tradeagent experiment-report --cohort-id <cohort>
tradeagent risk-pause --cohort-id <cohort>
```

`paper-preflight --confirm-experimental-paper` records the explicit confirmation,
but issues permission only when its operational checks pass. A failed preflight
does not clear the kill switch. An experimental worker cannot turn a failed preflight
or a rejected event into an order. Per-action source, quote, session, asset, capital,
position, rate, and loss checks still apply after certification.

`event-replay` uses a synthetic gateway with **zero network calls** and an in-memory
database. Its synthetic entry, exit, email-outbox item, and P&L never enter real cohorts.

## Free-IEX paper practice

The deployed Tuesday release, actual verification evidence and remaining source
limitations are recorded in [TUESDAY_PAPER_READINESS.md](TUESDAY_PAPER_READINESS.md).

This is an operational drill, not a new strategy family or evidence of profitability.
Use **real-time IEX** quotes already available on Alpaca Basic, not 15-minute-delayed SIP.
The practice purpose is explicit, starts on a declared date, and requires its own immutable
cohort and operator-confirmed preflight. The normal `research` purpose still requires SIP.

```powershell
tradeagent paper-preflight --cohort-id v20-tuesday-20260908-r2 --purpose iex-practice --practice-start-date 2026-09-08 --confirm-experimental-paper
tradeagent run --mode experimental-paper --cohort-id v20-tuesday-20260908-r2 --purpose iex-practice --practice-start-date 2026-09-08
```

The planned first session is **Tuesday, September 8, 2026**. The existing 09:35 Eastern
entry warm-up remains in effect. Between 09:35 and 10:00, the worker may make **one**
operator-calibration AAPL limit-entry attempt of at most $25, subject to all account,
quote, liquidity, halt, macro, lease, pause, exposure and loss checks. Unfilled entry
orders expire after 30 seconds; the risk supervisor targets an exit after 60 seconds
from submission. These are deadlines for supervision, not guarantees of fills.

The calibration has no fabricated news article or trading signal. Its durable claim,
client order ID and order links survive restarts. A missing or rejected fill is recorded
honestly, not replaced by a fresh forced attempt. A missed morning window is not replayed
the following day. After the calibration attempt, only actual H1/H2 events meeting the
existing source and signal rules can request another entry, within the same two-entry
daily cap. There is no promise of a trade on a day with no valid event.

The detailed Tuesday protocol adds **at most one news-strategy entry**. The two-total
budget and `EQUIPMENT_TEST` identity are bound to the paper account and planned session,
not the deployment/cohort name. Submitted rejections consume the budget; a candidate
check alone does not. Reserved-but-never-dispatched intents are reported separately.
New entries are restricted to the declared September 8 session; later sessions are not
silently presented as Tuesday. Risk recovery remains active after an incident.

The broker calendar independently confirms the regular session and the preceding
regular close. For this test the collection window starts **Friday, September 4, at
16:00 Eastern**, including the holiday weekend. The runtime persists and updates a
premarket brief before Tuesday's open. Collection eligibility never changes the
30-minute event-age, publication-latency, completed-bar, or quote-freshness rules.

At 09:35 the equipment test still needs the actual completed opening observation,
its receipt plus the existing processing delay, and sufficient observed daily history.
The clock alone is not permission. A missed window records `MISSED` with its prior
blocking evidence, not an invented fill.

Eligible news observations remain immutable while separate durable execution states
allow waiting candidates to be evaluated again. Equipment exposure/pending orders
block news entry. After the slot clears, the worker obtains a fresh quote and repeats
the complete source, price-reaction, session and risk decision. Simultaneously eligible
candidates use **latest verified primary receipt, then symbol, then evidence ID**.
Ranking alternatives and subsequent eligibility changes are recorded before selection.
Newly received related corrections/retractions veto a queued candidate without rewriting
its frozen extraction packet or earlier decision.

Every news entry has a durable ticket with source references/excerpts, comparable facts,
original and current decisions, observed price reaction, limits, actual order sizing,
invalidation conditions, the existing maximum 60-minute holding period, session exit
deadline and the frozen cost model. There is no invented price stop, profit target,
consensus surprise or positive expected return. The equipment test's 60-second exit
does not apply to the news position.

Risk-exit link identities are hashed to fit PostgreSQL's fixed-width key column, including
long production cohort names. The final dispatch rechecks quote age and the calibration
deadline after broker lookups. Temporarily denied operational checks can be re-evaluated
on the next eligible tick without resetting an order claim or granting missing consent.

Apply `0008_control_values` before preflight: complete certificates require a `TEXT`
control value, not the original 500-character field. The first production setup attempt
hit that storage limit before authorization was saved and submitted no orders. Its
`v20-iex-practice-20260908` cohort is preserved; the corrected release uses the separate
`-r2` cohort rather than rewriting a frozen configuration. The expanded Tuesday checklist
uses the new cohort named above. Its initial unsuffixed setup is preserved after a
final missing-worker calendar-report fallback repair; the unsuffixed setup submitted
no orders during preparation. Downgrade refuses to truncate certificates or other
long control values. Apply `0009_candidate_states` for deferred-candidate state; its
downgrade also refuses to discard populated history.

Runtime permission renewal requires the original explicit confirmation to match the
same account, cohort, code and configuration. A global replay attestation alone cannot
authorize another cohort. Intraday/risk settings and critical runtime modules are
included in the frozen fingerprint. Any changed code/settings require a new cohort.

Practice quotes must have valid positive sizes, the declared feed, causal receipt times
and an age of at most five seconds. Fifteen-minute-old quotes cannot authorize an order.
Partial fills, unknown submission outcomes and account mismatches retain the existing
cancel/reconcile/pause behavior; owned-position risk exits continue during pauses.
The paper-only trade-update stream is a reconciliation hint, never fill authority.
Updates are retained with their timestamps and checked against REST orders/positions.
Known out-of-order updates cannot reduce confirmed fills. Cancel acceptance is not
cancellation confirmation; late fills remain owned. Stalled partial exits request
cancellation before a replacement exit for the reconciled remaining quantity.

Practice broker fills and dollar P&L remain factual **broker-paper** observations. Any
cost-adjusted figures remain illustrative single-venue practice estimates. All practice
sessions and round trips contribute **zero** to strategy qualification, including the
60-session/60-round-trip floor. Dashboard, API, daily mail and round-trip mail identify
practice; none may describe these results as validated economic alpha.
Equipment and `NEWS_STRATEGY` ledgers are separate. The news ledger retains every actual
news outcome, including losses. Base residual-cost rates remain unchanged; 1.5x, 2x and
3x stresses are separate. Spread is not charged again on top of broker fill prices.
Returns state the actual deployed-notional and allocated-capital denominators. Cash is
the declared zero-interest comparison over matching timing/capital; missing passive
benchmark observations remain unknown. No meaningful Sharpe is inferred from two trades.

No real-money orders, new brokerage account, account reset, AI subscription, market-data
purchase or additional long-lived Render service is part of this change.

## Runtime and boundaries

- Existing PostgreSQL and Render services are reused; no new paid resource is created.
- Original research command: `tradeagent run --mode shadow --cohort-id v20-forward-shadow-001`.
  The separate practice command above uses a different cohort and explicit purpose.
- The original quote-stream recorder, notifier, and dashboard remain available.
- The event worker replaces the news worker's command, retaining its licensed-news
  storage and heartbeat while adding source versions, extraction, decisions and context.
- Apply migration `0006_event_experiments` before starting it.
- Event worker and dashboard are pinned to the verified release with automatic deploys
  disabled. Explicitly deploy a new code revision with a new cohort rather than allowing
  a documentation push to invalidate a running cohort.
- One worker lease and serialized, durable allocation reservations govern order entry.
  Lost ownership fences broker calls. A submission timeout becomes UNKNOWN: query the
  same client ID and reconcile, never blindly submit again.
- Startup and every tick supervise owned positions before source ingestion. Pauses and
  feed failures cancel unsafe pending entries; exits sell only reconciled owned quantity.
- The virtual allocation is $10,000, with at most one position, at most $25 per entry,
  at most two entries/session, a $50 daily loss ceiling and a $150 drawdown ceiling.
  Any stricter existing configuration wins. Fractional limit quantities are rounded
  down so the limit notional cannot exceed the cap.
- No overnight positions, shorts, leverage, crypto, options or extended-hours entries.
  A missed exit is an operational incident, not a guaranteed stop price.

The immutable cohort binds the policy, settings, actual code commit and normalized
runtime-module hashes. A changed code/model/prompt/configuration requires a new cohort;
it cannot silently continue the old experiment. Account switching/reset pauses entry.
Deposits are not credited as virtual-allocation trading profit.

## Sources and configuration

Existing `ALPACA_*`, database and notification credentials are reused. No inference
provider is configured: the declared fallback is a narrow deterministic source-span
extractor and inference budget is zero calls/day. Unknown facts and forecasts remain null.

The current common-stock universe is AAPL/MSFT/NVDA, verified with fixed CIKs and
broker asset eligibility. This is not a historical security master.

SEC polling uses `NEWS_CONTACT_EMAIL` or `EVENT_SEC_CONTACT_EMAIL` as an identifying
User-Agent. Trusted issuer release URLs may be configured with `EVENT_PRIMARY_URLS`
as a JSON mapping; URLs discovered inside arbitrary articles do not grant network authority.
Original content and receipt versions are retained only under the source's retention
profile. Licensed news defaults to metadata retention; do not enable body retention
without verifying the account's rights.

Fixed official-feed discovery is enabled for Apple Newsroom, NVIDIA Newsroom and
Microsoft Cloud Blog within the existing issuer-domain allowlists. It is not arbitrary
web crawling. NVIDIA and the permitted Microsoft feed supply genuine publication times;
the Microsoft feed is not complete corporate/IR coverage. Apple's Atom `updated` and
date-only article metadata do not establish an exact publication time. Such gaps remain
explicit. SEC acceptance is not relabeled as publication, and older documents first
downloaded now are not backdated into pre-event knowledge.

Fed meeting days, the BLS ICS calendar and Nasdaq halt RSS are polled with caching.
Date-only Fed meetings block the full day rather than inventing announcement times.
Missing, stale or malformed context abstains.

## Operator view and clocks

The existing Render notifier sends one **daily agent-status email at 18:00
America/New_York**, including weekends and holidays, to the already configured
`EMAIL_RECIPIENT`. It reports actual worker/cohort status, today's decisions,
last-recorded broker/economic allocation P&L, blockers, and deterministic next steps.
Stale data and unknown costs stay explicitly labelled; email never grants order permission.
Capability limitations such as deterministic-only extraction are separated from hard
blockers. Practice emails report the declared session and calibration state rather than
telling an authorized IEX practice run to purchase SIP data.

Schedule controls are `EMAIL_DAILY_ENABLED` (default `true`), `EMAIL_DAILY_TIMEZONE`
(default `America/New_York`), `EMAIL_DAILY_HOUR` (default `18`), and
`EMAIL_DAILY_MINUTE` (default `0`). A late worker start sends that local day's update
once, not a backlog for previous days. The scheduler runs inside `tradeagent notifier`,
not on the laptop and not as a new service.

Migration `0007_daily_status_email` allows non-trade messages in the existing outbox.
Daily messages have a deterministic date/timezone identity and no fictional position
cycle. Interrupted sends can retry with the same provider idempotency key; old
ambiguous claims become `needs_review` rather than risking a blind duplicate outside
the provider window. Round-trip trade emails continue unchanged. Downgrading this
migration requires explicitly archiving non-trade outbox history first.

The dashboard's Event paper experiments section and `/api/event-product` show:
mode, code/config identity, worker state, next session, separate allocation ledgers,
source errors, leading abstention reasons, and evidence/decision detail.

`/api/event-session-report` and `experiment-report` expose the structured session record:
source/decision funnel, premarket evidence, selection/tickets, equipment and news outcomes,
base/stress economics, meaningful incidents, and broker-confirmed quantities plus pending
orders. The 18:00 email includes this report through the existing outbox; the snapshot
persists even if delivery fails, and failures remain visible. A session is complete only
when positions are flat **and** no outstanding or unconfirmed order can change exposure.
Unknown state is an incident requiring continued recovery, not a successful flatten.

The heartbeat is not a trading day. Real news receipt is not a completed round trip.
Only prospective, usable session observations enter the forward clock. The initial
60-session/60-round-trip floor cannot be completed by replay or extra unnecessary trades.

Quote-path diagnostics at 1/5/15/60 minutes include abstentions where a causal quote
exists. They are not broker fills or independent portfolio-return samples. Cash is the
zero-interest baseline. Text, structured, overlay and shuffled controls are declared,
but have no measured comparative results before usable prospective events accumulate.

Broker-paper fills remain factual. Economic-paper P&L separately subtracts a conservative
residual-slippage and current-fee reserve; those reserves are not claimed as observed
live costs. Fixed hosting/inference billing that has not been retrieved stays unknown.

## Current next action

Run the separately authorized IEX practice cohort at the next regular session,
September 8, 2026, with the existing 09:35 entry warm-up and conditional calibration.
Observe actual acceptance, fills, cancellation, exit and reconciliation before claiming
the broker integration worked. If safety checks fail, record the blocker instead of
forcing a trade. The original research protocol still needs SIP entitlement and genuine
forward evidence; practice does not remove either requirement.
