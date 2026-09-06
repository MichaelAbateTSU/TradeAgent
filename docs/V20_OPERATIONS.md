# v20 event-paper operations

## Visible policy change

The September 6, 2026 v20 assignment replaces the v0.10 stop-discovery direction.
Only H1 (comparable numerical guidance revision), H2 (quantified binding contract), and
R1 (verified event/macro risk) enter this bounded program. Previous strategies are not
promoted. Both sealed holdouts and all archived failures remain unchanged.

Operational permission and statistical qualification are separate. A signed-in paper
account, deterministic mechanics, a frozen cohort, and current valid inputs can permit
a small experimental paper order without historical alpha. No code enables live money.

**Current deployment is shadow, not certified experimental paper.** Latest SIP access
is denied; IEX is available but the frozen event protocol keeps that non-NBBO feed
shadow-only. No subscription is purchased automatically.

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

## Runtime and boundaries

- Existing PostgreSQL and Render services are reused; no new paid resource is created.
- Event worker command: `tradeagent run --mode shadow --cohort-id v20-forward-shadow-001`.
- The original quote-stream recorder, notifier, and dashboard remain available.
- The event worker replaces the news worker's command, retaining its licensed-news
  storage and heartbeat while adding source versions, extraction, decisions and context.
- Apply migration `0006_event_experiments` before starting it.
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

Fed meeting days, the BLS ICS calendar and Nasdaq halt RSS are polled with caching.
Date-only Fed meetings block the full day rather than inventing announcement times.
Missing, stale or malformed context abstains.

## Operator view and clocks

The dashboard's Event paper experiments section and `/api/event-product` show:
mode, code/config identity, worker state, next session, separate allocation ledgers,
source errors, leading abstention reasons, and evidence/decision detail.

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

Keep shadow acquisition running through the next regular session, September 8, 2026.
Review an actual primary-source event and operational preflight before any experimental
allocation. Latest SIP entitlement and broker mechanics during a valid market session
remain unresolved for the frozen paper protocol. Do not relax a gate silently or
represent deployment as profitability.
