# September 9 bounded news-paper operating policy

## Authority and deployment gate

The September 9 owner request extends the equipment demonstration to a genuine
news-worker → evidence decision → risk/OMS → paper broker → Render notifier flow.
This is a new policy, **`news-paper-local-protection-v1`**, not permission to
silently broaden the old equipment-only cohort, loosen risk limits, fabricate
confidence, use holdouts, reset the account, or enable live trading.

Proposed new cohort: **`v20-news-paper-20260909-r1`**, session September 9 only.
No deployment or remote control change is authorized until the parent hands
back ownership after its separate submit/cancel diagnostic. That diagnostic
is not a filled round trip and does not qualify a news strategy.

The existing **09:35–10:05 ET, 1,800-second read-only incident acceptance** remains
first. Preserve all market provenance/progression, physical quotes/trades/bars,
lease/owner/code, memory, no-error/no-loss/restart, source/order-stream, broker,
database counter and reporting checks. No activation or orders during that gate.
Use actual reviewed per-role deployment pins, never a Git HEAD substituted for
an old deployed artifact. After-hours health cannot pass market-load acceptance.

Only after the entire gate passes can the separately explicit news preflight
activate this cohort. It requires the unchanged ACTIVE/unblocked paper account,
flat/no open orders, unused shared account/session budget, matching deployed
heartbeat/config/code, a fresh clean context, exact `OPERATOR_PAUSE` plus global
kill active, and the hash of the actual passing incident evidence. Activation
is limited to **10:05 inclusive–10:30 exclusive ET**. Startup never enables entries.
Concurrent safety-control changes abort atomic approval.

The prior operator R1 acknowledgement applies **only to its original cohort and
three reviewed records**. Do not inherit it as a whitelist. Any R1 in this new
cohort needs a separate fresh source/position review and explicit authority.
Unresolved risk or failed acceptance means **blocked/no orders**, not a forced
replacement or changed date.

## Fixed operating limits

| Control | Frozen bound |
| --- | --- |
| Broker | `https://paper-api.alpaca.markets` only; no live switch |
| Account digest | `b3ddf697092ad516c7e68024af06916f7c5dc921c8dfd44ff35ea6618b44947f` |
| Symbols | Existing AAPL/MSFT/NVDA supported issuer universe; AAPL equipment |
| Entry size | At most $25, additionally bounded by existing allocation/exposure/cash limits |
| Exposure | One position **including pending reservations** |
| Session entries | One equipment attempt, then at most one genuine news entry; shared account/day budget two |
| Equipment window | 10:10 inclusive–10:30 exclusive ET |
| Equipment recovery | Cancel unfilled remainder at 30 seconds; target owned exit at 60 seconds |
| News prerequisite | Broker-confirmed completed equipment round trip and flat/unreserved account |
| News holding | Existing maximum 60 minutes; original horizon/cutoff and pre-close flatten remain |
| Economic loss | Daily ≤$50, weekly ≤$50, high-watermark drawdown ≤$150 at $10,000 virtual capital |
| News polling | Existing default 30 seconds; not relaxed to 5–15 minutes |
| Qualification | Operational IEX paper only, never strategy/60-session qualification |

Loss limits can become stricter with a smaller allocation or lower fractions.
The account ledger includes retained fills across cohorts, modeled fee/residual
cost reserves, current marks, and historical closed-equity peaks. Its persistent
account key retains capital/peak and never increases monetary loss caps after
a settings/cohort change. Daily/weekly baselines are reconstructed from earlier
confirmed fills; unvalued or ambiguous history blocks rather than resetting
losses. Account changes fail pinned identity/reconciliation.

News uses the existing validated H1 comparable-guidance or H2 material-contract
rules and source spans, original publication/receipt/version clocks, issuer
mapping, economic facts, corrections, context/halt/macro, completed observation,
liquidity/chase and executable quote checks. The ledger records
**`validated_deterministic_rule`**, not a calibrated probability. Expected edge
and probability of profit remain **unknown**. An absence of qualifying news
must produce no news order.

## Protective exits: explicitly local, not native brackets

Fixed, unoptimized thresholds are **0.5% below** and **1% above** actual cumulative
broker-filled entry VWAP. Planned entry/thresholds appear in the decision ticket;
actual fill-based thresholds are persisted with each owned position. This is an
operational risk choice, not evidence those parameters produce profitable trades.

The worker obtains a current executable-feed quote and uses its bid to evaluate
the long position. Once triggered, the reason/quote/threshold is durably latched:
a later rebound or restart does not undo an exit decision. Missing/stale protection
inputs cause a risk pause and liquidation attempt rather than pretending a stop
is active. A partial entry remainder is canceled and **confirmed** before selling;
only actual reconciled owned quantity is sold. Unknown submissions always recover
by the original client ID, never a second buy. A final fresh broker clock prevents
news-policy exit dispatch outside the regular session.

These are **not broker-native stop-loss/take-profit orders**. The worker and REST
quotes must be available; source polling, supervision cadence, network delay,
halts, outages, price gaps and market-order slippage can delay or worsen fills.
Neither trigger price nor same-day liquidation is guaranteed. Original no-overnight
intent/pre-close flatten and recovery-until-flat remain; unresolved exposure is
an incident, never reported as successful completion. The existing 30-second loop
is retained instead of weakening freshness with a 5–15-minute news interval.

Official documentation checked September 9:

- [Fractional Trading](https://docs.alpaca.markets/us/docs/fractional-trading):
  supported-types section lists fractional market, limit, stop and stop-limit
  **DAY** orders; quantities/notional are mutually exclusive, asset must be
  fractionable. The page also contains a contradictory older market-only note.
- [Placing Orders](https://docs.alpaca.markets/us/docs/orders-at-alpaca):
  advanced bracket/OCO exits, full-entry activation and cancellation races are
  documented, but the inspected pages do **not explicitly establish fractional
  advanced-order-class support**. This implementation does not assume support
  and does not submit bracket/OCO/OTO requests. After-hours non-extended orders
  can queue for the next day; regular-session final gates remain mandatory.
- [Websocket Streaming](https://docs.alpaca.markets/us/docs/websocket-streaming):
  `trade_updates` reports accepted/new, partial fill, fill, canceled, expired,
  rejected and other states; event fill quantity differs from cumulative order
  filled quantity. REST original-client-ID reconciliation remains authoritative.

No new SDK, dependency, account reset, data entitlement, plan or recipient.

## Idempotent lifecycle emails and truthful evaluation

For this policy only, verified order observations atomically enqueue lifecycle
emails with the local state update: accepted, each distinct cumulative partial
fill, filled, canceled/expired/rejected, and sell-side exits. HTTP submission
rejections name the actual client ID/status/code without inventing a broker ID.
Unknown outcome is not called accepted or filled.

Lifecycle outbox records have no invented position cycle. Deterministic IDs
deduplicate retries, REST/stream duplicates and repeated partial quantities.
Existing round-trip reports/outbox remain separate; Render notifier uses the
existing recipient and provider idempotency keys. Persist provider acceptance
IDs as delivery evidence; outbox insertion or provider acceptance is not inbox
proof. Do not resend an already accepted notification.

The existing decision tickets, order/fill/cancellation timeline, separate
equipment/news economic ledgers, cost reserves and session reports remain the
evidence. Dashboard readiness, queued orders or synthetic tests are not trade
proof. Report full incident acceptance and actual equipment/news outcomes
separately, including abstentions, rejected/partial/unknown orders, stop reasons,
actual P&L, final broker-flat/no-open/no-unresolved state and notification status.

## Commands after exact review/deployment handoff

Worker command, initially paused and without approval:

```text
tradeagent run --mode experimental-paper --purpose iex-practice --practice-start-date 2026-09-09 --cohort-id v20-news-paper-20260909-r1 --entry-policy news-paper --news-account-digest b3ddf697092ad516c7e68024af06916f7c5dc921c8dfd44ff35ea6618b44947f --max-entries-per-session 2 --symbols AAPL,MSFT,NVDA
```

Same-image **preflight-only** job, only after passing incident acceptance:

```text
tradeagent paper-preflight --purpose iex-practice --practice-start-date 2026-09-09 --cohort-id v20-news-paper-20260909-r1 --entry-policy news-paper --news-account-digest b3ddf697092ad516c7e68024af06916f7c5dc921c8dfd44ff35ea6618b44947f --max-entries-per-session 2 --symbols AAPL,MSFT,NVDA --confirm-experimental-paper --confirm-news-paper --news-reviewed-code-sha EXACT_REVIEWED_DEPLOYED_SHA --news-acceptance-sha256 ACTUAL_PASSING_INCIDENT_EVIDENCE_SHA256
```

Only the already-running global lease owner submits through the existing OMS.
No competing worker or direct local/raw broker order job is authorized.

## Local validation and remaining release gates

Implementation pin: `c116324007357278c0b35bf8e6d116202c514930`.
On September 9 Eastern, the full existing suite passed **993 tests**, with
two skipped, and **87.49% branch-inclusive coverage** against the unchanged 85%
gate. The 40 policy-specific tests, repository-wide Ruff with `--no-cache`, and
`mypy src\tradeagent` (89 source files) also passed.
The [full validation output](../research/results/news-paper-20260909-tests.txt)
is synthetic/offline evidence, not a real filled trade.

An exact-pinned independent review is pending. This policy has not been deployed
or authorized. The parent-owned, canceled after-hours diagnostic and its actual
notifier acceptance are documented separately in
[the probe record](OPERATOR_PAPER_PROBE_20260909.md); its filled quantity is zero.
Deployment requires explicit remote handoff and clean review. The unchanged
09:35–10:05 Eastern read-only incident acceptance remains the first morning gate.
Only a subsequent, passing, explicitly scoped preflight may authorize this new
policy; no existing cohort acknowledgement transfers or waives future risks.
