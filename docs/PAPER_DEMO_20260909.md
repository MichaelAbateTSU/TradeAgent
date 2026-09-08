# September 9: one conditional paper equipment demo

## Authority and present status

The owner's September 8, 2026 19:16 Eastern request was: “do an example
paper trade through alpaca using the render bots. I want you to prove they
are working.” This is separate, narrow authority for **one paper round trip**,
not strategy rearming. The market was closed when preparation began. No
after-hours order, simulated fixture, deployment, heartbeat, or schedule is
evidence of an actual filled trade.

The original September 8 `MISSED` equipment record and all prior cohort pauses
remain unchanged. The new protocol is `manual-paper-demo-v1`, not a replay of
Tuesday's opening protocol.

| Bound | Value |
| --- | --- |
| Worker | Existing Render event/news worker `srv-dae4tr7qj5pc73a9e0k0` |
| Cohort | `v20-manual-demo-20260909-r1` |
| Policy | `equipment-only-demo`, purpose `iex-practice` |
| Broker | `https://paper-api.alpaca.markets` only |
| Account SHA-256 | `b3ddf697092ad516c7e68024af06916f7c5dc921c8dfd44ff35ea6618b44947f` |
| Equipment test ID | `555933b77cc6ca8a5e031ad96e23b9701203dbece26e47482dbac4ebbeb8b1b0` |
| Session | September 9, 2026, regular NYSE/Alpaca session only |
| Entry window | **10:10 inclusive–10:30 exclusive America/New_York** |
| Entry | One AAPL fractional marketable-limit BUY, at most **USD 25** |
| Exit | Sell only broker-confirmed owned quantity; target 60 seconds |
| Remainder | Cancel unfilled entry at 30 seconds or window end, whichever is earlier |
| Other entries | **Zero** news/H1/H2/general strategy entries |
| Classification | `EQUIPMENT_TEST`; no strategy/60-session qualification |

The ordinary 5-minute warmup, completed observation/processing latency, real-time
IEX quote age of at most 5 seconds, existing spread ceiling, source/context,
liquidity, cash/exposure/loss, account identity, global and cohort pauses,
singleton worker lease, and shared account/session reservation checks still
apply. No SIP entitlement purchase, delayed quote substitution, account reset,
leverage, crypto, options, new recipient, or paid plan change is authorized.

## Two separate gates, in order

1. **09:35–10:05 ET: unchanged read-only incident acceptance.** Follow
   `RENDER_INCIDENT_20260908.md`, `infra/render/POSTDEPLOY_ACCEPTANCE.md`, and the
   root session's September 9 13:35 UTC continuation. Use the actual reviewed
   per-role deployment pins, including the new event pin. Preserve global kill
   and operator pauses throughout this gate. Require the full 1,800-second
   market-open observation, fresh owner-validated physical quotes/trades/bars,
   complete raw-data provenance and advancing persisted exchange evidence,
   no HTTP errors/new gaps/drops/restarts, applications below 400 MiB and
   PostgreSQL below 230 MiB, normal dependency/lease/source/order-stream health,
   database trigger/totals checks and clean relevant PostgreSQL recovery logs.
   Check the actual paper account is unchanged, ACTIVE/unblocked, flat with no
   open orders. An after-hours partial pass never substitutes for this gate.
2. **Only after the full gate passes, and not before 10:05 ET:** a Render
   one-off **preflight-only** job may issue the separately scoped demo approval.
   It must use the same event-worker artifact, exact settings below, explicit
   reviewed SHA, and SHA-256 of the persisted passing acceptance evidence.
   The evidence hash is an audited operator attestation/reference, not an
   automatic independent validator of an arbitrary file. The continuation must
   inspect and pass the actual evidence before supplying it.
3. The already-running, lease-owning event worker alone may execute the demo
   from 10:10 ET. A preflight job never submits an order or acquires/steals the
   worker lease. No local raw broker POST or competing order worker is allowed.
4. If acceptance or preflight fails, leave the test blocked with **no orders**.
   If the finite window expires, record the missed/blocked outcome; never
   change its date/window, manufacture freshness, relax limits, or create a
   replacement test. Any future attempt needs new explicit authority.

Exact worker command (all other existing service environment is preserved):

```text
tradeagent run --mode experimental-paper --purpose iex-practice --practice-start-date 2026-09-09 --cohort-id v20-manual-demo-20260909-r1 --entry-policy equipment-only-demo --demo-account-digest b3ddf697092ad516c7e68024af06916f7c5dc921c8dfd44ff35ea6618b44947f --max-entries-per-session 1 --symbols AAPL
```

The only authorized approval command, **after gate 1**, is:

```text
tradeagent paper-preflight --purpose iex-practice --practice-start-date 2026-09-09 --cohort-id v20-manual-demo-20260909-r1 --entry-policy equipment-only-demo --demo-account-digest b3ddf697092ad516c7e68024af06916f7c5dc921c8dfd44ff35ea6618b44947f --max-entries-per-session 1 --symbols AAPL --confirm-experimental-paper --confirm-equipment-only-demo --demo-reviewed-code-sha REVIEWED_EVENT_SHA --demo-acceptance-sha256 PASSING_EVIDENCE_SHA256
```

Generic preflight cannot authorize this policy. The special preflight requires
the expected paused/unused state and matching fresh deployed-worker heartbeat,
an open and flat broker account, and the exact account/code/config/cohort/date.
Approval, certificate and narrowly clearing these entry pauses are atomic;
any concurrent change to safety controls aborts activation. Old cohort pauses
are not cleared. Startup never enables entries.

## Recovery and honest proof

The original deterministic account/date equipment client ID and durable intent
are retained across restarts, broker timeouts and partial fills. Unknown
submission outcomes are reconciled by that same client ID, never another BUY.
Cancel requests are not cancel confirmations. Recovery/owned-position exits
remain active after all entry authority expires. Target timing is measured,
not guaranteed: the existing polling/network latency can delay cancellation
or liquidation and any delay must be reported.

After a risk rejection, finished attempt, or window cutoff, a durable terminal
marker keeps new entries disabled and restores global kill/cohort pause. It is
not evidence of a successful round trip. The continuation must follow any
uncertain order or partial fill until actually resolved, including next-session
owned-position recovery if an outage prevents same-day liquidation.

Proof requires the actual broker BUY/SELL order IDs, original client IDs, fill
quantities/prices/timestamps, cancellation/recovery history, actual P&L without
profitability claims, an ending flat broker account with no open or unresolved
local orders, and normal existing-recipient round-trip notification outbox
acceptance/provider message ID. Distinguish provider acceptance from inbox
delivery. Do not resend a previously accepted message. Persist the detailed
session report and broker evidence and preserve/reapply entry pauses.

Release, review, after-hours deployed checks, and the later actual outcome are
recorded separately in `research/results/paper-demo-20260909-deployed.json`.
Preparation alone leaves actual trade proof and full market-load acceptance
**pending**.
