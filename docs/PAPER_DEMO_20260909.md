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

## Reviewed deployment and scoped source-risk acknowledgement

The initial `0999428` review found a final-dispatch timing defect; it was fixed
before any deployment. Exact release
`b354bf8f925d76ee33205735736ec4cf36ff54ce` received clean follow-up review,
including independent deadline, quote-age, concurrent-kill and no-I/O-after-clock
checks. Validation: **941 passed, 2 skipped, 87.35% coverage**, Ruff `--no-cache`
and mypy passed. Event-only deployment `dep-dag9q8h5efls73a212lg` became live
September 8 at 23:42:43 UTC. Normal old-lease expiry and Render restart backoff
delayed actual new-worker ownership until 23:49:30 UTC; the overlap/restarts
are retained, not mislabeled as a clean acceptance interval.

The same-image read-only probe at 23:49:38 UTC confirmed the exact account,
ACTIVE/unblocked, **zero positions/open orders and zero linked demo orders**,
the frozen configuration
`5a0acf612ce4fc886e342656cab2e9b2ea0295882fb7fbe731e65fc44f44cf65`,
paper order stream authenticated/subscribed without gaps/drops, and a successful
44,080-byte report build without email. No demo authorization exists.

Source evaluation initially changed the new cohort pause to
`R1_EVENT_REQUIRES_POSITION_REVIEW`. The original pause events, publication
uncertainty and rejected decisions remain immutable in the evidence below.
After explicit parent approval, a scoped operator review on September 8 at
**20:18:24.133135 ET** atomically changed **only this cohort's pause** to
`OPERATOR_PAUSE`. Immutable audit
`b0a5c5a0-6817-540c-adcb-500a77e5668f` (`event_operator_risk_review`) and its
reporting metadata sidecar were inserted in the same transaction.

The acknowledgement covers only these three retained Apple/Pusheen versions:

- `edf981a4e18b2ab7e79aa4307a533085902671b747b4fbb6faf80546ce7f800c`
- `6d6f95f5a20155d0efb039a3f6c609db6760527b3df3af08423f188b69a50c1a`
- `4554e950fe27994318dc707650fca38753fefe6d67e05ffab0fa9d36b65bf8ef`

All share content SHA-256
`bd75634768bf211283a527e6550d56d98a558ab80076ffbfa9cd56451641bb03`.
Frozen facts are empty, event types unclassified, hypotheses absent and
candidates rejected. Exact normalized text/headlines were unchanged; all 26
changed HTML characters per adjacent revision were cache/metadata timestamps,
not revised economic facts. Explicit correction/retraction flags were false.
Publication remains unknown; no exact time was inferred from the date-only field.

The operation revalidated the complete three-trigger/three-decision set, exact
retained HTML/content hashes, no unreviewed pending primary-source revision,
no waiting demo news candidate, and the actual ACTIVE/unblocked paper account
with zero positions, open orders or unresolved local intents. It locked and
compared the pause value **and timestamp**, closing concurrent insert/change
races, and aborted on any changed state. Every other control was unchanged,
including global kill `active` with timestamp **23:33:26.789305 UTC September 8**.
**No approval, certificate, order, rule change or deployment occurred.**

See `research/results/paper-demo-20260909-R1-source-review.json`,
`paper-demo-20260909-R1-all-triggers.json`, both retained HTML-diff artifacts,
and `paper-demo-20260909-R1-ack.json` for the source review and actual committed
acknowledgement. The one-use operation is retained as
`paper-demo-20260909-R1-ack-operation.py.txt`; it must not be reused for new risks.

**This is not permission to ignore any future R1.** Any new revision, risk or
stricter pause requires a separate fresh review and authority; no automatic
clearing is authorized. Unresolved risks tomorrow mean **blocked/no orders**,
even if incident market-flow acceptance succeeds. Full acceptance and the
separate explicit one-demo approval are still required. Actual trade proof
remains outstanding.

The root session's existing **September 9 13:35 UTC** wake is retained. It now
specifies the new event pin, preserves the entire read-only incident gate, and
adds only the separate conditional demo continuation and reconciliation/
notification/flat-account proof requirements. Its scoped acknowledgement note
does not authorize ignoring future R1 flags or scheduling an unconditional order.

After the handoff, a **662.93-second / 882-request** after-hours observation
finished at September 9 00:00:56 UTC with zero HTTP errors and no new ownership,
restart, gap or drop failures. Peak application RSS was 201.86 MiB; PostgreSQL
peaked at 210.05 MiB. The full acceptance result remains **failed/pending**:
closed-market recorder quotes/trades/bars and durable exchange timestamps did
not advance, and the recorder/feed correctly remained degraded. No thresholds
were waived. A bounded PostgreSQL log sample retained duplicate-evidence key
errors but no termination/recovery/fatal signatures; this is not an exhaustive
claim of error-free database logs. Nine enabled market-counter triggers were
independently enumerated.

The pre-review same-image broker probe at 19:57:46 Eastern showed the same
ACTIVE/unblocked paper account, zero positions/open orders, no demo orders or
authorization, global kill active and the stricter R1 pause unchanged.
