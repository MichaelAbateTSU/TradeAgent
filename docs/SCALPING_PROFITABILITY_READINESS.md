# Evidence-gated next step toward economic modeling

This is an **offline research prerequisite**, not a new strategy,
profitability result, model promotion or trading authorization.
Operational Render health is separate from measurement quality: the active
processes can be healthy while a failed study remains deliberately stopped.
See [Render agent operations](RENDER_AGENT_OPERATIONS.md).

The economic model remains `no_support`. Its next useful improvement is
to reject research inputs that cannot defend executable prices, complete
measurement, personal venue eligibility and actual costs **before**
training or claiming a positive edge.

## Read-only commands

Inspect the explicit evidence schema:

```powershell
python -m tradeagent.scalping_profitability_readiness schema
```

Review a prospective evidence envelope against an independently approved
trust manifest and its canonical SHA-256:

```powershell
python -m tradeagent.scalping_profitability_readiness evaluate `
  .\research\evidence\source-cost-envelope.json `
  --trust-manifest .\research\evidence\approved-trust-manifest.json `
  --trust-sha256 APPROVED_CANONICAL_SHA256
```

The manifest must independently approve the plan/source auditors,
execution relationship, account provider, fee provider and friction
evidence. Hashes identify the supplied evidence; they cannot prove an
external fact or turn a self-attested venue match into broker execution.
Unapproved or missing evidence is explicit **NO-GO**, never a fee-zero
default or implied account entitlement.

Review the actual frozen failed confirmation without editing it:

```powershell
python -m tradeagent.scalping_profitability_readiness closeout `
  .\docs\experiments\2026-10-book-confirmation-72h-v2\final-closeout-20261007\closeout-verdict.json `
  --protocol .\docs\experiments\2026-10-book-confirmation-72h-v2\frozen-protocol.json
```

Commands print bounded deterministic JSON. They use no broker client,
production database writes, runtime claim, artifact replacement or model
promotion. Saving the JSON is a separate operator action. The source
study's labels, protocol hashes and denominators stay unchanged.

## What the gate requires

The gate checks the predeclared schedule and contract, mature outcome
denominator, completed/no-restart source state, clean integrity/final
counters, independently approved provenance and both symbols separately.
BTC cannot compensate for failing ETH. A shorter written slice is a
diagnostic only, not a replacement coverage calculation.

The market-data source must have an evidenced relationship to the
intended execution venue/account. A 99%-covered unrelated source is still
ineligible. Account/jurisdiction eligibility and fee provenance must
correspond to that same venue and account. Public or configured rates
remain scenarios, not an actual fee tier.

Passing prerequisites would permit considering offline research only.
The report does not validate a model, authorize orders or claim profit.
Chronological out-of-sample support, conservative execution/fill testing
and separately authorized paper validation remain necessary.

## Exact economic hurdles

The shared Decimal fee helper preserves the existing research arithmetic.
An entry fee paid in base coins reduces inventory; an exit cash fee
reduces sale proceeds. Raw ask-to-bid returns apply both fee legs
multiplicatively. A return already net of the entry inventory fee applies
only the exit fee; a cash-net return deducts neither again. Fee embedding
must be supported by matching execution evidence rather than asserted
after a losing result.

Ask-to-bid returns already contain the spread: the frontier does not
charge it again. Residual nonembedded friction is additional
entry-notional basis points and must be explicit. Unknown friction means
the **full-cost hurdle remains unknown**, not zero. Passive order type
does not prove maker attribution or guarantee a fill.

The real failed-study command reports the following diagnostic fee-only
floors for raw ask-to-bid gross return:

| Scenario, not a verified personal tier | Fee-only break-even floor |
|---|---:|
| Historical public Kraken maker scenario, 40 bps per leg | 80.4826 bps |
| Historical public Kraken taker scenario, 80 bps per leg | 161.9407 bps |
| Existing Alpaca configured allowance, 25 bps per leg | 50.1881 bps |

These are not full economic forecasts. Slippage/impact and other
nonembedded friction are unverified; the actual output therefore retains
`unknown_nonembedded_friction` and null full-cost hurdle/expected-net
values. Public fee scenarios do not certify current rates, personal
eligibility or live execution.

## Actual result and decision

The e05 closeout input retains BTC 4,616/25,920 (17.81%) and ETH
4,689/25,920 (18.09%), with 20,526 unwritten mature outcomes each.
Its written-slice percentages are explicitly non-primary diagnostics.
No complete-case subset is made eligible for model fitting.

The readiness report remains **NO-GO**, with `offline_research_ready`,
`model_ready`, `trading_allowed` and `profitability_claim` false.
It identifies source completion/integrity/coverage, approved execution
relationship, account eligibility, personal fees and friction evidence
as unresolved prerequisites. No source is silently switched and no
replacement window is started.

The [October 8 validation record](releases/2026-10-08-operations-economics.json)
preserves this decision alongside the operational readback and completed checks.

This is the next step toward a defensible profitable model: establish
what can actually be measured and what a trade must earn after verified
costs. It is not evidence that the current strategy can earn that hurdle.
