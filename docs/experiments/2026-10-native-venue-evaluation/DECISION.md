# Native venue investigation: NO-GO

**No v2, no strategy change, no source migration and no trading
authorization.** The native alternative did not pass the unchanged
measurement gate, and Alpaca's private account/reference questions remain
provider-confirmation requirements.

## Alpaca result

The configured paper account's exact BTC/USD and ETH/USD simulated-fill
quote-reference location remains **unproven**. Public paper specifications
describe simulated real-time-quote fills, not a live exchange route.
Account/configuration/assets/historical-order evidence does not bind the
reference prices to `us`, `us-1` or `eu-1`.

The earlier native alternate-location attempts returned **406** while
the protected existing `us` connection remained active. This establishes
an access limitation, not its complete quota scope.

Current retail Basic/Algo Trader Plus documentation does not guarantee
more simultaneous crypto sessions. Equities/options symbol, SIP/OPRA and
request-rate upgrades are not crypto-connection entitlements. Broker API
partner connection tiers do not apply to this individual Trading API
account. **No upgrade was purchased or recommended as a proven fix.**

The definitive Alpaca answers cannot be fabricated from these read-only
interfaces. A provider inquiry is drafted in `provider-evidence.md`; it was
not sent. No account, credentials, subscription or v1 stream was changed.

## Same-venue alternative evaluated

Kraken primary public and authenticated spot APIs are documented against
the production spot engine. Public AssetPairs and the native instrument
channel identify BTC/USD and ETH/USD as online `international` spot pairs,
with coherent REST/v2 symbol identifiers and quantity/cost minimums.
This is a clearer **venue/instrument-level relationship** than using Kraken
observations to explain unknown Alpaca simulator prices.

It is not account-specific execution approval. There is no configured
Kraken account/region verification, no private API access and no claim
that these prices match Alpaca paper fills. Published leverage availability
does not authorize margin or shorting.

The public discovery used the native ticker's explicit `event_trigger: bbo`,
not its trade-trigger default. BBO prices, quantities and original provider
timestamps were retained. Discovery message counts were not treated as
successful forward labels.

## Frozen native feasibility result

| Symbol | Mature evaluations | Complete primary labels | Missing | Coverage | Required |
|---|---:|---:|---:|---:|---:|
| BTC/USD | 180 | 134 | 46 | **74.44%** | 95% |
| ETH/USD | 180 | 93 | 87 | **51.67%** | 95% |

The completed window was October 2 **22:27-22:57 UTC**, with the fixed tail
through **22:58:05 UTC**. Freeze hash:
`81ba161054860342739f2ca78a153fa10f9cbc01b310d52ee8c102511364316f`.
Isolated job `job-db02u2e0tbcc73fvb1g0` completed and exported its evidence.

The original two-second provider/receive age fences, ten-second cadence,
$10.25 entry-ask/future-bid displayed size, 60-second horizon, two-second
settlement and all-mature-evaluation denominator were unchanged. Actual
unmodified v1 evaluation/resolution times supplied common causal anchors.
No late quote, midpoint, last trade, synthetic quote or excluded interval
was used to improve coverage.

| Symbol | Stale/missing decision reasons | Stale/missing horizon reasons | Displayed-size reasons |
|---|---:|---:|---:|
| BTC/USD | 24 | 23 | 1 |
| ETH/USD | 49 | 46 | 0 |

Endpoint reason counts overlap. They are not additional evaluations.
The complete per-label rejection taxonomy and quote ages are archived.
There was no connection/capture failure or reconnect in the completed
native study.

The source delivered **17,995 frames and 15,969 native BBO observations**.
The fixed monotone native-clock rule rejected **93 provider-timestamp
regressions**, leaving 15,876 accepted quote observations. Original raw
messages remain available; timestamps were not rewritten to receipt time.
The presence of many updates or a genuine same-venue source does not waive
the chronological/freshness requirement.

This result applies to the tested native ticker/BBO acquisition mode and
window. It is not proof that every Kraken market-data channel, period or
order type is unsuitable. In particular, timestamp regressions and the
ticker timestamp's precise relation to BBO event generation remain
documented semantics to clarify before proposing another measuring
instrument. A timestamp reinterpretation or another channel would need
its own predeclared source contract and test, not a favorable rerun of
these labels.

## Preserved diagnostic export failure

The first predeclared native run, October 2 **20:00-20:30 UTC**, completed
capture but failed during report export because decimal instrument minimums
were not JSON serializable. Its job was `job-db00p6m7bikc73fo6mu0`.
The original freeze and failure evidence remain intact; **no coverage pass
or completed archive is claimed for that run**.

Only the diagnostic export representation was repaired: quantity/cost
minimums are emitted as explicit unit-bearing decimal strings. No price,
freshness, size, sampling, source or model parameter changed. A completely
new diagnostic window was frozen before the repeat capture. This was not
a v2 window, observer deployment or strategy retest.

The failure is a diagnostic artifact-export defect, not evidence of a
Kraken quote-feed or v1 collector failure. It is retained rather than
hidden behind the later completed result.

## Migration constraints beyond quote coverage

No supported retail **Kraken spot paper-execution endpoint** was established
in the inspected official documentation. Beta spot uses the production
engine and is not fake money. The futures demo is a separate derivatives
market, not spot BTC/USD/ETH/USD paper execution.

An eventual paper-first migration would require an explicit, separately
classified application-owned Kraken-feed paper simulator or provider
confirmation of a legitimate spot simulation facility. Such a simulator
would need depth/latency/slippage/fees/partial-fill/ownership/loss-control
validation; its fills would not become broker-confirmed by using native data.
No simulator or live Kraken order adapter was implemented here.

AssetPairs `fees` and `fees_maker` arrays were empty, and the official API
reference marks them deprecated. They are **not zero-fee evidence**.
Consumer Kraken+ fee promotions exclude API/Pro spot trading. Conservative
venue-specific API fee scenarios and actual account eligibility/tier must
be established separately before any future experiment.

## Exact readiness decision and next requirements

**Selected execution/source stack: none.**
**V2 configuration/release/start: blocked, not prepared or registered.**

1. Obtain authoritative Alpaca clarification of the actual paper crypto
   reference feed and crypto-session entitlement scope before treating
   an upgrade or alternate location as a solution.
2. If considering Kraken, clarify native BBO timestamp semantics and
   regional/account/spot-paper feasibility. Public venue-level coherence
   is not the missing account or execution-simulation authorization.
3. Only a separately frozen, unchanged-threshold feasibility test that
   passes at least 95% for both symbols with clean causal/integrity evidence
   can support observation-layer migration preparation.
4. After such a pass and explicit migration approval, design the smallest
   same-venue observation/paper-execution change, validate all controls
   and run an isolated bounded smoke. Then freeze a new v2 window; never
   reuse v1 dates or relabel its failures.

No source, subscription, routing, threshold or model modification is
justified by the completed negative result. No native-price coverage,
economic edge or future live-risk stage is assumed successful.

## Verification and preserved safety

All 360 completed-study primary records were independently reproduced from
the archived quote observations. Raw source-chain and artifact hashes,
source identity, receipt/acceptance clocks, maturity denominators and
no-forward-looking joins passed verification.

Application validation passed **1,925 tests, two skipped**, Ruff and strict
mypy over 121 source files. Diagnostic contract parity passed 2,118 checks.
The application code, strategy, migrations and all prior v1/feasibility
artifacts were not changed.

Before/after read-only safety proofs confirm zero positions, open orders
and broker order records since freeze. The original v1 worker stayed on
approved `94e80ec`, with the same connection, **expired authorization**,
**`no_support`**, and zero reported order attempts.

Native raw/quote journals, instrument/acknowledgment evidence, frozen plans,
all labels and independent verification are archived in `native-study-r2`.
`85404eb`, `dc00e4e`, original v1 and both earlier feasibility studies remain
preserved. No source was switched, no account/key was created, no subscription
was purchased, and no private Kraken request, broker order or v2 start occurred.
