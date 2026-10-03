# Venue fees and execution eligibility

No eligible authenticated Kraken account or user-specific fee tier has
been established. No Kraken API key/secret was present in the inspected
process/user configuration. No account, credentials or permissions were
created and no authenticated request is claimed.

## Verified public facts, not account authorization

The public AssetPairs response reports online international spot:

| Symbol | Minimum base quantity | Minimum USD cost | Quantity precision |
|---|---:|---:|---:|
| BTC/USD | 0.00005 BTC | 0.50 | 8 decimals |
| ETH/USD | 0.001 ETH | 0.50 | 8 decimals |

This verifies published pair rules, not account-specific jurisdiction,
product access, short/margin eligibility or fill probability. A $10.25
ticket must be checked against actual price, rounded quantity and cost
minimum before any separately authorized execution design.

## Current fee schedule matters

The official [cross-platform tier changes](https://support.kraken.com/articles/cross-platform-fee-tier-changes)
became effective **July 9, 2026**. The official HTML table, independently
retrieved rather than inferred from a generated search answer, states:

| Public spot tier | Spot 30-day volume OR assets threshold | Maker | Taker |
|---|---|---:|---:|
| Tier 1 | $0+; assets N/A | **0.40%** | **0.80%** |
| Tier 2 | $2,500+; assets N/A | 0.30% | 0.60% |
| Tier 3 | $10,000+ OR $20,000 assets | 0.22% | 0.38% |

These are **public scenarios, not verified personal account rates**.
The lowest-volume taker/taker fee scenario alone is 1.60% round trip before
spread, slippage and latency. Maker/maker is 0.80%, but does not imply fills
or eliminate adverse selection. Prior 0.25% fee assumptions must not be
quietly treated as current Kraken account evidence.

No deposit, trading-volume manufacture, tier optimization, Kraken+ purchase
or cost reduction is authorized. Consumer instant-buy and Kraken+ fees
do not verify API spot fees. Empty AssetPairs fee arrays are deprecated,
not zero fees.

## Authenticated fee proof required later

[TradeVolume](https://docs.kraken.com/api-reference/account-data/get-trade-volume)
is an authenticated **read-only account-data operation** using
`POST /0/private/TradeVolume`, `Funds permissions - Query`.
Pairs must be supplied to retrieve fees; request BTC/USD and ETH/USD
(or their verified REST identifiers) and retain timestamped responses.
`fees` is taker, `fees_maker` is maker. `fee_schedule: true` requests full
pair schedules. Values are **percentages**, not basis points.

No request was made because an eligible account/key was not available.
When such proof exists, separately document the legal entity/region,
pair access, minimums, role, rounding/credited-asset basis and effective tier.
Until then, fee-sensitive outputs stay scenario-only and unpromoted.

The confirmation is measurement research and computes no profitability
or strategy promotion. A coverage pass cannot establish that hypothetical
gross scalping returns exceed these costs.

## Alpaca option remains unresolved

The prepared inquiry is still ready for the user's **personal mailbox**.
Only a corporate WorkIQ sender was connected in the prior audit, so it
was not used. No provider answer is claimed. Paper quote-reference and
crypto-session entitlement clarification remains needed for Alpaca execution,
but does not block anonymous Kraken observation-only confirmation.
