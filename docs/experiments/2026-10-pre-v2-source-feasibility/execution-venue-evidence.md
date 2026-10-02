# Configured account execution and quote-source evidence

Checked October 2, 2026 using authoritative provider documentation and
read-only job `job-davshp67bikc73fa2uu0`. No account setting, authorization,
source configuration, order or v1 evidence was modified.

## What is established

The configured broker host is **`https://paper-api.alpaca.markets`**, with
the existing pinned account fingerprint. It is not the live Trading API
host and no live-account credentials were used.

[Alpaca's paper specification](https://docs.alpaca.markets/us/docs/paper-trading)
explicitly says paper orders, including crypto simulation, are **not routed
to a live exchange**. The simulator fills from real-time quotes. Its generic
rules describe best-current-price/NBBO matching, but do not identify which
crypto location supplies this account's BTC/USD or ETH/USD reference quotes.
The document also says paper quantities are not checked against displayed
NBBO quantities. A historical simulated fill therefore cannot establish
real displayed liquidity or prove execution on Alpaca US/Kraken.

| Read-only evidence | Actual result | What it does not prove |
|---|---|---|
| Account | ACTIVE, crypto ACTIVE, USD, reported crypto tier 1 | Crypto venue, jurisdiction or quote-reference location |
| Account routing-like fields | None | Account-specific price-source binding |
| Admin/user configuration keys | Empty | Hidden backend routing |
| Account configuration routing-like fields | None | Regional provider selection |
| BTC/USD and ETH/USD assets | Exchange `CRYPTO`, spot crypto, tradable, not marginable/shortable | Alpaca versus Kraken execution |
| Returned historical crypto examples | Two prior simulated BTC fills, no routing-like fields | Physical exchange execution or ETH routing |
| Broker safety | Zero positions/open orders/order records since freeze | A renewed worker authorization |

The actual account/configuration/asset/order evidence is archived in
`routing-evidence.json`. Account IDs, account numbers, balances and keys
are not copied into this report. Historical order identities are digests.
The returned historical page was not saturated; the inspected interval
is explicitly limited in the evidence and is not claimed to include every
historical order.

The broker account may be ACTIVE with `trading_blocked: false` while the
worker's separate authorization remains **expired**. Account capability
is not worker permission. No capability or configuration was changed.

## Published market-data locations are not account routing proof

[Alpaca's native crypto-stream documentation](https://docs.alpaca.markets/us/docs/real-time-crypto-pricing-data)
identifies:

| Location | Documented data source | Study role |
|---|---|---|
| `us` | Alpaca US | Existing v1 stream, read-only common-window baseline |
| `us-1` | Kraken US | Conditional market-data candidate |
| `eu-1` | Kraken EU | Conditional market-data candidate |

The stream document describes regional availability for `us-1` and warns
providers may change. It does not establish this account's jurisdiction
or paper-price reference. Successful data authentication proves access
to data, not eligible order routing.

The [latest-quote API schema](https://docs.alpaca.markets/us/reference/cryptolatestquotes-1)
also enumerates `us-2` and `bs-1`. They are not mapped to a provider/native
stream in the inspected crypto-stream document. GET discovery with this
account's data credentials returned HTTP 200 but **empty BTC/ETH quote
maps for both**. Their provider, native-stream contract and venue relationship
remain unverified. They are inventoried rather than quietly treated as
qualified sources or assigned synthetic zero-price labels.

Other unconfigured exchanges, sandbox feeds and the FAKEPACA test stream
are not defensible substitutes for executable BTC/USD and ETH/USD prices
for this account. No integration or account was created to bypass that
constraint.

## Connection and collection safeguards

The [stream specification](https://docs.alpaca.markets/us/docs/streaming-market-data)
documents connection limits to a **single endpoint**, commonly one.
The study does not open a second `us` connection, unsubscribe the current
worker or restart its stream. It uses the existing v1 evaluations,
60-second labels and raw source chain as its Alpaca US baseline.

One separate conditional connection is attempted for each documented
`us-1`/`eu-1` endpoint. Connection-limit, subscription, integrity and
capture errors remain evidence, not permission to reconfigure v1 or
exclude bad intervals. A detected v1 connection/ownership/safety change
invalidates the feasibility run and closes only the diagnostic clients.

The thirty-minute prospective study was frozen **before** its window:
October 2, 16:00-16:30 UTC, with capture tail ending 16:31:05 UTC.
The study is not a registered v2 dataset. `frozen-study.json` preserves
its exact dates, common anchor clocks, code hash, v1 reference and
unchanged contract.

The primary price tests retain both two-second age fences, the ten-second
evaluation cadence, 60-second horizon, two-second settlement and the
**$10.25 entry-ask/future-bid displayed-size requirement**. Generic
alternative-source records have their own location identity; Kraken
prices are not mislabeled as `MarketEvent.venue = alpaca-crypto-us`.

The alternative diagnostic captures retain processing entry and actual
consumer acceptance completion, using completion as availability.
Existing v1 `processed_at` values are copied exactly, never corrected
retroactively. All sources are evaluated at the same actual v1 decision
and resolution anchor clocks. Inputs received after a horizon cannot
supply its price.

## Current venue gate

**No location has a proven account-specific executable-price relationship.**
The physical paper destination is a simulator, and its crypto reference
location is not established by the inspected API responses or published
generic fill description. This is an explicit missing prerequisite, not
evidence that the fastest stream must be the correct venue.

A data-quality pass cannot override this venue gate. No source, observer
change or v2 start may be approved without sufficiently specific provider
evidence linking the quote source to the configured account's simulator
and, for any later live-risk consideration, its actual eligible live venue.

## Provider clarification required

The remaining question for Alpaca is:

> For this configured Trading API paper account and BTC/USD/ETH/USD, which
> crypto location/provider supplies the simulated bid/ask fill reference?
> Is it venue-specific or aggregated, how are sizes interpreted, and which
> corresponding market-data stream is a defensible execution-price proxy?
> Separately, what live crypto venue/region would an eligible linked live
> account use, and how can that binding be verified without placing orders?

No support message, live order, new credential, account change or Broker
API access was performed. Written clarification must precede treating any
location as a matched execution source; the inspected individual Trading
API fields are not Broker API platform routing controls.

Additional authoritative references:

- [Account schema](https://docs.alpaca.markets/us/reference/getaccount-1).
- [Account configurations](https://docs.alpaca.markets/us/reference/getaccountconfig-1).
- [Returned order schema](https://docs.alpaca.markets/us/reference/getallorders-1).
