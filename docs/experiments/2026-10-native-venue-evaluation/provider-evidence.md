# Execution and native-data relationship investigation

## Alpaca: unresolved provider facts, not permission to buy access

The current account remains a pinned **Trading API paper account**.
Its documentation establishes simulated quote-based fills, not a live
exchange route. The previously archived account/configuration/asset/order
evidence still does not disclose this account's BTC/USD/ETH/USD paper
quote-reference location.

The current [market-data subscription specification](https://docs.alpaca.markets/us/docs/about-market-data-api)
separates individual Trading API plans from Broker API partner plans:

- Retail Basic/Algo Trader Plus tables describe stocks/options market
  coverage, symbol limits and historical request limits.
- Unlimited equity symbols and SIP/OPRA data are not additional crypto
  WebSocket sessions.
- Broker partner tables with five/ten stream connections are explicitly
  equities/options plans, not proof of this personal account's crypto limit.
- No inspected public upgrade specification guarantees removal of the
  observed crypto **406** rejection across regional locations.

The [stream specification](https://docs.alpaca.markets/us/docs/streaming-market-data)
describes 406 as a connection-limit error and generally describes limits
per endpoint. Actual authenticated alternate-location attempts were rejected
while the original `us` connection stayed active. The relevant limit scope
and exact crypto entitlement are still provider-confirmation questions.
We do not translate an undocumented entitlement into a purchase recommendation.

No account configuration, key, subscription, region, stream or authorization
was changed, and no support message was sent. A definitive provider answer
is still required; this investigation cannot assert that Alpaca is unable
or unwilling to supply a matched feed.

### Provider inquiry draft, not sent

For the configured individual Trading API paper account:

1. Identify the exact BTC/USD/ETH/USD paper fill-reference quote source,
   including native versus aggregated prices and location/provider identity.
2. Identify the corresponding externally available quote feed and native
   timestamp/size semantics.
3. Confirm the current simultaneous crypto session limit and whether
   `us`, `us-1`, `eu-1` share the quota.
4. State which, if any, retail entitlement permits more crypto sessions,
   its explicit quota and pricing, and how it can be verified read-only.
5. Separately identify the eligible live crypto venue/region and how its
   relation to market data can be established without placing orders.

This is a draft only. No private account identifier, key or secret is sent
to a third party in this work.

## Kraken: a clearer venue-level relationship, not an approved account migration

The official [Spot WebSocket reference](https://docs.kraken.com/exchange/api-reference/spot-websocket)
and [connection overview](https://docs.kraken.com/exchange/guides/websockets/introduction)
identify primary public `wss://ws.kraken.com/v2` and private
`wss://ws-auth.kraken.com/v2` as APIs for the **production spot trading
engine**. Beta endpoints also use production; beta is not paper trading.

The public [ticker channel](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/ticker)
provides native best bid/ask, their base-asset quantities and an RFC3339
provider timestamp. Its explicit `event_trigger: bbo` publishes on changes
in best-bid-offer price levels; the default trade trigger is not used.

The [instrument channel](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/instrument)
provides pair identifiers, trading rules and a venue selector. The public
[AssetPairs response](https://docs.kraken.com/api-reference/market-data/get-tradable-asset-pairs)
reported `execution_venue: international` and `online` for both spot pairs:

| Pair | REST trading identifier | Native v2 symbol | Minimum base quantity | Minimum quote cost |
|---|---|---|---:|---:|
| BTC/USD | XBTUSD | BTC/USD | 0.00005 BTC | 0.50 USD |
| ETH/USD | ETHUSD | ETH/USD | 0.001 ETH | 0.50 USD |

This establishes a **venue/instrument-level** data-to-spot-order relationship
for a prospective eligible Kraken account. It does not establish this user's
jurisdiction, funding, Kraken account access or intended legal entity.
It does not make Kraken data match the current Alpaca paper simulator.
Quantity rounding and minimums must be checked using actual quotes before
any future execution design; availability of displayed size alone is not
an order-acceptance guarantee.

[Spot AddOrder](https://docs.kraken.com/api-reference/trading/add-order)
requires private create/modify-order permissions. Those permissions,
credentials and requests are **not used** in this evaluation. No validation
order is submitted either.

## Paper-only migration constraint

No supported retail **spot broker paper endpoint** was established in
the inspected official sources. Public spot data are credential-free;
private spot order entry is production. A Kraken beta endpoint is not
a sandbox.

The official [API testing environment](https://support.kraken.com/articles/360024809011-api-testing-environment-derivatives)
is **Kraken Futures demo**, a separate derivatives environment. It does
not prove spot BTC/USD/ETH/USD execution or replace spot liquidity evidence.
No demo account or API key was created.

A paper-first migration would therefore require either explicit provider
confirmation of an appropriate spot simulation facility or a clearly
classified application-owned **Kraken-feed paper simulator**. The latter
would model fills/fees/depth/latency/partial fills and is not broker-confirmed
execution. Neither is implemented or authorized by this investigation.

The AssetPairs `fees`/`fees_maker` arrays were empty. Official reference
marks them deprecated as of September 8, 2026; **empty is not zero fees**.
Any migration must separately establish conservative Kraken Pro/API fees
and account-specific tier evidence. Consumer Kraken+ fee promotions are
not API spot-fee evidence.

## Bounded native source study

A credential-free public discovery confirmed both BBO subscriptions and
online instrument rules. Its message counts are not a coverage claim.

The separate native feasibility window is October 2 **20:00-20:30 UTC**,
with capture tail to **20:31:05 UTC**. Frozen hash:
`b244754f5c75f9003ecb7a0e88dded938fb76ed73d4657b957ed61af74e022ec`.
Study role is public venue evaluation, not a source migration or v2.

The same actual v1 evaluation/resolution anchors, ten-second cadence,
60-second horizon, two-second settlement, provider/receive freshness fences
and $10.25 entry-ask/future-bid displayed-size tests are used. Native
provider times and consumer acceptance clocks are preserved. No price
or midpoint fallback, threshold tuning, v1 relabeling or strategy change
is permitted.

The unchanged Alpaca worker is monitored but never restarted or switched.
Alpaca credentials are used only for its existing read-only safety checks;
**no credential or authentication message is sent to Kraken**. There are
no private Kraken API calls or broker orders.

Even a data-quality pass would approve only a research migration candidate,
not configured-account execution, a profitable model, v2 collection or
live trading.
