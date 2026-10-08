# Venue and cost readiness: evidence before another study

**Research and trading remain NO-GO.** This October 8 investigation fills
the readiness gate with current provider/account evidence rather than
training on failed labels or automatically repeating a 72-hour window.
It does not assert that scalping is profitable or impossible.

`readiness.json` records the explicit conditional decision;
`profitability-readiness.json` is the corrected gate's actual output.
`validation.json` records 2,268 passing tests, two skips, repository Ruff
and strict typing over 134 files. The full suite included the opt-in real
PostgreSQL integration, not just an unexecuted/skipped database test.

The archival text and four release sources are pinned to LF. Original
Windows CRLF capture/validation source hashes remain preserved;
`source-line-ending-verification.json` maps them to the published LF
identities and proves identical Python ASTs. All 173 affected regressions,
lint and strict typing passed again after this format-only normalization.
`artifact-hashes.json` identifies the final portable artifact/source bytes.

## Verified account and costs

The current Alpaca account is the exact pinned paper account, with
`ACTIVE` account/crypto status and reported `crypto_tier: 1`. Both spot
assets remain tradable, not marginable or shortable. Current quantity
minimums and increments are archived in `account-and-fee-evidence.json`.
There are zero positions and open orders. Broker account capability is
not permission for the worker to trade.

Public Alpaca tier-one pricing is 0.15% maker and 0.25% taker, with BUY
fees in credited base coins and SELL fees in credited USD. The account's
reported tier corroborates a public-tier scenario, not an independently
returned current per-order charge schedule or confirmed liquidity role.

Complete bounded pagination for the preceding 30 days found 181 `CFEE`
paper receipts across two pages and no `FEE` records. Of the `CFEE`
receipts, 69 identify BTC, 24 ETH, and 88 have no symbol; cash
`net_amount` sums to -$15.34. That is not the total economic cost:
base-coin charges are separate. None of the 181 records provides an
explicit `order_id`. No unique fee/order pairing or current fee rate is
invented from matching dates, prices or quantities.

The Kraken public tier table was freshly fetched and independently
parsed. Tier one remains 0.40% maker and 0.80% taker. Public BTC/ETH
instrument minimums, ticks and `international` venue identity were
rechecked. Empty deprecated fee arrays are not zero fees.

No Kraken credentials were configured in the inspected process, Windows
user environment or repository `.env`. This does not prove the user has
no account elsewhere. There is no authenticated `TradeVolume` response,
personal legal-entity/jurisdiction proof, account pair entitlement or
confirmed Kraken settlement currency.

## A fee-convention assumption required correction

The existing historical scalping arithmetic correctly models the
Alpaca base-withheld BUY and USD-withheld SELL convention. Applying that
convention to a Kraken scenario does not establish Kraken economics.

Kraken REST `oflags` and native v2 `fee_preference` document a quote
preference for BUY and a base preference for SELL by default. A
preference is not verified personal settlement. Entry and exit treatment,
inventory reserved for a base SELL fee, fee embedding and capital/friction
basis must be explicit. Unknown or unsupported treatments cannot silently
fall back to Alpaca's convention.

The old archived scenario results remain historical assumption-based
diagnostics. They are not rewritten as if a different charge convention
had been used. The new validation and correction are additive; the
original research arithmetic and failed datasets remain unchanged.

The corrected gate requires independently approved confirmed fee-record
evidence for both legs and both symbols. Quote-added BUY cash outlay is
modeled separately from withheld BUY inventory. Base-currency SELL
settlement is currently unsupported and rejected, not extrapolated from
an order preference. Fee-net inputs must identify matching embedded
treatments; friction must declare trade-notional or all-in-capital basis.
The actual failed-study review still returns NO-GO with all research,
model, trading and profit flags false.

## Source access is different from execution matching

The earlier 406 errors occurred while another protected crypto stream
was active. The current original worker's feed is stopped. A new guarded
access discovery opened and closed diagnostic connections sequentially:
all three documented locations (`us-1`, `eu-1`, `us`) authenticated and
acknowledged BTC/ETH quote subscriptions on October 8. No 406 occurred in
those single-connection samples.

This establishes current individual data access, without an upgrade,
restart, account change or modification to v1. It does not establish the
historical cause of every 406, all simultaneous-session limits, a
95% label pass or the paper simulator's quote-reference location. The
bounded access discovery has no evaluation denominator or mature labels.

The current account/configuration still exposes no paper-fill routing
identity. Provider clarification is necessary to bind any Alpaca
location to this account's simulator. The prior support inquiry remains
unsent in
`..\2026-10-market-data-validation\support-inquiry.eml`; no corporate
mailbox is substituted for the user's personal sender.

## Alternatives evaluated

| Setup | Verified benefit | Unresolved requirement |
|---|---|---|
| Alpaca paper + documented native location | Existing pinned paper account; current individual stream access | Exact simulator/feed relationship, source-quality pass, realistic execution costs |
| Kraken production native book + Kraken spot | Provider-level same production engine; coherent pair identifiers/rules | Eligible personal account, actual fees/settlement, unchanged source-quality pass |
| Kraken beta spot | API software testing | Still production execution, not fake money |
| Coinbase Exchange sandbox | Separate keys and fake funds; functional order API testing | Sandbox subset is not production liquidity/fees or proven personal production eligibility; no configured adapter/account |
| Application-owned native-feed paper simulator | Could preserve native source identity and explicit fill assumptions | Separate approved implementation/validation; simulated fills are not exchange-confirmed |

No account/key is created, subscription purchased, exchange adapter
enabled, fake-money engine substituted or live order submitted. An
unrelated faster feed is not made valid by being faster.

## Collector validation and subsequent gates

The earlier component test used SQLite and did not certify PostgreSQL
row locking, fencing or pool contention. This request additionally uses
a disposable local PostgreSQL 17 container with a unique scratch
database, loopback-only port, no host-directory bind mounts and automatic
container/anonymous-volume removal. Production Render databases,
ownership rows and study tables are not test targets.

The final validation record identifies the production-code integration
checks actually run and their measured results. This local bounded test
does not certify Render-specific configuration, pool sizing, network
behavior or 72-hour endurance. It also does not make the native source
feasible.

**Real PostgreSQL mechanics passed.** The report lane was occupied by
`pg_sleep` for 110.094 seconds while twelve real-clock lease renewals
continued, with a maximum gap of 10.025 seconds. Twenty-four synthetic
evaluations and ten mature synthetic labels persisted without missed
slots or ring-expiry misses. Actual row-lock waits, post-lock expiry,
writer/owner fencing, the unchanged five-second lock timeout and
report-pool exhaustion were exercised against the production collector
methods. These are synthetic test clocks/quotes, not native-market
coverage.

A populated external witness (one table, two rows) retained its identical
before/after fingerprint. The test's UUID schema removal and witness
contents were independently checked afterward. The first invocation
failed on a non-scratch-named environment variable before schema creation;
that receipt is preserved beside the passing run. No production database
was contacted.

The harness requires an explicit environment variable with `SCRATCH` in
its name and an existing dedicated loopback database:

```powershell
python -m tradeagent.kraken_confirmation_pg_validation `
  --scratch-url-env MY_EXISTING_SCRATCH_URL `
  --expected-database tradeagent_confirmation_scratch `
  --expected-major-version 17 `
  --archived-protocol .\docs\experiments\2026-10-book-confirmation-72h-v2\frozen-protocol.json `
  --allow-schema-writes
```

It does not read `.env` or fall back to the production URL. The runtime
bound is 240 seconds plus 20 seconds for recovery cleanup. The archived
protocol is an unchanged synthetic-clock fixture, not a new formal
study. Exact receipts are `postgres-validation.json`,
`postgres-preflight-failure.json` and
`postgres-independent-verification.json`.

The immutable source audit separately demonstrated native-update
freshness and genuine displayed-size failures beyond the collector
reporting defect. Repairing ownership cannot remove those failures.
The full failed-window results remain BTC 4,616/25,920 and ETH
4,689/25,920. Missing outcomes after process loss are not classified as
upstream inactivity.

A new source study or model dataset is therefore not started. Before
freezing a new observation window, establish a defensible account/feed
relationship and justify a candidate against the unchanged two-second
freshness, $10.25 displayed-side notional, ten-second cadence, 60-second
horizon and 95% independent BTC/ETH gates. Do not shrink the denominator,
reduce the ticket, drop ETH, reinterpret timestamps or select a favorable
rerun to rescue the original two-symbol protocol.

Only a complete passing, independently approved measurement dataset and
verified execution/cost evidence can support chronological economic
research. Passive nonfills, adverse selection, latency and nonembedded
friction remain necessary execution assumptions; a limit order or paper
profit does not prove maker fees, fills or live profitability.

## Authoritative references

- [Alpaca paper simulation](https://docs.alpaca.markets/us/docs/paper-trading).
- [Alpaca crypto fees](https://docs.alpaca.markets/us/docs/crypto-fees).
- [Alpaca native crypto locations](https://docs.alpaca.markets/us/docs/real-time-crypto-pricing-data).
- [Alpaca connection-limit semantics](https://docs.alpaca.markets/us/docs/streaming-market-data).
- [Kraken public tier changes](https://support.kraken.com/articles/cross-platform-fee-tier-changes).
- [Kraken authenticated pair-specific fees](https://docs.kraken.com/api-reference/account-data/get-trade-volume).
- [Kraken native order fee preferences](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/add_order).
- [Kraken production/beta engines](https://docs.kraken.com/exchange/guides/websockets/introduction).
- [Coinbase Exchange sandbox](https://docs.cdp.coinbase.com/exchange/introduction/sandbox).
