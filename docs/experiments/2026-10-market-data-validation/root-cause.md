# Native ticker validation root cause

This audit explains all **93 original timestamp high-water rejections**
and correlates all **133 missing native primary labels**. It neither
rewrites the original study nor claims its 74.44% BTC / 51.67% ETH result
was a source-independent verdict about Kraken.

## Regression scope and exact meaning

| Evidence | Actual result |
|---|---|
| Native ticker messages | 15,969 |
| Accepted observations | 15,876 |
| Rejected below per-symbol timestamp high-water | 93 |
| BTC / ETH high-water rejections | 28 / 65 |
| Direct timestamp decreases versus previous wire message | 73 |
| Repeated timestamp equal to preceding message but below earlier maximum | 20 |
| Groups | One connection, ticker channel, separate BTC/ETH keys |
| Original snapshot held the preceding high-water maximum | 2 rejections |
| Rejections within formal window / warm-up | 75 / 18 |

The source decoder processes ticker messages sequentially in one receive
coroutine. The high-water map is keyed by symbol and resets on connection
changes. This is **not** a BTC-versus-ETH, channel mixing, concurrent
processing or different-session comparison error.

The original report's 93 counter is correctly reproducible as high-water
rejections, but **it is not 93 independent adjacent wire-time reversals**.
All observed rejection messages were updates. Snapshot/update mixing
explains two initial high-water contexts, not the remaining phenomenon.
`all-regressions.jsonl.gz` identifies every frame, symbol, channel, session,
raw quote, prior maximum, previous wire quote and precise time difference.

## Price, size and ticker time are different clocks

Across the source corpus:

- **13,765 updates changed size without changing either best price.**
- **5,727 updates changed BBO price or size while retaining ticker time.**
- Timestamp changed without last-price/trade-count changes in 8,321 cases.
- Trade count changed without ticker-time changes in 261 cases.
- At the 93 rejections, 66 changed price and 27 changed only size; none
  changed last price or trade count.

The data demonstrate publication of some size-only changes. They do not
prove publication of **every** size change or identify the ticker clock
as a last-trade-only clock. The correlations do not justify either claim.

The [ticker specification](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/ticker)
defines `bbo` as changes in best-bid-offer price levels and describes the
timestamp only as **ticker data timestamp**. It does not give the stronger
monotone, per-BBO-state clock assumed by the original measuring instrument.
The original rejection rule can therefore discard changed BBO state when
that timestamp falls below its maximum; the discarded state is not
retroactively inserted into any old label.

An unchanged price is not intrinsically invalid. Distinguish:

1. Time since each bid/ask price changed.
2. Time since each displayed size changed.
3. Time since the provider supplied a reliable updated book state.
4. Provider-to-local transport delay and consumer acceptance delay.

A received message with old ticker time fails the **original declared
timestamp-age contract**, not a proof that its displayed quote was
economically invalid. Heartbeats or recent receipt cannot establish a new
provider quote time. This audit does not replace ticker time with local time.

## Every original missing label

The following categories are non-overlapping at label level:

| Symbol | Missing labels | At least one receive-freshness gap | Provider-time age with no receive gap | Displayed-size failure |
|---|---:|---:|---:|---:|
| BTC/USD | 46 | 26 | 19 | 1 |
| ETH/USD | 87 | 70 | 17 | 0 |

At every endpoint the artifact preserves the last same-symbol causal raw
message, selected immutable observation, original reasons, age fields,
high-water rejection status and any actual accepted-observation reference.
Unselected consumer acceptance is not inferred from receipt alone.

The corpus's largest same-symbol receive gaps were **6.435 seconds BTC**
and **11.134 seconds ETH**. A receive gap does not prove all intervening
unchanged quotes invalid, but it cannot satisfy the old two-second local
receive fence. The remaining provider-time age failures show that
transport activity alone also does not satisfy that fence's partner clock.
These effects, not just the 93 rejection counter, drive the failed labels.

The immutable input study, rejection rule and denominator are preserved.
No revised P&L, coverage, outcome or model support is generated.

## Justified conditional native book test

A different source contract is justified because the native book feed
documents **snapshot/book-order-update timestamps** and CRC32 consistency.
The [book specification](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/book)
and [checksum guide](https://docs.kraken.com/exchange/guides/websockets/book-checksum-v2)
require ordered level updates, zero-quantity deletion, truncation to
subscription depth and top-ten checksum calculation.

The diagnostic processes all changes in wire order, including repeated
updates to the same price, validates every snapshot/update checksum using
decimal precision, and invalidates after disconnect, timestamp reversal
or reconstruction failure until a fresh valid snapshot. Checksums prove
local reconstruction consistency, **not execution or freshness**.

The initial bounded discovery validated all 2,741 actual messages
(1,538 BTC, 1,203 ETH). The parser also passed the official CRC example
`3310070434` and twelve order/truncation/disconnect/clock checks.
Discovery counts are not prospective label coverage.

**One** new, separately versioned prospective source-clock trial is frozen
for October 3 **07:34-08:04 UTC**, with fixed capture tail to **08:05:05 UTC**.
Its hash is:
`98656c49121007b81b3e9b0d1c9167a58b67e6aab4adc8ca437b7e9ae3c20706`.

The two-second provider/receive limits, $10.25 entry-ask/future-bid size,
ten-second cadence, 60-second horizon, two-second settlement and
all-mature-evaluation denominator are unchanged. The **provider clock's
meaning is explicitly versioned** as the timestamp of the checksum-valid
full top-ten book update/state. Bid/ask price-change and size-change clocks
are retained separately. New valid book updates are not synthetic
refreshes of the old ticker.

This is not v2, not a repair of old labels and not a strategy change.
The source remains anonymous public Kraken spot data; no private order
method, credential, subscription purchase or execution account is used.

## Alpaca ownership and support inquiry

Read-only inventory confirmed the existing Render observation worker is
the stream owner: `tradeagent shadow-dataset-run`, approved `94e80ec`,
one connection, zero reconnects. The legacy shadow recorder is suspended.
Dashboard and daily notifier commands are not native crypto collectors.
The prior feasibility jobs have finished; no connection was closed to
evade 406. The host-side process scan must exclude the inspection shell
itself; a command-line substring is not a second collector.

Alpaca's exact paper crypto reference and quota upgrade remain
provider-confirmation facts. No retail equities/options plan or symbol
limit is inferred to authorize extra crypto sessions.

The existing connected mailbox was verified to be a **work mailbox**.
The user was unavailable to approve its use for a personal trading-support
message. The safe decision is to leave the inquiry **unsent**, ready for
the user's personal email. No personal account identifier, key, source
code or raw trading records is included in `support-inquiry.eml`.
It has no sender header and must not be represented as sent or answered.

No model, trading authorization, v1 source/schema/window, prior study or
historical artifact was changed in this audit.
