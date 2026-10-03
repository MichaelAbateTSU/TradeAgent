# Market-data validation decision: NO-GO for v2

The requested raw-message audit is complete, and the one justified
prospective checksum-book test is complete. **Do not start v2, change the
scalping model, switch v1's source or weaken either symbol's gate.**

## Original ticker audit

All **93 original per-symbol high-water timestamp rejections** and all
**133 missing primary labels** were traced to immutable raw evidence.

The 93 comprises **73 direct adjacent wire-time reversals plus 20 repeated
timestamps below a prior maximum**, not 93 independent adjacent reversals.
There were 28 BTC and 65 ETH rejections; 75 fell in the formal window and
18 in warm-up. All were ticker updates on one connection. BTC/ETH mixing,
different channels/sessions and concurrent collector processing were
ruled out. Two rejections had the initial snapshot as high-water predecessor.

The corpus contained 13,765 size-only changes and 5,727 BBO changes with
unchanged ticker timestamps. Timestamp/trade-stat correlations do not prove
a last-trade-only clock. The provider promises only ticker data timestamp
and a BBO price-level trigger, not a monotone timestamp for every complete
BBO/size state.

| Original missing labels | BTC/USD | ETH/USD |
|---|---:|---:|
| At least one receive-freshness gap | 26 | 70 |
| Provider-time age without a receive gap | 19 | 17 |
| Displayed-size failure | 1 | 0 |
| Total missing | 46 | 87 |

These are failures under the **old declared clock contract**, not proof
that an unchanged price was invalid or that Kraken had no executable quote.
No old coverage, timestamp, price, label, P&L or model state was corrected.
See `root-cause.md` and the exact per-message/per-label audit artifacts.

## Separately versioned native book result

Window October 3 **07:34-08:04 UTC**, fixed capture tail to **08:05:05 UTC**.
Predeclared hash:
`98656c49121007b81b3e9b0d1c9167a58b67e6aab4adc8ca437b7e9ae3c20706`.
Read-only public-data job `job-db0auik9v7es73aljs6g` completed successfully.

| Symbol | Mature evaluations | Complete labels | Coverage | Required | Result |
|---|---:|---:|---:|---:|---|
| BTC/USD | 180 | 178 | **98.89%** | 95% | Data-quality pass |
| ETH/USD | 180 | 167 | **92.78%** | 95% | Data-quality fail |

BTC's two missing labels were freshness gaps. ETH had **five freshness
failures and eight displayed-size failures**. A 95% pass requires 171 of
180, so ETH was four completed labels short. The test ended on its fixed
date; collecting four favorable replacements, lowering notional or excluding
small displayed sizes is prohibited.

The two-second age limits, $10.25 entry-ask/future-bid size, ten-second
cadence, 60-second maturity horizon, two-second settlement and all-mature
denominator remained unchanged. No young evaluation was included.

The source clock was explicitly versioned as the native **checksum-valid
book snapshot/update timestamp**. Per-side price-change, size-change,
receipt and acceptance clocks were retained separately. A validated new
book update can establish updated book state even when best prices stay
unchanged. That is not a local-time refresh of the old ticker.

This source-clock definition was frozen before collection. It cannot
retroactively rescue the ticker study or v1.

## Reconstruction and causality evidence

Independent verification replayed:

- **98,200 raw frames**, their source hash chain and all artifact hashes.
- **96,200 native book messages**, applying every level change in wire order.
- Every top-ten **CRC32** after updates and depth truncation.
- All 360 primary records, exact quote identities, clocks, size tests,
  maturity denominators and no-forward-looking joins.

There were **zero reconstruction rejections**, zero checksum failures and
no capture failure/reconnect in the completed window. The initial
`new_connection_requires_snapshot` invalidation was expected initialization,
not a failed reconstruction.

The parser requires a valid snapshot, preserves decimal precision and
trailing-zero checksum representation, handles repeated same-level updates
in order, and deletes zero quantities. Disconnect, invalid checksum,
crossed/empty state or clock reversal invalidate both books until valid
fresh snapshots. Heartbeats and ping frames do not refresh book clocks.

A valid checksum proves local top-ten synchronization, not fill probability,
latency-free execution, exchange-sequence completeness or account eligibility.
The clearer native same-venue relationship is venue-level only; Kraken
account/region eligibility, spot-paper execution and verified costs remain
unestablished. No source migration, account, key or order adapter was created.

## Alpaca ownership and support status

The actual crypto connection owner remained the existing Render worker:

- Service `srv-dae4tr7qj5pc73a9e0k0`, running `tradeagent shadow-dataset-run`.
- Owner `srv-dae4tr7qj5pc73a9e0k0-6467cc5744-4txxf`.
- Approved source `94e80ec`.
- Connection `1bb1b7ef-06e8-4c44-9d0b-086ce2298122`, one connection,
  zero reconnects during the inspection.
- Old shadow recorder suspended; dashboard and notifier are not native
  crypto collector commands.

The filtered host-side Python/tradeagent process inventory found no
additional matching local collector. The earlier inspection shell's own
command text is not counted as a collector. This inventory cannot prove
there are no other account clients outside the inspected environment.
The observed 406 errors and current owner remain entitlement evidence;
no owner was terminated, unsubscribed or replaced and no paid plan was changed.

**Support inquiry status: UNSENT, sender identity blocked.**
The available connected mailbox is a corporate account. The user was
unavailable to approve its use for a personal trading-support message.
`support-inquiry.eml` is complete and ready to send from the appropriate
personal account, with no sender header, secrets, account numbers, code
or raw trading records. It was not sent through the work mailbox, notification
service or a newly installed email integration. No provider answer is claimed.

Provider confirmation of paper crypto fill-reference and exact native crypto
session entitlements remains external work. Public evidence does not prove
a retail stock/option upgrade will remove crypto 406.

## What is permitted next

**Overall readiness remains NO-GO. Selected source/configuration: none.**

The completed test improves understanding of the measuring instrument:
the verified book-state clock performed substantially better than the ticker
clock in its own window, but the different windows/source definitions are
not a controlled causal comparison. It still did not meet both symbols'
95% criterion, and it says nothing about profitable alpha.

Do not launch another favorable-window search, remove size failures,
reinterpret the frozen threshold or start v2. The immediate external
prerequisite is appropriately sent/provider-answered support clarification.
Any later architecture decision must explicitly address eligibility,
paper-execution classification and costs, with a new predeclared study if
its source contract changes. No such change is authorized by this negative
two-symbol result.

## Validation and preserved history

The official CRC example `3310070434` passed, as did twelve focused
checksum/order/deletion/truncation/disconnect/clock tests. A real
pre-study discovery independently passed 2,741 book CRCs and was not
counted as quality evidence.

The unchanged application passed **1,925 tests, two skipped**, Ruff and
strict mypy over 121 source files. Original v1, ticker, earlier source studies,
`fc026af`, `85404eb` and `dc00e4e` remain preserved without code or data edits.

Before/after read-only broker proofs show zero positions, open orders and
order records since freeze. V1 retained its existing source/connection,
**expired authorization**, **`no_support`**, and zero reported order attempts.
No real/paper trading cohort, private Kraken request, model promotion,
source switch or v2 start occurred.
