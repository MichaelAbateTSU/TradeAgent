# Fixed native-book confirmation, infrastructure revision 2

This is measurement research with **zero orders**, not Shadow Research
Dataset v2 or a model promotion. The original v1 dataset remains
`paused_invalid`; the first confirmation remains `failed_incomplete`.
Neither is restarted or rewritten.

`root-cause-and-repair.md` explains the three original broker-monitor
failures and the demonstrated independent-collector guard dependency.
The repaired guard requires preservation of that exact paused observer
and obtains its own fresh GET-only broker safety proof. Restored API
availability never clears the historical stop.

## Frozen contract

`frozen-protocol.json`, once emitted after validation, is the authority
for source commit, exact byte hashes, start/end times and guard pins.
The new schema and study ID are both `kraken-book-confirmation-v2`.
They are not aliases for the failed original ID.

| Requirement | Unchanged value |
|---|---|
| Symbols | BTC/USD and ETH/USD |
| Window | Exactly 72 hours, with 25,920 scheduled evaluations per symbol |
| Cadence / primary horizon / settlement | 10 seconds / 60 seconds / 2 seconds |
| Provider and receive freshness | 2 seconds each |
| Entry ask / future bid displayed notional | $10.25 at the best side |
| Source | Native public Kraken book, depth ten |
| Book validation | Every snapshot/update checksum, ordered decimal reconstruction |
| Capture tail | 75 seconds after the fixed end |
| Full-quality gate | At least 95% independently for each symbol |
| Breakdowns | Twelve fixed six-hour blocks, UTC dates, weekday/weekend |

Book-state, price-change, quantity-change, receive and acceptance clocks
remain distinct. Disconnects, checksum/reconstruction failures and clock
failures invalidate the book until resynchronized. Heartbeats and BTC
updates do not refresh ETH. Missing scheduled slots and unwritten mature
labels remain in the denominator. Integrity/freshness/conditional size
diagnostics do not replace the full gate.

The native book parser's bytes are unchanged from the independently
verified short trial. No ticket, threshold, maturity rule, venue, or
measurement formula was optimized to get a favorable result.

## Guard and immutable lineage

The protocol pins both the original shadow protocol hash and SHA-256 of
the exact persisted stop-control bytes. Each guard requires stopped,
unauthenticated, unsubscribed v1 in `paused_invalid`, no automatic rearm
or protocol-change flag, unchanged source/owner/connection, fresh
heartbeat/lease and the global 0015 schema.

It also verifies the fixed paper host/account digest, zero positions,
open orders and broker records since the original freeze, zero local
order attempts, expired authorization and `no_support`. Missing or
unsafe current proof fails closed. New HTTP failures record only safe
exception class, allowlisted endpoint and available response status,
never headers, queries, response bodies or credentials.

Read-only preflight verifies the exact receiver bytes, existing auxiliary
schema, original sealed journal compatibility and current safety before
any new claim. It explicitly reports **preflight only**, not market
integrity, coverage, executable-price validity or readiness for research v2.

The original confirmation identity remains:

```text
4a232f3b8ad5e1a02db909f4cae86cc90f6ea25e01578491cab8f5e3839dc982
```

Its three-chunk failed journal root remains:

```text
1ec4d51937c9761c3f5692c643d8431861cbeefa2ebfb8412865f04fe776b463
```

V1 serializers, default report/export selection, guard semantics and
all historical records remain compatible. Study ID selection is
explicit; neither claim can be resumed or replaced.

## Operations

Use the source revision and protocol recorded in this folder:

```powershell
python -m tradeagent.kraken_confirmation_runtime preflight frozen-protocol.json
python -m tradeagent.kraken_confirmation_runtime run frozen-protocol.json
python -m tradeagent.kraken_confirmation_runtime status --verify --study-id kraken-book-confirmation-v2
python -m tradeagent.kraken_confirmation_runtime export --study-id kraken-book-confirmation-v2
```

The old failed study remains separately inspectable:

```powershell
python -m tradeagent.kraken_confirmation_runtime status --verify --study-id kraken-book-confirmation-v1
```

The receiver runs as one isolated job with the exact four-module source
bundle. It does not redeploy the old observer or other services, install
a trading adapter, open an Alpaca market-data connection, or change
credentials. The global schema stays at 0015; do not run a global
`alembic upgrade head`.

Append-only compressed raw/quote/label/guard evidence and hash-chain
manifests persist in PostgreSQL. Bounded queues, rings and storage limits
fail closed. Hourly, fixed-block and terminal reports persist
automatically. Process death produces failed/incomplete evidence, not
automatic continuation. A successful warm-up smoke does not prove the
72-hour quality gate.

There is no favorable-window restart, denominator substitution, missing
label backfill, extension, source switch, order-size reduction or ETH
removal. At the fixed end, wait for the original capture tail and inspect
the terminal report; do not keep collecting replacement observations.

## Remaining economic and operational uncertainty

The existing database's nominal headroom was measured, but true
disk/WAL/index headroom and 72-hour endurance were not certified.
Budget enforcement is a safety stop, not a capacity guarantee.

Kraken account/jurisdiction eligibility and account-specific maker/taker
rates remain unverified. Public cost scenarios remain scenarios. Native
venue-level quote evidence does not establish personal execution
eligibility, fills, profit, or economic edge.

Only a completed fixed study with clean integrity and both symbols at
95% permits considering later research, after venue and cost assumptions
are resolved. BTC-only research would require a separate new protocol
and validation, preserving this two-symbol decision. No automatic
strategy revision, paper/live orders or promotion follows this study.
