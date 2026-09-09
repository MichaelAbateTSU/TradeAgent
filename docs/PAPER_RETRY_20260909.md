# September 9 afternoon workflow retry

## New owner request and capacity approval

At 12:18 Eastern the owner requested another workflow run while the market was
open. At **12:23:39 Eastern** the owner explicitly approved the quoted PostgreSQL
upgrade from **0.1 CPU / 256 MB, $6/month**, to **0.5 CPU / 1 GB, $19/month**.
The incremental compute charge is **$13/month**, separate from unchanged storage,
application services and Pro workspace charges.

Only database `dpg-dadn7nht0dsc73f9jja0-a` was resized. Its identity, 15 GB disk,
private connectivity, empty public allowlist and Production environment were
preserved. No worker replica, other compute plan, credential or recipient was
changed. The database was temporarily unavailable during the resize; application
restarts briefly rejected still-current leases and later recovered naturally.
No lease was stolen and no old data, order budget or experiment terminal was reset.

The new capacity experiment uses the reviewed `pg-1gb-v1` acceptance profile:
at least 30 minutes, PostgreSQL below 800 MiB, applications below 400 MiB, actual
plan verified before and after, and every original loss/freshness/HTTP/owner/
physical-data/report/broker gate unchanged. Historical 256 MB failures retain
their 230 MiB threshold and failed outcomes.

## Separate account-day limit blocks another entry

The broker's complete September 9 Eastern-day order read returned these actual
BUY submissions:

| Order | Purpose | Outcome |
| --- | --- | --- |
| `dd9ba5c8-8319-4466-83c0-af3cc9911c73` | Overnight AAPL diagnostic | Accepted, then canceled; zero fills |
| `385947be-92ac-4215-98f4-54380075a145` | Overnight BTC/USD operator test | Filled; subsequently sold |

The configured owner policy permits **two total account-day BUY entries/attempts**.
Canceled-after-submission orders do not restore a slot, and the separately
implemented operator paths are not an authorized exemption from an account-wide
limit. Therefore **zero additional BUY attempts are available today under the
unchanged limit**. A fresh infrastructure run or another cohort cannot reset it.

The operator tests use controls/audit rather than normal `event_order_links`.
The current shared OMS/economic ledger therefore does not automatically include
all their history. This is a known accounting integration gap, **not permission
to treat their orders or losses as absent**. No afternoon entry is authorized
using that incomplete ledger. The morning cohort's MISSED record, terminal,
original global kill and operator pause remain intact.

At the **12:28:45 Eastern** broker read, positions and open orders were both zero.
The canceled AAPL diagnostic is still not a filled equipment test, and the BTC
operator test is still not a qualifying strategy result.

## Posted crypto fees now visible

The same read returned the genuine BUY/SELL FILL activities, a **$0.05 USD CFEE**,
and a **0.00000062 BTC fee quantity** at the original BUY price. The BTC fee explains
the difference between purchased and subsequently sellable quantity.

The fill-value difference was `-$0.07723217556`. After the posted USD fee, the
cash-net round-trip result was **`-$0.12723217556`**. Do not subtract the BTC fee's
dollar value again: its effect is already reflected in the reduced sell quantity.
This new fee evidence supplements, rather than overwrites, the earlier explicitly
provisional fee estimate.

## Notifications and completion

The retry outcome must be sent once through the existing Render notifier and
recipient, including a blocked outcome. No previously accepted diagnostic,
crypto lifecycle or daily summary message may be resent. Provider acceptance
must be recorded separately from inbox delivery.

Permanent notification handling for a current session's MISSED/terminal outcome
is being added to close the omission discovered earlier today. It must not
change trading authority, query heavy market history, replay old-cohort emails,
or generate an expensive full report on every poll.

Capacity acceptance, trade authorization and email acceptance remain separate.
The 15:45 Eastern close watch still must observe actual flatness through close.

## Evidence

- [Approved resize request and response](../research/results/render-db-upgrade-20260909.json)
- [Pre-resize flat account and original controls](../research/results/render-db-upgrade-20260909-before-broker.json)
- [Planned outage and reconnection observations](../research/results/render-db-upgrade-20260909-reconnection.json)
- [Resize-related process events](../research/results/render-db-upgrade-20260909-process-events.json)
- [Complete account-day broker orders and posted fees](../research/results/news-paper-20260909-afternoon-broker-budget.json)
