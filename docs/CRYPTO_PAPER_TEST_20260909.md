# Completed Render-worker crypto paper test

**An actual BTC/USD paper round trip completed September 9, 2026.**
The owner's explicit 01:51 Eastern instruction authorized this one crypto
equipment test, separate from the frozen stock/news research and its entry pauses.

The already-running Render event/news worker fetched **16 BTC news items** and
current crypto quote/asset data. It submitted a market BUY capped at **$20
notional**, then sold only the broker-confirmed remaining BTC quantity. A one-off
Render job merely published the scoped database command; it made **no broker
calls**. Both orders were submitted by worker
`srv-dae4tr7qj5pc73a9e0k0-8c6c556df-5qsrm`, code
`dddb3d7e97decc2682f1af8210a8c27f16248a97`.

| Broker evidence | BUY | SELL |
| --- | --- | --- |
| Order ID | `385947be-92ac-4215-98f4-54380075a145` | `1d9dad72-351e-4478-bf2f-3f7d1079b8dd` |
| Filled at, Eastern | 02:20:49.378751 | 02:21:28.132488 |
| Filled BTC | 0.000247676 | 0.000247056 |
| Average USD price | 79,217.31 | 79,103.50 |
| Filled USD value | 19.62022647156 | 19.5429942960 |
| Status | filled | filled |

The worker exited because its monitoring quote was unavailable or failed the
five-second freshness/validity check. It did **not** wait indefinitely for a
positive price. The frozen one-test policy also had a 1% gross-loss trigger and
a five-minute holding target. Execution can be delayed or slip; neither a profit
nor an exact stop-fill price was guaranteed.

The fill-value difference was **-$0.07723217556**. The conservative net estimate
was **-$0.1260896613**, including a 25 bp estimated sell fee. The buy-side
credited-quantity shortfall was already included in the sale quantity and was not
deducted again. The readback had FILL activities but no CFEE/FEE activity yet;
this estimate is not a final reconciled account-fee statement.

An independent, read-only job on the same deployed image confirmed both order
IDs and their FILL activities at **02:23:17 Eastern**, with **zero positions and
zero open orders**. The global stock-trading kill switch remained active.
The existing Render notifier sent acceptance, buy-fill, sell-submission and
completion messages, each in one attempt. Provider acceptance is verified, not
recipient inbox delivery.

This proves a worker-executed paper round trip and notification workflow. It
does not establish a profitable crypto strategy, news-derived alpha, or a passed
regular-session stock-data acceptance test. The news was operator-test context,
not a fabricated confidence score or economic forecast. No further crypto entry
is authorized by this completed test.

The client ID and unresolved states are persisted before submission. Timeouts
recover by original ID; canceled remainders must be confirmed before a partial
exit is replaced. Nullable requested quantity on notional orders is retained
honestly; only confirmed filled/held quantity sizes exits. The live brokerage
host remains structurally unavailable.

Detailed immutable evidence, fees caveats and email IDs:
[crypto paper receipt](../research/results/crypto-paper-test-20260909.json).
The original stock/news cohort and morning acceptance remain separate.

The crypto release's live observation covered **662.47 seconds / 882 HTTP
requests**, with zero HTTP errors. Sampled application memory peaked at
217.96 MiB and PostgreSQL at 215.89 MiB. The regular-session stock-data acceptance
gate was not waived because the stock recorder was correctly inactive after hours.

After confirming liquidation, the existing event service was restored to the
previously reviewed morning code `29cf2327e8145af510735faa5a51d852a747a47b` and
original `v20-news-paper-20260909-r1` cohort. Its unchanged global kill and operator
pause were verified with the new owner. The 09:35 Eastern read-only market-load
acceptance and later conditional stock/news activation remain unchanged; this
crypto test is not substituted for that gate or the planned AAPL equipment test.
The restored deployment was observed again for **662.68 seconds / 882 HTTP
requests**, with zero HTTP errors. Application memory peaked at 217.26 MiB and
PostgreSQL at 178.07 MiB. Its same-image read-only probe independently confirmed
the original crypto orders still filled, account flat with no open orders, and
the original morning code/cohort/global/operator pauses restored. Regular-session
stock-data acceptance remains pending and was not waived.
