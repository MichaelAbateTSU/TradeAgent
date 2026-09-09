# Actual Render worker paper-order test

At **00:37:09 Eastern on September 9**, the running Render event worker submitted
an actual Alpaca paper AAPL limit order and canceled it approximately 0.18 seconds
later. This was a **submit/cancel API test, not a filled trade**.

| Evidence | Actual result |
| --- | --- |
| Running worker | `srv-dae4tr7qj5pc73a9e0k0-657f7bdb5b-vfzh4` |
| Worker code | `8bcaacd677b47c274ca869454ff28fc55b65c147` |
| Alpaca order ID | `dd9ba5c8-8319-4466-83c0-af3cc9911c73` |
| Client order ID | `ta-probe-20260909-0020-aapl` |
| Order | BUY 0.1 AAPL, $100 limit, $10 maximum notional, DAY, extended hours false |
| Broker status | Accepted, then canceled; filled quantity zero |
| Ending account | Zero positions and zero open orders, independently read back |
| Email worker | Existing `tradeagent-notifier`; sent in one attempt |
| Resend message ID | `4aa6eeb3-9947-43f1-9b21-329414ff3dde` |
| Normal trading | Global kill remained active; no strategy authorization issued |

A one-off Render job published only a bound database command. It made **no Alpaca
calls**. The already-running event worker consumed that command under its normal
ownership lease and used the application's typed paper-only broker adapter.
The worker persisted the result and queued an idempotent email. The separate
Render email worker sent it; provider acceptance was confirmed at 00:37:13 Eastern.
The later verification job performed only broker/database reads.

The fixed limit is a diagnostic ceiling, not a current quote, forecast, or trading
signal. Closed-market orders with extended hours disabled can queue for the next
regular session, so the worker canceled this one rather than leaving unintended
morning exposure. No stop, take-profit, P&L, or completed round trip is invented
for an order that never filled.

The single-use request is bound to the exact code, configuration, cohort and paper
account. Its client ID is persisted before submission; uncertain results never
cause another BUY. Recovery cancellation still operates if unrelated activity or
a changed entry pause appears. Normal position supervision runs before optional
diagnostics. The worker's finite new-submission window is September 9, 00:00–01:00
Eastern; only recovery is permitted after that. A terminal result cannot submit again.

Full broker/worker/notifier IDs and independent readback:
[receipt](../research/results/operator-paper-probe-20260909.json).
The new deployment completed **662.53 seconds / 882 HTTP requests with zero HTTP
errors**, maximum application memory **201.86 MiB** and PostgreSQL **206.81 MiB**.
No new recorder drops or gaps were observed. The full acceptance gate remains
pending: the closed-market recorder did not have advancing quotes, trades or bars.
Those checks cannot substitute for regular-session data flow or an actual filled
BUY/SELL round trip. Morning news-based trading remains separate, paper-only work.
