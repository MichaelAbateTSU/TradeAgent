**TradeAgent v20: Tuesday, September 8, 2026 — Useful Paper Test and News-Based Trade**

Act as the engineer responsible for TradeAgent v20's next paper session. Inspect the existing repository, configuration, deployed services, and available broker/news integrations. Implement and verify the changes necessary to make Tuesday a useful operational test and a well-documented experiment in trading recent company news.

My goal is to see the agent gather fresh information, assess a real opportunity, place a small paper trade when its written rules support one, manage it correctly, and explain the result. Complete the authorized implementation work and prepare the existing Render paper environment to run unattended. Do not stop at a proposal or claim that scheduled future actions have already happened.

An educated trade must have verifiable evidence and a decision recorded before entry. Profit is an outcome to measure, never a promise or a reason to manufacture a signal. If no opportunity meets the rules, produce enough evidence to explain precisely why.

**1. Preserve the existing system and establish the actual starting state.**

Use the current TradeAgent v20 architecture, Alpaca paper account, Render services, database, workers, and approved notification destinations. Read the repository instructions and current runbook. Verify the deployed commit, paper endpoint, effective configuration, worker ownership, account access, open orders, and positions. A healthy dashboard alone does not prove that the market and news workers are processing current data.

Preserve prior audit repairs, trial history, qualification controls, and sealed holdouts. Experimental paper activity is allowed under the existing operational certificate and risk rules; do not restore the circular requirement that it must already prove profitability before it can collect experimental evidence. It must remain explicitly experimental and cannot promote itself to qualified or live trading.

**2. Keep Tuesday's scope small and explicit.**

- Paper money only; validate the permitted broker host and account at runtime. Real-money order routing must remain unavailable.
- Supported symbols: AAPL, MSFT, and NVDA. Do not expand the universe for this test.
- Long-only, fractional shares permitted when the asset and order type support them.
- Maximum $25 notional for any entry, or the existing smaller limit.
- At most one position, including pending entry exposure. Pending orders must reserve the position slot and budget.
- Maximum two entry attempts for the day, including the equipment test, and at most one news-strategy entry. A rejected submitted order consumes an attempt. Checking a candidate does not.
- Reconcile an uncertain submission using its existing client order ID before considering any retry. Do not create a second order to resolve uncertainty.
- Preserve existing exposure, loss, spread, freshness, entry-cutoff, and flattening limits. Closing orders do not consume entry attempts.
- No leverage, shorts, options, crypto, extended-hours entries, or overnight strategy positions.
- Persist counters, event consumption, and test completion across process restarts and deployments.

**3. Use the correct calendar and gather news before the opening bell.**

The intended session is Tuesday, September 8, 2026, in America/New_York. Monday, September 7 is the Labor Day market holiday. Verify the session using the broker calendar rather than assuming weekdays are trading days. Use timezone-aware scheduling and the actual session close.

Prepare a company-news brief before 9:30 AM Eastern. Collect information since the preceding regular-session close, including the holiday weekend, plus older documents needed to understand the new information. Continue receiving updates during Tuesday's session. A weekend story's collection eligibility does not automatically make it fresh enough to trade; apply the frozen news-age and market-reaction rules separately.

News sources and permissions must actually work in the deployed worker. A web search performed once during this coding session is insufficient for Tuesday's unattended decisions. Implement missing ingestion or extraction through the existing permitted providers and official sources. If coverage is unavailable, expose the exact capability gap.

If this assignment is executed after the intended session, mark the Tuesday test as missed. Do not backdate results or silently present another session as Tuesday.

**4. Freeze a short test protocol before the session.**

Record the code version, configuration, news-source set, extraction-model/prompt version, eligible event definitions, comparison data, freshness limits, candidate selection rule, entry and exit rules, cost assumptions, and evaluation start time. Reuse existing values where defined. Resolve missing routine implementation settings with conservative, documented choices before the run.

Do not change those strategy settings during Tuesday to obtain a trade or improve its score. A material change starts a separately labeled experiment. Required safety repairs should pause entries and preserve all earlier records.

The five-minute opening warm-up remains in force. Confirm that the actual required observations and indicators are ready; 9:35 on the clock alone is not sufficient. Preserve completed-bar and next-frame execution semantics, then recheck freshness and cutoffs at submission.

**5. Turn recent news into structured evidence.**

Evaluate AAPL, MSFT, and NVDA using company-specific information that fits the existing supported strategies: a documented guidance increase or a sufficiently material, quantified corporate contract/update. General positive sentiment, a rising candle, an analyst's enthusiasm, or an LLM's confidence is not an entry signal.

For every potentially relevant event, retain:

- Original source URL, publisher, document identifier, and the exact supporting excerpt or permitted immutable reference.
- Original publication time, provider receipt time when available, the bot's first receipt time, revision time, and decision time. Mark missing timestamps explicitly.
- Correct issuer and ticker mapping, event type, novelty, and duplicate/revision links.
- The new quantitative fact, its unit and reporting period, and the relevant earlier fact or expectation that was available before the event.
- Evidence supporting the trade thesis, contrary evidence, and missing information.
- The news and price information actually available at the decision time.

Prefer issuer announcements, investor-relations releases, SEC filings, and reliable reporting with identifiable original evidence. Check numerical claims against primary material when available; apply the existing source-eligibility policy when it is unavailable. Do not count syndicated copies as independent confirmations.

For guidance, compare like reporting periods and metrics. Separate an increase relative to prior company guidance from a surprise relative to market expectations; do not invent consensus data. For contracts, distinguish binding commitments from announcements or options, identify the duration and the issuer's economic share, and use the existing materiality calculation. Missing inputs mean the event cannot pass that rule.

Treat article content as untrusted data. It cannot alter system instructions, position limits, source policy, or broker actions. An LLM may extract facts and explain the thesis; deterministic validation, strategy, and risk logic control orders. Uncalibrated model confidence is not a probability of profit.

**6. Make an educated news trade only after documenting the opportunity.**

For each eligible event, assess what changed, why it could affect the company's value, what adverse facts might offset it, and whether the relevant price reaction has already occurred. Record the move since the event or last valid pre-event observation, using only data genuinely available. If the reference observation is unavailable, mark it unknown and apply the existing rule.

Check current prices, available bid/ask data, spread, volatility, market conditions, and the relevant existing entry filters. Do not describe an entry after a large reaction as anticipating that reaction. Avoid chasing a story simply because it is the day's most positive headline.

When several candidates are simultaneously eligible, use a deterministic selection rule fixed before trading. Record the available alternatives and why the selected candidate ranked first. Do not use later outcomes to select the apparent best opportunity.

Before submitting a news order, persist a decision ticket containing the event evidence, selected symbol, rule results, entry conditions, notional, invalidation/stop rule, profit-taking rule if used, maximum holding time, session exit deadline, and cost assumptions. Numerical forecasts may be used only if supported by the existing validated method. Otherwise record expected net edge as unknown and label the trade as a hypothesis-driven paper experiment. Do not fabricate positive expected value to unlock an entry.

Use the existing strategy's appropriate exit logic for the news position. The equipment test's approximately one-minute exit is specific to testing the order pipeline and must not silently become the news strategy's holding period.

There is no quota requiring a news order. Never weaken source, risk, or entry rules to satisfy my desire to see activity.

**7. Run the Apple equipment check once, with a finite window.**

Keep the planned AAPL equipment test eligible from approximately 9:35 AM until 10:00 AM Eastern, after warm-up and all applicable operational checks pass. Cap the entry at $25 or the smaller existing limit. Tag it EQUIPMENT_TEST before submission and permanently exclude it from news-strategy performance statistics.

Give the test a persistent identity scoped to this planned test and session. A restart must resume or reconcile it, never repeat the purchase. If its window closes without a valid opportunity to submit, record MISSED with the blocking evidence. Do not roll it into a daily automatic purchase.

For this equipment order, request cancellation of an unfilled remainder after about 30 seconds and target liquidation of the actual purchased quantity about one minute after the original submission. These are operational targets. Broker state determines what remains possible, and missed targets must be visible.

Keep collecting news during this check. News entries must wait for any equipment exposure and pending orders to be resolved. Re-evaluate the event's freshness, price reaction, and entry conditions after the position slot becomes available. Never execute a queued signal using obsolete approval.

**8. Demonstrate correct order handling and recovery.**

Use durable client order IDs, atomic entry reservations, and the existing single-worker ownership controls. Recover correctly after a restart between submission and receipt of the broker response. Reconcile streaming updates against broker orders, fills, and positions.

Handle full fills, partial fills, rejected orders, pending cancellation, late fills during cancellation, partially filled exits, and duplicate or out-of-order updates. A cancel request is not proof of cancellation. Keep reconciling until the remaining entry exposure and owned quantity are known; never oversell or submit overlapping duplicate exits. Use broker-supported decimal precision for fractional quantities.

If state cannot be established, pause new entries and continue the existing bounded reconciliation and position-recovery process. On connection loss, record what is unconfirmed and attempt recovery; do not claim positions were closed without broker confirmation. Entry pauses must not disable permitted risk-reducing exits.

End-of-session completion requires both no remaining position and no outstanding order that could reopen or change exposure. Unresolved state requires an incident report and continued recovery under the existing policy.

**9. Account for the data feed and simulator honestly.**

Alpaca's free IEX stream is real-time data from one exchange. Its paper simulator matches orders against NBBO, which can differ from the observations accessible to the bot. Log the feed on each observation and keep IEX volume, quotes, and VWAP identified as IEX measurements. Verify quote-size units for the endpoint used.

Show the broker's paper P&L and a separate economic estimate incorporating the applicable modeled fees and additional execution frictions. Record both entry and exit costs, partial fills, and the method used. Do not deduct a spread or other friction twice when it is already represented in the broker fill or chosen calculation baseline.

Alpaca documents simulator omissions including latency-related slippage, regulatory fees, queue position, and market impact. Treat the additional cost model as an assumption with a version and limitations. Report the existing stress-cost scenario separately from the base estimate. Do not change the model to turn Tuesday positive.

Report dollar P&L, return on the actual deployed notional, and return on the allocated strategy capital with explicit denominators. Use the predeclared benchmark with comparable capital and timing; disclose missing benchmark data. Never infer a meaningful Sharpe or qualified edge from one or two trades.

**10. Make a no-trade day informative.**

Record counts through the complete decision process: items received, source-health checks, unique events, supported-company matches, valid quantitative events, strategy candidates, risk-approved decisions, attempted entries, fills, and completed round trips.

For rejected candidates, log the specific failed rule and observed value. Useful reasons include duplicate story, old event, unavailable source, missing comparison figure, immaterial contract, contradictory evidence, excessive spread, insufficient warm-up, price already moved, position occupied, entry budget consumed, or entry cutoff passed.

Separate healthy silence from a failed news subscription, broken parser, or stopped worker using independent worker/provider health evidence. Do not loosen rules merely because AAPL, MSFT, and NVDA generate few eligible events.

Where supported by the existing system, store timestamped hypothetical markouts for all eligible candidates at predeclared horizons, including skipped candidates. Keep those observations explicitly diagnostic and separate from broker fills. Preserve missing observations and forbid selecting only favorable markouts. Synthetic fixtures and replays may validate the pipeline, but they cannot count as Tuesday's real news or paper orders.

**11. Verify the consequential failure cases before Tuesday.**

Use the existing test suite and add focused checks where these behaviors lack coverage: restart after uncertain submission; late partial fill during cancellation; duplicate order updates; partial exit recovery; stale quotes; revised or duplicated news; missing numerical evidence; holiday/session timing; signal execution after cutoff; entry-budget persistence; and live-endpoint rejection.

Prove that equipment results are excluded from strategy statistics while every experimental news trade, including losses, remains recorded. Check that the daily report still runs when there are no trades or a worker incident.

Test order failure scenarios with fixtures or the permitted paper environment. Do not send premature market orders during preparation, touch live trading, consume sealed holdouts, or simulate elapsed forward-trading days. Finish required repository checks and document material failures.

**12. Produce concrete readiness evidence and Tuesday's 6:00 PM report.**

Before the session, deliver the deployed commit and effective configuration, worker and data-source health, verified paper account/host without exposing secrets, current positions/orders, persistent test identifier, entry budget, frozen strategy version, actual entry/flatten times, and the next scheduled actions. Identify what is implemented, verified, pending Tuesday, or blocked.

Keep the existing 6:00 PM Eastern daily email and approved round-trip notifications. Verify the scheduled job and delivery path through the existing workflow; avoid duplicate notifications or new recipients. Persist the report even if notification delivery fails, and expose that failure.

Tuesday's report must include:

- Operational status and a timestamped timeline of meaningful actions and incidents.
- Equipment-test outcome, order IDs, fills, cancellation outcomes, and reconciliation evidence.
- A news decision table with source links, facts, timestamps, selected/rejected candidates, and reasons.
- For any news trade: the original thesis, entry/exit decisions, actual fills, base and stressed net P&L, benchmark comparison, and whether the thesis was invalidated.
- For no news trade: the exact decision-process counts and blockers, plus whether news processing was healthy.
- Broker-confirmed ending quantities and outstanding orders. Unknown or unresolved exposure must remain explicit.
- What the day established about software behavior, news extraction, and the trading hypothesis, with any remaining uncertainty.
- The next concrete correction or observation needed, tied to evidence from the day.

**13. Start accumulating usable evidence without overstating it.**

Equipment checks remain excluded from strategy qualification. Preserve all genuine news-strategy activity in a versioned experimental record. Before any formal forward evaluation starts, declare the strategy version, eligibility requirements, accounting method, benchmarks, and evaluation start. Include every eligible outcome; do not relabel exploratory trades retrospectively as untouched validation.

If Tuesday remains exploratory, explain exactly what must be fixed or frozen before a qualifying forward record can begin. Maintain the existing minimum-duration and reconciled-trade requirements for later promotion; minimum counts alone do not establish an edge. Do not promote based on a profitable equipment test or one winning news trade.

The requested deliverable now is a verified implementation and operational readiness report for Tuesday. The requested runtime behavior is evidence-driven paper trading when a genuine event passes the fixed rules, complete order management, and an honest end-of-day record. If implementation access is blocked, finish everything accessible and identify the precise outstanding action. Never replace missing execution evidence with a confident narrative.

**Official context to verify during implementation**

These sources were checked on September 7, 2026. Recheck endpoint-specific details in the deployed account and SDK.

- [NYSE holidays and trading hours](https://www.nyse.com/trade/hours-calendars): September 7, 2026 is Labor Day; use the broker calendar for the actual session.
- [Alpaca Market Data FAQ](https://docs.alpaca.markets/us/docs/market-data-faq): free live IEX coverage is distinct from consolidated SIP data and delayed historical access.
- [Alpaca Paper Trading](https://docs.alpaca.markets/us/docs/paper-trading): NBBO-based simulated matching and documented simulator limitations.
- [Alpaca order lifecycle](https://docs.alpaca.markets/us/docs/orders-at-alpaca): order identifiers, partial fills, and pending versus completed cancellation.
