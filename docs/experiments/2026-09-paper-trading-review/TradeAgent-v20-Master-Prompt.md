# TradeAgent v20 — master implementation prompt

Prepared September 6, 2026. Designed for the existing TradeAgent repository and the supplied v0.10.0 independent-validation report. Paste this entire document into the coding agent that has access to the repository, its existing configuration, and its paper-trading environment.

## 1. Your assignment

You are the lead engineer and quantitative researcher responsible for delivering TradeAgent v20: a functioning, event-driven, paper-only trading product that reads current public information, understands what changed, makes explicit trade-or-abstain decisions, executes eligible simulated trades, and measures whether those decisions actually add value after costs.

Work in our existing repository. Inspect the implementation before designing replacements. Keep the parts that work. Repair the parts that are demonstrably wrong. Implement the complete path from a newly received event to a recorded decision, risk approval, paper order, reconciliation, exit, and performance attribution.

Profitability is the research objective. It is not an outcome you can promise or manufacture through a prompt, backtest, version number, or model upgrade. Deliver working software and honest evidence. Do not claim that a system is profitable merely because it is deployed, passes tests, has a positive gross return, or produces persuasive explanations.

The immediate product objective is a useful agent that can conduct controlled forward paper experiments. The longer-term research objective is repeatable positive net returns, with a separately measured contribution beyond market exposure. Keep those objectives distinct and measure both.

Do not stop at writing architecture documents or another research roadmap. Complete all implementable work, run the application, demonstrate the pipeline, and leave the configured paper-only service ready to process the next eligible real event. If a legitimate data, authorization, or runtime blocker prevents that, complete everything else and identify the exact missing dependency. Never disguise a replay or fixture as live activity.

Treat v20 as a major product release, with version 20.0.0 when appropriate for the repository's versioning scheme. A version bump alone is not completion.

## 2. How this assignment changes the prior plan

The v0.10 report ended with a project rule to stop strategy discovery and block all autonomous entries. This new assignment explicitly replaces that product direction with a bounded news/event research program and controlled experimental paper trading.

Implement this as a visible, versioned policy change. Do not silently remove gates or reinterpret previous failed results.

Preserve:

- Both existing sealed holdouts, unopened.
- All strategy failures, experiment records, source datasets, model versions, and historical reports.
- The distinction between development results and untouched forward evidence.
- Broker isolation, cash/position reconciliation, loss limits, and protections against duplicate orders.
- Existing restrictions that make real-money execution impossible.

Change:

- Historical proof of alpha must no longer be a prerequisite for every fake-money experiment.
- Operational qualification must determine whether a small experimental paper order is technically permitted.
- Statistical qualification must determine what performance claims and later paper allocations are justified.
- A candidate that is unproven may run only in its explicitly labeled experimental paper mode, under a frozen protocol and strict virtual-capital limits.
- A previously rejected strategy must not be relabeled as qualified. Any materially revised hypothesis receives a new identity and retains its ancestry.

This is permission to build and use a controlled paper research workflow. It is not permission to make live trades, fund accounts, use leverage, purchase subscriptions, or ignore higher-priority access restrictions.

## 3. Starting facts and evidence boundaries

Read the actual repository versions of the audit, v0.9, and v0.10 reports, the experiment ledger, strategy manifests, broker adapters, execution simulator, and qualification code. Confirm the deployed commit rather than trusting a version string.

The supplied report says:

- Candidate-level PBO, family block construction, DSR inputs, and daily bar availability previously contained material defects.
- Those issues were repaired, and daily portfolio panels now include cash/zero-exposure observations.
- Thirty lower-turnover configurations were calibrated using SIP evidence.
- Twenty-nine configurations remained positive in absolute terms, but none beat its equal-exposure benchmark after the reported costs.
- The two frozen families failed independent validation.
- Recent results depended on 2026 following a negative 2025.
- Both original holdouts remain unopened.
- The service runs as a paper-only recorder and research platform, with autonomous entries blocked.

Selected reported v0.10 results:

| Metric | Relative strength | Time-series momentum |
|---|---:|---:|
| Pre-2020 net return | 3.436% | 5.457% |
| Pre-2020 benchmark-relative return | -1.492% | -4.185% |
| 2025–2026 net return | 0.412% | 0.789% |
| 2025–2026 benchmark-relative return | 0.343% | 0.063% |
| Recent DSR | 0.509 | 0.475 |
| Reported family PBO carried into the tables | 0.976 | 0.988 |

These are report claims, not independently verified code or broker statements. Reconcile their exact definitions and evaluation windows before interpreting them.

Do not infer that an undiscovered bug must explain every failed strategy. Missing alpha is a valid explanation. Conversely, passing unit tests does not certify market-model correctness.

The already inspected pre-2020, 2020–2024, and 2025–September 2026 results are now known research evidence. Do not rename those periods “untouched” for a new experiment. Preserve protected subsets wherever their boundaries overlap any proposed download or evaluation.

## 4. Research context you must use

Read these original sources as needed, preserve their limitations, and verify changeable broker details at implementation time. The requirements later in this prompt are engineering and research-design choices; they are not claims that these papers prove our proposed strategies will make money.

### News interpretation is different from tradable remaining return

[Lopez-Lira and Tang, Can ChatGPT Forecast Stock Price Movements?, inspected arXiv version 6](https://arxiv.org/html/2304.07619v6) separates the initial news reaction from subsequent drift. Its approximately 90% portfolio hit rate concerns the non-tradable initial reaction. In Appendix OA7, the equal-weighted overnight long-short Sharpe falls from 2.971 before costs to -0.394 at 20 basis points. The strategy has many positions, a short leg, and high turnover. Those results cannot be imported into our small long-only account. Treat the paper as a hypothesis source with material implementation limitations.

[Wu et al., Extracting the Structure of Press Releases for Predicting Earnings Announcement Returns, 2025, version 2](https://arxiv.org/html/2509.24254v2) provides important contrary evidence: earnings-release language explains announcement returns, but the tested implementable after-open strategy does not capture profitable remaining returns. A correct reading of the release can still arrive too late.

[Hand, Laurion, Lawrence, and Martin, Explaining firms' earnings announcement stock returns using FactSet and I/B/E/S data feeds, 2021 online/2022 journal issue](https://link.springer.com/article/10.1007/s11142-021-09597-6) shows the relevance of sales and guidance surprises alongside EPS. It also warns that historical vendor feeds can include estimates added later that were not present in that vendor's contemporaneous feed. Numerical expectations require provenance and a known availability time.

[Tetlock, All the News That's Fit to Reprint, 2011](https://business.columbia.edu/faculty/research/all-news-thats-fit-reprint-do-investors-react-stale-information) studies repeated/stale news and market response. It supports investigating novelty and syndication. It does not justify automatically reversing every repeated headline.

[Martineau, Rest in Peace Post-Earnings Announcement Drift, author internet appendix, 2022](https://www.charlesmartineau.com/CFR_Internet_Appendix_v1.pdf), Table IA.1, gives recent-period counterevidence to earnings-surprise drift outside microcaps through 2019. The appendix was accessible; the full publisher article was not. Do not present classic PEAD as a proven contemporary liquid-stock strategy.

[Ng, Rusticus, and Verdi, Implications of Transaction Costs for the Post–Earnings Announcement Drift, 2008](https://onlinelibrary.wiley.com/doi/10.1111/j.1475-679X.2008.00290.x) connects larger drift with costly trading and finds implementation costs reduce profits. Moving toward illiquid names to recover attractive gross results creates a different execution problem.

### Historical LLM results can contain information from the future

[Glasserman and Lin, Assessing Look-Ahead Bias in Stock Return Predictions Generated By GPT Sentiment Analysis, 2023](https://arxiv.org/html/2309.17322v1) examines training overlap and company-knowledge distraction. A chronological price split does not establish independence from an LLM's training data. Entity masking is a diagnostic, not certification that leakage is absent.

[Araci, FinBERT, 2019](https://arxiv.org/abs/1908.10063) establishes a financial sentiment-classification baseline. Classification accuracy is a different outcome from calibrated return forecasts or profitable orders.

Use language models to extract verifiable event facts and uncertainty. Require numerical models or explicit experimental rules to connect those facts to post-arrival outcomes. Do not let model confidence stand in for a measured probability of profit.

### Public information has multiple availability times

[Alpaca's news stream documentation](https://docs.alpaca.markets/us/docs/streaming-real-time-news) exposes article identifiers, creation/update timestamps, symbols, content, and source. [Its historical news documentation](https://docs.alpaca.markets/us/docs/historical-news-data) describes Benzinga history dating back to 2015. Neither fact proves that our account can obtain every original article revision or its real-time delivery history.

[SEC's EDGAR API documentation](https://www.sec.gov/search-filings/edgar-application-programming-interfaces) provides submission and financial-fact APIs. [SEC's webmaster FAQ](https://www.sec.gov/about/webmaster-frequently-asked-questions) distinguishes filing acceptance from actual public availability and does not provide a definitive first-publication timestamp. Our recorder must retain when we first received each version. Follow the identification, caching, and request limits in [SEC Developer Resources](https://www.sec.gov/about/developer-resources).

[Benzinga's earnings endpoint](https://docs.benzinga.com/api-reference/calendar-api/get-earnings) includes actuals, estimates, and surprise fields. Access to a news feed does not imply access to this separate product or to historical consensus vintages. Probe existing entitlements and document missing fields.

Use [Federal Reserve meeting calendars](https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm) and [BLS release calendars](https://www.bls.gov/schedule/news_release/empsit.htm) for scheduled event context. [FRED versus ALFRED](https://fred.stlouisfed.org/docs/api/fred/fred_vs_alfred.html) distinguishes revised current data from historical vintages. [FRED release-date documentation](https://fred.stlouisfed.org/docs/api/fred/release_dates.html) warns that release dates do not necessarily equal availability on FRED/ALFRED. A day-level vintage is not an intraday delivery timestamp.

### Broker assumptions can change

[Alpaca's October 30, 2025 changelog](https://docs.alpaca.markets/us/v1.1/changelog/marketdata-bid-and-ask-size-display-change) says CTA/UTP quote sizes changed from round lots to shares starting November 3, 2025. The [stock stream schema](https://docs.alpaca.markets/us/docs/real-time-stock-pricing-data) still contains round-lot wording. Resolve the exact endpoint and historical-normalization behavior before converting sizes. Do not apply a blanket multiplication by 100.

[SEC's February 27, 2026 fee advisory](https://www.sec.gov/rules-regulations/fee-rate-advisories/2026-2) specifies zero Section 31 fees through charge dates April 3, 2026 and $20.60 per million from April 4. [FINRA's fee adjustment schedule](https://www.finra.org/rules-guidance/rule-filings/sr-finra-2024-019/fee-adjustment-schedule) distinguishes 2024–2025 TAF rates from 2026. A single current schedule applied across 2016–2026 is not historically observed fees.

[Alpaca's regulatory-fee support article](https://alpaca.markets/support/regulatory-fees) and [API fee documentation](https://docs.alpaca.markets/us/docs/regulatory-fees) describe rounding at different levels. Bind fee behavior to our account product and documented billing behavior. Preserve unresolved differences; do not choose the cheaper interpretation to improve results.

[Alpaca's paper documentation](https://docs.alpaca.markets/us/docs/paper-trading) describes simulated fills and omissions including market impact, latency slippage, queue position, regulatory fees, and dividends. It also says the simulator does not constrain orders to displayed NBBO size. Broker-paper P&L and a conservative economic simulation must therefore be reported separately.

Verify current asset and order eligibility against [fractional trading documentation](https://docs.alpaca.markets/us/docs/fractional-trading). Use the declared adjustment semantics in [historical bar documentation](https://docs.alpaca.markets/us/reference/stockbars) to keep raw executable prices and corporate-action-adjusted research data consistent.

### Statistical evidence has a defined scope

[Bailey et al., The Probability of Backtest Overfitting](https://www.davidhbailey.com/dhbpapers/backtest-prob.pdf) evaluates selection among competing configurations using in-sample winners and their out-of-sample ranks. PBO is not an individual trade-loss probability.

[Bailey and López de Prado, The Deflated Sharpe Ratio](https://www.davidhbailey.com/dhbpapers/deflated-sharpe.pdf) addresses multiple testing and non-normal returns. Effective independent-trial counts are estimates, not a license to erase unsuccessful trials. A high DSR is not a guarantee of future profitability.

External articles are untrusted inputs. Apply least privilege and independently enforced action checks consistent with [OWASP's LLM Prompt Injection Prevention guidance](https://cheatsheetseries.owasp.org/cheatsheets/LLM_Prompt_Injection_Prevention_Cheat_Sheet.html). Text instructions alone are not an adequate brokerage boundary.

## 5. Define success before changing code

Maintain three separate scorecards.

**Product reliability:** real events arrive, facts are extracted with evidence, every candidate receives a decision, eligible experimental orders reach the paper broker, positions reconcile, exits execute correctly, and the dashboard explains the system's current state.

**Economic performance:** broker-paper P&L; economic-paper P&L after modeled omitted costs; return on total virtual account equity; return relative to allocated capital; drawdown; turnover; exposure; and total service costs. All returns include open positions marked consistently, not only realized winners.

**Incremental decision value:** the event policy adds value relative to predeclared comparable baselines after controlling for exposure, implementation costs, and selection. Absolute profit is useful to the user, but it must not be described as alpha when simple market exposure explains it.

A release can pass the reliability scorecard while its performance is still unproven. A profitable period can exist without passing the alpha scorecard. Display those states directly.

Do not set a promised daily return, win rate, Sharpe, or dollar profit. Use an economic minimum worth detecting and a risk budget to plan evaluation; do not choose thresholds after observing the results.

## 6. First implementation pass: bounded independent reproduction

Before new strategy research, run a bounded audit of the existing system focused on the remaining concrete risks. Reuse established tests. Do not spend the entire milestone rewriting audited infrastructure or repeatedly searching for explanations that turn losses into wins.

Build a small independent reference ledger from raw market events and broker-account rules. It must not call the production P&L or fill functions that it is checking. Reconstruct a selected set of trades covering:

1. A daily-close decision.
2. A regular-session order arriving after the close.
3. A fractional order.
4. A fee-rate boundary.
5. The quote-size schema transition.
6. A split and a cash dividend.
7. A stale or missing quote.
8. A partial fill followed by cancellation or later completion.
9. An ambiguous broker submission response followed by recovery.
10. A simultaneous benchmark adjustment and strategy exit.

For each, retain the immutable raw responses, request parameters, source, timestamps, expected arithmetic, production arithmetic, and discrepancy. A report suspicion becomes a confirmed defect only when this reproduces it.

Run a causal-invariance test: changing any market, news, or corporate fact that becomes available after decision time must not change that earlier decision. Changing a future outcome may change the eventual P&L, never the prior signal.

When a genuine defect changes historical results, preserve the old result, mark it superseded for the specific reason, and append the corrected run. Recompute affected frozen specifications once without tuning. Do not call the corrected known dataset a fresh external test.

## 7. Execution and accounting questions that must be resolved

### Time and price basis

Represent bar interval start, interval end, exchange event time, provider time, local receipt time, and feature availability separately. Relabeling a daily bar to the close does not permit trading at that close using the completed bar.

Use the first eligible executable quote after the order could have arrived. A quote preceding a close may be a stale valuation mark if explicitly labeled; it cannot be a causal fill for an order decided later.

If a regular-session-only signal is produced after the close, either schedule it for the next permitted session under a frozen policy or let it expire. Do not simulate an unsupported after-hours fill to preserve a backtest's return.

Use raw prices and raw contemporaneous share quantities for orders. Handle splits, fractional shares, dividend entitlements, receivables, and payments in the ledger. Research features may use adjusted data only with documented causal semantics. Do not add dividend cash to a return series that already includes the same dividend adjustment.

### Fees

Create effective-dated fee rules with fee type, account product, side, asset class, rate basis, caps, rounding scope, effective date semantics, and source. Where a rule uses a charge date, do not silently substitute the signal date.

Support both historical fee schedules and a separately labeled current-business-cost counterfactual. Track unknown historical schedules as uncertainty rather than calling them observed.

Do not assume CAT fees existed at the same rate throughout the report's history. Resolve its actual applicable period and pass-through arrangement.

For small orders, quantify fee granularity. One extra cent equals 10 basis points on $10 and 4 basis points on $25. That arithmetic illustrates why rounding matters; it is not an assertion that every fee is rounded independently per order.

Reconcile charges per account and rule. Fees across thirty alternative backtests do not belong to one simulated trading account.

### Spread, latency, and impact

For a representative round trip, bridge decision-time midprice to arrival-time midprice, executable bid/ask, final simulated fill, and exit. Show favorable as well as adverse movement.

If the arrival quote already reflects waiting for execution, do not automatically charge that same move again as delay or slippage. Additional adverse execution must represent a distinct assumption. Mark residual uncertainty separately.

Quote observations, hypothetical fill rules, conservative slippage assumptions, and broker-paper fills are different evidence types. Use precise labels.

### Displayed size and fills

Investigate the reported 7,229 partial fills. Determine the actual requested quantities and definition of the count before deciding whether it is surprising.

Resolve quote size units by feed, endpoint, schema version, and date. Check whether historical retrieval normalizes older records to a newer schema. Preserve original payload values and the conversion provenance.

Do not count the same displayed liquidity repeatedly within one simulated account. Do not share depletion across mutually exclusive backtest configurations. A top-of-book snapshot is not complete market depth.

For passive limits, a bar touch is insufficient. For marketable limits, model the limit, arrival quote, remaining quantity, cancellation, and missed opportunities. If fill evidence is inadequate, mark the outcome unknown or apply the declared conservative policy.

### Portfolio economics

Explain the report's low exposure using target weights, caps, idle cash, warm-up time, rejected orders, sizing, and missed fills. Low exposure may be intentional; do not “fix” it by increasing size merely to produce larger dollar profit.

Keep cash and capital denominators consistent. A hypothetical passive benchmark must be investable under its declared rules. An exposure-matched attribution comparator may help explain returns, but must not be mislabeled as a separately implementable portfolio if it uses hindsight.

## 8. Build the smallest complete event-to-order architecture

Keep the current Python application, database, migrations, dashboard, workers, and Render setup where they are sound. Use existing libraries and conventions before adding dependencies. Do not introduce a new orchestration platform, vector database, microservice fleet, or multi-model debate unless a measured need justifies it.

Implement these logical components with clear typed contracts:

| Component | Responsibility | Must not own |
|---|---|---|
| Source adapters | Receive public articles, filings, calendars, quotes, trades, and bars | Trade decisions |
| Evidence store | Version raw inputs, receipt times, hashes, and source metadata | Retroactive overwrites |
| Event normalizer | Deduplicate, map issuers, identify material updates | Invented expectations |
| Fact extractor | Produce structured facts and evidence spans | Broker credentials or orders |
| Numeric feature builder | Calculate surprise, novelty, price reaction, costs, and liquidity | Future outcomes |
| Policy/forecast module | Generate a candidate or abstention under a frozen policy | Changing risk limits |
| Risk engine | Enforce account, sizing, exposure, timing, and loss constraints | LLM discretion |
| Order state machine | Submit, track, cancel, recover, and reconcile paper orders | Strategy rewriting |
| Evaluator | Produce independent performance and calibration reports | Silent promotion |

An LLM can help interpret complex documents inside this architecture. It is not the component authorized to submit arbitrary broker requests.

Complete one vertical slice before broadening coverage: one actual supported news source, one verified event category, one deterministic candidate rule, one paper instrument, one complete order lifecycle, and one dashboard explanation. Expand only after that slice is demonstrated.

## 9. Capability and data-quality inventory

At startup and through a diagnostic command, report:

- Broker account identifier in redacted form, account product, verified paper endpoint, buying power, and tradability restrictions.
- Entitlements for historical SIP, live SIP, live IEX, news history, news stream, earnings calendar, and point-in-time consensus.
- Supported symbols, fractional eligibility, order types, time-in-force, sessions, and precision.
- Source latency, pagination, rate limits, retention constraints, revision availability, and historical coverage.
- Configured inference provider, pinned model, timeouts, budget, and fallback behavior.

Probe existing credentials without printing secrets. Historical SIP access does not establish live SIP entitlement. An IEX quote is not an NBBO quote. The dashboard must show the feed actually used.

Use currently available sources first:

1. Existing Alpaca/Benzinga news stream.
2. Official issuer releases and public SEC filings as primary verification.
3. Existing entitled earnings/consensus feeds.
4. Fed/BLS calendars and official release documents for macro context.

Do not treat web-search snippets or a delayed public website as a low-latency trading feed. Public web search is useful for source discovery and later investigation; live decisions require recorded source arrival and reproducible content.

If a paid feature is unavailable, implement the adapter, capability state, and constrained fallback. Complete the available workflow. Do not subscribe or fabricate unavailable consensus, quotes, or historical revisions.

Maintain separate quality reports for articles, event clusters, issuer mappings, expectations, prices, and execution. Thousands of articles can represent a small number of independent events.

## 10. Immutable, point-in-time event records

Create a source-event record with at least:

~~~json
{
  "source_event_id": "provider-specific-id",
  "source": "configured-provider",
  "source_url": "verified-public-source-url",
  "source_version": "provider-version-or-content-hash",
  "published_at": null,
  "provider_created_at": null,
  "provider_updated_at": null,
  "first_received_at": "actual-local-UTC-timestamp",
  "content_available_at": "actual-local-UTC-timestamp",
  "content_sha256": "computed-hash",
  "revision_of": null,
  "event_cluster_id": "stable-cluster-id",
  "issuer_id": "verified-internal-issuer-id",
  "cik": null,
  "related_instruments": [],
  "event_type": "unclassified",
  "is_primary_source": false,
  "is_correction": false,
  "is_retraction": false,
  "rights_profile": "configured-source-policy"
}
~~~

Store raw content according to the source's permitted retention rules. If full text cannot be retained, preserve permitted metadata, extracted facts, hashes, and the limitation on replay.

Treat every material correction as a new version. Never attach a revised article body to its original creation time and pretend that body was available then.

For each derived feature, retain the input evidence IDs and its own available-at time. A decision may use only features available when the decision completes. Its eligible order time follows that completion plus the declared processing/submission latency and session rules.

Do not reconstruct an unrecorded historical receipt time by setting it equal to publication time. Label replay availability assumptions and run sensitivity analyses. Historical text with unknown original versions is suitable for parser development but does not establish clean historical alpha.

For macro inputs, preserve vintages and release revisions. For company inputs, preserve original filing/accession, fiscal period, amendments, currency, and accounting basis. For transcript inputs, use when the transcript or segment became available, not when the call began.

## 11. Entity resolution, novelty, and materiality

Resolve companies using stable issuer/security identifiers and a dated security master. Company mentions, parent companies, subsidiaries, suppliers, ETFs, and ticker symbols are not interchangeable.

The symbol list supplied by a news provider is a candidate list. Verify the economic relationship before treating every mentioned symbol as a trade candidate.

Cluster syndication and repeated coverage into one underlying event using deterministic IDs, normalized text hashes, URLs, timestamps, issuer identity, and bounded semantic similarity. A copied story should not add independent confirmation or trigger another entry.

A material update can create a new decision opportunity, but only when the event diff identifies new information: changed guidance, newly disclosed financial terms, a correction, a withdrawal, or a verified change in event status.

Distinguish:

- New facts.
- Old facts with a newly observed source.
- Analysis or opinion.
- Price-reaction recaps.
- Rumor and unconfirmed reports.
- Explicit correction or retraction.

Do not trade a retrospective “shares rose because…” story as though it preceded the price move.

Construct novelty against information already received by our system. Preserve uncertainty if the broader market may have known the fact earlier. A low historical similarity score is not proof of economic surprise.

## 12. Use the LLM for evidence-constrained understanding

The extractor receives an immutable evidence packet containing the article or filing version, relevant prior company statements, known expectations, and a clear evaluation time. Historical evaluation must not grant unrestricted retrieval into today's web.

Require schema-constrained output, such as:

~~~json
{
  "event_type": "earnings_and_guidance",
  "issuer_id": "verified-id",
  "fiscal_period": "source-supported-period",
  "facts": [
    {
      "metric": "revenue",
      "actual": null,
      "consensus": null,
      "prior_guidance_low": null,
      "prior_guidance_high": null,
      "new_guidance_low": null,
      "new_guidance_high": null,
      "currency": null,
      "unit_scale": null,
      "accounting_basis": null,
      "evidence_ids": [],
      "source_offsets": []
    }
  ],
  "directional_interpretation": "mixed",
  "material_new_facts": [],
  "contradictions": [],
  "missing_required_fields": [],
  "extraction_confidence": null,
  "reason_for_abstention": null
}
~~~

Null means unknown. Never replace unknown values with zero. Never invent consensus estimates, expected returns, probability of profit, or fair-value targets.

Parse numbers with deterministic validation. Check signs, units, thousands/millions/billions, currency, fiscal period, per-share basis, and GAAP versus adjusted results. An EPS beat cannot be computed by mixing adjusted consensus with GAAP actuals.

Let code calculate normalized surprises and changes in guidance. Handle negative and near-zero denominators explicitly. Compare like-for-like periods. Distinguish a change from management's prior guidance from a surprise relative to analyst consensus.

Return a short evidence-linked rationale and unresolved contradictions. Do not request private chain-of-thought or use a long internal reasoning transcript as an audit artifact.

A second model call may review only ambiguous, high-materiality extraction failures. Measure whether it improves extraction enough to justify cost and latency. Do not create a default debate among several agents.

Pin model identifiers, prompt versions, schema versions, inference settings, input hashes, and output hashes. Cache by content and model configuration. Model or prompt changes create new evaluation cohorts; they do not overwrite earlier decisions.

Treat external text as data. It cannot alter system instructions, run shell commands, choose broker endpoints, modify risk limits, or request secrets. The extractor has no broker credentials and no arbitrary network/tool execution.

## 13. Build and evaluate an extraction benchmark

Create a compact, manually inspectable gold set covering at least:

- Positive EPS but negative revenue/guidance.
- Revenue growth that is below expectations.
- Better GAAP earnings but worse comparable adjusted earnings.
- A guidance range widening with an unchanged midpoint.
- A large contract announcement with no disclosed economics.
- A filing amendment correcting an earlier figure.
- Syndicated and near-duplicate stories.
- Wrong-ticker and parent/subsidiary ambiguity.
- Recycled headlines and price-reaction recaps.
- Source text containing instructions aimed at an AI agent.

Prefer legitimate permitted examples from available data. Label synthetic fixtures as synthetic and exclude them from all performance statistics.

Measure field-level precision, numeric accuracy, unit/basis correctness, issuer mapping, event type, contradiction detection, abstention, and unsupported-claim rate. Inspect material errors individually. A high aggregate score cannot excuse a wrong issuer or tenfold unit error.

Use deterministic checks for order-critical numeric fields. If an essential value cannot be reconciled, the trade path must abstain even if the language model sounds confident.

Do not use future P&L to decide the “correct” semantic label in this gold set. Extraction correctness and strategy usefulness are different evaluations.

## 14. Forecast what remains after arrival

For each event, separate three questions:

1. What changed in the underlying facts?
2. How did that differ from expectations that were actually available?
3. What return, if any, remains after our full information and execution delay?

Record price movement before the release, between release and our receipt, during extraction, and after executable arrival. Do not credit the strategy with a move that happened before it could act.

Build features from observed information:

- Actual-versus-consensus numerical surprise, when contemporaneous consensus is available.
- Change from previously published management guidance.
- Revenue, margin, cash-flow, and dilution context.
- Source reliability, event type, materiality, novelty, and correction status.
- Time since receipt and time since publication, with uncertainty flags.
- Gap and market/sector-relative movement already completed.
- Spread, valid quote size, recent liquidity, and current trading status.
- Pre-event volatility, event-time volatility, and market regime.
- Event congestion and scheduled macro risk.
- Contradictory facts or disagreement between numeric and textual evidence.

Context is a feature set, not permission to add endless filters. Freeze features before evaluation and retain every feature-selection attempt.

When training a forecast, define the exact target: return from a realistically available entry to a declared horizon or exit, and whether it is gross or net. Include missing fills, opportunity cost, and open-position treatment.

Start with simple baselines: event-conditioned historical averages with uncertainty, regularized linear/logistic models where appropriate, and one combined structured-event model. Add more complex models only when a bounded comparison shows incremental value.

Use empirical calibration, not an LLM's self-reported confidence. When the target is gross expected return, the entry rule must subtract expected total cost and an uncertainty margin once. When the target is already net, do not subtract the same costs again.

A validated forecast should support a decision of the form:

~~~text
expected_remaining_gross_return_bps
    - estimated_round_trip_cost_bps
    - declared_uncertainty_margin_bps
    > required_economic_edge_bps
~~~

For an explicitly preregistered experimental rule that has no reliable forecast yet, record expected net return as unknown and use the experimental-paper policy. Do not invent a positive forecast to satisfy this formula. Known failed economic rules remain disabled unless a materially new, documented hypothesis or corrected evidence justifies a new experiment.

## 15. Research only two entry hypotheses and one risk overlay initially

Do not build a generic “trade any good news” system. Build a small event taxonomy and make these first hypotheses concrete. They are proposed experiments, not proven strategies.

### H1: Confirmed change in earnings outlook

Research long entries following verified positive changes in a company's forward outlook, conditional on consistent numerical evidence and a price response that has not already exhausted the proposed opportunity.

Use official releases/filings to identify actual EPS/revenue, prior and revised guidance, fiscal period, and material negative qualifications. If contemporaneous consensus is unavailable, call the feature “guidance revision,” not “consensus surprise.”

Preregister one initial event rule, one entry policy, and one exit policy before outcome testing. Suggested starting policy for the first engineering implementation:

- Eligible liquid, fractionable common stock under the universe policy.
- Confirmed primary-source quantitative guidance revision upward on a comparable basis.
- No material conflicting guidance cut or unreconciled numeric field.
- Evidence already received and extraction completed.
- Regular-session entry after at least one completed five-minute observation following availability; premarket/overnight events wait for a declared post-open observation.
- No entry if the current spread or permitted execution price exceeds its frozen threshold.
- A declared maximum post-event move relative to pre-event volatility, used as an anti-chasing constraint.
- One entry per issuer/event cluster.
- Time-based exit after the chosen horizon, plus independently enforced risk exits and confirmed thesis invalidation.

The precise thresholds and horizon must be frozen in a manifest before testing. Use a small declared choice set; do not search until a winning combination appears. If the session policy or event frequency makes the rule impossible to trigger, report a feasibility failure rather than a failed alpha estimate.

Because published evidence on after-open earnings drift is mixed or negative, this hypothesis must earn continuation through our own causal and forward results.

### H2: Verified material corporate update

Research a distinct hypothesis for a narrowly defined operational update with quantifiable economics, such as an explicitly valued contract or a material operational outlook revision.

Start with only one subcategory selected by reliable source coverage and a stated mechanism, not by whichever historical subtype earned the most.

Require:

- Primary public evidence and a verified issuer.
- Incremental economic terms rather than promotional language.
- Comparison with company scale using a point-in-time reference, where meaningful.
- No double-counting of an earlier announcement.
- A defensible statement of why the event might affect cash flows or risk.
- A specified remaining-return horizon and a declared entry-after-receipt delay.

If a contract is described as “major” but value, duration, or economic relevance cannot be established, abstain. Do not equate total contract value with near-term revenue or profit.

Exclude merger-arbitrage, unconfirmed takeover rumors, clinical binary outcomes, bankruptcy, penny-stock promotion, and other event types whose execution and downside models are outside the initial product.

### R1: News and macro risk overlay

Use verified negative developments, contradictions, major scheduled macro releases, retractions, halts, and severe feed deterioration to reduce new exposure or trigger a declared position review.

Do not hard-code universal interpretations such as “rate cuts are bullish” or “inflation down means buy.” A macro release has actuals, expectations, revisions, positioning, and already-observed price response.

Evaluate this overlay against the identical base strategy without it. Quantify both losses avoided and profitable trades missed. An overlay that merely keeps the account in cash should not be presented as successful prediction.

Initially use official macro calendars for risk context. Directional macro trading requires a separate later hypothesis with point-in-time expectations; it is not part of this initial experiment budget.

### Shared hypothesis budget

Allow at most three preregistered variants for each entry hypothesis in the first research cycle. The initial base-versus-overlay comparison and all ablations count as research choices. Maintain a global ledger and family-level accounting.

If neither hypothesis has sufficient event quality, fix acquisition/extraction or report the limitation. Do not expand to twenty unrelated strategy families.

## 16. Universe, position sizing, and virtual-capital policy

Start with the user's existing small-order intent: at most one open position and no more than $25 notional per experimental entry, subject to the current stricter cap if one exists. Do not increase size to make a chart look better.

Use an explicitly identified paper account or isolated virtual allocation. Anchor its starting equity and preserve its complete history. Never reset a losing account, hide deposits, or merge multiple alternative strategies into a fictional aggregate balance.

Default universe:

- US exchange-listed common stocks with supported paper/fractional trading.
- Price at least $5.
- A declared trailing liquidity floor, initially $50 million median daily dollar volume over completed sessions if compatible with available data.
- Valid current bid/ask and permitted quote freshness.
- No known trading halt, unsupported security type, or unresolved issuer mapping.
- ETFs may serve as benchmarks or a separately declared policy, not substitutes for a company's direct economic exposure without validation.

These are initial engineering/research defaults, not optimized market truths. Record their effect on event availability and liquidity.

A present-day stock universe cannot automatically be backtested as though all its members were eligible in the past. Use point-in-time eligibility or clearly label a fixed-universe study's selection limitations. Apply inception, delisting, symbol changes, and corporate actions explicitly.

For experimental paper mode, use the stricter of existing project limits and the following initial ceilings:

~~~yaml
mode: experimental_paper
live_execution_enabled: false
max_open_positions: 1
max_entry_notional_usd: 25
max_gross_exposure_fraction: 0.10
max_new_entries_per_session: 3
max_entries_per_event_cluster: 1
daily_economic_loss_stop_fraction: 0.005
experiment_drawdown_stop_fraction: 0.03
allow_short_sales: false
allow_leverage: false
allow_options: false
allow_crypto: false
allow_extended_hours_entries: false
allow_overnight_positions: false
~~~

Bind fractions to the documented virtual-equity anchor and explain the dollar ceilings. An entry below the broker's supported minimum must be skipped, not rounded upward through a cap.

The default initial vertical slice is intraday. An H1/H2 multi-session holding experiment must explicitly set and version overnight permission, maximum holding duration, gap-risk budget, and exit handling before running. Do not globally flatten every swing strategy simply because the old engine was intraday.

Risk exits are not guaranteed prices. Simulate gaps, missing quotes, and stop-limit nonexecution. A circuit breaker blocks new risk; it must not disable reconciliation or the ability to reduce existing risk.

## 17. Separate execution modes and qualification states

Implement the following policy table with visible mode names and separate records:

| Mode | Broker orders | Required evidence | Performance label |
|---|---|---|---|
| Offline replay | None | Valid fixture or historical replay contract | Historical or synthetic, as applicable |
| Shadow | None | Healthy acquisition and causal decisions | Hypothetical, unqualified |
| Experimental paper | Bounded paper orders | Operational certificate, frozen experiment, source quality, risk limits | Experimental, edge unproven |
| Frozen forward paper | Paper orders under a fixed candidate protocol | Locked policy/model, declared evaluation cohort, operational certificate | Evaluation in progress |
| Qualified paper | Paper only | Completed statistical/economic protocol with unchanged gates | Qualified for this measured scope |
| Live | Unavailable in v20 | Outside this assignment | Not implemented/enabled |

The operational certificate must verify:

- Paper-only account and allowed broker host.
- No live credentials on the order worker.
- Valid source schema and decision timestamps.
- Tested order lifecycle, idempotency, reconciliation, and risk exits.
- Declared strategy/event eligibility and experiment budget.
- Current healthy inputs for the proposed action.
- Appropriate asset/order/session capabilities.
- Authenticated configuration and audit trail.

It must not silently stand in for evidence of profitability.

Experimental mode may start before DSR is 0.95 because its purpose is to collect honest fake-money evidence. The existing failed strategies remain failed. The new experimental status is not a qualification pass.

For the later qualified-paper label, retain a predeclared performance protocol including positive economic net performance, the chosen incremental-value objective, dependence-aware uncertainty, required stress tests, and the applicable DSR/PBO gates. Use DSR at least 0.95 and family PBO no more than 0.20 when their defined inputs make the statistics applicable. If a statistic is undefined or underpowered, report that state; never substitute a favorable zero, one, or default.

The historical thresholds are project choices. Their presence does not prove a strategy is sound, and their failure does not mean a safe paper experiment is technically prohibited.

## 18. Implement a real order state machine

Use a single authoritative order ledger and an idempotency key derived from experiment, policy version, event cluster, instrument, side, and intent sequence. Duplicate news, worker retries, and restarts must not create duplicate exposure.

At minimum represent:

- Proposed intent.
- Risk-approved or rejected.
- Submission pending.
- Submission outcome unknown.
- Accepted/new.
- Partially filled.
- Filled.
- Cancel pending.
- Canceled.
- Rejected.
- Expired.
- Reconciled.

Persist the intent before making the broker call. On timeout, reconcile by client order ID and broker order state before resubmitting. A timeout is not proof that the broker did not receive the order.

Use transactional database updates or a durable outbox pattern consistent with the existing stack. Avoid claiming universal exactly-once network delivery; implement idempotent intent processing and recovery.

Represent quantities and monetary values with suitable precision. Never mix dollar notional and share quantity or send both when the broker rejects that combination.

Make risk checks both before initial submission and before a replacement that could increase exposure. Position, open-order, and available-cash checks must be consistent within a transaction or appropriately serialized.

Verify broker support for each fractional exit/stop/bracket combination. If unsupported, use a declared independently monitored synthetic exit only when its failure behavior is understood. Do not submit contradictory exits that can oversell a position.

On a feed outage, cancel unsafe pending entries, retain broker-state reconciliation, and follow the position's declared risk-reduction policy. Do not send new decisions using stale last prices.

At startup and deployment, recover open intents, orders, positions, and locks. Reconcile with the broker before accepting new entry work. A stale lease must not let two workers own the same trading allocation.

## 19. Maintain three performance ledgers

### Broker-paper ledger

Record actual paper-broker events and account changes exactly as returned, including raw timestamps, partial fills, rejects, and broker identifiers. Do not rewrite these fills to match expectations.

### Economic-paper ledger

Use the broker-paper trade history as the factual order stream, then transparently account for simulation omissions and an independently specified execution scenario. Record every adjustment with its reason and provenance.

If a separate quote-based simulator produces a different hypothetical order/fill path, treat that as a distinct scenario, not an invisible correction to the broker record.

Include estimated applicable costs, delayed execution assumptions, dividends/corporate actions where relevant, and uncertainty. Paper records cannot determine actual queue priority, hidden liquidity, or the market impact of a live order.

### Baseline ledger

Maintain investable baselines using the same initial virtual capital, compatible trading access, declared fees, and a reproducible rebalance schedule. Keep statistical attribution controls separately labeled.

At least include:

- Cash, with no imaginary interest unless the account treatment supports it.
- A relevant passive market or sector comparison.
- A structured-event-only rule.
- The corresponding rule with text features.
- The full rule with the risk overlay.
- Matched event controls and randomized event-association controls used only for diagnosis.

Do not add all alternative backtests' profits or capital together. Alternative configurations are comparisons, not simultaneous independent accounts.

Report gross P&L, economic net P&L, brokerage-paper P&L, and infrastructure/inference costs separately. A ten-cent trading gain can coexist with a negative product economics result if services cost more.

## 20. Historical replay and forward data must follow the same causal rules

Use one feature and policy implementation where practical, with adapters for replay and live receipt. The replay engine must support the same order timing, risk limits, and state transitions.

For historical replay:

- Restrict each event to the content version known at the declared availability time.
- Prevent joins to later revisions, future consensus, completed future bars, or later issuer mappings.
- Distinguish provider-created time from historically reconstructed local receipt assumptions.
- Simulate processing latency and the next eligible session.
- Record data gaps and how they affect candidate selection.
- Handle overnight label overlap and event clustering.
- Version the raw inputs, extraction outputs, features, parameters, and code.

Evaluate news at several predefined additional delays, such as one minute, five minutes, fifteen minutes, and the next eligible open. These are sensitivity scenarios, not choices from which to select a winning latency after the fact.

Measure the event-time return path from before publication through receipt, decision, entry, and exit. The strategy earns only post-fill returns.

Test time aggregation for feasibility before expensive matrix runs. A twenty-bar lookback that resets after a session containing fewer than twenty bars is structurally unable to trigger. Label such cells “inapplicable/no feasible signals,” not “evidence of negative edge.”

For prospective cohorts, append predictions and decisions before outcomes are available. Retain first-received content, failed extractions, abstentions, rejected signals, and missed fills. Recording only executed winners makes the dataset unusable.

## 21. Statistical validation without metric theater

Use synchronous marked portfolio returns with a consistent clock and capital denominator. Preserve zero-exposure days. Use active returns for incremental-value questions and total economic returns for absolute-return questions; label each metric's input.

Use chronological walk-forward evaluation with appropriate warm-up. Purge overlapping event/holding labels and apply a declared embargo where necessary. Cluster uncertainty by date and event; related articles or multiple stocks reacting to the same macro event are not independent observations.

Use a dependence-aware bootstrap and report the block choice. Compare with a reasonable alternative block length as a sensitivity check; do not select the most flattering interval.

Treat PBO as a diagnostic of a configuration-selection process. Verify ranking, tie handling, equal temporal partitions, and performance-function consistency. Keep the sample and strategy family attached to every PBO value. A development PBO copied into an external-era table must remain labeled as development PBO.

Use the established DSR implementation with correctly scaled inputs and a visible trial ledger. Report the sensitivity of conclusions to raw versus defensibly estimated effective trial counts. Count model prompts, feature choices, strategy variants, and selection rules that could influence reported winners. Do not pool incompatible return streams merely to produce a number.

Compare observed behavior with an independent reference implementation and meaningful synthetic controls. A test designed only to repeat the production formula is insufficient.

Predeclare the primary outcome, economically meaningful effect, acceptable drawdown, evaluation date/window, and continuation/stopping rules. Do not run significance tests after every trade and stop on the first favorable result without a valid sequential procedure.

No universal sample count guarantees edge. Retain at least 60 actual trading sessions and 60 reconciled round trips as the project's initial forward-observation floor where applicable, then assess effective sample size and uncertainty. Sparse or multi-day strategies may require substantially longer. Never generate unnecessary trades to satisfy a count.

Positive returns in every year are not a universal property of useful strategies. Concentration, worst periods, and removal of the best year are important diagnostics; interpret them under the predeclared objective rather than repeatedly inventing new pass/fail conditions.

## 22. Prove that news adds incremental value

Use the same candidate-event universe and comparable risk budget to evaluate:

1. Price/context only.
2. Structured numerical event features only.
3. Text features only.
4. Structured facts plus text.
5. The selected combination with and without the risk overlay.

Preregister this ablation set and count it in the research ledger. Do not present all comparisons as independent confirmatory tests.

Use shuffled issuer-event associations, stale-versus-fresh versions, and delayed signals as negative controls where they preserve the relevant sample structure. These controls are diagnostics, not trade recommendations.

Ask:

- Does the full system outperform a simple structured rule?
- Does the benefit survive realistic latency?
- Is it still present after event-specific costs?
- Does it persist on new events after the model is frozen?
- Does it come from one issuer, one unusual event, or a short market rally?
- Does the model improve decision quality after its own inference cost?
- Does the news overlay help returns/drawdown or merely reduce exposure?

If text adds no measurable value, retain it for explanations or risk triage and keep the simpler trading rule. If the simpler trading rule also fails, stop its experimental allocation according to protocol. A useful reader and a useful forecaster are different products.

## 23. Forward operation and learning discipline

After the operational certificate passes, run the configured experimental paper mode during eligible market sessions. The actual first live-source decision should be recorded whether it trades or abstains.

Establish a cohort manifest before its first outcome:

- Start date and market session.
- Frozen policy, model, extractor, features, universe rule, and exit logic.
- Code commit and configuration hash.
- Virtual capital and risk ceilings.
- Sources and known delays.
- Baselines.
- Primary metrics and review schedule.
- Kill conditions.
- Conditions for continuing, freezing, or stopping the experiment.

Start the operational evidence clock when the cohort actually receives usable live data and makes prospective decisions. A service heartbeat or a weekend deployment does not count as a trading day of evaluated activity.

A separate fixed-candidate forward qualification cohort may follow exploratory work. Exploration results can inform candidate selection but are not untouched confirmation. If the selected candidate changes, open a new cohort and keep the prior one visible.

Allow automated incident responses and declared risk reductions. Do not let the running trader rewrite its strategy, model prompt, thresholds, or risk budget in response to short-term losses.

Use scheduled research reviews for learning. Treat agent-generated “lessons” as proposed hypotheses with supporting evidence, not new instructions that immediately alter the live paper policy.

No overnight loop can create sixty genuine trading days. Complete the software and prospective collection path now, then report elapsed evidence honestly.

## 24. Dashboard and product behavior

Build on the existing dashboard. Make the main screen useful to the owner without requiring knowledge of statistics internals.

Show:

- Current mode: shadow, experimental paper, frozen forward paper, qualified paper, or paused.
- Paper-only status and redacted account/allocation.
- Market phase and next eligible session.
- Live source health, last useful event time, and active feed.
- Current position, pending orders, virtual exposure, and loss budget.
- Broker-paper P&L and economic-paper P&L, clearly separated.
- Candidate count, abstention count, and leading no-trade reasons.
- Evidence age and whether a strategy's performance is qualified or unproven.

Provide an event detail view showing:

- What happened and when the system received it.
- Links to the supporting source versions.
- Actuals, expectations, and guidance comparisons.
- Conflicting or missing information.
- Price movement already completed before the decision.
- Why the event was considered economically relevant.
- The exact trade rule or abstention reason.
- Expected return estimate if supported, otherwise “not established.”
- Planned exit, invalidation conditions, and risk allocation.
- Full decision-to-order-to-fill timeline.

Provide an experiment view showing the frozen protocol, cohort dates, number of independent event clusters, model/configuration versions, baselines, uncertainty, drawdown, and source limitations.

The owner should be able to answer “Why is it not trading?” without asking the coding agent. Distinguish market closed, no eligible event, stale feed, missing consensus, extraction failure, high costs, risk limit, and operational pause.

Do not use marketing labels such as “95% sure profit” based on extraction confidence or DSR. Do not hide losing positions behind realized-only metrics.

## 25. Notifications and operating cadence

Reuse the existing notifier and authorized recipient configuration. Do not add recipients or send external messages beyond existing authorization.

Generate concise operational summaries:

- New experimental cohort activated.
- Material trade decision and its supporting event.
- Entry/exit and reconciliation outcome.
- Daily economic P&L and costs.
- Source outage, duplicate-order prevention, drawdown stop, or unresolved position.
- End-of-cohort evaluation with an explicit pass, fail, or inconclusive status.

Batch routine information. Avoid one alert per syndicated article, repeated heartbeat, or price tick.

Use exchange calendars, including holidays and shortened sessions. Keep recording off-hours public events if the source is available, but apply the declared entry-session policy. Validate scheduled event times against source updates.

## 26. Runtime, deployment, and failure recovery

Keep the existing Render services and PostgreSQL architecture unless a concrete issue requires change. Use a single active order owner per paper allocation, renewable leases, and fencing or equivalent protection against stale owners.

Separate acquisition health, extraction health, policy health, broker health, and notification health. A healthy web server does not imply current market data or a functioning order worker.

Implement bounded retries, jitter, rate-limit handling, and a dead-letter path. Preserve malformed inputs for permitted diagnostics without allowing them to execute.

On deployment:

1. Verify schema compatibility and migrate safely.
2. Drain or reconcile pending order intents.
3. Ensure a single worker owns the allocation.
4. Restore open-position/exit supervision before permitting new entries.
5. Confirm the actual code/configuration version through a health endpoint.
6. Exercise the read-only and paper-only capability checks.

Use existing authorization for commits, pushes, and paper-only deployment. If an action lacks authorization or is blocked, prepare the exact changes and verification first, then state that specific blocker. Do not invent credentials, create new paid infrastructure, or work around account restrictions.

Do not publish “v20 operational” until the deployed service, if deployment is authorized, runs the intended code and the end-to-end paper path has been demonstrated under valid conditions.

## 27. Cost, latency, and practical viability

Measure the time and cost of ingestion, retrieval, extraction, validation, forecasting, submission, and reconciliation. Report latency percentiles and full event age, not only API request duration.

Avoid an LLM call for every tick, unchanged article, or empty polling result. Use deterministic filtering, deduplication, caching, and model escalation only when justified.

Use the model provider already configured in the repository. Verify its current structured-output capabilities, model availability, and prices before choosing a model or estimating its expense. Do not assume model names, context limits, or prices from this prompt.

Define a budget for each existing paid service and a cap on inference calls. No unapproved new spending. On budget exhaustion, use a declared deterministic fallback or abstain; never silently send incomplete evidence to produce a cheaper but misleading decision.

Report:

- Variable cost per received event, evaluated event, and completed trade.
- Fixed hosting/data cost per month.
- Trading net P&L before fixed service costs.
- Net product economics after allocated inference, data, and hosting costs.
- Hypothetical sizing sensitivity without increasing the running allocation.

More capital can change fee granularity and impact, but cannot be assumed to create alpha. Any larger paper-sizing experiment requires a new declared risk/capacity configuration and must not overwrite the small-size result.

## 28. Meaningful acceptance tests

Use focused tests that resolve real financial or operational risks. Reuse existing coverage instead of adding duplicates solely to raise test counts.

Required invariants and scenarios:

| Scenario | Required behavior |
|---|---|
| Future article revision introduced | Earlier decision unchanged |
| New article repeats the same event | No duplicate entry |
| Mixed GAAP/adjusted EPS | Comparison rejected or explicitly reconciled |
| Million/billion parsing error | Numeric validation blocks eligibility |
| Wrong issuer/ticker | No order |
| Positive headline with guidance cut | Contradiction retained; no unqualified bullish inference |
| News received after its useful window | Expire/abstain |
| Complete close needed for signal | No earlier or same-close look-ahead fill |
| SIP size schema differs by provider/version | Documented normalization, no blanket conversion |
| Fee schedule boundary | Correct applicable rule and rounding scope |
| Split/dividend | Quantity, cash, and total return reconcile |
| Passive limit touched in OHLC only | No assumed guaranteed fill |
| Submission timeout | Query/reconcile before retry |
| Duplicate worker receives same intent | At most one broker exposure intent |
| Partial fill then exit | Exit only actual owned quantity |
| Feed goes stale with a position | New entries blocked; risk supervision continues |
| Broker position differs from local ledger | Pause new entries and reconcile |
| Daily loss limit triggered | New exposure blocked under declared policy |
| Live broker host or live credential appears | Startup/submission rejected |
| No valid events today | Clear no-trade state, no fabricated signal |
| LLM prompt injection in article | Cannot change configuration, credentials, or order permissions |
| Model/prompt updated mid-cohort | New cohort required |
| Account reset/deposit | Performance history preserved and cash flows handled |

Add independent reference checks for daily return, cost attribution, DSR inputs, and PBO ranking. Preserve the existing statistical regression tests unless the reference method proves them wrong.

Demonstrate an end-to-end deterministic replay using labeled fixtures and an end-to-end live-source shadow path. After operational qualification and within authorized paper scope, demonstrate the broker-paper order lifecycle using an eligible strategy event or a separately labeled calibration test.

A calibration order tests mechanics. It is not alpha evidence and must be excluded from strategy performance. If the market is closed, show replay results and scheduled readiness; do not claim a broker fill that did not occur.

## 29. Example decision cases

These are fictional fixtures to clarify behavior. They are not current securities recommendations or profitable-strategy examples.

**Case A: “Earnings beat” with weaker outlook.** A release reports comparable EPS of 1.30 against a recorded 1.20 estimate, revenue below its recorded estimate, and reduced full-year revenue guidance. The extractor records each fact and the accounting basis. The system does not convert the positive EPS headline into an automatic buy. It records a mixed event and follows the frozen rule, normally abstaining from H1.

**Case B: Authentic upward guidance revision received too late.** A verified release raises the comparable guidance midpoint. By the time our system finishes processing, the price move exceeds the policy's declared chase threshold. The decision is abstain with a timing reason. The recorder still tracks the later outcome for honest coverage and latency analysis.

**Case C: Fresh, internally consistent outlook change.** A verified issuer raises comparable forward guidance, required facts reconcile, the source and quote are fresh, the instrument is eligible, and the frozen experimental entry condition occurs within its allowed window. The risk engine may permit the small paper entry. The decision remains labeled experimental until forward evidence qualifies it.

**Case D: Twenty copies of one contract announcement.** They create one event cluster. A vague “major contract” with no usable economics does not produce twenty confidence votes or repeated buys.

**Case E: Broker timeout after acceptance.** The network response is lost. The system finds the existing order by its client ID and reconciles it. It does not submit another entry.

**Case F: Paper profit with negative economic profit.** The broker shows a small gain, but the independently modeled omitted costs exceed it. Display both figures and classify economic performance accordingly.

**Case G: No usable consensus feed.** H1 may evaluate a separately declared management-guidance revision rule using the previous public guidance. It must not label that comparison as an analyst-consensus surprise or invent the missing estimate.

## 30. Work sequence and concrete deliverables

Complete the work in these dependency stages. Continue autonomously through available reversible work; report real blockers without abandoning unrelated work.

### Stage A — establish the truth

- Inspect repository and deployment state.
- Capture the baseline.
- Trace the bounded reference trades.
- Resolve or explicitly bound fee, quote-unit, adjustment, timing, and execution assumptions.
- Patch confirmed defects with meaningful regressions.
- Publish a concise evidence table: confirmed defect, corrected assumption, unresolved uncertainty, or verified component.

### Stage B — deliver the first functioning paper slice

- Implement mode separation and the operational certificate.
- Ingest one real source into immutable event records.
- Implement issuer mapping, deduplication, and one evidence-constrained extractor.
- Add the first deterministic event rule and complete risk/order/reconciliation/exit path.
- Show an event's decision timeline in the dashboard.
- Demonstrate replay, then live-source shadow, then authorized experimental paper mechanics.

### Stage C — complete the initial event research product

- Add the second bounded event hypothesis and the risk overlay.
- Build the extraction gold set.
- Add numerical/context features and simple forecast baselines.
- Implement the three ledgers, benchmark comparisons, and latency/cost attribution.
- Record all experiments and source limitations.

### Stage D — freeze and run prospective evaluation

- Freeze policy/model/configuration manifests.
- Start the first eligible prospective cohort.
- Record every candidate, abstention, fill, failure, and outcome.
- Verify operational reporting and automatic risk responses.
- Leave the evaluation running under its declared schedule without strategy self-modification.

### Stage E — hand over a reviewable release

- Run relevant unit/integration/regression tests, type checks, linting, build, and migration verification.
- Review the complete diff for accidental permissions, secrets, duplicate infrastructure, and research-result overwrites.
- Commit logical changes under existing repository rules.
- Deploy to the existing paper-only service if authorized and verify the actual version.
- Produce one concise operations guide and one evidence/report index rather than dozens of overlapping documents.

Adapt CLI names to existing conventions. Provide equivalent commands for:

~~~text
tradeagent doctor
tradeagent audit-execution
tradeagent source-capabilities
tradeagent news-record
tradeagent event-replay
tradeagent evaluate-extraction
tradeagent paper-preflight
tradeagent run --mode shadow
tradeagent run --mode experimental-paper
tradeagent reconcile
tradeagent experiment-freeze
tradeagent experiment-report
tradeagent risk-pause
~~~

Do not add duplicate commands where existing ones can be extended cleanly. Each command must have a useful failure state and a reproducible configuration.

Keep a durable implementation checkpoint containing completed work, exact commands, remaining tasks, findings, configuration versions, and blockers so the project survives context changes. This checkpoint is not evidence that unexecuted work is complete.

## 31. Definition of done

TradeAgent v20 is ready for experimental paper use only when:

- The remaining concrete execution/data risks have been reproduced and resolved or bounded with explicit conservative behavior.
- Current public events flow through immutable evidence and validated issuer mapping.
- At least one frozen event rule can generate both eligible candidates and justified abstentions.
- The deterministic risk layer independently controls every possible paper order.
- The order lifecycle and restart recovery have been demonstrated.
- The dashboard explains current behavior, event evidence, economic results, and qualification status.
- Broker-paper and economic-paper ledgers reconcile under their distinct definitions.
- The prospective experiment is configured and, when the market/source permits, is actually collecting forward evidence.
- No live-money execution path is enabled.

A separate statement that the system “has demonstrated profitable paper performance” requires positive measured economic-paper results for a declared completed window. A stronger statement that it “has demonstrated incremental edge” requires the preregistered comparative and statistical evidence. Neither follows from software completion.

If forward results are not available yet, say exactly that. If a hypothesis fails, keep the failure visible and stop its allocation according to the protocol. Do not claim the entire software project is worthless or restart an unlimited indicator search.

## 32. Required final implementation report

Lead with what actually runs and whether any strategy edge is established.

Return:

1. Current mode and verified code/configuration version.
2. Confirmed defects fixed, with numerical before/after examples.
3. Unresolved assumptions and their operating consequences.
4. Sources connected, actual entitlements, and measured delays.
5. The event policies implemented and their exact frozen manifests.
6. Extraction evaluation, including material failures and abstention behavior.
7. A real or clearly labeled replay example from event receipt through final decision.
8. Paper orders actually submitted, calibration orders separately identified, and reconciliation outcomes.
9. Broker-paper P&L, economic-paper P&L, baseline results, service costs, and confidence limitations.
10. Cohort start, elapsed trading sessions, independent events, and completed round trips.
11. Whether the product is operational, experimental, statistically inconclusive, qualified, or paused.
12. Files changed, meaningful tests run, commits, and deployment verification.
13. The exact remaining blocker or next scheduled evaluation, without pretending future evidence already exists.

Keep this final report concise and link to detailed artifacts. Do not replace implementation with a long explanation of what could be built.

## 33. Final working instruction

Start by inspecting the existing code and the v0.10 evidence. Form a short implementation plan, then execute it.

Prioritize a correct complete event-to-paper-order loop. Make every decision causally reproducible. Let current news contribute verified facts, let empirical evidence determine whether those facts predict remaining returns, and let deterministic code control risk and execution.

Deliver a working paper-trading product, preserve honest forward evidence, and document any external blocker precisely.
