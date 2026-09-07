# Frozen Tuesday paper protocol

Protocol: `tuesday-paper-v1`; cohort: `v20-tuesday-20260908`.
The deployed manifest, configuration fingerprint, module hashes and release identity
are recorded in `research/results/v20-tuesday-20260908-deployment.json`.
This is a separately versioned, hypothesis-driven IEX paper experiment, not a formal
forward qualification sample. Formal evaluation has **not** started.

## Scope and clock

- Planned observation session: September 8, 2026, 09:30–16:00 America/New_York
  (13:30–20:00 UTC), independently checked against the Alpaca paper calendar.
- News collection begins at the preceding regular close: September 4, 16:00 ET.
  Earlier immutable documents are comparison context, not fresh news.
- Paper endpoint only; AAPL/MSFT/NVDA, common equities, long-only, no leverage,
  no extended-hours entries. $10,000 virtual allocation, at most $25 per entry
  (stricter existing limits win), one occupied/reserved slot.
- At most two durable entry reservations and one news entry for the account/session.
  Submitted rejections count; candidate checks do not. Reserved but never submitted
  intents remain consumed conservatively and are distinguished in reporting.
- No entry before 09:35; actual completed five-minute observations, at least 20
  completed daily liquidity observations, and two-second processing latency are required.
  Equipment eligibility ends at 10:00. The 60-minute news horizon must fit before
  the 15:50 flatten, so news eligibility ends at 14:50 (earlier than 15:00).
  Existing intraday cutoff remains an additional gate. Flatten starts 15:50;
  hard deadline 15:55; broker session close 16:00. Recovery is not disabled by pauses.

## Frozen evidence and selection

- H1: comparable company revenue-guidance increase of at least 1%, matching metric,
  period and accounting basis. This is not a consensus surprise.
- H2: quantified binding contract with duration/economic share and total value at
  least 5% of observed annual revenue; comparison age at most 550 days and contract
  duration at most 60 months. Missing figures cause abstention.
- R1: supported primary-risk events and official macro/halt checks veto entry and
  can trigger position review. Untrusted article text never grants broker authority.
- Sources: licensed Alpaca/Benzinga metadata, fixed issuer-domain feeds and permitted
  articles, verified SEC submissions/filings/EX-99. Extraction is deterministic
  source-span numeric grammar; no LLM, inference calls, invented consensus or forecast.
- Publication-to-first-receipt latency at most 900 seconds; receipt age at most
  1,800 seconds. A collected weekend article is not automatically tradable.
  Publication, revision, receipt and availability times are never substituted/backdated.
- Quotes: IEX latest REST, sizes in shares, positive sizes, at most five seconds old;
  maximum 10 bps spread, minimum $5 price and $50 million median daily dollar volume.
  Daily raw historical SIP liquidity/volatility is separately identified.
- Require genuinely pre-event price/volatility; unknown reference abstains.
  Maximum chase is the smaller of 2% and one pre-event volatility measure.
- Rank simultaneously eligible candidates by latest primary receipt, symbol, then
  evidence ID. Persist alternatives and re-evaluate after equipment/pending orders
  clear. Persist a full decision/risk ticket before submission; final dispatch
  rechecks quotes and deadlines. Expected net edge stays unknown.

## Orders and measurement

- AAPL `EQUIPMENT_TEST`: persistent account/session identity, one attempt, cancellation
  request for unfilled remainder around 30 seconds, liquidation target around
  60 seconds from submission. These are observed operational targets, not fill guarantees.
  No replacement purchase, daily recurrence or news-performance inclusion.
- `NEWS_STRATEGY`: existing maximum 60-minute holding period, R1/feed/reconciliation/
  allocation-risk invalidation and session flatten. No invented price stop or profit target.
- Durable client IDs, atomic reservations and single-worker lease fencing.
  Uncertainty is reconciled with the same ID. Cancel acceptance is not cancellation;
  actual cumulative broker fills and positions govern partial exits/replacements.
  Completion requires confirmed zero quantity and no outstanding/unconfirmed order.
- Cost version `v20-paper-residual-20260906-v1`: actual broker-fill VWAP baseline;
  0.5 bps residual friction on each side, sell value fee reserve 0.00002060,
  $0.000195/share with $0.01 minimum per filled sell order. No second spread charge.
  Separate 1.5x/2x/3x residual-friction stresses retain the same fee reserve.
  These are assumptions for simulator omissions, not observed live costs.
- Report broker and modeled dollars; returns on actual deployed notional and
  $10,000 allocated capital with explicit denominators. Zero-interest cash is the
  same-timing/capital benchmark; passive instrument/data are undeclared/unavailable.
  Do not select a benchmark retrospectively.
- Retain all genuine news outcomes, including losses, and 1/5/15/60-minute diagnostic
  candidate markouts with missing observations explicit. Synthetic tests are not Tuesday.
  No Sharpe, profitability or qualification claim from the equipment check or one trade.

## Reporting and changes

The existing 18:00 ET daily notifier persists the full session report before using
the approved email outbox; no new recipients or duplicate test emails. Existing
round-trip notifications remain. Worker/source health, funnel, evidence/tickets,
reconciliation incidents, equipment/news ledgers and delivery failures stay inspectable.

No tuning during Tuesday to obtain a trade or improve its score. A material change
requires a new labeled cohort; necessary safety repairs pause entry and preserve history.
Later formal evaluation still requires a separately frozen eligible feed/strategy,
accounting/benchmark/evaluation start, all eligible outcomes, and existing minimum
60 genuine sessions/60 reconciled round trips plus statistical gates. Practice never
contributes to those floors. Neither sealed holdout is opened.
