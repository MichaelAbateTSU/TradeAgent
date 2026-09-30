# Full-population September signal research: results

This is an **exploratory retrospective**, not a new trading cohort or a
confirmation on untouched data. It leaves the completed September experiment,
its negative broker-paper results, its cutoff and the `no_support` model
unchanged.

## Scope and provenance

The successful isolated Render job `job-dau7tllg1s2s73bqmvig` ran on
September 30, 2026, 03:15:34-03:27:57 UTC. It read the declared cohort's
**3,528 retained candidate decisions** and **3,340,716 raw market events**
from 534,835 content-verified batches, producing 3,327,858 normalized quote/
continuity observations. No broker client, execution lease, production
database write, worker configuration change or model activation was used.

The exact aggregate report is [retrospective-results.json](retrospective-results.json).
Its source hash is
`c0c8aa5eaa47e641c90a6dc38c9cd0253070c6b957989768fdfab5a0048fc321`;
the market-manifest hash is
`914374a12811d35d33a303b4d37a52506a5fd2c4d9cb5dfc910efd354dc811e3`.
The research-code hash
`1d9ec3afcc0dee82bc87421f03fcd244a0a581e1c54e9841e8a9bd7991b8dc28`
matches the committed quote-only reader. The job generated per-signal
`markouts.jsonl` with hash
`368ab7db06507280dc002edb5acdb13b88caab0e5e49e2301e0723d365bbe27c`.
The aggregate report was retrieved and archived; the large per-signal file
was on the job's ephemeral disk, **not a claimed durable row export**. It
can be regenerated from the hashed retained database inputs.

The input interval was September 21 00:00 to September 28 14:40:07 UTC.
The post hoc split was September 25 00:00, with a 900-second horizon
embargo. This yielded 2,342 discovery and 1,186 later diagnostic signals.
Both populations are known September evidence, never relabeled as a
prospective or sealed holdout.

The first isolated attempt, `job-dau7krmk1f9s73amn6ag`, expired at its
900-second resource limit without a completed report. Replaying already
recorded feature history was replaced by quote-only reconstruction using
the existing book-update rules. The successful retry did **not** truncate
signals, drop horizons, lower fees or raise its 900-second timeout.

## 1. Horizon comparison after unchanged fees

The table includes only complete quote-based outcomes and discloses their
coverage. Returns are hypothetical arrival-ask to deadline-bid, with
25-bps base-inventory entry fees and 25-bps cash exit fees applied
multiplicatively. Spread and observed quote-price movement are embedded.
They are not actual broker fills or a portfolio return series.

| Horizon | Complete discovery labels / 2,342 | Mean gross bps | Mean net bps | Net win rate |
|---|---:|---:|---:|---:|
| 1 second | 191 (8.16%) | -2.639 | -52.563 | 0% |
| 5 seconds | 137 (5.85%) | -2.512 | -52.437 | 0% |
| 15 seconds | 129 (5.51%) | -1.464 | -51.394 | 0% |
| 30 seconds | 149 (6.36%) | -2.332 | -52.258 | 0% |
| 60 seconds | 134 (5.72%) | -2.922 | -52.845 | 0% |
| 5 minutes | 109 (4.65%) | 2.214 | -47.734 | 0.92% |
| 15 minutes | 82 (3.50%) | 3.104 | -46.849 | 6.10% |

These are **different complete-case populations**, not interchangeable
independent samples. On paired complete discovery signals:

- 30 minus 5 seconds: -0.122 bps mean net improvement (69 pairs).
- 60 minus 5 seconds: -0.152 bps (64 pairs).
- 300 minus 5 seconds: +4.447 bps (58 pairs).
- 900 minus 5 seconds: +3.857 bps (38 pairs).

The longer horizons did not create positive average after-fee payoffs.
Selecting the few positive long-horizon cases retrospectively would be
outcome cherry-picking, not a demonstrated entry rule.

## 2. Prediction versus economic payoff

For discovery midpoint-direction observations, five-second long accuracy
was 50.0% and score/rank-return correlation was approximately 0.016.
At 60 seconds they were 45.6% and -0.063. Midpoint and executable metrics
have different explicit coverage. These descriptive results neither prove
randomness nor establish tradable prediction.

The report contains win rate, mean winner/loser, lower-tail outcomes,
Pearson and tie-aware Spearman correlations, and a nonannualized mean/std
ratio. The ratio is **not portfolio Sharpe**: repeated overlapping signals
do not constitute independently funded trades. Complete discovery
economic labels span only three UTC days, so the five-day minimum for
day-block uncertainty intervals was not reached.

## 3. Where the apparent winners occur

There were no positive net complete discovery markouts at 1-60 seconds.
At 300 seconds there was one; at 900 seconds there were five. The
discovery-only top-decile report preserves the selected event IDs and
fixed regime counts, including negative top-decile members.

For the five positive 900-second cases:

| Predecision bucket dimension | Positive cases |
|---|---|
| Asset | 3 BTC/USD, 2 ETH/USD |
| Volatility | 3 medium (2-5 bps), 1 high (at least 5 bps), 1 low |
| UTC time | 3 at 06-12, 1 at 12-18, 1 at 18-24 |
| Observed trade-volume regime | 3 with no observed trades, 2 with one-to-four |

This does **not** isolate a high-volatility-only source of winners: the
high-volatility bucket had only two complete 900-second observations and
a negative mean net result. Every populated 900-second bucket mean was
negative. Five outcomes, extensive missingness and retrospective selection
are not an edge, a news attribution or a basis for a new paper allocation.
No point-in-time news join was available.

## 4. Fixed high-volatility 60-second hypothesis

The filter defined in [Experiment #2](experiment-2-proposal.md) matched
102 discovery and 63 later diagnostic signals. Its discovery results:

| Horizon | Complete labels | Mean net bps | Net winners |
|---|---:|---:|---:|
| 5 seconds | 7 | -56.573 | 0 |
| 60 seconds | 6 | -56.099 | 0 |

The means are not a paired proof of improvement. The populations and
coverage differ, only two discovery UTC days are represented, and the
later diagnostic half has zero complete five/sixty-second outcomes.

**Screening decision:** the proposed H2-60 rule does not qualify for a
broker-paper trial on these results. It is retained as a falsifiable
shadow-only proposal, not activated. Neither the economic target nor
coverage/support requirements passed.

## 5. The observation limit must remain visible

The later diagnostic population had one complete one-second outcome
(negative net), and zero complete 5/15/30/60/300/900-second outcomes under
the declared one-second quote-age, displayed-size, continuity and price-cap
contract. At 60 seconds its reasons included 699 stale decision quotes,
473 missing/stale arrival quotes, 1,185 missing/stale exit quotes, one
undersized entry and three non-entry hypotheses. Reasons overlap and
must not be summed into an independent incident count.

This is **inconclusive later-period evidence**, not a zero return or a
validation failure proving universal lack of alpha. The existing fourteen
broker round trips remain genuine execution evidence even when a stricter
hypothetical quote contract cannot reconstruct their counterfactuals.
Native L1 can be observed independently of L2 in this research reader;
that does not weaken the qualified worker's L2/data safeguards.

Do not loosen freshness or fill unavailable horizons from later favorable
quotes. The next predeclared shadow protocol must establish usable
decision/arrival/horizon coverage before any economic promotion claim.
The proposed October 1-15 dates are **not scheduled**; if prospective
collection is not frozen in time, that window is missed, not backfilled.
