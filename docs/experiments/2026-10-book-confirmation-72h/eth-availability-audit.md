# ETH data correctness versus size eligibility

The previous depth-ten trial remains **167/180, 92.78%, two-symbol NO-GO**.
This audit classifies its thirteen failures; it does not relabel the trial.

## Eight size failures are valid observed small quantities

All eight have positive, nonmissing ETH base quantities and valid raw
price/quantity fields that independently replay to the exchange's top-ten
CRC. Units are USD per ETH multiplied by ETH quantity, not a USD quantity
misread as ETH. Their insufficient side notionals are:

| Evaluation | Failing side | Displayed USD notional |
|---|---|---:|
| `256bc67a-28ec-5cac-bc29-1f5b74dc64db` | Future bid | 8.6445667341 |
| `e2060fd2-7afd-56db-ad0a-037f787537a8` | Entry ask | 9.3366283556 |
| `f3d4b5f5-7ace-5560-8751-643c0f3ee8dc` | Entry ask | 9.3366283556 |
| `f4d4c833-68af-5921-b721-5448db44e938` | Entry ask | 4.0118387100 |
| `f1b06ee4-4786-584b-b715-b273d17d5175` | Entry ask | 4.7063096900 |
| `86750d69-8ea9-5f41-8b0c-0bde508b35d3` | Entry ask | 7.4989313385 |
| `e7b7cc59-0e5e-53a2-81f1-1ee16a103a1f` | Entry ask | 7.0277110960 |
| `8615877f-7811-5da3-9e11-e5e6d5af03e1` | Future bid | 1.9572208644 |

The unchanged requirement is **$10.25 at the best executable side**. These
are trade-size ineligibility observations, not missing or corrupt market
data. They do not establish insufficient liquidity across deeper levels;
walking the book would be a different execution contract with slippage.
Ticket size, levels and eligibility are not changed here.

## Five freshness failures are ETH update inactivity at the cutoffs

The selected ETH book's provider age was 2.327-3.221 seconds and receive
age was 2.246-3.138 seconds. The same socket was still receiving BTC book
updates within 0.067-0.499 seconds of every failed cutoff. Provider-to-receive
delay was approximately 80 milliseconds, not a multi-second network delay.
No reconnect, CRC rejection or reconstruction loss occurred in that trial.

Thus the evidence supports **no newer native ETH book update received
within the frozen two-second fence**, not socket disconnection, a missing
quantity, a timestamp/unit decoder bug or multi-second transport delay.
It does not prove the venue's quote was invalid merely because unchanged,
nor establish an undocumented publication guarantee.

Price-change age was often much longer than book-state age even on good
observations. The confirmation will retain those clocks separately.
Neither recent BTC activity nor heartbeats refresh the ETH state timestamp.

## Separate metrics, unchanged combined gate

| Metric | ETH result | Interpretation |
|---|---:|---|
| Reconstruction consistency | All 96,200 trial book messages verified | Both symbols' raw CRC replay, not execution proof |
| Both decision/horizon endpoints fresh | 175/180, 97.22% | Data availability under the time contract |
| Size-eligible among fresh paired endpoints | 167/175, 95.43% | Conditional availability, correlated observations |
| Full primary contract | **167/180, 92.78%** | Original coverage and NO-GO preserved |

An unconditional side-size count includes stale observations and is not
permission to trade. Data integrity, freshness and conditional size
eligibility are diagnostics; none replaces the full scheduled/mature gate.

`eth-thirteen-failures.json` retains every original evaluation identity,
selected source/frame, exact quantity/notional, price-change versus
book-state clocks, receipt/acceptance and last/next causal socket/BTC/ETH
message context.

The evidence justifies one fixed longer confirmation using the same
native book implementation and thresholds. It does not justify reducing
order size, silently dropping ETH or repeatedly sampling favorable windows.
