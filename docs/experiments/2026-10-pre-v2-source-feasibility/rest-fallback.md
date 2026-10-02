# Separately frozen REST fallback study

The native-stream study preserved its original window and results. Both
conditional Kraken-location WebSockets returned provider error **406**
while the protected v1 `us` connection remained unchanged. Their lack of
captured quotes is an **access/connection limitation**, not a measurement
of Kraken's intrinsic quote quality.

The existing credentials previously provided GET access to both
location-specific latest-quote endpoints. A separate prospective,
bounded REST-acquisition study therefore tests that available alternative
without stopping v1, acquiring new credentials or opening another socket.
It does not turn REST snapshot freshness alone into a feasibility pass.

## Fixed fallback protocol

- Evaluation window: October 2 **16:45-17:15 UTC**, with fixed capture tail
  to **17:16:05 UTC**.
- Freeze hash:
  `ea8d5b3cfb635f9c6eac14be1cec66611324c159c50f00898af8e30c481ebd99`.
- Acquisition: one GET every 1.5 seconds for each alternate source,
  both BTC/USD and ETH/USD in each response; at most 80 requests per minute
  across the two sources. The acquisition cadence is not the evaluation
  cadence and is the same for both sources.
- Evaluation cadence, primary horizon, settlement, both age fences and
  entry-ask/exit-bid $10.25 displayed size remain **10s / 60s / 2s / 2s /
  $10.25**, unchanged.
- Shared clocks: the original v1 evaluations and primary-label resolution
  timestamps within this predeclared window.
- Every actual evaluation old enough for a primary label stays in the
  denominator; empty/stale/undersized/unavailable observations are missing,
  never filled from a later response.

The first study is not extended, reinterpreted or pooled with the fallback
to choose favorable intervals. `frozen-rest-study.json` contains the
complete fallback configuration and references the original study hash.
The different windows and acquisition methods remain separately identified.

## Timestamp and identity safeguards

Provider RFC3339 timestamps are unchanged. Local receipt is recorded when
the GET response arrives, not when it was requested. Consumer acceptance
completion is separately retained and bounds decision availability.
Recent poll receipt cannot make an old provider quote fresh.

Raw response text, HTTP status, request time, receipt/monotonic clocks,
rate-limit headers, source identity and a chained raw manifest are retained.
Source records identify `REST_GET`; they do not claim a WebSocket connection
or relabel Kraken quotes as Alpaca US. HTTP failures create diagnostic
continuity boundaries and remain part of source-quality evidence.

## Unchanged decision gate

A source must pass **both symbols' mature primary-label coverage at 95%**
and the independently established venue relationship. The configured
paper simulator's crypto reference location remains unproven. Data API
access does not waive that requirement.

No application code or configuration changes, source switch, broker order,
account modification, v2 registration or model promotion occur in this
study. The worker stays on approved release `94e80ec`, with expired
authorization and `no_support`.
