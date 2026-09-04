# Market data

TradeAgent accepts a deliberately small canonical bar schema:

```text
timestamp,symbol,open,high,low,close,volume
```

Timestamps must contain an offset and are normalized to UTC. Prices must be positive;
volume cannot be negative; OHLC ranges must be internally consistent. Bars for each
symbol must be unique and strictly chronological. Invalid input fails before research or
paper execution.

## Alpaca historical download

Set credentials in the process environment or an untracked `.env`:

```powershell
$env:ALPACA_KEY_ID = "..."
$env:ALPACA_SECRET_KEY = "..."
tradeagent download-alpaca `
  --symbol SPY `
  --start 2020-01-01 `
  --end 2026-01-01 `
  --timeframe 1Day `
  --output data\spy.csv
```

The client:

- can call only `https://data.alpaca.markets`;
- has no order or brokerage method;
- requests `adjustment=all`, ascending sort, and IEX by default;
- follows pagination and fails on a repeated token;
- stores no credentials in the output, ledger, or experiment registry;
- refuses to overwrite an existing file unless `--overwrite` is explicit.

Set `ALPACA_FEED=sip` only if the account is entitled to SIP data.

## Research use

```powershell
tradeagent backtest --csv data\spy.csv --symbol SPY
tradeagent evaluate --strategy sma --csv data\spy.csv --symbol SPY
```

The dataset hash is over the canonical validated bars, so content changes produce a
different experiment identity.

## Known limitations

Alpaca is a convenient prototype source, not a sufficient canonical institutional
research store. Before relying on results:

1. verify exchange calendars and missing sessions;
2. cross-check splits, dividends, symbol changes, and delistings;
3. avoid survivorship-biased current-index constituent lists;
4. preserve raw vendor responses and entitlement metadata outside this MVP;
5. compare adjusted and raw series as required by the strategy;
6. obtain quote/spread data for intraday execution research.

Broker history must not become the sole research source. A later storage layer should
retain immutable raw vendor data and materialize point-in-time feature datasets from it.

