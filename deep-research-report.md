# Building an Autonomous Trading Agent: A Production Blueprint

## What the DeEthiopian workshop actually tells us

The public DeEthiopian “Autopilot” page describes a workshop centered on building a trading bot around **a real strategy, a real server, and automated alerts**, with the goal of leaving with the same general setup used by the instructor. citeturn19search0 The additional text you supplied says the paid package contains the class recording, Windows and Mac walkthroughs, build files, four strategies, and Discord access.

The important distinction is that the public material does **not** expose enough of those paid build files or strategies to reconstruct them faithfully. I would not assume I know the “last four strategies” or pretend there is a secret strategy hidden in the public page. What *is* reconstructable is the underlying engineering pattern.

The likely class of system is:

```text
Trading strategy
      ↓
Signal / alert
      ↓
Internet webhook
      ↓
Server-side validation
      ↓
Broker API
      ↓
Order
      ↓
Fill / position tracking
      ↓
Notification + dashboard
```

That pattern is directly supported by the relevant infrastructure. TradingView alerts can send HTTP POST requests to a webhook endpoint, and TradingView provides a mechanism for a webhook recipient to verify its HTTPS client certificate. citeturn17search3 Alpaca exposes APIs to submit, monitor, and cancel orders as well as WebSocket streams for trade, account, and order updates. citeturn17search7turn17search4

The tools listed in the material you pasted—Claude, Lovable, Supabase, and Vercel—fit naturally around that core:

| Tool | Good role in this project | Bad role |
|---|---|---|
| Claude | Research, reasoning, strategy analysis, structured `TradeIntent` generation | Unrestricted direct broker execution |
| Lovable | Dashboard, controls, logs, admin interface | Core low-latency trading process |
| Supabase | Postgres ledger, configurations, audit log, realtime dashboard state | Indefinitely running market-data process |
| Vercel | Dashboard/API/control plane | Permanent execution daemon |
| TradingView | Charts, Pine strategies, alerts | Portfolio/account source of truth |
| Broker API | Orders, fills, positions, account state | Strategy reasoning |

That distinction matters because Vercel Functions have bounded execution durations and cannot stream indefinitely, while Supabase Edge Functions likewise have bounded wall-clock, CPU, and idle durations. Those properties make both excellent application backends but poor places for a permanent market-data/event loop. citeturn19search15turn19search18turn19search4

So the most useful thing to take from the workshop concept is **not “find the magic trading strategy.”** It is the separation of:

> signal generation → automation → execution → observability.

The production-grade version should go considerably further by adding deterministic risk controls, reconciliation, idempotency, realistic testing, and an agent boundary.

## What an autonomous trading agent should actually be

There are three substantially different systems that people call an “AI trading bot.”

### Deterministic automated trader

This is traditional algorithmic trading:

```text
prices → indicators/model → rules → position sizing → order
```

For example:

```python
if fast_ema > slow_ema and not position:
    enter()
elif fast_ema < slow_ema and position:
    exit()
```

There is no language model involved. This architecture is the easiest to backtest because the same inputs deterministically produce the same outputs.

### LLM-assisted trading system

Here, an LLM performs tasks that benefit from language understanding and flexible reasoning—such as analyzing filings, news, market regimes, contradictions among signals, or explaining why a candidate position does or does not fit the strategy—but a deterministic program retains final authority over risk and execution.

```text
                       ┌──────────────┐
news/fundamentals ────→│ LLM analyst  │
                       └──────┬───────┘
                              │ candidate intent
market data ─→ strategy ──────┤
                              ↓
                       deterministic
                        risk engine
                              ↓
                          executor
                              ↓
                           broker
```

**This is the architecture I would build today.**

Anthropic's tool-use system is well suited to this arrangement because application-defined tools can be called through structured inputs, and its strict tool mode can require schema-conforming arguments. citeturn20search1turn20search3 Your application—not Claude—then executes the requested client-side operation and returns its result to the model. citeturn20search15

### LLM-native autonomous trader

This is the more ambitious version:

```text
LLM agent
 ├── reads prices
 ├── reads news
 ├── queries fundamentals
 ├── remembers prior decisions
 ├── decides what to trade
 ├── sizes positions
 ├── executes orders
 └── learns/adapts
```

It is technically possible. The evidence that it should be entrusted with unrestricted capital is much weaker.

StockBench evaluates LLM agents in a realistic sequential stock-trading setting using market prices, fundamentals, and news rather than static financial questions. The benchmark's results found that most evaluated models struggled to beat simple passive baselines consistently. citeturn18search0turn18search12 LiveTradeBench was created precisely because static benchmarks do not test decision-making under evolving real-market uncertainty. citeturn18search26

A 2026 evidence review covering 77 studies found rapidly growing experimentation with LLM trading agents but also substantial comparability problems around temporal splits, execution assumptions, transaction costs, universes, and reproducibility. citeturn18search2

More concerning for completely autonomous execution, TradeTrap stress-tested market intelligence, strategy formulation, portfolio/ledger handling, and execution components. It found that relatively small perturbations could propagate through the decision loop into extreme concentration, runaway exposure, and large drawdowns. citeturn22search1

That leads to the central architectural rule:

> **Give the LLM autonomy over analysis, but not authority over risk.**

Claude can say:

```json
{
  "symbol": "XYZ",
  "side": "BUY",
  "target_weight": 0.03,
  "reason": "...",
  "signal_timestamp": "...",
  "expires_at": "...",
  "confidence": 0.71
}
```

It should **not** possess a tool equivalent to:

```text
place_any_order(symbol, side, quantity)
```

with no intermediary.

Instead:

```text
Claude
  ↓
TradeIntent
  ↓
schema validation
  ↓
strategy policy
  ↓
risk policy
  ↓
account-state reconciliation
  ↓
execution policy
  ↓
broker
```

This recommendation is an engineering inference from the current agent-reliability research and from the fact that structured tool calling lets the application retain control over what actually runs. citeturn22search1turn20search1turn20search15

## Recommended production architecture

A robust implementation should be split into **research, strategy, risk, execution, ledger, and control-plane components**.

```text
                         AUTONOMOUS TRADING PLATFORM

 ┌──────────────────────────────── DATA ──────────────────────────────┐
 │                                                                    │
 │  prices/bars     fundamentals      news/events       account data  │
 │      │                │                  │                 │        │
 └──────┼────────────────┼──────────────────┼─────────────────┼────────┘
        │                │                  │                 │
        ▼                └────────┬─────────┘                 │
 ┌─────────────┐                  ▼                           │
 │ Quant model │          ┌───────────────┐                   │
 │ / strategy  │          │ Claude agent  │                   │
 └──────┬──────┘          │ analyst       │                   │
        │                 └───────┬───────┘                   │
        └──────────────┬──────────┘                           │
                       ▼                                      │
                ┌──────────────┐                              │
                │ TradeIntent  │                              │
                └──────┬───────┘                              │
                       ▼                                      │
             ┌─────────────────────┐                          │
             │ DETERMINISTIC RISK  │◄─────────────────────────┘
             │ ENGINE              │
             │                     │
             │ exposure            │
             │ position limit      │
             │ loss limit          │
             │ stale-data guard    │
             │ liquidity guard     │
             │ duplicate guard     │
             │ kill switch         │
             └──────────┬──────────┘
                        ▼
                ┌───────────────┐
                │ OrderIntent   │
                └───────┬───────┘
                        ▼
               ┌─────────────────┐
               │ Broker adapter  │
               └───────┬─────────┘
                       ▼
               ┌─────────────────┐
               │ Alpaca / IBKR   │
               └───────┬─────────┘
                       │
                  order/fill events
                       ▼
                ┌───────────────┐
                │ Reconciler    │
                └───────┬───────┘
                        ▼
              ┌───────────────────┐
              │ Supabase/Postgres │
              │ immutable ledger  │
              └─────────┬─────────┘
                        ▼
            ┌──────────────────────┐
            │ Lovable/Vercel UI    │
            │ monitoring + control │
            └──────────────────────┘
```

### Market-data layer

Separate **historical research data** from **live market data**. They have different correctness requirements.

For an Alpaca-based MVP, Alpaca gives you broker-side order/account streaming over WebSockets and a paper environment with the same general API surface as live trading. citeturn17search4turn17search0 The paper environment is nevertheless a simulation. Alpaca explicitly warns that it does not reproduce factors including market impact, queue position, latency slippage, information leakage, regulatory fees, and several other live-market effects. citeturn4view0

Do not let individual strategy modules independently download whatever data they want. Normalize incoming observations into something like:

```python
@dataclass(frozen=True)
class MarketSnapshot:
    symbol: str
    timestamp: datetime
    bid: Decimal
    ask: Decimal
    last: Decimal
    volume: Decimal
    source: str
    age_ms: int
```

The `age_ms` field becomes important because a technically valid price that is several seconds or minutes old may be operationally invalid for a strategy.

### Strategy layer

Your first strategy should be **boring and deterministic**.

That does not mean it is profitable. It means you need a strategy simple enough that you can determine whether failures come from your software or from your alpha hypothesis.

For a smoke-test system you could use something as intentionally uninteresting as:

```text
fast moving average > slow moving average → candidate long
fast moving average < slow moving average → candidate exit
```

The objective at this point is verifying:

```text
data
 → calculation
 → signal
 → risk decision
 → order
 → acknowledgement
 → fill
 → position
 → exit
 → reconciliation
```

Only after that pipeline is reliable should you insert more sophisticated research.

### Agent layer

Claude's responsibility should be to produce a **candidate decision**, not a brokerage side effect.

A useful tool set is:

```text
get_market_snapshot(symbol)
get_recent_bars(symbol, timeframe, lookback)
get_account_state()
get_current_positions()
get_strategy_state(strategy_id)
get_recent_news(symbol)
get_fundamentals(symbol)
get_risk_context()
get_prior_agent_decisions(symbol)
propose_trade_intent(...)
```

Notice what is absent:

```text
place_market_order()
transfer_cash()
disable_risk_limits()
change_broker_credentials()
```

Anthropic's strict tool-use functionality can enforce the structure of tool arguments, which is useful for constructing typed trading workflows. citeturn20search1turn20search5 Strict schema validation does **not**, however, prove that an economically bad decision is safe; it only ensures the request is structurally valid. That is why the risk engine remains separate.

I would define the model output roughly as:

```json
{
  "strategy_id": "regime-v1",
  "decision_id": "uuid",
  "symbol": "SPY",
  "action": "INCREASE",
  "target_weight": 0.025,
  "order_preference": "LIMIT",
  "max_slippage_bps": 8,
  "signal_timestamp": "2026-09-04T15:41:00Z",
  "expires_at": "2026-09-04T15:46:00Z",
  "evidence": [
    {
      "type": "technical",
      "observation_id": "..."
    }
  ],
  "thesis": "...",
  "invalidation": "...",
  "confidence": 0.67
}
```

The model never calculates the final number of shares. The deterministic system converts target weight into quantity based on **broker-confirmed equity, current positions, prices, and the active risk policy**.

### Risk engine

Treat risk as a separate program with no generative AI in the decision path.

A request should move through something conceptually like:

```python
def evaluate(intent: TradeIntent, state: AccountState) -> RiskDecision:
    if state.kill_switch:
        return reject("GLOBAL_KILL_SWITCH")

    if intent.is_expired():
        return reject("STALE_INTENT")

    if state.market_data_is_stale(intent.symbol):
        return reject("STALE_MARKET_DATA")

    if state.has_seen(intent.decision_id):
        return reject("DUPLICATE_INTENT")

    proposed = calculate_resulting_exposure(intent, state)

    if proposed.position_notional > policy.max_position_notional:
        return reject("POSITION_LIMIT")

    if proposed.gross_exposure > policy.max_gross_exposure:
        return reject("GROSS_EXPOSURE_LIMIT")

    if state.daily_loss >= policy.daily_loss_limit:
        return reject("DAILY_LOSS_LIMIT")

    if proposed.order_rate > policy.max_order_rate:
        return reject("ORDER_RATE_LIMIT")

    return approve(
        quantity=calculate_safe_quantity(intent, state, policy)
    )
```

Important controls include position exposure, portfolio exposure, maximum order size, duplicate-order prevention, stale-data rejection, daily loss limits, abnormal spread/liquidity rejection, maximum order rate, maximum strategy allocation, and a global kill switch. Those exact thresholds depend on the strategy, instrument, account and tested behavior; they should be configuration, not LLM-generated constants.

The interesting implication from TradeTrap is that risk validation should inspect the **resulting portfolio state**, not merely whether the proposed order appears plausible. A compromised or mistaken decision can be individually valid while creating pathological concentration when combined with the existing portfolio. citeturn22search1

### Execution layer

Use an abstraction:

```python
class Broker:
    async def get_account(self) -> Account: ...
    async def get_positions(self) -> list[Position]: ...
    async def submit(self, order: OrderIntent) -> BrokerOrder: ...
    async def cancel(self, order_id: str) -> None: ...
    async def get_order(self, order_id: str) -> BrokerOrder: ...
    async def stream_updates(self): ...
```

Then implement:

```text
AlpacaBroker
IBKRBroker
PaperBroker
ReplayBroker
```

This is much better than spreading Alpaca or IBKR calls across your strategy code.

With Alpaca, assign a unique `client_order_id` to each submission. Alpaca explicitly supports client-defined order IDs and allows lookup by that ID, which makes them useful for associating orders with different strategies and for idempotency/recovery. citeturn17search0

A good identifier is:

```text
{strategy_id}:{decision_id}:{revision}
```

For example:

```text
macro-v3:0e91f56c-...:0
```

Then a worker crash between:

```text
POST order
```

and:

```text
save "submitted" to database
```

does not automatically imply a second order should be sent after restart. First query the broker by client order ID.

### Reconciliation layer

Your database is not authoritative about actual positions.

Your broker is.

The reconciler repeatedly compares:

```text
expected open orders  ↔ broker open orders
expected positions    ↔ broker positions
expected cash         ↔ broker account
stored fills          ↔ broker fills
```

and emits discrepancies.

This matters because live execution is asynchronous. QuantConnect's documentation similarly distinguishes simulated fills from live trading, where orders are sent to the brokerage and portfolio state updates as brokerage responses arrive. citeturn17search20 Its live-trading documentation also explicitly identifies reconciliation differences between backtests and live trading. citeturn17search2

An order state machine might be:

```text
CREATED
   ↓
RISK_APPROVED
   ↓
SUBMITTING
   ↓
ACKNOWLEDGED ───────→ REJECTED
   ↓
PARTIALLY_FILLED
   ↓
FILLED

ACKNOWLEDGED ───────→ CANCEL_PENDING ───────→ CANCELED
```

Never represent execution using only:

```text
order_success = true
```

because partial fills, late acknowledgements, cancellations, rejected modifications and reconnects are normal states in a trading system.

## Stack choices and the architecture I would use

### The fastest serious MVP

For a US-equity-focused personal system, I would start with:

```text
Python trading worker
        │
        ├── Alpaca paper trading
        ├── Claude API
        ├── Supabase Postgres
        └── REST/WebSocket
                 │
                 ▼
        Lovable or Next.js UI
                 │
                 ▼
              Vercel
```

Alpaca is particularly convenient for this stage because its Python example uses the `TradingClient` with `paper=True`, and the same order concepts can later be used against the live environment. citeturn17search0 Alpaca itself stresses that paper fills differ materially from real execution, so paper-to-live should be treated as another validation transition, not as a simple configuration toggle. citeturn4view0

### Where the worker should run

Run the actual trading worker as a **persistent process**:

```text
Docker container
    on
VM / container service
```

or use a dedicated algorithmic-trading environment such as QuantConnect/LEAN.

Do **not** make a Vercel Function the permanent execution engine. Vercel states that Functions have maximum durations and explicitly notes they cannot stream indefinitely. citeturn19search15turn19search18 Vercel Cron is also implemented by periodically making an HTTP request to a Vercel Function rather than providing a permanently resident process. citeturn19search12

Similarly, Supabase Edge Functions currently have a 256 MB maximum memory limit, bounded duration, two seconds of CPU time per request, and an idle timeout. citeturn19search4

So use:

```text
Vercel      = UI / HTTP control surface
Supabase    = state / config / ledger
Worker      = trading process
```

not:

```text
Vercel serverless function = always-running trader
```

### Where Supabase fits

A sensible schema is:

```text
strategies
strategy_versions
risk_policies

agent_runs
agent_observations
trade_intents
risk_decisions

orders
order_events
fills
positions_snapshots
account_snapshots

market_events
reconciliation_events
system_events
alerts
```

The critical tables should be append-heavy.

For example:

```sql
order_events
------------
id
order_id
broker_order_id
client_order_id
event_type
broker_status
filled_qty
avg_fill_price
payload_json
received_at
```

Instead of repeatedly overwriting:

```text
orders.status = "filled"
```

you preserve:

```text
09:30:02 CREATED
09:30:02 SUBMITTING
09:30:03 ACKNOWLEDGED
09:30:04 PARTIAL_FILL 20
09:30:05 PARTIAL_FILL 30
09:30:06 FILLED 50
```

That creates the audit trail needed to reconstruct what actually happened.

Supabase provides hosted Postgres and can support realtime application updates; Lovable's Supabase integration can generate database migrations, backend functions and UI wiring around a connected project. citeturn19search2 Lovable also stores backend secrets through its Supabase integration rather than putting them into frontend code. citeturn19search2turn19search5

Broker credentials should therefore be:

```text
worker environment / secret manager
```

and never:

```text
React source
browser localStorage
Lovable frontend
Claude prompt
database row readable by client
TradingView alert payload
GitHub repository
```

Lovable's own security documentation warns that frontend code executes in the browser and therefore cannot safely hold API keys. citeturn19search14turn19search26

### Scheduled work

Supabase is useful for lower-frequency jobs such as:

```text
daily report
overnight feature calculation
reconciliation check
strategy health summary
weekly evaluation
```

Supabase's `pg_cron` can invoke Edge Functions on a recurring schedule, and its Cron system supports schedules ranging from every second to once per year. citeturn19search1turn19search7

That still does not make an Edge Function the ideal home for a full-session market WebSocket. Its runtime limits remain. citeturn19search4

### When to use TradingView

A TradingView-first design is excellent for getting from an existing Pine Script strategy to automated paper execution:

```text
Pine strategy
      ↓
TradingView alert
      ↓
HTTPS webhook
      ↓
your webhook service
      ↓
risk engine
      ↓
broker
```

TradingView supports webhook POST alerts, and when HTTPS is used it exposes certificate information that can be checked by the recipient to establish that the request originated from TradingView's webhook infrastructure. citeturn17search3

The webhook should contain a **signal**, not an already authorized broker command:

```json
{
  "strategy_id": "tv-breakout-v2",
  "symbol": "AAPL",
  "signal": "LONG",
  "bar_timestamp": "...",
  "alert_id": "..."
}
```

Then your own system independently verifies:

```text
symbol allowed?
strategy enabled?
timestamp fresh?
duplicate?
market open?
account reconciled?
risk acceptable?
size acceptable?
kill switch off?
```

This is a safer extension of the DeEthiopian-style “strategy + server + automated alerts” pattern described publicly. citeturn19search0

### When to use QuantConnect/LEAN

For a more serious research platform, LEAN is compelling because the same algorithmic framework can be used for backtesting and live trading across different brokerage/data configurations. citeturn17search14

More importantly, LEAN exposes fill, fee, slippage and brokerage models instead of assuming that a historical bar means you could have traded arbitrary quantities at its displayed price. citeturn17search9turn17search18

QuantConnect explicitly notes that live results can deviate from simulations because brokerage fills, latency, slippage, stale prices and market impact differ from modeled behavior. citeturn17search2turn17search12

I would choose LEAN when:

| Requirement | LEAN fit |
|---|---:|
| Serious historical research | Excellent |
| Multiple strategies | Excellent |
| Custom slippage/fill models | Excellent |
| Backtest/live consistency | Excellent |
| Fastest tiny prototype | More machinery than necessary |
| Fully custom agent orchestration | Possible, but more integration work |

### When to use Interactive Brokers

IBKR makes sense when instrument breadth matters. Its API supports automated trading, portfolio information and market data, and IBKR's TWS platform covers stocks, options, futures, forex, bonds and funds across a broad set of markets. citeturn23search0turn23search1

The trade-off is operational complexity. TWS/IB Gateway-based integrations involve a running brokerage application, authentication/session considerations and reconnect behavior. IBKR documents specific connectivity loss/restoration events that API clients are expected to handle. citeturn23search1turn23search11

So, for this project:

```text
Fastest personal equity prototype → Alpaca
Rigorous research/live framework  → LEAN / QuantConnect
Broader instruments/markets       → IBKR
TradingView strategy automation   → webhook + one of the above
```

## Concrete implementation blueprint

For your engineering background, I would structure the repository roughly like this:

```text
autonomous-trader/
│
├── apps/
│   └── dashboard/
│       ├── positions/
│       ├── orders/
│       ├── strategies/
│       ├── agent-runs/
│       ├── risk/
│       └── system/
│
├── services/
│   ├── trader/
│   │   ├── main.py
│   │   ├── event_loop.py
│   │   ├── strategy_engine.py
│   │   ├── agent.py
│   │   ├── risk_engine.py
│   │   ├── execution.py
│   │   ├── reconciliation.py
│   │   └── monitoring.py
│   │
│   └── webhook/
│       └── tradingview.py
│
├── domain/
│   ├── market.py
│   ├── trade_intent.py
│   ├── order.py
│   ├── position.py
│   ├── risk.py
│   └── events.py
│
├── adapters/
│   ├── alpaca/
│   ├── ibkr/
│   ├── anthropic/
│   └── supabase/
│
├── research/
│   ├── strategies/
│   ├── features/
│   ├── notebooks/
│   └── validation/
│
├── tests/
│   ├── unit/
│   ├── integration/
│   ├── replay/
│   ├── failure/
│   └── broker/
│
└── infra/
    ├── docker/
    └── migrations/
```

### Core domain object

Make every strategy—human-coded or agent-generated—emit the same object.

```python
from datetime import datetime
from decimal import Decimal
from enum import Enum
from pydantic import BaseModel, Field


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class TradeIntent(BaseModel):
    decision_id: str
    strategy_id: str
    symbol: str
    side: Side

    # Strategy asks for desired exposure; execution decides quantity.
    target_weight: Decimal = Field(ge=Decimal("0"), le=Decimal("1"))

    signal_timestamp: datetime
    expires_at: datetime

    max_slippage_bps: Decimal = Field(ge=Decimal("0"))
    rationale: str
```

The same downstream risk engine works whether the object originated from:

```text
Pine Script
Python strategy
ML classifier
Claude
human operator
```

That is a powerful design property.

### Agent execution

Conceptually:

```python
async def generate_intent(context: TradingContext) -> TradeIntent | None:
    market = await tools.get_market_snapshot(context.symbol)
    portfolio = await tools.get_portfolio_context()
    strategy = await tools.get_strategy_policy(context.strategy_id)

    response = await claude.generate_trade_decision(
        market=market,
        portfolio=portfolio,
        strategy=strategy,
    )

    if response.action == "NO_TRADE":
        return None

    return TradeIntent.model_validate(response.trade_intent)
```

Then explicitly break the chain:

```python
intent = await generate_intent(context)

if intent is not None:
    risk_decision = risk_engine.evaluate(
        intent=intent,
        account=broker_account,
        positions=broker_positions,
        market=market_snapshot,
    )

    if risk_decision.approved:
        order = execution.create_order(intent, risk_decision)
        await execution.submit(order)
```

The LLM cannot call `execution.submit()`.

That is the architectural boundary I would defend most strongly given current evidence about trading-agent perturbation and reliability. citeturn22search1

### Main event loop

For a bar-based strategy:

```python
while running:
    health.assert_healthy()

    account = await broker.get_account()
    positions = await broker.get_positions()

    await reconciler.update(account, positions)

    bar = await market_data.next_completed_bar()

    for strategy in enabled_strategies:
        candidate = await strategy.evaluate(bar)

        if candidate:
            decision = risk_engine.evaluate(
                candidate,
                account=account,
                positions=positions,
            )

            await ledger.record(decision)

            if decision.approved:
                await executor.submit(decision)

    await alerts.process()
```

For event-driven data, replace polling with:

```text
market WebSocket ─┐
broker WebSocket ─┼→ internal event bus → handlers
news feed ────────┘
```

Alpaca's WebSocket API exposes trade/account/order updates, making asynchronous order-state handling possible without repeatedly polling every submitted order. citeturn17search4

### Kill switch

The kill switch should exist outside the agent:

```text
dashboard button
       ↓
risk_policies.global_enabled = false
       ↓
worker receives update
       ↓
NO NEW ORDERS
```

A second, stronger control can be:

```text
TRADING_ENABLED=false
```

at the worker/runtime level.

The model should never have a tool that can modify either control.

Your dashboard should prominently show:

```text
MODE: PAPER / LIVE

TRADING: ENABLED / DISABLED

BROKER: CONNECTED / DEGRADED / DISCONNECTED

MARKET DATA: FRESH / STALE

RECONCILIATION: OK / MISMATCH

DAILY P&L

GROSS EXPOSURE

OPEN ORDERS

LAST AGENT DECISION

LAST EXECUTION EVENT
```

### Agent memory

Do not initially give Claude a vague free-form “memory.”

Give it explicit historical facts:

```text
prior decisions
prior rationales
realized outcome
expected vs actual fill
strategy regime
portfolio state at decision time
risk rejection reason
```

For example:

```json
{
  "decision_id": "...",
  "thesis": "...",
  "approved": false,
  "risk_rejection": "MAX_SYMBOL_EXPOSURE",
  "future_1d_return": null,
  "actual_execution": null
}
```

This makes memory auditable.

A later version can retrieve similar historical decisions semantically, but the raw event ledger should remain the source underneath it.

### Observability

Every agent run should capture:

```text
model/provider
prompt or prompt version
strategy version
available observations
tool calls
tool results
raw decision
parsed TradeIntent
risk result
order IDs
fill result
latency
errors
token/API cost
```

That enables answering:

> Why did the bot make this trade?

without relying on the LLM to retrospectively invent an explanation.

## Validation and go-live gates

The hardest part is not getting a bot to submit an order. Alpaca's own documentation makes that straightforward. citeturn17search0

The hard part is determining whether the system behaves acceptably when reality stops looking like the backtest.

### Backtesting

A backtest should incorporate at minimum:

```text
historical data available at that time
fees
bid/ask effects
slippage
execution assumptions
position constraints
delays
delistings/universe rules where relevant
```

LEAN provides explicit fee and slippage models precisely because ignoring these effects makes simulated results less realistic. citeturn17search12turn17search18

It is especially important to avoid:

```text
test strategy
↓
change parameters
↓
test same history
↓
change parameters
↓
test same history
↓
choose best
↓
call final Sharpe "out-of-sample"
```

The Probability of Backtest Overfitting literature formalizes the problem that searching across many strategy configurations can create apparently strong historical results that do not generalize. citeturn11search0turn11search28 The Deflated Sharpe Ratio was proposed to adjust performance evaluation for selection bias, multiple testing/backtest overfitting, and non-normal returns. citeturn11search12

A stronger process is:

```text
Research / training
         ↓
validation
         ↓
untouched out-of-sample test
         ↓
paper forward test
         ↓
tiny live allocation
         ↓
controlled scaling
```

Use chronological splits, not random train/test splits, for sequential trading data.

### Walk-forward evaluation

Instead of one heroic backtest:

```text
2018 ─────────────────────────────── 2026
      TRAIN  TEST
             TRAIN  TEST
                    TRAIN  TEST
                           TRAIN TEST
```

Track consistency across regimes.

A useful scorecard contains more than return:

| Category | Measure |
|---|---|
| Return | cumulative return / CAGR where meaningful |
| Risk | max drawdown |
| Risk-adjusted | Sharpe / Sortino |
| Trading | turnover |
| Execution | expected vs actual slippage |
| Robustness | performance by period/regime |
| Concentration | largest position and sector exposure |
| Reliability | failed/duplicate/rejected orders |
| Agent | decision stability and risk rejection rate |
| Benchmark | excess return versus predefined baseline |

StockBench itself evaluates sequential agents using metrics that include cumulative return, maximum drawdown and Sortino ratio, reflecting the need to evaluate risk as well as raw profitability. citeturn18search12

### Paper trading

Paper trading tests the **system**, not merely the strategy:

```text
Does the worker stay alive?
Do WebSockets reconnect?
Are duplicate orders prevented?
Do fills update correctly?
Do positions reconcile?
Do alerts fire?
Does the kill switch work?
Do agent decisions expire?
Does stale data block execution?
What happens after a crash?
```

Alpaca explicitly states that its paper environment is a simulation rather than a substitute for real trading because it omits market impact, latency slippage, queue position and other real-world factors. citeturn4view0 QuantConnect similarly notes differences between modeled fills and live brokerage behavior. citeturn17search2

### Failure injection

This is where I would spend much more effort than the typical retail trading-bot tutorial.

Deliberately simulate:

```text
market data freezes
WebSocket disconnects
broker times out after receiving order
broker accepts order but HTTP response is lost
duplicate TradingView webhook
database temporarily unavailable
agent API unavailable
agent returns invalid/inconsistent decision
price moves between decision and submission
partial fill
order rejection
unexpected existing position
worker process crashes
worker restarts
clock skew
```

For the dangerous case:

```text
submit()
   ↓
broker receives it
   ↓
your process crashes before saving ACK
```

restart behavior must be:

```text
reconcile first
```

not:

```text
resubmit everything that looked unfinished
```

Client-defined order identifiers help make that recovery process tractable with Alpaca. citeturn17search0

TradeTrap's results reinforce the value of exactly this kind of component-level stress testing: faults in one trading-agent component can propagate across the entire decision/execution loop. citeturn22search1

### Shadow mode

Before letting the LLM make even paper orders, run it in:

```text
SHADOW MODE
```

where it produces decisions but cannot trade.

Record:

```text
Agent wanted BUY
Risk would approve
Hypothetical quantity = ...
Hypothetical entry = ...
Subsequent outcome = ...
```

Then compare the agent against:

```text
deterministic strategy
no-agent strategy
passive benchmark
random/control policy where useful
```

This is important because current research does not establish that higher general LLM capability automatically means stronger trading performance. Live-market benchmark work was motivated by precisely that evaluation gap. citeturn18search26turn18search2

### Progressive autonomy

I would use these autonomy levels:

| Mode | Agent | Risk engine | Orders |
|---|---|---|---|
| Observe | analyzes | evaluates | none |
| Recommend | generates intent | evaluates | human approves |
| Paper autonomous | generates intent | mandatory | paper |
| Restricted live | generates intent | mandatory | tightly constrained live |
| Full policy autonomy | generates intent | mandatory | within preapproved envelope |

There should **never** be a stage called:

```text
LLM unrestricted access to brokerage
```

Current agent reliability results do not justify that architecture. citeturn22search1

## The version I would build from the tools in your context

The cleanest implementation combining the DeEthiopian concept with the tools you listed is:

```text
                        ┌──────────────────┐
                        │ TradingView      │
                        │ optional signals │
                        └────────┬─────────┘
                                 │ webhook
                                 ▼
┌───────────────┐        ┌─────────────────┐
│ Market/news/  │───────→│ Python worker   │
│ fundamentals  │        │                 │
└───────────────┘        │ strategy engine │
                         │ Claude agent    │
                         │ risk engine     │
                         │ reconciler      │
                         └────────┬────────┘
                                  │
                                  ▼
                         ┌─────────────────┐
                         │ Alpaca PAPER    │
                         └────────┬────────┘
                                  │
                         orders / fills
                                  │
                    ┌─────────────┴────────────┐
                    ▼                          ▼
            ┌──────────────┐           ┌──────────────┐
            │ Supabase     │           │ Monitoring   │
            │ Postgres     │           │ / alerts     │
            └──────┬───────┘           └──────────────┘
                   │ realtime
                   ▼
            ┌───────────────┐
            │ Lovable UI    │
            │ deployed via  │
            │ Vercel        │
            └───────────────┘
```

The responsibilities would be:

**Claude:** analyst and intent generator. Use typed tool calls and a strict schema. citeturn20search1turn20search3

**Python worker:** actual autonomous state machine. It owns strategy computation, risk, broker calls, event processing and reconciliation.

**Alpaca:** first paper broker because its API directly supports paper trading, programmatic orders, client order IDs and streaming order updates. citeturn17search0turn17search4

**Supabase:** system-of-record for your *application's* history/configuration—agent decisions, risk decisions, order events, fills, alerts and strategy versions. Lovable can connect directly to Supabase and wire database-backed application features. citeturn19search2

**Lovable:** rapidly produce the monitoring/control interface.

**Vercel:** host that interface and request-oriented API endpoints, rather than the permanent trader, because Vercel Functions remain duration-bounded. citeturn19search15turn19search18

**TradingView:** optional. Keep it if strategies originate in Pine or you want visual manual verification. Remove it entirely when the strategy engine owns its own market-data computation.

The first end-to-end milestone should therefore be:

```text
market observation
       ↓
deterministic toy signal
       ↓
TradeIntent
       ↓
risk engine
       ↓
Alpaca PAPER
       ↓
broker acknowledgement
       ↓
fill
       ↓
Supabase event log
       ↓
Lovable dashboard
```

Only after this works reliably:

```text
market/news
       ↓
Claude
       ↓
TradeIntent
       ↓
SAME EXISTING risk/execution stack
```

That sequencing is important. You do not want to debug an LLM, brokerage integration, strategy logic, database, WebSocket state machine, dashboard and risk engine simultaneously.

A practical build order is therefore:

```text
Domain models
     ↓
Paper broker adapter
     ↓
Ledger
     ↓
Reconciler
     ↓
Risk engine
     ↓
Deterministic strategy
     ↓
Dashboard
     ↓
Failure testing
     ↓
Claude shadow agent
     ↓
Claude autonomous paper mode
     ↓
Out-of-sample + forward evaluation
     ↓
Only then consider restricted live execution
```

## Legal and operational boundaries

An automated system used solely for your own account is a materially different regulatory scenario from selling individualized automated investment advice or managing assets for customers. Investor.gov defines an investment adviser generally as a person or firm that, **for compensation**, is in the business of providing investment advice to others regarding securities or issuing securities analyses as part of a regular business. citeturn21search0turn21search3 The SEC also treats automated advice as a means of providing an advisory service rather than as an inherently separate category. citeturn21search19

Consequently, turning this from:

```text
"My bot trades my brokerage account"
```

into:

```text
"Customers pay me and my software tells/trades what securities they should own"
```

can introduce investment-adviser, broker/dealer, custody, disclosure, advertising and other questions depending on exactly how the service operates. The legal classification is fact-specific and needs securities counsel before commercialization. citeturn21search0turn21search13

FINRA's materials are also instructive operationally. FINRA describes algorithmic trading strategies in the broker-dealer context as automated programs that generate and route orders without material human intervention, and its guidance emphasizes supervision and control practices around algorithmic strategies. citeturn21search2turn21search5 Those rules are not automatically a compliance checklist for a person running a private retail bot, but the engineering principle—predeployment controls, supervision, and the ability to stop malfunctioning automation—is highly relevant.

There has also been a material U.S. day-trading rule change since older bot courses and YouTube videos were recorded. On **April 14, 2026**, the SEC approved FINRA's replacement of the longstanding pattern-day-trader framework. FINRA states that the new standards eliminate the old trade-count-based “pattern day trader” designation and the associated $25,000 minimum equity requirement. citeturn18search3turn18search22 FINRA's current investor guidance instead describes intraday margin requirements tied to actual intraday exposure, with brokerage firms monitoring whether adequate equity is maintained. citeturn18search11turn18search31 Therefore, older tutorials telling you that every U.S. equity day-trading bot necessarily operates under the old $25,000 PDT rule are outdated as of September 4, 2026.

The deeper engineering lesson is that an autonomous trader is not primarily an “AI prompt.”

It is a financial state machine:

```text
                 ┌───────────────┐
                 │ Intelligence  │
                 │ Claude / ML / │
                 │ quant rules   │
                 └───────┬───────┘
                         │
                    proposal only
                         │
                         ▼
              ┌────────────────────┐
              │ Deterministic Risk │
              └─────────┬──────────┘
                        │
                        ▼
              ┌────────────────────┐
              │ Reliable Execution │
              └─────────┬──────────┘
                        │
                        ▼
              ┌────────────────────┐
              │ Broker Truth       │
              └─────────┬──────────┘
                        │
                        ▼
              ┌────────────────────┐
              │ Reconciliation     │
              └─────────┬──────────┘
                        │
                        ▼
              ┌────────────────────┐
              │ Auditable History  │
              └────────────────────┘
```

The DeEthiopian “strategy + server + automated alerts” design is a reasonable starting skeleton. citeturn19search0 The production-grade upgrade is to make **risk, execution, account reconciliation, idempotency and observability deterministic**, then place Claude above that safety boundary rather than below it. That design preserves the useful part of agentic AI—flexible analysis and tool-driven reasoning—without making a probabilistic language model the final authority over irreversible brokerage actions, a separation strongly supported by the current reliability evidence for autonomous trading agents. citeturn20search15turn22search1turn18search2