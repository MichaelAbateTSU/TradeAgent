# Render always-on deployment

The root [`render.yaml`](../../render.yaml) creates:

- managed PostgreSQL with external access disabled;
- one Docker dashboard service;
- one always-on shadow worker;
- one always-on exactly-once email notifier.

The current hosted resource status is recorded in [`DEPLOYMENT.md`](DEPLOYMENT.md).

All compute uses the smallest paid always-on plan. Render will display current pricing
before Blueprint confirmation.

## Deploy

1. Open <https://dashboard.render.com/blueprints>.
2. Connect `MichaelAbateTSU/TradeAgent`.
3. Select the root `render.yaml`.
4. Enter the requested secrets:
   - `ALPACA_KEY_ID`
   - `ALPACA_SECRET_KEY`
   - `EMAIL_API_KEY`
   - `EMAIL_SENDER`
   - `EMAIL_RECIPIENT`
5. Review the estimated monthly cost and apply the Blueprint.

Render injects the private PostgreSQL connection string. Each worker runs one instance,
uses a database lock, restarts automatically, runs migrations before deploy, and remains
alive while the laptop is off.

## Initial verification

- `tradeagent-dashboard` returns `/health` with `mode: paper`.
- `tradeagent-shadow-worker` logs successful authentication and recurring heartbeats.
- `tradeagent-notifier` remains healthy with an empty outbox.
- PostgreSQL contains `alembic_version`, `heartbeats`, and `worker_locks`.
- The durable kill switch remains active.

The deployed worker is shadow-only and cannot submit orders.
