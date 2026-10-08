# Render agent diagnosis without restarting failed studies

The October 8, 2026, read-only inspection found the three intended active
services deployed and healthy: dashboard, event/observation worker and
notifier. Their current live source remains the pinned `94e80ec` release.
The original recorder is explicitly suspended in Render. Its old database
heartbeat is historical evidence, not a current process crash.

The observation worker's fresh ownership heartbeat and `paused_invalid`
state are compatible: the process is alive while the failed experiment's
market feed remains stopped. Fresh heartbeats do not imply quote coverage,
model support or trading authorization. Do not clear its safety stop or
restart a failed immutable confirmation to make a status indicator green.

## Corrected operator tooling

The historical `infra.render.observe_release` acceptance contract intentionally
requires all original recorder/news/feed roles and durable progress. It is
retained for that deployment profile; its failures must not be reinterpreted
as a passing historical acceptance test.

The new, separate GET-only diagnostic correlates current hosting state,
latest deployment, dashboard readiness, role heartbeats, ownership leases
and preserved experiment safety:

```powershell
python -m infra.render.agent_health `
  --credential-scope user `
  --output .\research\results\render-agent-health.json
```

`--credential-scope user` explicitly selects the Windows user-scoped
`RENDER_API_KEY`, avoiding a stale key inherited by a long-lived process.
It reads but does not replace, rotate or save credentials. On other systems,
use `--credential-scope process`, the default, with the existing environment
or authenticated Render CLI configuration. Missing user-scope credentials
do not silently fall back to a different identity.

The output exports only selected hosting/deployment fields and never
environment variables, API keys or request headers. API failures are
surfaced; unavailable or unowned active agents produce a failing diagnostic.
Suspension is accepted only from current Render hosting evidence, not
inferred from a stale heartbeat.

The distinction is explicit:

| Observation | Meaning |
|---|---|
| Live deploy plus fresh matching heartbeat/lease | Process is owned and running |
| Exact failed-study pause with expired authorization and stopped feed | Preserved containment, not process outage |
| Hosting explicitly suspended | No running process expected |
| Active hosting with stale/missing/mismatched ownership | Operational failure requiring investigation |
| Operational diagnostic passes | Not a market-quality, profitability or trading-permission pass |

The diagnostic performs no deploys, restarts, schema installs, broker
submissions or database writes. Existing study protocols, sources, claims
and labels remain unchanged.

The [October 8 validation record](releases/2026-10-08-operations-economics.json)
records the live readback, preserved source pins and completed checks.

## Bounded crypto access discovery

When the exact pinned v1 owner, source, account and stopped connection are
still preserved, a separate diagnostic can check documented native
locations without restarting the worker:

```powershell
python -m infra.render.crypto_access_probe `
  --output .\research\results\crypto-access-discovery.json
```

This uses only paper-account GET checks and market-data authentication/
subscription messages. It opens one diagnostic socket at a time for
`us-1`, `eu-1` and `us`, with an eight-second receive bound, capture limits
and safety/identity checks before and after. Only its own sockets close.
It exports received native payloads and clocks, never sent credentials.
Changed ownership, source, account, feed or trading containment fails
closed. It does not submit orders, evaluate labels or create a study.

The [venue-and-cost investigation](experiments/2026-10-venue-cost-readiness/README.md)
established current individual access to all three locations without
406 on October 8. That is not proof of simultaneous quotas, the account's
paper-fill reference, source quality or an upgrade entitlement.

## Research remains separate

The fixed Kraken confirmation closed **NO-GO**, with BTC at 17.81% and
ETH at 18.09% over the original 25,920 mature outcomes per symbol.
Most missing outcomes followed process loss; they cannot be labeled as
upstream quote inactivity. The later bounded component repair did not
retroactively complete that study.

The next economic/model step must retain the source, venue, account
eligibility, fee provenance and valid-label gates. No model may be called
profitable or promoted using the failed observation corpus, unverified
personal fees or an unrelated venue's fresher quotes.
