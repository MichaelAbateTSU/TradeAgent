# Render Pro: use platform features without increasing trading risk

The workspace owner selected the **$25/month Pro workspace plan** on September 9.
The workspace subscription is separate from application/database compute charges.
It does not increase the CPU or memory of the existing instances, buy SIP market
data, establish strategy profitability, or authorize orders.

## Appropriate platform features

| Feature | TradeAgent policy |
| --- | --- |
| Environment protection | Protect the Trade agent Production environment against non-admin destructive operations and secret access. |
| Private-network isolation | Keep the TradeAgent services/database together; block cross-environment private traffic only after dependency checks. Public broker/data/email egress remains necessary. |
| Separate environments | Use an empty isolated Validation environment for future testing; no resource duplication or copied credentials by default. |
| Workspace audit logs | Use the Pro audit API to inspect operational changes. Retention begins with the eligible plan, not retroactively. Never export credentials. |
| Previews | Manual, short-lived, read-only previews only; no broker/email/production database credentials. |
| Performance builds | Do not switch automatically: this tier has no included minutes and affects the entire workspace, including unrelated applications. |
| Horizontal autoscaling | Never enable on order workers or singleton recorders/notifier. Extra replicas do not fix the small database and can create contention. Dashboard scaling needs measured demand and a separate compute budget. |
| Extra services/domains | Availability is not a reason to create unused resources or additional bills. |
| Compliance documents | Render's certifications describe Render, not a certification of TradeAgent or its trading decisions. |

The current **0.1-CPU/256-MiB PostgreSQL instance** remains a distinct compute plan.
September 9 live measurements showed periods near its CPU allocation and a recorder
backlog. Optimize and test the measured bottleneck; if more compute is still needed,
obtain a specific budget rather than implying it is included in Pro.

## Preview configuration

`infra/render/preview.yaml` is a separate, optional read-only Blueprint. It is **not**
the deployment definition for the existing five production resources. It enables
manual previews with three-day expiry, a free base web instance and a single
separately billed Starter preview instance, no persistent disk and no production
service references. Render's validator rejects `free` as a preview plan; no
preview is being created without a compute budget. No API keys, email destinations, or
database connection strings are supplied. The dashboard uses its local/ephemeral
read-only demonstration ledger, not the paper account.

Do not connect the legacy root `render.yaml` over production: its Docker resources
and old dashboard name do not describe the current pinned native-Python deployment.
Preview creation is not part of the current live-market acceptance and is not
evidence that broker integration or trading has passed. Render bills preview
resources under their selected compute plans; do not add workers/databases to a
preview without reviewing isolation, credentials and cost.

## Release discipline

Keep explicit reviewed per-role commits, single-owner leases, active entry pauses
and immutable experiment identities. The mandatory
[post-deployment acceptance](../infra/render/POSTDEPLOY_ACCEPTANCE.md) still applies
after Pro adoption. A protected environment does not protect a service against
changes made through an automatically synchronized Blueprint; keep auto-sync and
auto-deploy off for these frozen production resources.

## Applied September 9

The original read-only acceptance interval finished at **14:06:31 UTC** and
failed. Only afterward, at **14:08:46 UTC**, Production
`evm-daf34jn40ujc739c8530` was made **protected** with **private-network isolation
enabled**. All four services and PostgreSQL remain in that same environment.
Its public inbound rule was not changed; PostgreSQL's separate public allowlist
remains empty.

At **14:09:13 UTC**, an empty, protected, isolated **Validation** environment
`evm-dagmh2e7bikc73br1sm0` was created in the existing Trade agent project. It has
no services, databases, environment groups or copied secrets, and therefore no
additional compute resources. No preview, autoscaler, Performance build setting,
compute upgrade, recipient or unrelated project was changed.

The Pro audit API recorded successful `UpdateEnvironmentIsolatedEvent`,
`ChangeEnvironmentProtectionEvent` and `CreateEnvironmentEvent` entries. The
preserved audit projection excludes token identifiers, actor email and source IP.
The unchanged event image's read-only job `job-dagmh2ad0e5s73d4tth0` subsequently
opened a new database connection successfully and found the pinned paper account
ACTIVE/unblocked, with zero positions and zero open orders. This checks allowed
same-environment connectivity; no cross-environment test service was created.

See [actual platform evidence](../research/results/render-pro-20260909.json) and
[post-isolation broker/database readback](../research/results/render-incident-20260909-post-isolation-probe.json).
These platform improvements do **not** turn the failed incident acceptance into
a pass or authorize paper entries. The separately reviewed recorder correction
also failed its complete 30-minute live acceptance on loss, freshness and
PostgreSQL memory. Application CPU improved, but the 0.1-core database still
reached its allocation. Further bounded ingestion measurements are separate
from the Pro configuration; no paid compute upgrade is being implied or applied.

## Official references

- [Projects, protection and network boundaries](https://render.com/docs/projects)
- [Workspace audit logs](https://render.com/docs/audit-logs)
- [Preview environments and billing](https://render.com/docs/preview-environments)
- [Build pipeline tiers and included minutes](https://render.com/docs/build-pipeline)
- [Pricing](https://render.com/pricing)
