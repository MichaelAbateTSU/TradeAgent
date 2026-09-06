"""Immutable event evidence and isolated experimental paper cohorts."""

from alembic import op

from tradeagent.event_store import (
    event_cluster_claims,
    event_cohorts,
    event_decisions,
    event_evidence,
    event_order_links,
)

revision = "0006_event_experiments"
down_revision = "0005_market_evidence"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for table in (
        event_cohorts,
        event_evidence,
        event_decisions,
        event_order_links,
        event_cluster_claims,
    ):
        table.create(bind=op.get_bind(), checkfirst=True)


def downgrade() -> None:
    for table in (
        event_cluster_claims,
        event_order_links,
        event_decisions,
        event_evidence,
        event_cohorts,
    ):
        table.drop(bind=op.get_bind(), checkfirst=True)
