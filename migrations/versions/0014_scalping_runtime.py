"""Isolated v30 paper scalping runs, cycles, orders, activities and compressed market batches."""

from alembic import op

from tradeagent.scalping_store import SCALPING_TABLES

revision = "0014_scalping_runtime"
down_revision = "0013_trade_event_identity"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for table in SCALPING_TABLES:
        table.create(bind=op.get_bind(), checkfirst=True)


def downgrade() -> None:
    for table in reversed(SCALPING_TABLES):
        table.drop(bind=op.get_bind(), checkfirst=True)
