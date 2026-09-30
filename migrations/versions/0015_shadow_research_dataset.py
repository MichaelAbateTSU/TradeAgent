"""Immutable observation-only research protocol, evaluations, labels and manifests."""

from alembic import op
from sqlalchemy import func, select

from tradeagent.shadow_dataset import DATASET_TABLES

revision = "0015_shadow_research_dataset"
down_revision = "0014_scalping_runtime"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for table in DATASET_TABLES:
        table.create(bind=op.get_bind(), checkfirst=True)


def downgrade() -> None:
    connection = op.get_bind()
    for table in DATASET_TABLES:
        if connection.scalar(select(func.count()).select_from(table)):
            raise ValueError("cannot discard populated prospective research evidence")
    for table in reversed(DATASET_TABLES):
        table.drop(bind=connection, checkfirst=True)
