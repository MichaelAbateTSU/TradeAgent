"""Add immutable strategy promotion records."""

from alembic import op

from tradeagent.persistence import strategy_promotions

revision = "0003_strategy_promotions"
down_revision = "0002_normalized_market_data"
branch_labels = None
depends_on = None


def upgrade() -> None:
    strategy_promotions.create(bind=op.get_bind(), checkfirst=True)


def downgrade() -> None:
    strategy_promotions.drop(bind=op.get_bind(), checkfirst=True)
