"""Add normalized point-in-time market data tables."""

from alembic import op

from tradeagent.persistence import market_bars, market_quotes

revision = "0002_normalized_market_data"
down_revision = "0001_initial_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    market_bars.create(bind=op.get_bind(), checkfirst=True)
    market_quotes.create(bind=op.get_bind(), checkfirst=True)


def downgrade() -> None:
    market_quotes.drop(bind=op.get_bind(), checkfirst=True)
    market_bars.drop(bind=op.get_bind(), checkfirst=True)
