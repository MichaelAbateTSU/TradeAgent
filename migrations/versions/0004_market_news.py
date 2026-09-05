"""Add point-in-time market news provenance."""

from alembic import op

from tradeagent.persistence import market_news

revision = "0004_market_news"
down_revision = "0003_strategy_promotions"
branch_labels = None
depends_on = None


def upgrade() -> None:
    market_news.create(bind=op.get_bind(), checkfirst=True)


def downgrade() -> None:
    market_news.drop(bind=op.get_bind(), checkfirst=True)
