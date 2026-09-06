"""Add normalized point-in-time market data tables."""

import sqlalchemy as sa
from alembic import op

revision = "0002_normalized_market_data"
down_revision = "0001_initial_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    if not inspector.has_table("market_bars"):
        op.create_table(
            "market_bars",
            sa.Column("bar_id", sa.String(length=36), nullable=False),
            sa.Column("symbol", sa.String(length=32), nullable=False),
            sa.Column("timeframe", sa.String(length=20), nullable=False),
            sa.Column("event_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("processed_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("open", sa.Numeric(precision=28, scale=12), nullable=False),
            sa.Column("high", sa.Numeric(precision=28, scale=12), nullable=False),
            sa.Column("low", sa.Numeric(precision=28, scale=12), nullable=False),
            sa.Column("close", sa.Numeric(precision=28, scale=12), nullable=False),
            sa.Column("volume", sa.Numeric(precision=28, scale=12), nullable=False),
            sa.PrimaryKeyConstraint("bar_id"),
            sa.UniqueConstraint(
                "symbol",
                "timeframe",
                "event_at",
                name="uq_market_bar_symbol_timeframe_event",
            ),
        )
        op.create_index(
            "ix_market_bars_symbol_time",
            "market_bars",
            ["symbol", "event_at"],
            unique=False,
        )
    if not inspector.has_table("market_quotes"):
        op.create_table(
            "market_quotes",
            sa.Column("quote_id", sa.String(length=36), nullable=False),
            sa.Column("symbol", sa.String(length=32), nullable=False),
            sa.Column("event_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("processed_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("bid_price", sa.Numeric(precision=28, scale=12), nullable=False),
            sa.Column("ask_price", sa.Numeric(precision=28, scale=12), nullable=False),
            sa.Column("bid_size", sa.Numeric(precision=28, scale=12), nullable=False),
            sa.Column("ask_size", sa.Numeric(precision=28, scale=12), nullable=False),
            sa.PrimaryKeyConstraint("quote_id"),
            sa.UniqueConstraint(
                "symbol",
                "event_at",
                "bid_price",
                "ask_price",
                name="uq_market_quote_event_prices",
            ),
        )
        op.create_index(
            "ix_market_quotes_symbol_time",
            "market_quotes",
            ["symbol", "event_at"],
            unique=False,
        )


def downgrade() -> None:
    op.drop_index("ix_market_quotes_symbol_time", table_name="market_quotes")
    op.drop_table("market_quotes")
    op.drop_index("ix_market_bars_symbol_time", table_name="market_bars")
    op.drop_table("market_bars")
