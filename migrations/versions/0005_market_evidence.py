"""Add feed provenance and trade-level market evidence."""

import sqlalchemy as sa
from alembic import op

revision = "0005_market_evidence"
down_revision = "0004_market_news"
branch_labels = None
depends_on = None


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    bar_columns = {column["name"] for column in inspector.get_columns("market_bars")}
    if "feed_source" not in bar_columns:
        with op.batch_alter_table("market_bars") as batch:
            batch.add_column(
                sa.Column("feed_source", sa.String(length=20), nullable=False, server_default="iex")
            )
    quote_columns = {column["name"] for column in inspector.get_columns("market_quotes")}
    if "feed_source" not in quote_columns:
        with op.batch_alter_table("market_quotes") as batch:
            batch.add_column(
                sa.Column("feed_source", sa.String(length=20), nullable=False, server_default="iex")
            )
            batch.add_column(
                sa.Column("bid_exchange", sa.String(length=20), nullable=False, server_default="")
            )
            batch.add_column(
                sa.Column("ask_exchange", sa.String(length=20), nullable=False, server_default="")
            )
            batch.drop_constraint("uq_market_quote_event_prices", type_="unique")
            batch.create_unique_constraint(
                "uq_market_quote_event_prices",
                ["symbol", "event_at", "bid_price", "ask_price", "bid_size", "ask_size"],
            )
    if not inspector.has_table("market_trades"):
        op.create_table(
            "market_trades",
            sa.Column("market_trade_id", sa.String(length=36), nullable=False),
            sa.Column("provider_trade_id", sa.String(length=128), nullable=False),
            sa.Column("symbol", sa.String(length=32), nullable=False),
            sa.Column("feed_source", sa.String(length=20), nullable=False),
            sa.Column("exchange", sa.String(length=20), nullable=False),
            sa.Column("event_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("processed_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("price", sa.Numeric(precision=28, scale=12), nullable=False),
            sa.Column("size", sa.Numeric(precision=28, scale=12), nullable=False),
            sa.Column("conditions", sa.JSON(), nullable=False),
            sa.Column("tape", sa.String(length=10), nullable=True),
            sa.PrimaryKeyConstraint("market_trade_id"),
            sa.UniqueConstraint(
                "symbol",
                "feed_source",
                "provider_trade_id",
                name="uq_market_trade_provider_id",
            ),
        )
        op.create_index(
            "ix_market_trades_symbol_time",
            "market_trades",
            ["symbol", "event_at"],
            unique=False,
        )


def downgrade() -> None:
    op.drop_index("ix_market_trades_symbol_time", table_name="market_trades")
    op.drop_table("market_trades")
    with op.batch_alter_table("market_quotes") as batch:
        batch.drop_constraint("uq_market_quote_event_prices", type_="unique")
        batch.create_unique_constraint(
            "uq_market_quote_event_prices",
            ["symbol", "event_at", "bid_price", "ask_price"],
        )
        batch.drop_column("ask_exchange")
        batch.drop_column("bid_exchange")
        batch.drop_column("feed_source")
    with op.batch_alter_table("market_bars") as batch:
        batch.drop_column("feed_source")
