"""Add a compact, versioned read model without rewriting original event evidence."""

import sqlalchemy as sa
from alembic import op

revision = "0011_reporting_metadata"
down_revision = "0010_event_audit_lookup"
branch_labels = None
depends_on = None


def upgrade() -> None:
    connection = op.get_bind()
    inspector = sa.inspect(connection)
    if inspector.has_table("event_reporting_metadata"):
        columns = {column["name"] for column in inspector.get_columns("event_reporting_metadata")}
        primary = inspector.get_pk_constraint("event_reporting_metadata")["constrained_columns"]
        if columns != {"event_id", "projection_version", "payload"} or set(primary) != {
            "event_id",
            "projection_version",
        }:
            raise RuntimeError("Existing reporting metadata table has an incompatible schema")
        return
    op.create_table(
        "event_reporting_metadata",
        sa.Column("event_id", sa.String(36), sa.ForeignKey("events_v2.event_id"), primary_key=True),
        sa.Column("projection_version", sa.Integer, primary_key=True),
        sa.Column("payload", sa.JSON, nullable=False),
    )


def downgrade() -> None:
    op.drop_table("event_reporting_metadata")
