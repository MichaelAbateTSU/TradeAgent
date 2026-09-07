"""Allow daily status mail in the existing outbox and recover interrupted claims."""

import sqlalchemy as sa
from alembic import op

revision = "0007_daily_status_email"
down_revision = "0006_event_experiments"
branch_labels = None
depends_on = None


def upgrade() -> None:
    columns = {
        column["name"]: column
        for column in sa.inspect(op.get_bind()).get_columns("notification_outbox")
    }
    with op.batch_alter_table("notification_outbox") as batch:
        if not columns["cycle_id"]["nullable"]:
            batch.alter_column("cycle_id", existing_type=sa.String(36), nullable=True)
        if "claimed_at" not in columns:
            batch.add_column(sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    # Preserve sent/status history rather than silently deleting non-trade notifications.
    remaining = op.get_bind().scalar(
        sa.text("SELECT count(*) FROM notification_outbox WHERE cycle_id IS NULL")
    )
    if remaining:
        raise ValueError(
            "Cannot downgrade while daily status history exists; archive it explicitly"
        )
    with op.batch_alter_table("notification_outbox") as batch:
        batch.drop_column("claimed_at")
        batch.alter_column("cycle_id", existing_type=sa.String(36), nullable=False)
