"""Preserve complete operational certificates and structured control state."""

import sqlalchemy as sa
from alembic import op

revision = "0008_control_values"
down_revision = "0007_daily_status_email"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("controls_v2") as batch:
        batch.alter_column(
            "control_value",
            existing_type=sa.String(500),
            type_=sa.Text(),
            existing_nullable=False,
        )


def downgrade() -> None:
    oversized = op.get_bind().scalar(
        sa.text("SELECT count(*) FROM controls_v2 WHERE length(control_value) > 500")
    )
    if oversized:
        raise ValueError(
            "Cannot narrow control values while full certificates or other long values exist; "
            "archive them explicitly"
        )
    with op.batch_alter_table("controls_v2") as batch:
        batch.alter_column(
            "control_value",
            existing_type=sa.Text(),
            type_=sa.String(500),
            existing_nullable=False,
        )
