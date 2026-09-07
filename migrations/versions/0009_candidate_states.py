"""Track deferred candidates without overwriting their first observed decision."""

import sqlalchemy as sa
from alembic import op

from tradeagent.event_store import event_candidate_states

revision = "0009_candidate_states"
down_revision = "0008_control_values"
branch_labels = None
depends_on = None


def upgrade() -> None:
    event_candidate_states.create(bind=op.get_bind(), checkfirst=True)


def downgrade() -> None:
    connection = op.get_bind()
    if connection.scalar(sa.select(sa.func.count()).select_from(event_candidate_states)):
        raise ValueError("Cannot remove candidate history; archive it explicitly")
    event_candidate_states.drop(bind=connection)
