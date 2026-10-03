"""A single isolated append-only public Kraken confirmation journal.

Do not apply this to the global revision while the old 0015 observer is unchanged.
Use tradeagent.kraken_confirmation.install_schema instead: it creates only the
research tables and records the same revision in the auxiliary version table.
This standard migration remains for a future coordinated global schema upgrade.
"""

from alembic import op
from sqlalchemy import func, select

from tradeagent.kraken_confirmation import confirmation_evidence

revision = "0016_kraken_confirmation"
down_revision = "0015_shadow_research_dataset"
branch_labels = None
depends_on = None


def upgrade() -> None:
    confirmation_evidence.create(bind=op.get_bind(), checkfirst=True)


def downgrade() -> None:
    connection = op.get_bind()
    if connection.scalar(select(func.count()).select_from(confirmation_evidence)):
        raise ValueError("cannot discard populated public confirmation evidence")
    confirmation_evidence.drop(bind=connection, checkfirst=True)
