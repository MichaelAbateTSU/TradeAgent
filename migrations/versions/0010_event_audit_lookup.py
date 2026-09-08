"""Index cohort and idempotent audit reads without rewriting retained evidence."""

import sqlalchemy as sa
from alembic import op

revision = "0010_event_audit_lookup"
down_revision = "0009_candidate_states"
branch_labels = None
depends_on = None

INDEX_NAME = "ix_events_v2_trace_type_time"


def upgrade() -> None:
    connection = op.get_bind()
    if any(
        index["name"] == INDEX_NAME for index in sa.inspect(connection).get_indexes("events_v2")
    ):
        if connection.dialect.name == "postgresql":
            valid = connection.scalar(
                sa.text(
                    "SELECT i.indisvalid FROM pg_index i "
                    "JOIN pg_class c ON c.oid=i.indexrelid "
                    "WHERE c.oid=to_regclass(:name)"
                ),
                {"name": INDEX_NAME},
            )
            if not valid:
                raise RuntimeError("Audit lookup index is invalid; rebuild it before upgrading")
        return
    if connection.dialect.name == "postgresql":
        with op.get_context().autocommit_block():
            op.create_index(
                INDEX_NAME,
                "events_v2",
                ["trace_id", "event_type", "occurred_at"],
                postgresql_concurrently=True,
                postgresql_ops={"trace_id": "varchar_pattern_ops"},
            )
    else:
        op.create_index(INDEX_NAME, "events_v2", ["trace_id", "event_type", "occurred_at"])


def downgrade() -> None:
    connection = op.get_bind()
    if connection.dialect.name == "postgresql":
        with op.get_context().autocommit_block():
            op.drop_index(INDEX_NAME, table_name="events_v2", postgresql_concurrently=True)
    else:
        op.drop_index(INDEX_NAME, table_name="events_v2")
