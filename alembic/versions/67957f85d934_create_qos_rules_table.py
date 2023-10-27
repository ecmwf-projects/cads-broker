"""create qos_rules table

Revision ID: 67957f85d934
Revises: 01374a5b3c41
Create Date: 2023-10-27 12:04:01.741917

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


# revision identifiers, used by Alembic.
revision = '67957f85d934'
down_revision = '01374a5b3c41'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "qos_rules",
        sa.Column("id", sa.Text, primary_key=True),
        sa.Column("rule", sa.Text),
        sa.Column("condition", sa.Text),
        sa.Column("conclusion", sa.Text),
        sa.Column("info", sa.Text),
        sa.Column("timestamp", sa.TIMESTAMP, default=sa.func.now()),
    )
    op.drop_column("system_requests", "qos_status")
    op.add_column(
        "system_requests",
        sa.Column(
            "qos_status_ids", sa.dialects.postgresql.ARRAY(sa.Text), default="{}"
        ),
    )
    op.execute("update system_requests set qos_status_ids=[]")


def downgrade() -> None:
    op.drop_column("system_requests", "qos_status_ids")
    op.add_column("system_requests", sa.Column("qos_status", JSONB, default={}))
    op.execute("UPDATE system_requests SET qos_status='{}'")
    op.drop_table("qos_rules")
