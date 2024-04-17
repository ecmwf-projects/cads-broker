"""add qos_rules table.

Revision ID: 8deb52d20c05
Revises: 6ee20703d353
Create Date: 2024-04-09 17:16:01.559118

"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "8deb52d20c05"
down_revision = "6ee20703d353"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "qos_rules",
        sa.Column("uid", sa.Text, primary_key=True),
        sa.Column("name", sa.Text),
        sa.Column("info", sa.Text),
        sa.Column("condition", sa.Text),
        sa.Column("conclusion", sa.Text),
        sa.Column("conclusion_value", sa.Text),
        sa.Column("queued", sa.Integer),
        sa.Column("running", sa.Integer),
    )
    op.create_table(
        "system_request_qos_rule",
        sa.Column(
            "request_uid",
            sa.dialects.postgresql.UUID(False),
            sa.ForeignKey("system_requests.request_uid"),
            primary_key=True,
        ),
        sa.Column(
            "rule_uid", sa.Text, sa.ForeignKey("qos_rules.uid"), primary_key=True
        ),
    )


def downgrade() -> None:
    op.drop_table("system_request_qos_rule")
    op.drop_table("qos_rules")
