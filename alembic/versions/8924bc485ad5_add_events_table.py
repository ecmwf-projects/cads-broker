"""Add events table

Revision ID: 8924bc485ad5
Revises: 6460fbf5a6d5
Create Date: 2024-01-26 14:43:44.421999

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '8924bc485ad5'
down_revision = '6460fbf5a6d5'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "events",
        sa.Column("event_id", sa.Integer, primary_key=True),
        sa.Column("event_type", ),
        sa.Column("message", sa.Text),
        sa.Column("timestamp", sa.TIMESTAMP, default=sa.func.now()),
        sa.Column(
            "request_uid",
            sa.dialects.postgresql.UUID(False),
            sa.ForeignKey("system_requests.request_uid"),
            index=True,
        ),
    )

def downgrade() -> None:
    op.drop_table("events")
