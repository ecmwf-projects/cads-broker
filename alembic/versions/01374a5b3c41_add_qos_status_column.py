"""Add qos_status column.

Revision ID: 01374a5b3c41
Revises: ba380b5614b8
Create Date: 2023-09-08 12:21:35.601773

"""
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

# revision identifiers, used by Alembic.
revision = "01374a5b3c41"
down_revision = "ba380b5614b8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("system_requests", sa.Column("qos_status", JSONB, default={}))
    op.execute("UPDATE system_requests SET qos_status='{}'")


def downgrade() -> None:
    op.drop_column("system_requests", "qos_status")
