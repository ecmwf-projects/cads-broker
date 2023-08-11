"""Add response_log column

Revision ID: ba380b5614b8
Revises: e09564dc7652
Create Date: 2023-08-03 14:10:38.083255

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


# revision identifiers, used by Alembic.
revision = 'ba380b5614b8'
down_revision = 'e09564dc7652'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("system_requests", sa.Column("response_log", JSONB,  default=[]))
    op.execute("UPDATE system_requests SET response_log='[]'")


def downgrade() -> None:
    op.drop_column("system_requests", "response_log")
