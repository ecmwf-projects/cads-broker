"""Add response_log column.

Revision ID: ba380b5614b8
Revises: e09564dc7652
Create Date: 2023-08-03 14:10:38.083255

"""

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

# revision identifiers, used by Alembic.
revision = "ba380b5614b8"
down_revision = "e09564dc7652"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("system_requests", sa.Column("response_log", JSONB, default=[]))
    op.execute("UPDATE system_requests SET response_log='[]'")

    op.add_column(
        "system_requests", sa.Column("response_user_visible_log", JSONB, default=[])
    )
    op.execute("UPDATE system_requests SET response_user_visible_log='[]'")


def downgrade() -> None:
    op.drop_column("system_requests", "response_log")
    op.drop_column("system_requests", "response_user_visible_log")
