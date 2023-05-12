"""rename column for the request error.

Revision ID: d51c4dd28c1b
Revises:
Create Date: 2023-05-12 10:52:11.114619

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "d51c4dd28c1b"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "system_requests",
        "response_traceback",
        server_default=sa.text("'{}'"),
        new_column_name="response_error",
    )
    op.execute(
        "UPDATE system_requests SET response_error='{}' WHERE response_error IS NULL"
    )


def downgrade() -> None:
    op.alter_column(
        "system_requests",
        "response_error",
        server_default=None,
        new_column_name="response_traceback",
    )
