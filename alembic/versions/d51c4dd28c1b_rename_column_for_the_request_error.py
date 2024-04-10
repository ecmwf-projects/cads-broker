"""rename column for the request error.

Revision ID: d51c4dd28c1b
Revises:
Create Date: 2023-05-12 10:52:11.114619

"""

import sqlalchemy as sa

from alembic import op

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
    op.add_column("system_requests", sa.Column("origin", sa.Text, default="ui"))
    op.execute(
        "UPDATE system_requests SET response_error='{}' WHERE response_error IS NULL"
    )
    op.execute("UPDATE system_requests SET origin='ui'")


def downgrade() -> None:
    op.alter_column(
        "system_requests",
        "response_error",
        server_default=None,
        new_column_name="response_traceback",
    )
    op.drop_column("system_requests", "origin")
