"""add rejected status to status enum

Revision ID: a52001eed6e1
Revises: a4e8be715296
Create Date: 2025-06-16 14:19:35.841247

"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "a52001eed6e1"
down_revision = "a4e8be715296"
branch_labels = None
depends_on = None


old_status_enum = sa.Enum(
    "accepted",
    "running",
    "failed",
    "successful",
    "dismissed",
    "deleted",
    name="status",
    create_type=False,
)
new_status_enum = sa.Enum(
    "accepted",
    "running",
    "failed",
    "successful",
    "dismissed",
    "deleted",
    "rejected",
    name="status",
    create_type=False,
)


def upgrade() -> None:
    # Add the new status to the enum
    op.execute("ALTER TYPE status ADD VALUE 'rejected'")


def downgrade() -> None:
    # Remove the new status from the enum
    # this doesn't work
    # op.execute("ALTER TYPE status DELETE VALUE 'rejected'")
    op.execute(
        "CREATE TYPE status_old AS ENUM ('accepted','running','failed','successful','dismissed', 'deleted')"
    )
    op.execute("DELETE FROM system_requests where status='rejected'")
    op.execute(
        "ALTER TABLE system_requests ALTER COLUMN status TYPE status_old USING (status::text::status_old)"
    )
    op.execute("DROP TYPE status")
    op.execute("ALTER TYPE status_old RENAME TO status")
