"""add deleted as new status.

Revision ID: a4e8be715296
Revises: d5d4afc97d40
Create Date: 2024-07-25 13:13:11.955119

"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "a4e8be715296"
down_revision = "d5d4afc97d40"
branch_labels = None
depends_on = None


old_status_enum = sa.Enum(
    "accepted",
    "running",
    "failed",
    "successful",
    "dismissed",
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
    name="status",
    create_type=False,
)


def upgrade() -> None:
    # Add the new status to the enum
    op.execute("ALTER TYPE status ADD VALUE 'deleted'")


def downgrade() -> None:
    # Remove the new status from the enum
    # this doesn't work
    #op.execute("ALTER TYPE status DELETE VALUE 'deleted'")
    op.execute("CREATE TYPE status_old AS ENUM ('accepted','running','failed','successful','dismissed')")
    op.execute("DELETE FROM system_requests where status='deleted'")
    op.execute("ALTER TABLE system_requests ALTER COLUMN status TYPE status_old USING (status::text::status_old)")
    op.execute("DROP TYPE status")
    op.execute("ALTER TYPE status_old RENAME TO status")