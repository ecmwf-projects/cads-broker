"""add deleted as new status.

Revision ID: a4e8be715296
Revises: d5d4afc97d40
Create Date: 2024-07-25 13:13:11.955119

"""
import sqlalchemy as sa

from sqlalchemy.dialects.postgresql import ENUM

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
    "pending",
    "in_progress",
    "completed",
    "failed",
    "deleted",
    name="status",
    create_type=False,
)


def upgrade() -> None:
    # Add the new status to the enum
    op.alter_column(
        "system_requests",
        "status",
        existing_type=old_status_enum,
        type_=new_status_enum,
        existing_nullable=False,
    )


def downgrade() -> None:
    # Remove the new status from the enum
    op.alter_column(
        "system_requests",
        "status",
        existing_type=new_status_enum,
        type_=old_status_enum,
        existing_nullable=False,
    )
