"""Add portal field.

Revision ID: cc6cc1cb3529
Revises: d51c4dd28c1b
Create Date: 2023-06-14 14:00:25.914273

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "cc6cc1cb3529"
down_revision = "d51c4dd28c1b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("system_requests", sa.Column("portal", sa.Text))
    op.execute("UPDATE system_requests SET portal='c3s'")


def downgrade() -> None:
    op.drop_column("system_requests", "portal")
