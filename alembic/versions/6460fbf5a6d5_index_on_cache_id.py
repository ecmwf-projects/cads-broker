"""index on cache_id.

Revision ID: 6460fbf5a6d5
Revises: 01374a5b3c41
Create Date: 2023-11-24 14:04:45.930916

"""

import alembic

# revision identifiers, used by Alembic.
revision = "6460fbf5a6d5"
down_revision = "01374a5b3c41"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.create_index(
        "idx_system_requests_cache_id", "system_requests", ["cache_id"]
    )


def downgrade() -> None:
    alembic.op.drop_index("idx_system_requests_cache_id")
