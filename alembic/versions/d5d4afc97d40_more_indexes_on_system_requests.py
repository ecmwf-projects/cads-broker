"""more indexes on system requests.

Revision ID: d5d4afc97d40
Revises: ca178571bdc5
Create Date: 2024-06-13 12:36:47.252394

"""

import alembic

# revision identifiers, used by Alembic.
revision = "d5d4afc97d40"
down_revision = "ca178571bdc5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.create_index("idx_system_requests_status", "system_requests", ["status"])
    alembic.op.create_index(
        "idx_system_requests_created_at", "system_requests", ["created_at"]
    )
    alembic.op.create_index(
        "idx_system_requests_finished_at", "system_requests", ["finished_at"]
    )


def downgrade() -> None:
    alembic.op.drop_index("idx_system_requests_status", if_exists=True)
    alembic.op.drop_index("idx_system_requests_created_at", if_exists=True)
    alembic.op.drop_index("idx_system_requests_finished_at", if_exists=True)
    alembic.op.drop_index("ix_system_requests_status", if_exists=True)
    alembic.op.drop_index("ix_system_requests_created_at", if_exists=True)
    alembic.op.drop_index("ix_system_requests_finished_at", if_exists=True)
