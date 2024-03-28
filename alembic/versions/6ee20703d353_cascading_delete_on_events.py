"""cascading delete on events

Revision ID: 6ee20703d353
Revises: 8924bc485ad5
Create Date: 2024-03-28 12:07:05.247016

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = '6ee20703d353'
down_revision = '8924bc485ad5'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint('events_request_uid_fkey', 'events')
    op.create_foreign_key(
        "events_request_uid_fkey", "events", "system_requests",
        ["request_uid"], ["request_uid"], ondelete="CASCADE"
    )


def downgrade() -> None:
    op.drop_constraint('events_request_uid_fkey', 'events')
    op.create_foreign_key(
        "events_request_uid_fkey", "events", "system_requests",
        ["request_uid"], ["request_uid"],
    )
