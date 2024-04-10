"""cascading delete on events.

Revision ID: 6ee20703d353
Revises: 8924bc485ad5
Create Date: 2024-03-28 12:07:05.247016

"""

import datetime

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "6ee20703d353"
down_revision = "8924bc485ad5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("system_requests", "request_id")
    op.create_primary_key("system_requests_pkey", "system_requests", ["request_uid"])
    op.drop_constraint("events_request_uid_fkey", "events")
    op.drop_index("ix_system_requests_request_uid", "system_requests_pkey")
    op.create_foreign_key(
        "events_request_uid_fkey",
        "events",
        "system_requests",
        ["request_uid"],
        ["request_uid"],
        ondelete="CASCADE",
    )
    op.add_column(
        "adaptor_properties",
        sa.Column("timestamp", sa.TIMESTAMP, default=sa.func.now()),
    )
    now_str = datetime.datetime.now().isoformat()
    op.execute(
        f"update adaptor_properties set timestamp='{now_str}' where timestamp is null"
    )


def downgrade() -> None:
    # do only on empty table
    op.drop_constraint("events_request_uid_fkey", "events")
    op.drop_constraint("system_requests_pkey", "system_requests")
    op.add_column(
        "system_requests", sa.Column("request_id", sa.Integer, primary_key=True)
    )
    op.create_foreign_key(
        "events_request_uid_fkey",
        "events",
        "system_requests",
        ["request_uid"],
        ["request_uid"],
    )
