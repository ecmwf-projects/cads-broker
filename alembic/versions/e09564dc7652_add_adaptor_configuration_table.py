"""Add Adaptor configuration table.

Revision ID: e09564dc7652
Revises: cc6cc1cb3529
Create Date: 2023-07-24 10:41:11.679876

"""

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

# revision identifiers, used by Alembic.
revision = "e09564dc7652"
down_revision = "cc6cc1cb3529"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "adaptor_properties",
        sa.Column("hash", sa.Text, primary_key=True),
        sa.Column("config", JSONB),
        sa.Column("form", JSONB),
    )
    op.add_column(
        "system_requests",
        sa.Column(
            "adaptor_properties_hash", sa.Text, sa.ForeignKey("adaptor_properties.hash")
        ),
    )
    op.add_column(
        "system_requests",
        sa.Column("entry_point", sa.Text),
    )
    op.execute("update system_requests set entry_point=request_body['entry_point']")
    op.execute(
        "update system_requests set request_body['request']=to_jsonb(request_body['kwargs']['request'])"
    )
    op.execute(
        "insert into adaptor_properties (hash, config, form) values "
        "('098f6bcd4621d373cade4e832627b4f6', '{}', '{}')"
    )
    op.execute(
        "update system_requests set adaptor_properties_hash='098f6bcd4621d373cade4e832627b4f6'"
    )


def downgrade() -> None:
    op.execute(
        "update system_requests set request_body['entry_point']=to_jsonb(\"entry_point\")"
    )
    op.execute("update system_requests set request_body['kwargs']=to_jsonb({})")
    op.execute(
        "update system_requests set request_body['kwargs']['request']=to_jsonb(request_body['request'])"
    )
    op.drop_column("system_requests", "entry_point")
    op.drop_column("system_requests", "adaptor_properties_hash")
    op.drop_table("adaptor_properties")
