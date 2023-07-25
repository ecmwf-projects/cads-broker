"""Add Adaptor configuration table

Revision ID: e09564dc7652
Revises: cc6cc1cb3529
Create Date: 2023-07-24 10:41:11.679876

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


# revision identifiers, used by Alembic.
revision = 'e09564dc7652'
down_revision = 'cc6cc1cb3529'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "adaptor_metadata",
        sa.Column("hash", sa.Text, primary_key=True),
        sa.Column("config", JSONB),
        sa.Column("form", JSONB),
    )
    op.add_column(
        "system_requests",
        sa.Column("adaptor_metadata_hash", sa.Text, sa.ForeignKey("adaptor_metadata.hash")),
    )
    op.add_column(
        "system_requests",
        sa.Column("entry_point", sa.Text),
    )
    op.execute(
        "update system_requests set entry_point=request_body['entry_point']"
    )
    op.execute(
        "insert into adaptor_metadata (hash, config, form) values ('098f6bcd4621d373cade4e832627b4f6', '{}', '{}')"
    )
    op.execute(
        "update system_requests set adaptor_metadata_hash='098f6bcd4621d373cade4e832627b4f6'"
    )


def downgrade() -> None:
    op.execute(
        "update system_requests set request_body['entry_point']=to_jsonb(\"entry_point\")"
    )
    op.drop_column("system_requests", "entry_point")
    op.drop_column("system_requests", "adaptor_metadata_hash")
    op.drop_table("adaptor_metadata")

