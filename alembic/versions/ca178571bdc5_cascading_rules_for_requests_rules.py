"""cascading rules for requests-rules.

Revision ID: ca178571bdc5
Revises: 8deb52d20c05
Create Date: 2024-06-10 17:36:06.551396

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "ca178571bdc5"
down_revision = "8deb52d20c05"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint(
        "system_request_qos_rule_rule_uid_fkey", "system_request_qos_rule"
    )
    op.drop_constraint(
        "system_request_qos_rule_request_uid_fkey", "system_request_qos_rule"
    )
    op.create_foreign_key(
        "system_request_qos_rule_rule_uid_fkey",
        "system_request_qos_rule",
        "qos_rules",
        ["rule_uid"],
        ["uid"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "system_request_qos_rule_request_uid_fkey",
        "system_request_qos_rule",
        "system_requests",
        ["request_uid"],
        ["request_uid"],
        ondelete="CASCADE",
    )


def downgrade() -> None:
    op.drop_constraint(
        "system_request_qos_rule_rule_uid_fkey", "system_request_qos_rule"
    )
    op.drop_constraint(
        "system_request_qos_rule_request_uid_fkey", "system_request_qos_rule"
    )
    op.create_foreign_key(
        "system_request_qos_rule_rule_uid_fkey",
        "system_request_qos_rule",
        "qos_rules",
        ["rule_uid"],
        ["uid"],
    )
    op.create_foreign_key(
        "system_request_qos_rule_request_uid_fkey",
        "system_request_qos_rule",
        "system_requests",
        ["request_uid"],
        ["request_uid"],
    )
