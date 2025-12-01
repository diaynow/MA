"""Add subscriptions and notification logs

Revision ID: 002_add_subscriptions
Revises: 001_add_users
Create Date: 2025-12-01 12:00:00.000000
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "002_add_subscriptions"
down_revision = "001_add_users"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "users",
        sa.Column("subscription_key", sa.String(length=255), nullable=True),
    )

    op.create_table(
        "subscribers",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("subscriber_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("author_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("NOW()")),
    )
    op.create_index("ix_subscribers_subscriber_id", "subscribers", ["subscriber_id"])
    op.create_index("ix_subscribers_author_id", "subscribers", ["author_id"])
    op.create_unique_constraint(
        "uq_subscriber_author", "subscribers", ["subscriber_id", "author_id"]
    )

    op.create_table(
        "notification_logs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("subscriber_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("author_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("article_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="pending"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("NOW()")),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
    )
    op.create_index(
        "ix_notification_logs_subscriber_id",
        "notification_logs",
        ["subscriber_id"],
    )
    op.create_index("ix_notification_logs_author_id", "notification_logs", ["author_id"])
    op.create_index(
        "ix_notification_logs_article_id",
        "notification_logs",
        ["article_id"],
    )
    op.create_unique_constraint(
        "uq_notification_subscriber_article",
        "notification_logs",
        ["subscriber_id", "article_id"],
    )


def downgrade():
    op.drop_constraint("uq_notification_subscriber_article", "notification_logs", type_="unique")
    op.drop_index("ix_notification_logs_article_id", table_name="notification_logs")
    op.drop_index("ix_notification_logs_author_id", table_name="notification_logs")
    op.drop_index("ix_notification_logs_subscriber_id", table_name="notification_logs")
    op.drop_table("notification_logs")

    op.drop_constraint("uq_subscriber_author", "subscribers", type_="unique")
    op.drop_index("ix_subscribers_author_id", table_name="subscribers")
    op.drop_index("ix_subscribers_subscriber_id", table_name="subscribers")
    op.drop_table("subscribers")

    op.drop_column("users", "subscription_key")

