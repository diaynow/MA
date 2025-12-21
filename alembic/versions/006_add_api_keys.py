"""Add api_keys table for internal service authentication

Revision ID: 006_api_keys
Revises: 005_status_preview
Create Date: 2025-01-20 16:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

# revision identifiers, used by Alembic.
revision = '006_api_keys'
down_revision = '005_status_preview'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'api_keys',
        sa.Column('id', UUID(as_uuid=True), primary_key=True),
        sa.Column('key', sa.String(255), nullable=False, unique=True),
        sa.Column('description', sa.String(255), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('expires_at', sa.DateTime(), nullable=True),
        sa.Column('is_active', sa.String(10), nullable=False, server_default='active'),
    )
    op.create_index('ix_api_keys_key', 'api_keys', ['key'])


def downgrade():
    op.drop_index('ix_api_keys_key', table_name='api_keys')
    op.drop_table('api_keys')

