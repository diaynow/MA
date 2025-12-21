"""Add article status and preview_url

Revision ID: 005_status_preview
Revises: 004_remove_users_and_fk
Create Date: 2025-01-20 15:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '005_status_preview'
down_revision = '004_remove_users_and_fk'
branch_labels = None
depends_on = None


def upgrade():
    # Add status column with default value DRAFT
    op.add_column('articles', sa.Column('status', sa.String(32), nullable=False, server_default='DRAFT'))
    op.add_column('articles', sa.Column('preview_url', sa.String(500), nullable=True))
    
    # Create index on status for faster queries
    op.create_index('ix_articles_status', 'articles', ['status'])


def downgrade():
    # Drop index
    op.drop_index('ix_articles_status', table_name='articles')
    
    # Drop columns
    op.drop_column('articles', 'preview_url')
    op.drop_column('articles', 'status')

