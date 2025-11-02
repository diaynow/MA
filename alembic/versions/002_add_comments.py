"""Add comments table

Revision ID: 002_add_comments
Revises: 001_add_users
Create Date: 2025-09-25 14:30:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '002_add_comments'
down_revision = '001_add_users'
branch_labels = None
depends_on = None


def upgrade():
    # Create comments table
    op.create_table('comments',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('body', sa.Text(), nullable=False),
        sa.Column('article_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('author_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Create indexes
    op.create_index(op.f('ix_comments_article_id'), 'comments', ['article_id'], unique=False)
    op.create_index(op.f('ix_comments_author_id'), 'comments', ['author_id'], unique=False)


def downgrade():
    # Drop indexes
    op.drop_index(op.f('ix_comments_author_id'), table_name='comments')
    op.drop_index(op.f('ix_comments_article_id'), table_name='comments')
    
    # Drop table
    op.drop_table('comments')
