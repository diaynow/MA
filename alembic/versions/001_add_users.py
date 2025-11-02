"""Add users table and update articles with author_id

Revision ID: 001_add_users
Revises: 
Create Date: 2024-01-01 12:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '001_add_users'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create users table
    op.create_table('users',
    sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
    sa.Column('email', sa.String(length=255), nullable=False),
    sa.Column('username', sa.String(length=50), nullable=False),
    sa.Column('password_hash', sa.String(length=255), nullable=False),
    sa.Column('bio', sa.Text(), nullable=True),
    sa.Column('image_url', sa.String(length=500), nullable=True),
    sa.Column('is_active', sa.Boolean(), nullable=True),
    sa.Column('created_at', sa.DateTime(), nullable=True),
    sa.Column('updated_at', sa.DateTime(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_users_email'), 'users', ['email'], unique=True)
    op.create_index(op.f('ix_users_username'), 'users', ['username'], unique=True)

    # Create articles table
    op.create_table('articles',
    sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
    sa.Column('title', sa.String(length=200), nullable=False),
    sa.Column('description', sa.String(length=500), nullable=False),
    sa.Column('body', sa.Text(), nullable=False),
    sa.Column('tag_list', postgresql.ARRAY(sa.String()), nullable=True),
    sa.Column('slug', sa.String(length=255), nullable=False),
    sa.Column('author_id', postgresql.UUID(as_uuid=True), nullable=False),
    sa.Column('created_at', sa.DateTime(), nullable=True),
    sa.Column('updated_at', sa.DateTime(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_articles_author_id'), 'articles', ['author_id'], unique=False)
    op.create_index(op.f('ix_articles_slug'), 'articles', ['slug'], unique=True)


def downgrade() -> None:
    # Drop articles table
    op.drop_index(op.f('ix_articles_slug'), table_name='articles')
    op.drop_index(op.f('ix_articles_author_id'), table_name='articles')
    op.drop_table('articles')
    
    # Drop users table
    op.drop_index(op.f('ix_users_username'), table_name='users')
    op.drop_index(op.f('ix_users_email'), table_name='users')
    op.drop_table('users')
