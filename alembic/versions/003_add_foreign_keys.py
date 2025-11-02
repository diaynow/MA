"""Add foreign key relationships

Revision ID: 003_add_foreign_keys
Revises: 002_add_comments
Create Date: 2025-09-25 14:45:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '003_add_foreign_keys'
down_revision = '002_add_comments'
branch_labels = None
depends_on = None


def upgrade():
    # Add foreign key constraints
    op.create_foreign_key('fk_articles_author_id', 'articles', 'users', ['author_id'], ['id'])
    op.create_foreign_key('fk_comments_article_id', 'comments', 'articles', ['article_id'], ['id'])
    op.create_foreign_key('fk_comments_author_id', 'comments', 'users', ['author_id'], ['id'])


def downgrade():
    # Drop foreign key constraints
    op.drop_constraint('fk_comments_author_id', 'comments', type_='foreignkey')
    op.drop_constraint('fk_comments_article_id', 'comments', type_='foreignkey')
    op.drop_constraint('fk_articles_author_id', 'articles', type_='foreignkey')

