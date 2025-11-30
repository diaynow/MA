"""Remove users table and foreign keys

Revision ID: 004_remove_users_and_fk
Revises: 003_add_foreign_keys
Create Date: 2025-01-20 12:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '004_remove_users_and_fk'
down_revision = '003_add_foreign_keys'
branch_labels = None
depends_on = None


def upgrade():
    # Drop foreign key constraints to users (cross-service FK removed)
    # Note: We keep FK to articles as they are in the same database
    op.drop_constraint('fk_comments_author_id', 'comments', type_='foreignkey')
    op.drop_constraint('fk_articles_author_id', 'articles', type_='foreignkey')
    
    # Note: We keep author_id columns but without foreign keys to users
    # This follows the data ownership pattern in microservices
    # FK to articles (fk_comments_article_id) is kept as articles are in the same DB
    
    # Drop users table (users are now in separate users-api service)
    op.drop_table('users')


def downgrade():
    # Recreate users table (simplified version for rollback)
    op.create_table(
        'users',
        sa.Column('id', sa.dialects.postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('email', sa.String(255), nullable=False, unique=True),
        sa.Column('username', sa.String(50), nullable=False, unique=True),
        sa.Column('password_hash', sa.String(255), nullable=False),
        sa.Column('bio', sa.Text(), nullable=True),
        sa.Column('image_url', sa.String(500), nullable=True),
        sa.Column('is_active', sa.Boolean(), default=True),
        sa.Column('created_at', sa.DateTime(), default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(), default=sa.func.now())
    )
    
    # Recreate foreign key constraints
    op.create_foreign_key('fk_articles_author_id', 'articles', 'users', ['author_id'], ['id'])
    op.create_foreign_key('fk_comments_author_id', 'comments', 'users', ['author_id'], ['id'])
    # Note: fk_comments_article_id should already exist if it wasn't dropped

