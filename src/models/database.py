from sqlalchemy import create_engine, Column, String, Text, DateTime, ARRAY, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.dialects.postgresql import UUID
import uuid
from datetime import datetime
from src.config import settings

# Database setup
engine = create_engine(settings.database_url)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class Article(Base):
    __tablename__ = "articles"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    title = Column(String(200), nullable=False)
    description = Column(String(500), nullable=False)
    body = Column(Text, nullable=False)
    tag_list = Column(ARRAY(String), nullable=True)
    slug = Column(String(255), unique=True, nullable=False, index=True)
    # user_id without foreign key - data ownership pattern
    author_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    # Status for publication workflow: DRAFT, PENDING_PUBLISH, PUBLISHED, REJECTED, ERROR
    status = Column(String(32), nullable=False, default="DRAFT", index=True)
    # Preview URL for published articles
    preview_url = Column(String(500), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    comments = relationship("Comment", back_populates="article", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Article(id={self.id}, title='{self.title}', slug='{self.slug}', status='{self.status}')>"


class Comment(Base):
    __tablename__ = "comments"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    body = Column(Text, nullable=False)
    # FK to articles is kept as they are in the same database
    article_id = Column(UUID(as_uuid=True), ForeignKey("articles.id"), nullable=False, index=True)
    # user_id without foreign key - data ownership pattern (users are in separate service)
    author_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    article = relationship("Article", back_populates="comments")

    def __repr__(self):
        return f"<Comment(id={self.id}, article_id='{self.article_id}', author_id='{self.author_id}')>"


class ApiKey(Base):
    __tablename__ = "api_keys"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    key = Column(String(255), unique=True, nullable=False, index=True)
    description = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    expires_at = Column(DateTime, nullable=True)
    is_active = Column(String(10), default="active", nullable=False)  # active, revoked

    def __repr__(self):
        return f"<ApiKey(id={self.id}, description='{self.description}')>"


# Dependency to get database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()