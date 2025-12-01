"""
Separate module for accessing Users DB without importing users_service config.
This avoids Pydantic validation errors when worker imports users_service models.
"""
from sqlalchemy import create_engine, Column, String, Text, DateTime, Boolean, Integer, UniqueConstraint, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.dialects.postgresql import UUID
import uuid
from datetime import datetime

from src.config import settings

# Create engine for Users DB using URL from main config
users_engine = create_engine(settings.users_database_url)
UsersSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=users_engine)
UsersBase = declarative_base()


class User(UsersBase):
    __tablename__ = "users"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email = Column(String(255), unique=True, nullable=False, index=True)
    username = Column(String(50), unique=True, nullable=False, index=True)
    password_hash = Column(String(255), nullable=False)
    bio = Column(Text, nullable=True)
    image_url = Column(String(500), nullable=True)
    subscription_key = Column(String(255), nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class Subscriber(UsersBase):
    __tablename__ = "subscribers"
    __table_args__ = (
        UniqueConstraint("subscriber_id", "author_id", name="uq_subscriber_author"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    subscriber_id = Column(
        UUID(as_uuid=True), ForeignKey("users.id"), nullable=False, index=True
    )
    author_id = Column(
        UUID(as_uuid=True), ForeignKey("users.id"), nullable=False, index=True
    )
    created_at = Column(DateTime, default=datetime.utcnow)

    subscriber = relationship(
        "User", back_populates="subscriptions", foreign_keys=[subscriber_id]
    )


# Configure relationship after both classes are defined
User.subscriptions = relationship(
    Subscriber,
    back_populates="subscriber",
    foreign_keys=[Subscriber.subscriber_id],
    cascade="all, delete-orphan",
)


class NotificationLog(UsersBase):
    __tablename__ = "notification_logs"
    __table_args__ = (
        UniqueConstraint(
            "subscriber_id",
            "article_id",
            name="uq_notification_subscriber_article",
        ),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    subscriber_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    author_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    article_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    status = Column(String(32), default="pending", nullable=False)
    attempts = Column(Integer, default=0, nullable=False)
    last_error = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


# Session factory
def get_users_session():
    """Get a new Users DB session."""
    return UsersSessionLocal()

