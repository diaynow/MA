import logging
from datetime import datetime
from typing import Optional
from uuid import UUID

import requests
from requests import HTTPError, RequestException
from sqlalchemy.orm import Session

from src.config import settings
from src.models.database import Article, SessionLocal as BackendSession
from src.tasks.celery_app import celery_app
from src.tasks.users_db import (
    NotificationLog,
    Subscriber,
    User,
    get_users_session,
)

logger = logging.getLogger(__name__)


def _format_message(author_id: UUID, article: Article) -> str:
    title = (article.title or "").strip() or "пост"
    return (
        f"Пользователь {author_id} выпустил новый пост: {title[:10]}..."
    )


def _get_subscribers(session: Session, author_id: UUID):
    return (
        session.query(Subscriber.subscriber_id, User.subscription_key)
        .join(User, User.id == Subscriber.subscriber_id)
        .filter(Subscriber.author_id == author_id)
        .all()
    )


def _get_or_create_notification_log(
    session: Session, *, subscriber_id: UUID, author_id: UUID, article_id: UUID
) -> Optional[NotificationLog]:
    log = (
        session.query(NotificationLog)
        .filter(
            NotificationLog.subscriber_id == subscriber_id,
            NotificationLog.article_id == article_id,
        )
        .one_or_none()
    )
    if log and log.status == "sent":
        return None
    if not log:
        log = NotificationLog(
            subscriber_id=subscriber_id,
            author_id=author_id,
            article_id=article_id,
            status="pending",
            attempts=0,
        )
        session.add(log)
    else:
        log.status = "pending"
    log.attempts = (log.attempts or 0) + 1
    log.last_error = None
    log.updated_at = datetime.utcnow()
    session.commit()
    session.refresh(log)
    return log


@celery_app.task(
    name="src.tasks.notifications.notify_followers",
    bind=True,
    max_retries=5,
    default_retry_delay=5,
    retry_backoff=True,
    retry_jitter=True,
)
def notify_followers(self, author_id: str, article_id: str):
    """Send push notifications to all subscribers of the author."""
    author_uuid = UUID(author_id)
    article_uuid = UUID(article_id)

    backend_session = BackendSession()
    users_session = get_users_session()

    try:
        article = (
            backend_session.query(Article)
            .filter(Article.id == article_uuid)
            .one_or_none()
        )
        if not article:
            logger.warning("Article %s not found, skipping notification", article_id)
            return

        subscribers = _get_subscribers(users_session, author_uuid)
        if not subscribers:
            logger.info("No subscribers found for author %s", author_id)
            return

        for subscriber_id, subscription_key in subscribers:
            if not subscription_key:
                logger.warning(
                    "Skip subscriber %s: subscription key is missing", subscriber_id
                )
                continue

            log_entry = _get_or_create_notification_log(
                users_session,
                subscriber_id=subscriber_id,
                author_id=author_uuid,
                article_id=article_uuid,
            )
            if not log_entry:
                continue  # already sent

            message = _format_message(author_uuid, article)
            log_entry.status = "processing"
            log_entry.updated_at = datetime.utcnow()
            users_session.commit()
            logger.info(
                "Sending notification for article %s to subscriber %s",
                article_id,
                subscriber_id,
            )

            try:
                response = requests.post(
                    settings.push_service_url,
                    headers={
                        "Authorization": f"Bearer {subscription_key}",
                        "Content-Type": "application/json",
                    },
                    json={"message": message},
                    timeout=settings.push_timeout_seconds,
                )
                if 400 <= response.status_code < 500:
                    logger.warning(
                        "Push rejected for subscriber %s: %s",
                        subscriber_id,
                        response.text,
                    )
                    log_entry.status = "failed"
                    log_entry.last_error = (
                        f"{response.status_code}: {response.text[:200]}"
                    )
                    users_session.commit()
                    continue
                response.raise_for_status()
                log_entry.status = "sent"
                log_entry.last_error = None
                log_entry.updated_at = datetime.utcnow()
                users_session.commit()
            except HTTPError as exc:
                logger.error(
                    "Push HTTP error for subscriber %s: %s", subscriber_id, exc
                )
                log_entry.status = "failed"
                log_entry.last_error = str(exc)
                users_session.commit()
                raise self.retry(exc=exc)
            except RequestException as exc:
                logger.error(
                    "Push network error for subscriber %s: %s", subscriber_id, exc
                )
                log_entry.status = "failed"
                log_entry.last_error = str(exc)
                users_session.commit()
                raise self.retry(exc=exc)
    finally:
        backend_session.close()
        users_session.close()


def enqueue_article_notification(author_id: UUID, article_id: UUID) -> None:
    """Helper for API layer."""
    notify_followers.apply_async(
        kwargs={"author_id": str(author_id), "article_id": str(article_id)},
        queue=settings.notifications_queue,
    )

