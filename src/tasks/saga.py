"""SAGA Choreography tasks for article publication workflow"""
import logging
import random
import requests
from typing import Optional
from uuid import UUID
from sqlalchemy.orm import Session

from src.config import settings
from src.models.database import Article, SessionLocal as BackendSession
from src.tasks.celery_app import celery_app
from src.tasks.users_db import get_users_session
from src.tasks.dlq import enqueue_dlq_task

logger = logging.getLogger(__name__)


def _get_internal_api_key() -> str:
    """Get internal API key from environment or config"""
    # In production, this should be stored securely (e.g., secrets manager)
    # For now, we'll use a config setting
    api_key = getattr(settings, 'internal_api_key', None)
    if not api_key or api_key == "change-me-in-production":
        raise ValueError("Internal API key not configured. Set INTERNAL_API_KEY environment variable.")
    return api_key


def _make_internal_request(method: str, url: str, data: dict = None) -> requests.Response:
    """Make internal API request with API key"""
    api_key = _get_internal_api_key()
    if not api_key:
        raise ValueError("Internal API key not configured")
    
    headers = {
        "Authorization": f"Token {api_key}",
        "Content-Type": "application/json"
    }
    
    if method.upper() == "GET":
        return requests.get(url, headers=headers, timeout=10)
    elif method.upper() == "POST":
        return requests.post(url, json=data, headers=headers, timeout=10)
    elif method.upper() == "PUT":
        return requests.put(url, json=data, headers=headers, timeout=10)
    else:
        raise ValueError(f"Unsupported HTTP method: {method}")


@celery_app.task(
    name="src.tasks.saga.moderate_post",
    bind=True,
    max_retries=3,
    default_retry_delay=5,
    retry_backoff=True,
    retry_jitter=True,
)
def moderate_post(self, post_id: str, author_id: str, title: str, body: str, requested_by: str):
    """
    Moderation task: Simulates AI moderation and decides to approve or reject.
    If approved, enqueues preview generation. If rejected, calls compensation.
    """
    logger.info("=" * 60)
    logger.info("MODERATION TASK STARTED")
    logger.info("Post ID: %s (type: %s)", post_id, type(post_id).__name__)
    logger.info("Author ID: %s (type: %s)", author_id, type(author_id).__name__)
    logger.info("Title: %s", title)
    logger.info("=" * 60)
    
    # Safely convert to UUID
    try:
        # Handle both string and UUID types
        if isinstance(post_id, UUID):
            post_uuid = post_id
        else:
            # Remove any whitespace and validate
            post_id_clean = str(post_id).strip()
            post_uuid = UUID(post_id_clean)
        
        if isinstance(author_id, UUID):
            author_uuid = author_id
        else:
            author_id_clean = str(author_id).strip()
            author_uuid = UUID(author_id_clean)
    except (ValueError, TypeError) as e:
        logger.error("Invalid UUID format: post_id=%s, author_id=%s, error=%s", post_id, author_id, e)
        raise ValueError(f"Invalid UUID format: post_id={post_id}, author_id={author_id}") from e
    
    backend_session = BackendSession()
    
    try:
        # Get article to verify it exists and is in correct status
        article = backend_session.query(Article).filter(Article.id == post_uuid).first()
        if not article:
            logger.warning("Article %s not found for moderation", post_id)
            return
        
        logger.info("Article found: status=%s, slug=%s", article.status, article.slug)
        
        if article.status != "PENDING_PUBLISH":
            logger.warning("Article %s is not in PENDING_PUBLISH status (current: %s), skipping moderation", 
                         post_id, article.status)
            return
        
        # Simulate moderation: random approval (70% chance of approval)
        approved = random.choice([True, True, True, True, True, True, True, False, False, False])
        
        logger.info("Moderation result for article %s: %s", post_id, "APPROVED" if approved else "REJECTED")
        
        if approved:
            # Enqueue preview generation
            logger.info("Article approved! Enqueueing preview generation...")
            generate_preview.apply_async(
                kwargs={
                    "post_id": post_id,
                    "author_id": author_id,
                    "title": title,
                    "body": body
                },
                queue=settings.notifications_queue
            )
            logger.info("✓ Preview generation task enqueued for article %s", post_id)
        else:
            # Compensation: reject the article
            logger.info("Article rejected! Calling compensation endpoint...")
            try:
                base_url = getattr(settings, 'backend_url', 'http://backend:8000')
                reject_url = f"{base_url}/internal/articles/{post_id}/reject"
                logger.info("Reject URL: %s", reject_url)
                response = _make_internal_request("POST", reject_url, {"reason": "Moderation rejected"})
                response.raise_for_status()
                logger.info("✓ Article %s rejected via compensation", post_id)
            except Exception as exc:
                logger.error("Failed to reject article %s: %s", post_id, exc)
                # Fallback: update status directly
                article.status = "REJECTED"
                backend_session.commit()
                raise self.retry(exc=exc)
    
    except Exception as exc:
        logger.error("✗ ERROR in moderation task for article %s: %s", post_id, exc)
        logger.error("Exception type: %s", type(exc).__name__)
        if self.request.retries >= self.max_retries:
            # Send to DLQ
            logger.error("Max retries reached, sending to DLQ...")
            try:
                enqueue_dlq_task(
                    "moderate_post",
                    {"post_id": post_id, "author_id": author_id},
                    str(exc)
                )
                logger.info("✓ Task sent to DLQ")
            except Exception as dlq_exc:
                logger.error("Failed to enqueue to DLQ: %s", dlq_exc)
        raise self.retry(exc=exc)
    finally:
        backend_session.close()
        logger.info("MODERATION TASK COMPLETED")
        logger.info("=" * 60)


@celery_app.task(
    name="src.tasks.saga.generate_preview",
    bind=True,
    max_retries=3,
    default_retry_delay=5,
    retry_backoff=True,
    retry_jitter=True,
)
def generate_preview(self, post_id: str, author_id: str, title: str, body: str):
    """
    Preview generation task: Creates a preview (fake or real) and saves it.
    Then enqueues publication task.
    """
    # Safely convert to UUID
    try:
        if isinstance(post_id, UUID):
            post_uuid = post_id
        else:
            post_id_clean = str(post_id).strip()
            post_uuid = UUID(post_id_clean)
    except (ValueError, TypeError) as e:
        logger.error("Invalid UUID format in generate_preview: post_id=%s, error=%s", post_id, e)
        raise ValueError(f"Invalid UUID format: post_id={post_id}") from e
    
    backend_session = BackendSession()
    
    try:
        # Get article
        article = backend_session.query(Article).filter(Article.id == post_uuid).first()
        if not article:
            logger.warning("Article %s not found for preview generation", post_id)
            return
        
        # Check if preview already exists (idempotency)
        if article.preview_url:
            logger.info("Preview already exists for article %s, skipping generation", post_id)
            # Still enqueue publish task if article is not yet published
            if article.status == "PENDING_PUBLISH":
                publish_post.apply_async(
                    kwargs={"post_id": post_id, "author_id": author_id},
                    queue=settings.notifications_queue
                )
            return
        
        # Generate fake preview URL (in production, use Pillow or similar)
        # For demo purposes, we'll create a fake URL
        preview_url = f"https://preview.example.com/articles/{post_id}/preview.png"
        
        # Save preview URL
        try:
            base_url = getattr(settings, 'backend_url', 'http://backend:8000')
            preview_endpoint = f"{base_url}/internal/articles/{post_id}/preview"
            response = _make_internal_request("PUT", preview_endpoint, {"preview_url": preview_url})
            response.raise_for_status()
            logger.info("Preview saved for article %s: %s", post_id, preview_url)
        except Exception as exc:
            logger.error("Failed to save preview for article %s: %s", post_id, exc)
            raise self.retry(exc=exc)
        
        # Enqueue publication task
        publish_post.apply_async(
            kwargs={"post_id": post_id, "author_id": author_id},
            queue=settings.notifications_queue
        )
        logger.info("Publication task enqueued for article %s", post_id)
    
    except Exception as exc:
        logger.error("Error in preview generation task for article %s: %s", post_id, exc)
        if self.request.retries >= self.max_retries:
            # Send to DLQ
            try:
                enqueue_dlq_task(
                    "generate_preview",
                    {"post_id": post_id, "author_id": author_id},
                    str(exc)
                )
            except Exception as dlq_exc:
                logger.error("Failed to enqueue to DLQ: %s", dlq_exc)
        raise self.retry(exc=exc)
    finally:
        backend_session.close()


@celery_app.task(
    name="src.tasks.saga.publish_post",
    bind=True,
    max_retries=3,
    default_retry_delay=5,
    retry_backoff=True,
    retry_jitter=True,
)
def publish_post(self, post_id: str, author_id: str):
    """
    Publication task: Changes article status to PUBLISHED and enqueues notifications.
    """
    # Safely convert to UUID
    try:
        if isinstance(post_id, UUID):
            post_uuid = post_id
        else:
            post_id_clean = str(post_id).strip()
            post_uuid = UUID(post_id_clean)
        
        if isinstance(author_id, UUID):
            author_uuid = author_id
        else:
            author_id_clean = str(author_id).strip()
            author_uuid = UUID(author_id_clean)
    except (ValueError, TypeError) as e:
        logger.error("Invalid UUID format in publish_post: post_id=%s, author_id=%s, error=%s", post_id, author_id, e)
        raise ValueError(f"Invalid UUID format: post_id={post_id}, author_id={author_id}") from e
    
    backend_session = BackendSession()
    
    try:
        # Get article
        article = backend_session.query(Article).filter(Article.id == post_uuid).first()
        if not article:
            logger.warning("Article %s not found for publication", post_id)
            return
        
        # Idempotency check: if already published, skip
        if article.status == "PUBLISHED":
            logger.info("Article %s already published, skipping", post_id)
            # Still enqueue notification if not sent yet (handled by notification worker)
            from src.tasks.notifications import enqueue_article_notification
            try:
                enqueue_article_notification(author_uuid, post_uuid)
            except Exception as notif_exc:
                logger.warning("Failed to enqueue notification for already published article: %s", notif_exc)
            return
        
        # Publish article
        try:
            base_url = getattr(settings, 'backend_url', 'http://backend:8000')
            publish_endpoint = f"{base_url}/internal/articles/{post_id}/publish"
            response = _make_internal_request("POST", publish_endpoint, {})
            response.raise_for_status()
            logger.info("Article %s published", post_id)
        except Exception as exc:
            logger.error("Failed to publish article %s: %s", post_id, exc)
            raise self.retry(exc=exc)
        
        # Enqueue notification task (uses existing notification worker from LAB3)
        from src.tasks.notifications import enqueue_article_notification
        try:
            enqueue_article_notification(author_uuid, post_uuid)
            logger.info("Notification task enqueued for article %s", post_id)
        except Exception as notif_exc:
            logger.error("Failed to enqueue notification for article %s: %s", post_id, notif_exc)
            # Don't fail the whole task if notification fails
    
    except Exception as exc:
        logger.error("Error in publication task for article %s: %s", post_id, exc)
        if self.request.retries >= self.max_retries:
            # Send to DLQ
            try:
                enqueue_dlq_task(
                    "publish_post",
                    {"post_id": post_id, "author_id": author_id},
                    str(exc)
                )
            except Exception as dlq_exc:
                logger.error("Failed to enqueue to DLQ: %s", dlq_exc)
        raise self.retry(exc=exc)
    finally:
        backend_session.close()


def enqueue_moderation_task(post_id: str, author_id: str, title: str, body: str, requested_by: str):
    """Helper to enqueue moderation task"""
    # Ensure all IDs are strings (UUID objects are converted to strings)
    import logging
    logger = logging.getLogger(__name__)
    
    # Convert UUID objects to strings if needed
    post_id_str = str(post_id) if post_id else None
    author_id_str = str(author_id) if author_id else None
    requested_by_str = str(requested_by) if requested_by else None
    
    # Validate UUID format before enqueueing
    try:
        UUID(post_id_str)
        UUID(author_id_str)
        UUID(requested_by_str)
    except (ValueError, TypeError) as e:
        logger.error("Invalid UUID format before enqueueing: post_id=%s, author_id=%s, requested_by=%s, error=%s",
                    post_id_str, author_id_str, requested_by_str, e)
        raise ValueError(f"Invalid UUID format: {e}") from e
    
    logger.info("Enqueueing moderation task: post_id=%s, author_id=%s", post_id_str, author_id_str)
    
    moderate_post.apply_async(
        kwargs={
            "post_id": post_id_str,
            "author_id": author_id_str,
            "title": title or "",
            "body": body or "",
            "requested_by": requested_by_str
        },
        queue=settings.notifications_queue
    )

