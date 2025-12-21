"""Dead Letter Queue consumer for failed tasks"""
import logging
from typing import Dict, Any
from uuid import UUID

from src.config import settings
from src.models.database import Article, SessionLocal as BackendSession
from src.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)


@celery_app.task(
    name="src.tasks.dlq.handle_failed_task",
    bind=True,
    max_retries=1,  # DLQ handler should not retry much
)
def handle_failed_task(self, task_name: str, task_data: Dict[str, Any], error: str):
    """
    Handle failed task from DLQ.
    Performs compensating actions based on task type.
    """
    logger.error("Processing failed task from DLQ: %s, error: %s", task_name, error)
    
    backend_session = BackendSession()
    
    try:
        # Extract post_id from task data
        post_id = task_data.get("post_id") or task_data.get("article_id")
        if not post_id:
            logger.error("No post_id found in failed task data: %s", task_data)
            return
        
        post_uuid = UUID(post_id)
        
        # Get article
        article = backend_session.query(Article).filter(Article.id == post_uuid).first()
        if not article:
            logger.warning("Article %s not found for DLQ compensation", post_id)
            return
        
        # Perform compensation based on task type
        if "moderate" in task_name.lower():
            # Moderation failed: mark as REJECTED
            logger.info("Compensating moderation failure: marking article %s as REJECTED", post_id)
            article.status = "REJECTED"
            backend_session.commit()
        
        elif "preview" in task_name.lower():
            # Preview generation failed: mark as ERROR
            logger.info("Compensating preview failure: marking article %s as ERROR", post_id)
            article.status = "ERROR"
            backend_session.commit()
        
        elif "publish" in task_name.lower():
            # Publication failed: rollback to DRAFT or mark as ERROR
            logger.info("Compensating publication failure: marking article %s as ERROR", post_id)
            article.status = "ERROR"
            backend_session.commit()
        
        elif "notify" in task_name.lower():
            # Notification failed: article is already published, just log
            logger.warning("Notification failed for published article %s, no compensation needed", post_id)
        
        else:
            logger.warning("Unknown task type for DLQ: %s", task_name)
            # Default: mark as ERROR
            article.status = "ERROR"
            backend_session.commit()
    
    except Exception as exc:
        logger.error("Error in DLQ handler: %s", exc)
        raise
    finally:
        backend_session.close()


def enqueue_dlq_task(task_name: str, task_data: Dict[str, Any], error: str):
    """Helper to enqueue task to DLQ"""
    handle_failed_task.apply_async(
        kwargs={
            "task_name": task_name,
            "task_data": task_data,
            "error": str(error)
        },
        queue=settings.dlq_queue
    )

