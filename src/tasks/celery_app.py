from celery import Celery

from src.config import settings


celery_app = Celery(
    "blog_platform_worker",
    broker=settings.redis_url,
    backend=settings.redis_url,
)

celery_app.conf.update(
    task_default_queue=settings.notifications_queue,
    task_routes={
        "src.tasks.notifications.*": {"queue": settings.notifications_queue}
    },
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    broker_connection_retry_on_startup=True,
    worker_max_tasks_per_child=1000,
)

__all__ = ["celery_app"]


