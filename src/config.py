from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    # Database settings
    database_url: str = "postgresql://app:app@db-main:5432/app_main"
    users_database_url: str = "postgresql://app:app@db-users:5432/app_users"
    
    # Queue / worker settings
    redis_url: str = "redis://redis:6379/0"
    notifications_queue: str = "article-notifications"
    dlq_queue: str = "dlq"
    push_service_url: str = "http://push-notificator:8000/api/v1/notify"
    push_timeout_seconds: int = 5
    backend_url: str = "http://backend:8000"
    internal_api_key: Optional[str] = None
    
    # API settings
    api_title: str = "Blog Platform API"
    api_version: str = "1.0.0"
    api_description: str = "Backend for simplified blog platform"
    
    # Security settings
    secret_key: str = "your-secret-key-change-in-production"
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 30
    
    # CORS settings
    allowed_origins: list = ["*"]
    
    # Rate limiting
    rate_limit_requests: int = 100
    rate_limit_window: int = 900  # 15 minutes in seconds
    
    # Debug mode
    debug: bool = False
    
    # Render specific settings
    port: int = 8000
    host: str = "0.0.0.0"
    
    class Config:
        env_file = ".env"


settings = Settings()
