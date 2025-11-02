from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    # Database settings
    database_url: str = "postgresql://user:password@localhost:5432/blog_platform"
    
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
