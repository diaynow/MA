from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Database settings
    database_url: str = Field(
        default="postgresql://app:app@db-users:5432/app_users",
        validation_alias=AliasChoices("USERS_DATABASE_URL", "DATABASE_URL"),
    )
    
    # API settings
    api_title: str = "Users API"
    api_version: str = "1.0.0"
    api_description: str = "User management microservice"
    
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
    
    # Port settings
    port: int = 8000
    host: str = "0.0.0.0"
    
    class Config:
        env_file = ".env"


settings = Settings()

