"""Simple script to generate API keys - run inside Docker container"""
import secrets
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.models.database import ApiKey, Base
from src.config import settings
from datetime import datetime

# Create database session
engine = create_engine(settings.database_url)
SessionLocal = sessionmaker(bind=engine)

# Create tables if they don't exist
Base.metadata.create_all(bind=engine)

def generate_api_key(length: int = 64) -> str:
    """Generate a secure random API key"""
    return secrets.token_urlsafe(length)

def create_api_key(description: str, expires_days: int = None) -> str:
    """Create and save an API key"""
    db = SessionLocal()
    try:
        key = generate_api_key()
        expires_at = None
        if expires_days:
            from datetime import timedelta
            expires_at = datetime.utcnow() + timedelta(days=expires_days)
        
        api_key = ApiKey(
            key=key,
            description=description,
            expires_at=expires_at,
            is_active="active"
        )
        
        db.add(api_key)
        db.commit()
        db.refresh(api_key)
        
        print(f"✓ API Key created: {description}")
        print(f"  Key: {key}")
        print(f"  Expires: {expires_at or 'Never'}\n")
        
        return key
    except Exception as e:
        db.rollback()
        print(f"✗ Error creating API key: {e}")
        raise
    finally:
        db.close()

if __name__ == "__main__":
    print("Generating API keys for internal services...\n")
    
    keys = {
        "moderation-worker": create_api_key("Moderation Worker", expires_days=None),
        "preview-worker": create_api_key("Preview Worker", expires_days=None),
        "publish-worker": create_api_key("Publish Worker", expires_days=None),
        "dlq-worker": create_api_key("DLQ Worker", expires_days=None),
    }
    
    print("="*60)
    print("API Keys generated successfully!")
    print("="*60)
    print("\nAdd ONE of these keys to your .env file:")
    print("INTERNAL_API_KEY=<any-key-above>")
    print("\nExample:")
    print(f"INTERNAL_API_KEY={list(keys.values())[0]}")
    print("\nAll workers will use the same key from INTERNAL_API_KEY env var.")

