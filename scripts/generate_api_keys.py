"""Script to generate internal API keys for service-to-service communication"""
import secrets
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.models.database import ApiKey, Base
from src.config import settings
from datetime import datetime, timedelta

# Create database session
# Use DATABASE_URL from environment or settings
# For local execution, use Docker database URL
database_url = os.getenv("DATABASE_URL")
if not database_url:
    # Try to use settings, but if it points to Docker hostname, replace with localhost
    database_url = settings.database_url
    if "db-main" in database_url:
        # Replace Docker hostname with localhost for local execution
        database_url = database_url.replace("db-main", "localhost")
        print("⚠️  Using localhost instead of db-main for local execution")
        print("   If Docker is not running, use: docker-compose up -d")
        print("   Or set DATABASE_URL environment variable")

if not database_url:
    print("ERROR: DATABASE_URL not set. Please set it in environment or .env file")
    print("Example: DATABASE_URL=postgresql://app:app@localhost:5432/app_main")
    sys.exit(1)

print(f"Connecting to database...")

engine = create_engine(database_url)
SessionLocal = sessionmaker(bind=engine)

# Create tables if they don't exist
try:
    Base.metadata.create_all(bind=engine)
    print("✓ Database connection successful")
except Exception as e:
    print(f"ERROR: Failed to connect to database: {e}")
    print("\nTroubleshooting:")
    print("1. Make sure Docker containers are running: docker-compose up -d")
    print("2. Or set DATABASE_URL environment variable:")
    print("   export DATABASE_URL=postgresql://app:app@localhost:5432/app_main")
    print("3. Or run inside Docker container:")
    print("   docker-compose exec backend python /app/scripts/generate_api_keys.py")
    sys.exit(1)


def generate_api_key(length: int = 64) -> str:
    """Generate a secure random API key"""
    return secrets.token_urlsafe(length)


def create_api_key(description: str, expires_days: int = None) -> str:
    """Create and save an API key"""
    db = SessionLocal()
    try:
        # Generate key
        key = generate_api_key()
        
        # Calculate expiration
        expires_at = None
        if expires_days:
            expires_at = datetime.utcnow() + timedelta(days=expires_days)
        
        # Create API key record
        api_key = ApiKey(
            key=key,
            description=description,
            expires_at=expires_at,
            is_active="active"
        )
        
        db.add(api_key)
        db.commit()
        db.refresh(api_key)
        
        print(f"API Key created successfully!")
        print(f"Description: {description}")
        print(f"Key: {key}")
        print(f"Expires: {expires_at or 'Never'}")
        print(f"\nUse this key in Authorization header:")
        print(f"Authorization: Token {key}")
        
        return key
    except Exception as e:
        db.rollback()
        print(f"Error creating API key: {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    print("⚠️  This script should be run inside Docker container!")
    print("   Use: docker-compose exec backend python generate_api_keys.py")
    print("   Or use the simplified version: scripts/generate_api_keys_simple.py\n")
    
    # Generate keys for different services
    keys = {
        "moderation-worker": create_api_key("Moderation Worker", expires_days=None),
        "preview-worker": create_api_key("Preview Worker", expires_days=None),
        "publish-worker": create_api_key("Publish Worker", expires_days=None),
        "dlq-worker": create_api_key("DLQ Worker", expires_days=None),
    }
    
    print("\n" + "="*60)
    print("API Keys generated successfully!")
    print("="*60)
    print("\nAdd ONE of these keys to your .env file:")
    print("INTERNAL_API_KEY=<any-of-the-keys-above>")
    print("\nExample:")
    print(f"INTERNAL_API_KEY={list(keys.values())[0]}")
    print("\nAll workers will use the same key from INTERNAL_API_KEY env var.")

