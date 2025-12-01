import os
import sys
import time
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

# Add src to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def wait_for_db(database_url: str, max_retries: int = 30, delay: int = 2):
    """Wait for database to be available"""
    print("⏳ Waiting for database to be ready...")
    
    for attempt in range(max_retries):
        try:
            engine = create_engine(database_url)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("✅ Database is ready!")
            return True
        except OperationalError as e:
            if attempt < max_retries - 1:
                print(f"⏳ Attempt {attempt + 1}/{max_retries}: Database not ready, retrying in {delay}s...")
                time.sleep(delay)
            else:
                print(f"❌ Failed to connect to database after {max_retries} attempts")
                print(f"Error: {e}")
                return False
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            return False
    
    return False


def run_migrations():
    """Run Alembic migrations"""
    try:
        print("🔄 Running database migrations...")
        os.system("alembic upgrade head")
        print("✅ Migrations completed successfully!")
        return True
    except Exception as e:
        print(f"⚠️ Warning: Migration failed: {e}")
        print("⚠️ Continuing anyway...")
        return False


if __name__ == "__main__":
    database_url = os.getenv("USERS_DATABASE_URL") or os.getenv(
        "DATABASE_URL", "postgresql://app:app@db-users:5432/app_users"
    )
    
    # Skip DB initialization if localhost (for local dev)
    if "localhost" in database_url or "127.0.0.1" in database_url:
        print("⚠️ Localhost detected, skipping DB initialization")
        sys.exit(0)
    
    if wait_for_db(database_url):
        run_migrations()
    else:
        print("⚠️ Warning: Database initialization failed, but continuing...")

