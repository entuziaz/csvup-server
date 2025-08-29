import os
import time
from alembic.config import Config
from alembic import command
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

def run_migrations():
    # Get database URL from environment
    database_url = os.getenv("DATABASE_URL")
    
    if not database_url:
        print("---DATABASE_URL not set")
        return False
    
    # Wait for database to be ready (important for cloud databases)
    max_retries = 5
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            engine = create_engine(database_url)
            with engine.connect() as conn:
                print("---Database connection successful")
                break
        except OperationalError as e:
            if attempt == max_retries - 1:
                print(f"---Failed to connect to database after {max_retries} attempts: {e}")
                return False
            print(f"---Database not ready (attempt {attempt + 1}/{max_retries}), retrying...")
            time.sleep(retry_delay)
    
    # Run migrations
    try:
        alembic_cfg = Config("alembic.ini")
        command.upgrade(alembic_cfg, "head")
        print("---Migrations completed successfully")
        return True
    except Exception as e:
        print(f"---Migration failed: {e}")
        return False

if __name__ == "__main__":
    run_migrations()