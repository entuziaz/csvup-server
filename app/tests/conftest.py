# tests/conftest.py
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from app.core.database import Base, get_db
from fastapi.testclient import TestClient
from main import app

import io
from sqlalchemy.orm import Session
from app.uploads import models
# import sys
# from pathlib import Path

# sys.path.append(str(Path(__file__).resolve().parents[2]))


# In-memory SQLite
SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL, 
    connect_args={"check_same_thread": False},
    poolclass=StaticPool 
    )
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create tables
Base.metadata.create_all(bind=engine)


@pytest.fixture
def db_session():
    """Create a clean database session for each test."""
    connection = engine.connect()
    transaction = connection.begin()
    session = TestingSessionLocal(bind=connection)
    
    # Override the dependency for this test
    def override_get_db():
        try:
            yield session
        finally:
            pass  # No closing here, handled in fixture teardown
    
    app.dependency_overrides[get_db] = override_get_db
    
    yield session
    
    # Cleanup
    app.dependency_overrides.clear()
    session.close()
    transaction.rollback()
    connection.close()

@pytest.fixture
def client(db_session: Session):
    """Create a test client that uses the same db_session."""
    # The db_session fixture already overrides the dependency
    with TestClient(app) as c:
        yield c