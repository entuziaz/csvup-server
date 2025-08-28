from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base


SQLALCHEMY_DATABASE_URL = "sqlite:///./uploads.db"
# SQLALCHEMY_DATABASE_URL = "postgresql://user:password@localhost/uploadsdb"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}  # For SQLite
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

# Dependency: getting DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
