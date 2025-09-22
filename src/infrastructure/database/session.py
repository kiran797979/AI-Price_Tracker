import os
from urllib.parse import urlparse

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .models import Base

load_dotenv()


def get_db_url():
    """Get database URL with fallback to SQLite for local development"""
    db_url = os.getenv("POSTGRES_URL")
    if db_url:
        try:
            # Clean up any quotes from the URL string
            db_url = db_url.strip('"').strip("'")

            result = urlparse(db_url)
            if result.scheme in ["postgres", "postgresql"]:
                # Ensure we're using the correct scheme
                db_url = db_url.replace("postgres://", "postgresql://", 1)

                # Add required connection parameters
                if "?" not in db_url:
                    db_url += "?sslmode=require"

                return db_url
        except Exception as e:
            print(f"Error parsing database URL: {e}")
            return None

    return "sqlite:///data/price_history.db"


def get_engine():
    """Create database engine with appropriate configuration for the database type"""
    db_url = get_db_url()
    
    # Base engine arguments
    engine_args = {
        "pool_pre_ping": True,
        "pool_size": 5,
        "max_overflow": 10,
    }
    
    # Add connection timeout only for PostgreSQL (not supported by SQLite)
    if db_url.startswith("postgresql://"):
        engine_args["connect_args"] = {"connect_timeout": 30}
    
    return create_engine(db_url, **engine_args)

engine = get_engine()

SessionLocal = sessionmaker(bind=engine)

# Create tables if they don't exist
Base.metadata.create_all(engine)


def get_session():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
