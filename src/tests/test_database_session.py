"""Tests for database session configuration"""
import os
import pytest
from unittest.mock import patch
from sqlalchemy import create_engine


def test_get_engine_sqlite():
    """Test that SQLite engine is created without connect_timeout"""
    from src.infrastructure.database.session import get_engine
    
    # Test with no POSTGRES_URL (should use SQLite)
    with patch.dict(os.environ, {'POSTGRES_URL': ''}, clear=False):
        engine = get_engine()
        
        # Verify engine was created (won't raise TypeError)
        assert engine is not None
        
        # SQLite URL should be used
        assert 'sqlite' in str(engine.url)


def test_get_engine_postgresql():
    """Test that PostgreSQL engine is created with connect_timeout"""
    from src.infrastructure.database.session import get_engine
    
    # Test with PostgreSQL URL
    test_postgres_url = "postgresql://user:pass@localhost:5432/testdb"
    with patch.dict(os.environ, {'POSTGRES_URL': test_postgres_url}, clear=False):
        engine = get_engine()
        
        # Verify engine was created
        assert engine is not None
        
        # PostgreSQL URL should be used
        assert 'postgresql' in str(engine.url)


def test_get_db_url_fallback():
    """Test that get_db_url falls back to SQLite when POSTGRES_URL is not set"""
    from src.infrastructure.database.session import get_db_url
    
    # Test with no POSTGRES_URL
    with patch.dict(os.environ, {'POSTGRES_URL': ''}, clear=False):
        db_url = get_db_url()
        assert db_url == "sqlite:///data/price_history.db"


def test_get_db_url_postgres():
    """Test that get_db_url returns PostgreSQL URL when set"""
    from src.infrastructure.database.session import get_db_url
    
    test_postgres_url = "postgresql://user:pass@localhost:5432/testdb"
    with patch.dict(os.environ, {'POSTGRES_URL': test_postgres_url}, clear=False):
        db_url = get_db_url()
        assert db_url.startswith("postgresql://")


def test_get_db_url_postgres_scheme_correction():
    """Test that get_db_url corrects postgres:// to postgresql://"""
    from src.infrastructure.database.session import get_db_url
    
    # Test with old postgres:// scheme
    test_postgres_url = "postgres://user:pass@localhost:5432/testdb"
    with patch.dict(os.environ, {'POSTGRES_URL': test_postgres_url}, clear=False):
        db_url = get_db_url()
        # Verify the scheme was corrected
        assert db_url.startswith("postgresql://")
        assert "postgres://" not in db_url
