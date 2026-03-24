"""
TalentScope AI — Database Connection
Provides engine, session, and raw connection utilities.
"""

from sqlalchemy import create_engine, text
from src.utils.config import DATABASE_URL
from src.utils.logger import get_logger

logger = get_logger("database")


def get_engine():
    """Create and return a SQLAlchemy engine."""
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    return engine


def test_connection():
    """Test database connectivity and return True if successful."""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            result.fetchone()
        logger.info("Database connection successful")
        return True
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False


if __name__ == "__main__":
    test_connection()