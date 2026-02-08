"""Database engine and session factory."""

from collections.abc import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from funda_finder.config import settings
from funda_finder.db.models import Base


def get_engine(db_url=None, db_path=None):
    """Create a SQLAlchemy engine for the configured database.

    Args:
        db_url: Optional database URL (PostgreSQL, etc.)
        db_path: Optional SQLite database path (legacy)

    Returns:
        SQLAlchemy engine

    Note:
        If db_url is provided, it takes precedence over db_path.
        If neither is provided, uses settings.db_url or settings.db_path.
    """
    # Use provided URL or fall back to settings
    url = db_url or settings.db_url
    path = db_path or settings.db_path

    # If URL starts with postgresql://, use it directly
    if url and (url.startswith("postgresql://") or url.startswith("postgres://")):
        return create_engine(url, echo=False, pool_pre_ping=True)

    # Otherwise use SQLite with the provided or configured path
    if path:
        path.parent.mkdir(parents=True, exist_ok=True)
        return create_engine(f"sqlite:///{path}", echo=False)

    # Fallback to in-memory SQLite if nothing configured
    return create_engine("sqlite:///:memory:", echo=False)


engine = get_engine()
SessionLocal = sessionmaker(bind=engine)


def init_db() -> None:
    """Create all tables (useful for quick bootstrapping without Alembic)."""
    Base.metadata.create_all(bind=engine)


def get_db() -> Generator[Session, None, None]:
    """Yield a database session, closing it when done.

    Intended for use as a FastAPI dependency.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
