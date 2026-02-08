"""Database engine and session factory."""

from collections.abc import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from funda_finder.config import settings
from funda_finder.db.models import Base


def get_engine(db_path=None):
    """Create a SQLAlchemy engine for the configured SQLite database."""
    path = db_path or settings.db_path
    path.parent.mkdir(parents=True, exist_ok=True)
    return create_engine(f"sqlite:///{path}", echo=False)


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
