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


_engine = None
_SessionLocal = None


def _get_default_engine():
    """Return the lazily-initialised default engine (singleton)."""
    global _engine
    if _engine is None:
        _engine = get_engine()
    return _engine


def _get_session_factory():
    """Return the lazily-initialised session factory (singleton)."""
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(bind=_get_default_engine())
    return _SessionLocal


# Backward-compatible aliases (accessed as attributes, trigger lazy init)
class _LazySessionLocal:
    """Proxy so that ``SessionLocal(...)`` works without eager engine creation."""

    def __call__(self, *args, **kwargs):
        return _get_session_factory()(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(_get_session_factory(), name)


SessionLocal = _LazySessionLocal()


def init_db() -> None:
    """Create all tables (useful for quick bootstrapping without Alembic)."""
    Base.metadata.create_all(bind=_get_default_engine())


def get_db() -> Generator[Session, None, None]:
    """Yield a database session, closing it when done.

    Intended for use as a FastAPI dependency.
    """
    db = _get_session_factory()()
    try:
        yield db
    finally:
        db.close()
