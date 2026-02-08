"""Database connection factory."""
from pathlib import Path
from typing import Generator

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from funda_finder.config import settings
from funda_finder.db.models import Base


@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_conn, connection_record):
    """Enable foreign key constraints for SQLite."""
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


def get_engine(db_path: Path | None = None):
    """Create SQLAlchemy engine for SQLite database.

    Args:
        db_path: Path to SQLite database file. If None, uses settings.db_path.

    Returns:
        SQLAlchemy Engine instance.
    """
    if db_path is None:
        db_path = settings.db_path

    # Ensure parent directory exists
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Create engine with SQLite-specific settings
    engine = create_engine(
        f"sqlite:///{db_path}",
        echo=False,  # Set to True for SQL debugging
        connect_args={"check_same_thread": False},  # Allow multithreaded access
    )

    return engine


def create_tables(engine: Engine | None = None):
    """Create all database tables.

    Args:
        engine: SQLAlchemy engine. If None, creates default engine.
    """
    if engine is None:
        engine = get_engine()

    Base.metadata.create_all(engine)


def get_session_factory(db_path: Path | None = None) -> sessionmaker:
    """Create a sessionmaker for the database.

    Args:
        db_path: Path to SQLite database file. If None, uses settings.db_path.

    Returns:
        Configured sessionmaker instance.
    """
    engine = get_engine(db_path)
    return sessionmaker(bind=engine, autocommit=False, autoflush=False)


def get_session(db_path: Path | None = None) -> Generator[Session, None, None]:
    """Get a database session (context manager).

    Args:
        db_path: Path to SQLite database file. If None, uses settings.db_path.

    Yields:
        SQLAlchemy Session instance.

    Example:
        with next(get_session()) as session:
            properties = session.query(Property).all()
    """
    SessionFactory = get_session_factory(db_path)
    session = SessionFactory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
