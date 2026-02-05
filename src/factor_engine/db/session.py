"""Session utilities for SQLite persistence."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker


def _build_engine(db_path: Path):
    url = f"sqlite:///{db_path}"
    return create_engine(url, connect_args={"check_same_thread": False})


DB_PATH = Path(os.getenv("FACTOR_ENGINE_DB_PATH", "sieng.db"))
_engine = _build_engine(DB_PATH)
_Session = sessionmaker(bind=_engine, autoflush=False, autocommit=False)


def configure_engine(path: str | Path) -> None:
    """Reconfigure the global engine/sessionmaker (useful for tests)."""
    global DB_PATH, _engine, _Session
    DB_PATH = Path(path)
    _engine = _build_engine(DB_PATH)
    _Session = sessionmaker(bind=_engine, autoflush=False, autocommit=False)


def get_engine():
    return _engine


def get_session() -> Session:
    return _Session()


@contextmanager
def session_scope() -> Iterator[Session]:
    session = get_session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def init_db(base) -> None:
    from . import models  # noqa: F401  # Ensure models are registered

    base.metadata.create_all(_engine)
