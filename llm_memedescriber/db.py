import os
from typing import Optional

from sqlmodel import SQLModel, Session, create_engine, select
from sqlalchemy import text, func
import logging

logger = logging.getLogger(__name__)

from .models import Meme, BasicAuthUser, UserToken, FileShareToken


def init_db(database_url: str = "sqlite:////data/memes.db"):
    """Create and return SQLAlchemy engine. Creates all tables on startup."""
    try:
        if database_url.startswith("sqlite:///"):
            file_path = database_url[len("sqlite:///"):]
            dirpath = os.path.dirname(file_path)
            if dirpath and not os.path.exists(dirpath):
                os.makedirs(dirpath, exist_ok=True)
    except Exception:
        pass

    # Increase timeout for SQLite to handle concurrent access better
    connect_args = {"check_same_thread": False, "timeout": 30}
    engine = create_engine(database_url, echo=False, connect_args=connect_args)
    
    try:
        with engine.connect() as conn:
            conn.execute(text("PRAGMA journal_mode=WAL"))
            conn.execute(text("PRAGMA synchronous=NORMAL"))
            conn.execute(text("PRAGMA temp_store=MEMORY"))
    except Exception as e:
        logger.debug("Unable to set SQLite pragmas: %s", e)

    SQLModel.metadata.create_all(engine)

    # Ensure auth tables exist even if database was created before they were added to models
    try:
        with engine.connect() as conn:
            # Check if BasicAuthUser table exists, create if not
            result = conn.execute(text(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='basicauthuser'"
            ))
            if not result.fetchone():
                logger.info("Creating missing BasicAuthUser table")
                BasicAuthUser.__table__.create(engine, checkfirst=True)
            else:
                # Fix empty string datetime values (should be NULL)
                conn.execute(text(
                    "UPDATE basicauthuser SET last_used_at = NULL WHERE last_used_at = '' OR last_used_at IS ''"
                ))
                conn.execute(text(
                    "UPDATE basicauthuser SET locked_until = NULL WHERE locked_until = '' OR locked_until IS ''"
                ))
                conn.commit()
                logger.debug("Fixed empty datetime strings in basicauthuser table")
    except Exception as e:
        logger.warning("Unable to ensure BasicAuthUser table exists: %s", e)

    return engine


def get_meme_by_filename(session: Session, filename: str) -> Optional[Meme]:
    """Get a single meme by filename."""
    return session.exec(select(Meme).where(Meme.filename == filename)).first()


def get_stats(session: Session) -> dict:
    """Get aggregated statistics for all memes (excluding removed).
    
    Returns dict with keys: total, filled, pending, failed, unsupported, completion_percent
    """
    
    statement = select(
        Meme.status,
        func.count(Meme.id).label("count")
    ).where(Meme.status != 'removed').group_by(Meme.status)
    
    results = session.exec(statement).all()
    stats = {
        'total': 0,
        'filled': 0,
        'pending': 0,
        'failed': 0,
        'unsupported': 0,
    }
    
    for status, count in results:
        stats['total'] += count
        if status == 'filled':
            stats['filled'] = count
        elif status == 'pending':
            stats['pending'] = count
        elif status == 'failed':
            stats['failed'] = count
        elif status == 'unsupported':
            stats['unsupported'] = count
    
    stats['completion_percent'] = round(stats['filled'] / stats['total'] * 100, 1) if stats['total'] > 0 else 0
    
    return stats


