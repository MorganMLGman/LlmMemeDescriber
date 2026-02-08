import os
import sys
from typing import Optional

from sqlmodel import Session, create_engine, select
from sqlalchemy import text, func
from alembic.config import Config
from alembic import command
import logging

from .models import Meme

logger = logging.getLogger(__name__)

def run_migrations(database_url: str):
    """Run Alembic migrations."""
    try:
        # Get the project root directory (parent of llm_memedescriber package)
        package_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        alembic_ini_path = os.path.join(package_dir, "alembic.ini")
        alembic_dir = os.path.join(package_dir, "alembic")
        
        # Debug output
        logger.debug(f"Package dir: {package_dir}")
        logger.debug(f"Alembic ini: {alembic_ini_path} (exists: {os.path.exists(alembic_ini_path)})")
        logger.debug(f"Alembic dir: {alembic_dir} (exists: {os.path.exists(alembic_dir)})")
        
        # Set up Alembic configuration
        alembic_cfg = Config(alembic_ini_path)
        
        # Explicitly set script_location to be safe
        alembic_cfg.set_main_option("script_location", alembic_dir)
        
        # Set database URL in config (overrides what's in alembic.ini)
        alembic_cfg.set_main_option("sqlalchemy.url", database_url)
        
        # Run migrations to head
        logger.info("Running Alembic migrations to HEAD...")
        sys.stdout.flush()
        sys.stderr.flush()
        command.upgrade(alembic_cfg, "head")
        sys.stdout.flush()
        sys.stderr.flush()
        logger.info("Database migrations completed successfully")
    except Exception as e:
        logger.error(f"Failed to run migrations: {e}", exc_info=True)
        # Don't raise - let the app continue with current schema
        

def init_db(database_url: str = "sqlite:////data/memes.db"):
    """Create and return SQLAlchemy engine. Schema is managed by Alembic migrations."""
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

    # Run Alembic migrations (manages all schema creation and changes)
    run_migrations(database_url)

    logger.info("Database initialization complete")
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


