import asyncio
import logging
import sqlite3
import time
from typing import Any, Optional

from sqlmodel import select
from .db_helpers import session_scope

from .deduplication import calculate_phash
from .models import Meme

logger = logging.getLogger(__name__)
_db_readonly_detected = False


async def call_storage(storage: Any, method_name: str, *args, **kwargs) -> Any:
    """Call storage method in an async-friendly way.

    If the storage object exposes an `async_<method_name>` coroutine, it is awaited.
    Otherwise the sync method is executed in the event loop's default executor.
    """
    async_name = f"async_{method_name}"
    if hasattr(storage, async_name):
        method = getattr(storage, async_name)
        return await method(*args, **kwargs)

    if hasattr(storage, method_name):
        loop = asyncio.get_running_loop()
        func = lambda: getattr(storage, method_name)(*args, **kwargs)
        return await loop.run_in_executor(None, func)

    raise AttributeError(f"Storage has no method '{method_name}' or '{async_name}'")


async def compute_and_persist_phash(filename: str, storage: Any, engine: Any, timestamp: float = 1.0) -> Optional[str]:
    """Download/extract a representative image for `filename`, compute phash and persist it.

    Returns the phash string on success, or None on failure.
    """
    global _db_readonly_detected
    if _db_readonly_detected:
        logger.debug("Skipping phash persist for %s because DB previously detected as readonly", filename)
        return None

    try:
        ext = filename.lower().rsplit('.', 1)[-1] if '.' in filename else ''
        if ext in (getattr(storage, 'VIDEO_EXTENSIONS', None) or []) or ext in ('mp4', 'mov', 'webm', 'mkv'):
            try:
                data = await call_storage(storage, 'extract_video_frame', filename, timestamp=timestamp)
                logger.debug("Extracted video frame for %s", filename)
            except Exception as e:
                logger.debug("Failed to extract video frame for %s: %s", filename, e)
                return None
        else:
            data = await call_storage(storage, 'download_file', filename)

        if not data:
            logger.debug("Empty data for %s", filename)
            return None

        phash = calculate_phash(data)
        if not phash:
            return None

        try:
            with session_scope(engine) as s:
                m = s.exec(select(Meme).where(Meme.filename == filename)).first()
                if not m:
                    logger.debug("No DB record for %s while persisting phash", filename)
                    return None
                m.phash = phash
                try:
                    m.updated_at = __import__('datetime').datetime.now(__import__('datetime').timezone.utc)
                except Exception:
                    pass
                s.add(m)
                last_exc = None
                for attempt in range(3):
                    try:
                        s.commit()
                        s.refresh(m)
                        return phash
                    except Exception as commit_exc:
                        last_exc = commit_exc
                        time.sleep(0.25 * (2 ** attempt))
                raise last_exc
        except Exception as e:
            msg = str(e).lower()
            if isinstance(e, sqlite3.OperationalError) or 'readonly' in msg:
                _db_readonly_detected = True
                logger.error("Database appears to be in read-only mode; cannot persist phash for %s. ", filename)
            else:
                logger.exception("Failed to persist phash for %s: %s", filename, e)
            return None

    except Exception as e:
        logger.exception("Exception in compute_and_persist_phash for %s: %s", filename, e)
        return None
