"""Database session helper utilities.

Provides a small contextmanager `session_scope(engine)` to centralize
creation/cleanup of `sqlmodel.Session` instances so callers don't repeat
`with Session(engine) as session:` everywhere.
"""
from contextlib import contextmanager
from typing import Iterator

from sqlmodel import Session
import logging

logger = logging.getLogger(__name__)


@contextmanager
def session_scope(engine) -> Iterator[Session]:
    """Yield a short-lived SQLModel `Session` bound to `engine`.

    Caller is responsible for committing when appropriate. Session is
    always closed on exit.
    """
    sess = Session(engine)
    logger.debug("Opening DB session %s", sess)
    try:
        yield sess
    finally:
        try:
            sess.close()
            logger.debug("Closed DB session %s", sess)
        except Exception as e:
            logger.exception("Failed to close DB session: %s", e)
