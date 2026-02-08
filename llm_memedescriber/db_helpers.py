"""Database session helper utilities.

Provides a small contextmanager `session_scope(engine)` to centralize
creation/cleanup of `sqlmodel.Session` instances so callers don't repeat
`with Session(engine) as session:` everywhere.
"""
from contextlib import contextmanager
from typing import Iterator, Optional, Union
import datetime
import json

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


def log_audit_action(
    engine,
    user_id: str,
    action: str,
    resource: Optional[str] = None,
    resource_type: str = "meme",
    details: Optional[Union[str, dict]] = None,
    ip_address: Optional[str] = None,
    status: str = "success",
    username: Optional[str] = None
) -> None:
    """Log a sensitive action to the AuditLog table.
    
    Args:
        engine: SQLModel engine
        user_id: User who performed the action (OIDC sub or user ID)
        action: Action type (e.g., "DELETE_MEME", "MERGE_DUPLICATES")
        resource: Resource identifier (e.g., filename, group_id). Optional for actions without specific resource.
        resource_type: Type of resource (default: "meme")
        details: Additional context (dict, JSON string or plain text). Dicts are auto-converted to JSON.
        ip_address: IP address of the request (optional)
        status: Action status (default: "success")
        username: Human-readable username from OIDC preferred_username (optional)
    """
    from .models import AuditLog
    
    # Convert dict to JSON string if needed
    if isinstance(details, dict):
        try:
            details = json.dumps(details)
        except (TypeError, ValueError) as e:
            logger.warning(f"Failed to serialize details dict to JSON: {e}, using str() instead")
            details = str(details)
    
    try:
        with session_scope(engine) as session:
            audit_entry = AuditLog(
                user_id=user_id,
                username=username,
                action=action,
                resource=resource,
                resource_type=resource_type,
                details=details,
                ip_address=ip_address,
                status=status,
                timestamp=datetime.datetime.now(datetime.timezone.utc)
            )
            session.add(audit_entry)
            session.commit()
            logger.debug(f"Audit log: {action} on {resource} by {user_id}")
    except Exception as e:
        logger.error(f"Failed to log audit action: {e}")
        # Don't raise - audit logging failure shouldn't break the app

