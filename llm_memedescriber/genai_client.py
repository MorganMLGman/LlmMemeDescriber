"""Centralized GenAI client factory and singleton.

Create the google.genai Client once per process and reuse for all generation calls.
"""
import logging
from typing import Optional

from google import genai as _genai

logger = logging.getLogger(__name__)

_client = None


def get_client(api_key: Optional[str]):
    """Return a singleton google.genai client for the given API key.

    If api_key is falsy, returns None.
    If client already exists, returns the same instance (ignores api_key mismatch).
    """
    global _client
    if not api_key:
        return None
    if _client is None:
        try:
            _client = _genai.Client(api_key=api_key)
            logger.debug("Created GenAI client singleton")
        except Exception as exc:
            logger.exception("Failed to create GenAI client: %s", exc)
            _client = None
    return _client


def clear_client():
    """Clear the singleton (mainly for testing)."""
    global _client
    _client = None
    logger.debug("Cleared GenAI client singleton")
