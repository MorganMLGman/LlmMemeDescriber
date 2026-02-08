"""LLM provider abstraction layer.

This package provides a unified interface for multiple LLM providers
(Gemini, OpenAI, Anthropic, etc.) with consistent error handling and
response formats.
"""

from .factory import get_client, clear_client, register_provider, get_available_providers
from .base import LLMProvider
from .types import MediaContent, DescriptionRequest, DescriptionResponse
from .exceptions import (
    LLMProviderError,
    RateLimitError,
    UnsupportedMediaError,
    InvalidResponseError
)

__all__ = [
    'get_client',
    'clear_client',
    'register_provider',
    'get_available_providers',
    'LLMProvider',
    'MediaContent',
    'DescriptionRequest',
    'DescriptionResponse',
    'LLMProviderError',
    'RateLimitError',
    'UnsupportedMediaError',
    'InvalidResponseError',
]
