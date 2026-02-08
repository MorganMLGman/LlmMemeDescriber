"""Custom exceptions for LLM provider operations."""


class LLMProviderError(Exception):
    """Base exception for LLM provider errors."""
    pass


class RateLimitError(LLMProviderError):
    """Raised when API rate limit is exceeded."""
    pass


class UnsupportedMediaError(LLMProviderError):
    """Raised when media type is not supported by provider."""
    pass


class InvalidResponseError(LLMProviderError):
    """Raised when provider response cannot be parsed."""
    pass
