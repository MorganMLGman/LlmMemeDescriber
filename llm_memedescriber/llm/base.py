"""Abstract base class for all LLM providers."""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional

from .types import DescriptionRequest


class LLMProvider(ABC):
    """Abstract base class for all LLM providers."""

    def __init__(self, api_key: str, model: str, config: Optional[Any] = None):
        """Initialize the provider.

        Args:
            api_key: API key for authentication
            model: Model identifier (e.g., 'gemini-3-flash-preview', 'gpt-4o', 'claude-3-5-sonnet')
            config: Provider-specific configuration
        """
        self.api_key = api_key
        self.model = model
        self.config = config or self._default_config()
        self._client = None

    @abstractmethod
    def _default_config(self) -> Any:
        """Return default configuration for this provider."""
        pass

    @abstractmethod
    def _initialize_client(self) -> Any:
        """Initialize the provider-specific client."""
        pass

    @abstractmethod
    def generate_description(self, request: DescriptionRequest) -> Dict[str, Any]:
        """Generate a description for the given media.

        Args:
            request: DescriptionRequest containing media and prompt

        Returns:
            Dict with keys: 'kategoria', 'opis', 'keywordy', 'tekst'
            or special dicts:
            - {'rate_limited': True, 'error': 'Rate limit exceeded'} on 429
            - {} (empty dict) on other errors

        Raises:
            LLMProviderError: On provider-specific errors
            UnsupportedMediaError: When media type is not supported
            RateLimitError: When rate limit is exceeded
        """
        pass

    @abstractmethod
    def is_media_supported(self, mime_type: str) -> bool:
        """Check if the given MIME type is supported by this provider."""
        pass

    @property
    def client(self) -> Any:
        """Lazy initialization of client."""
        if self._client is None:
            self._client = self._initialize_client()
        return self._client

    @classmethod
    @abstractmethod
    def provider_name(cls) -> str:
        """Return the provider identifier (e.g., 'gemini', 'openai', 'anthropic')."""
        pass
