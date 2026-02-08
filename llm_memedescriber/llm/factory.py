"""Factory for creating and managing LLM provider instances."""

import logging
from typing import Optional, Dict, Type, Any

from .base import LLMProvider

logger = logging.getLogger(__name__)

# Provider registry - populated lazily to avoid circular imports
_PROVIDER_REGISTRY: Dict[str, Type[LLMProvider]] = {}
_client_instance: Optional[LLMProvider] = None


def _lazy_load_providers():
    """Lazy load provider classes to avoid circular imports."""
    global _PROVIDER_REGISTRY

    if _PROVIDER_REGISTRY:
        return  # Already loaded

    try:
        from .providers.gemini import GeminiProvider
        _PROVIDER_REGISTRY['gemini'] = GeminiProvider
    except ImportError as e:
        logger.warning(f"Failed to import GeminiProvider: {e}")

    try:
        from .providers.openai_gpt import OpenAIProvider
        _PROVIDER_REGISTRY['openai'] = OpenAIProvider
    except ImportError as e:
        logger.debug(f"OpenAI provider not available: {e}")

    try:
        from .providers.anthropic_claude import ClaudeProvider
        _PROVIDER_REGISTRY['anthropic'] = ClaudeProvider
    except ImportError as e:
        logger.debug(f"Anthropic provider not available: {e}")


def get_client(
    provider: str,
    api_key: Optional[str],
    model: str,
    config: Optional[Any] = None
) -> Optional[LLMProvider]:
    """Get or create a singleton LLM provider client.

    Args:
        provider: Provider name ('gemini', 'openai', 'anthropic')
        api_key: API key for authentication
        model: Model identifier
        config: Optional provider-specific configuration

    Returns:
        LLMProvider instance or None if api_key is falsy
    """
    global _client_instance

    if not api_key:
        logger.warning("No API key provided, returning None")
        return None

    if _client_instance is None:
        _lazy_load_providers()

        logger.info(f"Creating new {provider} provider instance")
        provider_class = _PROVIDER_REGISTRY.get(provider.lower())

        if provider_class is None:
            available = list(_PROVIDER_REGISTRY.keys())
            logger.error(f"Unknown provider: {provider}. Available: {available}")
            return None

        try:
            _client_instance = provider_class(
                api_key=api_key,
                model=model,
                config=config
            )
            logger.debug(f"Created {provider} client singleton")
        except Exception as exc:
            logger.exception(f"Failed to create {provider} client: {exc}")
            _client_instance = None

    return _client_instance


def clear_client():
    """Clear the singleton (mainly for testing)."""
    global _client_instance
    _client_instance = None
    logger.debug("Cleared LLM client singleton")


def register_provider(name: str, provider_class: Type[LLMProvider]):
    """Register a custom provider (for extensions).

    Args:
        name: Provider name (e.g., 'custom')
        provider_class: Provider class that extends LLMProvider
    """
    _lazy_load_providers()
    _PROVIDER_REGISTRY[name.lower()] = provider_class
    logger.info(f"Registered provider: {name}")


def get_available_providers() -> list:
    """Get list of available provider names."""
    _lazy_load_providers()
    return list(_PROVIDER_REGISTRY.keys())
