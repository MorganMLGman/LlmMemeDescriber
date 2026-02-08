"""Tests for LLM factory module."""

import logging
from unittest.mock import Mock, patch

from llm_memedescriber.llm import get_client, clear_client, get_available_providers


def test_get_client_returns_none_without_api_key():
    """Test that get_client returns None when no API key is provided."""
    clear_client()
    assert get_client('gemini', None, 'gemini-3-flash-preview') is None
    assert get_client('gemini', '', 'gemini-3-flash-preview') is None


def test_get_client_creates_singleton():
    """Test that get_client creates a singleton instance."""
    clear_client()

    mock_provider_class = Mock()
    mock_instance = Mock()
    mock_provider_class.return_value = mock_instance

    with patch('llm_memedescriber.llm.factory._PROVIDER_REGISTRY', {'gemini': mock_provider_class}):
        # First call creates instance
        result1 = get_client('gemini', 'test_key', 'gemini-3-flash-preview')
        assert result1 is mock_instance
        mock_provider_class.assert_called_once_with(
            api_key='test_key',
            model='gemini-3-flash-preview',
            config=None
        )

        # Second call returns same instance
        result2 = get_client('gemini', 'other_key', 'other-model')
        assert result2 is mock_instance
        # Should still only be called once (singleton)
        assert mock_provider_class.call_count == 1


def test_get_client_with_unknown_provider():
    """Test that get_client returns None for unknown provider."""
    clear_client()

    result = get_client('unknown_provider', 'test_key', 'test-model')
    assert result is None


def test_clear_client():
    """Test that clear_client clears the singleton."""
    clear_client()

    mock_provider_class = Mock()
    mock_instance = Mock()
    mock_provider_class.return_value = mock_instance

    with patch('llm_memedescriber.llm.factory._PROVIDER_REGISTRY', {'gemini': mock_provider_class}):
        # Create client
        result1 = get_client('gemini', 'test_key', 'gemini-3-flash-preview')
        assert result1 is mock_instance
        assert mock_provider_class.call_count == 1

        # Clear and create again
        clear_client()
        result2 = get_client('gemini', 'test_key', 'gemini-3-flash-preview')
        assert result2 is mock_instance
        # Should be called again (new instance after clear)
        assert mock_provider_class.call_count == 2


def test_get_client_with_config():
    """Test that get_client passes config to provider."""
    clear_client()

    mock_provider_class = Mock()
    mock_instance = Mock()
    mock_provider_class.return_value = mock_instance
    mock_config = Mock()

    with patch('llm_memedescriber.llm.factory._PROVIDER_REGISTRY', {'gemini': mock_provider_class}):
        result = get_client('gemini', 'test_key', 'gemini-3-flash-preview', mock_config)
        assert result is mock_instance
        mock_provider_class.assert_called_once_with(
            api_key='test_key',
            model='gemini-3-flash-preview',
            config=mock_config
        )


def test_get_client_handles_provider_creation_error():
    """Test that get_client handles errors during provider creation."""
    clear_client()

    mock_provider_class = Mock()
    mock_provider_class.side_effect = Exception("Provider creation failed")

    with patch('llm_memedescriber.llm.factory._PROVIDER_REGISTRY', {'gemini': mock_provider_class}):
        result = get_client('gemini', 'test_key', 'gemini-3-flash-preview')
        assert result is None


def test_get_available_providers():
    """Test that get_available_providers returns list of providers."""
    # Lazy load will populate the registry
    providers = get_available_providers()
    assert isinstance(providers, list)
    # Should have at least Gemini
    assert 'gemini' in providers


def test_provider_registry_lazy_loading():
    """Test that providers are loaded lazily."""
    from llm_memedescriber.llm import factory

    # Clear the registry
    factory._PROVIDER_REGISTRY.clear()
    assert len(factory._PROVIDER_REGISTRY) == 0

    # Get available providers should trigger lazy loading
    providers = get_available_providers()
    assert len(factory._PROVIDER_REGISTRY) > 0
    assert 'gemini' in providers
