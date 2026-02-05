"""Tests for Redis configuration settings."""
import builtins
import os
import pytest
from unittest.mock import patch, MagicMock

from llm_memedescriber.config import Settings
from tests._helpers import make_fake_open


class TestRedisConfig:
    """Test Redis configuration loading."""
    
    def test_redis_url_from_env(self):
        """Test loading redis_url from environment."""
        s = Settings(
            public_mode=True,
            redis_url="redis://localhost:6379/0",
            redis_password="test_password"
        )
        assert s.redis_url == "redis://localhost:6379/0"
    
    def test_redis_password_from_env(self):
        """Test loading redis_password from environment."""
        s = Settings(
            public_mode=True,
            redis_password="test_password"
        )
        assert s.redis_password.get_secret_value() == "test_password"
    
    def test_redis_password_optional(self):
        """Test that redis_password is optional."""
        s = Settings(public_mode=True)
        assert s.redis_password is None
    
    def test_redis_url_optional(self):
        """Test that redis_url is optional."""
        s = Settings(public_mode=True)
        assert s.redis_url is None
    
    def test_redis_url_with_password(self):
        """Test Redis URL with separate password field."""
        s = Settings(
            public_mode=True,
            redis_url="redis://localhost:6379/0",
            redis_password="test_password"
        )
        assert "localhost" in s.redis_url
        assert s.redis_password.get_secret_value() == "test_password"
    
    def test_redis_url_requires_password(self):
        """Test that redis_password is required when redis_url is set."""
        from pydantic import ValidationError
        
        with pytest.raises(ValidationError, match="redis_password is required"):
            Settings(
                public_mode=True,
                redis_url="redis://localhost:6379/0"
                # Missing redis_password
            )

