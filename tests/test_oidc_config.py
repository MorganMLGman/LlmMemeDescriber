"""Tests for OIDC configuration and validation."""

import pytest
from pydantic import ValidationError

from llm_memedescriber.config import Settings
from tests._helpers import create_oidc_settings


class TestOIDCBasicValidation:
    """Test basic OIDC mode validation."""

    def test_oidc_enabled_basic(self):
        """OIDC mode can be enabled with minimum required settings."""
        s = create_oidc_settings()
        assert s.oidc_enabled is True
        assert s.public_mode is False
        assert s.basic_auth is False

    def test_oidc_with_custom_scopes(self):
        """OIDC mode can be configured with custom scopes."""
        s = create_oidc_settings({'oidc_scopes': 'openid profile email offline_access'})
        assert s.oidc_scopes == "openid profile email offline_access"

    def test_oidc_default_scopes(self):
        """OIDC mode uses default scopes if not specified."""
        s = create_oidc_settings()
        assert s.oidc_scopes == "openid profile email"

    def test_oidc_ssl_verification_enabled_by_default(self):
        """OIDC SSL verification is enabled by default."""
        s = create_oidc_settings()
        assert s.oidc_verify_ssl is True

    def test_oidc_ssl_verification_can_be_disabled(self):
        """OIDC SSL verification can be disabled for testing."""
        s = create_oidc_settings({'oidc_verify_ssl': False})
        assert s.oidc_verify_ssl is False

    def test_oidc_with_ca_bundle_path(self):
        """OIDC can be configured with custom CA bundle path."""
        s = create_oidc_settings({'oidc_ca_bundle_path': '/etc/ssl/certs/custom-ca.pem'})
        assert s.oidc_ca_bundle_path == "/etc/ssl/certs/custom-ca.pem"


class TestOIDCMutualExclusion:
    """Test that OIDC mode is mutually exclusive with other auth modes."""

    def test_oidc_and_public_mode_conflict(self):
        """Cannot enable both OIDC and public mode."""
        with pytest.raises(ValidationError) as exc:
            Settings(
                oidc_enabled=True,
                public_mode=True,
                oidc_provider_url="https://auth.example.com",
                oidc_client_id="client123",
                oidc_client_secret="secret123",
                oidc_redirect_uri="https://app.example.com/callback",
                jwt_secret="jwt-secret-key",
            )
        assert "Only one authentication mode" in str(exc.value)

    def test_oidc_and_basic_auth_conflict(self):
        """Cannot enable both OIDC and basic auth."""
        with pytest.raises(ValidationError) as exc:
            Settings(
                oidc_enabled=True,
                basic_auth=True,
                oidc_provider_url="https://auth.example.com",
                oidc_client_id="client123",
                oidc_client_secret="secret123",
                oidc_redirect_uri="https://app.example.com/callback",
                jwt_secret="jwt-secret-key",
            )
        # Should fail because only one authentication mode can be enabled
        assert "Only one authentication mode" in str(exc.value)


class TestOIDCJWTConfiguration:
    """Test JWT configuration for OIDC mode."""

    def test_oidc_requires_jwt_secret(self):
        """OIDC mode should work with jwt_secret."""
        s = create_oidc_settings({'jwt_secret': 'my-secret-key'})
        assert s.jwt_secret.get_secret_value() == "my-secret-key"

    def test_oidc_with_default_jwt_expiry(self):
        """OIDC uses default JWT expiry if not specified."""
        s = create_oidc_settings()
        assert s.jwt_expiry_days == 30

    def test_oidc_with_custom_jwt_expiry(self):
        """OIDC JWT expiry can be customized."""
        s = create_oidc_settings({'jwt_expiry_days': 60})
        assert s.jwt_expiry_days == 60


class TestOIDCURLValidation:
    """Test OIDC provider URL validation."""

    def test_oidc_provider_url_formats(self):
        """OIDC provider URL can be various formats."""
        urls = [
            "https://auth.example.com",
            "https://auth.example.com/",
            "https://auth.example.com:8443",
            "https://subdomain.auth.example.com",
        ]
        for url in urls:
            s = create_oidc_settings({'oidc_provider_url': url})
            assert s.oidc_provider_url == url

    def test_oidc_redirect_uri_formats(self):
        """OIDC redirect URI can be various formats."""
        uris = [
            "https://app.example.com/callback",
            "http://localhost:8000/callback",
            "https://app.example.com:443/auth/callback",
        ]
        for uri in uris:
            s = create_oidc_settings({'oidc_redirect_uri': uri})
            assert s.oidc_redirect_uri == uri


class TestOIDCWithOtherSettings:
    """Test OIDC mode combined with other settings."""

    def test_oidc_with_logging_settings(self):
        """OIDC mode can be combined with logging settings."""
        s = create_oidc_settings({'logging_level': 'DEBUG'})
        assert s.logging_level == "DEBUG"
        assert s.oidc_enabled is True

    def test_oidc_with_ssl_settings(self):
        """OIDC mode can be combined with SSL settings."""
        s = create_oidc_settings({
            'ssl_cert_file': '/path/to/cert.pem',
            'ssl_key_file': '/path/to/key.pem',
            'ssl_hostname': 'app.example.com',
        })
        assert s.ssl_hostname == "app.example.com"
        assert s.oidc_enabled is True

    def test_oidc_with_webdav_settings(self):
        """OIDC mode can be combined with WebDAV settings."""
        s = create_oidc_settings({
            'webdav_url': 'https://webdav.example.com',
            'webdav_username': 'user',
            'webdav_password': 'pass',
            'webdav_path': '/memes',
        })
        assert s.webdav_url == "https://webdav.example.com"
        assert s.oidc_enabled is True
