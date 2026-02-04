"""Tests for OIDC secrets handling and Docker secrets integration."""

import builtins
import io
import os

import pytest

from llm_memedescriber.config import Settings
from tests._helpers import make_fake_open, setup_oidc_secrets_monkeypatch, create_oidc_settings


class TestOIDCClientSecretHandling:
    """Test OIDC client secret reading from Docker secrets and environment."""

    def test_oidc_client_secret_from_docker_secret(self, monkeypatch):
        """OIDC client secret can be read from Docker secrets."""
        setup_oidc_secrets_monkeypatch(monkeypatch, {"/run/secrets/oidc_client_secret": "docker-client-secret"})
        s = create_oidc_settings()
        assert s.oidc_client_secret.get_secret_value() == "docker-client-secret"

    def test_oidc_client_secret_from_env_if_no_docker_secret(self, monkeypatch):
        """OIDC client secret falls back to environment if no Docker secret."""
        monkeypatch.setattr(os.path, "isfile", lambda p: False)
        s = create_oidc_settings()
        assert s.oidc_client_secret.get_secret_value() == "env-client-secret"

    def test_oidc_client_secret_empty_docker_secret_fallbacks_to_env(self, monkeypatch):
        """Empty OIDC client secret in Docker secrets falls back to environment."""
        setup_oidc_secrets_monkeypatch(monkeypatch, {"/run/secrets/oidc_client_secret": ""})
        s = create_oidc_settings()
        assert s.oidc_client_secret.get_secret_value() == "env-client-secret"

    def test_oidc_client_secret_whitespace_only_fallbacks_to_env(self, monkeypatch):
        """Whitespace-only OIDC client secret falls back to environment."""
        setup_oidc_secrets_monkeypatch(monkeypatch, {"/run/secrets/oidc_client_secret": "   \n\t  \n"})
        s = create_oidc_settings()
        assert s.oidc_client_secret.get_secret_value() == "env-client-secret"

    def test_oidc_client_secret_unicode_error_fallback(self, monkeypatch):
        """OIDC client secret with Unicode error falls back to environment."""
        secret_path = "/run/secrets/oidc_client_secret"
        monkeypatch.setattr(os.path, "isfile", lambda p: os.path.normpath(p) == os.path.normpath(secret_path))

        class BadReader:
            def read(self):
                raise UnicodeDecodeError("utf-8", b"", 0, 1, "invalid")

        def fake_open(path, mode='r', encoding=None, *args, **kwargs):
            if os.path.normpath(path) == os.path.normpath(secret_path):
                return BadReader()
            return builtins.open(path, mode, encoding=encoding, *args, **kwargs)

        monkeypatch.setattr(builtins, "open", fake_open)
        s = create_oidc_settings()
        assert s.oidc_client_secret.get_secret_value() == "env-client-secret"

    def test_oidc_client_secret_special_characters(self, monkeypatch):
        """OIDC client secret can contain special characters."""
        special = "secret-!@#$%^&*()_+{}[]|:;<>?,./~`"
        setup_oidc_secrets_monkeypatch(monkeypatch, {"/run/secrets/oidc_client_secret": special})
        s = create_oidc_settings()
        assert s.oidc_client_secret.get_secret_value() == special.strip()

    def test_oidc_client_secret_multiline(self, monkeypatch):
        """OIDC client secret can handle multiline content."""
        multiline = "line1\nline2\nline3"
        setup_oidc_secrets_monkeypatch(monkeypatch, {"/run/secrets/oidc_client_secret": multiline})
        s = create_oidc_settings()
        assert s.oidc_client_secret.get_secret_value() == multiline.strip()


class TestJWTSecretHandling:
    """Test JWT secret reading from Docker secrets and environment."""

    def test_jwt_secret_from_docker_secret(self, monkeypatch):
        """JWT secret can be read from Docker secrets."""
        setup_oidc_secrets_monkeypatch(monkeypatch, {"/run/secrets/jwt_secret": "docker-jwt-secret-key"})
        s = create_oidc_settings({'oidc_client_secret': 'client-secret'})
        assert s.jwt_secret.get_secret_value() == "docker-jwt-secret-key"

    def test_jwt_secret_from_env_if_no_docker_secret(self, monkeypatch):
        """JWT secret falls back to environment if no Docker secret."""
        monkeypatch.setattr(os.path, "isfile", lambda p: False)
        s = create_oidc_settings({'oidc_client_secret': 'client-secret'})
        assert s.jwt_secret.get_secret_value() == "env-jwt-secret"

    def test_jwt_secret_empty_docker_secret_fallbacks_to_env(self, monkeypatch):
        """Empty JWT secret in Docker secrets falls back to environment."""
        setup_oidc_secrets_monkeypatch(monkeypatch, {"/run/secrets/jwt_secret": ""})
        s = create_oidc_settings({'oidc_client_secret': 'client-secret'})
        assert s.jwt_secret.get_secret_value() == "env-jwt-secret"

    def test_jwt_secret_special_characters(self, monkeypatch):
        """JWT secret can contain special characters."""
        special = "jwt_key_!@#$%^&*()_+-={}[]|:;<>?,./~`eyJ"
        setup_oidc_secrets_monkeypatch(monkeypatch, {"/run/secrets/jwt_secret": special})
        s = create_oidc_settings({'oidc_client_secret': 'client-secret'})
        assert s.jwt_secret.get_secret_value() == special.strip()

    def test_jwt_secret_base64_encoded(self, monkeypatch):
        """JWT secret can be base64 encoded."""
        base64_secret = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9eyJzdWIiOiIxMjM0NTY3ODkwIn0SflKw0DjqcALcgF3XgKO8"
        setup_oidc_secrets_monkeypatch(monkeypatch, {"/run/secrets/jwt_secret": base64_secret})
        s = create_oidc_settings({'oidc_client_secret': 'client-secret'})
        assert s.jwt_secret.get_secret_value() == base64_secret


class TestOIDCMultipleSecretsHandling:
    """Test handling multiple OIDC secrets simultaneously."""

    def test_oidc_both_secrets_from_docker(self, monkeypatch):
        """Both OIDC client secret and JWT secret can be read from Docker secrets."""
        setup_oidc_secrets_monkeypatch(monkeypatch, {
            "/run/secrets/oidc_client_secret": "docker-client-secret",
            "/run/secrets/jwt_secret": "docker-jwt-secret"
        })
        s = create_oidc_settings()
        assert s.oidc_client_secret.get_secret_value() == "docker-client-secret"
        assert s.jwt_secret.get_secret_value() == "docker-jwt-secret"

    def test_oidc_client_secret_from_docker_jwt_from_env(self, monkeypatch):
        """Client secret from Docker, JWT secret from env."""
        setup_oidc_secrets_monkeypatch(monkeypatch, {
            "/run/secrets/oidc_client_secret": "docker-client-secret"
        })
        s = create_oidc_settings()
        assert s.oidc_client_secret.get_secret_value() == "docker-client-secret"
        assert s.jwt_secret.get_secret_value() == "env-jwt-secret"

    def test_oidc_client_secret_from_env_jwt_from_docker(self, monkeypatch):
        """Client secret from env, JWT secret from Docker."""
        setup_oidc_secrets_monkeypatch(monkeypatch, {
            "/run/secrets/jwt_secret": "docker-jwt-secret"
        })
        s = create_oidc_settings()
        assert s.oidc_client_secret.get_secret_value() == "env-client-secret"
        assert s.jwt_secret.get_secret_value() == "docker-jwt-secret"

    def test_oidc_both_secrets_empty_docker_fallback_to_env(self, monkeypatch):
        """Both secrets empty in Docker, fallback to environment."""
        setup_oidc_secrets_monkeypatch(monkeypatch, {
            "/run/secrets/oidc_client_secret": "   \n   ",
            "/run/secrets/jwt_secret": "   \n   "
        })
        s = create_oidc_settings()
        assert s.oidc_client_secret.get_secret_value() == "env-client-secret"
        assert s.jwt_secret.get_secret_value() == "env-jwt-secret"


class TestOIDCSecretsCaseInsensitivity:
    """Test OIDC secrets with uppercase/lowercase variants."""

    def test_oidc_client_secret_uppercase_preferred(self, monkeypatch):
        """Uppercase OIDC_CLIENT_SECRET is preferred over lowercase."""
        setup_oidc_secrets_monkeypatch(monkeypatch, {
            "/run/secrets/OIDC_CLIENT_SECRET": "UPPER-SECRET",
            "/run/secrets/oidc_client_secret": "lower-secret"
        })
        s = create_oidc_settings()
        assert s.oidc_client_secret.get_secret_value() == "UPPER-SECRET"

    def test_oidc_client_secret_uppercase_empty_prefers_lowercase(self, monkeypatch):
        """If uppercase OIDC_CLIENT_SECRET is empty, lowercase is used."""
        setup_oidc_secrets_monkeypatch(monkeypatch, {
            "/run/secrets/OIDC_CLIENT_SECRET": "   \n",
            "/run/secrets/oidc_client_secret": "lower-secret"
        })
        s = create_oidc_settings()
        assert s.oidc_client_secret.get_secret_value() == "lower-secret"

    def test_jwt_secret_uppercase_preferred(self, monkeypatch):
        """Uppercase JWT_SECRET is preferred over lowercase."""
        setup_oidc_secrets_monkeypatch(monkeypatch, {
            "/run/secrets/JWT_SECRET": "UPPER-JWT-SECRET",
            "/run/secrets/jwt_secret": "lower-jwt-secret"
        })
        s = create_oidc_settings({'oidc_client_secret': 'client-secret'})
        assert s.jwt_secret.get_secret_value() == "UPPER-JWT-SECRET"


class TestOIDCSecretsNoneHandling:
    """Test OIDC secrets with None values."""

    def test_oidc_client_secret_none_if_not_provided(self, monkeypatch):
        """OIDC client secret can be None if not provided and no Docker secret."""
        monkeypatch.setattr(os.path, "isfile", lambda p: False)
        s = create_oidc_settings({'oidc_client_secret': None})
        assert s.oidc_client_secret is None

    def test_jwt_secret_none_if_not_provided(self, monkeypatch):
        """JWT secret can be None if not provided and no Docker secret."""
        monkeypatch.setattr(os.path, "isfile", lambda p: False)
        s = create_oidc_settings({'jwt_secret': None})
        assert s.jwt_secret is None
