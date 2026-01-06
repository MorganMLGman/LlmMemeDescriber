import builtins
import io
import os

import pytest

from llm_memedescriber.config import Settings


def _make_fake_open(secret_path: str, secret_content: str):
    real_open = builtins.open

    def fake_open(path, mode='r', encoding=None, *args, **kwargs):
        if os.path.normpath(path) == os.path.normpath(secret_path):
            return io.StringIO(secret_content)
        return real_open(path, mode, encoding=encoding, *args, **kwargs)

    return fake_open


def test_secret_over_env(monkeypatch):
    secret_path = "/run/secrets/google_genai_api_key"
    monkeypatch.setattr(os.path, "isfile", lambda p: os.path.normpath(p) == os.path.normpath(secret_path))
    monkeypatch.setattr(builtins, "open", _make_fake_open(secret_path, "secret-value"))

    s = Settings(google_genai_api_key="env-value")
    assert s.google_genai_api_key == "secret-value"


def test_env_if_no_secret(monkeypatch):
    monkeypatch.setattr(os.path, "isfile", lambda p: False)
    s = Settings(google_genai_api_key="env-value")
    assert s.google_genai_api_key == "env-value"


def test_empty_secret_fallbacks_to_env(monkeypatch):
    secret_path = "/run/secrets/google_genai_api_key"
    monkeypatch.setattr(os.path, "isfile", lambda p: os.path.normpath(p) == os.path.normpath(secret_path))
    monkeypatch.setattr(builtins, "open", _make_fake_open(secret_path, ""))

    s = Settings(google_genai_api_key="env-value")
    assert s.google_genai_api_key == "env-value"


def test_uppercase_secret(monkeypatch):
    secret_path = "/run/secrets/GOOGLE_GENAI_API_KEY"
    monkeypatch.setattr(os.path, "isfile", lambda p: os.path.normpath(p) == os.path.normpath(secret_path))
    monkeypatch.setattr(builtins, "open", _make_fake_open(secret_path, "upper-secret"))

    s = Settings(google_genai_api_key="env-value")
    assert s.google_genai_api_key == "upper-secret"


def test_secret_with_special_characters(monkeypatch):
    """Secret contains newlines, tabs, backslashes, quotes, unicode, and other special chars."""
    secret_path = "/run/secrets/google_genai_api_key"
    special = (
        "  leading-space\n"
        "line1\n"
        "line2\twith\ttabs\\backslashes\"quotes'!@#$%^&*()_+-=[]{};:<>?/"
        "\nunicode: ☃️🌟\n\n"
    )
    monkeypatch.setattr(os.path, "isfile", lambda p: os.path.normpath(p) == os.path.normpath(secret_path))
    monkeypatch.setattr(builtins, "open", _make_fake_open(secret_path, special))

    s = Settings(google_genai_api_key="env-value")
    assert s.google_genai_api_key == special.strip()
