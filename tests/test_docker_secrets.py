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


def test_whitespace_only_secret_fallbacks_to_env(monkeypatch):
    secret_path = "/run/secrets/google_genai_api_key"
    monkeypatch.setattr(os.path, "isfile", lambda p: os.path.normpath(p) == os.path.normpath(secret_path))
    monkeypatch.setattr(builtins, "open", _make_fake_open(secret_path, "   \n\t  \n"))

    s = Settings(google_genai_api_key="env-value")
    assert s.google_genai_api_key == "env-value"


def test_open_raises_falls_back_to_env(monkeypatch):
    secret_path = "/run/secrets/google_genai_api_key"
    monkeypatch.setattr(os.path, "isfile", lambda p: os.path.normpath(p) == os.path.normpath(secret_path))

    real_open = builtins.open

    def raising_open(path, mode='r', encoding=None, *args, **kwargs):
        if os.path.normpath(path) == os.path.normpath(secret_path):
            raise PermissionError("denied")
        return real_open(path, mode, encoding=encoding, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", raising_open)

    s = Settings(google_genai_api_key="env-value")
    assert s.google_genai_api_key == "env-value"


def test_both_upper_and_lower_present_prefers_upper(monkeypatch):
    upper_path = "/run/secrets/GOOGLE_GENAI_API_KEY"
    lower_path = "/run/secrets/google_genai_api_key"

    def isfile(p):
        norm = os.path.normpath(p)
        return norm in (os.path.normpath(upper_path), os.path.normpath(lower_path))

    def fake_open(path, mode='r', encoding=None, *args, **kwargs):
        norm = os.path.normpath(path)
        if norm == os.path.normpath(upper_path):
            return io.StringIO("UPPER-SECRET")
        if norm == os.path.normpath(lower_path):
            return io.StringIO("lower-secret")
        return builtins.open(path, mode, encoding=encoding, *args, **kwargs)

    monkeypatch.setattr(os.path, "isfile", isfile)
    monkeypatch.setattr(builtins, "open", fake_open)

    s = Settings(google_genai_api_key="env-value")
    assert s.google_genai_api_key == "UPPER-SECRET"


def test_multiple_fields_read_from_secrets(monkeypatch):
    key_path = "/run/secrets/google_genai_api_key"
    pass_path = "/run/secrets/webdav_password"

    def isfile(p):
        norm = os.path.normpath(p)
        return norm in (os.path.normpath(key_path), os.path.normpath(pass_path))

    def fake_open(path, mode='r', encoding=None, *args, **kwargs):
        norm = os.path.normpath(path)
        if norm == os.path.normpath(key_path):
            return io.StringIO("KEY-SECRET")
        if norm == os.path.normpath(pass_path):
            return io.StringIO("PASS-SECRET")
        return builtins.open(path, mode, encoding=encoding, *args, **kwargs)

    monkeypatch.setattr(os.path, "isfile", isfile)
    monkeypatch.setattr(builtins, "open", fake_open)

    s = Settings(google_genai_api_key="env-value", webdav_password="env-pass")
    assert s.google_genai_api_key == "KEY-SECRET"
    assert s.webdav_password == "PASS-SECRET"


def test_no_secret_and_env_none_results_in_none(monkeypatch):
    monkeypatch.setattr(os.path, "isfile", lambda p: False)
    s = Settings()
    assert s.webdav_password is None
    assert s.google_genai_api_key is None
