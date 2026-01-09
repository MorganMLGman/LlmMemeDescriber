import io
import builtins
import os
import logging

import pytest
from pydantic import ValidationError
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import llm_memedescriber.config as config
from llm_memedescriber.config import Settings, parse_interval


from tests._helpers import make_fake_open


def test_max_generation_attempts_zero_raises():
    with pytest.raises(ValidationError):
        Settings(max_generation_attempts=0)


def test_max_generation_attempts_string_is_int():
    s = Settings(max_generation_attempts="5")
    assert isinstance(s.max_generation_attempts, int)
    assert s.max_generation_attempts == 5


def test_export_listing_interval_invalid_rejected():
    with pytest.raises(ValidationError):
        Settings(export_listing_interval="5d")


def test_export_listing_interval_rejects_empty_and_none():
    with pytest.raises(ValidationError):
        Settings(export_listing_interval="")
    with pytest.raises(ValidationError):
        Settings(export_listing_interval=None)


def test_webdav_secrets_prefer_secret_over_env(monkeypatch):
    secret_path = "/run/secrets/webdav_password"
    monkeypatch.setattr(os.path, "isfile", lambda p: os.path.normpath(p) == os.path.normpath(secret_path))
    monkeypatch.setattr(builtins, "open", make_fake_open(secret_path, "super-secret\n"))

    s = Settings(webdav_password="env-pass")
    assert s.webdav_password == "super-secret"


def test_load_settings_exits_on_validation_error(monkeypatch, caplog):
    monkeypatch.setenv('MAX_GENERATION_ATTEMPTS', '0')

    caplog.set_level(logging.ERROR)
    with pytest.raises(SystemExit):
        config.load_settings()
    assert any('Configuration error' in r.message for r in caplog.records)


def test_max_generation_attempts_non_numeric_raises():
    with pytest.raises(ValidationError):
        Settings(max_generation_attempts="abc")


def test_max_generation_attempts_float_string_is_accepted_and_cast():
    s = Settings(max_generation_attempts="1.0")
    assert isinstance(s.max_generation_attempts, int)
    assert s.max_generation_attempts == 1


def test_max_generation_attempts_negative_string_raises():
    with pytest.raises(ValidationError):
        Settings(max_generation_attempts="-1")



def test_max_generation_attempts_large_value_raises():
    with pytest.raises(ValidationError):
        Settings(max_generation_attempts=10_000_000)


def test_run_interval_rejects_whitespace_string():
    with pytest.raises(ValidationError):
        Settings(run_interval="   ")

def test_run_interval_rejects_none():
    with pytest.raises(ValidationError) as exc:
        Settings(run_interval=None)
    assert "None" in str(exc.value)

def test_run_interval_parses_valid_string():
    s = Settings(run_interval="10m")
    assert s.run_interval == "10m"


def test_export_listing_interval_rejects_whitespace():
    with pytest.raises(ValidationError):
        Settings(export_listing_interval="   ")



def test_secret_read_unicode_error_fallback(monkeypatch):
    secret_path = "/run/secrets/google_genai_api_key"
    monkeypatch.setattr(os.path, "isfile", lambda p: os.path.normpath(p) == os.path.normpath(secret_path))

    class BadReader:
        def read(self):
            raise UnicodeDecodeError("utf-8", b"", 0, 1, "invalid")

    def fake_open(path, mode='r', encoding=None, *args, **kwargs):
        if os.path.normpath(path) == os.path.normpath(secret_path):
            return BadReader()
        return builtins.open(path, mode, encoding=encoding, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", fake_open)
    s = Settings(google_genai_api_key="env-value")
    assert s.google_genai_api_key == "env-value"


def test_upper_secret_empty_prefers_lower(monkeypatch):
    upper_path = "/run/secrets/GOOGLE_GENAI_API_KEY"
    lower_path = "/run/secrets/google_genai_api_key"

    def isfile(p):
        return os.path.normpath(p) in (os.path.normpath(upper_path), os.path.normpath(lower_path))

    def fake_open(path, mode='r', encoding=None, *args, **kwargs):
        norm = os.path.normpath(path)
        if norm == os.path.normpath(upper_path):
            return io.StringIO("   \n")
        if norm == os.path.normpath(lower_path):
            return io.StringIO("lower-secret")
        return builtins.open(path, mode, encoding=encoding, *args, **kwargs)

    monkeypatch.setattr(os.path, "isfile", isfile)
    monkeypatch.setattr(builtins, "open", fake_open)

    s = Settings(google_genai_api_key="env-value")
    assert s.google_genai_api_key == "lower-secret"


def test_env_empty_string_preserved(monkeypatch):
    monkeypatch.setattr(os.path, "isfile", lambda p: False)
    s = Settings(webdav_password="")
    assert s.webdav_password == ""


def test_config_raises_when_run_interval_none(monkeypatch):
    monkeypatch.setattr(os.path, "isfile", lambda p: False)
    with pytest.raises(ValueError):
        Settings(run_interval=None)


def test_local_iso_formatter_uses_timezone():
    ZoneInfo('UTC')

    fmt = config.LocalISOFormatter(tz_name='UTC')
    record = logging.LogRecord(name="test", level=logging.INFO, pathname=__file__, lineno=1, msg="x", args=(), exc_info=None)
    record.created = 0.0
    s = fmt.formatTime(record)
    assert s.startswith('1970-01-01T00:00:00.')
    assert s.endswith('+00:00')
