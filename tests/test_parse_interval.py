import pytest

from pydantic import ValidationError

from llm_memedescriber.config import parse_interval, Settings


@pytest.mark.parametrize("input,expected", [
    ("5", 5),
    ("10s", 10),
    ("10sec", 10),
    ("10secs", 10),
    ("10second", 10),
    ("10seconds", 10),
    ("2m", 120),
    ("2min", 120),
    ("2mins", 120),
    ("2minute", 120),
    ("2minutes", 120),
    ("1h", 3600),
    ("1hr", 3600),
    ("1hrs", 3600),
    ("1hour", 3600),
    ("1hours", 3600),
    ("  3  m  ", 180),
    ("05s", 5),
    (50, 50),
])
def test_parse_interval_valid(input, expected):
    assert parse_interval(input) == expected


@pytest.mark.parametrize("invalid", ["", "abc", "5d", "1.5h", "   ", None])
def test_parse_interval_invalid(invalid):
    with pytest.raises(ValueError):
        parse_interval(invalid)


def test_parse_interval_zero_rejected_with_message():
    with pytest.raises(ValueError) as exc:
        parse_interval("0")
    assert "positive" in str(exc.value)

    with pytest.raises(ValueError) as exc2:
        parse_interval("0s")
    assert "positive" in str(exc2.value)


@pytest.mark.parametrize("invalid", ["-5", "-1m", "-12345678901234567890", "-0"])
def test_parse_interval_negative_numbers_rejected(invalid):
    # Negative numbers should be parsed (signed) and rejected with a specific message
    with pytest.raises(ValueError) as exc:
        parse_interval(invalid)
    assert "non-negative" in str(exc.value) or "positive" in str(exc.value)


def test_settings_accepts_valid_intervals():
    s = Settings(run_interval="2m")
    assert s.run_interval == "2m"


def test_settings_rejects_invalid_interval():
    with pytest.raises(ValidationError):
        Settings(run_interval="5d")


@pytest.mark.parametrize("input,expected", [
    ("2147483648", 2147483648),
    ("9999999999s", 9999999999),
    ("1000000000h", 1000000000 * 3600),
    ("99999999h", 99999999 * 3600),
])
def test_parse_interval_large_values(input, expected):
    assert parse_interval(input) == expected


@pytest.mark.parametrize("invalid", ["-5", "-1m", "-0", "-12345678901234567890"])
def test_parse_interval_negative_numbers_rejected(invalid):
    with pytest.raises(ValueError):
        parse_interval(invalid)
