import urllib.parse

import pytest

from llm_memedescriber.constants import (
    _get_extension,
    is_supported,
    is_image,
    is_video,
    sanitize_filename,
    MAX_FILENAME_LENGTH,
)


@pytest.mark.parametrize("fname,ext", [
    ("image.JPG", "jpg"),
    ("photo.png", "png"),
    ("photo.PNG", "png"),
    ("video.mp4", "mp4"),
    ("video.MKV", "mkv"),
    ("video.final.version.mov", "mov"),
    ("archive.tar.gz", "gz"),
    ("noext", ""),
    (".hiddenfile", ""),
])
def test_get_extension(fname, ext):
    assert _get_extension(fname) == ext


@pytest.mark.parametrize("fname,expected", [
    ("photo.jpg", True),
    ("video.mp4", True),
    ("document.txt", False),
    ("graphic.gif", True),
    ("archive.tar.gz", False),
    ("document.pdf", False),
    ("strange.PNG", True),
])
def test_is_supported(fname, expected):
    assert is_supported(fname) is expected


@pytest.mark.parametrize("fname,expected", [
    ("photo.jpg", True),
    ("image.jpeg", True),
    ("graphic.png", True),
    ("movie.mp4", False),
    ("picture.webp", True),
    ("animation.gif", True),
    ("video.MKV", False),
])
def test_is_image(fname, expected):
    assert is_image(fname) is expected


@pytest.mark.parametrize("fname,expected", [
    ("movie.mp4", True),
    ("clip.webm", True),
    ("photo.jpg", False),
    ("video.MKV", True),
    ("video.mov", True),
    ("document.pdf", False),
    ("animation.gif", False),
    ("film.avi", True),
    ("archive.tar.gz", False),
])
def test_is_video(fname, expected):
    assert is_video(fname) is expected


def test_sanitize_basic_removes_path_and_backslashes():
    assert sanitize_filename("/tmp/subdir/photo.jpg") == "photo.jpg"
    assert sanitize_filename(r"C:\path\to\picture.png") == "picture.png"


def test_sanitize_decode_url_and_preserve_unicode():
    encoded = urllib.parse.quote("młody obraz.jpg")
    assert sanitize_filename(encoded) == "młody obraz.jpg"


def test_sanitize_remove_traversal_parts():
    assert sanitize_filename("../../etc/passwd") == "passwd"
    assert sanitize_filename("..\\..\\secret.txt") == "secret.txt"


def test_sanitize_strips_leading_dots_and_slashes():
    assert sanitize_filename(".hiddenfile") == "hiddenfile"
    assert sanitize_filename("...weird.txt") == "weird.txt"
    assert sanitize_filename("/././foo.jpg") == "foo.jpg"


def test_sanitize_removes_dangerous_chars_and_nulls():
    s = "fi<le>\x00name?.jpg"
    out = sanitize_filename(s)
    assert "<" not in out and ">" not in out and "?" not in out and "\x00" not in out


def test_sanitize_rejects_empty_after_sanitization():
    # name made entirely of dots and dangerous chars -> empty
    with pytest.raises(ValueError):
        sanitize_filename("...\\/..")


def test_sanitize_rejects_too_long():
    long_name = "a" * (MAX_FILENAME_LENGTH + 1)
    with pytest.raises(ValueError):
        sanitize_filename(long_name)


def test_edge_case_percent_encoded_traversal():
    # %2e%2e%2f => ../, should be decoded and sanitized to final filename
    enc = "%2e%2e%2fetc%2fshadow"  # ../etc/shadow
    assert sanitize_filename(enc) == "shadow"


def test_sanitize_keeps_valid_characters():
    s = "valid-name_123.jpg"
    assert sanitize_filename(s) == s


def test_sanitize_keep_spaces_and_utf8():
    s = "my zdjęcie 2024 🌟.png"
    assert sanitize_filename(s) == s
