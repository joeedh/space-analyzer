import pytest

from util import formatsize, parse_size, safepath


def test_formatsize_bytes():
    assert formatsize(0) == "0b"
    assert formatsize(1023) == "1023b"


def test_formatsize_kb():
    assert formatsize(1024).endswith("kb")
    assert formatsize(1024 * 1024 - 1).endswith("kb")


def test_formatsize_mb():
    assert formatsize(1024 * 1024).endswith("mb")
    assert formatsize(1024 * 1024 * 1024 - 1).endswith("mb")


def test_formatsize_gb():
    assert formatsize(1024 ** 3).endswith("gb")
    assert formatsize(1024 ** 3 + 1).endswith("gb")


def test_formatsize_always_returns_string():
    for v in [0, 1, 100, 1023, 1024, 10**6, 10**9, 10**12]:
        assert isinstance(formatsize(v), str)


def test_safepath_replaces_weird_chars():
    out = safepath("hello\x00world")
    assert "\x00" not in out
    assert "?" in out


def test_safepath_passes_normal_ascii():
    assert safepath("C:/dev/space") == "C:/dev/space"


@pytest.mark.parametrize("text,expected", [
    ("0", 0),
    ("1024", 1024),
    ("1b", 1),
    ("1kb", 1024),
    ("1k", 1024),
    ("2KB", 2 * 1024),
    ("1mb", 1024 ** 2),
    ("1.5mb", int(1.5 * 1024 ** 2)),
    ("2gb", 2 * 1024 ** 3),
    ("1tb", 1024 ** 4),
    ("  3 mb  ", 3 * 1024 ** 2),
])
def test_parse_size_valid(text, expected):
    assert parse_size(text) == expected


@pytest.mark.parametrize("text", ["", "abc", "1xb", "-1mb", "mb"])
def test_parse_size_invalid(text):
    with pytest.raises(ValueError):
        parse_size(text)


def test_parse_size_accepts_ints():
    assert parse_size(1024) == 1024
    with pytest.raises(ValueError):
        parse_size(-1)
