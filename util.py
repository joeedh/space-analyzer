import re


_SIZE_RE = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([a-zA-Z]*)\s*$")
_SIZE_SUFFIXES = {
    "": 1,
    "b": 1,
    "k": 1024,
    "kb": 1024,
    "m": 1024 ** 2,
    "mb": 1024 ** 2,
    "g": 1024 ** 3,
    "gb": 1024 ** 3,
    "t": 1024 ** 4,
    "tb": 1024 ** 4,
}


def formatsize(f):
    if f >= 1024 ** 3:
        return "%.4fgb" % (f / 1024 / 1024 / 1024)
    if f >= 1024 ** 2:
        return "%.2fmb" % (f / 1024 / 1024)
    if f >= 1024:
        return "%.2fkb" % (f / 1024)
    return "%db" % int(f)


def parse_size(s):
    """Parse a human-readable size like '1gb', '500mb', '1024' into bytes.

    Case-insensitive. Supports b/kb/mb/gb/tb suffixes (and bare k/m/g/t).
    Raises ValueError for unrecognized input.
    """
    if isinstance(s, (int, float)):
        if s < 0:
            raise ValueError("size cannot be negative")
        return int(s)
    m = _SIZE_RE.match(s)
    if m is None:
        raise ValueError("cannot parse size: %r" % s)
    value = float(m.group(1))
    suffix = m.group(2).lower()
    if suffix not in _SIZE_SUFFIXES:
        raise ValueError("unknown size suffix: %r" % suffix)
    return int(value * _SIZE_SUFFIXES[suffix])


def safepath(path):
    out = []
    for c in path:
        n = ord(c)
        if n < 10 or n > 210:
            c = "?"
        out.append(c)
    return "".join(out)
