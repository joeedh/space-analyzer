import pytest

from db import DBFile, DBSqLite, key_for_path


def test_roundtrip(tmp_db_path):
    db = DBSqLite(tmp_db_path)
    f = DBFile(is_dir=False, path="/x/y", size=100, key="/x/y", db_version=0)
    db["/x/y"] = f
    db.flush()
    db.close()

    db2 = DBSqLite(tmp_db_path)
    got = db2["/x/y"]
    assert got is not None
    assert got.path == "/x/y"
    assert got.size == 100
    assert got.is_dir is False
    db2.close()


def test_get_top_orders_by_size(tmp_db_path):
    db = DBSqLite(tmp_db_path)
    for i, size in enumerate([10, 500, 30, 99, 1]):
        k = "k%d" % i
        db[k] = DBFile(False, k, size, k, 0)
    rows = db.get_top(3)
    assert [r.size for r in rows] == [500, 99, 30]
    db.close()


def test_get_top_limit_honored(tmp_db_path):
    db = DBSqLite(tmp_db_path)
    for i in range(10):
        k = "k%d" % i
        db[k] = DBFile(False, k, i, k, 0)
    assert len(db.get_top(3)) == 3
    assert len(db.get_top(50)) == 10
    db.close()


def test_paths_with_special_chars_roundtrip(tmp_db_path):
    """Regression test for the old string-formatted SQL bug."""
    db = DBSqLite(tmp_db_path)
    tricky = [
        "/a/has'quote",
        '/a/has"doublequote',
        "/a/has\nnewline",
        "/a/unicodé",
        "/a/back\\slash",
        "/a/semi;colon",
        "/a/percent%20",
    ]
    for i, p in enumerate(tricky):
        db[p] = DBFile(False, p, i + 1, p, 0)
    db.flush()
    for i, p in enumerate(tricky):
        got = db[p]
        assert got is not None, "lost row for %r" % p
        assert got.path == p
        assert got.size == i + 1
    db.close()


def test_reinsert_does_not_duplicate(tmp_db_path):
    db = DBSqLite(tmp_db_path)
    db["k"] = DBFile(False, "p", 10, "k", 0)
    db.flush()
    db["k"] = DBFile(False, "p", 999, "k", 1)
    db.flush()
    db.cur.execute("SELECT count(*) FROM files WHERE key='k'")
    assert db.cur.fetchone()[0] == 1
    assert db["k"].size == 999
    assert db["k"].db_version == 1
    db.close()


def test_key_for_path_normalizes_separators():
    assert key_for_path("C:\\Foo\\Bar") == "c:/foo/bar"
    assert key_for_path("c:/foo/bar") == "c:/foo/bar"


def _seed_filter_db(path):
    db = DBSqLite(path)
    rows = [
        ("c:/foo/big.bin", 5_000_000, False),
        ("c:/foo/small.txt", 100, False),
        ("c:/foo/medium.bin", 2_000_000, False),
        ("c:/foo", 7_000_100, True),
        ("c:/bar/other.bin", 1_000_000, False),
        ("c:/bar", 1_000_000, True),
    ]
    for p, sz, isdir in rows:
        k = key_for_path(p)
        db[k] = DBFile(is_dir=isdir, path=p, size=sz, key=k, db_version=0)
    db.flush()
    return db


def test_get_top_min_size_filter(tmp_db_path):
    db = _seed_filter_db(tmp_db_path)
    rows = db.get_top(n=10, min_size=3_000_000)
    sizes = [r.size for r in rows]
    assert all(s >= 3_000_000 for s in sizes)
    assert 100 not in sizes
    assert 2_000_000 not in sizes
    db.close()


def test_get_top_prefix_filter(tmp_db_path):
    db = _seed_filter_db(tmp_db_path)
    rows = db.get_top(n=10, prefix="c:/bar")
    assert all(r.path.lower().startswith("c:/bar") for r in rows)
    assert not any("foo" in r.path for r in rows)
    db.close()


def test_get_top_files_only(tmp_db_path):
    db = _seed_filter_db(tmp_db_path)
    rows = db.get_top(n=10, files_only=True)
    assert all(r.is_dir is False for r in rows)
    db.close()


def test_get_top_dirs_only(tmp_db_path):
    db = _seed_filter_db(tmp_db_path)
    rows = db.get_top(n=10, dirs_only=True)
    assert all(r.is_dir is True for r in rows)
    db.close()


def test_get_top_filters_compose(tmp_db_path):
    db = _seed_filter_db(tmp_db_path)
    rows = db.get_top(n=10, min_size=1_000_000, prefix="c:/foo", files_only=True)
    for r in rows:
        assert r.size >= 1_000_000
        assert r.path.lower().startswith("c:/foo")
        assert r.is_dir is False
    db.close()


def test_get_top_mutually_exclusive(tmp_db_path):
    db = _seed_filter_db(tmp_db_path)
    with pytest.raises(ValueError):
        db.get_top(files_only=True, dirs_only=True)
    db.close()


def test_get_by_path_found(tmp_db_path):
    db = _seed_filter_db(tmp_db_path)
    row = db.get_by_path("C:/Foo/big.bin")  # case-insensitive via key_for_path
    assert row is not None
    assert row.size == 5_000_000
    db.close()


def test_get_by_path_missing(tmp_db_path):
    db = _seed_filter_db(tmp_db_path)
    assert db.get_by_path("c:/does/not/exist") is None
    db.close()
