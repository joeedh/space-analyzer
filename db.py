"""SQLite-backed store for scanned file/dir size records.

Uses parameterized queries (no string-formatted SQL) and a UNIQUE(key)
constraint with INSERT ... ON CONFLICT DO UPDATE so re-inserts cannot
produce duplicate rows.
"""
import os
import sqlite3
import time


class DBFile:
    __slots__ = ("is_dir", "path", "size", "key", "db_version")

    def __init__(self, is_dir=False, path="", size=0, key="", db_version=0):
        self.is_dir = is_dir
        self.path = path
        self.size = size
        self.key = key
        self.db_version = db_version

    def clone(self):
        return DBFile(self.is_dir, self.path, self.size, self.key, self.db_version)

    def __eq__(self, other):
        return (
            isinstance(other, DBFile)
            and self.is_dir == other.is_dir
            and self.path == other.path
            and self.size == other.size
            and self.key == other.key
            and self.db_version == other.db_version
        )

    def __repr__(self):
        return (
            f"DBFile(key={self.key!r}, path={self.path!r}, "
            f"size={self.size}, is_dir={self.is_dir}, db_version={self.db_version})"
        )


class DBSqLite:
    FLUSH_INTERVAL_SECONDS = 15

    def __init__(self, path):
        self.path = path
        self.last_save = 0
        self.cache = {}
        self.write_cache = {}
        self.closed = False

        create = not os.path.exists(path)

        self.con = sqlite3.connect(path, check_same_thread=False)
        self.cur = self.con.cursor()

        if create:
            self.cur.executescript(
                """
                CREATE TABLE files (
                    key text PRIMARY KEY,
                    is_dir int,
                    path text,
                    size real,
                    db_version real
                );
                CREATE INDEX idx_size on files (size);
                """
            )
            self.con.commit()

    def __enter__(self):
        return self

    def __exit__(self, a, b, c):
        self.close()

    def flush(self):
        if not self.write_cache:
            return
        rows = []
        for k, f in self.write_cache.items():
            f.key = k
            rows.append((_sqlite_safe(f.key), 1 if f.is_dir else 0, _sqlite_safe(f.path), f.size, f.db_version))
        self.cur.executemany(
            """
            INSERT INTO files (key, is_dir, path, size, db_version)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                is_dir = excluded.is_dir,
                path = excluded.path,
                size = excluded.size,
                db_version = excluded.db_version
            """,
            rows,
        )
        self.con.commit()
        self.write_cache = {}

    def _lookup(self, k):
        self.cur.execute(
            "SELECT key, is_dir, path, size, db_version FROM files WHERE key = ?",
            (k,),
        )
        row = self.cur.fetchone()
        if row is None:
            return None
        return DBFile(
            is_dir=bool(row[1]),
            path=row[2],
            size=row[3],
            key=row[0],
            db_version=row[4],
        )

    def get_top(self, n=55, min_size=None, prefix=None,
                files_only=False, dirs_only=False):
        """Return the n largest rows, optionally filtered.

        All filters compose with AND. `prefix` matches paths starting with
        the given string (case-insensitive against the stored key).
        """
        if files_only and dirs_only:
            raise ValueError("files_only and dirs_only are mutually exclusive")
        self.flush()

        clauses, params = [], []
        if min_size is not None:
            clauses.append("size >= ?")
            params.append(min_size)
        if prefix is not None:
            clauses.append("key LIKE ?")
            params.append(key_for_path(prefix).rstrip("/") + "%")
        if files_only:
            clauses.append("is_dir = 0")
        if dirs_only:
            clauses.append("is_dir = 1")

        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = (
            "SELECT key, is_dir, path, size, db_version FROM files "
            f"{where} ORDER BY size DESC LIMIT ?"
        )
        params.append(n)
        self.cur.execute(sql, tuple(params))
        ret = []
        for row in self.cur.fetchall():
            ret.append(
                DBFile(
                    is_dir=bool(row[1]),
                    path=row[2],
                    size=row[3],
                    key=row[0],
                    db_version=row[4],
                )
            )
        return ret

    def get_by_path(self, path):
        return self._lookup(key_for_path(path))

    def count(self):
        self.flush()
        self.cur.execute("SELECT COUNT(*) FROM files")
        return self.cur.fetchone()[0]

    def clear(self):
        """Drop every row, plus the read/write caches that mirror them.

        Deliberately no VACUUM: on a drive-sized DB it can take minutes and
        block, and the freed pages get reused by the next scan anyway.
        """
        self.write_cache = {}
        self.cache = {}
        self.cur.execute("DELETE FROM files")
        self.con.commit()

    def iter_files(self, only_files=False):
        """Stream every row. Used by Scanner for the directory aggregation pass."""
        self.flush()
        if only_files:
            self.cur.execute(
                "SELECT key, is_dir, path, size, db_version FROM files WHERE is_dir = 0"
            )
        else:
            self.cur.execute(
                "SELECT key, is_dir, path, size, db_version FROM files"
            )
        for row in self.cur.fetchall():
            yield DBFile(
                is_dir=bool(row[1]),
                path=row[2],
                size=row[3],
                key=row[0],
                db_version=row[4],
            )

    def __getitem__(self, k):
        if self.closed:
            return None
        if k in self.cache:
            return self.cache[k]
        f = self._lookup(k)
        if f is not None:
            self.cache[k] = f
        return f

    def __setitem__(self, k, v):
        self.cache[k] = v
        self.write_cache[k] = v
        if time.time() - self.last_save > self.FLUSH_INTERVAL_SECONDS:
            self.flush()
            self.last_save = time.time()

    def __contains__(self, k):
        if k in self.cache:
            return True
        f = self._lookup(k)
        if f is not None:
            self.cache[k] = f
        return f is not None

    def close(self):
        if self.closed:
            return
        self.flush()
        self.con.commit()
        self.con.close()
        self.closed = True


def _sqlite_safe(s):
    """Strip lone surrogates so the string round-trips through SQLite's UTF-8.

    Windows filenames can contain unpaired UTF-16 surrogates that Python keeps
    as lone surrogate codepoints; these are not valid UTF-8 and crash sqlite3
    on insert. Replace them with U+FFFD so the row can still be stored.
    """
    return s.encode("utf-8", "replace").decode("utf-8")


def key_for_path(path):
    """Stable key for a path. Lowercased on Windows so case-insensitive
    paths don't produce duplicate rows."""
    return _sqlite_safe(path.replace("\\", "/").lower())
