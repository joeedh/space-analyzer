"""Scanner: walk an FsProvider and persist sizes to a DBSqLite.

Resume strategy: every file row carries `db_version`. A re-scan reuses
file rows whose db_version matches the scanner's; new/changed files are
re-stat'd. Directory totals are NOT updated in the hot loop — they are
recomputed in a single aggregation pass at the end of `run()` (and also
on demand). This is what makes resume immune to double-counting.
"""
import os
import stat as stat_mod
import sys
import threading
import time

from db import DBFile, key_for_path
from fs import FILE_ATTRIBUTE_REPARSE_POINT, FsProvider
from util import safepath


def _looks_like_reparse_point(entry):
    try:
        st = entry.stat(follow_symlinks=False)
    except OSError:
        return False
    attrs = getattr(st, "st_file_attributes", 0)
    return bool(attrs & FILE_ATTRIBUTE_REPARSE_POINT)


def should_skip(entry):
    """True if the walker must not follow/count `entry`.

    Directories: skipped if they are a symlink or any kind of reparse point.
    On Windows an NTFS junction reports is_dir() True and is_symlink() False,
    so the reparse attribute is the only reliable signal -- and if we could
    not stat the entry at all we skip it too, since a junction and a plain
    directory are indistinguishable without attributes. Following a junction
    would double-count its target and can loop forever on a cycle.

    Files: skipped only if they are symlinks. A file may legitimately carry a
    reparse point (OneDrive placeholders, dedup-backed files) and still
    occupies its reported size, so those are counted.
    """
    if entry.is_symlink():
        return True
    if not entry.is_dir():
        return False
    if not getattr(entry, "stat_ok", True):
        return True
    return _looks_like_reparse_point(entry)


class Scanner:
    def __init__(self, fs, db, root, state_path=None, db_version=0,
                 verbose=False, progress_callback=None, progress_interval=1.0):
        self.fs = fs
        self.db = db
        self.root = root
        self.state_path = state_path
        self.db_version = db_version
        self.verbose = verbose
        self.progress_callback = progress_callback
        self.progress_interval = progress_interval

        self.size = 0
        self.files_scanned = 0
        self.current_path = root
        self.lock = threading.Lock()
        self._stop = False

    def stop(self):
        self._stop = True

    # ---- walk ------------------------------------------------------------

    def walk(self, start=None):
        """Yield (dir_path, dir_nodes, file_nodes) from a DFS over the tree.

        Skips symlinks and reparse points so we never follow them.
        Skips directories that raise PermissionError on scandir.
        """
        start = start if start is not None else self.root
        stack = [start]
        while stack:
            cur = stack.pop()
            try:
                entries = self.fs.scandir(cur)
            except PermissionError:
                if self.verbose:
                    sys.stderr.write("permission denied: %s\n" % safepath(cur))
                continue
            except OSError as e:
                if self.verbose:
                    sys.stderr.write("scandir error %s: %s\n" % (safepath(cur), e))
                continue

            dirs, files = [], []
            for e in entries:
                try:
                    if should_skip(e):
                        if self.verbose and e.is_dir():
                            sys.stderr.write(
                                "skipping reparse point: %s\n" % safepath(e.path))
                        continue
                    if e.is_dir():
                        dirs.append(e)
                    else:
                        files.append(e)
                except OSError:
                    continue
            yield cur, dirs, files
            # push in reverse so traversal order matches sorted order
            for d in reversed(dirs):
                stack.append(d.path)

    # ---- main scan -------------------------------------------------------

    def run(self):
        """Single-shot scan. Safe to call again on the same DB to resume."""
        self.size = 0
        self.files_scanned = 0
        now = time.time()
        last_state_save = now
        last_progress = now
        for root, _dirs, files in self.walk():
            if self._stop:
                return
            self.current_path = root
            if self.state_path and time.time() - last_state_save > 0.5:
                try:
                    with open(self.state_path, "w") as f:
                        f.write(root)
                except OSError:
                    pass
                last_state_save = time.time()
            for e in files:
                if self._stop:
                    return
                self._record_file(e)
                self.files_scanned += 1
            if self.progress_callback is not None:
                if time.time() - last_progress >= self.progress_interval:
                    try:
                        self.progress_callback(self.files_scanned, self.size, root)
                    except Exception:
                        pass
                    last_progress = time.time()

        self.aggregate_directories()
        if self.progress_callback is not None:
            try:
                self.progress_callback(self.files_scanned, self.size, self.root)
            except Exception:
                pass

    def _record_file(self, entry):
        path = entry.path
        key = key_for_path(path)
        with self.lock:
            existing = self.db[key]
            if existing is not None and existing.db_version == self.db_version:
                self.size += existing.size
                return
            try:
                st = entry.stat(follow_symlinks=False)
            except OSError:
                return
            sz = st.st_size
            self.size += sz
            row = DBFile(
                is_dir=False,
                path=path,
                size=sz,
                key=key,
                db_version=self.db_version,
            )
            self.db[key] = row

    def aggregate_directories(self):
        """Re-derive every directory's size from current file rows.

        Idempotent — running it twice over the same file set yields the
        same directory sizes, so it doesn't matter if a previous partial
        scan already wrote dir rows.
        """
        totals = {}
        for f in self.db.iter_files(only_files=True):
            parent = os.path.dirname(f.path)
            last = None
            n = 0
            while parent and parent != last:
                n += 1
                if n > 1000:
                    break
                totals[parent] = totals.get(parent, 0) + f.size
                last = parent
                parent = os.path.dirname(parent)

        with self.lock:
            for path, total in totals.items():
                key = key_for_path(path)
                self.db[key] = DBFile(
                    is_dir=True,
                    path=path,
                    size=total,
                    key=key,
                    db_version=self.db_version,
                )
            self.db.flush()

    # ---- queries ---------------------------------------------------------

    def get_top(self, n=15):
        with self.lock:
            return self.db.get_top(n)

    def total_size(self):
        return self.size
