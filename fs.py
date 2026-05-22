"""Filesystem provider abstraction.

`FsProvider` is the seam the scanner uses instead of touching `os.scandir`
and `os.stat` directly. `RealFs` keeps production behavior; `MockFs` builds
a deterministic in-memory tree from a seed for tests.
"""
import os
import random
import stat


class StatLike:
    __slots__ = ("st_size", "st_mode", "st_file_attributes")

    def __init__(self, st_size, st_mode, st_file_attributes=0):
        self.st_size = st_size
        self.st_mode = st_mode
        self.st_file_attributes = st_file_attributes


class Entry:
    """Subset of os.DirEntry the scanner needs."""

    __slots__ = ("name", "path", "_is_dir", "_is_symlink", "_stat")

    def __init__(self, name, path, is_dir, is_symlink, stat_):
        self.name = name
        self.path = path
        self._is_dir = is_dir
        self._is_symlink = is_symlink
        self._stat = stat_

    def is_dir(self):
        return self._is_dir

    def is_symlink(self):
        return self._is_symlink

    def stat(self, follow_symlinks=True):
        return self._stat


class FsProvider:
    def scandir(self, path):
        raise NotImplementedError

    def stat(self, path):
        raise NotImplementedError


class RealFs(FsProvider):
    def scandir(self, path):
        entries = []
        for e in os.scandir(path):
            try:
                st = e.stat(follow_symlinks=False)
                stat_like = StatLike(
                    st.st_size,
                    st.st_mode,
                    getattr(st, "st_file_attributes", 0),
                )
            except OSError:
                stat_like = StatLike(0, 0, 0)
            entries.append(
                Entry(
                    name=e.name,
                    path=e.path,
                    is_dir=e.is_dir(follow_symlinks=False),
                    is_symlink=e.is_symlink(),
                    stat_=stat_like,
                )
            )
        entries.sort(key=lambda x: x.name.lower())
        return entries

    def stat(self, path):
        st = os.stat(path)
        return StatLike(
            st.st_size,
            st.st_mode,
            getattr(st, "st_file_attributes", 0),
        )


class _Node:
    __slots__ = (
        "name",
        "path",
        "is_dir",
        "is_symlink",
        "size",
        "children",
        "raises_permission_error",
    )

    def __init__(self, name, path, is_dir, is_symlink=False, size=0):
        self.name = name
        self.path = path
        self.is_dir = is_dir
        self.is_symlink = is_symlink
        self.size = size
        self.children = []
        self.raises_permission_error = False


class MockFs(FsProvider):
    """Deterministic in-memory filesystem driven by a seed.

    The whole tree is built up-front in __init__ so the same seed always
    produces the same tree (verified by test_mockfs).

    The root path defaults to "/mock". All node paths use forward slashes
    so tests behave the same on POSIX and Windows; the scanner reaches
    nodes only via this provider, never through the real os layer.
    """

    SEP = "/"

    def __init__(
        self,
        seed=1337,
        root="/mock",
        max_depth=4,
        max_children=5,
        p_dir=0.45,
        max_file_size=1_000_000,
        p_symlink=0.05,
    ):
        self.root_path = root
        self._nodes = {}
        rng = random.Random(seed)

        root_node = _Node(name=os.path.basename(root) or root, path=root, is_dir=True)
        self._nodes[root] = root_node
        self._build(root_node, rng, max_depth, max_children, p_dir, max_file_size, p_symlink)

    def _build(self, parent, rng, depth_left, max_children, p_dir, max_file_size, p_symlink):
        if depth_left <= 0:
            return
        n = rng.randint(0, max_children)
        for i in range(n):
            is_symlink = rng.random() < p_symlink
            is_dir = (not is_symlink) and rng.random() < p_dir
            name = self._random_name(rng, i)
            child_path = parent.path + self.SEP + name
            if is_dir:
                node = _Node(name=name, path=child_path, is_dir=True)
                self._nodes[child_path] = node
                parent.children.append(node)
                self._build(node, rng, depth_left - 1, max_children, p_dir, max_file_size, p_symlink)
            else:
                size = 0 if is_symlink else rng.randint(0, max_file_size)
                node = _Node(
                    name=name,
                    path=child_path,
                    is_dir=False,
                    is_symlink=is_symlink,
                    size=size,
                )
                self._nodes[child_path] = node
                parent.children.append(node)

    @staticmethod
    def _random_name(rng, i):
        alphabet = "abcdefghijklmnopqrstuvwxyz"
        n = rng.randint(3, 8)
        return "".join(rng.choice(alphabet) for _ in range(n)) + str(i)

    # ---- injection helpers used by tests --------------------------------

    def add_file(self, path, size=0, is_symlink=False):
        parent_path = path.rsplit(self.SEP, 1)[0]
        name = path.rsplit(self.SEP, 1)[1]
        parent = self._nodes[parent_path]
        node = _Node(name=name, path=path, is_dir=False, is_symlink=is_symlink, size=size)
        parent.children.append(node)
        self._nodes[path] = node
        return node

    def make_unreadable(self, path):
        self._nodes[path].raises_permission_error = True

    # ---- FsProvider interface -------------------------------------------

    def scandir(self, path):
        node = self._nodes.get(path)
        if node is None or not node.is_dir:
            raise FileNotFoundError(path)
        if node.raises_permission_error:
            raise PermissionError(path)
        entries = []
        for child in node.children:
            st = StatLike(child.size, self._mode_for(child))
            entries.append(
                Entry(
                    name=child.name,
                    path=child.path,
                    is_dir=child.is_dir,
                    is_symlink=child.is_symlink,
                    stat_=st,
                )
            )
        entries.sort(key=lambda x: x.name.lower())
        return entries

    def stat(self, path):
        node = self._nodes.get(path)
        if node is None:
            raise FileNotFoundError(path)
        return StatLike(node.size, self._mode_for(node))

    @staticmethod
    def _mode_for(node):
        if node.is_symlink:
            return stat.S_IFLNK
        if node.is_dir:
            return stat.S_IFDIR
        return stat.S_IFREG

    # ---- ground-truth helpers for assertions ----------------------------

    def total_size_under(self, path):
        node = self._nodes[path]
        return sum(
            n.size
            for n in self._iter_subtree(node)
            if not n.is_dir and not n.is_symlink
        )

    def all_files_under(self, path):
        return [
            n for n in self._iter_subtree(self._nodes[path])
            if not n.is_dir and not n.is_symlink
        ]

    def all_dirs_under(self, path):
        return [n for n in self._iter_subtree(self._nodes[path]) if n.is_dir]

    def largest_files(self, n):
        files = [node for node in self._nodes.values()
                 if not node.is_dir and not node.is_symlink]
        files.sort(key=lambda x: x.size, reverse=True)
        return files[:n]

    def _iter_subtree(self, node):
        stack = [node]
        while stack:
            cur = stack.pop()
            yield cur
            if cur.is_dir and not cur.raises_permission_error:
                stack.extend(cur.children)
