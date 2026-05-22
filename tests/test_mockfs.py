import pytest

from fs import MockFs


def _collect(fs, root):
    """DFS through the provider, returning (path, is_dir, size) tuples."""
    out = []
    stack = [root]
    while stack:
        p = stack.pop()
        for e in fs.scandir(p):
            out.append((e.path, e.is_dir(), e.is_symlink(), e.stat().st_size))
            if e.is_dir() and not e.is_symlink():
                stack.append(e.path)
    return sorted(out)


def test_same_seed_produces_identical_tree():
    a = MockFs(seed=42)
    b = MockFs(seed=42)
    assert _collect(a, a.root_path) == _collect(b, b.root_path)


def test_different_seeds_produce_different_trees():
    a = MockFs(seed=1)
    b = MockFs(seed=2)
    assert _collect(a, a.root_path) != _collect(b, b.root_path)


def test_total_size_under_matches_sum_of_files():
    fs = MockFs(seed=7)
    nodes = _collect(fs, fs.root_path)
    expected = sum(size for _, is_dir, is_sym, size in nodes if not is_dir and not is_sym)
    assert fs.total_size_under(fs.root_path) == expected


def test_permission_error_is_raised(mock_fs):
    # find a directory and mark it unreadable
    dirs = mock_fs.all_dirs_under(mock_fs.root_path)
    target = next(d for d in dirs if d.path != mock_fs.root_path)
    mock_fs.make_unreadable(target.path)
    with pytest.raises(PermissionError):
        mock_fs.scandir(target.path)


def test_symlinks_are_reported(mock_fs):
    mock_fs.add_file(mock_fs.root_path + "/__link", size=0, is_symlink=True)
    entries = mock_fs.scandir(mock_fs.root_path)
    links = [e for e in entries if e.is_symlink()]
    assert len(links) == 1
    assert links[0].name == "__link"


def test_largest_files_is_sorted_desc():
    fs = MockFs(seed=99)
    top = fs.largest_files(5)
    sizes = [n.size for n in top]
    assert sizes == sorted(sizes, reverse=True)
