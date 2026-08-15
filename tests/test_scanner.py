import os

from db import DBSqLite, key_for_path
from fs import MockFs
from scanner import Scanner


def test_full_scan_matches_ground_truth(scanner, mock_fs):
    scanner.run()
    expected = mock_fs.total_size_under(mock_fs.root_path)
    assert scanner.size == expected


def test_directory_sizes_match_sum_of_children(scanner, mock_fs):
    scanner.run()
    for d in mock_fs.all_dirs_under(mock_fs.root_path):
        row = scanner.db[key_for_path(d.path)]
        expected = mock_fs.total_size_under(d.path)
        if expected == 0:
            # empty dirs may legitimately have no row written
            assert row is None or row.size == 0
        else:
            assert row is not None, "missing dir row for %s" % d.path
            assert row.is_dir is True
            assert row.size == expected, (
                "dir %s expected %d got %d" % (d.path, expected, row.size)
            )


def test_top_n_matches_largest_files(scanner, mock_fs):
    scanner.run()
    top = scanner.get_top(5)
    # top results may include directories; filter to files for comparison
    top_files = [r for r in top if not r.is_dir][:3]
    expected_top = mock_fs.largest_files(3)
    assert [r.size for r in top_files] == [n.size for n in expected_top]


def test_resume_does_not_double_count(tmp_db_path, mock_fs):
    """Scan partway, then run again on the same DB. Final totals must
    equal a clean single-pass scan."""

    # clean pass to get the ground-truth totals
    clean_path = tmp_db_path + ".clean"
    db_clean = DBSqLite(clean_path)
    s_clean = Scanner(fs=mock_fs, db=db_clean, root=mock_fs.root_path, db_version=0)
    s_clean.run()
    clean_top = [(r.path, r.size, r.is_dir) for r in db_clean.get_top(100)]
    db_clean.close()

    # interrupted + resumed pass
    db = DBSqLite(tmp_db_path)
    s = Scanner(fs=mock_fs, db=db, root=mock_fs.root_path, db_version=0)

    # walk manually -- record half the files, then stop
    all_files = []
    for _root, _dirs, files in s.walk():
        all_files.extend(files)
    half = len(all_files) // 2
    for e in all_files[:half]:
        s._record_file(e)
    s.aggregate_directories()

    # now run a full second pass -- this is the resume
    s2 = Scanner(fs=mock_fs, db=db, root=mock_fs.root_path, db_version=0)
    s2.run()

    resumed_top = [(r.path, r.size, r.is_dir) for r in db.get_top(100)]
    db.close()

    assert resumed_top == clean_top


def test_symlinks_not_followed(tmp_db_path, mock_fs):
    mock_fs.add_file(mock_fs.root_path + "/__link", size=10**9, is_symlink=True)
    db = DBSqLite(tmp_db_path)
    s = Scanner(fs=mock_fs, db=db, root=mock_fs.root_path, db_version=0)
    s.run()
    # the symlink's "size" must not contribute
    assert s.size == mock_fs.total_size_under(mock_fs.root_path)
    # nor should it appear as a row
    assert db[key_for_path(mock_fs.root_path + "/__link")] is None
    db.close()


def test_permission_error_dirs_are_skipped(tmp_db_path, mock_fs):
    dirs = mock_fs.all_dirs_under(mock_fs.root_path)
    target = next(d for d in dirs if d.path != mock_fs.root_path)
    blocked_subtree_size = mock_fs.total_size_under(target.path)
    mock_fs.make_unreadable(target.path)

    db = DBSqLite(tmp_db_path)
    s = Scanner(fs=mock_fs, db=db, root=mock_fs.root_path, db_version=0)
    s.run()

    expected = mock_fs.total_size_under(mock_fs.root_path) - blocked_subtree_size
    assert s.size == expected
    db.close()


def test_scan_handles_special_char_filenames(tmp_db_path, mock_fs):
    """End-to-end regression: weird filenames must round-trip through the DB."""
    weird = mock_fs.root_path + "/has'quote"
    mock_fs.add_file(weird, size=12345)

    db = DBSqLite(tmp_db_path)
    s = Scanner(fs=mock_fs, db=db, root=mock_fs.root_path, db_version=0)
    s.run()

    row = db[key_for_path(weird)]
    assert row is not None
    assert row.path == weird
    assert row.size == 12345
    db.close()


def _tree_with_target():
    """Small explicit tree: /mock/target holds two files worth 3000 bytes."""
    fs = MockFs(seed=1337, max_depth=0)
    fs.add_dir("/mock/target")
    fs.add_file("/mock/target/a.bin", size=1000)
    fs.add_file("/mock/target/b.bin", size=2000)
    fs.add_file("/mock/loose.bin", size=7)
    return fs


def test_junctions_are_not_descended_into(tmp_db_path):
    mock_fs = _tree_with_target()
    """An NTFS junction reports is_dir()=True and is_symlink()=False, so only
    the reparse attribute distinguishes it. Descending would double-count."""
    target = mock_fs._nodes["/mock/target"]
    expected = mock_fs.total_size_under(mock_fs.root_path)
    mock_fs.add_junction(mock_fs.root_path + "/__junction", target=target.path)

    db = DBSqLite(tmp_db_path)
    s = Scanner(fs=mock_fs, db=db, root=mock_fs.root_path, db_version=0)
    s.run()

    assert s.size == expected, "junction target counted twice"
    assert db[key_for_path(mock_fs.root_path + "/__junction")] is None
    db.close()


def test_junction_children_are_not_recorded_under_the_junction(tmp_db_path):
    mock_fs = _tree_with_target()
    target = mock_fs._nodes["/mock/target"]
    junction = mock_fs.root_path + "/__junction"
    mock_fs.add_junction(junction, target=target.path)

    db = DBSqLite(tmp_db_path)
    s = Scanner(fs=mock_fs, db=db, root=mock_fs.root_path, db_version=0)
    s.run()

    for row in db.iter_files():
        assert not row.path.startswith(junction + "/"), row.path
    db.close()


def test_symlinked_directories_are_not_descended_into(tmp_db_path):
    from fs import IO_REPARSE_TAG_SYMLINK

    mock_fs = _tree_with_target()
    target = mock_fs._nodes["/mock/target"]
    expected = mock_fs.total_size_under(mock_fs.root_path)
    mock_fs.add_junction(mock_fs.root_path + "/__dirlink", target=target.path,
                         reparse_tag=IO_REPARSE_TAG_SYMLINK)

    db = DBSqLite(tmp_db_path)
    s = Scanner(fs=mock_fs, db=db, root=mock_fs.root_path, db_version=0)
    s.run()

    assert s.size == expected
    db.close()


def test_reparse_point_files_are_still_counted(tmp_db_path, mock_fs):
    """OneDrive placeholders and dedup-backed files carry a reparse point but
    are real files occupying real bytes -- they must not be skipped."""
    from fs import IO_REPARSE_TAG_CLOUD

    baseline = mock_fs.total_size_under(mock_fs.root_path)
    cloud = mock_fs.root_path + "/__cloud.bin"
    mock_fs.add_file(cloud, size=4242, reparse_tag=IO_REPARSE_TAG_CLOUD)

    db = DBSqLite(tmp_db_path)
    s = Scanner(fs=mock_fs, db=db, root=mock_fs.root_path, db_version=0)
    s.run()

    assert s.size == baseline + 4242
    row = db[key_for_path(cloud)]
    assert row is not None and row.size == 4242
    db.close()


def test_unstattable_directory_is_not_descended_into(tmp_db_path):
    """Without attributes we cannot tell a junction from a plain directory,
    so the walker must refuse to follow it."""
    from fs import Entry, StatLike

    mock_fs = _tree_with_target()
    target = mock_fs._nodes["/mock/target"]
    blocked = mock_fs.total_size_under(target.path)
    real_scandir = mock_fs.scandir

    def scandir(path):
        out = []
        for e in real_scandir(path):
            if e.path == target.path:
                e = Entry(e.name, e.path, is_dir=True, is_symlink=False,
                          stat_=StatLike(0, 0, 0, 0), stat_ok=False)
            out.append(e)
        return out

    mock_fs.scandir = scandir

    db = DBSqLite(tmp_db_path)
    s = Scanner(fs=mock_fs, db=db, root=mock_fs.root_path, db_version=0)
    s.run()

    assert s.size == mock_fs.total_size_under(mock_fs.root_path) - blocked
    db.close()


def test_current_path_tracks_the_walk(tmp_db_path, mock_fs):
    db = DBSqLite(tmp_db_path)
    s = Scanner(fs=mock_fs, db=db, root=mock_fs.root_path, db_version=0)
    assert s.current_path == mock_fs.root_path
    seen = []
    s.progress_callback = lambda f, b, root: seen.append(s.current_path)
    s.progress_interval = 0
    s.run()
    assert seen
    assert all(p.startswith(mock_fs.root_path) for p in seen)
    db.close()


def test_scanner_can_be_rerun_after_stop(mock_fs, tmp_path):
    """`stop()` must not poison the Scanner -- reset restarts the same one."""
    db = DBSqLite(str(tmp_path / "t.db"))
    sc = Scanner(mock_fs, db, "/mock")
    sc.run()
    first = sc.files_scanned
    assert first > 0

    sc.stop()          # what the reset command does before restarting
    sc.run()           # run() clears the stop flag itself
    assert sc.files_scanned == first
    assert sc.size == mock_fs.total_size_under(mock_fs.root_path)
    db.close()
