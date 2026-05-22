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
