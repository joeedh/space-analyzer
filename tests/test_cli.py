import json
import os

from click.testing import CliRunner

import space_analyzer
from db import DBFile, DBSqLite, key_for_path


# ---------------------------------------------------------------------------
# scan / bare-invocation
# ---------------------------------------------------------------------------

def _patch_scan(monkeypatch):
    captured = {}

    def fake(scan_root, interactive=False, json_mode=False, verbose=False):
        captured["path"] = scan_root
        captured["interactive"] = interactive
        captured["json_mode"] = json_mode
        captured["verbose"] = verbose
        return 0

    monkeypatch.setattr(space_analyzer, "_scan_impl", fake)
    return captured


def _patch_reset(monkeypatch):
    captured = {}

    def fake(scan_root):
        captured["path"] = scan_root
        return 0

    monkeypatch.setattr(space_analyzer, "_reset_impl", fake)
    return captured


def test_bare_invocation_runs_interactive_scan(monkeypatch):
    captured = _patch_scan(monkeypatch)
    result = CliRunner().invoke(space_analyzer.cli, [])
    assert result.exit_code == 0, result.output
    assert captured["path"] == "c:/"
    assert captured["interactive"] is True


def test_bare_invocation_with_explicit_path(monkeypatch):
    captured = _patch_scan(monkeypatch)
    result = CliRunner().invoke(space_analyzer.cli, ["D:/foo"])
    assert result.exit_code == 0, result.output
    assert captured["path"] == "D:/foo"
    assert captured["interactive"] is True


def test_bare_invocation_interactive_flag_at_group_level(monkeypatch):
    """`space_analyzer.py --interactive [PATH]` should route to scan-interactive,
    not fail with `No such option '--interactive'`."""
    captured = _patch_scan(monkeypatch)
    result = CliRunner().invoke(space_analyzer.cli, ["--interactive", "D:/foo"])
    assert result.exit_code == 0, result.output
    assert captured["path"] == "D:/foo"
    assert captured["interactive"] is True


def test_bare_invocation_interactive_flag_no_path(monkeypatch):
    captured = _patch_scan(monkeypatch)
    result = CliRunner().invoke(space_analyzer.cli, ["--interactive"])
    assert result.exit_code == 0, result.output
    assert captured["interactive"] is True


def test_bare_invocation_with_quoted_path(monkeypatch):
    captured = _patch_scan(monkeypatch)
    result = CliRunner().invoke(space_analyzer.cli, ["C:/Program Files"])
    assert result.exit_code == 0, result.output
    assert captured["path"] == "C:/Program Files"


def test_legacy_reset_flag(monkeypatch):
    captured = _patch_reset(monkeypatch)
    result = CliRunner().invoke(space_analyzer.cli, ["--reset"])
    assert result.exit_code == 0
    assert captured["path"] == "c:/"


def test_legacy_reset_with_explicit_path(monkeypatch):
    captured = _patch_reset(monkeypatch)
    result = CliRunner().invoke(space_analyzer.cli, ["D:/foo", "--reset"])
    assert result.exit_code == 0
    assert captured["path"] == "D:/foo"


def test_scan_subcommand_non_interactive(monkeypatch):
    captured = _patch_scan(monkeypatch)
    result = CliRunner().invoke(space_analyzer.cli, ["scan", "D:/foo"])
    assert result.exit_code == 0, result.output
    assert captured["path"] == "D:/foo"
    assert captured["interactive"] is False
    assert captured["json_mode"] is False


def test_scan_subcommand_interactive_flag(monkeypatch):
    captured = _patch_scan(monkeypatch)
    result = CliRunner().invoke(space_analyzer.cli, ["scan", "--interactive", "D:/foo"])
    assert result.exit_code == 0
    assert captured["interactive"] is True


def test_scan_subcommand_json_flag(monkeypatch):
    captured = _patch_scan(monkeypatch)
    result = CliRunner().invoke(space_analyzer.cli, ["scan", "--json", "D:/foo"])
    assert result.exit_code == 0
    assert captured["json_mode"] is True


def test_scan_subcommand_verbose_flag(monkeypatch):
    captured = _patch_scan(monkeypatch)
    result = CliRunner().invoke(space_analyzer.cli, ["scan", "-v", "D:/foo"])
    assert result.exit_code == 0
    assert captured["verbose"] is True


def test_help_exits_zero():
    result = CliRunner().invoke(space_analyzer.cli, ["--help"])
    assert result.exit_code == 0
    assert "scan" in result.output
    assert "top" in result.output
    assert "query" in result.output
    assert "reset" in result.output


def test_help_short_flag():
    result = CliRunner().invoke(space_analyzer.cli, ["-h"])
    assert result.exit_code == 0


def test_scan_help_mentions_options():
    result = CliRunner().invoke(space_analyzer.cli, ["scan", "--help"])
    assert result.exit_code == 0
    assert "--interactive" in result.output
    assert "--json" in result.output


def test_unknown_flag_exits_nonzero():
    result = CliRunner().invoke(space_analyzer.cli, ["--bogus"])
    assert result.exit_code != 0


# ---------------------------------------------------------------------------
# top / query / reset subcommands -- exercised via a real tmp DB
# ---------------------------------------------------------------------------

def _seed_db(tmp_path):
    """Create a DB at the location `_<key>_space_analyzer.db` derived from
    a scan-root inside tmp_path, then populate it with known rows."""
    scan_root = str(tmp_path / "myscan")
    # ensure dir exists so _normalize_path produces a stable result
    os.makedirs(scan_root, exist_ok=True)

    scan_path_norm = space_analyzer._normalize_path(scan_root)
    db_path, _ = space_analyzer.db_paths_for(scan_path_norm)
    # put the DB file inside tmp_path so it gets cleaned up
    db_path = str(tmp_path / os.path.basename(db_path))

    db = DBSqLite(db_path)
    rows = [
        ("c:/foo/big_file.bin", 5_000_000, False),
        ("c:/foo/small.txt", 100, False),
        ("c:/foo/medium.bin", 2_000_000, False),
        ("c:/foo", 7_000_100, True),
        ("c:/bar/other.bin", 1_000_000, False),
        ("c:/bar", 1_000_000, True),
    ]
    for path, size, is_dir in rows:
        k = key_for_path(path)
        db[k] = DBFile(is_dir=is_dir, path=path, size=size, key=k, db_version=0)
    db.close()
    return scan_root, db_path


def _run_with_db(monkeypatch, tmp_path, args_after_scan_root, subcmd):
    """Invoke `cli` so that `db_paths_for` returns a DB inside tmp_path."""
    scan_root, db_path = _seed_db(tmp_path)
    real_db_paths_for = space_analyzer.db_paths_for

    def fake_db_paths_for(p):
        _db, _last = real_db_paths_for(p)
        return db_path, str(tmp_path / os.path.basename(_last))

    monkeypatch.setattr(space_analyzer, "db_paths_for", fake_db_paths_for)
    return CliRunner().invoke(
        space_analyzer.cli,
        [subcmd, "--scan-root", scan_root, *args_after_scan_root],
    )


def test_top_text_output(monkeypatch, tmp_path):
    result = _run_with_db(monkeypatch, tmp_path, ["-n", "3"], "top")
    assert result.exit_code == 0, result.output
    lines = [l for l in result.output.splitlines() if l.strip()]
    assert len(lines) == 3
    # the biggest is the c:/foo dir at 7_000_100
    assert "c:/foo" in lines[0]


def test_top_json_output(monkeypatch, tmp_path):
    result = _run_with_db(monkeypatch, tmp_path, ["-n", "5", "--json"], "top")
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert isinstance(data, list)
    assert len(data) == 5
    sizes = [d["size"] for d in data]
    assert sizes == sorted(sizes, reverse=True)
    for d in data:
        assert set(d.keys()) >= {"path", "size", "is_dir", "db_version"}


def test_top_min_size_filter(monkeypatch, tmp_path):
    result = _run_with_db(monkeypatch, tmp_path,
                          ["--min-size", "3mb", "--json"], "top")
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert all(d["size"] >= 3 * 1024 * 1024 for d in data)
    # the 7_000_100-byte dir and the 5_000_000 file qualify; 2mb does not
    paths = {d["path"] for d in data}
    assert "c:/foo" in paths
    assert "c:/foo/big_file.bin" in paths
    assert "c:/foo/medium.bin" not in paths


def test_top_prefix_filter(monkeypatch, tmp_path):
    result = _run_with_db(monkeypatch, tmp_path,
                          ["--prefix", "c:/bar", "--json"], "top")
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    paths = {d["path"] for d in data}
    assert all(p.lower().startswith("c:/bar") for p in paths)
    assert "c:/foo" not in paths


def test_top_files_only(monkeypatch, tmp_path):
    result = _run_with_db(monkeypatch, tmp_path, ["--files-only", "--json"], "top")
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert all(d["is_dir"] is False for d in data)


def test_top_dirs_only(monkeypatch, tmp_path):
    result = _run_with_db(monkeypatch, tmp_path, ["--dirs-only", "--json"], "top")
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert all(d["is_dir"] is True for d in data)


def test_top_files_and_dirs_only_mutually_exclusive(monkeypatch, tmp_path):
    result = _run_with_db(monkeypatch, tmp_path,
                          ["--files-only", "--dirs-only"], "top")
    assert result.exit_code != 0


def test_top_invalid_min_size_exits_nonzero(monkeypatch, tmp_path):
    result = _run_with_db(monkeypatch, tmp_path, ["--min-size", "abc"], "top")
    assert result.exit_code != 0


def test_top_no_db_exits_1(tmp_path):
    # no DB written -- should exit 1
    result = CliRunner().invoke(space_analyzer.cli, [
        "top", "--scan-root", str(tmp_path / "nope"),
    ])
    assert result.exit_code == 1


def test_query_found(monkeypatch, tmp_path):
    result = _run_with_db(monkeypatch, tmp_path,
                          ["c:/foo/big_file.bin", "--json"], "query")
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert data["found"] is True
    assert data["size"] == 5_000_000
    assert data["path"] == "c:/foo/big_file.bin"


def test_query_missing_exits_1(monkeypatch, tmp_path):
    result = _run_with_db(monkeypatch, tmp_path,
                          ["c:/does/not/exist", "--json"], "query")
    assert result.exit_code == 1
    data = json.loads(result.output)
    assert data["found"] is False


def test_reset_subcommand(monkeypatch):
    captured = _patch_reset(monkeypatch)
    result = CliRunner().invoke(space_analyzer.cli, ["reset", "D:/foo"])
    assert result.exit_code == 0
    assert captured["path"] == "D:/foo"
