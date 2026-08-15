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


# ---------------------------------------------------------------------------
# REPL commands + progress rendering
# ---------------------------------------------------------------------------

class _FakeScanner:
    def __init__(self, rows=()):
        self.verbose = False
        self.files_scanned = 3
        self.current_path = "c:/windows/system32"
        self.size = 1234
        self.root = "c:/"
        self.db_version = 0
        self.rows = list(rows)

    def total_size(self):
        return self.size

    def get_top(self, n=15):
        return self.rows[:n]


def _console(stdout=None, stdin=None, state="running", rows=()):
    import io
    c = space_analyzer.Console(_FakeScanner(rows), {"state": state, "error": None})
    c.stdout = stdout if stdout is not None else io.StringIO()
    c.stdin = stdin if stdin is not None else io.StringIO("\n")
    return c


def test_verbose_gate_toggles_progress_output():
    """The `v` toggle must affect a callback that is already installed on a
    running scan -- that is the whole point of gating it."""
    scanner = _FakeScanner()
    cb = space_analyzer._make_progress_callback(
        False, gate=lambda: scanner.verbose, in_place=False)

    import io
    import contextlib
    buf = io.StringIO()
    with contextlib.redirect_stderr(buf):
        cb(1, 100, "c:/a")
        assert buf.getvalue() == ""
        scanner.verbose = True
        cb(2, 200, "c:/b")
    assert "c:/b" in buf.getvalue()


def test_v_command_toggles_scanner_verbose():
    c = _console()
    c.do_v("")
    assert c.scanner.verbose is True
    c.do_v("")
    assert c.scanner.verbose is False


def test_status_line_includes_total_files_and_current_path():
    line = _console().status_line()
    assert "(1234 bytes)" in line
    assert "3 files" in line
    assert "c:/windows/system32" in line


def test_t_command_prints_one_snapshot_when_not_a_tty():
    import io
    out = io.StringIO()          # StringIO.isatty() is False
    c = _console(stdout=out)
    c.do_t("")
    assert "c:/windows/system32" in out.getvalue()
    assert "\r" not in out.getvalue()


def test_t_command_rewrites_in_place_on_a_tty():
    import io

    class TtyIO(io.StringIO):
        def isatty(self):
            return True

    out = TtyIO()
    c = _console(stdout=out, stdin=io.StringIO("\n"))
    c.do_t("")
    text = out.getvalue()
    assert "\r" in text, "expected a carriage-return rewrite"
    assert space_analyzer.ERASE_LINE in text
    assert text.endswith("\n"), "must leave the cursor on a fresh line"


def test_t_command_is_listed_in_help():
    import io
    out = io.StringIO()
    c = _console(stdout=out)
    with __import__("contextlib").redirect_stdout(out):
        c.do_help("")
    assert "\nt " in out.getvalue() or "  t " in out.getvalue()


def test_progress_callback_in_place_fits_terminal_width(monkeypatch):
    monkeypatch.setattr(space_analyzer, "_term_width", lambda default=100: 40)
    line = space_analyzer._fit("x" * 200)
    assert len(line) <= 39


def test_progress_callback_json_mode_is_never_in_place():
    cb = space_analyzer._make_progress_callback(True)
    assert cb.in_place is False


# ---------------------------------------------------------------------------
# `j` -- JSON report
# ---------------------------------------------------------------------------

def _report_rows():
    return [
        DBFile(is_dir=True, path="c:/foo", size=7_000_100,
               key=key_for_path("c:/foo"), db_version=0),
        DBFile(is_dir=False, path="c:/foo/big_file.bin", size=5_000_000,
               key=key_for_path("c:/foo/big_file.bin"), db_version=0),
    ]


def test_j_writes_report_to_named_file(tmp_path, capsys):
    dest = tmp_path / "report.json"
    c = _console(rows=_report_rows())
    c.do_j(str(dest))

    data = json.loads(dest.read_text(encoding="utf-8"))
    assert data["scan_root"] == "c:/"
    assert data["scan_state"] == "running"
    assert data["files_scanned"] == 3
    assert data["bytes_scanned"] == 1234
    assert data["entry_count"] == 2
    assert [e["path"] for e in data["entries"]] == ["c:/foo", "c:/foo/big_file.bin"]
    assert data["entries"][0]["is_dir"] is True
    assert "wrote" in capsys.readouterr().out


def test_j_defaults_to_a_name_derived_from_the_scan_root(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    c = _console(rows=_report_rows())
    c.do_j("")

    expected = space_analyzer.report_path_for("c:/")
    assert (tmp_path / expected).exists()
    assert expected.endswith("_space_report.json")


def test_j_overwrites_an_existing_file(tmp_path):
    dest = tmp_path / "report.json"
    dest.write_text("stale contents that are not valid json" * 100, encoding="utf-8")
    c = _console(rows=_report_rows())
    c.do_j(str(dest))

    data = json.loads(dest.read_text(encoding="utf-8"))
    assert data["entry_count"] == 2
    assert "stale" not in dest.read_text(encoding="utf-8")


def test_j_strips_quotes_from_the_filename(tmp_path):
    dest = tmp_path / "quoted report.json"
    c = _console(rows=_report_rows())
    c.do_j('"%s"' % dest)
    assert dest.exists()


def test_j_reports_scan_state_done(tmp_path):
    dest = tmp_path / "report.json"
    c = _console(rows=_report_rows(), state="done")
    c.do_j(str(dest))
    assert json.loads(dest.read_text(encoding="utf-8"))["scan_state"] == "done"


def test_j_handles_unwritable_destination(tmp_path, capsys):
    c = _console(rows=_report_rows())
    c.do_j(str(tmp_path))  # a directory -- open() must fail
    assert "could not write" in capsys.readouterr().out


def test_j_is_listed_in_help(capsys):
    _console().do_help("")
    assert "j [FILE]" in capsys.readouterr().out


def test_report_path_for_matches_db_naming():
    db_path, _last = space_analyzer.db_paths_for("c:/")
    report = space_analyzer.report_path_for("c:/")
    assert report.startswith(db_path.lstrip("_").replace("_space_analyzer.db", ""))
