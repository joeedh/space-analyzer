#!/usr/bin/env python3
"""space_analyzer: scan a Windows drive for large files and folders.

CLI entry point. Heavy lifting lives in `db.py`, `fs.py`, `scanner.py`,
and `util.py`. Importing this module has no side effects -- everything
happens inside the click commands.

Output discipline (for LLM/script consumers):
  - Data (top, query results) goes to stdout.
  - Progress, status, and informational messages go to stderr.
  - --json modes emit machine-parseable structures on stdout (or NDJSON
    progress events on stderr for `scan --json`).
"""
import cmd
import json
import os
import sys
import threading
import time

import click

from db import DBSqLite
from fs import RealFs
from scanner import Scanner
from util import formatsize, parse_size, safepath


DB_VERSION = 0
DEFAULT_PATH = "c:/"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def db_paths_for(scan_path):
    key = (
        scan_path
        .replace("/", "_")
        .replace("\\", "_")
        .replace("-", "_")
        .replace(" ", "_")
        .replace(":", "_")
    )
    return "_" + key + "_space_analyzer.db", key + "_space_last_path.txt"


def _normalize_path(path):
    path = os.path.normpath(os.path.abspath(path))
    if not path.endswith(os.path.sep):
        path += os.path.sep
    return path


def _row_to_dict(row):
    return {
        "path": row.path,
        "size": int(row.size),
        "is_dir": bool(row.is_dir),
        "db_version": row.db_version,
    }


# ---------------------------------------------------------------------------
# REPL (interactive mode only)
# ---------------------------------------------------------------------------

class ExitSignal(RuntimeError):
    pass


class Console(cmd.Cmd):
    intro = "Scanning"
    prompt = "> "
    file = None

    def __init__(self, scanner):
        super().__init__()
        self.scanner = scanner

    def do_v(self, arg):
        "toggle verbose output"
        self.scanner.verbose = not self.scanner.verbose
        print("VERBOSE", self.scanner.verbose)

    def do_q(self, arg):
        "exit"
        raise ExitSignal

    def do_quit(self, arg):
        "exit"
        raise ExitSignal

    def do_exit(self, arg):
        "exit"
        raise ExitSignal

    def do_s(self, arg):
        "print current size sum"
        print(formatsize(self.scanner.total_size()))

    def do_p(self, arg):
        "print top N entries by size: p [N]"
        maxn = 15
        if arg.strip():
            try:
                maxn = int(arg.strip().split()[0])
            except ValueError:
                pass
        rows = self.scanner.get_top(maxn)
        for f in rows:
            print(formatsize(f.size), safepath(f.path))
        print()
        print("Size:", formatsize(self.scanner.total_size()))


# ---------------------------------------------------------------------------
# scan implementations
# ---------------------------------------------------------------------------

def _make_progress_callback(json_mode):
    """Return a callback that emits progress to stderr. Text or NDJSON."""
    start = time.time()

    def cb(files_scanned, bytes_scanned, current_root):
        if json_mode:
            evt = {
                "event": "progress",
                "files": int(files_scanned),
                "bytes": int(bytes_scanned),
                "current_path": current_root,
                "elapsed_seconds": round(time.time() - start, 3),
            }
            sys.stderr.write(json.dumps(evt) + "\n")
        else:
            sys.stderr.write(
                "scanned %d files, %s, in %s\n"
                % (files_scanned, formatsize(bytes_scanned),
                   safepath(current_root))
            )
        sys.stderr.flush()

    cb.start = start
    return cb


def _scan_impl(scan_root, interactive=False, json_mode=False, verbose=False):
    """Run a scan. Returns exit code."""
    scan_path = _normalize_path(scan_root)
    db_path, last_path = db_paths_for(scan_path)

    if os.path.exists(last_path):
        with open(last_path, "r") as fh:
            saved = fh.read().strip()
        if os.path.exists(saved):
            click.echo("Resuming from %s" % safepath(saved), err=True)
            scan_path = saved
        else:
            click.echo("WARNING: last_path does not exist: %s" % safepath(saved),
                       err=True)

    click.echo("PATH: %s" % scan_path, err=True)

    db = DBSqLite(db_path)
    progress_cb = _make_progress_callback(json_mode) if (json_mode or verbose) else None
    scanner = Scanner(
        fs=RealFs(),
        db=db,
        root=scan_path,
        state_path=last_path,
        db_version=DB_VERSION,
        verbose=verbose,
        progress_callback=progress_cb,
    )

    if interactive:
        thread = threading.Thread(target=scanner.run, daemon=True)
        thread.start()
        try:
            Console(scanner).cmdloop()
        except (ExitSignal, KeyboardInterrupt):
            click.echo("Exiting", err=True)
        finally:
            scanner.stop()
            thread.join(timeout=5)
            db.close()
        return 0

    # non-interactive: run synchronously and exit
    try:
        scanner.run()
    except KeyboardInterrupt:
        click.echo("Interrupted", err=True)
        scanner.stop()
        db.close()
        return 130

    if json_mode:
        elapsed = time.time() - progress_cb.start if progress_cb else 0
        sys.stderr.write(json.dumps({
            "event": "done",
            "files": int(scanner.files_scanned),
            "bytes": int(scanner.size),
            "elapsed_seconds": round(elapsed, 3),
        }) + "\n")
        sys.stderr.flush()
    else:
        click.echo("Scan complete: %d files, %s"
                   % (scanner.files_scanned, formatsize(scanner.size)), err=True)
    db.close()
    return 0


def _reset_impl(scan_root):
    scan_path = _normalize_path(scan_root)
    db_path, last_path = db_paths_for(scan_path)
    removed = []
    for p in (db_path, last_path):
        if os.path.exists(p):
            os.unlink(p)
            removed.append(p)
    for p in removed:
        click.echo("Removed %s" % p, err=True)
    if not removed:
        click.echo("Nothing to remove for %s" % scan_path, err=True)
    return 0


# ---------------------------------------------------------------------------
# click group + subcommands
# ---------------------------------------------------------------------------

class CliGroup(click.Group):
    """Group that supports legacy bare-invocation usage:

      space_analyzer.py                -> scan --interactive c:/
      space_analyzer.py D:/foo         -> scan --interactive D:/foo
      space_analyzer.py D:/foo --reset -> reset D:/foo
      space_analyzer.py --reset        -> reset c:/
      space_analyzer.py scan ...       -> normal subcommand routing
      space_analyzer.py top ...        -> normal subcommand routing
    """

    def resolve_command(self, ctx, args):
        if args and args[0] in self.commands:
            return super().resolve_command(ctx, args)
        if "--reset" in args:
            path_args = [a for a in args if a != "--reset"]
            return "reset", self.commands["reset"], path_args
        return "scan", self.commands["scan"], ["--interactive"] + args


@click.group(
    cls=CliGroup,
    context_settings={"help_option_names": ["-h", "--help"]},
    invoke_without_command=True,
)
@click.option("--reset", "legacy_reset", is_flag=True, hidden=True)
@click.pass_context
def cli(ctx, legacy_reset):
    """Scan a drive for large files and folders.

    Subcommands:
      scan   - walk a path and persist sizes (with --interactive for a REPL)
      top    - print the largest entries from a previous scan
      query  - look up a single path's size
      reset  - delete the DB for a scan root

    With no subcommand, behaves like the old tool: scans interactively.
    Quote PATH if it contains spaces.
    """
    if ctx.invoked_subcommand is None:
        if legacy_reset:
            ctx.exit(_reset_impl(DEFAULT_PATH))
        ctx.exit(_scan_impl(DEFAULT_PATH, interactive=True))


@cli.command()
@click.argument("path", type=click.Path(), default=DEFAULT_PATH)
@click.option("--interactive", is_flag=True,
              help="Stay in the REPL after launching the scan thread.")
@click.option("--json", "json_mode", is_flag=True,
              help="Emit JSONL progress events on stderr; nothing on stdout.")
@click.option("-v", "--verbose", is_flag=True,
              help="Print human-readable progress on stderr.")
def scan(path, interactive, json_mode, verbose):
    """Walk PATH and persist file/dir sizes to the per-path DB.

    Without --interactive, runs to completion and exits.
    """
    sys.exit(_scan_impl(path, interactive=interactive, json_mode=json_mode, verbose=verbose))


@cli.command()
@click.option("--scan-root", default=DEFAULT_PATH, show_default=True,
              help="Which scan's DB to read from.")
@click.option("-n", "n", type=int, default=15, show_default=True,
              help="Number of rows to return.")
@click.option("--json", "json_mode", is_flag=True,
              help="Emit results as a JSON array on stdout.")
@click.option("--min-size", default=None,
              help="Only rows >= this size (e.g. 1gb, 500mb, 1024).")
@click.option("--prefix", default=None,
              help="Only rows whose path starts with this prefix.")
@click.option("--files-only", is_flag=True, help="Exclude directories.")
@click.option("--dirs-only", is_flag=True, help="Exclude files.")
def top(scan_root, n, json_mode, min_size, prefix, files_only, dirs_only):
    """Print the largest entries from a previous scan, optionally filtered."""
    if files_only and dirs_only:
        raise click.UsageError("--files-only and --dirs-only are mutually exclusive")

    min_size_bytes = None
    if min_size is not None:
        try:
            min_size_bytes = parse_size(min_size)
        except ValueError as e:
            raise click.UsageError(str(e))

    scan_path = _normalize_path(scan_root)
    db_path, _ = db_paths_for(scan_path)
    if not os.path.exists(db_path):
        click.echo("No scan DB found for %s (run `scan` first)" % scan_path, err=True)
        sys.exit(1)

    db = DBSqLite(db_path)
    try:
        rows = db.get_top(
            n=n,
            min_size=min_size_bytes,
            prefix=prefix,
            files_only=files_only,
            dirs_only=dirs_only,
        )
    finally:
        db.close()

    if json_mode:
        click.echo(json.dumps([_row_to_dict(r) for r in rows]))
    else:
        for r in rows:
            click.echo("%s %s" % (formatsize(r.size), safepath(r.path)))


@cli.command()
@click.argument("path", type=click.Path())
@click.option("--scan-root", default=DEFAULT_PATH, show_default=True,
              help="Which scan's DB to read from.")
@click.option("--json", "json_mode", is_flag=True,
              help="Emit the result as a JSON object on stdout.")
def query(path, scan_root, json_mode):
    """Look up a single PATH's recorded size."""
    scan_path = _normalize_path(scan_root)
    db_path, _ = db_paths_for(scan_path)
    if not os.path.exists(db_path):
        click.echo("No scan DB found for %s (run `scan` first)" % scan_path, err=True)
        sys.exit(1)

    db = DBSqLite(db_path)
    try:
        row = db.get_by_path(path)
    finally:
        db.close()

    if row is None:
        if json_mode:
            click.echo(json.dumps({"path": path, "found": False}))
        else:
            click.echo("not in DB: %s" % path, err=True)
        sys.exit(1)

    if json_mode:
        d = _row_to_dict(row)
        d["found"] = True
        click.echo(json.dumps(d))
    else:
        click.echo("%s %s" % (formatsize(row.size), safepath(row.path)))


@cli.command()
@click.argument("path", type=click.Path(), default=DEFAULT_PATH)
def reset(path):
    """Delete the DB and resume-state files for a scan root."""
    sys.exit(_reset_impl(path))


if __name__ == "__main__":
    cli()
