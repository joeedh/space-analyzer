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
import shutil
import sys
import threading
import time
import traceback

import click

from db import DBSqLite
from fs import RealFs
from scanner import Scanner
from util import formatsize, parse_size, safepath


DB_VERSION = 0
DEFAULT_PATH = "c:/"

# ANSI: erase from the cursor to the end of the line. Paired with a leading
# CR this rewrites the current line without leaving stale characters behind.
ERASE_LINE = "\x1b[K"
CURSOR_UP = "\x1b[A"
SAVE_CURSOR = "\x1b7"
RESTORE_CURSOR = "\x1b8"

# How often the live total (`s`) redraws. Five times a second reads as
# continuous without the flicker and wasted wakeups of a faster loop; much
# slower than this and the number visibly lags the scan.
LIVE_REFRESH_SECONDS = 0.2

# How many of the largest entries a `j` report carries.
REPORT_TOP_N = 500


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _isatty(stream):
    try:
        return bool(stream.isatty())
    except (AttributeError, ValueError):
        return False


def _term_width(default=100):
    try:
        return shutil.get_terminal_size((default, 24)).columns
    except OSError:
        return default


def _fit(line):
    """Truncate to one terminal line so the CR rewrite never wraps.

    A wrapped line leaves the cursor on the next row, and the following CR
    would then rewrite the wrong row -- the classic smeared-progress bug.
    """
    width = _term_width() - 1
    if width > 0 and len(line) > width:
        return line[:width - 1] + ">"
    return line


def _scan_key(scan_path):
    return (
        scan_path
        .replace("/", "_")
        .replace("\\", "_")
        .replace("-", "_")
        .replace(" ", "_")
        .replace(":", "_")
    )


def db_paths_for(scan_path):
    key = _scan_key(scan_path)
    return "_" + key + "_space_analyzer.db", key + "_space_last_path.txt"


def report_path_for(scan_path):
    """Default filename for a JSON report, named like the DB for the same root."""
    return _scan_key(scan_path) + "_space_report.json"


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
    intro = (
        "Scanning in the background. Type 'help' (or '?') for commands, "
        "'q' to exit."
    )
    prompt = "> "
    file = None

    # Ordered list of (usage, one-line description) for the `help` table.
    # Detailed help for any single command comes from its docstring.
    _COMMAND_HELP = [
        ("p [N]",
         "Print the top N largest entries scanned so far (default 15)."),
        ("s",
         "Toggle a live running total that redraws in place; `s` again stops it."),
        ("j [FILE]",
         "Write a JSON report of the largest entries (overwrites FILE)."),
        ("r [-y]",
         "Reset: discard the scan DB and start the scan over from the root."),
        ("v",
         "Toggle verbose mode -- log the current directory to stderr."),
        ("h, help [CMD], ?",
         "Show this help, or detailed help for CMD."),
        ("q, quit, exit",
         "Stop the scan, flush the DB, and exit."),
    ]

    def __init__(self, scanner, status=None, restart=None):
        super().__init__()
        self.scanner = scanner
        self.status = status if status is not None else {"state": "unknown", "error": None}
        # Callable that wipes the DB and relaunches the scan thread; supplied
        # by _scan_impl, which owns the thread. None disables `r`.
        self.restart = restart
        # Live-total state. `_live_attached` is True only while the status
        # line owns the row directly above the cursor -- see _live_loop.
        self._live = False
        self._live_attached = False
        self._live_thread = None
        self._live_stop = threading.Event()
        self._live_lock = threading.Lock()

    # ---- commands -------------------------------------------------------

    def do_p(self, arg):
        """Print the top N largest entries currently in the DB.

        Usage: p [N]
          N   number of rows to print (default 15)

        Queries the DB while the scan is still running, so the list grows
        and shifts between calls. After the rows, prints the running total
        of bytes scanned this session.
        """
        maxn = 15
        if arg.strip():
            try:
                maxn = int(arg.strip().split()[0])
            except ValueError:
                print("p: expected an integer, got %r" % arg.strip())
                return
        rows = self.scanner.get_top(maxn)
        for f in rows:
            print(formatsize(f.size), safepath(f.path))
        print()
        print("Size:", formatsize(self.scanner.total_size()))

    def scan_state(self):
        state = self.status.get("state", "unknown")
        err = self.status.get("error")
        if err is not None:
            return "%s (%s: %s)" % (state, type(err).__name__, err)
        return state

    def status_line(self):
        """One-line summary of the scan's progress.

        The size is the in-memory sum the scanner has accumulated since this
        process started; it does not include sizes carried over from a
        previous scan's DB rows.
        """
        total = self.scanner.total_size()
        return "%s (%d bytes) | %d files | %s | %s" % (
            formatsize(total),
            total,
            self.scanner.files_scanned,
            self.scan_state(),
            safepath(self.scanner.current_path),
        )

    def do_s(self, arg):
        """Toggle the live running total. Run `s` again to stop it.

        Usage: s

        Draws one line -- bytes accounted for so far, files scanned, the
        scan thread's state, and the directory being walked -- and keeps
        redrawing it in place about five times a second. The line always
        sits just above the prompt: output from other commands scrolls above
        it and it re-appears underneath, so nothing you print gets eaten.

        On a non-terminal stdout there is nowhere to redraw, so this prints
        a single snapshot instead.
        """
        if not _isatty(self.stdout):
            self.stdout.write(self.status_line() + "\n")
            self.stdout.flush()
            return
        if self._live:
            self.stop_live()
            print("live total: off")
            return
        self._live = True
        self._live_stop.clear()
        self._live_thread = threading.Thread(target=self._live_loop, daemon=True)
        self._live_thread.start()
        print("live total: on (run 's' again to stop)")

    def stop_live(self):
        """Stop the refresh thread, leaving the last line drawn on screen."""
        self._live = False
        self._live_stop.set()
        thread, self._live_thread = self._live_thread, None
        if thread is not None:
            thread.join(timeout=1)
        with self._live_lock:
            self._live_attached = False

    def _live_loop(self):
        """Redraw the status row while it sits directly above the cursor.

        `postcmd` leaves the status on its own row and the cursor on the row
        below, where the prompt is drawn. So a refresh saves the cursor,
        steps up exactly one row, rewrites it, and restores. Moving up and
        back can never scroll the terminal, which is what keeps the prompt
        and anything typed at it untouched -- we never write on that row.
        """
        while not self._live_stop.wait(LIVE_REFRESH_SECONDS):
            with self._live_lock:
                if not self._live_attached:
                    # A command is running; its output owns the screen.
                    continue
                self.stdout.write(
                    SAVE_CURSOR + CURSOR_UP + "\r" + ERASE_LINE
                    + _fit(self.status_line()) + RESTORE_CURSOR
                )
                self.stdout.flush()

    def precmd(self, line):
        # Detach for the duration of the command so its output scrolls
        # normally instead of being fought over by the refresh thread.
        with self._live_lock:
            self._live_attached = False
        return line

    def postcmd(self, stop, line):
        # Re-draw the status below whatever the command printed, then leave
        # the cursor on the next row for the prompt.
        with self._live_lock:
            if self._live and not stop:
                self.stdout.write("\r" + ERASE_LINE + _fit(self.status_line()) + "\n")
                self.stdout.flush()
                self._live_attached = True
        return stop

    def report(self):
        """Build the JSON-serializable report for the scan's current state."""
        rows = self.scanner.get_top(REPORT_TOP_N)
        return {
            "scan_root": self.scanner.root,
            "db_version": self.scanner.db_version,
            "scan_state": self.status.get("state", "unknown"),
            "generated_at": time.time(),
            "files_scanned": int(self.scanner.files_scanned),
            "bytes_scanned": int(self.scanner.total_size()),
            "current_path": self.scanner.current_path,
            "entry_count": len(rows),
            "entries": [_row_to_dict(r) for r in rows],
        }

    def do_j(self, arg):
        """Write a JSON report of the largest entries found so far.

        Usage: j [FILE]
          FILE   where to write (default: a name derived from the scan root,
                 e.g. c__space_report.json). An existing file is overwritten.

        The report holds the scan's totals plus the largest entries currently
        in the DB (REPORT_TOP_N of them). `scan_state` says whether the scan
        was still running when the report was taken, so a partial report is
        recognizable as one.
        """
        path = arg.strip().strip('"').strip("'")
        if not path:
            path = report_path_for(self.scanner.root)

        data = self.report()
        try:
            # Plain "w" truncates, so an existing report is replaced outright.
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(data, fh, indent=2)
                fh.write("\n")
        except OSError as exc:
            print("could not write %s: %s" % (path, exc))
            return

        print("wrote %s (%d entries, %s)"
              % (path, data["entry_count"], formatsize(data["bytes_scanned"])))

    def do_r(self, arg):
        """Throw away the scan DB and start the scan over from the root.

        Usage: r [-y]
          -y   skip the confirmation prompt

        Equivalent to `space_analyzer.py reset` followed by a fresh scan,
        except the DB file is emptied in place rather than deleted -- SQLite
        still has it open. The resume marker is removed too, so the new scan
        starts at the scan root rather than wherever the last one left off.

        This discards every row recorded so far, including rows carried over
        from earlier runs, so it asks first unless given -y.
        """
        if self.restart is None:
            print("reset is not available here (no scan thread to restart)")
            return

        assume_yes = arg.strip().lower() in ("-y", "y", "yes")
        if not assume_yes and not self._confirm_reset():
            print("reset cancelled")
            return

        print("resetting...")
        try:
            dropped = self.restart()
        except Exception as exc:
            print("reset failed: %s: %s" % (type(exc).__name__, exc))
            return
        print("dropped %d rows; rescanning %s"
              % (dropped, safepath(self.scanner.root)))

    def _confirm_reset(self):
        # The scan thread is still writing through the same sqlite cursor, so
        # take the scanner's lock the way get_top() does before counting.
        try:
            with self.scanner.lock:
                rows = self.scanner.db.count()
        except Exception:
            rows = -1
        what = "the scan DB" if rows < 0 else "%d rows" % rows
        try:
            answer = input("discard %s and rescan %s? [y/N] "
                           % (what, safepath(self.scanner.root)))
        except (EOFError, KeyboardInterrupt):
            print()
            return False
        return answer.strip().lower() in ("y", "yes")

    def do_v(self, arg):
        """Toggle verbose mode.

        Usage: v

        When verbose is on, the scanner logs the directory it is currently
        walking to stderr (about once a second), along with permission
        errors, scandir failures, and skipped reparse points. When off, all
        of that is silently skipped.
        """
        self.scanner.verbose = not self.scanner.verbose
        print("verbose:", "on" if self.scanner.verbose else "off")

    def do_q(self, arg):
        """Stop the scan, flush the DB, and exit."""
        raise ExitSignal

    def do_quit(self, arg):
        """Stop the scan, flush the DB, and exit."""
        raise ExitSignal

    def do_exit(self, arg):
        """Stop the scan, flush the DB, and exit."""
        raise ExitSignal

    def do_h(self, arg):
        """Alias for `help`."""
        return self.do_help(arg)

    # ---- help -----------------------------------------------------------

    def do_help(self, arg):
        """Show available commands, or detailed help for a single command.

        Usage: help [CMD]
        """
        arg = arg.strip()
        if arg:
            # Delegate to cmd.Cmd's default, which prints the do_<cmd> docstring.
            return super().do_help(arg)
        usage_w = max(len(u) for u, _ in self._COMMAND_HELP)
        print()
        print("Commands:")
        for usage, desc in self._COMMAND_HELP:
            print("  %-*s   %s" % (usage_w, usage, desc))
        print()
        print("Type 'help <command>' for detailed help on a command,")
        print("e.g. 'help p'.")
        print()

    # Hide aliases from cmd.Cmd's auto-listing so `help` (when not overridden)
    # would not duplicate them. Our override doesn't use this, but `?` still
    # falls through to cmd.Cmd internals which inspects do_* attribute names.
    def get_names(self):
        return [n for n in super().get_names()
                if n not in ("do_quit", "do_exit", "do_h")]

    def emptyline(self):
        # Default cmd.Cmd behavior is to repeat the last command on empty
        # input, which is surprising here. Do nothing instead.
        pass

    def default(self, line):
        print("Unknown command: %r. Type 'help' for the command list." % line)


# ---------------------------------------------------------------------------
# scan implementations
# ---------------------------------------------------------------------------

def _make_progress_callback(json_mode, gate=None, in_place=None):
    """Return a callback that emits progress to stderr. Text or NDJSON.

    `gate` is an optional predicate consulted on every call; when it returns
    False the event is dropped. That is how the REPL's `v` toggle can turn
    logging on and off while the scan thread is already running.

    `in_place` rewrites a single line via CR + "erase to end of line" instead
    of scrolling. Defaults to on when stderr is a terminal and we are not in
    JSON mode.
    """
    start = time.time()
    if in_place is None:
        in_place = (not json_mode) and _isatty(sys.stderr)

    def cb(files_scanned, bytes_scanned, current_root):
        if gate is not None and not gate():
            return
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
            line = ("scanned %d files, %s, in %s"
                    % (files_scanned, formatsize(bytes_scanned),
                       safepath(current_root)))
            if in_place:
                sys.stderr.write("\r" + ERASE_LINE + _fit(line))
            else:
                sys.stderr.write(line + "\n")
        sys.stderr.flush()

    cb.start = start
    cb.in_place = in_place
    return cb


def _end_progress_line(progress_cb):
    """Close off an in-place progress line so later output starts fresh."""
    if progress_cb is not None and getattr(progress_cb, "in_place", False):
        sys.stderr.write("\n")
        sys.stderr.flush()


def _scan_impl(scan_root, interactive=False, json_mode=False, verbose=False):
    """Run a scan. Returns exit code."""
    scan_path = _normalize_path(scan_root)
    db_path, last_path = db_paths_for(scan_path)
    # Kept separate from scan_path, which the resume logic below may move to
    # a subdirectory: a reset has to go back to the real root.
    root_path = scan_path

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
    scanner = Scanner(
        fs=RealFs(),
        db=db,
        root=scan_path,
        state_path=last_path,
        db_version=DB_VERSION,
        verbose=verbose,
    )

    if interactive:
        # Always attach the callback and gate it on `scanner.verbose`, so the
        # REPL's `v` command can turn per-directory logging on and off while
        # the scan thread is already running. Scroll rather than rewrite in
        # place: `s` owns the in-place line, and this shares the terminal
        # with the prompt.
        scanner.progress_callback = _make_progress_callback(
            json_mode, gate=lambda: scanner.verbose, in_place=False)
    elif json_mode or verbose:
        scanner.progress_callback = _make_progress_callback(json_mode)
    progress_cb = scanner.progress_callback

    if interactive:
        status = {"state": "running", "error": None}

        def run_scan():
            try:
                scanner.run()
            except BaseException as exc:
                status["state"] = "crashed"
                status["error"] = exc
                sys.stderr.write(
                    "\n[scan thread crashed: %s: %s]\n%s"
                    % (type(exc).__name__, exc, traceback.format_exc())
                )
                sys.stderr.flush()
            else:
                status["state"] = "done"
                sys.stderr.write(
                    "\n[scan complete: %d files, %s]\n"
                    % (scanner.files_scanned, formatsize(scanner.size))
                )
                sys.stderr.flush()

        threads = {"scan": None}

        def start_scan():
            status["state"] = "running"
            status["error"] = None
            threads["scan"] = threading.Thread(target=run_scan, daemon=True)
            threads["scan"].start()

        def stop_scan():
            scanner.stop()
            thread = threads["scan"]
            if thread is not None:
                thread.join(timeout=10)
            threads["scan"] = None

        def restart_scan():
            """Empty the DB and rescan from the root. Returns rows dropped."""
            stop_scan()
            dropped = db.count()
            db.clear()
            # Drop the resume marker too, or the next start would jump back
            # to where the discarded scan left off.
            if os.path.exists(last_path):
                try:
                    os.unlink(last_path)
                except OSError:
                    pass
            scanner.root = root_path
            scanner.current_path = root_path
            scanner.size = 0
            scanner.files_scanned = 0
            start_scan()
            return dropped

        start_scan()
        console = Console(scanner, status, restart=restart_scan)
        try:
            console.cmdloop()
        except (ExitSignal, KeyboardInterrupt):
            click.echo("Exiting", err=True)
        finally:
            console.stop_live()
            stop_scan()
            db.close()
        return 0

    # non-interactive: run synchronously and exit
    try:
        scanner.run()
    except KeyboardInterrupt:
        _end_progress_line(progress_cb)
        click.echo("Interrupted", err=True)
        scanner.stop()
        db.close()
        return 130

    _end_progress_line(progress_cb)
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
    context_settings={
        "help_option_names": ["-h", "--help"],
        # Let unknown options (e.g. `--interactive`, `-v`) flow through to
        # CliGroup.resolve_command so legacy invocations like
        # `space_analyzer.py --interactive D:/foo` still route to `scan`.
        "ignore_unknown_options": True,
    },
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
