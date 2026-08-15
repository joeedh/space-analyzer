# CLAUDE.md

Disk space analyzer for Windows: walks a drive, records file/dir sizes in SQLite,
and reports the largest entries. Python 3, `click` for the CLI, `pytest` for tests.

## Commands

```
python -m pytest -q                  # full suite (79 tests, <1s, no real FS access)
python space_analyzer.py             # legacy bare form -> interactive scan of c:/
python space_analyzer.py scan D:/foo # non-interactive scan
python space_analyzer.py top -n 20 --min-size 1gb --dirs-only --scan-root c:/
python space_analyzer.py query c:/windows --json
python space_analyzer.py reset D:/foo
```

Deps live in `Pipfile` (`click`, dev: `pytest`). No linter or formatter is configured.

## Layout

- `space_analyzer.py` — CLI only (click group + `_scan_impl`/`_reset_impl` + the `cmd.Cmd` REPL).
  Import has no side effects; everything happens inside commands.
- `scanner.py` — `Scanner`: DFS walk, per-file rows, then directory aggregation.
- `db.py` — `DBSqLite` (dict-like: `db[key]` / `db[key] = DBFile`) plus query methods.
- `fs.py` — `FsProvider` seam: `RealFs` for production, `MockFs` (seeded, in-memory) for tests.
- `util.py` — `formatsize`, `parse_size`, `safepath`.

## Invariants worth preserving

**Output discipline.** Data (top rows, query results, JSON) goes to **stdout**; progress,
status, warnings, and `scan --json` NDJSON events go to **stderr**. Machine consumers
depend on this — don't `click.echo` status without `err=True`.

**Directory sizes are never accumulated in the hot loop.** `Scanner.run()` writes only
file rows, then `aggregate_directories()` re-derives every directory total from scratch.
That's what makes resume idempotent; incrementally adding to dir rows would double-count.

**Resume is `db_version`-keyed.** A file row whose `db_version` matches the scanner's is
reused without re-stat'ing. Bumping `DB_VERSION` in `space_analyzer.py` invalidates all rows.

**Paths are keyed via `key_for_path`** (backslashes → `/`, lowercased) so Windows'
case-insensitivity doesn't create duplicate rows. All SQL is parameterized; keys and paths
pass through `_sqlite_safe` to strip lone UTF-16 surrogates that Windows filenames can carry
and that crash `sqlite3` on insert.

**The walker never descends into a junction** (`scanner.should_skip`). On Windows an NTFS
junction reports `is_dir()` True and `is_symlink()` False, so the `FILE_ATTRIBUTE_REPARSE_POINT`
bit is the only reliable signal — and a directory we could not stat at all (`entry.stat_ok`
False) is skipped too, since it's indistinguishable from a junction. Files carrying a reparse
point (OneDrive placeholders, dedup-backed files) *are* counted: they occupy real bytes.

**Interactive progress logging is gated, not conditionally installed.** The scan thread always
gets a progress callback whose `gate` reads `scanner.verbose`, so the REPL's `v` command can
toggle logging on a scan that's already running. Installing the callback only when
`verbose` was true at startup is what made `v` a no-op.

**In-place output uses CR + `ERASE_LINE` and runs every line through `_fit`.** A line wider
than the terminal wraps, which leaves the cursor a row down and makes the next CR rewrite the
wrong row. Anything drawn in place must be truncated first, and must be closed with a newline
(`_end_progress_line`) before other output follows.

**The live total (`s`) owns exactly one row: the one above the cursor.** `postcmd` draws it
and leaves the cursor on the next row, where `input()` puts the prompt; `_live_loop` then
refreshes with save-cursor → `CURSOR_UP` → rewrite → restore-cursor. It only ever moves *up*
and back, which can never scroll the terminal, and it never writes on the prompt row — that
is what keeps the prompt and anything half-typed at it intact. `precmd` detaches for the
duration of a command so its output scrolls normally, and `postcmd` re-attaches below it.
Drawing the status on the prompt row instead (the obvious implementation) erases whatever
the user is typing every refresh.

**Tests never touch the real filesystem** — they build a `MockFs` from a seed. New scanner
tests should go through the `mock_fs` / `scanner` fixtures in `tests/conftest.py` rather
than scanning a temp dir.

## Gotchas

- The DB, resume-state, and default JSON-report filenames all derive from the scan root via
  `_scan_key` (`db_paths_for`, `report_path_for`) and are written
  to the **current working directory**, not next to the scan root. Real scans produce
  multi-GB `.db` files in the repo root; they're gitignored.
- `Scanner.total_size()` is the in-memory sum for *this session* only — a resumed scan's
  total won't match the DB's contents.
- Bare invocation is legacy-compatible: unknown args route to `scan --interactive` via
  `CliGroup.resolve_command`, and the group sets `ignore_unknown_options`. Adding a
  top-level option means checking that path still works.
