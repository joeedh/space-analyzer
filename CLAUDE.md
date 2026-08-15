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

**Symlinks and reparse points are skipped**, never followed (`_looks_like_reparse_point`).

**Tests never touch the real filesystem** — they build a `MockFs` from a seed. New scanner
tests should go through the `mock_fs` / `scanner` fixtures in `tests/conftest.py` rather
than scanning a temp dir.

## Gotchas

- The DB and resume-state files are named from the scan root (`db_paths_for`) and written
  to the **current working directory**, not next to the scan root. Real scans produce
  multi-GB `.db` files in the repo root; they're gitignored.
- `Scanner.total_size()` is the in-memory sum for *this session* only — a resumed scan's
  total won't match the DB's contents.
- Bare invocation is legacy-compatible: unknown args route to `scan --interactive` via
  `CliGroup.resolve_command`, and the group sets `ignore_unknown_options`. Adding a
  top-level option means checking that path still works.
