# space_analyzer

A little tool to find large files and folders on Windows.

It walks a drive, records the size of every file in a SQLite database, then lets
you query the largest files and directories. Scans are resumable — interrupt one
and the next run picks up where it left off instead of starting over.

## Setup

Requires Python 3 and [`click`](https://click.palletsprojects.com/).

With pipenv (a `Pipfile` is included):

```
pipenv install --dev
pipenv shell
```

Or with plain pip:

```
python -m venv .venv
.venv\Scripts\activate
pip install click pytest
```

## Usage

### Scan

```
python space_analyzer.py scan c:/
```

Walks the path and writes sizes to a database. Runs to completion and exits.
Useful flags:

- `-v` / `--verbose` — print progress to stderr as it goes
- `--json` — emit newline-delimited JSON progress events on stderr
- `--interactive` — scan in a background thread and drop into a REPL

With `-v` on a terminal, progress is drawn on a single self-updating line;
piped to a file it scrolls one line per update instead.

Scanning a whole drive takes a while. If you interrupt it (Ctrl-C), just run the
same command again — it resumes from the last directory it recorded.

Symlinks and NTFS junctions are never followed, so a junction's target is
counted once, where it actually lives. Files backed by a reparse point
(OneDrive placeholders, deduplicated files) are still counted normally.

### Interactive mode

```
python space_analyzer.py
```

With no arguments, scans `c:/` interactively: the scan runs in the background
while you query it from a prompt.

| Command | Description |
| --- | --- |
| `p [N]` | Print the top N largest entries found so far (default 15) |
| `s` | Toggle a live running total that redraws in place; `s` again stops it |
| `j [FILE]` | Write a JSON report of the largest entries so far |
| `v` | Toggle verbose logging of the directory being walked |
| `help [CMD]`, `h`, `?` | Show commands, or detailed help for one |
| `q`, `quit`, `exit` | Stop the scan, flush the database, and exit |

`s` starts a running total that rewrites its own line about five times a
second and sits just above the prompt:

```
14.65kb (15000 bytes) | 30 files | running | c:/windows/system32
>
```

Other commands print above it and it re-appears underneath, so nothing you
print gets eaten:

```
> p
  1.94gb c:/users/joeed/appdata
  1.21gb c:/windows/winsxs
14.65kb (15000 bytes) | 30 files | running | c:/windows/system32
>
```

Run `s` again to stop it, leaving the last line on screen. Piped to a file
there is nowhere to redraw, so `s` prints a single snapshot instead.

`j` writes a snapshot without leaving the REPL — the scan's totals plus the 500
largest entries recorded so far:

```
> j                     # -> c__space_report.json (derived from the scan root)
> j biggest.json        # -> biggest.json, overwriting it if it exists
```

The report's `scan_state` field says whether the scan was still running when it
was taken, so a partial report is recognizable as one.

### Query a finished scan

Print the biggest entries:

```
python space_analyzer.py top
python space_analyzer.py top -n 30
python space_analyzer.py top --min-size 1gb --dirs-only
python space_analyzer.py top --prefix c:/users --files-only
```

Sizes accept `b`, `kb`, `mb`, `gb`, `tb` suffixes (or a bare number of bytes).

Look up one path:

```
python space_analyzer.py query "c:/program files"
```

Both commands read the database for `c:/` by default; pass `--scan-root D:/` to
read a different scan. Both accept `--json` for machine-readable output.

### Start over

```
python space_analyzer.py reset c:/
```

Deletes the database and resume state for that scan root.

## Where the data lives

Each scan root gets its own database file, named after the path (e.g. `c:/`
becomes `_c___space_analyzer.db`) alongside a small `*_last_path.txt` resume
marker. **Both are written to the current working directory**, not next to the
scanned path — so run the tool from wherever you want the database to live. A
full-drive scan can produce a database of a few gigabytes.

## Output conventions

Results go to stdout; progress, warnings, and status go to stderr. So this works
cleanly in a pipeline:

```
python space_analyzer.py top --json -n 50 > biggest.json
```

## Running the tests

```
python -m pytest
```

The suite uses an in-memory mock filesystem, so it never touches your real
drives and finishes in under a second.
