# Troubleshooting

The long versions of everything abridged in the [README](../README.md) and the [guide](guide/).

> **First, the classic:** if `all_promo_orders_attributed` is **red** on your first
> materialization — that is not a bug, that is the tutorial. See [guide Chapter 3](guide/03-checks.md). Every
> other check (`order_amounts_valid`, `orders_reference_known_events`,
> `show_up_rate_in_bounds`) should be green on shipped data.

## Port 3000 is already in use

**Symptom** — `make dev` fails with something like:

```
OSError: [Errno 98] Address already in use
```

(or on macOS: `[Errno 48]`), or the browser shows a different app at `http://localhost:3000`.

**Cause** — another process holds port 3000: a previous `dagster dev` you never stopped, or a
JavaScript dev server (Create React App and friends love this port).

**Fix** — either stop the other process, or run Dagster on a different port:

```bash
# macOS/Linux
DAGSTER_HOME=$PWD/.dagster_home uv run dagster dev -p 3001
```

```powershell
# Windows PowerShell
$env:DAGSTER_HOME = "$PWD\.dagster_home"; uv run dagster dev -p 3001
```

Then browse to `http://localhost:3001`. To find the squatter on macOS/Linux:
`lsof -i :3000`; on Windows: `netstat -ano | findstr :3000`.

## DuckDB: "Conflicting lock" or "different configuration"

**Symptom** — a materialization (or `make query`) fails with an error like:

```
duckdb.duckdb.IOException: IO Error: Could not set lock on file
"…/data/warehouse/cadence.duckdb": Conflicting lock is held in …/python (PID 12345).
See also https://duckdb.org/docs/stable/connect/concurrency
```

or:

```
duckdb.duckdb.ConnectionException: Connection Error: Can't open a connection to same
database file with a different configuration than existing connections
```

**Cause** — the warehouse is a single DuckDB file, and **DuckDB allows one writing process
at a time**. Something else has the file open: a Python REPL or notebook where you ran
`duckdb.connect(...)` and never closed it, a `make query` racing a materialization,
overlapping Dagster runs, or a DB browser app pointed at the file. The second error is the
same disease inside one process — mixing a read-only connection with a writer.

**Fix** — find and close the other connection:

1. Close any notebook/REPL/DB-browser session holding `cadence.duckdb` (or call `con.close()`).
2. Wait for the in-flight Dagster run to finish before running another writer.
3. Re-run whatever failed.

This is exactly why `scripts/query.py` opens the file with `read_only=True` and closes the
connection in a `finally` block — copy that pattern into any exploration script you write.
Worst case, nothing is truly stuck: stop `dagster dev`, run `make reset`, and materialize
again (the warehouse is disposable; the CSVs are the source of truth).

## Windows without `make` — the full command table

Stock Windows has no `make`. Every target is a single command; run these from the repo root
in PowerShell:

| `make` target | PowerShell equivalent |
|---|---|
| `make setup` | `uv sync --frozen` |
| `make dev` | `$env:DAGSTER_HOME = "$PWD\.dagster_home"; uv run dagster dev` |
| `make materialize` | `uv run dagster job execute -m cadence.definitions -j refresh_all` |
| `make publish` | `$env:PUBLISH_REPORT = "1"; uv run dagster job execute -m cadence.definitions -j refresh_all` |
| `make test` | `uv run pytest` |
| `make lint` | `uv run ruff check .` |
| `make fmt` | `uv run ruff format .` |
| `make query Q="SELECT …"` | `uv run python scripts/query.py "SELECT …"` |
| `make new-day` | `uv run python scripts/add_night.py` |
| `make data` | `uv run python scripts/generate_data.py --seed 42 --out data` |
| `make reset` | `Remove-Item data\warehouse\*.duckdb, data\warehouse\*.duckdb.wal, data\scans\ticket_scans_2025-07-0[89].csv -ErrorAction SilentlyContinue; Remove-Item -Recurse data\nights -ErrorAction SilentlyContinue` |

(If you have Git Bash or WSL, the macOS/Linux commands in the guide work as-is; `winget
install GnuWin32.Make` also exists if you want `make` itself.)

## What `make reset` does (and doesn't do)

`make reset` runs:

```bash
rm -f data/warehouse/*.duckdb data/warehouse/*.duckdb.wal data/scans/ticket_scans_2025-07-08.csv
rm -rf data/warehouse/*.duckdb.tmp .tmp_dagster_home_* data/nights
rm -f data/scans/ticket_scans_2025-07-0[9].csv data/scans/ticket_scans_2025-07-[1-9][0-9].csv
```

- **Deletes the warehouse** (`cadence.duckdb` and any write-ahead log). It's fully disposable —
  the next materialization rebuilds every table from the CSVs.
- **Restores `data/scans/` to nights 1–7** by removing night 8's file plus any shows `make
  new-day` synthesized past the roster (nights 9+, which live in `data/nights/`). All of that is
  gitignored, so growing the season never dirties your `git status`; night 8's original stays in
  `data/extra/`.
- **Does not** touch run history or sensor state. Both live in `.dagster_home/` — and both
  block a sensor-demo replay: the sensor's cursor remembers the last filename it saw, *and*
  Dagster deduplicates sensor runs by run key (the filename), so even a reset cursor won't
  re-fire night 8. To replay the demo: stop `dagster dev`, delete everything in
  `.dagster_home/` *except* `dagster.yaml`, start `dagster dev` again, materialize the graph
  ([guide Chapter 1](guide/01-assets.md)), then toggle the sensor and `make new-day`.
- **One ordering rule after a reset:** materialize the full graph *before* playing with the
  sensor. `make reset` deletes the warehouse, and the sensor's `refresh_admissions` job reads
  `stg_orders` from it — on an empty warehouse the run fails with
  `Catalog Error: Table with name stg_orders does not exist!`. That error always means the
  same thing: materialize the graph first ([guide Chapter 1](guide/01-assets.md)).

## Regenerating data vs. the committed CSVs

The CSVs under `data/` are **committed** and are the interface of record — you never need to
run the generator. But they're also fully reproducible:

- `make data` (= `uv run python scripts/generate_data.py --seed 42 --out data`) rewrites
  every CSV **byte-for-byte identical** to what's committed. `git status` stays clean;
  `tests/test_generator.py` enforces this.
- `make data SEED=7` (any other seed) writes a *different but coherent* season — same 8
  events and 12 campaigns, new orders, scans, and dirt. Two things follow:
  1. `git status` will show modified CSVs. Restore with `make data SEED=42` or
     `git checkout -- data` (newer git: `git restore data`).
  2. `make test` will fail its byte-identity test (`test_committed_csvs_reproducible`
     compares a fresh seed-42 generation against the files on disk) until you restore.
     That failure is the guardrail working, not a broken repo.

After regenerating with any seed, re-materialize everything (the warehouse still holds the
old season) — and note the planted-dirt *counts* (150 dirty codes, 3 orphans) are constants
in `cadence/data_gen.py`, so the Chapter 3 story survives a re-seed.

## uv behind a corporate proxy

**Symptom** — the `curl … astral.sh` / `irm … astral.sh` installers from guide Chapter 0 are
blocked, hang, or fail TLS.

**Fixes**, in order of preference:

1. **Install uv from PyPI instead** — it's just a Python package, and your proxy likely
   already allows pip:

   ```bash
   pip install uv          # or: pipx install uv
   ```

2. **Point uv at your proxy** — uv honors the standard variables:

   ```bash
   export HTTPS_PROXY=http://proxy.example.com:8080   # PowerShell: $env:HTTPS_PROXY = "http://proxy.example.com:8080"
   ```

3. **Custom CA bundle** — if your proxy re-signs TLS, export
   `SSL_CERT_FILE=/path/to/corporate-ca.pem` so uv (and pip) trust it. Don't disable TLS
   verification.

Remember: the network is needed **only** for installing uv and the first
`uv sync --frozen` (downloading Python 3.11 + the pinned wheels). Everything after that —
the pipeline, the UI, the tests — runs fully offline.

## Still stuck?

Open an issue on the repo — unless it's the red check. It's supposed to be red. Step 5.
