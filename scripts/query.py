"""Run a read-only SQL query against the Cadence warehouse and print it as Markdown.

Usage:
    uv run python scripts/query.py "SELECT * FROM campaign_performance"
    make query Q="SELECT * FROM campaign_performance"

Deliberately dependency-light: resolves the DuckDB path itself instead of importing
the `cadence` package. Opens read-only and always closes the connection — DuckDB is
single-writer, and a lingering connection would block Dagster's next materialization.
"""

import sys
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "warehouse" / "cadence.duckdb"


def main() -> int:
    if len(sys.argv) < 2 or not sys.argv[1].strip():
        print('Usage: uv run python scripts/query.py "SELECT ..."', file=sys.stderr)
        return 2
    if not DB_PATH.exists():
        print(
            f"No warehouse found at {DB_PATH}.\n"
            "Materialize the assets first (make dev, then click 'Materialize all' "
            "at http://localhost:3000 — or run: make materialize).",
            file=sys.stderr,
        )
        return 1
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        df = con.execute(sys.argv[1]).fetchdf()
        print(df.to_markdown(index=False))
    except duckdb.Error as exc:
        print(exc, file=sys.stderr)
        return 1
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
