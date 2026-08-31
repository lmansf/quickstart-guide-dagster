from pathlib import Path

from dagster_duckdb_pandas import DuckDBPandasIOManager

PROJECT_ROOT = Path(__file__).resolve().parent.parent  # repo root (flat layout)
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
SCANS_DIR = DATA_DIR / "scans"
# Synthesized shows past the committed roster (gitignored) — see scripts/add_night.py
EXTRA_NIGHTS_DIR = DATA_DIR / "nights"
DB_PATH = DATA_DIR / "warehouse" / "cadence.duckdb"

database_io_manager = DuckDBPandasIOManager(database=str(DB_PATH), schema="main")
