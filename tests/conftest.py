"""Shared fixtures and helpers for the Cadence Hall test suite.

Every materialization here targets a throwaway DuckDB file under pytest's tmp
directories — never data/warehouse/cadence.duckdb.
"""

import inspect
import shutil
from pathlib import Path

import dagster as dg
import duckdb
import pandas as pd
import pytest
from dagster_duckdb_pandas import DuckDBPandasIOManager

from cadence import checks
from cadence.assets import daily, marts, raw, report, staging

CORE_GROUPS = ("raw", "staging", "marts", "reporting")
DB_FILENAME = "test.duckdb"


def load_defs_assets() -> list[dg.AssetsDefinition]:
    """The same load_assets_from_modules call cadence/definitions.py makes."""
    return dg.load_assets_from_modules([raw, staging, marts, report, daily])


def load_defs_checks() -> list:
    """The same load_asset_checks_from_modules call cadence/definitions.py makes."""
    return dg.load_asset_checks_from_modules([checks])


@pytest.fixture(scope="session")
def defs_assets() -> list[dg.AssetsDefinition]:
    return load_defs_assets()


@pytest.fixture(scope="session")
def defs_checks() -> list:
    return load_defs_checks()


@pytest.fixture
def tmp_io_manager(tmp_path: Path) -> DuckDBPandasIOManager:
    return DuckDBPandasIOManager(database=str(tmp_path / DB_FILENAME), schema="main")


def invoke_definition(definition, frames: dict[str, pd.DataFrame]):
    """Directly invoke an asset or asset-check definition's compute function.

    Passes only the inputs the underlying function actually declares, and supplies a
    build_asset_context() when (and only when) the function takes a context parameter.
    Returns whatever the compute function returns (a DataFrame for assets, an
    AssetCheckResult for checks).
    """
    fn = definition.op.compute_fn.decorated_fn
    params = list(inspect.signature(fn).parameters)
    kwargs = {name: frames[name] for name in params if name in frames}
    if params and params[0] == "context":
        return definition(dg.build_asset_context(), **kwargs)
    return definition(**kwargs)


def _with_promo_fix(assets: list[dg.AssetsDefinition]) -> list[dg.AssetsDefinition]:
    """Swap the shipped stg_orders for one that applies the README Step 5 fix."""
    key = dg.AssetKey("stg_orders")
    original = next(a for a in assets if key in a.keys)

    @dg.asset(
        name="stg_orders",
        group_name="staging",
        description="stg_orders with the promo-code normalization fix applied (test-only).",
    )
    def stg_orders(raw_orders: pd.DataFrame) -> pd.DataFrame:
        df = invoke_definition(original, {"raw_orders": raw_orders}).copy()
        df["promo_code"] = staging.normalize_promo_codes(df["promo_code"])
        return df

    return [a for a in assets if key not in a.keys] + [stg_orders]


def materialize_all(tmp_path: Path, fix_promo_codes: bool = False) -> dg.ExecuteInProcessResult:
    """Materialize the full non-partitioned graph (groups raw/staging/marts/reporting)
    plus all asset checks into a throwaway DuckDB at tmp_path/test.duckdb.

    Scans are read from a copy of data/scans/ pinned to the committed nights 1-7, so
    the suite gives the same answers whether or not `make new-day` has delivered the
    (gitignored) night-8 file into the live directory.

    Gotcha (verified): dagster.materialize() silently skips asset checks unless the
    check definitions are included in the ``assets`` list — so they are always passed
    here alongside the asset definitions.
    """
    assets = load_defs_assets()
    if fix_promo_codes:
        assets = _with_promo_fix(assets)
    io_manager = DuckDBPandasIOManager(database=str(tmp_path / DB_FILENAME), schema="main")
    scans_copy = tmp_path / "scans-nights-1-7"
    shutil.copytree(raw.SCANS_DIR, scans_copy)
    (scans_copy / "ticket_scans_2025-07-08.csv").unlink(missing_ok=True)
    original_scans_dir = raw.SCANS_DIR
    raw.SCANS_DIR = scans_copy
    try:
        return dg.materialize(
            assets=[*assets, *load_defs_checks()],
            resources={"io_manager": io_manager},
            selection=dg.AssetSelection.groups(*CORE_GROUPS),
            raise_on_error=False,
        )
    finally:
        raw.SCANS_DIR = original_scans_dir


@pytest.fixture(scope="session")
def shipped_run(tmp_path_factory: pytest.TempPathFactory):
    """One shared full materialization of the shipped (dirty) data.

    Returns (result, db_path): the ExecuteInProcessResult and the DuckDB file it wrote.
    """
    tmp = tmp_path_factory.mktemp("shipped_warehouse")
    result = materialize_all(tmp)
    return result, tmp / DB_FILENAME


def read_table(db_path: Path, table: str) -> pd.DataFrame:
    """Read one table from a materialized test warehouse (read-only, always closed)."""
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        return con.execute(f"SELECT * FROM main.{table}").df()
    finally:
        con.close()


def metadata_value(evaluation, key: str):
    """Unwrap a MetadataValue from an AssetCheckEvaluation's metadata dict."""
    value = evaluation.metadata[key]
    return getattr(value, "value", value)
