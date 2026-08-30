"""The planted promo-code bug, encoded as tests: the attribution check fails on
shipped data (exactly 150 unattributed orders) and passes once the codes are
normalized — so CI stays green while the bug ships."""

import shutil

import duckdb
import pandas as pd
import pytest
from conftest import (
    STEP5_SKIP_REASON,
    invoke_definition,
    materialize_all,
    metadata_value,
    read_table,
)

from cadence import checks
from cadence.assets import marts, raw, staging
from cadence.assets.staging import normalize_promo_codes
from cadence.resources import RAW_DIR

PASSING_CHECKS = (
    "order_amounts_valid",
    "orders_reference_known_events",
    "show_up_rate_in_bounds",
)
FAILING_CHECK = "all_promo_orders_attributed"


def _evaluations(result) -> dict:
    return {e.check_name: e for e in result.get_asset_check_evaluations()}


def test_all_four_checks_evaluated(shipped_run):
    result, _ = shipped_run
    assert set(_evaluations(result)) == {*PASSING_CHECKS, FAILING_CHECK}


def test_promo_check_fails_on_shipped_data(shipped_run, fix_applied):
    if fix_applied:
        pytest.skip(STEP5_SKIP_REASON)
    result, _ = shipped_run
    evaluation = _evaluations(result)[FAILING_CHECK]
    assert evaluation.passed is False
    assert metadata_value(evaluation, "unattributed_orders") == 150


def test_other_checks_pass_on_shipped_data(shipped_run):
    result, _ = shipped_run
    evaluations = _evaluations(result)
    for name in PASSING_CHECKS:
        assert evaluations[name].passed is True, f"{name} should pass on shipped data"


def test_promo_check_passes_on_normalized_frame(shipped_run):
    """Apply the README Step 5 fix to the dirty frames and re-run the check body."""
    _, db_path = shipped_run
    orders = read_table(db_path, "stg_orders")
    campaigns = read_table(db_path, "stg_campaigns")

    clean_orders = orders.copy()
    clean_orders["promo_code"] = normalize_promo_codes(clean_orders["promo_code"])

    clean_cp = invoke_definition(
        marts.campaign_performance,
        {"stg_orders": clean_orders, "stg_campaigns": campaigns},
    )
    check_result = invoke_definition(
        checks.all_promo_orders_attributed,
        {
            "campaign_performance": clean_cp,
            "stg_orders": clean_orders,
            "stg_campaigns": campaigns,
        },
    )
    assert check_result.passed is True


def test_promo_check_passes_after_full_pipeline_fix(tmp_path):
    """End-to-end: with a fixed stg_orders, the whole graph goes green AND the
    README Step 5 story holds — the campaign ranking actually changes."""
    result = materialize_all(tmp_path, fix_promo_codes=True)
    assert result.success
    evaluations = _evaluations(result)
    assert evaluations[FAILING_CHECK].passed is True
    for name in PASSING_CHECKS:
        assert evaluations[name].passed is True

    report = read_table(tmp_path / "test.duckdb", "box_office_report")
    kpis = dict(zip(report["metric"], report["value"], strict=True))
    assert kpis["best_campaign_by_revenue"] == "Summer Kickoff"
    assert kpis["best_campaign_per_dollar"] == "VIP Love Letter"


def test_order_amounts_valid_fails_on_duplicate_order_id(shipped_run):
    """The blocking check's failure path: the README's break-it-on-purpose exercise."""
    _, db_path = shipped_run
    orders = read_table(db_path, "stg_orders")
    doctored = pd.concat([orders, orders.iloc[[0]]], ignore_index=True)
    result = invoke_definition(checks.order_amounts_valid, {"stg_orders": doctored})
    assert result.passed is False


def test_stg_orders_drops_nonpositive_rows():
    """The README also promises qty <= 0 rows never reach the check — staging drops them."""
    raw_orders = pd.read_csv(RAW_DIR / "orders.csv")
    victim = raw_orders.loc[0, "order_id"]
    raw_orders.loc[0, "qty"] = -1
    stg = invoke_definition(staging.stg_orders, {"raw_orders": raw_orders})
    assert victim not in set(stg["order_id"])


def test_blocking_check_halts_downstream(tmp_path, monkeypatch):
    """A duplicated order_id fails blocking order_amounts_valid and the marts never build."""
    doctored_raw = tmp_path / "raw"
    shutil.copytree(RAW_DIR, doctored_raw)
    orders_csv = doctored_raw / "orders.csv"
    lines = orders_csv.read_text().splitlines(keepends=True)
    orders_csv.write_text("".join(lines) + lines[1])
    monkeypatch.setattr(raw, "RAW_DIR", doctored_raw)

    result = materialize_all(tmp_path)
    assert result.success is False

    con = duckdb.connect(str(tmp_path / "test.duckdb"), read_only=True)
    try:
        tables = {
            r[0]
            for r in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }
    finally:
        con.close()
    assert "stg_orders" in tables
    assert "campaign_performance" not in tables
