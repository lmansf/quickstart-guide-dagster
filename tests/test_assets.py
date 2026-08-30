"""Materialize the real asset graph into a throwaway DuckDB and check the business
numbers the guide promises."""

import dagster as dg
import duckdb
import pytest
from conftest import DB_FILENAME, STEP5_SKIP_REASON, read_table

from cadence.assets import daily, raw, staging

CORE_TABLES = [
    "raw_campaigns",
    "raw_events",
    "raw_orders",
    "raw_ticket_scans",
    "stg_campaigns",
    "stg_events",
    "stg_orders",
    "stg_ticket_scans",
    "campaign_performance",
    "revenue_by_tier",
    "attendance_by_event",
    "box_office_report",
]
KPI_METRICS = {
    "total_net_revenue",
    "tickets_sold",
    "overall_show_up_rate",
    "best_campaign_by_revenue",
    "best_campaign_per_dollar",
    "top_tier_by_revenue",
    "sellout_events",
    "worst_no_show_event",
}


def test_full_graph_materializes(shipped_run):
    result, _ = shipped_run
    assert result.success


def test_all_core_tables_written(shipped_run):
    _, db_path = shipped_run
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
    finally:
        con.close()
    assert set(CORE_TABLES) <= {r[0] for r in rows}


def test_box_office_report_has_eight_kpis(shipped_run):
    _, db_path = shipped_run
    df = read_table(db_path, "box_office_report")
    assert len(df) == 8
    assert set(df["metric"]) == KPI_METRICS


def test_campaign_performance_rows(shipped_run):
    _, db_path = shipped_run
    df = read_table(db_path, "campaign_performance")
    # 12 campaigns + "(unattributed)" + "(organic)"
    assert len(df) == 14
    labels = set(df["campaign_id"].astype(str)) | set(df["name"].astype(str))
    assert "(unattributed)" in labels
    assert "(organic)" in labels
    # CMP-04 has no promo code, so code-based attribution cannot see it
    cmp04 = df[df["campaign_id"] == "CMP-04"]
    assert len(cmp04) == 1
    assert int(cmp04["attributed_orders"].iloc[0]) == 0


def test_revenue_by_tier_total(shipped_run):
    _, db_path = shipped_run
    df = read_table(db_path, "revenue_by_tier")
    is_total = df.apply(lambda row: "TOTAL" in {str(v) for v in row.values}, axis=1)
    assert int(is_total.sum()) == 1
    total_net = float(df.loc[is_total, "net_revenue"].iloc[0])
    # the README prints this to the cent, and the inputs are byte-pinned — assert exactly
    assert total_net == pytest.approx(335_166.00, abs=0.01)
    # the TOTAL row equals the sum of the per-(event, tier) rows
    assert abs(total_net - float(df.loc[~is_total, "net_revenue"].sum())) < 1.0


def test_overall_show_up_rate_in_band(shipped_run):
    _, db_path = shipped_run
    df = read_table(db_path, "attendance_by_event")
    # attendance_by_event only lists events that have scans: the committed data
    # covers nights 1-7, and EV-08 appears after `make new-day` feeds the sensor
    assert len(df) == 7
    assert "EV-08" not in set(df["event_id"].astype(str))
    rate = float(df["tickets_scanned"].sum()) / float(df["tickets_sold"].sum())
    assert 0.78 <= rate <= 0.90


def test_pre_fix_kpi_story(shipped_run, fix_applied):
    """Before the Step 5 fix, dirty promo codes crown the WRONG campaigns —
    the exact misdirection README Step 3 shows the reader."""
    if fix_applied:
        pytest.skip(STEP5_SKIP_REASON)
    _, db_path = shipped_run
    report = read_table(db_path, "box_office_report")
    kpis = dict(zip(report["metric"], report["value"], strict=True))
    assert kpis["best_campaign_by_revenue"] == "Radio Week"
    assert kpis["best_campaign_per_dollar"] == "Student Rush"


def test_daily_sales_single_partition(tmp_path, tmp_io_manager):
    result = dg.materialize(
        [raw.raw_orders, staging.stg_orders, daily.daily_sales],
        partition_key="2025-07-01",
        resources={"io_manager": tmp_io_manager},
    )
    assert result.success
    df = read_table(tmp_path / DB_FILENAME, "daily_sales")
    assert len(df) >= 1
    assert set(df["order_date"].astype(str)) == {"2025-07-01"}


def test_daily_sales_empty_final_partition_succeeds(tmp_path, tmp_io_manager):
    """2025-07-08 is legitimately empty (sales close the night before the last show):
    the README promises the empty slice still materializes without error."""
    result = dg.materialize(
        [raw.raw_orders, staging.stg_orders, daily.daily_sales],
        partition_key="2025-07-08",
        resources={"io_manager": tmp_io_manager},
    )
    assert result.success
    # the IO manager may skip table creation entirely for an all-empty write;
    # if the table exists it must hold no rows for that date
    con = duckdb.connect(str(tmp_path / DB_FILENAME), read_only=True)
    try:
        tables = {
            r[0]
            for r in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }
        if "daily_sales" in tables:
            rows = con.execute(
                "SELECT COUNT(*) FROM main.daily_sales WHERE order_date >= '2025-07-08'"
            ).fetchone()[0]
            assert rows == 0
    finally:
        con.close()
