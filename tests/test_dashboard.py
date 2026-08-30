"""The publishing asset: dashboard data export writes a coherent payload."""

import json

import dagster as dg
import pytest
from conftest import read_table

from cadence.assets.dashboard import boxoffice_dashboard_data


@pytest.fixture
def exported(shipped_run, tmp_path, monkeypatch):
    """Run the export against the shipped warehouse, redirected into tmp_path."""
    _, db_path = shipped_run
    out_dir = tmp_path / "boxoffice"
    monkeypatch.setenv("BOXOFFICE_REPORT_DIR", str(out_dir))
    boxoffice_dashboard_data(
        dg.build_asset_context(),
        stg_orders=read_table(db_path, "stg_orders"),
        stg_events=read_table(db_path, "stg_events"),
        stg_campaigns=read_table(db_path, "stg_campaigns"),
        attendance_by_event=read_table(db_path, "attendance_by_event"),
    )
    return out_dir


def test_writes_json_and_js_twins(exported):
    payload = json.loads((exported / "data.json").read_text(encoding="utf-8"))
    js = (exported / "data.js").read_text(encoding="utf-8")
    assert js.startswith("//")
    assert "window.BOXOFFICE_DATA = {" in js
    assert json.dumps(payload)  # round-trips

    assert set(payload) >= {
        "generated_at",
        "goal",
        "totals",
        "attribution",
        "derived",
        "events",
        "daily",
        "campaigns",
        "levers",
        "levers_total",
    }


def test_totals_match_the_warehouse(exported, shipped_run):
    _, db_path = shipped_run
    payload = json.loads((exported / "data.json").read_text(encoding="utf-8"))
    orders = read_table(db_path, "stg_orders")
    assert payload["totals"]["net"] == pytest.approx(float(orders["net_revenue"].sum()))
    assert payload["totals"]["tickets_sold"] == int(orders["qty"].sum())
    assert len(payload["events"]) == 8
    assert len(payload["campaigns"]) == 12
    # Attribution is computed on CLEANED codes even on a pristine checkout, so the
    # dirty-code revenue shows up as reassignable, not lost.
    assert payload["attribution"]["still_unattributed_net"] == 0.0


def test_the_plan_covers_the_gap(exported):
    """The four levers must add up to more than the +5% gap at seed 42 —
    the report's whole narrative rests on this."""
    payload = json.loads((exported / "data.json").read_text(encoding="utf-8"))
    assert payload["goal"]["target_net"] == pytest.approx(payload["totals"]["net"] * 1.05)
    assert payload["levers_total"] >= payload["goal"]["gap"]
    assert all(lever["amount"] > 0 for lever in payload["levers"])
