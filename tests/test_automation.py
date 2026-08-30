"""The file-drop sensor: baseline on first evaluation, one run per genuinely new file."""

import shutil

import dagster as dg

from cadence import automation
from cadence.resources import SCANS_DIR

NIGHT_8 = "ticket_scans_2025-07-08.csv"


def _run_sensor(context):
    result = automation.new_scan_file_sensor(context)
    return list(result) if isinstance(result, list) else result


def _committed_nights(tmp_path, monkeypatch):
    """A copy of data/scans/ pinned to nights 1-7, whatever the live checkout holds
    (night 8 may be present locally if someone ran `make new-day`)."""
    scans = tmp_path / "scans"
    shutil.copytree(SCANS_DIR, scans)
    (scans / NIGHT_8).unlink(missing_ok=True)
    monkeypatch.setattr(automation, "SCANS_DIR", scans)
    return scans


def test_first_evaluation_records_baseline_and_skips(tmp_path, monkeypatch):
    _committed_nights(tmp_path, monkeypatch)
    context = dg.build_sensor_context(cursor=None)
    result = _run_sensor(context)
    assert isinstance(result, dg.SkipReason)
    assert "baseline" in str(result.skip_message)
    # cursor now points at the last existing night, so history never re-fires
    assert context.cursor == "ticket_scans_2025-07-07.csv"


def test_new_file_triggers_exactly_one_run(tmp_path, monkeypatch):
    scans = _committed_nights(tmp_path, monkeypatch)

    context = dg.build_sensor_context(cursor="ticket_scans_2025-07-07.csv")
    assert isinstance(_run_sensor(context), dg.SkipReason)

    (scans / NIGHT_8).write_text("scan_id,ticket_id,order_id,event_id,gate,scanned_at\n")
    context = dg.build_sensor_context(cursor="ticket_scans_2025-07-07.csv")
    requests = _run_sensor(context)
    assert [r.run_key for r in requests] == [NIGHT_8]
    assert context.cursor == NIGHT_8

    # same cursor, same files: nothing new
    context = dg.build_sensor_context(cursor=NIGHT_8)
    assert isinstance(_run_sensor(context), dg.SkipReason)
