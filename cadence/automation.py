"""Jobs, the (stopped) schedule, and the file-drop sensor — everything that runs on its own."""

import dagster as dg

from cadence.resources import SCANS_DIR

# The default "refresh the business" button: everything except the optional daily partitions.
refresh_all_job = dg.define_asset_job(
    "refresh_all",
    selection=dg.AssetSelection.groups("raw", "staging", "marts", "reporting"),
)

# What the sensor kicks off: the scans entry point plus everything downstream of it.
admissions_job = dg.define_asset_job(
    "refresh_admissions",
    selection=dg.AssetSelection.assets("raw_ticket_scans").downstream(),
)

daily_refresh_schedule = dg.ScheduleDefinition(
    name="daily_refresh",
    job=refresh_all_job,
    cron_schedule="0 6 * * *",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)


@dg.sensor(
    job=admissions_job,
    minimum_interval_seconds=15,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def new_scan_file_sensor(context: dg.SensorEvaluationContext):
    """Watch data/scans/ for new nightly files (try it: `make new-day`).

    The cursor is the lexicographically greatest filename seen so far — filenames
    embed the date, so name-ordering is date-ordering, and (unlike mtime) it is
    immune to checkout/copy timestamp noise.

    On its very first evaluation (no cursor yet) it records the files already on
    disk as the baseline and skips, instead of firing one run per historical file —
    turning the sensor on should watch for what's NEW, not reprocess the archive.
    """
    files = sorted(p.name for p in SCANS_DIR.glob("ticket_scans_*.csv"))
    if context.cursor is None:
        context.update_cursor(files[-1] if files else "")
        return dg.SkipReason(f"first evaluation: recorded baseline of {len(files)} existing files")
    new_files = [name for name in files if name > context.cursor]
    if not new_files:
        return dg.SkipReason("no new scan files")
    context.update_cursor(max(new_files))
    return [dg.RunRequest(run_key=name) for name in new_files]
