import dagster as dg

from cadence import checks
from cadence.assets import daily, dashboard, marts, publish, raw, report, staging
from cadence.automation import (
    admissions_job,
    daily_refresh_schedule,
    new_scan_file_sensor,
    refresh_all_job,
)
from cadence.resources import database_io_manager

defs = dg.Definitions(
    assets=dg.load_assets_from_modules([raw, staging, marts, report, daily, dashboard, publish]),
    asset_checks=dg.load_asset_checks_from_modules([checks]),
    jobs=[refresh_all_job, admissions_job],
    schedules=[daily_refresh_schedule],
    sensors=[new_scan_file_sensor],
    resources={"io_manager": database_io_manager},
)
