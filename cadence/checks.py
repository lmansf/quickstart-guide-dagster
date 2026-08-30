"""Four data-quality checks — one of them is SUPPOSED to fail red. See README Step 5."""

import dagster as dg
import pandas as pd

from cadence.assets.marts import attendance_by_event, campaign_performance
from cadence.assets.staging import stg_orders


@dg.asset_check(
    asset=stg_orders,
    blocking=True,
    description=(
        "Blocking guardrail: qty >= 1, positive prices, non-negative net revenue, "
        "unique order ids. If this fails, nothing downstream runs."
    ),
)
def order_amounts_valid(stg_orders: pd.DataFrame) -> dg.AssetCheckResult:
    bad_qty = int((stg_orders["qty"] < 1).sum())
    bad_price = int((stg_orders["unit_price_usd"] <= 0).sum())
    bad_net = int((stg_orders["net_revenue"] < 0).sum())
    duplicate_ids = int(stg_orders["order_id"].duplicated().sum())
    return dg.AssetCheckResult(
        passed=(bad_qty + bad_price + bad_net + duplicate_ids) == 0,
        severity=dg.AssetCheckSeverity.ERROR,
        metadata={
            "bad_qty_rows": dg.MetadataValue.int(bad_qty),
            "bad_price_rows": dg.MetadataValue.int(bad_price),
            "negative_net_revenue_rows": dg.MetadataValue.int(bad_net),
            "duplicate_order_ids": dg.MetadataValue.int(duplicate_ids),
        },
    )


@dg.asset_check(
    asset=stg_orders,
    additional_ins={"stg_events": dg.AssetIn("stg_events")},
    description="Referential integrity: every order points at a real event on the roster.",
)
def orders_reference_known_events(
    stg_orders: pd.DataFrame, stg_events: pd.DataFrame
) -> dg.AssetCheckResult:
    unknown = stg_orders[~stg_orders["event_id"].isin(set(stg_events["event_id"]))]
    return dg.AssetCheckResult(
        passed=unknown.empty,
        severity=dg.AssetCheckSeverity.ERROR,
        metadata={"orders_with_unknown_event": dg.MetadataValue.int(len(unknown))},
    )


@dg.asset_check(
    asset=campaign_performance,
    additional_ins={
        "stg_orders": dg.AssetIn("stg_orders"),
        "stg_campaigns": dg.AssetIn("stg_campaigns"),
    },
    description=(
        "Every promo-coded order matches a known campaign code. "
        "EXPECTED TO FAIL on first run — see README Step 5."
    ),
)
def all_promo_orders_attributed(
    campaign_performance: pd.DataFrame,
    stg_orders: pd.DataFrame,
    stg_campaigns: pd.DataFrame,
) -> dg.AssetCheckResult:
    known_codes = set(stg_campaigns["promo_code"].dropna())
    coded = stg_orders[stg_orders["promo_code"].notna()]
    bad = coded[~coded["promo_code"].isin(known_codes)]
    sample = bad["promo_code"].drop_duplicates().head(10)
    return dg.AssetCheckResult(
        passed=bad.empty,
        severity=dg.AssetCheckSeverity.ERROR,
        metadata={
            "unattributed_orders": dg.MetadataValue.int(len(bad)),
            "unattributed_revenue": dg.MetadataValue.float(float(bad["net_revenue"].sum())),
            "sample_bad_codes": dg.MetadataValue.md("\n".join(f"- `{code!r}`" for code in sample)),
            "hint": dg.MetadataValue.text(
                "Open cadence/assets/staging.py and find the TODO — this check is expected "
                "to fail until you fix it. See README Step 5."
            ),
        },
    )


@dg.asset_check(
    asset=attendance_by_event,
    description=(
        "Sanity (WARN): show-up rates within [0, 1], overall rate within [0.5, 1.0], "
        "and never more scans than tickets sold."
    ),
)
def show_up_rate_in_bounds(attendance_by_event: pd.DataFrame) -> dg.AssetCheckResult:
    rates_ok = bool(attendance_by_event["show_up_rate"].between(0, 1).all())
    scans_ok = bool(
        (attendance_by_event["tickets_scanned"] <= attendance_by_event["tickets_sold"]).all()
    )
    overall = float(
        attendance_by_event["tickets_scanned"].sum() / attendance_by_event["tickets_sold"].sum()
    )
    overall_ok = 0.5 <= overall <= 1.0
    return dg.AssetCheckResult(
        passed=rates_ok and scans_ok and overall_ok,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={"overall_show_up_rate": dg.MetadataValue.float(overall)},
    )
