"""Marts layer: one asset per business question — marketing, box office, operations."""

import dagster as dg
import pandas as pd


@dg.asset(
    group_name="marts",
    description="Marketing's question: which campaign actually sold tickets, and at what cost?",
)
def campaign_performance(
    context: dg.AssetExecutionContext,
    stg_orders: pd.DataFrame,
    stg_campaigns: pd.DataFrame,
) -> pd.DataFrame:
    known_codes = set(stg_campaigns["promo_code"].dropna())
    has_code = stg_orders["promo_code"].notna()
    matches_campaign = stg_orders["promo_code"].isin(known_codes)

    # Exact-match join, codes as-is: dirty codes ("' summer25'") fail to join
    # until the learner applies the staging fix (docs/guide/03-checks.md).
    matched = stg_orders[has_code & matches_campaign]
    unattributed = stg_orders[has_code & ~matches_campaign]
    organic = stg_orders[~has_code]

    per_code = matched.groupby("promo_code").agg(
        attributed_orders=("order_id", "count"),
        tickets_sold=("qty", "sum"),
        attributed_revenue=("net_revenue", "sum"),
    )
    df = stg_campaigns[["campaign_id", "name", "channel", "spend_usd", "promo_code"]].merge(
        per_code, how="left", left_on="promo_code", right_index=True
    )
    df = df.drop(columns=["promo_code"])

    special_rows = pd.DataFrame(
        [
            {
                "campaign_id": label,
                "name": label,
                "channel": "-",
                "spend_usd": 0.0,
                "attributed_orders": len(subset),
                "tickets_sold": int(subset["qty"].sum()),
                "attributed_revenue": float(subset["net_revenue"].sum()),
            }
            for label, subset in (("(unattributed)", unattributed), ("(organic)", organic))
        ]
    )
    df = pd.concat([df, special_rows], ignore_index=True)

    df["attributed_orders"] = df["attributed_orders"].fillna(0).astype("int64")
    df["tickets_sold"] = df["tickets_sold"].fillna(0).astype("int64")
    df["attributed_revenue"] = df["attributed_revenue"].fillna(0.0).astype("float64")
    df["cost_per_ticket"] = (df["spend_usd"] / df["tickets_sold"]).where(df["tickets_sold"] > 0)
    df["revenue_per_dollar"] = (df["attributed_revenue"] / df["spend_usd"]).where(
        df["spend_usd"] > 0
    )
    df = df.sort_values("attributed_revenue", ascending=False).reset_index(drop=True)

    real = df[df["campaign_id"].str.startswith("CMP-")]
    context.add_output_metadata(
        {
            "row_count": dg.MetadataValue.int(len(df)),
            "top_campaign": dg.MetadataValue.text(str(real.iloc[0]["name"])),
            "leaderboard": dg.MetadataValue.md(df.to_markdown(index=False, floatfmt=".2f")),
        }
    )
    return df


@dg.asset(
    group_name="marts",
    description="The box office's question: what's revenue by tier, net of refunds?",
)
def revenue_by_tier(
    context: dg.AssetExecutionContext,
    stg_orders: pd.DataFrame,
    stg_events: pd.DataFrame,
) -> pd.DataFrame:
    joined = stg_orders.merge(stg_events[["event_id", "event_name"]], on="event_id", how="left")
    joined["refunded_qty"] = joined["qty"].where(joined["status"] == "refunded", 0)
    df = joined.groupby(["event_name", "tier"], as_index=False).agg(
        tickets_sold=("qty", "sum"),
        gross_revenue=("gross_revenue", "sum"),
        refunded_tickets=("refunded_qty", "sum"),
        net_revenue=("net_revenue", "sum"),
    )
    total = pd.DataFrame(
        [
            {
                "event_name": "TOTAL",
                "tier": "TOTAL",
                "tickets_sold": int(df["tickets_sold"].sum()),
                "gross_revenue": float(df["gross_revenue"].sum()),
                "refunded_tickets": int(df["refunded_tickets"].sum()),
                "net_revenue": float(df["net_revenue"].sum()),
            }
        ]
    )
    df = pd.concat([df, total], ignore_index=True)

    context.add_output_metadata(
        {
            "row_count": dg.MetadataValue.int(len(df)),
            "total_net_revenue": dg.MetadataValue.float(float(total.iloc[0]["net_revenue"])),
            "table": dg.MetadataValue.md(df.to_markdown(index=False, floatfmt=".2f")),
        }
    )
    return df


@dg.asset(
    group_name="marts",
    description="Operations' question: how many sold tickets actually walk through the door?",
)
def attendance_by_event(
    context: dg.AssetExecutionContext,
    stg_ticket_scans: pd.DataFrame,
    stg_orders: pd.DataFrame,
    stg_events: pd.DataFrame,
) -> pd.DataFrame:
    completed = stg_orders[stg_orders["status"] == "completed"]
    sold = (
        completed.groupby("event_id", as_index=False)["qty"]
        .sum()
        .rename(columns={"qty": "tickets_sold"})
    )
    scanned = (
        stg_ticket_scans.groupby("event_id", as_index=False)["ticket_id"]
        .nunique()
        .rename(columns={"ticket_id": "tickets_scanned"})
    )
    # Inner-join on scans: a night with no scan file yet (night 8 until the sensor
    # ingests it) has no attendance story to tell and would drag the overall rate down.
    df = (
        stg_events[["event_id", "event_name", "event_date"]]
        .merge(sold, on="event_id", how="left")
        .merge(scanned, on="event_id", how="inner")
    )
    df["tickets_sold"] = df["tickets_sold"].fillna(0).astype("int64")
    df["show_up_rate"] = df["tickets_scanned"] / df["tickets_sold"]
    df["no_shows"] = df["tickets_sold"] - df["tickets_scanned"]
    df = df.sort_values("event_id").reset_index(drop=True)

    overall = float(df["tickets_scanned"].sum() / df["tickets_sold"].sum())
    context.add_output_metadata(
        {
            "row_count": dg.MetadataValue.int(len(df)),
            # Numeric on purpose: the UI plots it across materializations.
            "overall_show_up_rate": dg.MetadataValue.float(overall),
            "table": dg.MetadataValue.md(df.to_markdown(index=False, floatfmt=".3f")),
        }
    )
    return df
