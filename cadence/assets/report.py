"""Reporting layer: the executive one-pager that sits on top of all three marts."""

import dagster as dg
import pandas as pd

# Cadence Hall is a 1,200-cap room; a night is a sellout when every ticket moved.
VENUE_CAPACITY = 1200


@dg.asset(
    group_name="reporting",
    description="The executive one-pager: eight KPIs answering all three teams at once.",
)
def box_office_report(
    context: dg.AssetExecutionContext,
    campaign_performance: pd.DataFrame,
    revenue_by_tier: pd.DataFrame,
    attendance_by_event: pd.DataFrame,
) -> pd.DataFrame:
    tiers = revenue_by_tier[revenue_by_tier["event_name"] != "TOTAL"]
    total_net = float(tiers["net_revenue"].sum())
    tickets_sold = int(tiers["tickets_sold"].sum())

    overall_rate = float(
        attendance_by_event["tickets_scanned"].sum() / attendance_by_event["tickets_sold"].sum()
    )

    real = campaign_performance[campaign_performance["campaign_id"].str.startswith("CMP-")]
    best_by_revenue = str(real.sort_values("attributed_revenue", ascending=False).iloc[0]["name"])
    spenders = real[real["spend_usd"] > 0].dropna(subset=["revenue_per_dollar"])
    best_per_dollar = str(
        spenders.sort_values("revenue_per_dollar", ascending=False).iloc[0]["name"]
    )

    tier_totals = tiers.groupby("tier")["net_revenue"].sum().sort_values(ascending=False)
    top_tier = str(tier_totals.index[0])

    per_event_sold = tiers.groupby("event_name")["tickets_sold"].sum()
    sellouts = sorted(per_event_sold[per_event_sold >= VENUE_CAPACITY].index)
    sellout_str = ", ".join(sellouts) if sellouts else "none"

    worst = attendance_by_event.sort_values("show_up_rate").iloc[0]
    worst_str = f"{worst['event_name']} ({worst['show_up_rate']:.1%} show-up)"

    df = pd.DataFrame(
        [
            ("total_net_revenue", f"${total_net:,.2f}"),
            ("tickets_sold", f"{tickets_sold:,}"),
            ("overall_show_up_rate", f"{overall_rate:.1%}"),
            ("best_campaign_by_revenue", best_by_revenue),
            ("best_campaign_per_dollar", best_per_dollar),
            ("top_tier_by_revenue", top_tier),
            ("sellout_events", sellout_str),
            ("worst_no_show_event", worst_str),
        ],
        columns=["metric", "value"],
    )

    top5 = campaign_performance.head(5)[
        ["name", "channel", "attributed_orders", "attributed_revenue", "revenue_per_dollar"]
    ]
    show_up = attendance_by_event[["event_name", "tickets_sold", "tickets_scanned", "show_up_rate"]]
    executive_summary = "\n".join(
        [
            f"**Cadence Hall — July 2025 stand.** Net revenue **${total_net:,.2f}** on "
            f"{tickets_sold:,} tickets; {overall_rate:.0%} of sold tickets walked through the "
            f"door. Top campaign by revenue: **{best_by_revenue}**; best return per dollar: "
            f"**{best_per_dollar}**.",
            "",
            "**Top 5 campaigns by attributed revenue**",
            "",
            top5.to_markdown(index=False, floatfmt=".2f"),
            "",
            "**Show-up by event**",
            "",
            show_up.to_markdown(index=False, floatfmt=".3f"),
        ]
    )
    context.add_output_metadata(
        {
            "row_count": dg.MetadataValue.int(len(df)),
            "executive_summary": dg.MetadataValue.md(executive_summary),
        }
    )
    return df
