"""Daily layer (optional chapter): a bounded 19-day partitioned slice of sales."""

import dagster as dg
import pandas as pd

daily_partitions = dg.DailyPartitionsDefinition(start_date="2025-06-20", end_date="2025-07-09")


@dg.asset(
    group_name="daily",
    partitions_def=daily_partitions,
    metadata={"partition_expr": "order_date"},
    description="One day of sales by tier — each partition rewrites its slice of main.daily_sales.",
)
def daily_sales(context: dg.AssetExecutionContext, stg_orders: pd.DataFrame) -> pd.DataFrame:
    day = stg_orders[stg_orders["order_date"] == context.partition_key]
    df = day.groupby("tier", as_index=False).agg(
        orders=("order_id", "count"),
        tickets=("qty", "sum"),
        net_revenue=("net_revenue", "sum"),
    )
    # Stored as TIMESTAMP so the IO manager's partition-window DELETE
    # (order_date >= '<day> 00:00:00' AND order_date < '<day+1> 00:00:00')
    # matches exactly this slice on re-materialization and backfills.
    df.insert(0, "order_date", pd.Timestamp(context.partition_key))

    context.add_output_metadata(
        {
            "row_count": dg.MetadataValue.int(len(df)),
            "partition_net_revenue": dg.MetadataValue.float(float(df["net_revenue"].sum())),
        }
    )
    return df
