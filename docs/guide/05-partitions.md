# Chapter 5 — Partitions and backfills

*~5 minutes, optional. [Guide index](README.md) · Prev: [Automation](04-automation.md) · Next: [Publishing →](06-publishing.md)*

Everything so far rebuilt whole tables. That stops scaling the moment "whole table" means three
years of orders. Partitions let you slice an asset and rebuild slices independently.

## Declaring a partitioned asset

From `cadence/assets/daily.py`:

```python
daily_partitions = dg.DailyPartitionsDefinition(start_date="2025-06-20", end_date="2025-07-09")


@dg.asset(
    group_name="daily",
    partitions_def=daily_partitions,
    metadata={"partition_expr": "order_date"},
)
def daily_sales(context, stg_orders: pd.DataFrame) -> pd.DataFrame:
    day = context.partition_key
    ...
```

`daily_sales` is the same `stg_orders` data sliced by calendar day — **19 partitions**,
2025-06-20 through 2025-07-08, exactly the sales window.

Bounding it with an `end_date` is deliberate: no partitions ahead of the data, so "materialize
the latest partition" always has something in it and a backfill can't run away from you.

`context.partition_key` is how the asset knows which slice it's computing — the same code runs
once per partition with a different key.

## The line that makes it warehouse-friendly

The `metadata={"partition_expr": "order_date"}` line in that decorator is the one that matters.

It tells the DuckDB IO manager which column identifies a slice. On each run it
**deletes-and-inserts only that date's rows** in the single `main.daily_sales` table.

One table, 19 independently rebuildable slices. Re-running July 1 never touches July 4 — which
is the entire point: reprocessing one bad day shouldn't mean rebuilding the year, and two
partitions running at once shouldn't clobber each other.

## Try it

1. Click `daily_sales` in the lineage view — note the **partition bar**: 19 slots, all missing.
2. Materialize a single partition, say `2025-07-01`. One day's orders, grouped by tier.
3. Open the **Materialize** dialog again and select **all 19** — Dagster launches a
   **backfill**. Watch the runs fan out and the bar fill in.

Then look at the shape of the season:

```bash
make query Q="SELECT order_date, SUM(net_revenue) AS net FROM daily_sales GROUP BY 1 ORDER BY 1"
```

Raw equivalent (all platforms — quote the SQL):

```bash
uv run python scripts/query.py "SELECT order_date, SUM(net_revenue) AS net FROM daily_sales GROUP BY 1 ORDER BY 1"
```

> [!NOTE]
> **One honest wrinkle.** The last slice, July 8, materializes **zero rows** — ticket sales close
> the night before each show, and July 8 is show day for the final concert. An empty partition
> that *should* be empty still succeeds. But an empty-only write doesn't create the table at all,
> so materialize at least one non-empty day before running that query.

## Where partitions pay off

Time is the common case, but the mechanism is general — `StaticPartitionsDefinition` slices by
region, tenant, or venue; multi-dimensional partitions combine both.

The three wins, in practice: **cheap reprocessing** (one bad day, not the year), **parallelism**
(slices are independent, so they run concurrently), and **honest tracking** (the partition bar
tells you exactly which slices exist — "is last March loaded?" stops being a question you answer
with a query).

[docs/use-cases.md](../use-cases.md) shows both variants in context — monthly partitions for
membership billing, static partitions per venue for a multi-site box office.

## What you learned

- A `partitions_def` turns one asset into many independently materializable slices
- `context.partition_key` tells the asset which slice it's computing
- `partition_expr` lets the IO manager delete-and-insert **just that slice** of one table
- **Backfills** materialize many partitions in one launch; bounded ranges keep them sane

---

**[Next: Publishing a report →](06-publishing.md)**
