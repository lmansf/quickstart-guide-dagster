# Chapter 1 — Assets and lineage

*~8 minutes. [Guide index](README.md) · Prev: [Set up and launch](00-setup.md) · Next: [IO managers →](02-io-managers.md)*

This is the concept the rest of Dagster hangs off. Get it and everything else is detail.

## The idea: declare what should exist

Every asset in your UI says **"Never materialized."** Dagster knows all 14 assets, and every
dependency between them, before a single row has been computed.

That's the flip. A cron scheduler or an Airflow DAG is a list of *tasks to run* — imperative,
ordered, and it has no idea what data those tasks leave behind. Dagster inverts it: you declare
the **assets that should exist** (a table, a file, a model, a report), and a run is just the act
of making reality match that declaration.

Everything downstream comes from this. Dagster can tell you what's stale because it knows what
each asset is built from. It can recompute one table instead of twelve because it knows the
graph. It can attach a data-quality check to a table rather than to a task, because tables are
the things it models.

## Declaring one

Open `cadence/assets/marts.py` and look at a signature:

```python
@dg.asset(group_name="marts", description="Spend, orders, and revenue per campaign.")
def campaign_performance(
    stg_orders: pd.DataFrame,
    stg_campaigns: pd.DataFrame,
) -> pd.DataFrame: ...
```

Three things are happening, and only one of them is obvious:

1. **The function name is the asset name.** `campaign_performance` is what you see in the UI and
   what lands in the warehouse.
2. **A parameter named after another asset *is* the dependency.** `stg_orders` isn't just an
   argument — it declares "this asset is built from `stg_orders`," and Dagster hands you that
   asset's contents at runtime. No DAG file, no `>>` operators, no YAML. The name must match
   exactly.
3. **The return value is the asset's contents.** What happens to it is Chapter 2's subject.

That's the entire dependency mechanism in this project. The lineage graph you're looking at was
derived from parameter names.

## Materialize the graph

In the lineage view, click **Materialize all**. Dagster opens a *"Launch runs to materialize 14
assets"* dialog asking about partitions — that's `daily_sales` raising its hand early
([Chapter 5](05-partitions.md) is about it). For now take the simpler road:

**Cancel** that dialog → open the **Jobs** page → click **`refresh_all`** → **Launch run**.
That job packages exactly the 13 un-partitioned assets. (Jobs are Chapter 4; for now it's just
"the everything button.")

Watch the run: 13 assets flow left to right in about 15 seconds, each turning green as it
completes.

Headless equivalent, any time you want it without the UI:

```bash
make materialize
```

```bash
uv run dagster job execute -m cadence.definitions -j refresh_all
```

## Read the results

Click **`box_office_report`** → its latest materialization → the **metadata** pane →
**`executive_summary`**.

That one Markdown block answers all three questions from your inbox. Total net revenue for the
stand is **$335,166.00**. **Static Bloom** drew the worst show-up rate (~72%). The KPI row
`sellout_events: Neon Coyote` means one night sold all 1,200 seats.

Metadata is worth dwelling on. Assets emit it on every materialization — row counts, previews,
numbers, Markdown — and the UI keeps the history. Numeric metadata gets **plotted across
materializations**, which is how `overall_show_up_rate` becomes a trend line rather than a
number you have to remember. Look at `cadence/assets/report.py` to see how it's attached:

```python
context.add_output_metadata({"row_count": dg.MetadataValue.int(len(df)), ...})
```

Notice something **red** in the checks column along the way? Good eye. That's
[Chapter 3](03-checks.md) — leave it for now.

## The aha: selective recomputation

Here's the payoff for declaring a graph instead of a script.

In the lineage view, select **only** `campaign_performance` and materialize it. Two things
happen:

1. **Only that asset runs.** Dagster recomputed one table, not twelve — it knows the others
   didn't need it.
2. **`box_office_report` gets flagged stale** (an "unsynced"/stale badge downstream). Dagster
   knows its input changed underneath it, so it can tell you the report no longer reflects its
   sources.

You can see the reverse direction too. In the selection box at the top of the lineage view,
type:

```
key:"stg_orders"+
```

The trailing `+` means "this asset and everything downstream of it." The graph filters to
`stg_orders` plus the six assets that depend on it — the blast radius of any change you make
there. You'll use exactly this in Chapter 3 to see what a one-line fix touches.

## What you learned

- An asset is a **declaration that some data should exist**, not a task to run
- A **parameter named after an upstream asset** is how dependencies are declared
- Materializing makes reality match the declaration; **metadata** travels with each run
- Dagster recomputes **selectively** and tracks **staleness**, because it knows the graph

---

**[Next: IO managers and the warehouse →](02-io-managers.md)** — where did those DataFrames
actually go?
