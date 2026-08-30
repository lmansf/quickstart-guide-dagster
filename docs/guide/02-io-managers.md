# Chapter 2 — IO managers and the warehouse

*~6 minutes. [Guide index](README.md) · Prev: [Assets and lineage](01-assets.md) · Next: [Checks →](03-checks.md)*

Your assets return `pandas.DataFrame`s. Twelve tables now exist in a DuckDB file. Nobody wrote
any SQL. This chapter is about the piece in between.

## Where the data went

The warehouse is one plain file: `data/warehouse/cadence.duckdb`. Query it:

```bash
make query Q="SELECT * FROM campaign_performance ORDER BY attributed_revenue DESC"
```

Raw equivalent (all platforms — quote the SQL):

```bash
uv run python scripts/query.py "SELECT * FROM campaign_performance ORDER BY attributed_revenue DESC"
```

You get all 12 campaigns plus two special rows: **`(organic)`** (orders with no promo code at
all) and **`(unattributed)`** (orders whose code matched *no* campaign — remember that row, it's
Chapter 3's smoking gun).

Also note **Retarget Blitz (CMP-04)**: real spend, zero attributed orders. It has no promo code,
so code-based attribution is structurally blind to it. That's a lesson about attribution, not a
bug — and one every marketing team eventually learns the hard way.

## The IO manager

Look at `cadence/resources.py`. The whole storage layer is one object:

```python
database_io_manager = DuckDBPandasIOManager(database=str(DB_PATH), schema="main")
```

wired into `cadence/definitions.py` under the resource key `io_manager`:

```python
defs = dg.Definitions(
    assets=dg.load_assets_from_modules([raw, staging, marts, report, daily, dashboard]),
    asset_checks=dg.load_asset_checks_from_modules([checks]),
    jobs=[refresh_all_job, admissions_job],
    schedules=[daily_refresh_schedule],
    sensors=[new_scan_file_sensor],
    resources={"io_manager": database_io_manager},
)
```

An **IO manager** handles both directions of the boundary between your code and storage:

- **Output** — an asset returns a DataFrame; the IO manager writes it to `main.<asset_name>`
- **Input** — a downstream asset declares `stg_orders: pd.DataFrame`; the IO manager reads that
  table back and hands it over

So `campaign_performance` never opens a connection, never writes a `CREATE TABLE`, never knows
DuckDB exists. It takes DataFrames and returns a DataFrame. **Table name = asset name**, schema
`main`, every time.

`io_manager` is the *default* resource key, which is why no asset in this project specifies one.

## Why this separation earns its keep

Because storage becomes a swappable detail rather than something twelve assets each re-implement.

Point that one line at Snowflake (`SnowflakePandasIOManager`), BigQuery, or Postgres and the
entire pipeline follows — no asset function changes. Your business logic stays pure DataFrame
transformations, which also means you can call it directly in a test without a database at all
(you'll do exactly that in [Chapter 7](07-testing.md)).

It's the same instinct as dependency injection: the thing that varies by environment lives in
one place, injected, instead of hard-coded into every unit of logic.

## Reading the code

Now that both core concepts are in hand, the whole pipeline reads quickly — it's a dozen small
files:

```
cadence/
├── definitions.py        # the table of contents — pure wiring, no logic
├── resources.py          # ONE resource: the DuckDB IO manager + path constants
├── data_gen.py           # the deterministic sample-data generator (seed 42)
├── assets/
│   ├── raw.py            # 4 assets: read the CSVs verbatim
│   ├── staging.py        # 4 assets: parse, clean… and one loud TODO
│   ├── marts.py          # 3 assets: one business question each
│   ├── report.py         # 1 asset: the executive report
│   ├── daily.py          # 1 asset: the partitioned one (Chapter 5)
│   └── dashboard.py      # 1 asset: the published report (Chapter 6)
├── checks.py             # 4 data-quality checks (Chapter 3)
└── automation.py         # 2 jobs, 1 schedule, 1 sensor (Chapter 4)
```

Read them in flow order — `raw.py` → `staging.py` → `marts.py` → `report.py` — then
`definitions.py` to see how little wiring it takes.

The layering is worth copying: **raw** reads sources verbatim with no cleaning (so you can always
see what actually arrived), **staging** does the typing and cleaning once, **marts** answer one
business question each. When something's wrong, that structure tells you which layer to fix —
which is exactly what happens next.

> [!NOTE]
> **Why does `query.py` open read-only and close immediately?** DuckDB allows **one writer at a
> time**. A lingering connection — a notebook, a REPL, a query left open — blocks Dagster's next
> materialization. The exact error and fix are in [troubleshooting](../troubleshooting.md).

> [!NOTE]
> **Production notes (for later):** when one schema stops being enough, asset `key_prefix`es can
> route groups to different schemas. And when you'd rather write SQL than pandas,
> `DuckDBResource` hands assets a raw connection instead of DataFrame round-trips. This project
> deliberately uses neither — one concept at a time.

## What you learned

- An **IO manager** owns the boundary between asset logic and storage, in both directions
- Assets stay pure transformations; **table name = asset name** is a convention, not a rule you
  write out
- Swapping the warehouse is a one-line change, and testable logic falls out for free
- **raw → staging → marts** layering tells you where to fix things

---

**[Next: Checks and data quality →](03-checks.md)** — time to deal with that red mark.
