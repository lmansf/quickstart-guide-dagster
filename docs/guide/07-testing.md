# Chapter 7 — Testing a pipeline

*~5 minutes, optional. [Guide index](README.md) · Prev: [Publishing](06-publishing.md)*

Data pipelines get tested less than application code, usually for one reason: people assume a
test needs a warehouse. It doesn't. This chapter shows the three levels that cover a pipeline
like this one.

```bash
make test
```

Raw equivalent (all platforms):

```bash
uv run pytest
```

## Level 1 — does it even load?

`tests/test_definitions.py` is one line:

```python
dg.Definitions.validate_loadable(defs)
```

That catches every wiring error in the project: a parameter naming an asset that doesn't exist,
a missing resource key, a job selecting nothing, a duplicate asset name. Cheap, fast, and it
fails at the exact moment you'd otherwise discover the problem by launching a run and waiting.

If you take one idea from this chapter into your own project, take this one.

## Level 2 — pure functions, no database

Because assets are plain functions of DataFrames ([Chapter 2](02-io-managers.md)), you can call
one directly with a hand-built frame and assert on what comes back. No warehouse, no IO manager,
milliseconds.

That's what `tests/test_checks.py` does to prove the blocking check actually blocks: build a
frame with a duplicated `order_id`, invoke the check, assert it failed.

The design win from Chapter 2 is what makes this possible. Logic that opens its own database
connections can't be tested this way.

## Level 3 — materialize the real graph

The strongest test runs the actual pipeline into a **throwaway DuckDB** in a temp directory, then
asserts on real numbers:

```python
result = dg.materialize(
    assets=[*assets, *checks],
    resources={
        "io_manager": DuckDBPandasIOManager(database=str(tmp / "test.duckdb"), schema="main")
    },
    selection=dg.AssetSelection.groups("raw", "staging", "marts", "reporting"),
)
```

Swapping in a temp-path IO manager is the whole trick — same assets, same checks, disposable
storage. `tests/test_assets.py` uses it to assert the report has its 8 KPI rows, the
`(unattributed)` and `(organic)` rows exist, CMP-04 sits at zero attributed orders, and total net
revenue is exactly **$335,166.00**.

> [!NOTE]
> **A verified gotcha:** `dagster.materialize()` silently skips asset checks unless the check
> definitions are included in the `assets` list. A suite that forgets this passes happily while
> testing no checks at all.

## What the suite actually covers

| File | Guards |
|---|---|
| `test_definitions.py` | Everything wires together |
| `test_assets.py` | Real materialization; business numbers; partitioned and empty-partition runs |
| `test_checks.py` | The planted bug fails dirty and passes clean; blocking check halts downstream |
| `test_generator.py` | Sample data regenerates **byte-identically** at seed 42; planted-dirt counts |
| `test_automation.py` | The sensor baselines on first tick, then fires once per new file |
| `test_dashboard.py` | The export is deterministic and matches the committed report |

Two of those deserve a note.

**The bug is a test.** `test_checks.py` asserts `all_promo_orders_attributed` *fails* on shipped
data with exactly 150 unattributed orders, and *passes* once codes are normalized. CI stays green
while a real bug ships — because here the bug is a feature. And if you applied the
[Chapter 3](03-checks.md) fix in your working tree, that test notices and **skips** with an
explanation rather than failing. A guide whose own tests break when you follow it teaches the
wrong lesson.

**Determinism is a test.** The sample data regenerates byte-identically at seed 42, and so does
the published report. Both properties are asserted, which is what lets this repo promise that
`make materialize` leaves `git status` clean.

## Keep it tidy

```bash
make lint
```

Raw equivalent: `uv run ruff check .` (and `make fmt` → `uv run ruff format .`)

CI runs exactly this — `uv sync --frozen`, ruff check, ruff format check, pytest — see
`.github/workflows/ci.yml`.

## What you learned

- **`validate_loadable`** catches wiring errors in one line
- Pure-function assets are **directly callable** in tests — no database required
- `dg.materialize()` with a **temp-path IO manager** runs the real graph disposably
- Include check definitions in `materialize()` or your checks silently don't run
- Encoding intentional behavior (a planted bug, determinism) keeps a teaching repo honest

---

## That's the guide

You've built and operated a pipeline end to end: assets and lineage, storage through an IO
manager, data-quality checks and a real fix, jobs and schedules and a sensor, partitions and
backfills, a published report, and tests over all of it.

**Where to go next:**

- **[docs/use-cases.md](../use-cases.md)** — six adaptations of this exact skeleton: marketing
  attribution, ticketing ops, admissions funnels, memberships, webinars, merch. Each with a
  scenario, an asset-graph sketch, and a table of what to copy.
- **Deal a new season.** `make data SEED=7` regenerates every CSV as a different-but-coherent
  season — same events and campaigns, new orders, new dirt. Re-materialize and watch every answer
  change. (`make test`'s byte-identity check compares seed 42 to the committed files, so restore
  with `make data SEED=42` when you're done.)
- **[Dagster docs](https://docs.dagster.io/)** — every concept here, in full depth.
- **[Dagster University](https://courses.dagster.io/)** — free structured courses; Dagster
  Essentials is a natural next four hours.
- **[dagster-dbt](https://docs.dagster.io/integrations/libraries/dbt)** — when your marts outgrow
  pandas, dbt models become assets in this same graph.
