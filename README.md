# Dagster Quickstart Guide

A hands-on introduction to [Dagster](https://dagster.io) that you can finish over lunch. Eight
short chapters, one concept each, every one of them something you *run* rather than read about.

By the end you'll have built and operated a real pipeline: assets and lineage, an IO manager
writing to a local warehouse, data-quality checks (including one you get to fix), jobs,
schedules, a sensor that fires on its own, partitions and backfills, a published report, and a
test suite over all of it.

**→ [Start the guide](docs/guide/)**

> [!NOTE]
> Written against **Dagster 1.13** (pinned to `1.13.20`). Everything runs **fully offline** and
> deterministically after install — no API keys, no cloud account, no Docker.

---

## The 60-second start

If `uv --version` prints something, you're three commands from a running Dagster UI:

```bash
git clone https://github.com/lmansf/quickstart-guide-dagster.git
cd quickstart-guide-dagster
make setup && make dev
```

Then open **http://localhost:3000**. No `uv` yet, or no `make`? That's
[Chapter 0](docs/guide/00-setup.md) — it covers cold machines, Windows, and corporate proxies.

## The guide

| # | Chapter | You'll learn |
|---|---|---|
| 0 | [Set up and launch](docs/guide/00-setup.md) | Install, bootstrap, get the UI running |
| 1 | [Assets and lineage](docs/guide/01-assets.md) | The core idea: declare what data *should exist* |
| 2 | [IO managers and the warehouse](docs/guide/02-io-managers.md) | How a DataFrame becomes a table, and why that's separate from your logic |
| 3 | [Checks and data quality](docs/guide/03-checks.md) | Catch bad data — then fix a real bug that's shipped on purpose |
| 4 | [Jobs, schedules, and sensors](docs/guide/04-automation.md) | Make the pipeline run without you |
| 5 | [Partitions and backfills](docs/guide/05-partitions.md) | Slice a dataset by day and rebuild slices independently |
| 6 | [Publishing a report](docs/guide/06-publishing.md) | Turn assets into something non-engineers actually open |
| 7 | [Testing a pipeline](docs/guide/07-testing.md) | Test data code like software |

Chapters 1–4 are the core (**about 30 minutes**). Chapters 5–7 are each ~5 minutes and can be
taken in any order, or skipped.

> [!IMPORTANT]
> **One data-quality check fails — red — on your very first run.** That's deliberate; it's the
> subject of [Chapter 3](docs/guide/03-checks.md). **Don't file a bug.**

## Use case examples

The pipeline in this guide happens to be a live-music venue, but the *shape* — sources → staging
that normalizes join keys → one table per business question → a published report — is the shape
of most analytics pipelines.

**[docs/use-cases.md](docs/use-cases.md)** — *"Six pipelines hiding inside this one"* — maps it
onto six adjacent domains, each with a scenario, a mini asset graph, and a table of what to copy
from this repo:

- **Marketing attribution** — messy UTM/promo codes, spend vs. revenue by campaign
- **Box office / ticketing ops** — multi-venue settlement, refunds, per-venue partitions
- **Admissions funnels** — applications → offers → enrollments, yield by program
- **Memberships & season passes** — recurring billing, MRR, pass utilization
- **Webinar registrations** — registered vs. actually attended, dedupe on rejoins
- **Merch sales** — per-event POS files, revenue per SKU, attach rate to attendance

## What's in here

The guide drives a worked example — a fictional venue's data platform, small enough to read in
one sitting:

```
docs/guide/        the guide itself — start here
docs/use-cases.md  adapting the pattern to your domain
cadence/           the example pipeline (15 assets, 4 checks, 2 jobs, 1 schedule, 1 sensor)
data/              seeded sample CSVs — campaigns, orders, gate scans
reports/boxoffice/ the published HTML report from Chapter 6
tests/             the test suite from Chapter 7
```

Two more references, for when you want them:
[data dictionary](docs/data-dictionary.md) (every table and column, including the deliberately
planted dirt) and [troubleshooting](docs/troubleshooting.md) (port conflicts, DuckDB locks,
Windows without `make`).

## Command reference

Every `make` target and the raw command it runs. On Windows without `make`, use the raw column.

| `make` target | What it does | Raw equivalent (macOS/Linux; PowerShell where it differs) |
|---|---|---|
| `make setup` | Create `.venv`, install exact pins | `uv sync --frozen` |
| `make dev` | Launch Dagster at `localhost:3000` | `DAGSTER_HOME=$PWD/.dagster_home uv run dagster dev` · PS: `$env:DAGSTER_HOME = "$PWD\.dagster_home"; uv run dagster dev` |
| `make materialize` | Headless full refresh (no UI) | `uv run dagster job execute -m cadence.definitions -j refresh_all` |
| `make publish` | Refresh **and** push the report so the host redeploys | `PUBLISH_REPORT=1 uv run dagster job execute -m cadence.definitions -j refresh_all` |
| `make test` | Run the test suite | `uv run pytest` |
| `make lint` | Lint with ruff | `uv run ruff check .` |
| `make fmt` | Format with ruff | `uv run ruff format .` |
| `make query Q="…"` | Read-only SQL against the warehouse | `uv run python scripts/query.py "…"` |
| `make new-day` | Deliver night 8 to the sensor (Chapter 4) | `cp data/extra/ticket_scans_2025-07-08.csv data/scans/` · PS: `Copy-Item data\extra\ticket_scans_2025-07-08.csv data\scans\` |
| `make data` | Regenerate CSVs (`SEED=42` default) | `uv run python scripts/generate_data.py --seed 42 --out data` |
| `make reset` | Delete warehouse + temp dirs, restore nights 1–7 | `rm -f data/warehouse/*.duckdb data/warehouse/*.duckdb.wal data/scans/ticket_scans_2025-07-08.csv && rm -rf data/warehouse/*.duckdb.tmp .tmp_dagster_home_*` · PS: `Remove-Item data\warehouse\*.duckdb, data\warehouse\*.duckdb.wal, data\scans\ticket_scans_2025-07-08.csv -ErrorAction SilentlyContinue; Remove-Item -Recurse data\warehouse\*.duckdb.tmp, .tmp_dagster_home_* -ErrorAction SilentlyContinue` |

---

*The venue is fictional and the data is synthetic — generated by `cadence/data_gen.py` at seed
42 to make the story land. Interesting, but not industry benchmarks; don't quote a made-up
venue's ROAS in a real meeting.*
