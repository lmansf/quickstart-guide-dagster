# Chapter 6 — Publishing a report

*~5 minutes, optional. [Guide index](README.md) · Prev: [Partitions](05-partitions.md) · Next: [Testing →](07-testing.md)*

Everything you've built so far lives in a warehouse and a developer UI. The people who asked the
three questions in your inbox — marketing, box office, operations — don't have either. This
chapter closes that gap, and introduces a kind of asset that doesn't return a table at all.

## Look at it first

```bash
make materialize
```

Then open **`reports/boxoffice/index.html`** in a browser (double-click it — no server needed).

Three pages, unified by one goal: **grow net revenue +5% next season**.

| Page | File | Carries |
|---|---|---|
| Overview & plan | `index.html` | Season KPIs and a four-lever bridge to +5% |
| The revenue engine | `revenue.html` | Revenue by night, sell-through, tier mix, sales pacing |
| Marketing & attendance | `marketing.html` | Campaign ROI, the promo-code attribution story, show-up rates |

The four levers are the pipeline's own findings turned into proposals: rescue the soft night,
reprice/expand VIP, reallocate ad spend, and convert refunds into exchange credits. Page 3 shows
attribution **as-recorded vs. cleaned** side by side — the Chapter 3 bug, quantified for someone
who will never open Dagster.

## The asset behind it

`cadence/assets/dashboard.py`, group `publishing`:

```python
@dg.asset(group_name="publishing", description="Export the box office dashboard data …")
def boxoffice_dashboard_data(
    context,
    stg_orders: pd.DataFrame,
    stg_events: pd.DataFrame,
    stg_campaigns: pd.DataFrame,
    attendance_by_event: pd.DataFrame,
) -> None: ...
```

Same dependency rule as every other asset — parameters name the upstreams. One thing is new:
**it returns `None`.**

Assets whose value is an external side effect — a file written, a dashboard refreshed, an email
sent, a model deployed — return nothing. There's no DataFrame for the IO manager to store,
because the artifact *is* the point. Dagster still tracks it as an asset: it appears in the
lineage graph downstream of the marts, records materializations, and emits metadata.

That's the mental model worth taking away. "Asset" doesn't mean "table." It means *a thing that
should exist and can be rebuilt from its inputs* — and a published report qualifies.

It writes two files, the same payload twice:

- **`data.json`** — for programmatic consumers (a notebook, an API, a build step)
- **`data.js`** — the same payload as `window.BOXOFFICE_DATA`, loaded by a plain `<script>` tag
  so the pages work opened straight off the filesystem, where `fetch()` of a local file is
  blocked

`render.js` builds every chart and table from that payload and fills the numbers quoted in prose
via `data-fill` spans. Change the data, re-materialize, and the prose changes with it — no HTML
editing.

## Why the export is deterministic

The payload is a **pure function of its upstream assets**: no wall-clock timestamp, no
randomness. Re-materializing unchanged data rewrites byte-identical files.

That's a deliberate design choice with a practical payoff. The report is committed to the repo,
so it renders the moment you clone — and because a re-run produces identical bytes, `make
materialize` leaves `git status` clean instead of showing a diff whose only content is "the
clock moved."

If you want provenance in a generated artifact, derive it from the data rather than the clock.
This report stamps `season.scans_through` and `season.nights_scanned` — which tell a reader
something true and useful ("this covers 7 of 8 nights") and stay stable across runs.

> [!TIP]
> Run [Chapter 4](04-automation.md)'s sensor demo (`make new-day`), then re-materialize and
> reload the page. Night 8 appears in the attendance chart and the stamp reads 8 nights. A night
> with no scan file simply drops out — the report degrades gracefully rather than erroring.

## Refreshing and deploying

`refresh_all` includes the `publishing` group, so a normal refresh republishes the report. Or
target it alone:

```bash
uv run dagster asset materialize -m cadence.definitions --select boxoffice_dashboard_data
```

`reports/boxoffice/` is a self-contained static site — serve it as a site root, no build step.
To keep it live, have your deployment materialize the asset and publish the refreshed `data.js`
/ `data.json`. Set `BOXOFFICE_REPORT_DIR` to redirect the export somewhere else (the tests use
this; so would a deploy that writes into a build directory).

## What you learned

- Assets can produce **external artifacts**, not just tables — they return `None` and still get
  lineage, materialization history, and metadata
- Separating **data export from presentation** means the pipeline owns numbers and the HTML owns
  layout
- **Deterministic exports** keep committed artifacts honest and diffs meaningful — derive
  provenance from data, not the clock

---

**[Next: Testing a pipeline →](07-testing.md)**
