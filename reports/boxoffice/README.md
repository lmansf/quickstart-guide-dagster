# Box office report — Summer season 2025

A three-page HTML report over the Cadence Hall warehouse, unified by one goal:
**maximize profit by growing net revenue +5% YoY**. The pages render from data
the pipeline exports, so re-running the pipeline refreshes every number and
chart — no HTML edits needed.

| Page | File | Carries |
|---|---|---|
| 1 · Overview & plan | `index.html` | Season KPIs and the four-lever bridge to +5% |
| 2 · The revenue engine | `revenue.html` | Revenue by night, sell-through, tier mix, sales pacing — levers 1–2 |
| 3 · Marketing & attendance | `marketing.html` | Campaign ROI, the promo-code attribution fix, show-up rates — levers 3–4 |

## How it works

```
warehouse (cadence.duckdb)
        │  boxoffice_dashboard_data asset  (cadence/assets/dashboard.py,
        ▼  group "publishing", included in the refresh_all job)
data.json  +  data.js          ← the numbers, one payload in two formats
        │
        ▼  <script src="data.js"> + render.js
index.html / revenue.html / marketing.html
```

- **`data.json`** — the payload for programmatic consumers (APIs, notebooks, a
  Vercel build step).
- **`data.js`** — the same payload as `window.BOXOFFICE_DATA`, loaded by the
  pages via a plain `<script>` tag so they work when opened straight off the
  filesystem (where `fetch()` of a local JSON is blocked).
- **`render.js`** — builds every chart and table from the payload and fills the
  numbers quoted in prose (`data-fill` spans). The values baked into the HTML
  are a static fallback if the data files are missing.
- **`report.css` / `report.js`** — shared styling (light + dark, CVD-validated
  palette) and the hover/focus tooltip layer.

To refresh after new data lands:

```sh
uv run dagster asset materialize -m cadence.definitions --select boxoffice_dashboard_data
# or: make materialize   (refresh_all includes the publishing group)
```

Set `BOXOFFICE_REPORT_DIR` to redirect the export (used by the tests, and handy
for a deploy pipeline that writes into a build directory).

## Deploying (e.g. Vercel)

The directory is a self-contained static site: serve `reports/boxoffice/` as
the site root, no build step. To keep it live, have your Dagster deployment
materialize `boxoffice_dashboard_data` and publish the refreshed `data.js` /
`data.json` (commit + push, or upload during a deploy hook) — the pages pick up
the new numbers on the next load.

## Provenance notes

- All figures come from the pipeline's staging/mart tables (`stg_orders`,
  `stg_events`, `stg_campaigns`, `attendance_by_event`).
- **Campaign figures use cleaned promo codes** (`TRIM` + `UPPER`) regardless of
  whether the README Step 5 staging fix is applied; page 3 shows the
  as-recorded vs cleaned attribution side by side and explains the difference.
- The dataset has no true prior year, so "+5% YoY" is framed as next season's
  target against this season's actuals. Lever-sizing assumptions are constants
  at the top of `cadence/assets/dashboard.py` and are restated on the pages.
- A night with no scan file yet (night 8 before `make new-day`) simply drops
  out of the attendance chart; everything else renders.
