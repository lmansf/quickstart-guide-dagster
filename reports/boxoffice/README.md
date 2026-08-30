# Box office report — Summer season 2025

A three-page static HTML report over the Cadence Hall warehouse
(`data/warehouse/cadence.duckdb`), unified by one goal: **maximize profit by
growing net revenue +5% YoY** — from $335,166 this season to $351,924 next
season (+$16,758).

| Page | File | Carries |
|---|---|---|
| 1 · Overview & plan | `index.html` | Season KPIs and the four-lever bridge to +5% (~$20.7K identified) |
| 2 · The revenue engine | `revenue.html` | Revenue by night, sell-through, tier mix, sales pacing — levers 1–2 |
| 3 · Marketing & attendance | `marketing.html` | Campaign ROI, the promo-code attribution fix, show-up rates — levers 3–4 |

Open `index.html` in a browser (pages link to each other; no build step, no
external dependencies).

## Provenance

- All figures were queried from the mart/staging tables the Dagster pipeline
  writes to DuckDB (`stg_orders`, `revenue_by_tier`, `attendance_by_event`,
  `campaign_performance`, `daily_sales` inputs).
- **Campaign figures on page 3 use cleaned promo codes** (`TRIM` + `UPPER`),
  i.e. the post–README-Step-5 attribution. The shipped warehouse's
  `campaign_performance` table is the *pre-fix* view; the page shows both and
  explains the $13,703 difference.
- The season has no true prior year in the dataset, so "+5% YoY" is framed as
  next season's target against this season's actuals.
- Charts are hand-rolled HTML/CSS/inline-SVG following the dataviz reference
  palette (validated for color-vision-deficiency safety in light and dark
  modes); every chart has a table twin.
