# Data dictionary

Everything Cadence Hall knows, table by table: the three committed source CSVs, the per-night
scan files, and every derived table the pipeline writes into `data/warehouse/cadence.duckdb`
(schema `main`, one table per asset, table name = asset name). At the bottom: the
**planted-dirt registry** — every anomaly the generator deliberately buries in the data, with
its exact count, the constant in `cadence/data_gen.py` that plants it, and where the pipeline
surfaces it.

**Source of truth.** The CSVs under `data/` are committed *and* reproducible: under the
pinned environment, `uv run python scripts/generate_data.py --seed 42 --out data`
(= `make data`) rewrites them **byte-for-byte identical**. Formatting rules: fixed column
order, floats `%.2f`, dates `YYYY-MM-DD`, timestamps `YYYY-MM-DD HH:MM:SS`, `\n` line
endings, UTF-8, header row, no index column. The generator uses Python's stdlib
`random.Random(seed)` with `SEED = 42` — no numpy RNG, so determinism doesn't depend on
numpy's version.

## Source tables

### `data/raw/events.csv` — 8 rows (hardcoded roster, identical across seeds)

| column | dtype | semantics |
|---|---|---|
| `event_id` | str | `EV-01` … `EV-08` |
| `event_name` | str | show name |
| `genre` | str | jazz, folk, indie, funk, electronic, comedy, punk, classical |
| `event_date` | date `YYYY-MM-DD` | 2025-07-01 … 2025-07-08, one show per night |
| `capacity` | int64 | always 1200 |
| `doors_time` | str | always `"19:00"` |
| `show_time` | str | always `"20:00"` |

The roster, with each show's role in the story (sell-through target in parentheses):

| event_id | event_name | genre | date | story role |
|---|---|---|---|---|
| EV-01 | The Midnight Standard | jazz | 2025-07-01 | normal night (0.82) |
| EV-02 | Copper & Oak | folk | 2025-07-02 | normal night (0.74) |
| EV-03 | Glass Anthem | indie | 2025-07-03 | **the flop** (0.45) — also carries the 3 orphan scans |
| EV-04 | Brasswork Riot | funk | 2025-07-04 | big Friday (0.93) |
| EV-05 | Neon Coyote | electronic | 2025-07-05 | **the sellout** (1.00 — all 1,200 tickets) |
| EV-06 | Laugh Track Live | comedy | 2025-07-06 | best show-up rate (0.85 sell-through) |
| EV-07 | Static Bloom | punk | 2025-07-07 | **worst show-up, ~72%** (0.78 sell-through) |
| EV-08 | Riverlight Orchestra | classical | 2025-07-08 | **the sensor night** — scans held back in `data/extra/` |

Tier capacities (every event): GA 800, Balcony 300, VIP 100. Per-event tier prices (USD):

| event | GA | Balcony | VIP |
|---|---|---|---|
| EV-01 | 36.00 | 52.00 | 85.00 |
| EV-02 | 32.00 | 46.00 | 75.00 |
| EV-03 | 30.00 | 44.00 | 70.00 |
| EV-04 | 40.00 | 56.00 | 90.00 |
| EV-05 | 42.00 | 58.00 | 95.00 |
| EV-06 | 38.00 | 50.00 | 80.00 |
| EV-07 | 34.00 | 48.00 | 72.00 |
| EV-08 | 45.00 | 62.00 | 110.00 |

The price/capacity table lives only in `cadence/data_gen.py` (`TIER_CAPACITY` and the event
roster) — it is not a CSV. Orders carry their own `unit_price_usd`.

### `data/raw/campaigns.csv` — 12 rows (hardcoded roster)

| column | dtype | semantics |
|---|---|---|
| `campaign_id` | str | `CMP-01` … `CMP-12` |
| `name` | str | campaign name |
| `channel` | str | instagram, email, radio, facebook, google_search, street_team, youtube |
| `promo_code` | str, **may be empty** | canonical form: UPPERCASE, no whitespace; empty for CMP-04 (reads as NaN in pandas) |
| `spend_usd` | float64 | campaign spend |
| `starts_on` | date | attribution window start (inclusive) |
| `ends_on` | date | attribution window end (inclusive) |

| campaign_id | name | channel | promo_code | spend | window | story role |
|---|---|---|---|---|---|---|
| CMP-01 | Summer Kickoff | instagram | SUMMER25 | 1800.00 | 06-18 → 07-08 | volume leader; **most undercounted pre-fix** (~90 dirty codes) |
| CMP-02 | VIP Love Letter | email | VIPNIGHT | 120.00 | 06-20 → 07-08 | tiny spend, **best revenue-per-dollar post-fix** |
| CMP-03 | Radio Week | radio | ONAIR10 | 2400.00 | 06-20 → 06-30 | expensive, mediocre |
| CMP-04 | Retarget Blitz | facebook | *(empty)* | 950.00 | 06-22 → 07-08 | **no code → invisible to code attribution** |
| CMP-05 | Search Brand | google_search | FINDUS | 700.00 | 06-20 → 07-08 | steady |
| CMP-06 | Street Team Flyers | street_team | STREET15 | 600.00 | 06-24 → 07-06 | cheap tickets |
| CMP-07 | Local Press Blast | email | PRESSPLAY | 200.00 | 06-21 → 07-02 | early spike |
| CMP-08 | Genre Nights | instagram | GENREGEM | 850.00 | 06-25 → 07-08 | mid |
| CMP-09 | Last Call Push | instagram | LASTCALL | 1100.00 | 07-01 → 07-08 | last-week surge |
| CMP-10 | Student Rush | street_team | STUDENT10 | 300.00 | 06-20 → 07-08 | mid |
| CMP-11 | Encore Newsletter | email | ENCORE | 150.00 | 06-20 → 07-08 | quiet performer |
| CMP-12 | Partner Playlist | youtube | PLAYLOUD | 500.00 | 06-23 → 07-08 | mid |

### `data/raw/orders.csv` — 3,618 rows at seed 42 (sorted by `ordered_at`, then `order_id`)

| column | dtype | semantics |
|---|---|---|
| `order_id` | str | `ORD-00001` … zero-padded 5 digits, assigned in `ordered_at` order |
| `event_id` | str | FK → `events.event_id` |
| `ordered_at` | timestamp | purchase time; window 2025-06-20 00:00 → day before `event_date` 23:59 |
| `tier` | str | `GA` \| `Balcony` \| `VIP` |
| `qty` | int64 | tickets on the order, 1–6 |
| `unit_price_usd` | float64 | exactly the roster price for (event, tier) — promo codes are trackers, no discount math |
| `promo_code` | str, nullable | campaign tracker as typed by marketing — **150 rows are deliberately mangled** |
| `status` | str | `completed` \| `refunded` (~7%) |

**Distribution notes** (all knobs are module constants in `cadence/data_gen.py`):

- Orders are drawn per event until cumulative tickets reach `capacity × sell_through_target`,
  never exceeding per-tier capacity; EV-05 fills all three tiers to exactly 100%.
- `qty`: weighted choice 1–6 with p = [.35, .38, .13, .08, .04, .02] (`QTY_WEIGHTS`).
- `tier`: GA .62 / Balcony .26 / VIP .12 (`TIER_WEIGHTS`), subject to tier-capacity caps.
- `ordered_at`: two-hump mixture — with p = 0.55 exponential decay from window start
  (mean 5 days), else exponential ramp toward the event (mean 4 days before); hour-of-day
  weighted toward 18:00–22:00; clamped into the window. Every order therefore falls inside
  the 19 daily partitions (2025-06-20 … 2025-07-08).
- `promo_code`: 45% of orders (`PROMO_ATTACH_RATE`) get the code of a campaign whose
  window contains `ordered_at`, weighted by spend — except CMP-02's weight is boosted ×4 for
  VIP-tier orders (`VIP_BOOST_CAMPAIGN`, `VIP_BOOST_FACTOR`), which plants the
  "VIPNIGHT wins revenue-per-dollar" headline. The rest are null. CMP-04 never attaches (no code).
- `status`: 7% refunded (`REFUND_RATE`), per-order Bernoulli. Refunded orders keep their
  price and qty; net revenue is zeroed downstream, not here.

### `data/scans/ticket_scans_<YYYY-MM-DD>.csv` — one file per show night

Nights 1–7 committed in `data/scans/`; night 8 held back at
`data/extra/ticket_scans_2025-07-08.csv` (delivered by `make new-day` for the sensor demo).
Rows sorted by `scanned_at`, then `scan_id`.

| column | dtype | semantics |
|---|---|---|
| `scan_id` | str | `SCN-<night#>-00001` … zero-padded per night, assigned in `scanned_at` order |
| `ticket_id` | str | `ORD-#####-T#` — tickets exist implicitly: an order with `qty=3` owns `-T1`…`-T3`; suffix ≤ the order's `qty` |
| `order_id` | str | FK → `orders.order_id` (except the 3 planted orphans) |
| `event_id` | str | equals the event of the owning order |
| `gate` | str | `MAIN-1` \| `MAIN-2` \| `SIDE-A` |
| `scanned_at` | timestamp | gate time |

**Distribution notes:** for each ticket of each **completed** order, attendance probability is
`clamp(0.85 × tier_mult × event_mult, 0, 0.98)` with `tier_mult` VIP 1.08 / Balcony 1.02 /
GA 0.98 and `event_mult` EV-07 0.85 (worst show-up), EV-06 1.05 (best), else 1.00. Refunded
orders' tickets never scan. `scanned_at` ~ Normal(mean = show time − 25 min, σ = 18 min),
truncated to [19:00 doors, 21:00].

## Derived tables (written to DuckDB by the assets)

Each staging/mart table carries all upstream columns unless noted; listed here is what each
asset **adds, changes, or drops**.

### `stg_campaigns`

`starts_on`/`ends_on` parsed to datetime; `spend_usd` cast float64; empty `promo_code`
(CMP-04) becomes `pd.NA`. Codes are already canonical here — the mess is on the orders side.

### `stg_events`

`event_date` parsed to datetime; `capacity` cast int64; adds `show_ts` (timestamp) =
`event_date` at 20:00.

### `stg_orders`

| added / changed | dtype | semantics |
|---|---|---|
| `ordered_at` | datetime | parsed |
| `order_date` | str `YYYY-MM-DD` | date part of `ordered_at`; the `daily_sales` partition key |
| `gross_revenue` | float64 | `qty × unit_price_usd` |
| `net_revenue` | float64 | = `gross_revenue`, but `0.0` where `status == "refunded"` |
| `promo_code` | str, nullable | **shipped untouched — this is the planted bug.** The TODO in `cadence/assets/staging.py` tells you to apply `.str.strip().str.upper()` (README Step 5) |

Also drops any row with `qty ≤ 0` or `unit_price_usd ≤ 0` (none in shipped data — the
blocking `order_amounts_valid` check and the break-it-on-purpose exercise live on this rule).

### `stg_ticket_scans`

`scanned_at` parsed to datetime; **deduped** to the first scan per `ticket_id` (sorted by
`scanned_at`); **orphans dropped** (rows whose `order_id` matches no `stg_orders` row).
Materialization metadata reports `duplicate_scans_dropped` and `orphan_scans_dropped`
(= 3 on shipped data).

### `campaign_performance` — one row per campaign + 2 special rows (14 total)

Left-join of `stg_orders` to `stg_campaigns` on `promo_code` — **exact match, as-is**, so
dirty codes fail to join until you apply the Step 5 fix. Special rows: `(unattributed)`
(non-null code matching no campaign — 150 orders' worth pre-fix, zeroed post-fix) and
`(organic)` (null code).

| column | dtype | semantics |
|---|---|---|
| `campaign_id`, `name`, `channel` | str | from the campaign roster; `(unattributed)`/`(organic)` label the special rows |
| `spend_usd` | float64 | campaign spend (0.0 on special rows) |
| `attributed_orders` | int | orders whose code joined this campaign |
| `tickets_sold` | int | sum of `qty` |
| `attributed_revenue` | float | sum of `net_revenue`; sort key (descending) |
| `cost_per_ticket` | float | `spend_usd / tickets_sold`, NaN-safe |
| `revenue_per_dollar` | float | `attributed_revenue / spend_usd`, NaN-safe |

### `revenue_by_tier` — one row per (event, tier) + a `TOTAL` row

| column | dtype | semantics |
|---|---|---|
| `event_name`, `tier` | str | grouping keys; both `"TOTAL"` on the grand-total row |
| `tickets_sold` | int | sum of `qty` (all orders) |
| `gross_revenue` | float | sum of `gross_revenue` |
| `refunded_tickets` | int | tickets on refunded orders |
| `net_revenue` | float | sum of `net_revenue` (refunds count 0) |

### `attendance_by_event` — one row per event with scans ingested

| column | dtype | semantics |
|---|---|---|
| `event_id`, `event_name`, `event_date` | str / datetime | from `stg_events` |
| `tickets_sold` | int | sum of `qty` over **completed** orders |
| `tickets_scanned` | int | distinct deduped `ticket_id`s |
| `show_up_rate` | float 0–1 | `tickets_scanned / tickets_sold` |
| `no_shows` | int | `tickets_sold − tickets_scanned` |

Events join scans with an inner join, so a night with no scan file yet contributes no row:
until `make new-day` delivers night 8 and the sensor ingests it, this mart has **7 rows**
(EV-08 absent); afterwards, 8.

### `box_office_report` — exactly 8 KPI rows

Columns `metric (str), value (str)`. The rows: `total_net_revenue`, `tickets_sold`,
`overall_show_up_rate`, `best_campaign_by_revenue`, `best_campaign_per_dollar`,
`top_tier_by_revenue`, `sellout_events`, `worst_no_show_event`. The asset's
`executive_summary` metadata is the Markdown version you read in the UI (README Step 3).

### `daily_sales` — partitioned; one slice per day, single table

Partitioned by `daily_partitions = DailyPartitionsDefinition("2025-06-20", "2025-07-09")` —
19 partitions, 2025-06-20 … 2025-07-08. The asset metadata `{"partition_expr": "order_date"}`
makes the DuckDB IO manager delete-and-insert only the materialized partition's slice.

| column | dtype | semantics |
|---|---|---|
| `order_date` | timestamp | the partition key (stored as TIMESTAMP so the partition-window delete matches the slice exactly) |
| `tier` | str | GA / Balcony / VIP |
| `orders` | int | count of orders that day and tier |
| `tickets` | int | sum of `qty` |
| `net_revenue` | float | sum of `net_revenue` |

## The planted-dirt registry

Every anomaly in the shipped data is deliberate, deterministic at seed 42, and surfaced
somewhere you can see it. If you find dirt that isn't in this table, *that* would be a bug.

| anomaly | exact count | planted by (`cadence/data_gen.py`) | surfaced by |
|---|---|---|---|
| **Mangled promo codes** — lowercase, leading/trailing space, or Title Case (e.g. `' SUMMER25'`, `'summer25'`, `'VIPNIGHT '`) | **150** total: 90 from SUMMER25, 30 from VIPNIGHT, 30 from other codes | `DIRTY_SUMMER25_COUNT`, `DIRTY_VIPNIGHT_COUNT`, `DIRTY_OTHER_COUNT` (sum = `DIRTY_PROMO_TOTAL`), mutations from `PROMO_MUTATIONS` | Check `all_promo_orders_attributed` — **red by design** on first run; the `(unattributed)` row in `campaign_performance`. Fix: the TODO in `stg_orders` (README Step 5) |
| **Duplicate gate scans** — a second scan row for the same `ticket_id`, 1–3 min later, possibly at a different gate | 1.5% of scanned tickets (deterministic at seed 42; exact figure in the asset metadata) | `DUPLICATE_SCAN_RATE`, `DUPLICATE_DELAY_SECONDS` | `stg_ticket_scans` dedupes (first scan wins) and reports `duplicate_scans_dropped` in its materialization metadata |
| **Orphan scans** — scans referencing orders that don't exist | **3**, all in the 2025-07-03 file: `ORD-99991`, `ORD-99992`, `ORD-99993` | `ORPHAN_NIGHT`, `ORPHAN_ORDER_IDS` | `stg_ticket_scans` drops them and reports `orphan_scans_dropped` (= 3) |
| **Refunded orders** — kept in the data with full price/qty | ~7% of orders | `REFUND_RATE` | `stg_orders` zeroes their `net_revenue` and reports `refunded_orders`; `revenue_by_tier` counts `refunded_tickets` |
| **The code-less campaign** — CMP-04 Retarget Blitz spends real money but has no promo code | 1 campaign | empty `promo_code` in the hardcoded campaign roster | `campaign_performance` shows CMP-04 with spend and `attributed_orders = 0` — the "code attribution can't see this channel" lesson |

Deterministic guardrails on the whole dataset (asserted by `tests/test_generator.py`):
3,618 orders at seed 42 (asserted range 3,000–4,000), 5,000–7,000 total scan rows across 8
nights, overall show-up
rate after dedupe/orphan-drop in [0.78, 0.90], and EV-05 at exactly 1,200 tickets.
