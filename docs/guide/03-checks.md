# Chapter 3 — Checks and data quality

*~10 minutes. [Guide index](README.md) · Prev: [IO managers](02-io-managers.md) · Next: [Automation →](04-automation.md)*

This is the chapter the whole guide is built around. Your run **succeeded** and there's still a
red mark on `campaign_performance` — and fixing it will change which campaign the venue should
fund next season.

## What an asset check is

A check is an assertion attached to an **asset**, not to a task. It runs when that asset
materializes, and its result lives on the asset in the UI — so "is this table trustworthy?" is
answered in the same place as "when was it last built?"

This project ships four, in `cadence/checks.py`:

| Check | On | Severity | Blocking? |
|---|---|---|---|
| `order_amounts_valid` | `stg_orders` | ERROR | **yes** — downstream won't run |
| `orders_reference_known_events` | `stg_orders` | ERROR | no |
| `all_promo_orders_attributed` | `campaign_performance` | ERROR | no ← **the red one** |
| `show_up_rate_in_bounds` | `attendance_by_event` | WARN | no |

Severity in one breath: **WARN** is advisory. **ERROR** is a real problem, loudly annotated, but
the run completes and downstream assets still build. **Blocking** means downstream assets don't
run at all — bad data stops here rather than propagating.

That distinction matters more than it looks. Most quality problems shouldn't halt a pipeline;
you want the numbers *and* the warning. A few — corrupt keys, duplicate IDs — should stop
everything, because computing on them produces confident, wrong answers.

## Read the failure

Click the red check: **`all_promo_orders_attributed`**. Its description says *"EXPECTED TO FAIL
on first run."* Read its metadata like the analyst you now are:

- `unattributed_orders`: **150** — promo-coded orders that matched no campaign
- `unattributed_revenue`: **$13,703.00** — money marketing spent real dollars to earn, credited
  to nobody
- `sample_bad_codes`: strings like `' SUMMER25'`, `'summer25'`, `'VIPNIGHT '`
- `hint`: *"Open `cadence/assets/staging.py` and find the TODO…"*

There's the crime scene. Marketing typed promo codes by hand — leading spaces, trailing spaces,
lowercase, Title Case — and `campaign_performance` joins on the code **exactly as typed**. 150
orders fall through the join into that `(unattributed)` row you saw in Chapter 2.

Good check metadata is the difference between an alert and a diagnosis. This one tells you how
many rows, how much money, what the bad values look like, and where to go next.

## Follow the lineage to the fix

Here's the part worth internalizing.

The check lives on **`campaign_performance`** — that's where the business question is answered,
so that's where quality gets asserted. But the graph shows that mart is built from
`stg_orders`. Fixing it in the mart would patch one consumer; fixing it in **staging** fixes
every consumer, forever.

**The check tells you what's broken. The lineage tells you where to fix it.**

Open `cadence/assets/staging.py`, find the loud TODO block in `stg_orders`, and add the one line
it asks for:

```python
df["promo_code"] = normalize_promo_codes(df["promo_code"])
```

(`normalize_promo_codes` is defined at the top of the same file — it's just
`.str.strip().str.upper()`, NA-preserving. Writing that inline works too.)

## See the blast radius, then re-run

Before re-running, check what your one line touches. In the lineage view's selection box:

```
key:"stg_orders"+
```

`stg_orders` plus the six assets downstream of it — everything your fix will change.

Now re-run: **Jobs** → **`refresh_all`** → **Launch run** (the same button as Chapter 1).

Two things flip:

1. **The check goes green.** All 150 orders find their campaign.
2. **The answer changes.** Re-run the Chapter 2 query:

```bash
make query Q="SELECT * FROM campaign_performance ORDER BY attributed_revenue DESC"
```

**Summer Kickoff jumps to #1 among campaigns** (the `(organic)` no-code row still tops the raw
table) — it was the most undercounted, since ~90 of the dirty codes were its `SUMMER25`. And
tiny **VIP Love Letter** — $120 of spend, ~30 recovered `VIPNIGHT` orders — becomes the **best
revenue-per-dollar** campaign on the board.

Before the fix, the numbers said Radio Week and Student Rush were your winners. They weren't.

You didn't just silence an alert. You changed which campaign gets next season's budget. That's
the guide in one sentence: **orchestration, lineage, and data quality are one subject.**

> [!TIP]
> **Break it on purpose (optional, 2 min).** Open `data/raw/orders.csv`, copy any order row and
> paste it as a duplicate line (same `order_id` twice), then materialize. The **blocking**
> `order_amounts_valid` check fails on the duplicate ID and **halts the downstream graph** — bad
> data stops at staging instead of poisoning the report.
>
> Why not just set a `qty` to `-1`? Try it: nothing turns red, because `stg_orders` *drops*
> non-positive rows before the check ever sees them. Defense in depth — the check guards what
> cleaning can't silently repair.
>
> Undo with `git restore data/raw/orders.csv`.

> [!NOTE]
> **Keep your fix.** The rest of the guide works either way. If you'd rather return to the
> shipped bug, `git restore cadence/assets/staging.py`. And if you keep the fix, the test that
> documents the bug ([Chapter 7](07-testing.md)) notices and *skips* rather than failing.

## What you learned

- Checks attach to **assets**, and their results live where the data's freshness lives
- **WARN / ERROR / blocking** are three different answers to "how bad is this?"
- Metadata turns an alert into a diagnosis
- The check says *what*; the **lineage says where** — fix upstream, and every consumer inherits it

---

**[Next: Jobs, schedules, and sensors →](04-automation.md)** — stop being the one who clicks.
