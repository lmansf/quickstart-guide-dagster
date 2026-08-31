# Chapter 4 — Jobs, schedules, and sensors

*~8 minutes. [Guide index](README.md) · Prev: [Checks](03-checks.md) · Next: [Partitions →](05-partitions.md)*

So far you've been the one clicking. This chapter is the last core concept: making the pipeline
run without you. All of it lives in `cadence/automation.py` — four short declarations.

## Jobs: a named selection of the graph

A **job** is a saved selection of assets you want to run together.

```python
refresh_all_job = dg.define_asset_job(
    "refresh_all",
    selection=dg.AssetSelection.groups("raw", "staging", "marts", "reporting", "publishing"),
)

admissions_job = dg.define_asset_job(
    "refresh_admissions",
    selection=dg.AssetSelection.assets("raw_ticket_scans").downstream(),
)
```

- **`refresh_all`** — the everyday button you've been using. Groups by name, deliberately
  excluding the partitioned `daily` group so the common case stays simple.
- **`refresh_admissions`** — `raw_ticket_scans` **plus everything downstream of it**. That's the
  same "`+`" idea you typed into the selection box in Chapter 3, expressed in Python: the minimal
  slice that must recompute when new gate scans arrive.

Selections are how you avoid the classic scheduler problem of "rebuild everything because
something might have changed." You describe the affected region of the graph and Dagster works
out the rest.

## Schedules: run on a clock

```python
daily_refresh_schedule = dg.ScheduleDefinition(
    name="daily_refresh",
    job=refresh_all_job,
    cron_schedule="0 6 * * *",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
```

A job plus a cron expression. Try it: **Automation** tab → toggle **`daily_refresh`** on → the
UI shows its next tick → toggle it back off.

It ships **stopped on purpose** — nothing in this repo runs behind your back. That's also a real
convention worth keeping: schedules that arrive enabled surprise people, especially in a repo
someone just cloned.

## Sensors: run on an event

Schedules answer "what time is it?" **Sensors** answer "did something happen?" — a file landed,
a row appeared, an API returned something new. They poll, and when their condition is met they
*request* a run.

The venue's gate hardware exports one CSV per show night into `data/scans/`. Nights 1–7 are
already there. Night 8 — Riverlight Orchestra — is held back in `data/extra/`, as if the night
hasn't happened yet.

```python
@dg.sensor(
    job=admissions_job, minimum_interval_seconds=15, default_status=dg.DefaultSensorStatus.STOPPED
)
def new_scan_file_sensor(context):
    files = sorted(p.name for p in SCANS_DIR.glob("ticket_scans_*.csv"))
    if context.cursor is None:
        context.update_cursor(files[-1] if files else "")
        return dg.SkipReason(f"first evaluation: recorded baseline of {len(files)} existing files")
    new_files = [name for name in files if name > context.cursor]
    if not new_files:
        return dg.SkipReason("no new scan files")
    context.update_cursor(max(new_files))
    return [dg.RunRequest(run_key=name) for name in new_files]
```

Two ideas do the heavy lifting:

**The cursor** is the sensor's memory between ticks — here, the last filename it has seen.
Filenames embed the date, so name-ordering *is* date-ordering, and unlike file mtimes it survives
a fresh `git clone` (which rewrites timestamps).

**The run key** deduplicates. Dagster will not launch two runs with the same `run_key` for the
same sensor, so a file can't be processed twice even if the cursor logic misbehaves. Belt and
braces, and a good habit in anything event-driven.

Note the first-evaluation branch: turning a sensor on records a **baseline and skips**, rather
than firing seven runs for the seven files already sitting there. Watching for what's *new*
should not mean reprocessing the archive.

## Run the demo

1. **Automation** tab → toggle **`new_scan_file_sensor`** on. Its first tick shows **Skipped** —
   *"first evaluation: recorded baseline of 7 existing files"*. That's the branch above.
2. Deliver night 8:

   ```bash
   make new-day
   ```

   Raw equivalent (macOS/Linux):

   ```bash
   cp data/extra/ticket_scans_2025-07-08.csv data/scans/
   ```

   Windows PowerShell:

   ```powershell
   Copy-Item data\extra\ticket_scans_2025-07-08.csv data\scans\
   ```

3. Watch the **Runs** page. **Within ~15 seconds** — the polling interval, so up to that long,
   not instantly — a `refresh_admissions` run appears, requested by the sensor, with the new
   file's name as its run key.

When it finishes, open `attendance_by_event`: **eight rows now instead of seven**, and the
`overall_show_up_rate` metadata point moves. Because Dagster plots numeric metadata across
materializations, you can watch the needle move run over run.

Night 8's scans arrived and nobody clicked anything.

### Keep going

`make new-day` doesn't stop at night 8. Once the held-back show is delivered, it **synthesizes a
brand-new one** — night 9, 10, 11 … each with its own event, orders, and gate scans, seeded so
the same night number always produces the same data. Run it again and the sensor fires again:

```bash
make new-day      # → "Added night 9: Harbour Lights (indie) on 2025-07-09 — 340 orders, 586 scans"
```

Synthesized shows land in `data/nights/<date>/` (gitignored) and the raw assets read them
alongside the committed CSVs, so `data/raw/*.csv` stays byte-identical to the seed-42 generator —
the guarantee [Chapter 7](07-testing.md) tests. `make reset` clears them and puts the season back
to nights 1–7.

> [!NOTE]
> **Replaying the sensor demo** takes more than `make reset`, because the sensor's cursor and its
> run keys live in `.dagster_home/`. The full recipe is in
> [troubleshooting](../troubleshooting.md#what-make-reset-does-and-doesnt-do). Adding a *new*
> night needs none of that — a filename it has never seen always trips it.

## What you learned

- A **job** is a named selection of the graph — including "this asset and everything downstream"
- **Schedules** fire on a clock; **sensors** fire on events, using a **cursor** to remember and a
  **run key** to deduplicate
- Both ship **stopped**, because surprising people is a bad default
- Sensible sensors baseline on first evaluation instead of replaying history

---

That's the core. The remaining chapters are independent add-ons — take them in any order:
**[Partitions →](05-partitions.md)** · **[Publishing →](06-publishing.md)** ·
**[Testing →](07-testing.md)**
