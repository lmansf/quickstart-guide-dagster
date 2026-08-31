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

The repo ships a `vercel.json` wired for exactly this: `outputDirectory` points at
`reports/boxoffice`, and install/build are stubbed because there is nothing to build. A
`.vercelignore` hides the Python project, so the host doesn't mistake a data pipeline for a web
service and go looking for a server entrypoint that was never meant to exist — a small lesson in
its own right about publishing artifacts out of a pipeline repo.

## Keeping the deployed report live

Here's the part that trips people up. A git-backed host builds from **the repository**, not from
your machine. Your warehouse (`data/warehouse/cadence.duckdb`) is a local file and gitignored —
the host can't see it, and a "deploy hook" won't help, because it would just rebuild the data
that's already committed.

So publishing means getting the refreshed `data.json` / `data.js` into git. That's what
`published_report` (`cadence/assets/publish.py`) does — the last asset in the graph:

```python
@dg.asset(group_name="publishing", deps=[boxoffice_dashboard_data])
def published_report(context) -> None: ...
```

It stages **only** the two data files, commits them if they actually changed, and pushes. The
host sees the push and redeploys.

### Turning it on

Publishing is controlled by three environment variables:

| Variable | Default | Purpose |
|---|---|---|
| `PUBLISH_REPORT` | *(unset — skips)* | `1` / `true` / `yes` / `on` enables publishing |
| `PUBLISH_BRANCH` | current branch | Branch to push to (use the one your host deploys) |
| `PUBLISH_REMOTE` | `origin` | Git remote to push to |

**Where to put them: a `.env` file in the project root.** Dagster's CLI loads it automatically,
so the UI, the daemon, and every schedule and sensor run see it — no shell setup, nothing to
remember:

```bash
cp .env.example .env      # then edit PUBLISH_BRANCH to the branch your host deploys
```

```ini
# .env
PUBLISH_REPORT=1
PUBLISH_BRANCH=main
PUBLISH_REMOTE=origin
```

Restart `dagster dev` afterwards — the file is read at startup. `.env` is gitignored: it's your
machine's config, not the repo's.

> [!NOTE]
> **One catch worth knowing.** `.env` is a feature of the Dagster **CLI** (`dagster dev`,
> `dagster job execute`). A bare `python` script that calls `dg.materialize()` directly does not
> read it, and will skip publishing. Verified both ways — if publishing mysteriously no-ops,
> check how the process was launched.

Two other ways, for when `.env` doesn't fit:

```bash
make publish              # one-off: a refresh with publishing enabled, no .env needed
```

```ini
# /etc/systemd/system/dagster.service — for an always-on daemon
[Service]
WorkingDirectory=/home/you/quickstart-guide-dagster
Environment=PUBLISH_REPORT=1
Environment=PUBLISH_BRANCH=main
ExecStart=/home/you/.local/bin/uv run dagster dev
```

### Checking it worked

Open `published_report` in the UI and read the latest materialization's metadata. It tells you
exactly what happened, in all three cases:

| `published` | `reason` / `commit` | Meaning |
|---|---|---|
| `false` | `PUBLISH_REPORT is not set` | Publishing is off — the variable isn't reaching the process |
| `false` | `data unchanged since last publish` | Working as intended; nothing to deploy |
| `true` | a short SHA + branch | Pushed — your host is redeploying now |

Four design choices in there are worth stealing for any "pipeline publishes to git" job:

- **Opt-in.** Unset, the asset records a skip and touches nothing — so a reader following this
  guide never has their repo committed to behind their back.
- **Pathspec-limited.** The commit names the two data files explicitly, so unrelated work in
  your tree — even *staged* work — is never swept into it.
- **No-op when nothing changed.** This is the [determinism](#why-the-export-is-deterministic)
  payoff: identical data produces identical bytes, so a scheduled run that changes nothing makes
  no commit and triggers no deploy. Without determinism you'd get a junk commit every tick.
- **Loud on failure.** A push that can't authenticate fails the run with git's own stderr, rather
  than silently leaving the site stale.

Running it on a schedule (a Dagster daemon on your own box, say) needs two things: a checkout
whose remote accepts **non-interactive** pushes — an SSH key without a passphrase, or a token in
the remote URL — and a branch that doesn't diverge from the remote, since the asset pushes rather
than reconciling. A dedicated publishing checkout is the tidiest way to guarantee both.

> [!NOTE]
> If you'd rather not have a pipeline commit to your repository at all, the alternative is to
> upload the payload to object storage and have the page `fetch()` it at load. That trades a
> redeploy for a runtime request — and means the numbers can change without a git history of
> what changed when, which is a real loss for a report people make decisions from.

## What you learned

- Assets can produce **external artifacts**, not just tables — they return `None` and still get
  lineage, materialization history, and metadata
- Separating **data export from presentation** means the pipeline owns numbers and the HTML owns
  layout
- **Deterministic exports** keep committed artifacts honest and diffs meaningful — derive
  provenance from data, not the clock

---

**[Next: Testing a pipeline →](07-testing.md)**
