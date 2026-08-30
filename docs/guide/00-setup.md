# Chapter 0 — Set up and launch

*~5 minutes. [Guide index](README.md) · Next: [Assets and lineage →](01-assets.md)*

By the end of this chapter you'll have the Dagster UI open in your browser with 14 assets
waiting in it. Nothing will have run yet — that's Chapter 1.

## Install `uv` (skip if you have it)

**If `uv --version` prints something, skip to [Bootstrap](#bootstrap).**

[uv](https://docs.astral.sh/uv/) is the only tool you need to install — it fetches Python for
you. (You'll also want `git`, which you almost certainly have.)

macOS / Linux:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Windows (PowerShell):

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

Homebrew, if that's your thing:

```bash
brew install uv
```

> [!TIP]
> **Escape hatch:** if a corporate proxy blocks those installers, `pip install uv` works too —
> uv is just a Python package. More proxy notes in [troubleshooting](../troubleshooting.md).

You do **not** need to install Python separately. The repo commits a `.python-version` file
pinning `3.11`, and uv fetches that interpreter the first time you sync.

## Bootstrap

```bash
git clone https://github.com/lmansf/quickstart-guide-dagster.git
cd quickstart-guide-dagster
make setup
```

Raw equivalent of `make setup` (all platforms):

```bash
uv sync --frozen
```

That created `.venv/`, fetched Python 3.11 if you didn't have it, and installed the **exact**
dependency versions recorded in the committed `uv.lock`. `--frozen` means uv reproduces the
lockfile rather than re-resolving it.

Pinning matters more in a tutorial than almost anywhere else: you and this guide need to be
looking at the same Dagster, the same pandas, the same everything — otherwise you hit "my screen
doesn't match the words" bugs that teach you nothing.

> [!NOTE]
> This and the `uv` install are the **only** steps that need the network. From here on,
> everything — every materialization, every query — runs offline against local files.

## Launch Dagster

```bash
make dev
```

Raw equivalent (macOS/Linux):

```bash
DAGSTER_HOME=$PWD/.dagster_home uv run dagster dev
```

Windows PowerShell:

```powershell
$env:DAGSTER_HOME = "$PWD\.dagster_home"; uv run dagster dev
```

Open **http://localhost:3000**. Leave this terminal running for the rest of the guide — it's the
Dagster webserver *plus* the daemon that powers schedules and sensors in Chapter 4.

(First launch pops a "Join the Dagster community" dialog — **Skip** is fine.)

> [!NOTE]
> **Why the `DAGSTER_HOME` bit?** It points Dagster's instance state — run history, sensor
> cursors, schedule state — at the committed `.dagster_home/` directory, so your history
> survives restarts. The one real file committed there, `dagster.yaml`, does exactly one thing:
> turns telemetry off.

## Check your work

In the left nav, open **Assets** → the **lineage** (graph) view. You should see 14 assets in
six groups — `raw`, `staging`, `marts`, `reporting`, `publishing`, `daily` — wired left to right, exactly
matching [the diagram in the index](README.md#the-example-this-guide-drives).

Click any asset. Every one has a description, and every one says **"Never materialized."**

That's the state Chapter 1 starts from — and that phrase turns out to be the whole idea.

---

*Stuck? [troubleshooting](../troubleshooting.md) covers port 3000 conflicts, Windows without
`make`, and proxy issues.*

**[Next: Assets and lineage →](01-assets.md)**
