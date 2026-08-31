"""Publish the exported report: commit the data files and push, so the host redeploys.

Vercel (and every other git-backed static host) builds from the repository, not from
your machine — so "make the deployed report live" means getting the refreshed
``data.json`` / ``data.js`` into git. This asset does exactly that, and nothing else:
it stages those two paths, commits them if they actually changed, and pushes.

It is **opt-in**: without ``PUBLISH_REPORT=1`` in the environment it records a skip and
returns. A guide reader who materializes the graph never has their repo touched.
"""

import json
import os
import subprocess

import dagster as dg

from cadence.assets.dashboard import boxoffice_dashboard_data
from cadence.resources import PROJECT_ROOT

PUBLISH_ENV = "PUBLISH_REPORT"
REMOTE_ENV = "PUBLISH_REMOTE"
BRANCH_ENV = "PUBLISH_BRANCH"

# The only paths this asset is ever allowed to commit.
TRACKED_PATHS = ("reports/boxoffice/data.json", "reports/boxoffice/data.js")
TRUTHY = {"1", "true", "yes", "on"}


def _git(*args: str) -> subprocess.CompletedProcess:
    """Run git in the project root and return the completed process (never raises)."""
    return subprocess.run(
        ["git", *args],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def _require(result: subprocess.CompletedProcess, what: str) -> str:
    if result.returncode != 0:
        raise dg.Failure(
            description=f"git {what} failed",
            metadata={
                "command": dg.MetadataValue.text(" ".join(result.args)),
                "stderr": dg.MetadataValue.text(result.stderr.strip() or "(no stderr)"),
                "hint": dg.MetadataValue.text(
                    "The publishing checkout needs non-interactive push access "
                    "(an SSH key without a passphrase, or a token in the remote URL) "
                    "and must not have diverged from the remote."
                ),
            },
        )
    return result.stdout.strip()


def _headline() -> str:
    """A human-readable summary of what changed, for the commit message."""
    payload_path = PROJECT_ROOT / "reports" / "boxoffice" / "data.json"
    try:
        payload = json.loads(payload_path.read_text(encoding="utf-8"))
        net = payload["totals"]["net"]
        nights = payload["season"]["nights_scanned"]
        return f"${net:,.0f} net, {nights} nights of scans"
    except (OSError, KeyError, ValueError):
        return "refreshed data"


@dg.asset(
    group_name="publishing",
    deps=[boxoffice_dashboard_data],
    description=(
        "Commit and push the exported report data so the static host redeploys it. "
        "Opt in with PUBLISH_REPORT=1."
    ),
)
def published_report(context: dg.AssetExecutionContext) -> None:
    if os.environ.get(PUBLISH_ENV, "").strip().lower() not in TRUTHY:
        context.log.info(
            "Skipping publish: set %s=1 to commit and push the refreshed report.", PUBLISH_ENV
        )
        context.add_output_metadata(
            {
                "published": dg.MetadataValue.bool(False),
                "reason": dg.MetadataValue.text(f"{PUBLISH_ENV} is not set"),
            }
        )
        return

    _require(_git("rev-parse", "--git-dir"), "rev-parse (is this a git checkout?)")

    branch = os.environ.get(BRANCH_ENV, "").strip() or _require(
        _git("rev-parse", "--abbrev-ref", "HEAD"), "rev-parse HEAD"
    )
    if branch == "HEAD":
        raise dg.Failure(
            description="Detached HEAD: nothing to push to",
            metadata={"hint": dg.MetadataValue.text(f"Set {BRANCH_ENV} to a branch name.")},
        )
    remote = os.environ.get(REMOTE_ENV, "").strip() or "origin"

    _require(_git("add", "--", *TRACKED_PATHS), "add")

    # Exit code 1 from `diff --cached --quiet` means there ARE staged changes.
    if _git("diff", "--cached", "--quiet", "--", *TRACKED_PATHS).returncode == 0:
        context.log.info("Report data is unchanged — nothing to publish.")
        context.add_output_metadata(
            {
                "published": dg.MetadataValue.bool(False),
                "reason": dg.MetadataValue.text("data unchanged since last publish"),
                "branch": dg.MetadataValue.text(branch),
            }
        )
        return

    # Commit ONLY the report data, whatever else may be staged in the working tree.
    _require(
        _git(
            "commit",
            "-m",
            f"chore(report): refresh box office data — {_headline()}",
            "--",
            *TRACKED_PATHS,
        ),
        "commit",
    )
    sha = _require(_git("rev-parse", "--short", "HEAD"), "rev-parse --short HEAD")
    _require(_git("push", remote, f"HEAD:{branch}"), f"push to {remote}/{branch}")

    context.log.info("Published %s to %s/%s — the host will redeploy.", sha, remote, branch)
    context.add_output_metadata(
        {
            "published": dg.MetadataValue.bool(True),
            "commit": dg.MetadataValue.text(sha),
            "branch": dg.MetadataValue.text(f"{remote}/{branch}"),
            "files": dg.MetadataValue.md("\n".join(f"- `{p}`" for p in TRACKED_PATHS)),
            "summary": dg.MetadataValue.text(_headline()),
        }
    )
