"""The publishing asset: opt-in, no-op when nothing changed, and never commits
anything except the two report data files."""

import subprocess

import dagster as dg
import pytest

from cadence.assets import publish
from cadence.assets.publish import TRACKED_PATHS, published_report


def _materialize() -> dict:
    result = dg.materialize([published_report], raise_on_error=True)
    events = result.get_asset_materialization_events()
    return events[0].materialization.metadata


def test_skips_unless_opted_in(monkeypatch):
    """A guide reader materializing the graph must never have their repo touched."""
    monkeypatch.delenv(publish.PUBLISH_ENV, raising=False)
    calls = []
    monkeypatch.setattr(publish, "_git", lambda *a: calls.append(a))

    metadata = _materialize()
    assert metadata["published"].value is False
    assert calls == [], "publish must not shell out to git when opted out"


@pytest.mark.parametrize("value", ["1", "true", "YES", "on"])
def test_truthy_values_opt_in(value):
    assert value.strip().lower() in publish.TRUTHY


def test_only_the_report_data_is_ever_committed():
    """The commit is pathspec-limited, so unrelated (even staged) work stays local."""
    assert TRACKED_PATHS == (
        "reports/boxoffice/data.json",
        "reports/boxoffice/data.js",
    )


def _run(*args, cwd):
    return subprocess.run(args, cwd=cwd, capture_output=True, text=True, check=True)


def test_publishes_only_changed_data_to_a_real_remote(tmp_path, monkeypatch):
    """End-to-end against a throwaway bare remote: first publish pushes a commit
    touching only the data files; a second publish with unchanged data is a no-op."""
    remote = tmp_path / "remote.git"
    work = tmp_path / "work"
    _run("git", "init", "--quiet", "--bare", str(remote), cwd=tmp_path)
    work.mkdir()
    _run("git", "init", "--quiet", "-b", "main", cwd=work)
    _run("git", "config", "user.email", "t@example.com", cwd=work)
    _run("git", "config", "user.name", "Tester", cwd=work)
    _run("git", "remote", "add", "origin", str(remote), cwd=work)

    data_dir = work / "reports" / "boxoffice"
    data_dir.mkdir(parents=True)
    for rel in TRACKED_PATHS:
        (work / rel).write_text("initial\n", encoding="utf-8")
    (work / "unrelated.txt").write_text("local only\n", encoding="utf-8")
    _run("git", "add", "-A", cwd=work)
    _run("git", "commit", "--quiet", "-m", "init", cwd=work)
    _run("git", "push", "--quiet", "origin", "HEAD:main", cwd=work)

    monkeypatch.setattr(publish, "PROJECT_ROOT", work)
    monkeypatch.setenv(publish.PUBLISH_ENV, "1")
    monkeypatch.setenv(publish.BRANCH_ENV, "main")

    # nothing changed yet -> no commit
    assert _materialize()["published"].value is False

    # change the data and dirty an unrelated file
    (work / TRACKED_PATHS[0]).write_text('{"totals": {"net": 1.0}}\n', encoding="utf-8")
    (work / "unrelated.txt").write_text("EDITED LOCALLY\n", encoding="utf-8")
    _run("git", "add", "unrelated.txt", cwd=work)

    metadata = _materialize()
    assert metadata["published"].value is True

    pushed = _run("git", "show", "--stat", "--format=", "origin/main", cwd=work).stdout
    assert "data.json" in pushed
    assert "unrelated.txt" not in pushed, "unrelated staged work must never be published"

    # publishing again with unchanged data is a no-op
    assert _materialize()["published"].value is False
