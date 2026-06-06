from __future__ import annotations

import json
import subprocess
from pathlib import Path

from scripts.ci import prepare_self_hosted_workspace as prepare


def _git(repo: Path, *args: str) -> str:
    proc = subprocess.run(
        ["git", *args],
        cwd=repo,
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr or proc.stdout
    return proc.stdout.strip()


def test_prepare_workspace_clones_local_source_without_root_worktree_writes(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "source"
    dest = tmp_path / "runner" / "_work" / "AIstock" / "AIstock"
    summary_path = tmp_path / "runner" / "_temp" / "self_hosted_workspace" / "test.json"
    dest.mkdir(parents=True)
    (dest / "stale.txt").write_text("old checkout\n", encoding="utf-8")
    source.mkdir(parents=True)
    _git(source, "init")
    _git(source, "config", "user.email", "ci@example.invalid")
    _git(source, "config", "user.name", "CI")
    _git(source, "remote", "add", "origin", "https://github.com/licong01-cloud/AIstock.git")
    (source / "README.md").write_text("local source\n", encoding="utf-8")
    _git(source, "add", "README.md")
    _git(source, "commit", "-m", "seed")
    commit = _git(source, "rev-parse", "HEAD")

    monkeypatch.setenv("RUNNER_WORKSPACE", str(dest.parent))
    monkeypatch.chdir(dest)
    result = prepare.main(
        [
            "--source",
            str(source),
            "--dest",
            str(dest),
            "--expected-commit",
            commit,
            "--repo",
            "licong01-cloud/AIstock",
            "--summary-json",
            str(summary_path),
        ]
    )

    assert result == 0
    assert not (dest / "stale.txt").exists()
    assert (dest / "README.md").read_text(encoding="utf-8") == "local source\n"
    assert _git(dest, "rev-parse", "HEAD") == commit
    assert _git(dest, "status", "--short") == ""
    assert _git(source, "status", "--short") == ""
    assert not (source / ".git" / "worktrees").exists()

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["workflow_gate"] == "ready"
    assert summary["expected_commit"] == commit
    assert summary["root_worktree_written"] is False
