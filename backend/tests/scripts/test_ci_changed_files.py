from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
import yaml

from scripts.ci_changed_files import ChangedFilesError, build_changed_files


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _write(repo: Path, relative: str, content: str) -> None:
    path = repo / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _commit(repo: Path, message: str) -> str:
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", message)
    return _git(repo, "rev-parse", "HEAD")


def _repo_with_stale_pr_base(tmp_path: Path) -> tuple[Path, str, str, str]:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-b", "main")
    _git(repo, "config", "user.email", "ci@example.invalid")
    _git(repo, "config", "user.name", "CI Test")
    _write(repo, "README.md", "base\n")
    old_base = _commit(repo, "base")

    _git(repo, "checkout", "-b", "feature")
    _write(repo, "docs/hmm.md", "feature\n")
    _write(repo, "tests/aistock_validation/bugs/BUG.json", "{}\n")
    feature_head = _commit(repo, "feature")

    _git(repo, "checkout", "main")
    _write(repo, "backend/services/quantevolver/unrelated.py", "UNRELATED = True\n")
    current_base = _commit(repo, "main advances")
    return repo, old_base, current_base, feature_head


def test_pull_request_uses_current_base_ref_when_event_base_sha_is_stale(tmp_path: Path) -> None:
    repo, old_base, current_base, feature_head = _repo_with_stale_pr_base(tmp_path)

    changed, receipt = build_changed_files(
        repo_root=repo,
        base_ref="main",
        base_sha=old_base,
        head_sha=feature_head,
    )

    assert changed == ["docs/hmm.md", "tests/aistock_validation/bugs/BUG.json"]
    assert "backend/services/quantevolver/unrelated.py" not in changed
    assert receipt["base_source"] == "current_base_ref"
    assert receipt["base_commit"] == current_base
    assert receipt["event_base_sha"] == old_base


def test_push_uses_event_before_sha_without_a_base_ref(tmp_path: Path) -> None:
    repo, _, current_base, _ = _repo_with_stale_pr_base(tmp_path)
    _write(repo, "backend/main.py", "NEXT = True\n")
    pushed_head = _commit(repo, "push")

    changed, receipt = build_changed_files(
        repo_root=repo,
        base_sha=current_base,
        head_sha=pushed_head,
        diff_filter="ACMRT",
    )

    assert changed == ["backend/main.py"]
    assert receipt["base_source"] == "event_base_sha"


def test_explicit_pull_request_base_ref_fails_closed_when_missing(tmp_path: Path) -> None:
    repo, old_base, _, feature_head = _repo_with_stale_pr_base(tmp_path)

    with pytest.raises(ChangedFilesError, match="current base ref cannot be resolved"):
        build_changed_files(
            repo_root=repo,
            base_ref="missing-base",
            base_sha=old_base,
            head_sha=feature_head,
        )


def test_pull_request_workflows_use_shared_current_base_resolver() -> None:
    workflow_paths = (
        Path(".github/workflows/test.yml"),
        Path(".github/workflows/codeql.yml"),
        Path(".github/workflows/semgrep.yml"),
        Path(".github/workflows/dependency-update-validate.yml"),
    )
    for path in workflow_paths:
        source = path.read_text(encoding="utf-8")
        yaml.safe_load(source)
        assert "github.event.pull_request.base.sha" not in source
        assert "github.event.pull_request.base.ref" in source
        assert "scripts/ci_changed_files.py" in source
