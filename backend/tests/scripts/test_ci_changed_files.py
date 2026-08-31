from __future__ import annotations

import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

import scripts.ci_changed_files as changed_files_module
from scripts.ci_changed_files import ChangedFilesError, build_changed_files, prepare_pr_merge_base


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


def test_prepare_pr_merge_base_avoids_fetch_when_ancestry_is_already_proven(tmp_path: Path) -> None:
    repo, old_base, _, feature_head = _repo_with_stale_pr_base(tmp_path)
    _git(repo, "checkout", "feature")

    receipt = prepare_pr_merge_base(
        repo_root=repo,
        base_ref="main",
        base_sha=old_base,
        checkout_ref="refs/heads/feature",
    )

    assert receipt["head_commit"] == feature_head
    assert receipt["merge_base"] == old_base
    assert receipt["fetch_used"] is False
    assert _git(repo, "rev-parse", "refs/remotes/origin/main") == old_base


def test_prepare_pr_merge_base_fetches_only_exact_refs_with_bounded_deepening(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fetched = False
    commands: list[list[str]] = []
    base_sha = "a" * 40
    head_sha = "b" * 40

    def fake_commit(repo_root: Path, revision: str, field: str) -> str:
        if revision == "HEAD":
            return head_sha
        if revision == base_sha and fetched:
            return base_sha
        raise ChangedFilesError(f"{field} unavailable")

    def fake_git(repo_root: Path, *args: str) -> str:
        if args[0] == "check-ref-format" or args[0] == "update-ref":
            return ""
        if args[0] == "merge-base" and fetched:
            return base_sha
        raise ChangedFilesError("merge base unavailable")

    def fake_run(args: list[str], **kwargs: object) -> SimpleNamespace:
        nonlocal fetched
        commands.append(args)
        fetched = True
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(changed_files_module, "_commit", fake_commit)
    monkeypatch.setattr(changed_files_module, "_git", fake_git)
    monkeypatch.setattr(changed_files_module.subprocess, "run", fake_run)

    receipt = prepare_pr_merge_base(
        repo_root=tmp_path,
        base_ref="main",
        base_sha=base_sha,
        checkout_ref="refs/pull/3884/merge",
    )

    assert receipt["fetch_used"] is True
    assert receipt["fetch_attempts"] == 1
    assert commands == [[
        "git",
        "fetch",
        "--no-tags",
        "--no-write-fetch-head",
        "--deepen=64",
        "origin",
        "+refs/heads/main:refs/remotes/origin/main",
        "+refs/pull/3884/merge:refs/remotes/origin/aistock-pr-checkout",
    ]]


def test_pull_request_workflows_use_shared_current_base_resolver() -> None:
    workflow_paths = (
        Path(".github/workflows/test.yml"),
        Path(".github/workflows/codeql.yml"),
        Path(".github/workflows/semgrep.yml"),
        Path(".github/workflows/dependency-update-validate.yml"),
        Path(".github/workflows/pr-quality.yml"),
    )
    for path in workflow_paths:
        source = path.read_text(encoding="utf-8")
        yaml.safe_load(source)
        assert "--prepare-pr-merge-base-only" in source
        assert "github.event.pull_request.base.sha" in source
        assert "github.event.pull_request.base.ref" in source or "github.base_ref" in source
        assert "scripts/ci_changed_files.py" in source
