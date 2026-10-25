from __future__ import annotations

import subprocess
import json
from pathlib import Path

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


def test_pull_request_uses_pinned_source_diff_when_main_advances(tmp_path: Path) -> None:
    repo, old_base, current_base, feature_head = _repo_with_stale_pr_base(tmp_path)

    changed, receipt = build_changed_files(
        repo_root=repo,
        base_ref="main",
        base_sha=old_base,
        head_sha=feature_head,
    )

    assert changed == ["docs/hmm.md", "tests/aistock_validation/bugs/BUG.json"]
    assert "backend/services/quantevolver/unrelated.py" not in changed
    assert receipt["base_source"] == "pinned_pr_base_sha"
    assert receipt["base_commit"] == old_base
    assert receipt["event_base_sha"] == old_base
    assert _git(repo, "rev-parse", "main") == current_base


def test_synthetic_merge_checkout_does_not_attribute_later_main_files(tmp_path: Path) -> None:
    repo, old_base, current_base, feature_head = _repo_with_stale_pr_base(tmp_path)
    _git(repo, "merge", "--no-ff", "feature", "-m", "synthetic PR merge")
    integration_head = _git(repo, "rev-parse", "HEAD")

    prepared = prepare_pr_merge_base(
        repo_root=repo, base_ref="main", base_sha=old_base,
        checkout_ref="refs/heads/main", source_head_sha=feature_head,
    )
    changed, receipt = build_changed_files(
        repo_root=repo, base_ref="main", base_sha=old_base, head_sha=feature_head,
    )
    contaminated, _ = build_changed_files(repo_root=repo, base_sha=old_base, head_sha=integration_head)

    assert changed == ["docs/hmm.md", "tests/aistock_validation/bugs/BUG.json"]
    assert "backend/services/quantevolver/unrelated.py" in contaminated
    assert (repo / "backend/services/quantevolver/unrelated.py").is_file()
    assert prepared["head_commit"] == integration_head
    assert prepared["source_head_commit"] == receipt["head_commit"] == feature_head
    assert prepared["base_commit"] == receipt["base_commit"] == old_base
    assert prepared["merge_base"] == old_base
    # A later ref movement cannot change a commit-bound scope result.
    _git(repo, "update-ref", "refs/remotes/origin/main", current_base)
    assert build_changed_files(
        repo_root=repo, base_ref="main", base_sha=old_base, head_sha=feature_head,
    )[0] == changed


@pytest.mark.parametrize("source", ["main", "f" * 40])
def test_pr_preparation_rejects_invalid_or_unavailable_source(tmp_path: Path, source: str) -> None:
    repo, old_base, _, _ = _repo_with_stale_pr_base(tmp_path)
    with pytest.raises(ChangedFilesError):
        prepare_pr_merge_base(
            repo_root=repo, base_ref="main", base_sha=old_base,
            checkout_ref="refs/heads/main", source_head_sha=source, attempts=1,
        )


def test_pr_preparation_rejects_source_not_contained_in_checkout(tmp_path: Path) -> None:
    repo, old_base, _, feature_head = _repo_with_stale_pr_base(tmp_path)
    with pytest.raises(ChangedFilesError, match="--is-ancestor"):
        prepare_pr_merge_base(
            repo_root=repo, base_ref="main", base_sha=old_base,
            checkout_ref="refs/heads/main", source_head_sha=feature_head,
        )


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
            base_sha="",
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
        if revision in {"HEAD", head_sha}:
            return head_sha
        if revision == base_sha and fetched:
            return base_sha
        raise ChangedFilesError(f"{field} unavailable")

    def fake_git(repo_root: Path, *args: str, **kwargs: object) -> str:
        nonlocal fetched
        if args[0] == "fetch":
            commands.append(["git", *args])
            assert kwargs["timeout"] <= 45
            fetched = True
            return ""
        if args[0] == "check-ref-format" or args[0] == "update-ref":
            return ""
        if args[0] == "merge-base" and fetched:
            return base_sha
        raise ChangedFilesError("merge base unavailable")

    monkeypatch.setattr(changed_files_module, "_commit", fake_commit)
    monkeypatch.setattr(changed_files_module, "_git", fake_git)
    monkeypatch.setattr(changed_files_module, "_restore_local_mirror_ancestry", lambda *args: {"status": "not_required"})

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
        f"+{base_sha}:refs/remotes/origin/main",
        "+refs/pull/3884/merge:refs/remotes/origin/aistock-pr-checkout",
    ]]


@pytest.mark.parametrize("extra_commits", [0, 3])
def test_shallow_pr_restores_main_ancestry_from_verified_local_mirror(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, extra_commits: int) -> None:
    source, old_base, base, feature = _repo_with_stale_pr_base(tmp_path)
    mirror = tmp_path / "mirror.git"
    _git(tmp_path, "clone", "--bare", "--single-branch", "--branch", "main", "--no-local", source.as_uri(), str(mirror))
    _git(mirror, "config", "aistock.repository", "licong01-cloud/AIstock")
    Path(str(mirror) + ".aistock-mirror.json").write_text(json.dumps({
        "schema_version": "aistock_git_object_mirror_v1", "repository": "licong01-cloud/AIstock",
        "main_sha": base, "mirror_root": str(mirror), "object_directory": str(mirror / "objects")}), encoding="utf-8")
    _git(source, "checkout", "feature")
    for index in range(extra_commits):
        _write(source, "docs/hmm.md", f"change {index}\n")
        feature = _commit(source, "feature advances")
    _git(source, "checkout", "main")
    _git(source, "merge", "--no-ff", "feature", "-m", "integration")
    workspace = tmp_path / "workspace"
    _git(tmp_path, "clone", "--depth=2", source.as_uri(), str(workspace))
    monkeypatch.setenv("GIT_ALTERNATE_OBJECT_DIRECTORIES", str(mirror / "objects"))
    restored = changed_files_module._restore_local_mirror_ancestry(workspace, base)
    assert restored["status"] == "restored", restored
    receipt = prepare_pr_merge_base(repo_root=workspace, base_ref="main", base_sha=base,
        checkout_ref="refs/heads/main", source_head_sha=feature)
    assert receipt["fetch_used"] == bool(extra_commits)
    files, _ = build_changed_files(repo_root=workspace, base_ref="main", base_sha=base, head_sha=feature)
    assert files == ["docs/hmm.md", "tests/aistock_validation/bugs/BUG.json"]


def test_git_timeout_and_fetch_budget_remain_fail_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def timeout(args: list[str], **kwargs: object) -> None:
        assert kwargs["timeout"] == 0.01
        raise subprocess.TimeoutExpired(args, 0.01)
    monkeypatch.setattr(changed_files_module.subprocess, "run", timeout)
    with pytest.raises(ChangedFilesError, match="timed out"):
        changed_files_module._git(tmp_path, "rev-parse", timeout=0.01)


@pytest.mark.parametrize("exhausted", [False, True])
def test_failed_history_fetch_is_capped_without_weakening_ancestry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, exhausted: bool) -> None:
    calls: list[object] = []
    def commit(root: Path, revision: str, field: str) -> str:
        if revision == "HEAD":
            return "b" * 40
        raise ChangedFilesError("missing base")
    def git(root: Path, *args: str, **kwargs: object) -> str:
        if args[0] == "fetch":
            calls.append(kwargs["timeout"])
            raise ChangedFilesError("fetch timed out")
        return ""
    monkeypatch.setattr(changed_files_module, "_commit", commit)
    monkeypatch.setattr(changed_files_module, "_git", git)
    monkeypatch.setattr(changed_files_module, "_restore_local_mirror_ancestry", lambda *args: {"status": "not_required"})
    monkeypatch.setattr(changed_files_module.time, "sleep", lambda seconds: None)
    with pytest.raises(ChangedFilesError, match="history preparation failed"):
        prepare_pr_merge_base(repo_root=tmp_path, base_ref="main", base_sha="a" * 40,
            checkout_ref="refs/heads/feature", attempts=99, total_budget=0 if exhausted else 150)
    assert len(calls) == (0 if exhausted else 3)
    assert all(0 < timeout <= 45 for timeout in calls)


def test_pull_request_workflows_use_shared_current_base_resolver() -> None:
    workflow_paths = (
        Path(".github/workflows/test.yml"),
        Path(".github/workflows/semgrep.yml"),
        Path(".github/workflows/dependency-update-validate.yml"),
        Path(".github/workflows/pr-quality.yml"),
    )
    for path in workflow_paths:
        source = path.read_text(encoding="utf-8")
        from scripts.ci_workflow_policy_scan import has_event_bound_base_preparation
        document = yaml.safe_load(source)
        manual = path.name != "test.yml"
        assert has_event_bound_base_preparation(source, manual=manual)
        if manual:
            assert "base_sha" in document.get("on", document.get(True))["workflow_dispatch"]["inputs"]


@pytest.mark.parametrize("pinned", [False, True])
def test_manual_event_compares_complete_branch_and_pins_identities(tmp_path: Path, pinned: bool) -> None:
    repo, old_base, current_base, feature_head = _repo_with_stale_pr_base(tmp_path)
    _git(repo, "remote", "add", "origin", str(repo))
    _git(repo, "checkout", "feature")
    prepared = prepare_pr_merge_base(
        repo_root=repo, base_ref="main", base_sha=old_base if pinned else "",
        checkout_ref="refs/heads/feature", source_head_sha=feature_head,
        resolve_current_base=True,
    )
    assert prepared["base_commit"] == (old_base if pinned else current_base)
    assert prepared["event_mode"] == "workflow_dispatch"
    assert prepared["fetch_used"] is not pinned
    changed, _ = build_changed_files(
        repo_root=repo, base_ref="main", base_sha=str(prepared["base_commit"]), head_sha=feature_head,
    )
    assert changed == ["docs/hmm.md", "tests/aistock_validation/bugs/BUG.json"]


@pytest.mark.parametrize("kwargs", [
    {"base_sha": "", "source_head_sha": ""},
    {"base_sha": "main", "source_head_sha": ""},
])
def test_pr_event_still_rejects_missing_or_symbolic_base(tmp_path: Path, kwargs: dict) -> None:
    repo, _, _, _ = _repo_with_stale_pr_base(tmp_path)
    with pytest.raises(ChangedFilesError, match="base_sha must be"):
        prepare_pr_merge_base(repo_root=repo, base_ref="main", checkout_ref="refs/heads/main", **kwargs)


def test_main_ci_uses_one_pinned_source_diff_for_all_scope_consumers() -> None:
    steps = yaml.safe_load(Path(".github/workflows/test.yml").read_text(encoding="utf-8"))["jobs"]["ci-verdict"]["steps"]
    by_name = {step.get("name"): step for step in steps}
    prepare = by_name["Fetch current PR base ref only"]
    changed = by_name["Build changed-file list"]
    quality = by_name["Run PR quality enforcement"]
    assert prepare["env"]["SOURCE_HEAD_SHA"] == "${{ github.event.pull_request.head.sha }}"
    assert '--source-head-sha "${SOURCE_HEAD_SHA}"' in prepare["run"]
    assert "github.event.pull_request.head.sha" in changed["env"]["HEAD_SHA"]
    assert "github.event.pull_request.base.sha" in changed["env"]["BASE_SHA"]
    assert quality["env"]["HEAD_SHA"] == "${{ github.event.pull_request.head.sha }}"
    assert quality["env"]["BASE_SHA"] == prepare["env"]["BASE_SHA"]
    assert '--head "${HEAD_SHA}"' in quality["run"]
    assert '${BASE_COMMIT}...${HEAD_SHA}' in quality["run"]
    assert '${BASE_COMMIT}...HEAD' not in quality["run"]
    # Selection and actual changed-test coverage reuse the classified source list.
    backend = next(step for step in steps if step.get("id") == "backend_validation")
    assert backend["env"]["AISTOCK_CI_CLASSIFIER_SUMMARY"].endswith("ci_change_classifier/summary.json")
