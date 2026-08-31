from __future__ import annotations

import json
import stat
import subprocess
from pathlib import Path

import pytest

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


def test_clear_directory_removes_readonly_git_object_tree(tmp_path: Path) -> None:
    destination = tmp_path / "runner" / "_work" / "AIstock" / "AIstock"
    readonly_object = destination / ".git" / "objects" / "21" / ("9" * 40)
    readonly_object.parent.mkdir(parents=True)
    readonly_object.write_bytes(b"git object")
    readonly_object.chmod(stat.S_IREAD)

    prepare._clear_directory(destination)

    assert list(destination.iterdir()) == []


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


def test_prepare_workspace_exports_canonical_package_asset_store_root(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "source"
    dest = tmp_path / "runner" / "_work" / "AIstock" / "AIstock"
    summary_path = tmp_path / "runner" / "_temp" / "self_hosted_workspace" / "test.json"
    github_env = tmp_path / "runner" / "_temp" / "github_env.txt"
    asset_root = source / "rdagent_assets" / "package_assets"
    blob = asset_root / "blobs" / "05" / ("0" * 64)
    blob.parent.mkdir(parents=True)
    blob.write_bytes(b"factor-code")

    source.mkdir(parents=True, exist_ok=True)
    _git(source, "init")
    _git(source, "config", "user.email", "ci@example.invalid")
    _git(source, "config", "user.name", "CI")
    _git(source, "remote", "add", "origin", "https://github.com/licong01-cloud/AIstock.git")
    (source / "README.md").write_text("local source\n", encoding="utf-8")
    _git(source, "add", "README.md")
    _git(source, "commit", "-m", "seed")
    commit = _git(source, "rev-parse", "HEAD")

    monkeypatch.setenv("RUNNER_WORKSPACE", str(dest.parent))
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
            "--package-asset-store-root",
            str(asset_root),
            "--github-env-file",
            str(github_env),
        ]
    )

    assert result == 0
    assert f"AISTOCK_PACKAGE_ASSET_STORE_ROOT={asset_root.resolve()}" in github_env.read_text(encoding="utf-8")
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    package_asset_store = summary["package_asset_store"]
    assert package_asset_store["root"] == str(asset_root.resolve())
    assert package_asset_store["github_env_exported"] is True
    assert package_asset_store["smoke_blob"] == str(blob.resolve())
    assert package_asset_store["smoke_blob_size"] == len(b"factor-code")


def test_prepare_workspace_loud_fails_when_package_asset_store_missing(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "source"
    dest = tmp_path / "runner" / "_work" / "AIstock" / "AIstock"
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
    missing_root = source / "rdagent_assets" / "package_assets"

    try:
        prepare.main(
            [
                "--source",
                str(source),
                "--dest",
                str(dest),
                "--expected-commit",
                commit,
                "--repo",
                "licong01-cloud/AIstock",
                "--package-asset-store-root",
                str(missing_root),
            ]
        )
    except SystemExit as exc:
        assert exc.code == 1
    else:
        raise AssertionError("missing package asset store must fail loud")


def test_prepare_workspace_links_lockfile_matched_prebuilt_frontend_dependencies(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source = tmp_path / "source"
    dest = tmp_path / "runner" / "_work" / "AIstock" / "AIstock"
    summary_path = tmp_path / "runner" / "_temp" / "self_hosted_workspace" / "test.json"
    frontend = source / "frontend"
    node_modules = frontend / "node_modules"
    for relative in prepare.REQUIRED_FRONTEND_ENTRYPOINTS:
        entrypoint = node_modules / relative
        entrypoint.parent.mkdir(parents=True, exist_ok=True)
        entrypoint.write_text(f"prebuilt-{relative.as_posix()}\n", encoding="utf-8")
    marker = node_modules / "@playwright" / "test" / "cli.js"
    (frontend / "package-lock.json").write_text('{"lockfileVersion":3}\n', encoding="utf-8")
    (source / ".gitignore").write_text("frontend/node_modules/\n", encoding="utf-8")
    _git(source, "init")
    _git(source, "config", "user.email", "ci@example.invalid")
    _git(source, "config", "user.name", "CI")
    _git(source, "remote", "add", "origin", "https://github.com/licong01-cloud/AIstock.git")
    _git(source, "add", ".gitignore", "frontend/package-lock.json")
    _git(source, "commit", "-m", "seed")
    commit = _git(source, "rev-parse", "HEAD")

    monkeypatch.setenv("RUNNER_WORKSPACE", str(dest.parent))
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
            "--frontend-node-modules-source",
            str(node_modules),
        ]
    )

    assert result == 0
    assert (dest / "frontend" / "node_modules" / "@playwright" / "test" / "cli.js").read_text(
        encoding="utf-8"
    ) == "prebuilt-@playwright/test/cli.js\n"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["frontend_node_modules"]["source"] == str(node_modules.resolve())
    assert len(summary["frontend_node_modules"]["package_lock_sha256"]) == 64
    assert len(summary["frontend_node_modules"]["direct_entrypoints"]) == 3
    prepare._clear_directory(dest)
    assert marker.read_text(encoding="utf-8") == "prebuilt-@playwright/test/cli.js\n"


def test_frontend_dependency_link_fails_closed_on_package_lock_mismatch(tmp_path: Path) -> None:
    source = tmp_path / "source" / "frontend" / "node_modules"
    destination = tmp_path / "checkout"
    marker = source / "@playwright" / "test" / "cli.js"
    marker.parent.mkdir(parents=True)
    marker.write_text("playwright\n", encoding="utf-8")
    (source.parent / "package-lock.json").write_text("source-lock\n", encoding="utf-8")
    (destination / "frontend").mkdir(parents=True)
    (destination / "frontend" / "package-lock.json").write_text("checkout-lock\n", encoding="utf-8")

    try:
        prepare._materialize_frontend_node_modules(source, destination)
    except SystemExit as exc:
        assert exc.code == 1
    else:
        raise AssertionError("package-lock mismatch must fail loud")


def test_attach_frontend_only_preserves_checkout_and_links_verified_dependencies(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source = tmp_path / "source"
    dest = tmp_path / "runner" / "_work" / "AIstock" / "AIstock"
    summary_path = tmp_path / "runner" / "_temp" / "self_hosted_workspace" / "frontend.json"
    frontend = source / "frontend"
    node_modules = frontend / "node_modules"
    for relative in prepare.REQUIRED_FRONTEND_ENTRYPOINTS:
        entrypoint = node_modules / relative
        entrypoint.parent.mkdir(parents=True, exist_ok=True)
        entrypoint.write_text(f"prebuilt-{relative.as_posix()}\n", encoding="utf-8")
    (frontend / "package-lock.json").write_text('{"lockfileVersion":3}\n', encoding="utf-8")
    tracked_marker = source / "tracked-marker.txt"
    tracked_marker.write_text("keep checkout\n", encoding="utf-8")
    (source / ".gitignore").write_text("frontend/node_modules/\n", encoding="utf-8")
    _git(source, "init")
    _git(source, "config", "user.email", "ci@example.invalid")
    _git(source, "config", "user.name", "CI")
    _git(source, "remote", "add", "origin", "https://github.com/licong01-cloud/AIstock.git")
    _git(source, "add", ".gitignore", "frontend/package-lock.json", "tracked-marker.txt")
    _git(source, "commit", "-m", "seed")
    commit = _git(source, "rev-parse", "HEAD")
    _git(tmp_path, "clone", "--local", "--no-hardlinks", str(source), str(dest))

    monkeypatch.setenv("RUNNER_WORKSPACE", str(dest.parent))
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
            "--frontend-node-modules-source",
            str(node_modules),
            "--attach-frontend-only",
        ]
    )

    assert result == 0
    assert (dest / "tracked-marker.txt").read_text(encoding="utf-8") == "keep checkout\n"
    assert (dest / "frontend" / "node_modules" / "typescript" / "bin" / "tsc").is_file()
    assert _git(dest, "status", "--short") == ""
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["mode"] == "attach_frontend_only"
    assert summary["checked_out_commit"] == commit
    assert summary["root_worktree_written"] is False


def test_attach_frontend_only_fails_closed_on_checkout_commit_mismatch(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source = tmp_path / "source"
    dest = tmp_path / "runner" / "_work" / "AIstock" / "AIstock"
    (source / ".git").mkdir(parents=True)
    dest.mkdir(parents=True)
    actual_commit = "a" * 40
    expected_commit = "b" * 40

    def fake_git_stdout(args: list[str], *, cwd: Path, timeout: int = 120) -> str:
        del timeout
        if args == ["config", "--get", "remote.origin.url"]:
            return "https://github.com/licong01-cloud/AIstock.git"
        if args == ["rev-parse", "--show-toplevel"] and cwd == dest.resolve():
            return str(dest.resolve())
        if args == ["rev-parse", "HEAD"] and cwd == dest.resolve():
            return actual_commit
        raise AssertionError(f"unexpected git query: {args} cwd={cwd}")

    monkeypatch.setattr(prepare, "_git_stdout", fake_git_stdout)
    monkeypatch.setenv("RUNNER_WORKSPACE", str(dest.parent))

    with pytest.raises(SystemExit) as exc_info:
        prepare.main(
            [
                "--source",
                str(source),
                "--dest",
                str(dest),
                "--expected-commit",
                expected_commit,
                "--repo",
                "licong01-cloud/AIstock",
                "--frontend-node-modules-source",
                str(source / "frontend" / "node_modules"),
                "--attach-frontend-only",
            ]
        )

    assert exc_info.value.code == 1
