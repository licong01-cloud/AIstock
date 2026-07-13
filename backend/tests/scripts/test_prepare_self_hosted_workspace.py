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
