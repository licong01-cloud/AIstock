from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "aistock_self_hosted_workspace_v1"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _run(args: list[str], *, cwd: Path | None = None, timeout: int = 120) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=str(cwd) if cwd else None,
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        timeout=timeout,
        check=False,
    )


def _fail(message: str, *, code: str = "blocked") -> None:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "workflow_gate": "blocked",
        "code": code,
        "error": message,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    print(f"::error::{message}", file=sys.stderr)
    raise SystemExit(1)


def _resolve(path: str) -> Path:
    return Path(path).expanduser().resolve()


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _normalize_repo_url(value: str) -> str:
    normalized = value.strip().lower().replace("\\", "/")
    if normalized.endswith(".git"):
        normalized = normalized[:-4]
    return normalized


def _repo_matches(remote_url: str, expected_repo: str) -> bool:
    expected = expected_repo.strip().lower()
    if not expected:
        return True
    normalized = _normalize_repo_url(remote_url)
    expected_suffix = expected.replace("\\", "/").removeprefix("https://github.com/")
    return normalized.endswith(expected_suffix) or normalized.endswith(f"github.com/{expected_suffix}")


def _git_stdout(args: list[str], *, cwd: Path, timeout: int = 120) -> str:
    proc = _run(["git", *args], cwd=cwd, timeout=timeout)
    if proc.returncode != 0:
        _fail(
            f"git {' '.join(args)} failed in {cwd}: {proc.stderr.strip() or proc.stdout.strip()}",
            code="git_failed",
        )
    return proc.stdout.strip()


def _first_readable_blob(store_root: Path) -> Path | None:
    blobs_root = store_root / "blobs"
    for bucket in sorted(blobs_root.iterdir(), key=lambda item: item.name):
        if not bucket.is_dir():
            continue
        for candidate in sorted(bucket.iterdir(), key=lambda item: item.name):
            if candidate.is_file():
                with candidate.open("rb") as fh:
                    fh.read(1)
                return candidate
    return None


def _validate_package_asset_store_root(path: Path) -> dict[str, Any]:
    root = path.resolve()
    blobs_root = root / "blobs"
    if not root.is_dir():
        _fail(f"package asset store root is missing: {root}", code="package_asset_store_missing")
    if not blobs_root.is_dir():
        _fail(
            f"package asset store blobs directory is missing: {blobs_root}",
            code="package_asset_store_blobs_missing",
        )
    try:
        smoke_blob = _first_readable_blob(root)
    except OSError as exc:
        _fail(f"package asset store is not readable: {root}: {exc}", code="package_asset_store_unreadable")
    if smoke_blob is None:
        _fail(f"package asset store contains no readable blobs: {blobs_root}", code="package_asset_store_empty")
    return {
        "root": str(root),
        "blobs_root": str(blobs_root),
        "smoke_blob": str(smoke_blob),
        "smoke_blob_size": smoke_blob.stat().st_size,
    }


def _append_github_env(env_file: Path | None, *, key: str, value: str) -> bool:
    if env_file is None:
        return False
    env_file.parent.mkdir(parents=True, exist_ok=True)
    with env_file.open("a", encoding="utf-8", newline="\n") as out:
        out.write(f"{key}={value}\n")
    return True


def _clear_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    for child in path.iterdir():
        if child.name in {".", ".."}:
            continue
        if child.is_dir() and not child.is_symlink():
            shutil.rmtree(child)
        else:
            child.unlink()


def _validate_destination(dest: Path, source: Path, *, allow_any_dest: bool) -> None:
    if dest == source or _is_relative_to(source, dest):
        _fail(f"destination {dest} must not be the canonical source repo {source}", code="unsafe_destination")
    if _is_relative_to(dest, source):
        _fail(f"destination {dest} must not be inside the canonical source repo {source}", code="unsafe_destination")
    if allow_any_dest:
        return
    runner_workspace = os.environ.get("RUNNER_WORKSPACE")
    runner_temp = os.environ.get("RUNNER_TEMP")
    allowed_roots = [_resolve(value) for value in (runner_workspace, runner_temp) if value]
    if allowed_roots and any(dest == root or _is_relative_to(dest, root) for root in allowed_roots):
        return
    _fail(
        "destination must be under RUNNER_WORKSPACE/RUNNER_TEMP unless --allow-any-dest is set",
        code="unsafe_destination",
    )


def prepare_workspace(args: argparse.Namespace) -> dict[str, Any]:
    source = _resolve(args.source)
    dest = _resolve(args.dest)
    expected_commit = args.expected_commit.strip()

    if not source.exists():
        _fail(f"source repo does not exist: {source}", code="missing_source")
    if not (source / ".git").exists():
        _fail(f"source path is not a git checkout: {source}", code="invalid_source")
    if not expected_commit:
        _fail("--expected-commit is required", code="missing_expected_commit")

    _validate_destination(dest, source, allow_any_dest=args.allow_any_dest)

    remote_url = _git_stdout(["config", "--get", "remote.origin.url"], cwd=source)
    if not _repo_matches(remote_url, args.repo):
        _fail(
            f"source repo remote {remote_url!r} does not match expected repository {args.repo!r}",
            code="repo_mismatch",
        )

    cat_file = _run(["git", "cat-file", "-e", f"{expected_commit}^{{commit}}"], cwd=source, timeout=30)
    if cat_file.returncode != 0:
        _fail(
            f"expected commit {expected_commit} is not present in local source {source}; sync the root main checkout first",
            code="missing_commit",
        )

    source_head = _git_stdout(["rev-parse", "HEAD"], cwd=source)
    _clear_directory(dest)

    clone = _run(["git", "clone", "--local", "--no-hardlinks", "--no-checkout", str(source), str(dest)], timeout=300)
    if clone.returncode != 0:
        _fail(f"local git clone failed: {clone.stderr.strip() or clone.stdout.strip()}", code="clone_failed")

    _git_stdout(["checkout", "--detach", expected_commit], cwd=dest, timeout=120)
    checked_out = _git_stdout(["rev-parse", "HEAD"], cwd=dest)
    status = _git_stdout(["status", "--short"], cwd=dest)
    if checked_out != expected_commit:
        _fail(f"checked out {checked_out}, expected {expected_commit}", code="commit_mismatch")
    if status:
        _fail(f"prepared workspace is not clean: {status}", code="dirty_workspace")

    package_asset_store: dict[str, Any] | None = None
    if args.package_asset_store_root:
        package_asset_store = _validate_package_asset_store_root(_resolve(args.package_asset_store_root))
        github_env_file = args.github_env_file or os.environ.get("GITHUB_ENV")
        exported = _append_github_env(
            _resolve(github_env_file) if github_env_file else None,
            key="AISTOCK_PACKAGE_ASSET_STORE_ROOT",
            value=package_asset_store["root"],
        )
        package_asset_store["github_env_exported"] = exported
        package_asset_store["env_name"] = "AISTOCK_PACKAGE_ASSET_STORE_ROOT"

    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "workflow_gate": "ready",
        "source": str(source),
        "source_head": source_head,
        "destination": str(dest),
        "expected_commit": expected_commit,
        "checked_out_commit": checked_out,
        "repo": args.repo,
        "remote_url": remote_url,
        "root_worktree_written": False,
    }
    if package_asset_store is not None:
        payload["package_asset_store"] = package_asset_store

    if args.summary_json:
        requested_summary_path = Path(args.summary_json)
        summary_path = requested_summary_path if requested_summary_path.is_absolute() else dest / requested_summary_path
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        payload["summary_json"] = str(summary_path)

    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare a self-hosted runner workspace from the local AIstock root.")
    parser.add_argument("--source", required=True, help="Synced local AIstock source checkout, e.g. F:/Dev/AIstock.")
    parser.add_argument("--dest", required=True, help="Disposable destination workspace, usually $GITHUB_WORKSPACE.")
    parser.add_argument("--expected-commit", required=True, help="GitHub Actions commit SHA to checkout.")
    parser.add_argument("--repo", default="licong01-cloud/AIstock", help="Expected GitHub repository full name.")
    parser.add_argument("--summary-json", help="Relative path under destination for a compact evidence JSON.")
    parser.add_argument(
        "--package-asset-store-root",
        help=(
            "Canonical StrategyPackage package asset CAS root to validate and export "
            "as AISTOCK_PACKAGE_ASSET_STORE_ROOT for later validation steps."
        ),
    )
    parser.add_argument(
        "--github-env-file",
        help="GitHub Actions env file to append exports to; defaults to $GITHUB_ENV when present.",
    )
    parser.add_argument("--allow-any-dest", action="store_true", help="Allow destinations outside RUNNER_WORKSPACE for local tests.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    payload = prepare_workspace(args)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
