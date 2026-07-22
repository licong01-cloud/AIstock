"""Build an exact changed-file list for pull-request and push CI events."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Sequence


class ChangedFilesError(RuntimeError):
    """Raised when the requested Git comparison cannot be proven."""


def _git(repo_root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or "git command failed"
        raise ChangedFilesError(f"git {' '.join(args)}: {detail}")
    return result.stdout.rstrip("\r\n")


def _commit(repo_root: Path, revision: str, field: str) -> str:
    value = revision.strip()
    if not value:
        raise ChangedFilesError(f"{field} is empty")
    return _git(repo_root, "rev-parse", "--verify", f"{value}^{{commit}}").strip()


def _current_base_commit(repo_root: Path, base_ref: str) -> str:
    branch = base_ref.strip()
    if not branch:
        raise ChangedFilesError("base_ref is empty")
    _git(repo_root, "check-ref-format", "--branch", branch)
    candidates = (f"refs/remotes/origin/{branch}", f"refs/heads/{branch}")
    failures: list[str] = []
    for candidate in candidates:
        try:
            return _commit(repo_root, candidate, "base_ref")
        except ChangedFilesError as exc:
            failures.append(str(exc))
    raise ChangedFilesError(
        f"current base ref cannot be resolved for {branch}; checked {', '.join(candidates)}; "
        f"details={' | '.join(failures)}"
    )


def build_changed_files(
    *,
    repo_root: Path,
    base_ref: str = "",
    base_sha: str = "",
    head_sha: str = "HEAD",
    diff_filter: str = "",
) -> tuple[list[str], dict[str, str | int | None]]:
    """Return changed paths, preferring the current PR base ref over stale event SHA."""

    root = repo_root.resolve()
    head_commit = _commit(root, head_sha or "HEAD", "head_sha")
    normalized_base_sha = base_sha.strip()
    if base_ref.strip():
        base_commit = _current_base_commit(root, base_ref)
        base_source = "current_base_ref"
    elif normalized_base_sha and set(normalized_base_sha) != {"0"}:
        base_commit = _commit(root, normalized_base_sha, "base_sha")
        base_source = "event_base_sha"
    else:
        base_commit = None
        base_source = "single_commit_fallback"

    filter_args: list[str] = []
    if diff_filter.strip():
        filter_args.append(f"--diff-filter={diff_filter.strip()}")
    if base_commit is None:
        output = _git(root, "diff-tree", "--no-commit-id", "--name-only", *filter_args, "-r", head_commit)
    else:
        output = _git(root, "diff", "--name-only", *filter_args, f"{base_commit}...{head_commit}")
    changed_files = [line for line in output.splitlines() if line]
    if len(changed_files) != len(set(changed_files)):
        raise ChangedFilesError("git comparison returned duplicate changed paths")
    receipt: dict[str, str | int | None] = {
        "schema_version": "aistock_ci_changed_files_v1",
        "base_source": base_source,
        "base_ref": base_ref.strip() or None,
        "base_commit": base_commit,
        "event_base_sha": normalized_base_sha or None,
        "head_commit": head_commit,
        "diff_filter": diff_filter.strip() or None,
        "changed_file_count": len(changed_files),
    }
    return changed_files, receipt


def _write_changed_files(path: Path, changed_files: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = "".join(f"{item}\n" for item in changed_files)
    path.write_text(payload, encoding="utf-8", newline="\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--output", required=True)
    parser.add_argument("--base-ref", default="")
    parser.add_argument("--base-sha", default="")
    parser.add_argument("--head-sha", default="HEAD")
    parser.add_argument("--diff-filter", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        changed_files, receipt = build_changed_files(
            repo_root=Path(args.repo_root),
            base_ref=args.base_ref,
            base_sha=args.base_sha,
            head_sha=args.head_sha,
            diff_filter=args.diff_filter,
        )
        output = Path(args.output)
        _write_changed_files(output, changed_files)
        print(json.dumps({**receipt, "output": output.as_posix()}, sort_keys=True))
        return 0
    except (ChangedFilesError, OSError) as exc:
        print(
            json.dumps(
                {
                    "schema_version": "aistock_ci_changed_files_error_v1",
                    "status": "failed",
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                },
                sort_keys=True,
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
