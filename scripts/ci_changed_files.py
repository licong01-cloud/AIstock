"""Build an exact changed-file list for pull-request and push CI events."""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Sequence


class ChangedFilesError(RuntimeError):
    """Raised when the requested Git comparison cannot be proven."""


_FULL_SHA_RE = re.compile(r"^[0-9a-fA-F]{40}$")


def _bounded_fetch(root: Path, args: tuple[str, ...], timeout: float) -> subprocess.CompletedProcess[str]:
    """Timeout only this CI-owned Git/SSH tree, never a runner/service process."""
    options = {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW} if os.name == "nt" else {"start_new_session": True}
    proc = subprocess.Popen(["git", *args], cwd=root, text=True, stdin=subprocess.DEVNULL,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, **options)
    try:
        stdout, stderr = proc.communicate(timeout=timeout)
        return subprocess.CompletedProcess(proc.args, proc.returncode, stdout, stderr)
    except subprocess.TimeoutExpired:
        if proc.poll() is None:
            try:
                if os.name == "nt":
                    subprocess.run(["taskkill", "/PID", str(proc.pid), "/T", "/F"],
                                   capture_output=True, timeout=5, check=False)
                else:
                    os.killpg(proc.pid, signal.SIGKILL)
            finally:
                if proc.poll() is None:
                    proc.kill()
                proc.wait(timeout=5)
        raise
    finally:
        for stream in (proc.stdout, proc.stderr):
            if stream:
                stream.close()


def _git(repo_root: Path, *args: str, timeout: float = 10) -> str:
    try:
        result = _bounded_fetch(repo_root, args, timeout) if args[0] == "fetch" else subprocess.run(
            ["git", *args], cwd=repo_root, check=False, capture_output=True, text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        raise ChangedFilesError(f"git {args[0]} timed out after {timeout:g} seconds") from exc
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or "git command failed"
        raise ChangedFilesError(f"git {' '.join(args)}: {detail}")
    return result.stdout.rstrip("\r\n")


def _restore_local_mirror_ancestry(root: Path, pinned_base: str) -> dict[str, str]:
    """Remove shallow main boundaries by fetching only from a verified local mirror."""
    objects = os.environ.get("GIT_ALTERNATE_OBJECT_DIRECTORIES", "")
    if not objects or os.pathsep in objects or not pinned_base:
        return {"status": "not_required"}
    try:
        object_path = Path(objects).resolve()
        mirror = object_path.parent
        manifest = json.loads(Path(str(mirror) + ".aistock-mirror.json").read_text(encoding="utf-8-sig"))
        if not isinstance(manifest, dict):
            raise ChangedFilesError("local mirror manifest is not an object")
        if (object_path.name != "objects" or not object_path.is_dir()
                or manifest.get("schema_version") != "aistock_git_object_mirror_v1"
                or manifest.get("repository") != "licong01-cloud/AIstock"
                or Path(manifest.get("object_directory", "")).resolve() != object_path
                or Path(manifest.get("mirror_root", "")).resolve() != mirror
                or _git(mirror, "rev-parse", "--is-bare-repository") != "true"
                or _git(mirror, "config", "--get", "aistock.repository") != manifest["repository"]
                or _commit(mirror, "refs/heads/main", "mirror_main") != manifest.get("main_sha")):
            raise ChangedFilesError("local mirror identity/manifest mismatch")
        _commit(mirror, pinned_base, "mirror_base")
        if _git(root, "rev-parse", "--is-shallow-repository") != "true":
            return {"status": "not_required"}
        shallow = Path(_git(root, "rev-parse", "--git-path", "shallow"))
        shallow = shallow if shallow.is_absolute() else root / shallow
        if shallow.is_symlink() or root not in shallow.resolve().parents:
            raise ChangedFilesError("shallow file is not owned by this checkout")
        original = shallow.read_bytes()
        boundaries = original.decode("ascii").splitlines()
        retained: list[str] = []
        # Ignore shallow markers only for proof, never for the final diff. A missing
        # parent rejects reuse. This traverses commits, not worktrees/files/BUG JSON.
        with tempfile.NamedTemporaryFile(delete=False) as empty:
            proof_path = Path(empty.name)
        try:
            for boundary in boundaries:
                if not _FULL_SHA_RE.fullmatch(boundary):
                    raise ChangedFilesError("invalid shallow commit identity")
                try:
                    _git(root, "--shallow-file", str(proof_path), "rev-list", "--missing=error", boundary)
                except ChangedFilesError:
                    retained.append(boundary)
        finally:
            proof_path.unlink()
        if retained == boundaries:
            return {"status": "not_required"}
        # Use Git's own exclusive shallow.lock convention and re-read before write.
        lock_path = Path(str(shallow) + ".lock")
        locked = lock_path.open("xb")
        owns_lock = True
        try:
            with locked:
                if shallow.read_bytes() != original:
                    raise ChangedFilesError("shallow boundaries changed during local proof")
                locked.write("".join(f"{sha}\n" for sha in retained).encode("ascii"))
                locked.flush()
                os.fsync(locked.fileno())
            if retained:
                os.replace(lock_path, shallow)
            else:
                shallow.unlink()
                lock_path.unlink()
            owns_lock = False
        finally:
            if owns_lock:
                lock_path.unlink(missing_ok=True)
        return {"status": "restored"}
    except (ChangedFilesError, OSError, ValueError, TypeError) as exc:
        return {"status": "warning", "reason": str(exc)}


def _commit(repo_root: Path, revision: str, field: str) -> str:
    value = revision.strip()
    if not value:
        raise ChangedFilesError(f"{field} is empty")
    return _git(repo_root, "rev-parse", "--verify", f"{value}^{{commit}}").strip()


def prepare_pr_merge_base(
    *,
    repo_root: Path,
    base_ref: str,
    base_sha: str,
    checkout_ref: str,
    source_head_sha: str = "",
    resolve_current_base: bool = False,
    attempts: int = 3,
    deepen_by: int = 64,
    fetch_timeout: float = 45,
    total_budget: float = 150,
) -> dict[str, object]:
    """Prove pinned PR source ancestry without attributing integration-only files."""

    root = repo_root.resolve()
    branch = base_ref.strip()
    pinned_base = base_sha.strip().lower()
    source_ref = checkout_ref.strip()
    if not branch:
        raise ChangedFilesError("base_ref is empty")
    _git(root, "check-ref-format", "--branch", branch)
    resolve_manual_base = resolve_current_base and not pinned_base
    if not _FULL_SHA_RE.fullmatch(pinned_base) and not resolve_manual_base:
        raise ChangedFilesError("base_sha must be a full Git commit identity")
    if not source_ref.startswith("refs/"):
        raise ChangedFilesError("checkout_ref must be an exact refs/* name")
    _git(root, "check-ref-format", source_ref)
    head_commit = _commit(root, "HEAD", "head_sha")
    pinned_source = source_head_sha.strip().lower()
    if pinned_source and not _FULL_SHA_RE.fullmatch(pinned_source):
        raise ChangedFilesError("source_head_sha must be a full Git commit identity")
    if resolve_current_base and pinned_source != head_commit:
        raise ChangedFilesError("manual source_head_sha must match the checked-out HEAD")

    def ready() -> bool:
        try:
            _commit(root, pinned_base, "base_sha")
            source_commit = _commit(root, pinned_source or head_commit, "source_head_sha")
            if pinned_source:
                _git(root, "merge-base", "--is-ancestor", source_commit, head_commit)
            _git(root, "merge-base", pinned_base, source_commit)
            return True
        except ChangedFilesError as exc:
            if (pinned_source and "--is-ancestor" in str(exc)
                    and _git(root, "rev-parse", "--is-shallow-repository") == "false"):
                raise
            return False

    started = time.monotonic()
    used_attempts = 0
    local_history: dict[str, str] = {"status": "not_required"}
    history_ready = ready()
    if not history_ready:
        local_history = _restore_local_mirror_ancestry(root, pinned_base)
        history_ready = ready()
    if not history_ready:
        fetch_specs = [
            f"+{pinned_base or 'refs/heads/' + branch}:refs/remotes/origin/{branch}",
            f"+{source_ref}:refs/remotes/origin/aistock-pr-checkout",
        ]
        detail = "merge base remains unavailable"
        for index in range(min(3, max(1, int(attempts)))):
            remaining = total_budget - (time.monotonic() - started)
            if remaining <= 0:
                raise ChangedFilesError("pinned PR base/head history preparation failed: total fetch budget exhausted")
            used_attempts = index + 1
            try:
                _git(root,
                    "fetch",
                    "--no-tags",
                    "--no-write-fetch-head",
                    f"--deepen={max(1, int(deepen_by))}",
                    "origin",
                    *fetch_specs,
                    timeout=min(fetch_timeout, remaining),
                )
                if resolve_manual_base and not pinned_base:
                    pinned_base = _commit(root, f"refs/remotes/origin/{branch}", "base_ref")
                if ready():
                    break
            except ChangedFilesError as exc:
                detail = str(exc)
            if index + 1 < min(3, max(1, int(attempts))):
                time.sleep(0.5 * (index + 1))
        else:
            raise ChangedFilesError(f"pinned PR base/head history preparation failed: {detail}")
    _git(root, "update-ref", f"refs/remotes/origin/{branch}", pinned_base)
    source_commit = _commit(root, pinned_source or head_commit, "source_head_sha")
    if _commit(root, "HEAD", "head_sha") != head_commit:
        raise ChangedFilesError("checkout HEAD changed during source ancestry preparation")
    if pinned_source:
        _git(root, "merge-base", "--is-ancestor", source_commit, head_commit)
    merge_base = _git(root, "merge-base", pinned_base, source_commit).strip()
    return {
        "schema_version": "aistock_ci_pr_merge_base_preparation_v1",
        "event_mode": "workflow_dispatch" if resolve_current_base else "pull_request",
        "base_commit": pinned_base,
        "head_commit": head_commit,
        "source_head_commit": source_commit,
        "merge_base": merge_base,
        "fetch_used": used_attempts > 0,
        "fetch_attempts": used_attempts,
        "deepen_by": max(1, int(deepen_by)),
        "local_mirror_ancestry": local_history,
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }


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
    """Return source changes using pinned PR identities or the current manual base."""

    root = repo_root.resolve()
    head_commit = _commit(root, head_sha or "HEAD", "head_sha")
    normalized_base_sha = base_sha.strip()
    if base_ref.strip():
        if normalized_base_sha:
            if not _FULL_SHA_RE.fullmatch(normalized_base_sha):
                raise ChangedFilesError("PR base_sha must be a full Git commit identity")
            _git(root, "check-ref-format", "--branch", base_ref.strip())
            base_commit = _commit(root, normalized_base_sha, "base_sha")
            base_source = "pinned_pr_base_sha"
        else:
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
    parser.add_argument("--output")
    parser.add_argument("--base-ref", default="")
    parser.add_argument("--base-sha", default="")
    parser.add_argument("--head-sha", default="HEAD")
    parser.add_argument("--diff-filter", default="")
    parser.add_argument("--prepare-pr-merge-base-only", action="store_true")
    parser.add_argument("--prepare-manual-merge-base-only", action="store_true")
    parser.add_argument("--checkout-ref", default="")
    parser.add_argument("--source-head-sha", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        if args.prepare_pr_merge_base_only and args.prepare_manual_merge_base_only:
            raise ChangedFilesError("choose exactly one preparation event mode")
        if args.prepare_pr_merge_base_only or args.prepare_manual_merge_base_only:
            receipt = prepare_pr_merge_base(
                repo_root=Path(args.repo_root),
                base_ref=args.base_ref,
                base_sha=args.base_sha,
                checkout_ref=args.checkout_ref,
                source_head_sha=args.source_head_sha,
                resolve_current_base=args.prepare_manual_merge_base_only,
            )
            print(json.dumps(receipt, sort_keys=True))
            return 0
        if not args.output:
            raise ChangedFilesError("--output is required unless --prepare-pr-merge-base-only is used")
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
