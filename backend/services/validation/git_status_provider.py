from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.services.validation.file_ownership import FileOwnershipCatalog
from backend.services.validation.module_registry import REPO_ROOT


WORKSPACE_STATUS_SCHEMA = "aistock_git_workspace_status_v1"
BRANCH_STATUS_SCHEMA = "aistock_git_branch_status_v1"
ALLOWED_GIT_COMMANDS = {
    ("status", "--porcelain=v2", "--branch", "-z"),
    ("rev-parse", "--short", "HEAD"),
}


class GitStatusProviderError(ValueError):
    """Raised when read-only git status collection fails."""


@dataclass(frozen=True)
class GitStatusEntry:
    path: str
    status: str
    xy: str | None = None
    old_path: str | None = None
    staged: bool = False
    unstaged: bool = False
    untracked: bool = False
    conflicted: bool = False


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _repo_path(path: Path, repo_root: Path) -> str:
    try:
        return path.resolve().relative_to(repo_root.resolve()).as_posix()
    except ValueError:
        return str(path)


def _display_repo_root(path: Path) -> str:
    try:
        return str(path.resolve()).replace("\\", "/")
    except OSError:
        return str(path).replace("\\", "/")


def _normalize_git_path(path: str) -> str:
    return path.strip().replace("\\", "/")


def _risk_rank(risk_level: str | None) -> int:
    return {"low": 1, "medium": 2, "high": 3, "critical": 4}.get(str(risk_level or ""), 0)


def _status_from_xy(xy: str) -> str:
    if len(xy) < 2:
        return "unknown"
    index_status = xy[0]
    worktree_status = xy[1]
    if "U" in xy:
        return "conflicted"
    if index_status in {"R", "C"}:
        return "renamed"
    if index_status == "A":
        return "staged_added"
    if index_status == "D":
        return "staged_deleted"
    if index_status in {"M", "T"}:
        return "staged_modified"
    if worktree_status == "D":
        return "unstaged_deleted"
    if worktree_status in {"M", "T"}:
        return "unstaged_modified"
    return "unknown"


class GitWorkspaceStatusProvider:
    """Collect read-only git workspace and branch status for Validation Center."""

    def __init__(
        self,
        *,
        repo_root: Path | None = None,
        file_ownership_catalog: FileOwnershipCatalog | None = None,
    ) -> None:
        self.repo_root = Path(repo_root or REPO_ROOT)
        self.file_ownership_catalog = file_ownership_catalog or FileOwnershipCatalog()

    def branch_status(self) -> dict[str, Any]:
        parsed = self._parsed_status()
        branch = parsed["branch"]
        return {
            "schema_version": BRANCH_STATUS_SCHEMA,
            "generated_at": _now_iso(),
            "repo_root": _display_repo_root(self.repo_root),
            "branch": branch.get("head"),
            "detached": branch.get("head") == "(detached)",
            "upstream": branch.get("upstream"),
            "head_commit": branch.get("oid"),
            "short_head_commit": self._short_head_commit(),
            "ahead_count": int(branch.get("ahead") or 0),
            "behind_count": int(branch.get("behind") or 0),
            "upstream_known": bool(branch.get("upstream")),
            "git_command_mode": "read_only_allowlist",
            "arbitrary_shell_allowed": False,
            "production_8001_touched": False,
        }

    def workspace_status(self) -> dict[str, Any]:
        parsed = self._parsed_status()
        branch_status = self.branch_status()
        files = [self._entry_to_file_payload(entry) for entry in parsed["entries"]]
        status_counts: dict[str, int] = {}
        by_module: dict[str, dict[str, Any]] = {}
        summary = {
            "changed_files": len(files),
            "staged_files": 0,
            "unstaged_files": 0,
            "untracked_files": 0,
            "conflicted_files": 0,
            "deleted_files": 0,
            "renamed_files": 0,
            "unmapped_files": 0,
            "ambiguous_files": 0,
            "critical_risk_files": 0,
        }
        for item in files:
            status = str(item["status"])
            status_counts[status] = status_counts.get(status, 0) + 1
            if item["staged"]:
                summary["staged_files"] += 1
            if item["unstaged"]:
                summary["unstaged_files"] += 1
            if item["untracked"]:
                summary["untracked_files"] += 1
            if item["conflicted"]:
                summary["conflicted_files"] += 1
            if status in {"staged_deleted", "unstaged_deleted"}:
                summary["deleted_files"] += 1
            if status == "renamed":
                summary["renamed_files"] += 1
            if item["ownership_status"] == "unmapped":
                summary["unmapped_files"] += 1
            if item["ownership_status"] == "ambiguous":
                summary["ambiguous_files"] += 1
            if item.get("risk_level") == "critical":
                summary["critical_risk_files"] += 1
            primary_module = item.get("primary_module")
            if primary_module:
                bucket = by_module.setdefault(
                    str(primary_module),
                    {
                        "module_id": primary_module,
                        "changed_file_count": 0,
                        "max_risk_level": item.get("risk_level"),
                        "statuses": {},
                    },
                )
                bucket["changed_file_count"] += 1
                bucket["statuses"][status] = bucket["statuses"].get(status, 0) + 1
                if _risk_rank(item.get("risk_level")) > _risk_rank(bucket.get("max_risk_level")):
                    bucket["max_risk_level"] = item.get("risk_level")
        reason_codes: list[str] = []
        if files:
            reason_codes.append("workspace_dirty")
        if summary["untracked_files"]:
            reason_codes.append("untracked_files_present")
        if summary["unmapped_files"]:
            reason_codes.append("unmapped_files_present")
        if summary["ambiguous_files"]:
            reason_codes.append("ambiguous_files_present")
        if summary["conflicted_files"]:
            reason_codes.append("conflicted_files_present")
        return {
            "schema_version": WORKSPACE_STATUS_SCHEMA,
            "generated_at": _now_iso(),
            "repo_root": _display_repo_root(self.repo_root),
            "branch": branch_status["branch"],
            "upstream": branch_status["upstream"],
            "head_commit": branch_status["head_commit"],
            "short_head_commit": branch_status["short_head_commit"],
            "ahead_count": branch_status["ahead_count"],
            "behind_count": branch_status["behind_count"],
            "dirty": bool(files),
            "summary": summary,
            "by_status": dict(sorted(status_counts.items())),
            "by_module": sorted(by_module.values(), key=lambda item: item["module_id"]),
            "files": sorted(files, key=lambda item: item["path"]),
            "reason_codes": reason_codes,
            "git_command_mode": "read_only_allowlist",
            "arbitrary_shell_allowed": False,
            "production_8001_touched": False,
        }

    def _entry_to_file_payload(self, entry: GitStatusEntry) -> dict[str, Any]:
        ownership = self.file_ownership_catalog.match_path(entry.path).to_dict()
        reason_codes = list(ownership.get("reason_codes") or [])
        if entry.untracked:
            reason_codes.append("git_untracked")
        if entry.conflicted:
            reason_codes.append("git_conflicted")
        if entry.status in {"staged_deleted", "unstaged_deleted"}:
            reason_codes.append("git_deleted")
        if entry.status == "renamed":
            reason_codes.append("git_renamed")
        return {
            "path": entry.path,
            "old_path": entry.old_path,
            "status": entry.status,
            "git_xy": entry.xy,
            "staged": entry.staged,
            "unstaged": entry.unstaged,
            "untracked": entry.untracked,
            "conflicted": entry.conflicted,
            "primary_module": ownership.get("primary_module"),
            "impact_modules": ownership.get("impact_modules") or [],
            "layer": ownership.get("layer"),
            "risk_level": ownership.get("risk_level"),
            "ownership_status": ownership.get("ownership_status"),
            "matched_rule_ids": ownership.get("matched_rule_ids") or [],
            "reason_codes": reason_codes,
            "recommended_action": self._recommended_action(entry, str(ownership.get("ownership_status"))),
        }

    @staticmethod
    def _recommended_action(entry: GitStatusEntry, ownership_status: str) -> str:
        if entry.conflicted:
            return "resolve_conflict_before_validation"
        if ownership_status in {"unmapped", "ambiguous"}:
            return "add_file_ownership_mapping_before_commit"
        if entry.untracked:
            return "review_add_and_validate_before_commit"
        if entry.staged:
            return "run_changed_files_guard_and_commit"
        return "stage_validate_and_commit"

    def _parsed_status(self) -> dict[str, Any]:
        output = self._run_git(["status", "--porcelain=v2", "--branch", "-z"])
        records = output.split("\0")
        branch: dict[str, Any] = {}
        entries: list[GitStatusEntry] = []
        index = 0
        while index < len(records):
            record = records[index]
            if not record:
                index += 1
                continue
            if record.startswith("# "):
                self._parse_branch_header(record, branch)
                index += 1
                continue
            if record.startswith("? "):
                entries.append(
                    GitStatusEntry(
                        path=_normalize_git_path(record[2:]),
                        status="untracked",
                        untracked=True,
                    )
                )
                index += 1
                continue
            if record.startswith("! "):
                index += 1
                continue
            if record.startswith("1 "):
                entry = self._parse_ordinary_record(record)
                if entry:
                    entries.append(entry)
                index += 1
                continue
            if record.startswith("2 "):
                old_path = records[index + 1] if index + 1 < len(records) else None
                entry = self._parse_rename_record(record, old_path)
                if entry:
                    entries.append(entry)
                    index += 2 if old_path else 1
                else:
                    index += 1
                continue
            if record.startswith("u "):
                entry = self._parse_unmerged_record(record)
                if entry:
                    entries.append(entry)
                index += 1
                continue
            index += 1
        return {"branch": branch, "entries": entries}

    @staticmethod
    def _parse_branch_header(record: str, branch: dict[str, Any]) -> None:
        text = record[2:]
        if text.startswith("branch.oid "):
            branch["oid"] = text.removeprefix("branch.oid ").strip()
        elif text.startswith("branch.head "):
            branch["head"] = text.removeprefix("branch.head ").strip()
        elif text.startswith("branch.upstream "):
            branch["upstream"] = text.removeprefix("branch.upstream ").strip()
        elif text.startswith("branch.ab "):
            parts = text.removeprefix("branch.ab ").split()
            for part in parts:
                if part.startswith("+"):
                    branch["ahead"] = int(part[1:] or 0)
                elif part.startswith("-"):
                    branch["behind"] = int(part[1:] or 0)

    @staticmethod
    def _parse_ordinary_record(record: str) -> GitStatusEntry | None:
        parts = record.split(" ", 8)
        if len(parts) < 9:
            return None
        xy = parts[1]
        path = _normalize_git_path(parts[8])
        status = _status_from_xy(xy)
        return GitStatusEntry(
            path=path,
            status=status,
            xy=xy,
            staged=xy[0] != ".",
            unstaged=xy[1] != ".",
            conflicted=status == "conflicted",
        )

    @staticmethod
    def _parse_rename_record(record: str, old_path: str | None) -> GitStatusEntry | None:
        parts = record.split(" ", 9)
        if len(parts) < 10:
            return None
        xy = parts[1]
        path = _normalize_git_path(parts[9])
        old = _normalize_git_path(old_path or "") or None
        return GitStatusEntry(
            path=path,
            old_path=old,
            status="renamed",
            xy=xy,
            staged=xy[0] != ".",
            unstaged=xy[1] != ".",
        )

    @staticmethod
    def _parse_unmerged_record(record: str) -> GitStatusEntry | None:
        parts = record.split(" ", 10)
        if len(parts) < 11:
            return None
        return GitStatusEntry(
            path=_normalize_git_path(parts[10]),
            status="conflicted",
            xy=parts[1],
            staged=True,
            unstaged=True,
            conflicted=True,
        )

    def _short_head_commit(self) -> str | None:
        try:
            value = self._run_git(["rev-parse", "--short", "HEAD"]).strip()
        except GitStatusProviderError:
            return None
        return value or None

    def _run_git(self, args: list[str]) -> str:
        command_key = tuple(args)
        if command_key not in ALLOWED_GIT_COMMANDS:
            raise GitStatusProviderError(f"git command is not allowlisted: {json.dumps(args)}")
        try:
            completed = subprocess.run(
                ["git", *args],
                cwd=str(self.repo_root),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                shell=False,
                check=False,
                timeout=10,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise GitStatusProviderError(f"git command failed: {type(exc).__name__}: {exc}") from exc
        if completed.returncode != 0:
            raise GitStatusProviderError(
                f"git command {json.dumps(args)} failed: {(completed.stderr or '').strip()}"
            )
        return completed.stdout or ""
