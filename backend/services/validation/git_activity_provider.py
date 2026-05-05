from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.services.validation.file_ownership import FileOwnershipCatalog
from backend.services.validation.git_status_provider import GitWorkspaceStatusProvider
from backend.services.validation.module_registry import ModuleRegistry, REPO_ROOT


COMMIT_ACTIVITY_SCHEMA = "aistock_git_commit_activity_v1"


class GitActivityProviderError(ValueError):
    """Raised when read-only git commit activity collection fails."""


@dataclass(frozen=True)
class GitCommitFile:
    path: str
    change_type: str
    old_path: str | None = None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _display_repo_root(path: Path) -> str:
    try:
        return str(path.resolve()).replace("\\", "/")
    except OSError:
        return str(path).replace("\\", "/")


def _normalize_git_path(path: str) -> str:
    return path.strip().replace("\\", "/")


def _risk_rank(risk_level: str | None) -> int:
    return {"low": 1, "medium": 2, "high": 3, "critical": 4}.get(str(risk_level or ""), 0)


def _safe_limit(limit: int) -> int:
    return max(1, min(int(limit or 50), 200))


class GitCommitActivityProvider:
    """Collect read-only git commit activity and map changed files to AIstock modules."""

    def __init__(
        self,
        *,
        repo_root: Path | None = None,
        file_ownership_catalog: FileOwnershipCatalog | None = None,
        module_registry: ModuleRegistry | None = None,
        git_status_provider: GitWorkspaceStatusProvider | None = None,
    ) -> None:
        self.repo_root = Path(repo_root or REPO_ROOT)
        self.module_registry = module_registry or ModuleRegistry()
        self.file_ownership_catalog = file_ownership_catalog or FileOwnershipCatalog(
            module_registry=self.module_registry,
        )
        self.git_status_provider = git_status_provider or GitWorkspaceStatusProvider(
            repo_root=self.repo_root,
            file_ownership_catalog=self.file_ownership_catalog,
        )

    def commit_activity(self, *, limit: int = 50) -> dict[str, Any]:
        safe_limit = _safe_limit(limit)
        branch = self.git_status_provider.branch_status()
        commits = [self._commit_to_payload(item) for item in self._read_commits(safe_limit)]
        return {
            "schema_version": COMMIT_ACTIVITY_SCHEMA,
            "generated_at": _now_iso(),
            "repo_root": _display_repo_root(self.repo_root),
            "branch": branch.get("branch"),
            "upstream": branch.get("upstream"),
            "head_commit": branch.get("head_commit"),
            "short_head_commit": branch.get("short_head_commit"),
            "limit": safe_limit,
            "summary": self._summary(commits),
            "by_day": self._period_counts(commits, period="day"),
            "by_week": self._period_counts(commits, period="week"),
            "by_month": self._period_counts(commits, period="month"),
            "by_module": self._module_counts(commits),
            "commits": commits,
            "git_command_mode": "read_only_allowlist",
            "arbitrary_shell_allowed": False,
            "production_8001_touched": False,
        }

    def _commit_to_payload(self, item: dict[str, Any]) -> dict[str, Any]:
        files = [self._file_to_payload(file_item) for file_item in item["files"]]
        status_counts: dict[str, int] = {}
        module_ids: set[str] = set()
        ownership_summary = {"mapped": 0, "unmapped": 0, "ambiguous": 0}
        max_risk_level: str | None = None
        for file_item in files:
            change_type = str(file_item.get("change_type") or "unknown")
            status_counts[change_type] = status_counts.get(change_type, 0) + 1
            status = str(file_item.get("ownership_status") or "unmapped")
            ownership_summary[status] = ownership_summary.get(status, 0) + 1
            primary_module = file_item.get("primary_module")
            if primary_module:
                module_ids.add(str(primary_module))
            if _risk_rank(file_item.get("risk_level")) > _risk_rank(max_risk_level):
                max_risk_level = str(file_item.get("risk_level") or "")
        return {
            "commit_hash": item["commit_hash"],
            "short_hash": item["short_hash"],
            "author_name": item["author_name"],
            "author_email": item["author_email"],
            "authored_at": item["authored_at"],
            "subject": item["subject"],
            "changed_file_count": len(files),
            "file_status_counts": dict(sorted(status_counts.items())),
            "module_ids": sorted(module_ids),
            "ownership_summary": ownership_summary,
            "max_risk_level": max_risk_level,
            "files": sorted(files, key=lambda file_item: str(file_item["path"])),
        }

    def _file_to_payload(self, file_item: GitCommitFile) -> dict[str, Any]:
        ownership = self.file_ownership_catalog.match_path(file_item.path).to_dict()
        reason_codes = list(ownership.get("reason_codes") or [])
        if file_item.old_path:
            reason_codes.append("git_renamed")
        if ownership.get("ownership_status") in {"unmapped", "ambiguous"}:
            reason_codes.append("commit_file_requires_ownership_review")
        return {
            "path": file_item.path,
            "old_path": file_item.old_path,
            "change_type": file_item.change_type,
            "primary_module": ownership.get("primary_module"),
            "impact_modules": ownership.get("impact_modules") or [],
            "layer": ownership.get("layer"),
            "risk_level": ownership.get("risk_level"),
            "ownership_status": ownership.get("ownership_status"),
            "matched_rule_ids": ownership.get("matched_rule_ids") or [],
            "reason_codes": reason_codes,
        }

    def _read_commits(self, limit: int) -> list[dict[str, Any]]:
        output = self._run_git(
            [
                "log",
                f"-n{limit}",
                "--date=iso-strict",
                "--pretty=format:%x1e%H%x1f%h%x1f%an%x1f%ae%x1f%aI%x1f%s",
                "--name-status",
            ]
        )
        commits: list[dict[str, Any]] = []
        for raw_record in output.split("\x1e"):
            record = raw_record.strip("\n")
            if not record:
                continue
            lines = [line for line in record.splitlines() if line.strip()]
            if not lines:
                continue
            header = lines[0].split("\x1f")
            if len(header) < 6:
                continue
            files = [parsed for line in lines[1:] if (parsed := self._parse_name_status_line(line)) is not None]
            commits.append(
                {
                    "commit_hash": header[0],
                    "short_hash": header[1],
                    "author_name": header[2],
                    "author_email": header[3],
                    "authored_at": header[4],
                    "subject": header[5],
                    "files": files,
                }
            )
        return commits

    @staticmethod
    def _parse_name_status_line(line: str) -> GitCommitFile | None:
        parts = line.split("\t")
        if len(parts) < 2:
            return None
        change_type = parts[0].strip()
        if change_type.startswith("R") or change_type.startswith("C"):
            if len(parts) < 3:
                return None
            return GitCommitFile(
                path=_normalize_git_path(parts[2]),
                old_path=_normalize_git_path(parts[1]),
                change_type=change_type,
            )
        return GitCommitFile(path=_normalize_git_path(parts[1]), change_type=change_type)

    @staticmethod
    def _summary(commits: list[dict[str, Any]]) -> dict[str, Any]:
        total_files = sum(int(commit.get("changed_file_count") or 0) for commit in commits)
        unmapped_commits = sum(1 for commit in commits if (commit.get("ownership_summary") or {}).get("unmapped"))
        ambiguous_commits = sum(1 for commit in commits if (commit.get("ownership_summary") or {}).get("ambiguous"))
        return {
            "commit_count": len(commits),
            "changed_file_count": total_files,
            "unmapped_commit_count": unmapped_commits,
            "ambiguous_commit_count": ambiguous_commits,
            "latest_commit": commits[0] if commits else None,
        }

    @staticmethod
    def _period_counts(commits: list[dict[str, Any]], *, period: str) -> list[dict[str, Any]]:
        counts: dict[str, int] = {}
        for commit in commits:
            parsed = _parse_datetime(commit.get("authored_at"))
            if parsed is None:
                key = "unknown"
            elif period == "month":
                key = f"{parsed.year:04d}-{parsed.month:02d}"
            elif period == "week":
                iso_year, iso_week, _ = parsed.isocalendar()
                key = f"{iso_year:04d}-W{iso_week:02d}"
            else:
                key = parsed.date().isoformat()
            counts[key] = counts.get(key, 0) + 1
        return [{"period": key, "commit_count": counts[key]} for key in sorted(counts, reverse=True)]

    def _module_counts(self, commits: list[dict[str, Any]]) -> list[dict[str, Any]]:
        module_lookup = {module.module_id: module for module in self.module_registry.list_modules()}
        buckets: dict[str, dict[str, Any]] = {}
        for commit in commits:
            commit_modules = set(commit.get("module_ids") or [])
            for file_item in commit.get("files") or []:
                module_id = file_item.get("primary_module")
                if not module_id:
                    continue
                module_def = module_lookup.get(str(module_id))
                bucket = buckets.setdefault(
                    str(module_id),
                    {
                        "module_id": module_id,
                        "display_name": module_def.display_name if module_def else str(module_id),
                        "commit_count": 0,
                        "changed_file_count": 0,
                        "latest_commit": None,
                        "max_risk_level": file_item.get("risk_level"),
                        "file_status_counts": {},
                        "required_test_plans": list(module_def.test_plans_required) if module_def else [],
                        "recommended_test_plans": list(module_def.test_plans_recommended) if module_def else [],
                    },
                )
                bucket["changed_file_count"] += 1
                change_type = str(file_item.get("change_type") or "unknown")
                bucket["file_status_counts"][change_type] = bucket["file_status_counts"].get(change_type, 0) + 1
                if _risk_rank(file_item.get("risk_level")) > _risk_rank(bucket.get("max_risk_level")):
                    bucket["max_risk_level"] = file_item.get("risk_level")
            for module_id in commit_modules:
                bucket = buckets.get(str(module_id))
                if not bucket:
                    continue
                bucket["commit_count"] += 1
                if bucket["latest_commit"] is None:
                    bucket["latest_commit"] = {
                        "commit_hash": commit.get("commit_hash"),
                        "short_hash": commit.get("short_hash"),
                        "authored_at": commit.get("authored_at"),
                        "subject": commit.get("subject"),
                    }
        return sorted(buckets.values(), key=lambda item: (-int(item["commit_count"]), str(item["module_id"])))

    def _run_git(self, args: list[str]) -> str:
        if not self._is_allowlisted_git_command(args):
            raise GitActivityProviderError(f"git command is not allowlisted: {json.dumps(args)}")
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
                timeout=20,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise GitActivityProviderError(f"git command failed: {type(exc).__name__}: {exc}") from exc
        if completed.returncode != 0:
            raise GitActivityProviderError(
                f"git command {json.dumps(args)} failed: {(completed.stderr or '').strip()}"
            )
        return completed.stdout or ""

    @staticmethod
    def _is_allowlisted_git_command(args: list[str]) -> bool:
        if len(args) != 5:
            return False
        if args[0] != "log":
            return False
        if not args[1].startswith("-n") or not args[1][2:].isdigit():
            return False
        return args[2:] == [
            "--date=iso-strict",
            "--pretty=format:%x1e%H%x1f%h%x1f%an%x1f%ae%x1f%aI%x1f%s",
            "--name-status",
        ]


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value)
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
