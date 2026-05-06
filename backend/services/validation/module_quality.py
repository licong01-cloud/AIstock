from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.services.validation.file_ownership import FileOwnershipCatalog
from backend.services.validation.finding_store import ValidationFindingStore
from backend.services.validation.git_activity_provider import GitCommitActivityProvider
from backend.services.validation.git_status_provider import GitWorkspaceStatusProvider
from backend.services.validation.history_store import ValidationHistoryStore
from backend.services.validation.module_registry import ModuleRegistry, REPO_ROOT


MODULE_QUALITY_SCHEMA = "aistock_validation_module_quality_v1"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _display_repo_root(path: Path) -> str:
    try:
        return str(path.resolve()).replace("\\", "/")
    except OSError:
        return str(path).replace("\\", "/")


def _risk_rank(risk_level: str | None) -> int:
    return {"low": 1, "medium": 2, "high": 3, "critical": 4}.get(str(risk_level or ""), 0)


def _severity_rank(severity: str | None) -> int:
    return {"P0": 5, "P1": 4, "P2": 3, "P3": 2}.get(str(severity or "").upper(), 1)


class ModuleQualityService:
    """Build a read-only module quality cockpit from Git, validation, and ownership evidence."""

    def __init__(
        self,
        *,
        repo_root: Path | None = None,
        module_registry: ModuleRegistry | None = None,
        file_ownership_catalog: FileOwnershipCatalog | None = None,
        git_status_provider: GitWorkspaceStatusProvider | None = None,
        git_activity_provider: GitCommitActivityProvider | None = None,
        history_store: ValidationHistoryStore | None = None,
        finding_store: ValidationFindingStore | None = None,
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
        self.git_activity_provider = git_activity_provider or GitCommitActivityProvider(
            repo_root=self.repo_root,
            module_registry=self.module_registry,
            file_ownership_catalog=self.file_ownership_catalog,
            git_status_provider=self.git_status_provider,
        )
        self.history_store = history_store or ValidationHistoryStore(repo_root=self.repo_root)
        self.finding_store = finding_store or ValidationFindingStore(repo_root=self.repo_root)

    def module_quality_summary(self, *, commit_limit: int = 50) -> dict[str, Any]:
        modules = self.module_registry.list_modules()
        module_ids = {module.module_id for module in modules}
        aliases = self._module_aliases(module_ids)
        buckets = {
            module.module_id: {
                "module_id": module.module_id,
                "display_name": module.display_name,
                "parent_module": module.parent_module,
                "module_type": module.module_type,
                "registry_risk_level": module.risk_level,
                "description": module.description,
                "description_zh": module.description_zh,
                "ui_routes": list(module.ui_routes),
                "api_routes": list(module.api_routes),
                "test_plans": {
                    "required_on_change": list(module.test_plans_required),
                    "recommended": list(module.test_plans_recommended),
                },
                "workspace": {
                    "changed_file_count": 0,
                    "staged_file_count": 0,
                    "unstaged_file_count": 0,
                    "untracked_file_count": 0,
                    "max_risk_level": None,
                    "files": [],
                },
                "commits": {
                    "commit_count": 0,
                    "changed_file_count": 0,
                    "latest_commit": None,
                    "max_risk_level": None,
                },
                "coverage": {
                    "snapshot_id": None,
                    "status": "missing",
                    "line_percent": None,
                    "branch_percent": None,
                    "generated_at": None,
                },
                "quality": {
                    "finding_count": 0,
                    "bug_count": 0,
                    "by_severity": {},
                    "by_status": {},
                },
                "priority": {
                    "score": 0,
                    "level": "low",
                    "reason_codes": [],
                },
            }
            for module in modules
        }

        workspace = self.git_status_provider.workspace_status()
        commit_activity = self.git_activity_provider.commit_activity(limit=commit_limit)
        self._apply_workspace(buckets, workspace)
        self._apply_commit_activity(buckets, commit_activity)
        self._apply_coverage(buckets, aliases)
        self._apply_findings_and_bugs(buckets, aliases)
        for bucket in buckets.values():
            bucket["priority"] = self._priority(bucket)
        module_items = sorted(
            buckets.values(),
            key=lambda item: (-int(item["priority"]["score"]), str(item["module_id"])),
        )
        summary = {
            "module_count": len(module_items),
            "modules_with_workspace_changes": sum(1 for item in module_items if item["workspace"]["changed_file_count"]),
            "modules_with_recent_commits": sum(1 for item in module_items if item["commits"]["commit_count"]),
            "modules_needing_validation": sum(1 for item in module_items if item["priority"]["level"] in {"medium", "high", "critical"}),
            "unmapped_workspace_files": (workspace.get("summary") or {}).get("unmapped_files", 0),
            "ambiguous_workspace_files": (workspace.get("summary") or {}).get("ambiguous_files", 0),
            "recent_commit_count": (commit_activity.get("summary") or {}).get("commit_count", 0),
        }
        return {
            "schema_version": MODULE_QUALITY_SCHEMA,
            "generated_at": _now_iso(),
            "repo_root": _display_repo_root(self.repo_root),
            "summary": summary,
            "modules": module_items,
            "workspace_summary": workspace.get("summary") or {},
            "commit_summary": commit_activity.get("summary") or {},
            "global_reason_codes": self._global_reason_codes(summary),
            "git_command_mode": "read_only_allowlist",
            "arbitrary_shell_allowed": False,
            "production_8001_touched": False,
        }

    def _apply_workspace(self, buckets: dict[str, dict[str, Any]], workspace: dict[str, Any]) -> None:
        for item in workspace.get("files") or []:
            module_id = item.get("primary_module")
            if not module_id or module_id not in buckets:
                continue
            bucket = buckets[str(module_id)]["workspace"]
            bucket["changed_file_count"] += 1
            if item.get("staged"):
                bucket["staged_file_count"] += 1
            if item.get("unstaged"):
                bucket["unstaged_file_count"] += 1
            if item.get("untracked"):
                bucket["untracked_file_count"] += 1
            if _risk_rank(item.get("risk_level")) > _risk_rank(bucket.get("max_risk_level")):
                bucket["max_risk_level"] = item.get("risk_level")
            if len(bucket["files"]) < 20:
                bucket["files"].append(
                    {
                        "path": item.get("path"),
                        "status": item.get("status"),
                        "risk_level": item.get("risk_level"),
                        "ownership_status": item.get("ownership_status"),
                    }
                )

    @staticmethod
    def _apply_commit_activity(buckets: dict[str, dict[str, Any]], commit_activity: dict[str, Any]) -> None:
        for item in commit_activity.get("by_module") or []:
            module_id = item.get("module_id")
            if not module_id or module_id not in buckets:
                continue
            bucket = buckets[str(module_id)]["commits"]
            bucket["commit_count"] = item.get("commit_count") or 0
            bucket["changed_file_count"] = item.get("changed_file_count") or 0
            bucket["latest_commit"] = item.get("latest_commit")
            bucket["max_risk_level"] = item.get("max_risk_level")

    def _apply_coverage(self, buckets: dict[str, dict[str, Any]], aliases: dict[str, str]) -> None:
        for item in self.history_store.list_coverage_snapshots(limit=10000)["items"]:
            module_id = self._canonical_module(item.get("module"), aliases)
            if not module_id or module_id not in buckets:
                continue
            current = buckets[module_id]["coverage"]
            if current.get("generated_at") and str(current["generated_at"]) >= str(item.get("generated_at") or ""):
                continue
            totals = item.get("totals") or {}
            buckets[module_id]["coverage"] = {
                "snapshot_id": item.get("snapshot_id"),
                "status": item.get("status"),
                "line_percent": totals.get("line_percent"),
                "branch_percent": totals.get("branch_percent"),
                "generated_at": item.get("generated_at"),
            }

    def _apply_findings_and_bugs(self, buckets: dict[str, dict[str, Any]], aliases: dict[str, str]) -> None:
        for item in self.finding_store.list_findings(page_size=10000)["items"]:
            module_id = self._module_for_finding(item, aliases)
            if not module_id or module_id not in buckets:
                continue
            quality = buckets[module_id]["quality"]
            quality["finding_count"] += 1
            severity = str(item.get("severity") or "unknown")
            status = str(item.get("status") or "unknown")
            quality["by_severity"][severity] = quality["by_severity"].get(severity, 0) + 1
            quality["by_status"][status] = quality["by_status"].get(status, 0) + 1
        for item in self.finding_store.list_bugs(page_size=10000)["items"]:
            module_id = self._canonical_module(item.get("module"), aliases)
            if not module_id or module_id not in buckets:
                continue
            quality = buckets[module_id]["quality"]
            quality["bug_count"] += 1
            severity = str(item.get("severity") or "unknown")
            status = str(item.get("status") or "unknown")
            quality["by_severity"][severity] = quality["by_severity"].get(severity, 0) + 1
            quality["by_status"][status] = quality["by_status"].get(status, 0) + 1

    def _module_for_finding(self, item: dict[str, Any], aliases: dict[str, str]) -> str | None:
        file_path = item.get("file_path")
        if file_path:
            ownership = self.file_ownership_catalog.match_path(str(file_path))
            if ownership.primary_module:
                return ownership.primary_module
        return self._canonical_module(item.get("module"), aliases)

    @staticmethod
    def _priority(bucket: dict[str, Any]) -> dict[str, Any]:
        score = _risk_rank(bucket.get("registry_risk_level"))
        reasons: list[str] = []
        workspace_count = int(bucket["workspace"]["changed_file_count"] or 0)
        commit_count = int(bucket["commits"]["commit_count"] or 0)
        finding_count = int(bucket["quality"]["finding_count"] or 0)
        bug_count = int(bucket["quality"]["bug_count"] or 0)
        if workspace_count:
            score += min(25, workspace_count * 3)
            reasons.append("workspace_changed")
        if commit_count:
            score += min(15, commit_count * 2)
            reasons.append("recent_commits")
        if finding_count:
            severity_bonus = sum(
                _severity_rank(severity) * count
                for severity, count in (bucket["quality"].get("by_severity") or {}).items()
            )
            score += min(35, severity_bonus)
            reasons.append("quality_findings")
        if bug_count:
            score += min(30, bug_count * 8)
            reasons.append("open_bugs")
        if workspace_count and bucket["coverage"].get("status") in {None, "missing", "failed"}:
            score += 12
            reasons.append("changed_without_passing_coverage")
        if _risk_rank(bucket["workspace"].get("max_risk_level")) >= _risk_rank("high"):
            score += 10
            reasons.append("high_risk_workspace_files")
        if score >= 45:
            level = "critical"
        elif score >= 30:
            level = "high"
        elif score >= 15:
            level = "medium"
        else:
            level = "low"
        return {"score": score, "level": level, "reason_codes": reasons}

    @staticmethod
    def _global_reason_codes(summary: dict[str, Any]) -> list[str]:
        reasons: list[str] = []
        if summary.get("unmapped_workspace_files"):
            reasons.append("unmapped_workspace_files_present")
        if summary.get("ambiguous_workspace_files"):
            reasons.append("ambiguous_workspace_files_present")
        if summary.get("modules_needing_validation"):
            reasons.append("modules_need_validation")
        return reasons

    @staticmethod
    def _module_aliases(module_ids: set[str]) -> dict[str, str]:
        aliases: dict[str, str] = {}
        for module_id in module_ids:
            aliases[module_id.lower()] = module_id
            aliases[module_id.replace(".", "_").lower()] = module_id
            aliases[module_id.replace("_", ".").lower()] = module_id
        aliases["validation_center"] = "validation.center" if "validation.center" in module_ids else aliases.get("validation_center", "validation_center")
        return aliases

    @staticmethod
    def _canonical_module(value: Any, aliases: dict[str, str]) -> str | None:
        if value is None:
            return None
        return aliases.get(str(value).strip().lower())
