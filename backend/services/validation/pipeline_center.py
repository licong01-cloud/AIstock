from __future__ import annotations

import json
import os
import re
import subprocess
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.services.validation.execution_runner import ValidationExecutionRunner
from backend.services.validation.file_ownership import FileOwnershipCatalog
from backend.services.validation.finding_store import ValidationFindingStore
from backend.services.validation.git_status_provider import GitWorkspaceStatusProvider
from backend.services.validation.history_store import ValidationHistoryStore
from backend.services.validation.module_quality import ModuleQualityService
from backend.services.validation.module_registry import REPO_ROOT, ModuleRegistry
from backend.services.validation.platform_health import ValidationPlatformHealthService
from backend.services.validation.plan_catalog import ValidationPlanCatalog
from backend.services.validation.ui_target_catalog import ValidationUiTargetCatalog


CARDS_SCHEMA = "aistock_validation_cards_v2"
MERGE_GATE_SCHEMA = "aistock_merge_gate_v1"
ISSUE_WORKFLOW_SCHEMA = "aistock_issue_workflow_v1"
MODULE_DETAIL_SCHEMA = "aistock_module_detail_summary_v1"
PIPELINE_TEST_SCHEMA = "aistock_pipeline_tests_v1"
FEATURE_SCHEMA = "aistock_feature_validation_v1"
GITHUB_ISSUE_SCHEMA = "aistock_github_issue_sync_v1"
BRANCH_DETAIL_SCHEMA = "aistock_git_branch_detail_v1"
PR_SCHEMA = "aistock_github_prs_v1"
LEGACY_DEBT_SCHEMA = "aistock_legacy_debt_v1"
AUTOMATION_SCHEMA = "aistock_validation_automation_v1"
CANDIDATE_QUEUE_SCHEMA = "aistock_validation_candidate_queue_v1"
LOCAL_GITHUB_ENV_FILE = ".env.github-issues-local"

HIGH_CONFLICT_PATHS = (
    "backend/main.py",
    "backend/db/",
    "backend/migrations/",
    "backend/services/quantevolver/config_composer.py",
    "backend/services/strategy_package/",
    "backend/services/paper_trading_v2/",
    "frontend/next.config.mjs",
    "noxfile.py",
    ".github/workflows/",
    "docs/standards/",
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _page(items: list[dict[str, Any]], *, page: int, page_size: int) -> dict[str, Any]:
    start = (page - 1) * page_size
    end = start + page_size
    return {
        "items": items[start:end],
        "total": len(items),
        "page": page,
        "page_size": page_size,
        "has_more": end < len(items),
    }


def _repo_display(path: Path) -> str:
    try:
        return str(path.resolve()).replace("\\", "/")
    except OSError:
        return str(path).replace("\\", "/")


def _github_repo_from_remote_url(url: str) -> str | None:
    raw = url.strip()
    if not raw:
        return None
    patterns = (
        r"^git@github\.com:([^/]+)/(.+?)(?:\.git)?$",
        r"^ssh://git@github\.com/([^/]+)/(.+?)(?:\.git)?$",
        r"^https://github\.com/([^/]+)/(.+?)(?:\.git)?/?$",
        r"^http://github\.com/([^/]+)/(.+?)(?:\.git)?/?$",
    )
    for pattern in patterns:
        match = re.match(pattern, raw, flags=re.I)
        if match:
            owner, name = match.group(1), match.group(2)
            name = name[:-4] if name.endswith(".git") else name
            if owner and name and "/" not in name:
                return f"{owner}/{name}"
    return None


def _norm_path(value: Any) -> str:
    return str(value or "").replace("\\", "/").lstrip("./")


def _workflow_state(status: Any) -> str:
    raw = str(status or "open").strip().lower().replace("-", "_")
    return {
        "detected": "open",
        "new": "open",
        "open": "open",
        "triaged": "triaged",
        "assigned": "in_progress",
        "in_progress": "in_progress",
        "review_ready": "review_ready",
        "submitted": "fixed",
        "fixed": "fixed",
        "verified": "verified",
        "closed": "closed",
    }.get(raw, "open")


def _is_open_bug(bug: dict[str, Any]) -> bool:
    return _workflow_state(bug.get("status")) not in {"fixed", "verified", "closed"}


def _severity_rank(value: Any) -> int:
    return {"P0": 4, "P1": 3, "P2": 2, "P3": 1}.get(str(value or "").upper(), 0)


def _risk_rank(value: Any) -> int:
    return {"critical": 4, "high": 3, "medium": 2, "low": 1}.get(str(value or "").lower(), 0)


def _tone(score: int | None) -> str:
    if score is None:
        return "gray"
    if score >= 70:
        return "red"
    if score >= 40:
        return "orange"
    if score >= 20:
        return "yellow"
    return "green"


class ValidationPipelineCenterService:
    """Read-only phase-1 aggregation layer for Validation Center pages."""

    def __init__(
        self,
        *,
        repo_root: Path | None = None,
        history_store: ValidationHistoryStore | None = None,
        plan_catalog: ValidationPlanCatalog | None = None,
        finding_store: ValidationFindingStore | None = None,
        execution_runner: ValidationExecutionRunner | None = None,
        git_status_provider: GitWorkspaceStatusProvider | None = None,
        module_quality_service: ModuleQualityService | None = None,
        ui_target_catalog: ValidationUiTargetCatalog | None = None,
        module_registry: ModuleRegistry | None = None,
        file_ownership_catalog: FileOwnershipCatalog | None = None,
    ) -> None:
        self.repo_root = Path(repo_root or REPO_ROOT)
        self.history_store = history_store or ValidationHistoryStore(repo_root=self.repo_root)
        self.plan_catalog = plan_catalog or ValidationPlanCatalog()
        self.finding_store = finding_store or ValidationFindingStore(repo_root=self.repo_root)
        self.execution_runner = execution_runner or ValidationExecutionRunner()
        self.module_registry = module_registry or ModuleRegistry()
        self.file_ownership_catalog = file_ownership_catalog or FileOwnershipCatalog(module_registry=self.module_registry)
        self.git_status_provider = git_status_provider or GitWorkspaceStatusProvider(
            repo_root=self.repo_root,
            file_ownership_catalog=self.file_ownership_catalog,
        )
        self.module_quality_service = module_quality_service or ModuleQualityService(
            repo_root=self.repo_root,
            module_registry=self.module_registry,
            file_ownership_catalog=self.file_ownership_catalog,
            git_status_provider=self.git_status_provider,
            history_store=self.history_store,
            finding_store=self.finding_store,
        )
        self.ui_target_catalog = ui_target_catalog or ValidationUiTargetCatalog()

    def cards_summary(self) -> dict[str, Any]:
        merge_gate = self.merge_gate_summary()
        issue_summary = self.issue_workflow_summary()
        module_summary = self.modules_detail_summary()
        pipeline_summary = self.pipeline_tests_summary()
        feature_summary = self.features_summary()
        github_summary = self.github_issues_summary()
        branch_summary = self.git_branches_detail_summary()
        legacy_summary = self.legacy_debt_summary()
        automation = self.automation_summary()
        cards = [
            self._card("merge_gate", "合入门禁", "#merge-gate", self._gate_tone(merge_gate["decision"]), merge_gate["risk_score"], merge_gate),
            self._card("issue_workflow", "Issue 修复流程", "#issue-workflow", "yellow" if issue_summary["missing_scope_count"] else "green", issue_summary["missing_scope_count"] * 15, issue_summary),
            self._card("pipeline_tests", "流水线测试", "#pipeline-tests", "red" if pipeline_summary["failed_count"] else "green", pipeline_summary["failed_count"] * 20 + pipeline_summary["missing_evidence_count"] * 5, pipeline_summary),
            self._card("features", "功能验证", "#features", "yellow" if feature_summary["targets_requiring_action"] else "green", feature_summary["targets_requiring_action"] * 6, feature_summary),
            self._card("modules", "模块质量", "#modules", _tone(module_summary["summary"]["max_risk_score"]), module_summary["summary"]["max_risk_score"], module_summary["summary"]),
            self._card("github_issues", "GitHub 议题", "#github-issues", "yellow" if github_summary["missing_link_count"] else "green", github_summary["missing_link_count"] * 5, github_summary),
            self._card("branches_prs", "分支与 PR", "#branches-prs", "orange" if branch_summary["worktree_count"] > 20 else "green", min(100, branch_summary["worktree_count"]), branch_summary),
            self._card("legacy_debt", "历史遗留问题", "#legacy-debt", "yellow" if legacy_summary["debt_count"] else "green", min(100, legacy_summary["p0_p1_count"] * 18 + legacy_summary["debt_count"] // 10), legacy_summary),
            self._card("automation", "MCP 自动化", "#automation", "green" if automation["gh_auth_status"] == "ok" else "yellow", 8 if automation["gh_auth_status"] == "ok" else 35, automation["summary"]),
        ]
        branch = self._safe_branch_status()
        return {
            "schema_version": CARDS_SCHEMA,
            "generated_at": _now_iso(),
            "repo": {
                "root": _repo_display(self.repo_root),
                "current_branch": branch.get("branch"),
                "head_commit": branch.get("head_commit"),
                "target_branch": "main",
            },
            "cards": cards,
            "data_state": "complete",
            "production_8001_touched": False,
        }

    def merge_gate_summary(self, *, branch: str | None = None, target: str = "main") -> dict[str, Any]:
        branch_status = self._safe_branch_status()
        workspace = self._safe_workspace_status()
        changed_files = self._changed_files(target)
        dirty_files = [_norm_path(item.get("path")) for item in workspace.get("files") or []]
        files = sorted({path for path in [*changed_files, *dirty_files] if path})
        touched_modules = self._modules_for_paths(files)
        checks: list[dict[str, Any]] = []
        blocking: list[str] = []
        warnings: list[str] = []
        manual = ["merge_to_main_requires_user_confirmation"]

        dirty = bool(workspace.get("dirty"))
        checks.append(self._check("workspace_clean", "工作区干净", "blocking", not dirty, "workspace_dirty"))
        if dirty:
            blocking.append("workspace_dirty")

        high_conflict = [path for path in files if self._is_high_conflict(path)]
        integrator_ok = not high_conflict or str(branch_status.get("branch") or "").startswith(("codex/", "integrator/"))
        checks.append(self._check("high_conflict_integrator", "高冲突文件有集成责任方", "warning", integrator_ok, "high_conflict_file_requires_integrator", {"files": high_conflict[:20]}))
        if not integrator_ok:
            manual.append("high_conflict_file_requires_integrator_confirmation")
            warnings.append("high_conflict_file_requires_integrator")

        linked_bug = self._linked_bug(branch or str(branch_status.get("branch") or ""))
        open_p0_p1 = self._open_p0_p1_for_modules(touched_modules)
        linked_ok = not open_p0_p1 or linked_bug is not None
        checks.append(self._check("linked_bug", "P0/P1 触达修复绑定 BUG", "blocking", linked_ok, "missing_linked_bug"))
        if not linked_ok:
            blocking.append("missing_linked_bug")

        scope_ok, scope_violations = self._scope_check(linked_bug, files)
        scope_level = "blocking" if linked_bug and str(linked_bug.get("severity") or "").upper() in {"P0", "P1"} else "warning"
        checks.append(self._check("allowed_write_scope", "写入范围符合 Issue scope", scope_level, scope_ok, "scope_violation", {"violations": scope_violations[:20]}))
        if not scope_ok:
            (blocking if scope_level == "blocking" else warnings).append("scope_violation")

        module_detail = self.modules_detail_summary(changed_files=files)
        coverage_blockers = [
            item["module_id"]
            for item in module_detail["modules"]
            if item["touched_by_current_branch"]
            and item["coverage"]["coverage_state"] in {"missing", "stale", "failed"}
            and item["coverage_threshold"]["strict_for_merge"]
        ]
        checks.append(self._check("touched_module_coverage", "触达模块覆盖率有效", "blocking", not coverage_blockers, "touched_module_coverage_missing_or_stale", {"modules": coverage_blockers}))
        if coverage_blockers:
            blocking.append("touched_module_coverage_missing_or_stale")

        historical_count = self._historical_p2_p3_count(touched_modules)
        if historical_count:
            warnings.append("historical_p2_p3_debt_exists")
        checks.append(self._check("historical_baseline", "历史基线默认不阻塞", "warning", True, None, {"historical_p2_p3_count": historical_count}))

        if self.github_issues_summary()["missing_link_count"]:
            warnings.append("github_issue_link_gaps_exist")

        risk_score = min(100, len(blocking) * 30 + len(warnings) * 8 + len(manual) * 5)
        if blocking:
            decision, label = "blocked", "暂不建议合入"
        elif warnings or manual:
            decision, label = "warning", "可人工确认后合入"
        else:
            decision, label = "pass", "可合入"
        return {
            "schema_version": MERGE_GATE_SCHEMA,
            "generated_at": _now_iso(),
            "decision": decision,
            "decision_label": label,
            "source_branch": branch or branch_status.get("branch"),
            "target_branch": target,
            "head_commit": branch_status.get("head_commit"),
            "base_commit": self._git_one(["rev-parse", self._target_ref(target)], default=None),
            "change_class": self._change_class(files),
            "changed_files": files,
            "touched_modules": sorted(touched_modules),
            "checks": checks,
            "blocking_reasons": sorted(set(blocking)),
            "warnings": sorted(set(warnings)),
            "manual_confirmations": sorted(set(manual)),
            "recommended_next_actions": self._merge_next_actions(blocking, warnings),
            "evidence_bundles": self._evidence_bundle_ids(touched_modules),
            "risk_score": risk_score,
            "health_tone": self._gate_tone(decision),
            "data_state": branch_status.get("data_state", "complete"),
            "production_8001_touched": False,
        }

    def merge_gate_detail(self, *, branch: str | None = None, target: str = "main") -> dict[str, Any]:
        summary = self.merge_gate_summary(branch=branch, target=target)
        summary["detail"] = {
            "workspace": self._safe_workspace_status(),
            "branch": self._safe_branch_status(),
            "modules": self.modules_detail_summary(changed_files=summary["changed_files"]),
            "issue_workflow": self.issue_workflow_summary(),
            "github_issues": self.github_issues_summary(),
            "legacy_debt": self.legacy_debt_summary(),
        }
        return summary

    def issue_workflow_summary(self) -> dict[str, Any]:
        items = self.issue_workflow_items(page=1, page_size=10000)["items"]
        by_state = Counter(item["workflow_state"] for item in items)
        missing_scope = sum(1 for item in items if item["allowed_write_scope_state"] != "complete")
        missing_verification = sum(1 for item in items if item["required_verification_state"] in {"missing", "pending"})
        reasons = []
        if missing_scope:
            reasons.append("issues_missing_allowed_write_scope")
        if missing_verification:
            reasons.append("issues_missing_required_verification")
        return {
            "schema_version": ISSUE_WORKFLOW_SCHEMA,
            "generated_at": _now_iso(),
            "open_count": by_state.get("open", 0),
            "triaged_count": by_state.get("triaged", 0),
            "triage_only_count": sum(1 for item in items if item["gate_state"] == "triage_only_until_allowed_write_scope_is_set"),
            "in_progress_count": by_state.get("in_progress", 0),
            "review_ready_count": by_state.get("review_ready", 0),
            "fixed_count": by_state.get("fixed", 0),
            "verified_count": by_state.get("verified", 0),
            "closed_count": by_state.get("closed", 0),
            "missing_scope_count": missing_scope,
            "missing_required_verification_count": missing_verification,
            "by_workflow_state": dict(sorted(by_state.items())),
            "reason_codes": reasons,
            "data_state": "complete",
            "production_8001_touched": False,
        }

    def issue_workflow_items(
        self,
        *,
        module: str | None = None,
        severity: str | None = None,
        workflow_state: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        bugs = self.finding_store.list_bugs(module=module, severity=severity, page_size=10000)["items"]
        items = [self._issue_item(bug) for bug in bugs]
        if workflow_state:
            wanted = workflow_state.lower()
            items = [item for item in items if item["workflow_state"] == wanted]
        items.sort(key=lambda item: (_severity_rank(item["severity"]), item["bug_id"]), reverse=True)
        payload = _page(items, page=page, page_size=page_size)
        payload.update({"schema_version": ISSUE_WORKFLOW_SCHEMA, "generated_at": _now_iso(), "data_state": "complete"})
        return payload

    def issue_workflow_detail(self, bug_id: str) -> dict[str, Any] | None:
        bug = self.finding_store.get_bug(bug_id)
        if bug is None:
            return None
        item = self._issue_item(bug)
        item["detail"] = bug
        item["linked_entities"] = {
            "issues": [bug.get("github_issue_url")] if bug.get("github_issue_url") else [],
            "prs": [],
            "branches": [bug.get("fix_branch")] if bug.get("fix_branch") else [],
            "files": bug.get("allowed_write_scope") or [],
            "tests": bug.get("required_verification") or [],
            "evidence_bundles": bug.get("evidence_uris") or [],
        }
        return item

    def issue_candidate_summary(self) -> dict[str, Any]:
        items = self.issue_candidates(page=1, page_size=10000)["items"]
        outcome = self._candidate_outcome_metrics(items)
        by_status = Counter(str(item.get("status") or "unknown") for item in items)
        by_module = Counter(str(item.get("module_id") or "unknown") for item in items)
        by_severity = Counter(str(item.get("severity") or "unknown") for item in items)
        by_source_type = Counter(str(item.get("source_type") or "unknown") for item in items)
        by_quality_gate = Counter(str(item.get("quality_gate_state") or "unknown") for item in items)
        no_submit_reason_counts: Counter[str] = Counter()
        for item in items:
            no_submit_reason_counts.update(self._candidate_string_list(item.get("no_submit_reasons")))
        linked_issue_count = sum(1 for item in items if item.get("github_issue_url") or item.get("github_issue_number"))
        issue_payload_ready_count = sum(1 for item in items if item.get("issue_payload_ready") is True)
        open_count = sum(1 for item in items if str(item.get("status") or "").lower() not in {"ignored", "promoted", "closed"})
        reason_codes: list[str] = []
        if not items:
            reason_codes.append("candidate_queue_empty")
        elif issue_payload_ready_count == 0:
            reason_codes.append("no_issue_ready_candidate")
        if len(items) - linked_issue_count > 0:
            reason_codes.append("missing_github_issue_links")
        if outcome["confirmed_issue_count"]:
            reason_codes.append("confirmed_nightly_issue")
        elif items:
            reason_codes.append("no_confirmed_nightly_issue_yet")
        return {
            "schema_version": CANDIDATE_QUEUE_SCHEMA,
            "generated_at": _now_iso(),
            "candidate_count": len(items),
            "open_count": open_count,
            "linked_issue_count": linked_issue_count,
            "missing_issue_link_count": max(0, len(items) - linked_issue_count),
            "nightly_candidate_count": sum(1 for item in items if str(item.get("source_type") or "").startswith("nightly_")),
            "issue_payload_ready_count": issue_payload_ready_count,
            "draft_count": sum(1 for item in items if str(item.get("status") or "").lower() == "draft"),
            "deduped_count": sum(1 for item in items if str(item.get("status") or "").lower() == "deduped"),
            "artifact_only_count": sum(1 for item in items if str(item.get("status") or "").lower() == "artifact_only"),
            "by_status": dict(sorted(by_status.items())),
            "by_module": dict(sorted(by_module.items())),
            "by_severity": dict(sorted(by_severity.items())),
            "by_source_type": dict(sorted(by_source_type.items())),
            "by_quality_gate": dict(sorted(by_quality_gate.items())),
            "no_submit_reason_counts": dict(sorted(no_submit_reason_counts.items())),
            "outcome_metrics": outcome,
            "reason_codes": reason_codes,
            "data_state": "complete",
            "production_8001_touched": False,
        }

    def issue_candidates(
        self,
        *,
        module: str | None = None,
        severity: str | None = None,
        status: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        items = self._candidate_queue_items()
        if module:
            wanted = module.lower()
            items = [item for item in items if str(item.get("module_id") or "").lower() == wanted]
        if severity:
            wanted = severity.upper()
            items = [item for item in items if str(item.get("severity") or "").upper() == wanted]
        if status:
            wanted = status.lower()
            items = [item for item in items if str(item.get("status") or "").lower() == wanted]
        items.sort(
            key=lambda item: (
                _severity_rank(item.get("severity")),
                str(item.get("last_seen_at") or item.get("created_at") or ""),
                str(item.get("candidate_id") or ""),
            ),
            reverse=True,
        )
        payload = _page(items, page=page, page_size=page_size)
        payload.update({"schema_version": CANDIDATE_QUEUE_SCHEMA, "generated_at": _now_iso(), "data_state": "complete"})
        return payload

    def modules_detail_summary(self, *, changed_files: list[str] | None = None) -> dict[str, Any]:
        base = self.module_quality_service.module_quality_summary(commit_limit=80)
        changed = changed_files if changed_files is not None else self._changed_files("main")
        touched = self._modules_for_paths(changed)
        path_index = self._module_path_index()
        bugs_by_module = self._bugs_by_module()
        findings_by_module = self._findings_by_module()
        modules: list[dict[str, Any]] = []
        for item in base.get("modules") or []:
            module_id = str(item.get("module_id"))
            bugs = bugs_by_module.get(module_id, [])
            findings = findings_by_module.get(module_id, [])
            p0_p1 = sum(1 for bug in bugs if _is_open_bug(bug) and str(bug.get("severity") or "").upper() in {"P0", "P1"})
            coverage = dict(item.get("coverage") or {})
            coverage_state = self._coverage_state(coverage, module_id in touched)
            coverage["coverage_state"] = coverage_state
            coverage["stale_reason"] = self._coverage_reason(coverage_state, module_id in touched)
            threshold = self._coverage_threshold(item)
            risk_score = self._module_risk(item, bugs, findings, coverage_state, module_id in touched)
            modules.append(
                {
                    **item,
                    "owned_paths": path_index.get(module_id, {}).get("owned_paths", []),
                    "shared_paths": path_index.get(module_id, {}).get("shared_paths", []),
                    "coverage": coverage,
                    "coverage_threshold": threshold,
                    "touched_by_current_branch": module_id in touched,
                    "merge_gate_state": self._module_gate_state(coverage_state, p0_p1, threshold, module_id in touched),
                    "blocking_issue_count_for_current_branch": p0_p1 if module_id in touched else 0,
                    "historical_issue_count": len([bug for bug in bugs if _is_open_bug(bug)]) + len(findings),
                    "issues": bugs[:10],
                    "findings": findings[:10],
                    "risk_score": risk_score,
                    "health_tone": _tone(risk_score),
                    "reason_codes": self._module_reasons(coverage_state, p0_p1, module_id in touched),
                }
            )
        modules.sort(key=lambda item: (-int(item["risk_score"]), str(item["module_id"])))
        summary = {
            **(base.get("summary") or {}),
            "touched_module_count": len(touched),
            "max_risk_score": max((int(item["risk_score"]) for item in modules), default=0),
            "blocking_module_count": sum(1 for item in modules if item["merge_gate_state"] == "blocked"),
            "warning_module_count": sum(1 for item in modules if item["merge_gate_state"] == "warning"),
        }
        return {
            "schema_version": MODULE_DETAIL_SCHEMA,
            "generated_at": _now_iso(),
            "repo_root": _repo_display(self.repo_root),
            "summary": summary,
            "modules": modules,
            "changed_files": changed,
            "global_reason_codes": base.get("global_reason_codes") or [],
            "data_state": "complete",
            "production_8001_touched": False,
        }

    def pipeline_tests_summary(self) -> dict[str, Any]:
        items = self.pipeline_tests(page=1, page_size=10000)["items"]
        failed = sum(1 for item in items if item["status"] in {"failed", "error"})
        missing = sum(1 for item in items if not item.get("evidence_bundle_id"))
        return {
            "schema_version": PIPELINE_TEST_SCHEMA,
            "generated_at": _now_iso(),
            "test_count": len(items),
            "blocking_count": sum(1 for item in items if item["test_level"] == "blocking"),
            "failed_count": failed,
            "missing_evidence_count": missing,
            "by_status": dict(Counter(item["status"] for item in items)),
            "reason_codes": ["pipeline_test_failures"] if failed else [],
            "data_state": "complete",
            "production_8001_touched": False,
        }

    def pipeline_tests(self, *, page: int = 1, page_size: int = 20) -> dict[str, Any]:
        plans = self.plan_catalog.load().get("plans") or []
        jobs = self.execution_runner.list_jobs(page_size=10000).get("items") or []
        runs = self.history_store.list_runs(page_size=10000).get("items") or []
        items = [self._test_item(plan, jobs, runs) for plan in plans]
        payload = _page(items, page=page, page_size=page_size)
        payload.update({"schema_version": PIPELINE_TEST_SCHEMA, "generated_at": _now_iso(), "data_state": "complete"})
        return payload

    def pipeline_test_detail(self, test_id: str) -> dict[str, Any] | None:
        for item in self.pipeline_tests(page=1, page_size=10000)["items"]:
            if item["test_id"] == test_id:
                item["recent_runs"] = self.history_store.list_runs(module=item.get("module"), level=item.get("level"), page_size=10).get("items") or []
                return item
        return None

    def features_summary(self) -> dict[str, Any]:
        summary = self.ui_target_catalog.summary()
        return {
            "schema_version": FEATURE_SCHEMA,
            "generated_at": _now_iso(),
            "target_count": summary.get("target_count", 0),
            "nav_group_count": summary.get("nav_group_count", 0),
            "warning_count": summary.get("warning_count", 0),
            "targets_requiring_action": summary.get("targets_requiring_action", 0),
            "by_nav_group": summary.get("by_nav_group") or [],
            "by_coverage_status": summary.get("by_coverage_status") or {},
            "by_risk_level": summary.get("by_risk_level") or {},
            "reason_codes": ["feature_validation_warnings"] if summary.get("warning_count") else [],
            "data_state": "complete",
            "production_8001_touched": False,
        }

    def features(self, *, page: int = 1, page_size: int = 20) -> dict[str, Any]:
        payload = dict(self.ui_target_catalog.list_targets(page=page, page_size=page_size))
        payload.update({"schema_version": FEATURE_SCHEMA, "generated_at": _now_iso(), "data_state": "complete"})
        return payload

    def feature_detail(self, route_id: str) -> dict[str, Any] | None:
        target = self.ui_target_catalog.get_target(route_id)
        if target is None:
            return None
        return {"schema_version": FEATURE_SCHEMA, "generated_at": _now_iso(), "target": target, "data_state": "complete"}

    def github_issues_summary(self) -> dict[str, Any]:
        items = self.github_issues(page=1, page_size=10000)["items"]
        states = Counter(item["sync_state"] for item in items)
        return {
            "schema_version": GITHUB_ISSUE_SCHEMA,
            "generated_at": _now_iso(),
            "bug_count": len(items),
            "linked_count": states.get("linked", 0),
            "missing_link_count": states.get("missing_link", 0),
            "not_in_scope_count": states.get("not_in_scope", 0),
            "workflow_mismatch_count": states.get("workflow_mismatch", 0),
            "unavailable_count": states.get("unavailable", 0),
            "by_sync_state": dict(sorted(states.items())),
            "reason_codes": ["github_issue_links_missing"] if states.get("missing_link") else [],
            "data_state": "complete",
            "production_8001_touched": False,
        }

    def github_issues(self, *, page: int = 1, page_size: int = 20) -> dict[str, Any]:
        items = [self._github_issue_item(bug) for bug in self.finding_store.list_bugs(page_size=10000)["items"]]
        items.sort(key=lambda item: (_severity_rank(item["severity"]), item["sync_state"] == "missing_link"), reverse=True)
        payload = _page(items, page=page, page_size=page_size)
        payload.update({"schema_version": GITHUB_ISSUE_SCHEMA, "generated_at": _now_iso(), "data_state": "complete"})
        return payload

    def git_branches_detail_summary(self) -> dict[str, Any]:
        branch = self._safe_branch_status()
        branches = self._local_branches()
        worktrees = self._worktrees()
        return {
            "schema_version": BRANCH_DETAIL_SCHEMA,
            "generated_at": _now_iso(),
            "repo_root": _repo_display(self.repo_root),
            "current_branch": branch.get("branch"),
            "head_commit": branch.get("head_commit"),
            "branch_count": len(branches),
            "worktree_count": len(worktrees),
            "branches": branches,
            "worktrees": worktrees,
            "reason_codes": ["many_local_worktrees"] if len(worktrees) > 20 else [],
            "data_state": "complete" if branches or worktrees else "unavailable",
            "production_8001_touched": False,
        }

    def github_prs_summary(self) -> dict[str, Any]:
        prs = self.github_prs(page=1, page_size=10000)
        items = prs["items"]
        states = Counter(item.get("state") for item in items)
        return {
            "schema_version": PR_SCHEMA,
            "generated_at": _now_iso(),
            "pr_count": len(items),
            "open_count": states.get("OPEN", 0) + states.get("open", 0),
            "by_state": dict(sorted(states.items())),
            "data_state": prs["data_state"],
            "reason_codes": prs.get("reason_codes") or [],
            "production_8001_touched": False,
        }

    def github_prs(self, *, page: int = 1, page_size: int = 20) -> dict[str, Any]:
        raw, error = self._gh_json(["pr", "list", "--state", "open", "--json", "number,title,headRefName,baseRefName,state,isDraft,url,updatedAt,mergeStateStatus"])
        if error:
            payload = _page([], page=page, page_size=page_size)
            payload.update({"schema_version": PR_SCHEMA, "generated_at": _now_iso(), "data_state": "unavailable", "reason_codes": ["github_pr_data_unavailable"], "unavailable_reason": error})
            return payload
        items = [
            {
                "number": item.get("number"),
                "title": item.get("title"),
                "head_ref": item.get("headRefName"),
                "base_ref": item.get("baseRefName"),
                "state": item.get("state"),
                "is_draft": item.get("isDraft"),
                "url": item.get("url"),
                "updated_at": item.get("updatedAt"),
                "merge_state_status": item.get("mergeStateStatus"),
                "merge_gate_state": "warning" if item.get("isDraft") else "unknown",
            }
            for item in raw
            if isinstance(item, dict)
        ]
        payload = _page(items, page=page, page_size=page_size)
        payload.update({"schema_version": PR_SCHEMA, "generated_at": _now_iso(), "data_state": "complete", "reason_codes": []})
        return payload

    def legacy_debt_summary(self) -> dict[str, Any]:
        groups = self.legacy_debt_groups(page=1, page_size=10000)["items"]
        debt_count = sum(int(group["count"]) for group in groups)
        p0_p1 = sum(int(group["p0_p1_count"]) for group in groups)
        return {
            "schema_version": LEGACY_DEBT_SCHEMA,
            "generated_at": _now_iso(),
            "group_count": len(groups),
            "debt_count": debt_count,
            "p0_p1_count": p0_p1,
            "reason_codes": ["legacy_p0_p1_debt_exists"] if p0_p1 else (["legacy_debt_exists"] if debt_count else []),
            "data_state": "complete",
            "production_8001_touched": False,
        }

    def legacy_debt_groups(self, *, page: int = 1, page_size: int = 20) -> dict[str, Any]:
        findings = self.finding_store.list_findings(page_size=10000)["items"]
        grouped: dict[str, dict[str, Any]] = {}
        for item in findings:
            if item.get("source_type") not in {"legacy_inventory", "guardrail"}:
                continue
            module = str(item.get("module") or "unknown")
            category = str(item.get("category") or item.get("source_type") or "legacy")
            group_id = f"{module}:{category}".replace("/", "_")
            group = grouped.setdefault(group_id, {"debt_group_id": group_id, "module": module, "category": category, "baseline_state": "baseline_existing", "count": 0, "p0_p1_count": 0, "sample_items": [], "blocks_current_merge": False})
            group["count"] += 1
            if str(item.get("severity") or "").upper() in {"P0", "P1"}:
                group["p0_p1_count"] += 1
            if len(group["sample_items"]) < 10:
                group["sample_items"].append(item)
        items = sorted(grouped.values(), key=lambda item: (-int(item["p0_p1_count"]), -int(item["count"])))
        payload = _page(items, page=page, page_size=page_size)
        payload.update({"schema_version": LEGACY_DEBT_SCHEMA, "generated_at": _now_iso(), "data_state": "complete"})
        return payload

    def legacy_debt_group_detail(self, debt_group_id: str) -> dict[str, Any] | None:
        for group in self.legacy_debt_groups(page=1, page_size=10000)["items"]:
            if group["debt_group_id"] == debt_group_id:
                return {"schema_version": LEGACY_DEBT_SCHEMA, "generated_at": _now_iso(), "group": group, "data_state": "complete"}
        return None

    def automation_summary(self) -> dict[str, Any]:
        gh_auth_ok, gh_auth_message = self._gh_auth_state()
        gh_repo_ok, gh_repo_message, gh_repo = self._github_repo_state()
        gh_ok = gh_auth_ok and gh_repo_ok
        scripts = {
            "mcp_server": (self.repo_root / "scripts" / "aistock_mcp_server.py").exists(),
            "bug_github_sync": (self.repo_root / "scripts" / "bug_github_sync.py").exists(),
            "guardrail_scan": (self.repo_root / "scripts" / "aistock_guardrail_scan.py").exists(),
        }
        actions = [
            {"level": "L0", "action_type": "read_only_check", "default_policy": "auto_allowed", "enabled": True},
            {"level": "L1", "action_type": "dry_run", "default_policy": "auto_allowed", "enabled": True},
            {"level": "L2", "action_type": "local_file_fix", "default_policy": "scope_limited", "enabled": True},
            {"level": "L3", "action_type": "github_issue_write", "default_policy": "dry_run_then_confirm", "enabled": gh_ok},
            {"level": "L4", "action_type": "create_pr", "default_policy": "preview_required", "enabled": gh_ok},
            {"level": "L5", "action_type": "merge_or_close", "default_policy": "user_confirmation_required", "enabled": False},
            {"level": "L6", "action_type": "production_restart_or_db_write", "default_policy": "explicit_user_authorization_only", "enabled": False},
        ]
        return {
            "schema_version": AUTOMATION_SCHEMA,
            "generated_at": _now_iso(),
            "summary": {
                "gh_authenticated": gh_auth_ok,
                "github_repository_configured": gh_repo_ok,
                "github_issue_write_ready": gh_ok,
                "script_count": sum(1 for ok in scripts.values() if ok),
                "action_level_count": len(actions),
                "write_actions_require_confirmation": True,
            },
            "github_data_state": "complete" if gh_ok else "unavailable",
            "gh_auth_status": "ok" if gh_auth_ok else "unavailable",
            "gh_status_message": gh_auth_message,
            "github_repository_status": "ok" if gh_repo_ok else "unavailable",
            "github_repository": gh_repo,
            "github_repository_message": gh_repo_message,
            "scripts": scripts,
            "actions": actions,
            "mcp_policy": {"read_only_allowed": True, "dry_run_allowed": True, "github_write_requires_dry_run": True, "merge_and_production_requires_user_confirmation": True, "secret_values_redacted": True},
            "reason_codes": [] if gh_ok else [*([] if gh_auth_ok else ["gh_auth_unavailable_or_offline"]), *([] if gh_repo_ok else ["github_repository_unconfigured"])],
            "data_state": "complete",
            "production_8001_touched": False,
        }

    def platform_health_summary(self) -> dict[str, Any]:
        return ValidationPlatformHealthService(repo_root=self.repo_root).summary()

    def catalog_integrity_summary(self) -> dict[str, Any]:
        return ValidationPlatformHealthService(repo_root=self.repo_root).catalog_integrity()

    def nightly_summary(self) -> dict[str, Any]:
        return ValidationPlatformHealthService(repo_root=self.repo_root).nightly_summary()

    def nightly_runs(self, *, limit: int = 10) -> dict[str, Any]:
        summary = self.nightly_summary()
        items = []
        if summary.get("latest_run"):
            items.append(summary["latest_run"])
        return {**_page(items[:limit], page=1, page_size=limit), "schema_version": summary.get("schema_version"), "data_state": summary.get("data_state"), "reason_codes": summary.get("reason_codes") or []}

    def nightly_runner_health(self) -> dict[str, Any]:
        return ValidationPlatformHealthService(repo_root=self.repo_root).runner_health()

    @staticmethod
    def _card(card_id: str, title: str, route: str, tone: str, score: int, summary: dict[str, Any]) -> dict[str, Any]:
        return {"card_id": card_id, "title": title, "primary_route": route, "health_tone": tone, "risk_score": min(100, int(score or 0)), "summary": summary, "reason_codes": summary.get("reason_codes") or []}

    @staticmethod
    def _gate_tone(decision: str) -> str:
        return {"pass": "green", "warning": "yellow", "need_confirm": "orange", "blocked": "red", "unknown": "gray"}.get(decision, "gray")

    @staticmethod
    def _check(check_id: str, title: str, level: str, ok: bool, reason: str | None, extra: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = {"check_id": check_id, "title": title, "level": level, "status": "pass" if ok else ("blocked" if level == "blocking" else "warning"), "reason_codes": [] if ok or not reason else [reason]}
        if extra:
            payload.update(extra)
        return payload

    def _safe_branch_status(self) -> dict[str, Any]:
        try:
            return self.git_status_provider.branch_status()
        except Exception as exc:  # noqa: BLE001
            return {"branch": None, "head_commit": None, "data_state": "unavailable", "error": str(exc)}

    def _safe_workspace_status(self) -> dict[str, Any]:
        try:
            return self.git_status_provider.workspace_status()
        except Exception as exc:  # noqa: BLE001
            return {"dirty": False, "files": [], "summary": {}, "data_state": "unavailable", "error": str(exc)}

    def _run(self, args: list[str], *, timeout: int = 20) -> tuple[int, str, str]:
        try:
            completed = subprocess.run(args, cwd=str(self.repo_root), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding="utf-8", errors="replace", shell=False, timeout=timeout, check=False)
            return completed.returncode, completed.stdout or "", completed.stderr or ""
        except Exception as exc:  # noqa: BLE001
            return 1, "", str(exc)

    def _git_one(self, args: list[str], *, default: str | None = "") -> str | None:
        code, out, _err = self._run(["git", *args], timeout=15)
        return default if code else (out.strip() or default)

    def _git_lines(self, args: list[str]) -> list[str]:
        code, out, _err = self._run(["git", *args], timeout=20)
        return [] if code else [line.strip() for line in out.splitlines() if line.strip()]

    def _target_ref(self, target: str) -> str:
        for candidate in (f"origin/{target}", target):
            if self._git_one(["rev-parse", "--verify", candidate], default=None):
                return candidate
        return "HEAD"

    def _changed_files(self, target: str) -> list[str]:
        target_ref = target if target.startswith("origin/") else self._target_ref(target)
        committed = self._git_lines(["diff", "--name-only", f"{target_ref}...HEAD"])
        dirty = [_norm_path(item.get("path")) for item in self._safe_workspace_status().get("files") or []]
        return sorted({path for path in [*committed, *dirty] if path})

    def _modules_for_paths(self, paths: list[str]) -> set[str]:
        modules: set[str] = set()
        for path in paths:
            ownership = self.file_ownership_catalog.match_path(path)
            if ownership.primary_module:
                modules.add(ownership.primary_module)
            modules.update(ownership.impact_modules or [])
        return modules

    @staticmethod
    def _is_high_conflict(path: str) -> bool:
        return any(path == prefix.rstrip("/") or path.startswith(prefix) for prefix in HIGH_CONFLICT_PATHS)

    @staticmethod
    def _change_class(paths: list[str]) -> str:
        if not paths:
            return "no_change"
        if all(path.startswith("docs/") or path.endswith(".md") for path in paths):
            return "docs_only"
        if any(path.startswith(("backend/db/", "backend/migrations/", ".github/workflows/")) for path in paths):
            return "schema_or_workflow_high_risk"
        if any(path.startswith(("backend/services/paper_trading_v2/", "backend/services/strategy_package/")) for path in paths):
            return "runtime_high_risk"
        if any(path.startswith("backend/") for path in paths) and any(path.startswith("frontend/") for path in paths):
            return "full_stack_targeted"
        if any(path.startswith("backend/") for path in paths):
            return "backend_targeted"
        if any(path.startswith("frontend/") for path in paths):
            return "frontend_targeted"
        return "mixed_supporting"

    def _linked_bug(self, branch: str) -> dict[str, Any] | None:
        branch_l = branch.lower()
        for bug in self.finding_store.list_bugs(page_size=10000)["items"]:
            bug_id = str(bug.get("bug_id") or "").lower()
            if str(bug.get("fix_branch") or "").lower() == branch_l or (bug_id and bug_id in branch_l):
                return bug
        return None

    def _open_p0_p1_for_modules(self, modules: set[str]) -> list[dict[str, Any]]:
        return [bug for bug in self.finding_store.list_bugs(page_size=10000)["items"] if _is_open_bug(bug) and str(bug.get("severity") or "").upper() in {"P0", "P1"} and str(bug.get("module") or "") in modules]

    @staticmethod
    def _scope_check(bug: dict[str, Any] | None, paths: list[str]) -> tuple[bool, list[str]]:
        if not bug:
            return True, []
        scope = [_norm_path(item).rstrip("/") for item in bug.get("allowed_write_scope") or []]
        if not scope:
            return False, paths
        violations = [path for path in paths if not any(path == item or path.startswith(f"{item}/") for item in scope)]
        return not violations, violations

    def _historical_p2_p3_count(self, touched_modules: set[str]) -> int:
        return sum(1 for bug in self.finding_store.list_bugs(page_size=10000)["items"] if _is_open_bug(bug) and str(bug.get("severity") or "").upper() in {"P2", "P3"} and str(bug.get("module") or "") not in touched_modules)

    @staticmethod
    def _merge_next_actions(blockers: list[str], warnings: list[str]) -> list[str]:
        actions = []
        if "workspace_dirty" in blockers:
            actions.append("提交或清理当前分支未提交文件后重新执行门禁。")
        if "missing_linked_bug" in blockers:
            actions.append("为触达模块的 P0/P1 修复绑定 BUG JSON 或调整分支范围。")
        if "scope_violation" in blockers or "scope_violation" in warnings:
            actions.append("修正超出 allowed_write_scope 的文件，或先更新 Issue scope。")
        if "touched_module_coverage_missing_or_stale" in blockers:
            actions.append("重跑触达模块覆盖率或补齐验证证据包。")
        return actions or ["确认 warning 与人工确认项后可创建 PR；合入 main 仍需用户确认。"]

    def _evidence_bundle_ids(self, modules: set[str]) -> list[str]:
        result = []
        for item in self.history_store.list_evidence_manifests(limit=10000).get("items") or []:
            if not modules or str(item.get("module") or "") in modules:
                result.append(str(item.get("manifest_id")))
        return result[:10]

    def _module_path_index(self) -> dict[str, dict[str, list[str]]]:
        index: dict[str, dict[str, list[str]]] = defaultdict(lambda: {"owned_paths": [], "shared_paths": []})
        for rule in self.file_ownership_catalog.list_rules():
            index[rule.primary_module]["owned_paths"].extend(rule.include)
            for module in rule.impact_modules:
                if module != rule.primary_module:
                    index[module]["shared_paths"].extend(rule.include)
        return {key: {inner: sorted(set(values)) for inner, values in value.items()} for key, value in index.items()}

    def _module_aliases(self) -> dict[str, str]:
        aliases = {}
        for module in self.module_registry.list_modules():
            aliases[module.module_id.lower()] = module.module_id
            aliases[module.module_id.replace(".", "_").lower()] = module.module_id
            aliases[module.module_id.replace("_", ".").lower()] = module.module_id
        return aliases

    def _bugs_by_module(self) -> dict[str, list[dict[str, Any]]]:
        aliases = self._module_aliases()
        buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for bug in self.finding_store.list_bugs(page_size=10000)["items"]:
            module = aliases.get(str(bug.get("module") or "").lower(), str(bug.get("module") or ""))
            buckets[module].append(bug)
        return buckets

    def _findings_by_module(self) -> dict[str, list[dict[str, Any]]]:
        aliases = self._module_aliases()
        buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for finding in self.finding_store.list_findings(page_size=10000)["items"]:
            module = aliases.get(str(finding.get("module") or "").lower(), str(finding.get("module") or ""))
            buckets[module].append(finding)
        return buckets

    @staticmethod
    def _coverage_state(coverage: dict[str, Any], touched: bool) -> str:
        status = str(coverage.get("status") or "missing").lower()
        if status in {"passed", "valid"}:
            return "valid"
        if status in {"failed", "stale", "missing"}:
            return status
        if touched and status in {"not_collected", "unknown", "none", ""}:
            return "missing"
        return status or "unknown"

    @staticmethod
    def _coverage_reason(state: str, touched: bool) -> str | None:
        if state == "missing":
            return "当前模块缺少覆盖率产物" if touched else "未触达模块缺少覆盖率产物，仅作背景风险"
        if state == "stale":
            return "覆盖率执行提交早于模块最新变更"
        if state == "failed":
            return "最近覆盖率质量门禁失败"
        return None

    @staticmethod
    def _coverage_threshold(item: dict[str, Any]) -> dict[str, Any]:
        risk = str(item.get("registry_risk_level") or "low")
        return {"line_percent_min": 65 if risk == "critical" else (60 if risk == "high" else 50), "strict_for_merge": risk == "critical"}

    @staticmethod
    def _module_gate_state(coverage_state: str, p0_p1: int, threshold: dict[str, Any], touched: bool) -> str:
        if touched and p0_p1:
            return "blocked"
        if touched and coverage_state in {"missing", "stale", "failed"} and threshold.get("strict_for_merge"):
            return "blocked"
        if touched and coverage_state in {"missing", "stale", "failed"}:
            return "warning"
        if p0_p1:
            return "warning"
        return "pass"

    @staticmethod
    def _module_risk(item: dict[str, Any], bugs: list[dict[str, Any]], findings: list[dict[str, Any]], coverage_state: str, touched: bool) -> int:
        score = _risk_rank(item.get("registry_risk_level")) * 5
        score += sum({"P0": 35, "P1": 18, "P2": 6, "P3": 3}.get(str(bug.get("severity") or "").upper(), 1) for bug in bugs)
        score += sum({"P0": 18, "P1": 9, "P2": 3, "P3": 1}.get(str(finding.get("severity") or "").upper(), 1) for finding in findings[:20])
        if coverage_state == "missing":
            score += 18 if touched else 6
        elif coverage_state == "stale":
            score += 12 if touched else 4
        elif coverage_state == "failed":
            score += 20
        score += min(20, int((item.get("workspace") or {}).get("changed_file_count") or 0) * 3)
        return min(100, score)

    @staticmethod
    def _module_reasons(coverage_state: str, p0_p1: int, touched: bool) -> list[str]:
        reasons = []
        if touched:
            reasons.append("touched_by_current_branch")
        if coverage_state in {"missing", "stale", "failed"}:
            reasons.append(f"coverage_{coverage_state}")
        if p0_p1:
            reasons.append("open_p0_p1_issue")
        return reasons

    def _test_item(self, plan: dict[str, Any], jobs: list[dict[str, Any]], runs: list[dict[str, Any]]) -> dict[str, Any]:
        plan_key = str(plan.get("plan_key") or "")
        module = str(plan.get("module") or "")
        level = str(plan.get("level") or "")
        latest_job = next((job for job in jobs if job.get("plan_key") == plan_key), None)
        latest_run = next((run for run in runs if str(run.get("module") or run.get("module_slug") or "") == module and (not level or str(run.get("level") or "") == level)), None)
        status = str((latest_job or {}).get("status") or (latest_run or {}).get("status") or "missing")
        return {
            "test_id": plan_key,
            "title": plan.get("title") or plan_key,
            "module": module,
            "level": level,
            "test_level": "blocking" if level.upper() in {"L0", "L1", "L2"} else "warning",
            "status": status,
            "command_key": plan.get("command_key"),
            "nox_session": plan.get("nox_session"),
            "blocking_for_change_classes": ["docs_only", "frontend_targeted", "backend_targeted", "full_stack_targeted"] if level.upper() in {"L0", "L1"} else ["backend_targeted", "frontend_targeted", "full_stack_targeted"],
            "fast_path_eligible": level.upper() in {"L0", "L1"} or module.startswith("validation"),
            "evidence_bundle_id": (latest_job or {}).get("archive", {}).get("evidence_manifest_path") or (latest_run or {}).get("evidence_manifest_id"),
            "latest_job_id": (latest_job or {}).get("job_id"),
            "latest_run_id": (latest_run or {}).get("run_id"),
            "rerun_cost_level": {"L0": "low", "L1": "low", "L2": "medium", "L3": "medium", "L4": "high", "L5": "high"}.get(level.upper(), "unknown"),
            "recommended_command": f"python -m nox -s {plan.get('nox_session')}" if plan.get("nox_session") else None,
            "data_state": "complete",
        }

    def _github_issue_item(self, bug: dict[str, Any]) -> dict[str, Any]:
        workflow = _workflow_state(bug.get("status"))
        linked = bool(bug.get("github_issue_number") or bug.get("github_issue_url"))
        should_sync = str(bug.get("severity") or "").upper() in {"P0", "P1"} or workflow in {"in_progress", "review_ready", "fixed"}
        sync_state = "linked" if linked else ("missing_link" if should_sync else "not_in_scope")
        return {
            "bug_id": bug.get("bug_id"),
            "title": bug.get("title"),
            "module_id": bug.get("module"),
            "severity": bug.get("severity"),
            "local_status": bug.get("status"),
            "workflow_state": workflow,
            "github_issue_number": bug.get("github_issue_number"),
            "github_issue_url": bug.get("github_issue_url"),
            "sync_state": sync_state,
            "allowed_write_scope_state": "complete" if bug.get("allowed_write_scope") else "missing",
            "required_verification_state": "complete" if bug.get("required_verification") else "missing",
            "closure_requirements_state": "complete" if bug.get("closure_requirements") else "missing",
            "next_action": "补齐 GitHub Issue 链接" if sync_state == "missing_link" else "保持本地 BUG JSON 与 GitHub 镜像一致",
        }

    def _issue_item(self, bug: dict[str, Any]) -> dict[str, Any]:
        workflow = _workflow_state(bug.get("status"))
        scope_state = "complete" if bug.get("allowed_write_scope") else "missing"
        verification_state = "satisfied" if bug.get("verification_run_id") else ("pending" if bug.get("required_verification") else "missing")
        closure_state = "satisfied" if workflow in {"verified", "closed"} else ("pending" if bug.get("closure_requirements") else "missing")
        if scope_state != "complete":
            gate_state, next_action = "triage_only_until_allowed_write_scope_is_set", "补齐 allowed_write_scope 后才能进入编码。"
        elif workflow == "open":
            gate_state, next_action = "triage_required", "完成根因、模块、最小修复范围和验证标准分诊。"
        elif verification_state in {"missing", "pending"} and workflow in {"review_ready", "fixed", "verified"}:
            gate_state, next_action = "verification_required", "补齐 required_verification 对应证据。"
        else:
            gate_state, next_action = "allowed", "按当前生命周期推进下一步。"
        return {
            "schema_version": ISSUE_WORKFLOW_SCHEMA,
            "bug_id": bug.get("bug_id"),
            "title": bug.get("title"),
            "github_issue_number": bug.get("github_issue_number"),
            "github_issue_url": bug.get("github_issue_url"),
            "workflow_state": workflow,
            "severity": bug.get("severity"),
            "module_id": bug.get("module"),
            "risk_area": bug.get("risk_area"),
            "allowed_write_scope_state": scope_state,
            "worktree_state": "declared" if bug.get("fix_branch") else "not_declared",
            "fix_branch": bug.get("fix_branch"),
            "assigned_agent": bug.get("assigned_agent"),
            "integration_owner": bug.get("integration_owner") or "codex-app",
            "required_verification_state": verification_state,
            "closure_requirements_state": closure_state,
            "next_action": next_action,
            "gate_state": gate_state,
            "allowed_write_scope": bug.get("allowed_write_scope") or [],
            "required_verification": bug.get("required_verification") or [],
            "closure_requirements": bug.get("closure_requirements") or [],
            "created_at": bug.get("created_at"),
            "last_seen_at": bug.get("last_seen_at"),
            "data_state": "complete",
        }

    def _candidate_queue_roots(self) -> list[Path]:
        roots = [
            self.repo_root / "tests" / "aistock_validation" / "runs" / "candidates",
            self.repo_root / "tests" / "aistock_validation" / "history" / "issue_candidates",
            self.repo_root / "tmp" / "validation" / "ci_failure_issue",
            self.repo_root / "tmp" / "validation" / "nightly_failure_issue",
        ]
        code_intelligence_root = self.repo_root / "tmp" / "validation" / "code-intelligence"
        if code_intelligence_root.exists():
            roots.extend(path for path in sorted(code_intelligence_root.glob("*/bug-candidates")) if path.is_dir())
        return roots

    def _candidate_queue_items(self) -> list[dict[str, Any]]:
        by_key: dict[str, dict[str, Any]] = {}
        for root in self._candidate_queue_roots():
            if not root.exists():
                continue
            for path in sorted(root.rglob("*.json")):
                if not path.is_file():
                    continue
                try:
                    payload = json.loads(path.read_text(encoding="utf-8-sig"))
                except (OSError, json.JSONDecodeError, UnicodeDecodeError):
                    continue
                item = self._candidate_item_from_payload(path, payload)
                items: list[dict[str, Any]]
                if item:
                    items = [item]
                elif payload.get("schema_version") == "aistock_bug_candidate_queue_v1" and isinstance(payload.get("candidates"), list):
                    items = [
                        queue_item
                        for queue_item in (
                            self._candidate_item_from_payload(path, candidate)
                            for candidate in payload.get("candidates") or []
                            if isinstance(candidate, dict)
                        )
                        if queue_item
                    ]
                else:
                    continue
                for queue_item in items:
                    key = str(queue_item.get("fingerprint") or queue_item.get("candidate_id") or queue_item.get("source_path"))
                    if key in by_key:
                        by_key[key] = self._merge_candidate_items(by_key[key], queue_item)
                    else:
                        by_key[key] = queue_item
        return list(by_key.values())

    def _candidate_item_from_payload(self, path: Path, payload: dict[str, Any]) -> dict[str, Any] | None:
        if not isinstance(payload, dict):
            return None
        candidate = payload.get("candidate") if isinstance(payload.get("candidate"), dict) else {}
        event = payload.get("event") if isinstance(payload.get("event"), dict) else {}
        failure_event = payload.get("failure_event") if isinstance(payload.get("failure_event"), dict) else {}
        handoff = payload.get("agent_handoff") if isinstance(payload.get("agent_handoff"), dict) else {}
        dedupe = payload.get("dedupe") if isinstance(payload.get("dedupe"), dict) else {}
        schema = str(payload.get("schema_version") or candidate.get("schema_version") or event.get("schema_version") or "")
        if schema == "aistock_bug_candidate_queue_v1":
            return None

        if (
            not candidate
            and not schema.startswith(("aistock_ci_failure_", "aistock_failure_event_", "aistock_bug_candidate_"))
            and "fingerprint" not in payload
        ):
            return None

        issue_number = (
            candidate.get("github_issue_number")
            or payload.get("github_issue_number")
            or event.get("github_issue_number")
            or failure_event.get("github_issue_number")
        )
        issue_url = (
            candidate.get("github_issue_url")
            or payload.get("github_issue_url")
            or event.get("github_issue_url")
            or failure_event.get("github_issue_url")
        )
        fingerprint = (
            candidate.get("fingerprint")
            or payload.get("fingerprint")
            or event.get("fingerprint")
            or failure_event.get("fingerprint")
            or dedupe.get("fingerprint")
            or payload.get("pack_id")
            or path.stem
        )
        module_id = (
            candidate.get("module")
            or payload.get("module")
            or event.get("module_guess")
            or failure_event.get("module_guess")
            or self._candidate_first_string(payload.get("suspected_modules"))
            or "validation"
        )
        severity = (
            candidate.get("severity")
            or payload.get("severity")
            or event.get("severity_guess")
            or failure_event.get("severity_guess")
            or self._candidate_severity_from_labels(payload.get("labels"))
            or "P1"
        )
        created_at = candidate.get("created_at") or payload.get("created_at") or event.get("timestamp") or failure_event.get("timestamp") or self._candidate_path_mtime(path)
        evidence_refs = self._candidate_string_list(candidate.get("evidence"))
        evidence_refs.extend(self._candidate_string_list(payload.get("evidence_refs")))
        evidence_refs.extend(self._candidate_string_list(event.get("evidence_refs")))
        evidence_refs.extend(self._candidate_string_list(failure_event.get("evidence_refs")))
        evidence_refs.extend(self._candidate_string_list([payload.get("run_url"), issue_url]))
        evidence_refs.extend(self._candidate_string_list(candidate.get("evidence_refs")))

        required_verification = self._candidate_string_list(candidate.get("required_validation"))
        required_verification.extend(self._candidate_string_list(candidate.get("required_verification")))
        required_verification.extend(self._candidate_string_list(payload.get("required_verification")))
        required_verification.extend(self._candidate_string_list(payload.get("suggested_validation")))
        required_verification.extend(self._candidate_string_list(candidate.get("suggested_validation")))
        required_verification.extend(self._candidate_string_list(handoff.get("required_verification")))
        if not required_verification:
            required_verification = self._candidate_string_list([event.get("reproduce_command"), failure_event.get("reproduce_command")])

        quality_gate = candidate.get("quality_gate") if isinstance(candidate.get("quality_gate"), dict) else payload.get("quality_gate")
        quality_gate = quality_gate if isinstance(quality_gate, dict) else {}
        issue_payload_ready = quality_gate.get("issue_payload_ready")
        if issue_payload_ready is None and schema == "aistock_bug_candidate_github_issue_payload_v1":
            issue_payload_ready = True
        auto_submit_allowed = quality_gate.get("auto_submit_allowed")
        if auto_submit_allowed is None:
            auto_submit_allowed = payload.get("auto_submit_allowed")
        codegraph_refs = self._dedupe_strings(
            self._candidate_string_list(candidate.get("codegraph_refs")) + self._candidate_string_list(payload.get("codegraph_refs"))
        )
        ua_refs = self._dedupe_strings(self._candidate_string_list(candidate.get("ua_refs")) + self._candidate_string_list(payload.get("ua_refs")))
        quality_reasons = self._dedupe_strings(self._candidate_string_list(quality_gate.get("reasons")))
        status_value = str(candidate.get("status") or payload.get("status") or payload.get("candidate_status") or event.get("candidate_status") or failure_event.get("candidate_status") or "new")
        source_path = self._candidate_source_path(path)
        no_submit_reasons = self._candidate_no_submit_reasons(
            status=status_value,
            issue_payload_ready=issue_payload_ready,
            auto_submit_allowed=auto_submit_allowed,
            quality_reasons=quality_reasons,
            issue_number=issue_number,
            issue_url=issue_url,
        )
        issue_payload_ref = candidate.get("github_issue_payload_ref") or payload.get("github_issue_payload_ref")
        if not issue_payload_ref and schema == "aistock_bug_candidate_github_issue_payload_v1":
            issue_payload_ref = source_path

        return {
            "schema_version": CANDIDATE_QUEUE_SCHEMA,
            "candidate_id": candidate.get("candidate_id") or payload.get("candidate_id") or payload.get("pack_id") or event.get("event_id") or failure_event.get("event_id") or f"CAND-{str(fingerprint)[:16]}",
            "title": candidate.get("title") or payload.get("title") or payload.get("problem_statement") or event.get("normalized_error") or failure_event.get("normalized_error") or "Issue workflow candidate",
            "source_type": self._candidate_source_type(schema),
            "source_types": [self._candidate_source_type(schema)],
            "source_schema": schema or "unknown",
            "module_id": str(module_id),
            "severity": str(severity).upper(),
            "status": status_value,
            "fingerprint": str(fingerprint),
            "dedupe_key": candidate.get("dedupe_key") or payload.get("dedupe_fingerprint") or dedupe.get("marker") or dedupe.get("search_query"),
            "run_count": int(payload.get("run_count") or payload.get("occurrence_count") or payload.get("recurrence_count") or 1),
            "github_issue_number": issue_number,
            "github_issue_url": issue_url,
            "linked_pr_url": candidate.get("pr_url") or payload.get("pr_url") or payload.get("pull_request_url"),
            "confidence": candidate.get("confidence") or payload.get("confidence"),
            "summary": candidate.get("summary") or payload.get("summary"),
            "llm_hypothesis": candidate.get("llm_hypothesis") or payload.get("llm_hypothesis"),
            "expected": candidate.get("expected") or payload.get("expected"),
            "actual": candidate.get("actual") or payload.get("actual"),
            "verification_result": candidate.get("verification_result") or payload.get("verification_result"),
            "reproduce": self._dedupe_strings(self._candidate_string_list(candidate.get("reproduce")) + self._candidate_string_list(payload.get("reproduce"))),
            "source_plan_key": candidate.get("source_plan_key") or payload.get("source_plan_key"),
            "quality_gate": quality_gate,
            "quality_gate_state": quality_gate.get("workflow_gate") or ("ready" if issue_payload_ready is True else "draft" if quality_gate else None),
            "issue_payload_ready": issue_payload_ready,
            "auto_submit_allowed": auto_submit_allowed,
            "promotion_mode": candidate.get("promotion_mode") or payload.get("promotion_mode") or ("deterministic_quality_gate" if issue_payload_ready is True else None),
            "llm_enhancement_opt_in": candidate.get("llm_enhancement_opt_in") or payload.get("llm_enhancement_opt_in"),
            "quality_gate_reasons": quality_reasons,
            "no_submit_reasons": no_submit_reasons,
            "why_not_submitted": " / ".join(no_submit_reasons) if no_submit_reasons else None,
            "active_discovery_reason": candidate.get("active_discovery_reason") or payload.get("active_discovery_reason"),
            "codegraph_refs": codegraph_refs,
            "ua_refs": ua_refs,
            "github_issue_payload_ref": issue_payload_ref,
            "evidence_refs": self._dedupe_strings(evidence_refs),
            "recommended_validation": self._dedupe_strings(required_verification),
            "allowed_write_scope": self._dedupe_strings(
                self._candidate_string_list(candidate.get("allowed_write_scope"))
                + self._candidate_string_list(payload.get("allowed_write_scope"))
                + self._candidate_string_list(handoff.get("allowed_write_scope"))
            ),
            "created_at": created_at,
            "last_seen_at": payload.get("last_seen_at") or created_at,
            "source_path": source_path,
            "source_paths": [source_path],
            "data_state": "complete",
        }

    @staticmethod
    def _candidate_no_submit_reasons(
        *,
        status: str,
        issue_payload_ready: Any,
        auto_submit_allowed: Any,
        quality_reasons: list[str],
        issue_number: Any,
        issue_url: Any,
    ) -> list[str]:
        if issue_number or issue_url:
            return []
        reasons: list[str] = []
        status_text = str(status or "").lower()
        if status_text in {"deduped", "artifact_only", "rejected", "ignored"}:
            reasons.append(status_text)
        if issue_payload_ready is not True:
            reasons.extend(quality_reasons or ["quality_gate_not_ready"])
        else:
            reasons.append("awaiting_operator_promotion")
        if auto_submit_allowed is False:
            reasons.append("auto_submit_disabled")
        return ValidationPipelineCenterService._dedupe_strings(reasons)

    @staticmethod
    def _candidate_source_type(schema: str) -> str:
        if schema == "aistock_ci_failure_candidate_history_v1":
            return "ci_candidate_history"
        if schema == "aistock_ci_failure_context_pack_v1":
            return "ci_context_pack"
        if schema == "aistock_ci_failure_github_issue_payload_v1":
            return "ci_github_issue_payload"
        if schema == "aistock_failure_event_v1":
            return "failure_event"
        if schema == "aistock_bug_candidate_v1":
            return "nightly_bug_candidate"
        if schema == "aistock_bug_candidate_github_issue_payload_v1":
            return "nightly_bug_candidate_issue_payload"
        if schema == "aistock_bug_candidate_queue_v1":
            return "nightly_bug_candidate_queue"
        return "issue_flow_candidate"

    @staticmethod
    def _candidate_string_list(value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item or "").strip()]
        if isinstance(value, tuple):
            return [str(item).strip() for item in value if str(item or "").strip()]
        text = str(value).strip()
        return [text] if text else []

    @staticmethod
    def _candidate_first_string(value: Any) -> str | None:
        items = ValidationPipelineCenterService._candidate_string_list(value)
        return items[0] if items else None

    def _candidate_outcome_metrics(self, items: list[dict[str, Any]]) -> dict[str, Any]:
        bugs = self.finding_store.list_bugs(page_size=10000)["items"]
        bugs_by_issue = {
            str(value): bug
            for bug in bugs
            for value in (bug.get("github_issue_number"), bug.get("github_issue_url"))
            if value
        }
        confirmed = 0
        false_positive = 0
        promoted = 0
        deduped = 0
        open_unlinked = 0
        for item in items:
            if item.get("github_issue_number") or item.get("github_issue_url"):
                promoted += 1
            if str(item.get("status") or "").lower() == "deduped":
                deduped += 1
            if not (item.get("github_issue_number") or item.get("github_issue_url")):
                open_unlinked += 1
            bug = bugs_by_issue.get(str(item.get("github_issue_number"))) or bugs_by_issue.get(str(item.get("github_issue_url")))
            if bug:
                workflow = _workflow_state(bug.get("status"))
                if workflow in {"fixed", "verified", "closed"}:
                    confirmed += 1
                elif str(bug.get("status") or "").lower() in {"rejected", "false_positive", "not_a_bug"}:
                    false_positive += 1
        total = len(items)
        return {
            "candidate_count": total,
            "promoted_issue_count": promoted,
            "confirmed_issue_count": confirmed,
            "false_positive_count": false_positive,
            "deduped_count": deduped,
            "open_unlinked_count": open_unlinked,
            "promotion_rate": round(promoted / total, 4) if total else 0.0,
            "confirmation_rate": round(confirmed / promoted, 4) if promoted else 0.0,
            "false_positive_rate": round(false_positive / promoted, 4) if promoted else 0.0,
        }

    @staticmethod
    def _candidate_severity_from_labels(value: Any) -> str | None:
        for item in ValidationPipelineCenterService._candidate_string_list(value):
            text = item.lower()
            if text in {"p0", "p1", "p2", "p3"}:
                return text.upper()
            if text.startswith("severity:p"):
                return text.split(":", 1)[1].upper()
        return None

    @staticmethod
    def _dedupe_strings(items: list[str]) -> list[str]:
        seen: set[str] = set()
        result: list[str] = []
        for item in items:
            text = str(item or "").strip()
            if not text or text in seen:
                continue
            seen.add(text)
            result.append(text)
        return result

    @staticmethod
    def _candidate_path_mtime(path: Path) -> str:
        try:
            return datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat()
        except OSError:
            return _now_iso()

    def _candidate_source_path(self, path: Path) -> str:
        try:
            return path.resolve().relative_to(self.repo_root.resolve()).as_posix()
        except ValueError:
            return _repo_display(path)

    def _merge_candidate_items(self, left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
        merged = dict(left)
        for key in (
            "github_issue_number",
            "github_issue_url",
            "linked_pr_url",
            "dedupe_key",
            "confidence",
            "summary",
            "llm_hypothesis",
            "expected",
            "actual",
            "verification_result",
            "source_plan_key",
            "issue_payload_ready",
            "auto_submit_allowed",
            "promotion_mode",
            "llm_enhancement_opt_in",
            "github_issue_payload_ref",
            "why_not_submitted",
            "active_discovery_reason",
        ):
            if not merged.get(key) and right.get(key):
                merged[key] = right[key]
        for key in (
            "evidence_refs",
            "recommended_validation",
            "allowed_write_scope",
            "source_paths",
            "source_types",
            "reproduce",
            "quality_gate_reasons",
            "no_submit_reasons",
            "codegraph_refs",
            "ua_refs",
        ):
            merged[key] = self._dedupe_strings(self._candidate_string_list(merged.get(key)) + self._candidate_string_list(right.get(key)))
        if not merged.get("quality_gate") and right.get("quality_gate"):
            merged["quality_gate"] = right["quality_gate"]
        if not merged.get("quality_gate_state") and right.get("quality_gate_state"):
            merged["quality_gate_state"] = right["quality_gate_state"]
        merged["run_count"] = max(int(merged.get("run_count") or 1), int(right.get("run_count") or 1))
        if str(right.get("last_seen_at") or "") > str(merged.get("last_seen_at") or ""):
            merged["last_seen_at"] = right.get("last_seen_at")
        merged["no_submit_reasons"] = self._candidate_no_submit_reasons(
            status=str(merged.get("status") or ""),
            issue_payload_ready=merged.get("issue_payload_ready"),
            auto_submit_allowed=merged.get("auto_submit_allowed"),
            quality_reasons=self._candidate_string_list(merged.get("quality_gate_reasons")),
            issue_number=merged.get("github_issue_number"),
            issue_url=merged.get("github_issue_url"),
        )
        merged["why_not_submitted"] = " / ".join(self._candidate_string_list(merged.get("no_submit_reasons"))) or None
        return merged

    def _local_branches(self) -> list[dict[str, Any]]:
        rows = self._git_lines(["branch", "--format", "%(refname:short)|%(upstream:short)|%(objectname:short)|%(committerdate:iso8601)|%(subject)"])
        current = str(self._safe_branch_status().get("branch") or "")
        branches = []
        for row in rows:
            parts = row.split("|", 4)
            if len(parts) != 5:
                continue
            name, upstream, head, updated_at, subject = parts
            branches.append({"branch": name, "upstream": upstream or None, "head_commit": head, "updated_at": updated_at, "subject": subject, "current": name == current, "bound_task_state": self._branch_task_state(name), "merge_gate_state": "current" if name == current else "unknown"})
        return branches

    def _worktrees(self) -> list[dict[str, Any]]:
        rows = self._git_lines(["worktree", "list", "--porcelain"])
        items: list[dict[str, Any]] = []
        current: dict[str, Any] = {}
        for row in [*rows, ""]:
            if not row:
                if current:
                    items.append(current)
                current = {}
                continue
            key, _sep, value = row.partition(" ")
            if key == "worktree":
                current["path"] = value.replace("\\", "/")
            elif key == "HEAD":
                current["head_commit"] = value[:12]
            elif key == "branch":
                current["branch"] = value.removeprefix("refs/heads/")
            elif key == "detached":
                current["detached"] = True
        for item in items:
            branch = str(item.get("branch") or "")
            item["bound_task_state"] = self._branch_task_state(branch)
            item["worktree_state"] = "feature_worktree" if "AIstock_worktrees" in str(item.get("path") or "") else "main_or_external"
        return items

    @staticmethod
    def _branch_task_state(branch: str) -> str:
        if not branch:
            return "detached_or_unknown"
        if branch.startswith("bug/") or "BUG-" in branch.upper():
            return "bug_bound"
        if branch.startswith(("codex/", "claude/")):
            return "feature_or_integration_bound"
        if branch == "main":
            return "main"
        return "unknown"

    def _gh_json(self, args: list[str]) -> tuple[Any, str | None]:
        code, out, err = self._run(["gh", *args], timeout=15)
        if code:
            return None, (err or out or "gh command failed").strip()
        try:
            return json.loads(out or "[]"), None
        except json.JSONDecodeError as exc:
            return None, f"gh returned invalid JSON: {exc}"

    def _gh_auth_state(self) -> tuple[bool, str]:
        code, out, err = self._run(["gh", "auth", "status", "--hostname", "github.com"], timeout=8)
        if code == 0:
            return True, "gh auth status ok; token value redacted"
        return False, (err or out or "gh auth status unavailable").strip()

    def _github_repo_state(self) -> tuple[bool, str, str | None]:
        env_repo = os.environ.get("GITHUB_REPOSITORY")
        if env_repo:
            return env_repo.count("/") == 1, "GITHUB_REPOSITORY env configured" if env_repo.count("/") == 1 else "GITHUB_REPOSITORY must be owner/name", env_repo

        file_repo = self._github_repo_from_env_file()
        if file_repo:
            return file_repo.count("/") == 1, f"{LOCAL_GITHUB_ENV_FILE} configured" if file_repo.count("/") == 1 else f"{LOCAL_GITHUB_ENV_FILE} GITHUB_REPOSITORY must be owner/name", file_repo

        remote_url = self._git_one(["remote", "get-url", "origin"], default=None)
        remote_repo = _github_repo_from_remote_url(remote_url or "")
        if remote_repo:
            return True, "inferred from git origin remote", remote_repo
        return False, f"missing GITHUB_REPOSITORY, {LOCAL_GITHUB_ENV_FILE}, or GitHub origin remote", None

    def _github_repo_from_env_file(self) -> str | None:
        path = self.repo_root / LOCAL_GITHUB_ENV_FILE
        if not path.is_file():
            return None
        try:
            lines = path.read_text(encoding="utf-8-sig").splitlines()
        except OSError:
            return None
        for line in lines:
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            if key.strip() == "GITHUB_REPOSITORY":
                return value.strip().strip('"').strip("'") or None
        return None
