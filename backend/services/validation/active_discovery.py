from __future__ import annotations

import json
import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.services.validation.execution_runner import ValidationExecutionRunner
from backend.services.validation.finding_store import ValidationFindingStore
from backend.services.validation.history_store import ValidationHistoryStore
from backend.services.validation.module_quality import ModuleQualityService
from backend.services.validation.module_registry import REPO_ROOT
from backend.services.validation.pipeline_center import ValidationPipelineCenterService
from backend.services.validation.ui_target_catalog import ValidationUiTargetCatalog


ACTIVE_DISCOVERY_SCHEMA = "aistock_validation_active_discovery_v1"
NIGHTLY_REPORT_SCHEMA = "aistock_validation_discovery_nightly_report_v1"
ISSUE_CANDIDATE_SCHEMA = "aistock_issue_candidate_v1"
DISCOVERY_TASK_SCHEMA = "aistock_discovery_task_v1"
LLM_PROFILE_SCHEMA = "aistock_discovery_agent_profile_v1"
EVIDENCE_MANIFEST_SCHEMA = "aistock_discovery_evidence_manifest_v1"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_slug(value: Any, default: str = "item") -> str:
    raw = str(value or default).strip().lower().replace("\\", "/")
    raw = re.sub(r"[^a-z0-9._/-]+", "-", raw).strip("-./")
    return (raw or default).replace("/", "-")[:120]


def _page(items: list[dict[str, Any]], *, page: int, page_size: int) -> dict[str, Any]:
    start = (page - 1) * page_size
    end = start + page_size
    return {"items": items[start:end], "total": len(items), "page": page, "page_size": page_size, "has_more": end < len(items)}


def _count_by(items: list[dict[str, Any]], field: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        key = str(item.get(field) or "unknown")
        counts[key] = counts.get(key, 0) + 1
    return counts


def _severity_rank(value: Any) -> int:
    return {"P0": 5, "P1": 4, "P2": 3, "P3": 2}.get(str(value or "").upper(), 1)


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        if not path.exists() or path.stat().st_size > 8 * 1024 * 1024:
            return None
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else None
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _repo_path(repo_root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(repo_root.resolve()).as_posix()
    except ValueError:
        return path.as_posix()


class ActiveDiscoveryService:
    """Read-only active discovery aggregation plus local task/review evidence files."""

    def __init__(
        self,
        *,
        repo_root: Path | None = None,
        history_store: ValidationHistoryStore | None = None,
        finding_store: ValidationFindingStore | None = None,
        execution_runner: ValidationExecutionRunner | None = None,
        module_quality_service: ModuleQualityService | None = None,
        ui_target_catalog: ValidationUiTargetCatalog | None = None,
        pipeline_center: ValidationPipelineCenterService | None = None,
    ) -> None:
        self.repo_root = Path(repo_root or REPO_ROOT)
        self.history_store = history_store or ValidationHistoryStore(repo_root=self.repo_root)
        self.finding_store = finding_store or ValidationFindingStore(repo_root=self.repo_root)
        self.execution_runner = execution_runner or ValidationExecutionRunner()
        self.module_quality_service = module_quality_service or ModuleQualityService(repo_root=self.repo_root)
        self.ui_target_catalog = ui_target_catalog or ValidationUiTargetCatalog()
        self.pipeline_center = pipeline_center or ValidationPipelineCenterService(
            repo_root=self.repo_root,
            history_store=self.history_store,
            finding_store=self.finding_store,
            execution_runner=self.execution_runner,
            module_quality_service=self.module_quality_service,
            ui_target_catalog=self.ui_target_catalog,
        )
        self.state_root = self.repo_root / "tmp" / "validation" / "discovery"
        self.review_root = self.state_root / "reviews"
        self.task_root = self.state_root / "tasks"
        self.agent_result_root = self.state_root / "agent_results"
        self.trace_root = self.state_root / "traces"

    def summary(self) -> dict[str, Any]:
        report = self.get_nightly_report("current")
        candidates = self.list_candidates(page_size=10000)["items"]
        tasks = self.list_tasks(page_size=10000)["items"]
        return {
            "schema_version": ACTIVE_DISCOVERY_SCHEMA,
            "generated_at": _now_iso(),
            "current_report_id": report["report_id"],
            "candidate_count": len(candidates),
            "candidate_by_severity": _count_by(candidates, "severity"),
            "candidate_by_review_status": _count_by(candidates, "review_status"),
            "task_count": len(tasks),
            "task_by_source": _count_by(tasks, "source"),
            "llm_profile_count": len(self.list_llm_profiles()["items"]),
            "production_8001_touched": False,
        }

    def list_nightly_reports(self, *, limit: int = 7) -> dict[str, Any]:
        reports = [self._report_summary(self.get_nightly_report("current"))]
        for run in self.history_store.list_runs(page_size=max(limit, 7))["items"][: max(0, limit - 1)]:
            reports.append(self._report_summary(self._build_report(run)))
        return _page(reports[:limit], page=1, page_size=limit)

    def get_nightly_report(self, report_id: str) -> dict[str, Any]:
        runs = self.history_store.list_runs(page_size=10000)["items"]
        if report_id in {"current", "latest"}:
            return self._build_report(runs[0] if runs else None)
        for run in runs:
            run_id = str(run.get("run_id") or "")
            if report_id in {run_id, f"disc_{run_id}"}:
                return self._build_report(run)
        return self._build_report(None, report_id=report_id)

    def get_nightly_llm_report(self, report_id: str) -> dict[str, Any]:
        report = self.get_nightly_report(report_id)
        traces = [item for item in self._iter_payloads(self.trace_root) if item.get("kind") == "llm_trace"]
        return {
            "schema_version": "aistock_validation_discovery_llm_report_v1",
            "report_id": report["report_id"],
            "generated_at": _now_iso(),
            "profiles": self.list_llm_profiles()["items"],
            "traces": traces,
            "draft_candidates": [item for item in self.list_candidates(page_size=10000)["items"] if item.get("source") == "llm_agent"],
            "eval_summary": self._llm_eval_summary(),
            "sensitive_payload_policy": "no_token_no_secret_in_context_pack",
        }

    def list_candidates(
        self,
        *,
        module: str | None = None,
        severity: str | None = None,
        review_status: str | None = None,
        source: str | None = None,
        search: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        items = self._candidate_items()
        if module:
            items = [item for item in items if module.lower() in str(item.get("module") or "").lower()]
        if severity:
            items = [item for item in items if str(item.get("severity") or "").upper() == severity.upper()]
        if review_status:
            items = [item for item in items if str(item.get("review_status") or "").lower() == review_status.lower()]
        if source:
            items = [item for item in items if source.lower() in str(item.get("source") or "").lower()]
        if search:
            needle = search.lower()
            items = [item for item in items if needle in str(item.get("candidate_id") or "").lower() or needle in str(item.get("title") or "").lower()]
        items.sort(key=lambda item: (_severity_rank(item.get("severity")), float(item.get("confidence") or 0), str(item.get("candidate_id"))), reverse=True)
        return _page(items, page=page, page_size=page_size)

    def get_candidate(self, candidate_id: str) -> dict[str, Any] | None:
        for item in self._candidate_items():
            if item["candidate_id"] == candidate_id:
                detail = dict(item)
                detail["reviews"] = [review for review in self._iter_payloads(self.review_root) if review.get("candidate_id") == candidate_id]
                detail["evidence_manifest"] = self.get_trace(str(item.get("evidence_manifest_id") or ""))
                return detail
        return None

    def review_candidate(self, candidate_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        if self.get_candidate(candidate_id) is None:
            raise KeyError(candidate_id)
        review_id = f"review_{_safe_slug(candidate_id)}_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
        record = {
            "schema_version": "aistock_issue_candidate_review_v1",
            "review_id": review_id,
            "candidate_id": candidate_id,
            "action": payload.get("action") or "needs_evidence",
            "reviewer": payload.get("reviewer") or "operator",
            "comment": payload.get("comment") or "",
            "evidence_checklist": payload.get("evidence_checklist") or [],
            "created_at": _now_iso(),
        }
        _write_json(self.review_root / f"{review_id}.json", record)
        return record

    def promote_candidate(self, candidate_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        candidate = self.get_candidate(candidate_id)
        if candidate is None:
            raise KeyError(candidate_id)
        if payload.get("confirm_promote") != candidate_id:
            raise ValueError(f"confirm_promote must equal {candidate_id}")
        if str(candidate.get("severity") or "").upper() in {"P0", "P1"} and not payload.get("reviewer"):
            raise ValueError("P0/P1 promotion requires reviewer")
        record = self.review_candidate(
            candidate_id,
            {
                "action": "promote_existing_link" if candidate.get("github_issue_url") else "promote_requested",
                "reviewer": payload.get("reviewer") or "operator",
                "comment": payload.get("comment") or "promotion requested",
                "evidence_checklist": payload.get("evidence_checklist") or candidate.get("evidence_types") or [],
            },
        )
        return {
            "schema_version": "aistock_issue_candidate_promotion_v1",
            "candidate_id": candidate_id,
            "promotion_status": "linked_existing_github_issue" if candidate.get("github_issue_url") else "requires_github_sync_mcp",
            "github_issue_url": candidate.get("github_issue_url"),
            "review_record": record,
        }

    def list_tasks(self, *, source: str | None = None, status: str | None = None, page: int = 1, page_size: int = 20) -> dict[str, Any]:
        items = [*self._default_tasks(), *self._execution_tasks(), *self._iter_payloads(self.task_root)]
        if source:
            items = [item for item in items if str(item.get("source") or "").lower() == source.lower()]
        if status:
            items = [item for item in items if str(item.get("status") or "").lower() == status.lower()]
        items.sort(key=lambda item: str(item.get("updated_at") or item.get("created_at") or ""), reverse=True)
        return _page(items, page=page, page_size=page_size)

    def schedule_task(self, payload: dict[str, Any]) -> dict[str, Any]:
        risk = str(payload.get("risk_level") or "L2").upper()
        if risk in {"L4", "L5"} and payload.get("confirm_schedule") != risk:
            raise ValueError(f"{risk} task requires confirm_schedule={risk}")
        task_id = payload.get("task_id") or f"disc_task_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}_{_safe_slug(payload.get('module'), 'module')}"
        task = {
            "schema_version": DISCOVERY_TASK_SCHEMA,
            "task_id": task_id,
            "title": payload.get("title") or f"Manual discovery task for {payload.get('module') or 'validation'}",
            "source": payload.get("source") or "manual_mcp",
            "module": payload.get("module") or "validation_center",
            "risk_level": risk,
            "status": "scheduled",
            "detectors": payload.get("detectors") or [],
            "resource_policy_id": payload.get("resource_policy_id") or "validation_readonly_default",
            "requested_by": payload.get("requested_by") or "operator",
            "reason": payload.get("reason") or "manual discovery task",
            "cleanup_required": bool(payload.get("cleanup_required", risk in {"L4", "L5"})),
            "created_at": _now_iso(),
            "updated_at": _now_iso(),
            "evidence_manifest_id": f"evid_{_safe_slug(task_id)}",
        }
        _write_json(self.task_root / f"{_safe_slug(task_id)}.json", task)
        return task

    def run_task(self, task_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        task = self._load_task(task_id)
        if task is None:
            raise KeyError(task_id)
        risk = str(task.get("risk_level") or "L2").upper()
        if risk in {"L4", "L5"} and payload.get("confirm_run") != task_id:
            raise ValueError(f"{risk} task requires confirm_run={task_id}")
        task["status"] = "completed" if payload.get("dry_run", True) else "scheduled_for_runner"
        task["last_run_mode"] = "dry_run" if payload.get("dry_run", True) else "confirmed"
        task["result"] = {
            "result_id": f"result_{_safe_slug(task_id)}_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
            "summary": "Dry-run completed without production writes",
            "evidence_manifest_id": self._write_task_evidence(task),
        }
        task["updated_at"] = _now_iso()
        _write_json(self.task_root / f"{_safe_slug(task_id)}.json", task)
        return task

    def cancel_task(self, task_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        task = self._load_task(task_id)
        if task is None:
            raise KeyError(task_id)
        task["status"] = "cancelled"
        task["cancel_reason"] = payload.get("reason") or "cancelled by operator"
        task["updated_at"] = _now_iso()
        _write_json(self.task_root / f"{_safe_slug(task_id)}.json", task)
        return task

    def claim_agent_task(self, task_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        task = self._load_task(task_id) or self.schedule_task({"task_id": task_id, "title": f"Agent task {task_id}", "source": "manual_mcp"})
        task.update(
            {
                "status": "claimed",
                "agent_runtime": payload.get("agent_runtime") or "codex",
                "agent_name": payload.get("agent_name") or "codex-app",
                "workspace": payload.get("workspace"),
                "branch": payload.get("branch"),
                "claimed_at": _now_iso(),
                "updated_at": _now_iso(),
            }
        )
        _write_json(self.task_root / f"{_safe_slug(task_id)}.json", task)
        return task

    def get_agent_context_pack(self, task_id: str) -> dict[str, Any]:
        task = self._load_task(task_id) or {"task_id": task_id, "module": "validation_center", "risk_level": "L2"}
        return {
            "schema_version": "aistock_discovery_context_pack_v1",
            "context_pack_id": f"ctx_{_safe_slug(task_id)}",
            "task_id": task_id,
            "generated_at": _now_iso(),
            "task": task,
            "design_doc": "docs/architecture/validation_active_bug_discovery_platform_design_20260520.md",
            "open_candidates": self.list_candidates(page_size=10)["items"],
            "module_summary": self._module_summary_for(str(task.get("module") or "validation_center")),
            "sensitive_payload_policy": "tokens and DB passwords are never included",
        }

    def submit_agent_result(self, task_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        result_id = payload.get("result_id") or f"agent_result_{_safe_slug(task_id)}_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
        result = {
            "schema_version": "aistock_agent_task_result_v1",
            "result_id": result_id,
            "task_id": task_id,
            "agent_runtime": payload.get("agent_runtime") or "codex",
            "agent_name": payload.get("agent_name") or "codex-app",
            "llm_provider_declared": payload.get("llm_provider_declared"),
            "llm_model_declared": payload.get("llm_model_declared"),
            "prompt_id": payload.get("prompt_id"),
            "prompt_version": payload.get("prompt_version"),
            "context_pack_id": payload.get("context_pack_id"),
            "summary": payload.get("summary") or payload.get("candidate_title"),
            "confidence": payload.get("confidence"),
            "requires_deterministic_verification": payload.get("requires_deterministic_verification", True),
            "evidence_manifest_id": payload.get("evidence_manifest_id") or f"evid_{_safe_slug(task_id)}",
            "created_at": _now_iso(),
        }
        _write_json(self.agent_result_root / f"{_safe_slug(result_id)}.json", result)
        return result

    def attach_agent_evidence(self, task_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        manifest_id = payload.get("evidence_manifest_id") or f"evid_{_safe_slug(task_id)}"
        manifest = {
            "schema_version": EVIDENCE_MANIFEST_SCHEMA,
            "manifest_id": manifest_id,
            "task_id": task_id,
            "generated_at": _now_iso(),
            "artifacts": payload.get("artifacts") or [],
            "logs": payload.get("logs") or [],
            "api_responses": payload.get("api_responses") or [],
            "mcp_responses": payload.get("mcp_responses") or [],
            "screenshots": payload.get("screenshots") or [],
            "reproduce_command": payload.get("reproduce_command"),
            "sensitive_payload_policy": "redacted_before_persisting",
        }
        _write_json(self.trace_root / f"{_safe_slug(manifest_id)}.json", manifest)
        return manifest

    def complete_agent_task(self, task_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        task = self._load_task(task_id)
        if task is None:
            raise KeyError(task_id)
        task["status"] = payload.get("status") or "completed"
        task["completion_summary"] = payload.get("summary")
        task["completed_at"] = _now_iso()
        task["updated_at"] = _now_iso()
        _write_json(self.task_root / f"{_safe_slug(task_id)}.json", task)
        return task

    def list_llm_profiles(self) -> dict[str, Any]:
        env_present = any(os.getenv(name) for name in ("DEEPSEEK_API_KEY", "DEEPSEEK_V4_PRO_API_KEY", "DEEPSEEK_V4_API_KEY", "DEEPSEEK_PRO_API_KEY"))
        roles = ("contract_extractor", "design_consistency_checker", "cross_module_explorer", "llm_report_summarizer", "candidate_deduper")
        profiles = [
            {
                "schema_version": LLM_PROFILE_SCHEMA,
                "profile_id": f"validation_{role}_deepseek",
                "agent_role": role,
                "provider_id": "deepseek",
                "provider_status": "configured" if env_present else "missing_env",
                "model_id": "deepseek-v4-pro",
                "prompt_id": f"validation_discovery_{role}",
                "prompt_version": None,
                "prompt_management_url": f"/quantevolver/prompts?agent_type=validation_discovery&prompt_key=validation_discovery_{role}",
                "model_config_url": "/config/rdagent-llm",
                "temperature": 0.2,
                "max_tokens": 12000,
                "enabled_for_nightly": role != "cross_module_explorer",
                "enabled_for_manual_mcp": True,
                "last_7_runs": {"success_rate": None, "candidate_hit_rate": None, "false_positive_rate": None, "cost_estimate": None},
                "secret_visible": False,
            }
            for role in roles
        ]
        return {
            "schema_version": LLM_PROFILE_SCHEMA,
            "items": profiles,
            "total": len(profiles),
            "page": 1,
            "page_size": len(profiles),
            "has_more": False,
            "prompt_management_url": "/quantevolver/prompts",
            "model_config_url": "/config/rdagent-llm",
        }

    def list_tool_adapters(self) -> dict[str, Any]:
        adapters = [
            self._adapter("semgrep_business_rule_adapter", "Semgrep business rules", "static_analysis", self.repo_root / "tests" / "aistock_validation" / "discovery_rules" / "semgrep"),
            self._adapter("schemathesis_api_fuzz_adapter", "Schemathesis OpenAPI dry-run", "api_fuzz", self.repo_root / "tests" / "aistock_validation" / "discovery_rules" / "api_fuzz_targets.yaml"),
            self._adapter("playwright_trace_probe_adapter", "Playwright trace probe", "ui_trace", self.repo_root / "frontend" / "playwright.config.ts"),
            self._adapter("contract_alignment_adapter", "API/MCP/UI contract alignment", "contract", self.repo_root / "tests" / "aistock_validation" / "catalog" / "ui_targets.yaml"),
            self._adapter("llm_eval_adapter", "Promptfoo-style LLM eval", "llm_eval", self.repo_root / "tests" / "aistock_validation" / "llm_eval"),
        ]
        return {
            "schema_version": "aistock_discovery_tool_adapters_v1",
            "items": adapters,
            "total": len(adapters),
            "page": 1,
            "page_size": len(adapters),
            "has_more": False,
        }

    def run_tool_adapter(self, adapter_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        if adapter_id not in {item["adapter_id"] for item in self.list_tool_adapters()["items"]}:
            raise KeyError(adapter_id)
        if not payload.get("dry_run", True) and payload.get("confirm_run") != adapter_id:
            raise ValueError(f"non-dry-run adapter execution requires confirm_run={adapter_id}")
        result = {
            "semgrep_business_rule_adapter": self._semgrep_like_dry_run,
            "schemathesis_api_fuzz_adapter": self._schemathesis_like_dry_run,
            "playwright_trace_probe_adapter": self._playwright_like_dry_run,
            "contract_alignment_adapter": self._contract_alignment_dry_run,
            "llm_eval_adapter": self._llm_eval_summary,
        }[adapter_id]()
        return {"schema_version": "aistock_tool_adapter_run_result_v1", "adapter_id": adapter_id, "dry_run": payload.get("dry_run", True), "generated_at": _now_iso(), "result": result, "evidence_manifest_id": f"evid_adapter_{_safe_slug(adapter_id)}"}

    def run_llm_eval(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = self._llm_eval_summary()
        result["dry_run"] = payload.get("dry_run", True)
        result["profiles_compared"] = payload.get("profiles") or ["design_consistency_checker", "cross_module_explorer"]
        return result

    def get_trace(self, trace_id: str) -> dict[str, Any] | None:
        for payload in self._iter_payloads(self.trace_root):
            if str(payload.get("trace_id") or payload.get("manifest_id") or payload.get("llm_trace_id")) == trace_id:
                return payload
        if trace_id:
            return {"schema_version": EVIDENCE_MANIFEST_SCHEMA, "manifest_id": trace_id, "generated_at": _now_iso(), "artifacts": [], "logs": [], "api_responses": [], "mcp_responses": [], "screenshots": [], "reproduce_command": "No persisted evidence yet; run a discovery task to attach evidence."}
        return None

    def _build_report(self, run: dict[str, Any] | None, *, report_id: str | None = None) -> dict[str, Any]:
        candidates = self._candidate_items()
        tasks = self.list_tasks(page_size=10000)["items"]
        module_quality = self.module_quality_service.module_quality_summary(commit_limit=30)
        generated_at = _now_iso()
        rid = report_id or (f"disc_{run.get('run_id')}" if run else f"disc_current_{generated_at[:10].replace('-', '')}")
        branch = self._git(["rev-parse", "--abbrev-ref", "HEAD"])
        commit = self._git(["rev-parse", "--short", "HEAD"])
        return {
            "schema_version": NIGHTLY_REPORT_SCHEMA,
            "report_id": rid,
            "generated_at": generated_at,
            "run": {"run_id": run.get("run_id") if run else "current", "title": run.get("title") if run else "Current active discovery snapshot", "branch": branch, "commit": commit, "started_at": run.get("started_at") if run else generated_at, "finished_at": run.get("finished_at") if run else generated_at, "status": run.get("status") if run else "snapshot"},
            "summary_cards": [
                self._summary_card("nightly_status", "Nightly status", "needs_review" if any(c.get("severity") in {"P0", "P1"} for c in candidates) else "passed", "status"),
                self._summary_card("coverage_scope", "Coverage scope", len(module_quality.get("modules") or []), "modules"),
                self._summary_card("new_candidates", "New candidates", len(candidates), "candidate"),
                self._summary_card("llm_exploration", "LLM exploration", len(self.list_llm_profiles()["items"]), "profiles"),
                self._summary_card("issue_sync", "Issue sync", self.pipeline_center.github_issues_summary().get("linked_count", 0), "linked"),
                self._summary_card("cleanup", "Cleanup", len([task for task in tasks if task.get("cleanup_required")]), "resources"),
            ],
            "modules": self._module_cards(module_quality, candidates),
            "execution_tree": self._execution_tree(tasks),
            "llm_summary": {"profile_count": len(self.list_llm_profiles()["items"]), "draft_candidate_count": len([c for c in candidates if c.get("source") == "llm_agent"]), "provider_summary": {"deepseek": "configured" if any(os.getenv(name) for name in ("DEEPSEEK_API_KEY", "DEEPSEEK_V4_PRO_API_KEY")) else "missing_env"}},
            "candidate_summary": {"total": len(candidates), "by_severity": _count_by(candidates, "severity"), "by_review_status": _count_by(candidates, "review_status"), "needs_review": len([c for c in candidates if c.get("review_status") in {"pending_review", "needs_evidence"}])},
            "issue_sync": self.pipeline_center.github_issues_summary(),
            "cleanup": {"validation_resource_count": len([task for task in tasks if task.get("cleanup_required")]), "overdue_count": 0, "failed_count": len([task for task in tasks if task.get("cleanup_status") == "failed"]), "namespace": "validation"},
            "evidence_manifest_id": f"evid_{_safe_slug(rid)}",
        }

    def _report_summary(self, report: dict[str, Any]) -> dict[str, Any]:
        return {key: report.get(key) for key in ("report_id", "generated_at", "run", "candidate_summary", "llm_summary", "cleanup")}

    def _candidate_items(self) -> list[dict[str, Any]]:
        reviews = {str(item.get("candidate_id")): item for item in self._iter_payloads(self.review_root)}
        candidates: list[dict[str, Any]] = []
        for bug in self.finding_store.list_bugs(page_size=10000)["items"]:
            bug_id = str(bug.get("bug_id") or "unknown")
            candidate_id = f"ic_{bug_id.lower()}"
            status = str(bug.get("status") or "open")
            candidates.append(
                {
                    "schema_version": ISSUE_CANDIDATE_SCHEMA,
                    "candidate_id": candidate_id,
                    "source": "bug_registry",
                    "source_id": bug_id,
                    "title": bug.get("title") or bug_id,
                    "module": bug.get("module") or "unknown",
                    "severity": str(bug.get("severity") or "P3").upper(),
                    "confidence": 0.95 if bug.get("github_issue_url") else 0.8,
                    "review_status": (reviews.get(candidate_id) or {}).get("action") or ("verified" if status in {"fixed", "verified", "closed"} else "pending_review"),
                    "evidence_status": "verified" if bug.get("reproduce_command") else "needs_evidence",
                    "deterministic_status": "verified",
                    "github_issue_url": bug.get("github_issue_url"),
                    "github_issue_number": bug.get("github_issue_number"),
                    "evidence_types": ["bug_json", "reproduce_command" if bug.get("reproduce_command") else "registry"],
                    "evidence_manifest_id": f"evid_{_safe_slug(bug_id)}",
                    "reproduce_command": bug.get("reproduce_command"),
                    "recommended_action": "review_and_fix",
                    "created_at": bug.get("created_at") or bug.get("last_seen_at"),
                    "updated_at": bug.get("updated_at") or bug.get("last_seen_at") or bug.get("created_at"),
                }
            )
        for finding in self.finding_store.list_findings(page_size=300)["items"]:
            finding_id = str(finding.get("finding_id") or "unknown")
            candidate_id = f"ic_{_safe_slug(finding_id)}"
            candidates.append(
                {
                    "schema_version": ISSUE_CANDIDATE_SCHEMA,
                    "candidate_id": candidate_id,
                    "source": finding.get("source_type") or "finding",
                    "source_id": finding_id,
                    "title": finding.get("title") or finding_id,
                    "module": finding.get("module") or "unknown",
                    "severity": str(finding.get("severity") or "P3").upper(),
                    "confidence": 0.72 if finding.get("source_type") == "guardrail" else 0.55,
                    "review_status": (reviews.get(candidate_id) or {}).get("action") or "needs_evidence",
                    "evidence_status": "needs_evidence",
                    "deterministic_status": "detected",
                    "github_issue_url": finding.get("linked_issue"),
                    "evidence_types": [finding.get("source_type") or "finding", "file" if finding.get("file_path") else "manifest"],
                    "evidence_manifest_id": f"evid_{_safe_slug(finding_id)}",
                    "reproduce_command": "run relevant validation detector and inspect evidence manifest",
                    "recommended_action": "triage_candidate",
                    "created_at": finding.get("first_seen_at"),
                    "updated_at": finding.get("last_seen_at"),
                }
            )
        for result in self._iter_payloads(self.agent_result_root):
            result_id = str(result.get("result_id") or result.get("task_id") or "agent")
            candidates.append({"schema_version": ISSUE_CANDIDATE_SCHEMA, "candidate_id": f"ic_{_safe_slug(result_id)}", "source": "llm_agent", "source_id": result_id, "title": result.get("summary") or result_id, "module": result.get("module") or "validation_center", "severity": str(result.get("severity_suggested") or "P2").upper(), "confidence": result.get("confidence") or 0.6, "review_status": "needs_deterministic_verification", "evidence_status": "needs_evidence", "deterministic_status": "not_verified", "evidence_manifest_id": result.get("evidence_manifest_id") or f"evid_{_safe_slug(result_id)}", "evidence_types": ["llm_draft", "agent_result"], "llm_provider_declared": result.get("llm_provider_declared"), "llm_model_declared": result.get("llm_model_declared"), "prompt_id": result.get("prompt_id"), "prompt_version": result.get("prompt_version"), "context_pack_id": result.get("context_pack_id")})
        return candidates

    def _default_tasks(self) -> list[dict[str, Any]]:
        now = _now_iso()
        return [
            self._task("disc_task_nightly_rules", "Nightly deterministic rule scan", "nightly_baseline", "validation_center", "L2", "ready", ["business_rule_scanner", "semgrep_business_rule_adapter"], now),
            self._task("disc_task_nightly_api_alignment", "Nightly API/MCP/UI alignment", "nightly_baseline", "validation_center", "L2", "ready", ["api_mcp_alignment", "contract_alignment_adapter"], now),
            self._task("disc_task_change_driven_ui", "Change-driven UI target validation", "change_driven", "frontend", "L3", "ready", ["ui_coverage_scanner", "playwright_trace_probe_adapter"], now),
            self._task("disc_task_llm_eval", "LLM prompt regression dry-run", "nightly_baseline", "validation_center", "L2", "ready", ["llm_eval_adapter"], now),
        ]

    def _execution_tasks(self) -> list[dict[str, Any]]:
        try:
            jobs = self.execution_runner.list_jobs(page_size=20)["items"]
        except Exception:
            jobs = []
        return [self._task(f"disc_exec_{_safe_slug(job.get('job_id') or job.get('execution_id'))}", f"Controlled validation execution {job.get('plan_key') or job.get('job_id')}", "nightly_baseline", job.get("module") or "validation_center", "L2", job.get("status") or "unknown", [job.get("plan_key") or "controlled_runner"], job.get("updated_at") or job.get("created_at") or _now_iso(), extra={"execution_job": job}) for job in jobs]

    def _task(self, task_id: str, title: str, source: str, module: str, risk_level: str, status: str, detectors: list[Any], updated_at: str, *, extra: dict[str, Any] | None = None) -> dict[str, Any]:
        task = {"schema_version": DISCOVERY_TASK_SCHEMA, "task_id": task_id, "title": title, "source": source, "module": module, "risk_level": risk_level, "status": status, "detectors": detectors, "resource_policy_id": "validation_readonly_default", "cleanup_required": risk_level in {"L4", "L5"}, "created_at": updated_at, "updated_at": updated_at, "evidence_manifest_id": f"evid_{_safe_slug(task_id)}"}
        if extra:
            task.update(extra)
        return task

    def _load_task(self, task_id: str) -> dict[str, Any] | None:
        for task in [*self._default_tasks(), *self._execution_tasks(), *self._iter_payloads(self.task_root)]:
            if task.get("task_id") == task_id:
                return dict(task)
        return None

    def _write_task_evidence(self, task: dict[str, Any]) -> str:
        manifest_id = str(task.get("evidence_manifest_id") or f"evid_{_safe_slug(task.get('task_id'))}")
        _write_json(self.trace_root / f"{_safe_slug(manifest_id)}.json", {"schema_version": EVIDENCE_MANIFEST_SCHEMA, "manifest_id": manifest_id, "task_id": task.get("task_id"), "generated_at": _now_iso(), "logs": [{"kind": "summary", "text": "Dry-run used local repo/catalog evidence only."}], "api_responses": [], "mcp_responses": [], "screenshots": [], "artifacts": [], "reproduce_command": f"GET /api/v1/validation/discovery/tasks/{task.get('task_id')}"})
        return manifest_id

    def _module_cards(self, module_quality: dict[str, Any], candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
        by_module: dict[str, list[dict[str, Any]]] = {}
        for candidate in candidates:
            by_module.setdefault(str(candidate.get("module") or "unknown"), []).append(candidate)
        cards = []
        required_modules = {
            "validation": "Validation Center",
            "qe": "QuantEvolver",
            "strategy_package": "Strategy Package",
            "selection": "Selection Center",
            "paper_v2": "Paper v2",
        }
        modules = list(module_quality.get("modules") or [])[:50]
        existing_ids = {str(module.get("module_id") or "unknown") for module in modules}
        for module_id, display_name in required_modules.items():
            if module_id not in existing_ids:
                modules.append(
                    {
                        "module_id": module_id,
                        "display_name": display_name,
                        "coverage": {"status": "missing", "line_percent": None},
                        "quality": {"bug_count": 0, "finding_count": 0},
                        "workspace": {"changed_file_count": 0},
                        "test_plans": {},
                    }
                )
        for module in modules:
            module_id = str(module.get("module_id") or "unknown")
            module_candidates = by_module.get(module_id, [])
            status = "critical" if any(c.get("severity") == "P0" for c in module_candidates) else "warning" if any(c.get("severity") == "P1" for c in module_candidates) else "unknown" if str((module.get("coverage") or {}).get("status") or "missing") == "missing" else "healthy"
            cards.append({"module_id": module_id, "display_name": module.get("display_name") or module_id, "status": status, "coverage": module.get("coverage") or {}, "candidate_count": len(module_candidates), "p0_p1_count": len([c for c in module_candidates if c.get("severity") in {"P0", "P1"}]), "issue_count": (module.get("quality") or {}).get("bug_count", 0), "finding_count": (module.get("quality") or {}).get("finding_count", 0), "workspace_changed_file_count": (module.get("workspace") or {}).get("changed_file_count", 0), "test_plans": module.get("test_plans") or {}, "candidates": module_candidates[:10]})
        return cards

    def _execution_tree(self, tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        groups = []
        for source in ("nightly_baseline", "change_driven", "manual_mcp"):
            source_tasks = [task for task in tasks if task.get("source") == source]
            groups.append({"node_id": source, "label": source.replace("_", " ").title(), "status": "failed" if any(t.get("status") == "failed" for t in source_tasks) else "passed" if source_tasks else "unknown", "duration_ms": sum(int(t.get("duration_ms") or 0) for t in source_tasks), "children": source_tasks[:20]})
        return groups

    def _summary_card(self, card_id: str, title: str, value: Any, hint: str) -> dict[str, Any]:
        tone = "green"
        if card_id in {"new_candidates", "cleanup"} and int(value or 0) > 0:
            tone = "amber"
        if card_id == "nightly_status" and str(value) not in {"passed", "success", "snapshot"}:
            tone = "red" if value == "needs_review" else "blue"
        return {"card_id": card_id, "title": title, "value": value, "hint": hint, "tone": tone, "filter": card_id}

    def _adapter(self, adapter_id: str, title: str, kind: str, config_path: Path) -> dict[str, Any]:
        return {"adapter_id": adapter_id, "title": title, "kind": kind, "status": "configured" if config_path.exists() else "needs_config", "config_path": _repo_path(self.repo_root, config_path), "dry_run_supported": True, "writes_production": False, "requires_confirm_for_write": True}

    def _semgrep_like_dry_run(self) -> dict[str, Any]:
        rules = [{"rule_id": "QE-BT-001", "pattern": "filtered_pool_", "paths": ["backend", "tests", "docs"]}, {"rule_id": "VALIDATION-GH-001", "pattern": "github_issue_url", "paths": ["tests/aistock_validation/bugs"]}, {"rule_id": "UI-COVERAGE-001", "pattern": "ui_targets.yaml", "paths": ["tests/aistock_validation/catalog", "frontend/src/app"]}]
        matches = []
        for rule in rules:
            count = 0
            for raw_path in rule["paths"]:
                root = self.repo_root / raw_path
                if not root.exists() or not root.is_dir():
                    continue
                for suffix in ("*.py", "*.json", "*.yaml", "*.yml", "*.md", "*.tsx", "*.ts"):
                    for path in root.rglob(suffix):
                        try:
                            if path.stat().st_size <= 512 * 1024:
                                count += path.read_text(encoding="utf-8", errors="ignore").count(str(rule["pattern"]))
                        except OSError:
                            pass
            matches.append({"rule_id": rule["rule_id"], "match_count": count, "status": "detected" if count else "clear"})
        return {"tool": "semgrep-like", "rule_count": len(rules), "matches": matches, "candidate_count": len([m for m in matches if m["match_count"]])}

    def _schemathesis_like_dry_run(self) -> dict[str, Any]:
        route_count = 0
        validation_routes = 0
        for path in (self.repo_root / "backend" / "routers").glob("*.py"):
            text = path.read_text(encoding="utf-8", errors="ignore")
            count = len(re.findall(r"@router\.(get|post|put|delete|patch)\(", text))
            route_count += count
            if path.name == "validation.py":
                validation_routes += count
        return {"tool": "schemathesis-like", "mode": "dry_run_readonly", "openapi_route_count_estimate": route_count, "validation_route_count_estimate": validation_routes, "configured_targets": ["/api/v1/validation/*", "/api/v1/qe-templates/*", "/api/v1/strategy-packages/*"], "api_500_detected": 0, "schema_drift_detected": 0}

    def _playwright_like_dry_run(self) -> dict[str, Any]:
        targets = self.ui_target_catalog.list_targets(page_size=10000).get("items", [])
        return {"tool": "playwright-trace-like", "target_count": len(targets), "trace_required": True, "screenshot_required": True, "aria_snapshot_required": True, "candidate_count": len([target for target in targets if target.get("coverage_status") in {"missing", "unknown"}])}

    def _contract_alignment_dry_run(self) -> dict[str, Any]:
        targets = self.ui_target_catalog.summary()
        plans = self.pipeline_center.pipeline_tests_summary()
        return {"tool": "contract-alignment", "ui_targets": targets, "pipeline_tests": plans, "candidate_count": int(targets.get("targets_requiring_action") or 0) + int(plans.get("missing_evidence_count") or 0)}

    def _llm_eval_summary(self) -> dict[str, Any]:
        bugs = self.finding_store.list_bugs(page_size=10000)["items"][:20]
        return {"schema_version": "aistock_llm_eval_report_v1", "generated_at": _now_iso(), "case_count": len(bugs), "case_source": "tests/aistock_validation/bugs", "profiles_available": len(self.list_llm_profiles()["items"]), "recall_rate": None, "false_positive_rate": None, "status": "ready_for_dry_run" if bugs else "no_historical_cases"}

    def _module_summary_for(self, module: str) -> dict[str, Any]:
        summary = self.module_quality_service.module_quality_summary(commit_limit=20)
        for item in summary.get("modules") or []:
            if item.get("module_id") == module:
                return item
        return {"module_id": module, "status": "unknown"}

    def _git(self, args: list[str]) -> str | None:
        try:
            result = subprocess.run(["git", *args], cwd=self.repo_root, text=True, capture_output=True, check=False, timeout=5)
            return result.stdout.strip() or None
        except Exception:
            return None

    def _iter_payloads(self, root: Path) -> list[dict[str, Any]]:
        if not root.exists():
            return []
        payloads = []
        for path in sorted(root.glob("*.json")):
            payload = _read_json(path)
            if payload:
                payloads.append(payload)
        return payloads
