from __future__ import annotations

import argparse
import contextlib
import hashlib
import io
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

REPO_ROOT = Path(__file__).resolve().parents[1]
BUGS_ROOT = REPO_ROOT / "tests" / "aistock_validation" / "bugs"
WORKFLOW_ROOT = Path("tmp") / "issue_workflow"
ALLOWED_FIX_STATUSES = {"open", "in_progress"}
GITHUB_REPO = "licong01-cloud/AIstock"
ACTIVE_WORKFLOW_STATES = {
    "discovered",
    "context_ready",
    "fix_in_progress",
    "fix_applied",
    "validation_planned",
    "validation_running",
    "validation_passed",
    "pushed",
    "pr_opened",
    "ci_running",
    "ci_green",
}
TERMINAL_WORKFLOW_STATES = {"merged", "close_synced", "cleanup_done", "complete"}
NON_BLOCKING_CHECK_CONCLUSIONS = {"SUCCESS", "NEUTRAL", "SKIPPED"}
ARTIFACT_PATH_PATTERNS = (
    ".codex_tmp",
    ".coverage",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "__pycache__",
    "catboost_info",
    ".next",
    "node_modules",
)
BUG_ID_RE = re.compile(r"\bBUG-(\d{3,})\b", re.IGNORECASE)
OUTPUT_FORMAT_TOKENS = {"json", "yaml", "yml", "text", "txt", "stdout", "stderr", "console"}
OUTPUT_FORMAT_CHOICES = ("compact", "summary", "full-json")
ACTIONABLE_CI_CLASSIFICATIONS = {"real_regression_candidate", "test_fixture_gap_or_real_regression"}
SUPERSEDED_CI_CLASSIFICATIONS = {
    "superseded_by_later_main_success",
    "superseded_by_later_branch_success",
}
SAFE_OUTPUT_DIRS = (WORKFLOW_ROOT, Path("tmp") / "validation")
COMMITTABLE_BUG_REGISTRY_PATHS = (
    "tests/aistock_validation/bugs",
)
FAST_PATH_TIER_ORDER = {"T0": 0, "T1": 1, "T2": 2, "T3": 3}
FAST_PATH_REGISTRY_PREFIXES = ("tests/aistock_validation/bugs/",)
FAST_PATH_CATALOG_PREFIXES = ("tests/aistock_validation/catalog/",)
FAST_PATH_WORKFLOW_FILES = {
    "scripts/aistock_issue_workflow.py",
    "scripts/issue_flow.py",
    "scripts/ci_failure_issue_summary.py",
    "scripts/code_intelligence_adapter.py",
}
FAST_PATH_DEPENDENCY_FILES = {
    "frontend/package.json",
    "frontend/package-lock.json",
    "frontend/pnpm-lock.yaml",
    "package.json",
    "package-lock.json",
    "pnpm-lock.yaml",
    "requirements.txt",
    "requirements-dev.txt",
    "requirements.lock.txt",
    "pyproject.toml",
}

UI_ROUTE_HINTS = {
    "advisory": {
        "routes": ["/paper-v2/advisory"],
        "scope": [
            "frontend/src/app/paper-v2/advisory/page.tsx",
            "frontend/src/lib/api/advisory.ts",
            "frontend/tests/paper-v2/paper-v2-advisory-ui.spec.ts",
            "backend/routers/advisory.py",
            "backend/services/advisory_program.py",
            "backend/tests/watchlist/test_advisory_api.py",
            "backend/tests/watchlist/test_advisory_program.py",
        ],
        "verification": [
            "frontend_tsc",
            "paper_v2_ui",
            "backend/tests/watchlist/test_advisory_api.py",
            "backend/tests/watchlist/test_advisory_program.py",
        ],
    },
    "paper_v2": {
        "routes": ["/paper-v2"],
        "scope": [
            "frontend/src/app/paper-v2",
            "frontend/src/lib/api",
            "frontend/tests/paper-v2",
            "backend/routers/paper_trading_v2.py",
            "backend/services/paper_trading_v2",
        ],
        "verification": ["frontend_tsc", "paper_v2_ui", "paper_v2_backend"],
    },
}
UI_KEYWORDS = ("ui", "页面", "前端", "显示", "按钮", "弹窗", "表格", "分页", "排序", "json", "route", "page")

sys.path.insert(0, str(REPO_ROOT))
from scripts import issue_flow as flow  # noqa: E402
from scripts import ci_failure_issue_summary as ci_failure_summary  # noqa: E402
from scripts import code_intelligence_adapter as code_intelligence  # noqa: E402


class WorkflowError(ValueError):
    """Raised when the high-level AIstock issue workflow cannot proceed safely."""


class WorkflowPayloadError(WorkflowError):
    """Raised with a compact machine-readable recovery payload."""

    def __init__(self, payload: dict[str, Any]) -> None:
        super().__init__(str(payload.get("message") or payload.get("reason") or "workflow payload error"))
        self.payload = payload


def _estimate_tokens(text: str) -> int:
    if not text:
        return 0
    # Conservative mixed Chinese/English estimate for workflow budgeting.
    return max(1, (len(text) + 3) // 4)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _today_compact() -> str:
    return datetime.now().strftime("%Y%m%d")


def _slug(value: str, max_len: int = 72) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower()).strip("-")
    return (slug or "issue")[:max_len].strip("-") or "issue"


def _short_hash(*values: Any, length: int = 8) -> str:
    payload = json.dumps(values, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:length]


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise WorkflowError(f"JSON root must be an object: {path}")
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _size_and_token_estimate(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False, "bytes": 0, "estimated_tokens": 0}
    text = path.read_text(encoding="utf-8", errors="replace") if path.is_file() else ""
    return {
        "path": str(path),
        "exists": True,
        "bytes": path.stat().st_size,
        "estimated_tokens": _estimate_tokens(text),
    }


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def _sha256_file(path: Path) -> str | None:
    if not path.exists() or not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256_tree(path: Path) -> str | None:
    if not path.exists():
        return None
    if path.is_file():
        return _sha256_file(path)
    digest = hashlib.sha256()
    for child in sorted(item for item in path.rglob("*") if item.is_file()):
        if ".git" in child.parts:
            continue
        rel = child.relative_to(path).as_posix()
        digest.update(rel.encode("utf-8"))
        digest.update(b"\0")
        digest.update((_sha256_file(child) or "").encode("ascii"))
        digest.update(b"\0")
    return digest.hexdigest()


def _resolve_output_path(output: str | None) -> Path | None:
    if not output or output == "-":
        return None
    raw = output.strip()
    if not raw:
        return None
    if raw.lower() in OUTPUT_FORMAT_TOKENS:
        raise WorkflowError("--output expects a JSON file path; omit it or use --output - for stdout")

    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path

    roots = [REPO_ROOT.resolve()]
    canonical = _canonical_root().resolve()
    if canonical not in roots:
        roots.append(canonical)
    safe_roots = [root / safe_dir for root in roots for safe_dir in SAFE_OUTPUT_DIRS]
    in_safe_dir = any(_is_inside(path, safe_root) for safe_root in safe_roots)
    parent = path.parent.resolve()
    if not path.suffix and parent in roots:
        raise WorkflowError(
            "--output refuses a root-level bare file; use --output - or write under tmp/issue_workflow/"
        )
    if not path.suffix and not in_safe_dir:
        raise WorkflowError("--output path must include a file suffix or be under tmp/issue_workflow/ or tmp/validation/")
    return path


def _pick(payload: dict[str, Any], *keys: str) -> dict[str, Any]:
    return {key: payload.get(key) for key in keys if key in payload}


def _compact_count(value: Any) -> int | None:
    if isinstance(value, (list, tuple, set, dict)):
        return len(value)
    return None


def _compact_check_summary(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    summary: dict[str, Any] = {}
    for key in ("failed", "pending", "non_blocking", "passed"):
        items = value.get(key)
        count = _compact_count(items)
        if count is not None:
            summary[f"{key}_count"] = count
            if key in {"failed", "pending"} and items:
                summary[key] = list(items)[:5]
    return summary


def _compact_stale_pr_check(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return value if value is not None else None
    compact = _pick(value, "status")
    open_prs = value.get("open_prs")
    merged_prs = value.get("merged_prs")
    if isinstance(open_prs, list):
        compact["open_pr_count"] = len(open_prs)
    if isinstance(merged_prs, list):
        compact["merged_pr_count"] = len(merged_prs)
    return compact


def _compact_phase_summary(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    return _pick(
        value,
        "event_count",
        "known_duration_seconds",
        "inferred_elapsed_seconds",
        "code_repair_seconds",
        "total_estimated_tokens",
        "context_estimated_tokens",
        "artifact_estimated_tokens",
        "top_phase",
        "token_usage_status",
    )


def _compact_merge(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    compact = _pick(value, "already_merged", "merge_commit", "recovered_from_local_merge_error")
    verified = value.get("verified")
    if isinstance(verified, dict):
        merge_commit = _merge_commit_from_pr_check(verified)
        if merge_commit:
            compact["merge_commit"] = merge_commit
        pr = verified.get("pr")
        if isinstance(pr, dict):
            compact["merged_at"] = pr.get("mergedAt")
            compact["pr_state"] = pr.get("state")
    if "check_summary" in value:
        compact["check_summary"] = _compact_check_summary(value["check_summary"])
    return compact


def _compact_pr_automation(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    compact = _pick(value, "branch", "dry_run", "pr_url")
    if "pre_pr_gate" in value:
        compact["pre_pr_gate"] = _pick(value["pre_pr_gate"], "workflow_gate", "blocking", "warnings", "next_actions")
    actions = value.get("actions")
    if isinstance(actions, list):
        compact["actions_count"] = len(actions)
    if isinstance(value.get("ci_watch"), dict):
        compact["ci_watch"] = _pick(value["ci_watch"], "workflow_gate", "check_summary", "attempts", "next_actions")
    if value.get("next_commands"):
        compact["next_commands"] = value.get("next_commands")
    return compact


def _compact_timing_summary(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    return _pick(
        value,
        "event_count",
        "known_duration_seconds",
        "inferred_elapsed_seconds",
        "queue_seconds",
        "active_fix_seconds",
        "local_validation_seconds",
        "pr_ci_seconds",
        "merge_aftercare_seconds",
        "code_repair_seconds",
        "started_at",
        "ended_at",
    )


def _compact_start(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    compact = _pick(
        value,
        "bug_id",
        "module",
        "status",
        "workflow_gate",
        "source_bug_json",
        "context_pack_md",
        "fix_ready_path",
        "task_card_json",
        "task_card_md",
        "state_path",
        "events_path",
        "github_issue_url",
    )
    worktree_plan = value.get("worktree_plan")
    if isinstance(worktree_plan, dict):
        compact["worktree_plan"] = _pick(worktree_plan, "branch", "worktree", "created", "dry_run")
    if "required_verification" in value:
        compact["required_verification"] = value.get("required_verification")
    if "production_gates" in value:
        compact["production_gates"] = value.get("production_gates")
    return compact


def _compact_finish(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    compact = _pick(
        value,
        "bug_id",
        "workflow_gate",
        "closure_ready",
        "changed_files",
        "required_verification",
        "recommended_verification",
        "production_gates",
        "pr_body_path",
        "state_path",
        "events_path",
        "error",
    )
    if "validation_evidence" in value:
        compact["validation_evidence_count"] = len(value.get("validation_evidence") or [])
    if "scope_check" in value:
        compact["scope_check"] = _pick(value["scope_check"], "status", "violations", "status_source")
    if "code_intelligence" in value:
        compact["code_intelligence"] = _pick(
            value["code_intelligence"],
            "status",
            "context_ref",
            "affected_tests_ref",
            "affected_tests_count",
            "understand_anything_summary_ref",
            "fallback_used",
        )
    if "pre_pr_gate" in value:
        compact["pre_pr_gate"] = _pick(value["pre_pr_gate"], "workflow_gate", "blocking")
    return compact


def _compact_postmortem(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    compact = _pick(
        value,
        "bug_id",
        "workflow_root",
        "duplicate_active_count",
        "production_gates",
        "postmortem_md_path",
        "postmortem_json_path",
    )
    if isinstance(value.get("state"), dict):
        compact["state"] = _pick(value["state"], "state", "branch", "worktree", "pr_url", "commit", "next_actions")
    timing_summary = _compact_timing_summary(value.get("timing_summary"))
    if timing_summary:
        compact["timing_summary"] = timing_summary
    flow_summary = _compact_phase_summary(value.get("flow_overhead_estimate"))
    if flow_summary:
        compact["flow_overhead_estimate"] = flow_summary
    if isinstance(value.get("phase_cost_table"), list) and value["phase_cost_table"]:
        compact["top_phase"] = value["phase_cost_table"][0]
    if isinstance(value.get("h6_summary"), dict):
        compact["h6_summary"] = _compact_phase_summary(value["h6_summary"])
    if isinstance(value.get("h7_code_intelligence"), dict):
        compact["h7_code_intelligence"] = _pick(
            value["h7_code_intelligence"],
            "status",
            "codegraph_status",
            "codegraph_freshness",
            "codegraph_freshness_ref",
            "fallback_used",
            "readiness_next_command",
            "fallback_reason",
        )
    active = value.get("active_workflows")
    if isinstance(active, list):
        compact["active_workflow_count"] = len(active)
    stale = value.get("stale_pr_check")
    stale_compact = _compact_stale_pr_check(stale)
    if stale_compact is not None:
        compact["stale_pr_check"] = stale_compact
    return compact


def _compact_finalizer(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    compact = _pick(value, "workflow_gate", "blocking", "bug_ids", "batch_mode", "source_merge_commit", "next_actions")
    if "close_sync" in value:
        compact["close_sync"] = _pick(value["close_sync"], "workflow_gate", "registry_root", "updated_bug_json", "merge_commit")
    if "close_sync_commit" in value:
        compact["close_sync_commit"] = _pick(value["close_sync_commit"], "workflow_gate", "branch", "pr_url", "commit", "next_command")
    if "close_sync_pr_merge" in value:
        compact["close_sync_pr_merge"] = _pick(value["close_sync_pr_merge"], "workflow_gate", "merge_commit", "blocking")
    if "cleanup" in value and isinstance(value["cleanup"], dict):
        compact["cleanup"] = _pick(value["cleanup"], "workflow_gate", "branch", "worktree", "sync_root", "blocking", "warnings")
    if "close_sync_cleanup" in value and isinstance(value["close_sync_cleanup"], dict):
        compact["close_sync_cleanup"] = _pick(
            value["close_sync_cleanup"],
            "workflow_gate",
            "branch",
            "worktree",
            "sync_root",
            "blocking",
            "warnings",
        )
    if "postmortem" in value:
        postmortem = _compact_postmortem(value["postmortem"])
        if postmortem:
            compact["postmortem"] = _pick(
                postmortem,
                "bug_id",
                "timing_summary",
                "flow_overhead_estimate",
                "top_phase",
                "h6_summary",
                "stale_pr_check",
                "production_gates",
                "postmortem_md_path",
                "postmortem_json_path",
            )
    return compact


def _compact_triage_ci_issue(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    compact = _pick(
        value,
        "detected_run_id",
        "classification_recommendation",
        "needs_bug_json",
        "failure_event_path",
        "context_pack_md_path",
        "triage_action",
    )
    issue = value.get("github_issue")
    if isinstance(issue, dict):
        compact["github_issue"] = _pick(issue, "number", "url", "title", "state")
    summary = value.get("summary")
    if isinstance(summary, dict):
        compact["diagnostic_status"] = summary.get("diagnostic_status")
        compact["workflow"] = summary.get("workflow")
        compact["run_url"] = summary.get("run_url")
        failed_jobs = summary.get("failed_jobs")
        if isinstance(failed_jobs, list):
            compact["failed_job_count"] = len(failed_jobs)
        modules = summary.get("suspected_modules")
        if modules:
            compact["suspected_modules"] = modules[:5] if isinstance(modules, list) else modules
        files = summary.get("suspected_files")
        if files:
            compact["suspected_files_count"] = len(files) if isinstance(files, list) else None
        if summary.get("reproduce_command"):
            compact["reproduce_command"] = summary.get("reproduce_command")
    linked = value.get("linked_bug")
    if isinstance(linked, dict) and linked:
        compact["linked_bug"] = _pick(linked, "bug_id", "path")
    suggested = value.get("suggested_bug")
    if isinstance(suggested, dict) and value.get("needs_bug_json"):
        compact["suggested_bug"] = _pick(suggested, "module", "severity", "title", "risk_area")
        required = suggested.get("required_verification")
        if isinstance(required, list):
            compact["suggested_bug"]["required_verification_count"] = len(required)
        scope = suggested.get("allowed_write_scope")
        if isinstance(scope, list):
            compact["suggested_bug"]["allowed_write_scope_count"] = len(scope)
    infra = value.get("infra_action")
    if isinstance(infra, dict):
        compact["infra_action"] = _pick(infra, "workflow_gate", "reason", "production_gates")
        actions = infra.get("next_actions")
        if isinstance(actions, list):
            compact["infra_action"]["next_actions"] = actions[:4]
    superseded = value.get("superseded_action")
    if isinstance(superseded, dict):
        compact["superseded_action"] = _pick(superseded, "workflow_gate", "reason", "next_command", "production_gates")
        run = superseded.get("superseding_run")
        if isinstance(run, dict):
            compact["superseded_action"]["superseding_run"] = _pick(run, "run_id", "run_url", "head_sha", "created_at")
    return compact


def _compact_promote_ci_issue(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    compact = _pick(value, "dry_run")
    triage = _compact_triage_ci_issue(value.get("triage"))
    if triage:
        compact["triage"] = triage
    if isinstance(value.get("infra_action"), dict):
        compact["infra_action"] = _pick(value["infra_action"], "workflow_gate", "reason")
    if isinstance(value.get("superseded_action"), dict):
        compact["superseded_action"] = _pick(value["superseded_action"], "workflow_gate", "reason", "next_command")
    if "triage_action" in value:
        compact["triage_action"] = value.get("triage_action")
    if isinstance(value.get("submit_bug"), dict):
        submit_bug = value["submit_bug"]
        compact["submit_bug"] = _pick(submit_bug, "workflow_gate", "bug_id", "state_path", "events_path", "next_command")
        if isinstance(submit_bug.get("fix_chain"), dict):
            compact["submit_bug"]["fix_chain"] = _pick(
                submit_bug["fix_chain"],
                "continue_to_fix_in_same_workflow",
                "registry_pr_only",
                "next_command",
            )
    return compact


def _compact_ci_issue_janitor(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    compact = _pick(
        value,
        "workflow_gate",
        "dry_run",
        "scanned_count",
        "superseded_count",
        "infra_count",
        "closed_count",
        "skipped_count",
        "failed_count",
        "production_gates",
        "next_command",
    )
    if value.get("closed_issues"):
        compact["closed_issues"] = value.get("closed_issues")
    if value.get("failed_issues"):
        compact["failed_issues"] = value.get("failed_issues")
    return compact


def _compact_payload(payload: dict[str, Any]) -> dict[str, Any]:
    schema = str(payload.get("schema_version") or "")
    if not schema:
        return payload
    compact = _pick(payload, "schema_version", "workflow_gate", "bug_id", "mode", "next_command")
    if payload.get("blocking"):
        compact["blocking"] = payload.get("blocking")
    if payload.get("warnings"):
        compact["warnings_count"] = len(payload.get("warnings") or [])
    if "ui_intake_hints" in payload and isinstance(payload.get("ui_intake_hints"), dict):
        compact["ui_intake_hints"] = _pick(
            payload["ui_intake_hints"],
            "ui_issue",
            "ui_route",
            "scope_source",
            "reproduce_required",
            "visual_acceptance_required",
        )
        compact["ui_intake_hints"]["scope_count"] = len(payload["ui_intake_hints"].get("ui_component_scope") or [])
        compact["ui_intake_hints"]["recommended_verification_count"] = len(payload["ui_intake_hints"].get("recommended_verification") or [])
    if "workflow_efficiency_recommendations" in payload and isinstance(payload.get("workflow_efficiency_recommendations"), dict):
        compact["workflow_efficiency_recommendations"] = _pick(
            payload["workflow_efficiency_recommendations"],
            "batch_candidate",
            "docs_only_merge_with_related_code",
            "compact_success_output",
            "full_json_on_failure_only",
        )

    if schema.endswith("_run_v1"):
        compact.update(_pick(payload, "mode"))
        if "start" in payload:
            compact["start"] = _compact_start(payload["start"])
        if "finish" in payload:
            compact["finish"] = _compact_finish(payload["finish"])
        if "pr_automation" in payload:
            compact["pr_automation"] = _compact_pr_automation(payload["pr_automation"])
        if "merge" in payload:
            compact["merge"] = _compact_merge(payload["merge"])
        if "finalizer" in payload:
            compact["finalizer"] = _compact_finalizer(payload["finalizer"])
        if "close_sync" in payload and "finalizer" not in payload:
            compact["close_sync"] = _pick(payload["close_sync"], "workflow_gate", "registry_root", "updated_bug_json", "merge_commit")
        if "close_sync_commit" in payload and "finalizer" not in payload:
            compact["close_sync_commit"] = _pick(payload["close_sync_commit"], "workflow_gate", "branch", "pr_url", "commit")
        if "cleanup" in payload and isinstance(payload["cleanup"], dict):
            compact["cleanup"] = _pick(payload["cleanup"], "workflow_gate", "branch", "worktree", "sync_root", "blocking", "warnings")
        if "active_decision" in payload:
            compact["active_decision"] = _pick(payload["active_decision"], "decision", "workflow_gate", "next_command", "blocking", "warnings")
    elif schema.endswith("_doctor_v1"):
        compact.update(_pick(payload, "restart_recommended", "install_client_next_command"))
        if "client_manifest" in payload:
            compact["client_manifest"] = _pick(
                payload["client_manifest"],
                "codex_skill_status",
                "claude_command_status",
                "restart_recommended",
            )
        if "h7_code_intelligence" in payload:
            compact["h7_code_intelligence"] = _pick(
                payload["h7_code_intelligence"],
                "workflow_gate",
                "codegraph_status",
                "codegraph_freshness",
                "codegraph_freshness_ref",
                "understand_anything_status",
                "understand_anything_graph_exists",
                "understand_anything_next_command",
                "fallback_used",
                "readiness_next_command",
                "fallback_reason",
            )
        if "bug_id_allocation" in payload:
            compact["bug_id_allocation"] = _pick(
                payload["bug_id_allocation"],
                "next_number",
                "allocator_max_number",
                "observed_max_number",
                "github_max_number",
                "github_scanned",
            )
    elif schema.endswith("_start_v1"):
        compact.update(_compact_start(payload) or {})
    elif schema.endswith("_finish_v1") or schema.endswith("_finish_batch_v1"):
        compact.update(_compact_finish(payload) or {})
        if "batch_id" in payload:
            compact["batch_id"] = payload.get("batch_id")
            compact["bug_ids"] = payload.get("bug_ids")
    elif schema.endswith("_resume_v1"):
        compact.update(
            _pick(
                payload,
                "workflow_root",
                "worktree",
                "planned_worktree",
                "branch",
                "planned_branch",
                "task_card_json",
                "task_card_md",
                "state_path",
                "events_path",
                "stop_conditions",
            )
        )
    elif schema == "aistock_nightly_intake_smoke_v1":
        compact.update(
            _pick(
                payload,
                "dry_run",
                "github_writes",
                "candidate_history_path",
                "nightly_failed_stages",
                "unexpected_dirty_paths",
                "production_gates",
            )
        )
        if isinstance(payload.get("artifacts"), dict):
            compact["artifacts"] = _pick(payload["artifacts"], "summary", "context", "github_issue_payload")
    elif schema == "aistock_batch_workflow_smoke_v1":
        compact.update(
            _pick(
                payload,
                "dry_run",
                "github_writes",
                "batch_id",
                "bug_ids",
                "unexpected_dirty_paths",
                "production_gates",
            )
        )
        if isinstance(payload.get("artifacts"), dict):
            compact["artifacts"] = _pick(payload["artifacts"], "batch_state", "finish_plan", "pr_body")
        if isinstance(payload.get("finish"), dict):
            compact["finish"] = _pick(payload["finish"], "workflow_gate", "closure_ready", "validation_evidence_count")
    elif schema.endswith("_smoke_v1"):
        compact.update(
            _pick(
                payload,
                "dry_run",
                "unexpected_dirty_paths",
                "production_gates",
            )
        )
        compact["changed_files_count"] = len(payload.get("changed_files") or [])
        if isinstance(payload.get("fast_path"), dict):
            compact["fast_path"] = _pick(payload["fast_path"], "task_tier", "module", "workflow_gate")
        if "postmortem_preview" in payload:
            preview = _compact_postmortem(payload["postmortem_preview"])
            if preview:
                compact["postmortem_preview"] = _pick(preview, "bug_id", "timing_summary", "stale_pr_check")
    elif schema.endswith("_postmortem_v1"):
        compact.update(_compact_postmortem(payload) or {})
    elif schema.endswith("_close_sync_v1") or schema.endswith("_close_sync_batch_v1"):
        compact.update(
            _pick(
                payload,
                "bug_ids",
                "source_bug_json",
                "registry_root",
                "current_status",
                "github_issue_url",
                "merged_pr",
                "merge_commit",
                "production_gates",
                "dry_run",
                "updated_bug_json",
                "updated_bug_jsons",
                "timing_summary",
            )
        )
        compact["validation_evidence_count"] = len(payload.get("validation_evidence") or [])
        if "github_issue_sync" in payload:
            compact["github_issue_sync"] = _pick(payload["github_issue_sync"], "status", "channel", "fallback_used")
    elif schema.endswith("_cleanup_v1"):
        compact.update(
            _pick(
                payload,
                "branch",
                "worktree",
                "canonical_root",
                "sync_root",
                "merged_into_origin_main",
                "worktree_clean",
                "dry_run",
                "duration_seconds",
                "blocking",
                "warnings",
            )
        )
        if "actions" in payload:
            compact["actions_count"] = len(payload.get("actions") or [])
        if "applied" in payload:
            compact["applied_count"] = len(payload.get("applied") or [])
        if "complete_state" in payload:
            compact["complete_state"] = _pick(payload["complete_state"], "state", "updated_at")
    elif schema.endswith("_merge_finalizer_v1"):
        compact.update(_compact_finalizer(payload) or {})
    elif schema.endswith("_triage_ci_issue_v1"):
        compact.update(_compact_triage_ci_issue(payload) or {})
    elif schema.endswith("_ci_issue_janitor_v1"):
        compact.update(_compact_ci_issue_janitor(payload) or {})
    elif schema.endswith("_promote_ci_issue_v1"):
        compact.update(_compact_promote_ci_issue(payload) or {})
    elif schema.endswith("_watch_ci_v1") or schema.endswith("_check_watch_v1"):
        compact.update(_pick(payload, "pr_url", "state", "check_summary", "next_actions"))
    elif schema.endswith("_missing_bug_record_v1"):
        compact.update(
            _pick(
                payload,
                "reason",
                "github_issue",
                "inferred_module",
                "inferred_severity",
                "blocking",
                "warnings",
                "next_actions",
                "next_command",
            )
        )
    else:
        for key in (
            "module",
            "status",
            "task_tier",
            "required_verification",
            "recommended_verification",
            "production_gates",
            "pr_url",
            "branch",
            "worktree",
            "state_path",
            "events_path",
        ):
            if key in payload:
                compact[key] = payload[key]
        if "code_intelligence_hint" in payload and isinstance(payload["code_intelligence_hint"], dict):
            compact["code_intelligence_hint"] = _pick(
                payload["code_intelligence_hint"],
                "workflow_gate",
                "latest_freshness",
                "artifact_path",
                "consume_command",
                "readiness_next_command",
                "blocking_for_issue_workflow",
            )

    compact["full_payload"] = "use --output-format full-json or --output <tmp/issue_workflow/...json> for details"
    return compact


def _short_status_word(gate: str | None) -> str:
    value = (gate or "").lower()
    if value in {"passed", "ready", "ready_for_pr", "ready_for_apply", "checks_passed", "planned", "promoted", "complete", "merged", "merged_close_synced"}:
        return "PASS"
    if "blocked" in value or value in {"failed", "checks_failed", "validation_evidence_missing"}:
        return "BLOCKED"
    if "warning" in value or "pending" in value:
        return "WAIT"
    return "INFO"


def _format_summary_lines(payload: dict[str, Any], compact: dict[str, Any]) -> list[str]:
    schema = str(payload.get("schema_version") or compact.get("schema_version") or "")
    gate = str(compact.get("workflow_gate") or payload.get("workflow_gate") or "unknown")
    bug_id = str(compact.get("bug_id") or payload.get("bug_id") or "").strip()
    prefix = f"{_short_status_word(gate)} {bug_id}".strip()
    if schema == "aistock_issue_workflow_watch_ci_v1":
        checks = compact.get("check_summary") if isinstance(compact.get("check_summary"), dict) else payload.get("check_summary") or {}
        return [
            (
                f"{prefix} workflow_gate={gate} pr={compact.get('pr_url') or 'unknown'} "
                f"passed={checks.get('passed_count', 0)} pending={checks.get('pending_count', 0)} "
                f"failed={checks.get('failed_count', 0)} next={';'.join(payload.get('next_actions') or compact.get('next_actions') or []) or 'none'}"
            )
        ]
    if schema == "aistock_nightly_intake_smoke_v1":
        return [
            (
                f"{prefix} nightly-intake-smoke workflow_gate={gate} github_writes={str(compact.get('github_writes')).lower()} "
                f"unexpected_dirty_paths={len(compact.get('unexpected_dirty_paths') or [])} "
                f"next={compact.get('next_command') or 'none'}"
            )
        ]
    if schema.endswith("_triage_ci_issue_v1"):
        return [
            (
                f"{prefix} triage-ci-issue workflow_gate={gate} classification={compact.get('classification_recommendation') or 'unknown'} "
                f"needs_bug_json={str(bool(compact.get('needs_bug_json'))).lower()} next={compact.get('next_command') or compact.get('triage_action') or 'none'}"
            )
        ]
    if schema.endswith("_promote_ci_issue_v1"):
        triage = compact.get("triage") if isinstance(compact.get("triage"), dict) else {}
        return [
            (
                f"{prefix} promote-ci-issue workflow_gate={gate} classification={triage.get('classification_recommendation') or 'unknown'} "
                f"next={compact.get('next_command') or compact.get('triage_action') or 'none'}"
            )
        ]
    if schema.endswith("_start_v1"):
        worktree_plan = compact.get("worktree_plan") if isinstance(compact.get("worktree_plan"), dict) else {}
        return [
            (
                f"{prefix} workflow_gate={gate} module={compact.get('module') or 'unknown'} "
                f"worktree_created={str(bool(worktree_plan.get('created'))).lower()} "
                f"context={compact.get('context_pack_md') or 'not_generated'}"
            )
        ]
    if schema.endswith("_finish_v1") or schema.endswith("_finish_batch_v1"):
        return [
            (
                f"{prefix} workflow_gate={gate} closure_ready={str(bool(compact.get('closure_ready'))).lower()} "
                f"validation_evidence={compact.get('validation_evidence_count', 0)} pr_body={compact.get('pr_body_path') or 'not_generated'}"
            )
        ]
    if schema.endswith("_doctor_v1"):
        return [
            (
                f"{prefix} doctor workflow_gate={gate} restart_recommended={str(bool(compact.get('restart_recommended'))).lower()} "
                f"warnings={compact.get('warnings_count', 0)} next={compact.get('next_command') or 'none'}"
            )
        ]
    if schema.endswith("_smoke_v1") or schema == "aistock_batch_workflow_smoke_v1":
        return [
            (
                f"{prefix} workflow_gate={gate} dry_run={str(bool(compact.get('dry_run'))).lower()} "
                f"unexpected_dirty_paths={len(compact.get('unexpected_dirty_paths') or [])} next={compact.get('next_command') or 'none'}"
            )
        ]
    if schema.endswith("_run_v1"):
        return [
            (
                f"{prefix} workflow_gate={gate} mode={compact.get('mode') or payload.get('mode') or 'unknown'} "
                f"next={compact.get('next_command') or 'none'}"
            )
        ]
    return [
        (
            f"{prefix} workflow_gate={gate} "
            f"next={compact.get('next_command') or 'none'}"
        )
    ]


def _emit(payload: dict[str, Any], output: str | None = None, output_format: str = "compact") -> None:
    output_path = _resolve_output_path(output)
    if output_path:
        _write_json(output_path, payload)
    if output_format == "full-json":
        sys.stdout.write(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
        return
    compact = _compact_payload(payload)
    if output_format == "summary":
        sys.stdout.write("\n".join(_format_summary_lines(payload, compact)) + "\n")
        return
    sys.stdout.write(json.dumps(compact, ensure_ascii=True, indent=2, sort_keys=True) + "\n")


def _emit_args(payload: dict[str, Any], args: argparse.Namespace) -> None:
    _emit(payload, getattr(args, "output", None), getattr(args, "output_format", "compact"))


def _repo_rel(path: Path, root: Path | None = None) -> str:
    root = root or REPO_ROOT
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except Exception:
        return str(path).replace("\\", "/")


def _git(args: list[str], cwd: Path | None = None, check: bool = True) -> str:
    cwd = cwd or REPO_ROOT
    proc = subprocess.run(["git", *args], cwd=str(cwd), text=True, capture_output=True, check=False)
    if check and proc.returncode != 0:
        raise WorkflowError(proc.stderr.strip() or proc.stdout.strip() or f"git {' '.join(args)} failed")
    return proc.stdout.strip()


def _run_command(args: list[str], cwd: Path | None = None, timeout: int = 30) -> dict[str, Any]:
    cwd = cwd or REPO_ROOT
    try:
        proc = subprocess.run(
            args,
            cwd=str(cwd),
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            check=False,
            timeout=timeout,
        )
        return {
            "ok": proc.returncode == 0,
            "returncode": proc.returncode,
            "stdout": proc.stdout.strip(),
            "stderr": proc.stderr.strip(),
        }
    except Exception as exc:
        return {"ok": False, "returncode": None, "stdout": "", "stderr": str(exc)}


def _parse_git_porcelain_path(line: str) -> str:
    raw = line.rstrip()
    if not raw.strip():
        return ""
    if len(raw) >= 3 and raw[2] == " ":
        path = raw[3:]
    elif len(raw) >= 2 and raw[1] == " ":
        # _run_command strips stdout; recover paths from lines like "M path".
        path = raw[2:]
    else:
        path = raw[3:] if len(raw) > 3 else raw
    if " -> " in path:
        path = path.split(" -> ", 1)[1]
    return path.strip().strip('"')


def _workflow_dir(bug_id: str, root: Path | None = None) -> Path:
    return (root or REPO_ROOT) / WORKFLOW_ROOT / bug_id


def _state_path(bug_id: str, root: Path | None = None) -> Path:
    return _workflow_dir(bug_id, root) / "state.json"


def _events_path(bug_id: str, root: Path | None = None) -> Path:
    return _workflow_dir(bug_id, root) / "events.jsonl"


def _task_card_json_path(bug_id: str, root: Path | None = None) -> Path:
    return _workflow_dir(bug_id, root) / "task-card.json"


def _task_card_md_path(bug_id: str, root: Path | None = None) -> Path:
    return _workflow_dir(bug_id, root) / "task-card.md"


def _active_index_path(root: Path | None = None) -> Path:
    return (root or REPO_ROOT) / WORKFLOW_ROOT / "index" / "active_bugs.json"


def _load_state(bug_id: str, root: Path | None = None) -> dict[str, Any] | None:
    path = _state_path(bug_id, root)
    if not path.exists():
        return None
    return _load_json(path)


def _append_event(
    bug_id: str,
    *,
    event: str,
    state: str,
    actor: str = "aistock_issue_workflow.py",
    command: str | None = None,
    cwd: Path | None = None,
    result: str = "ok",
    evidence: dict[str, Any] | None = None,
    root: Path | None = None,
    duration_seconds: float | None = None,
) -> dict[str, Any]:
    event_payload = {
        "timestamp": _utc_now(),
        "client": os.environ.get("AISTOCK_WORKFLOW_CLIENT") or os.environ.get("CODEX_WORKFLOW_CLIENT") or "unknown",
        "actor": actor,
        "event": event,
        "state": state,
        "command": command,
        "cwd": str((cwd or root or REPO_ROOT).resolve()),
        "duration_seconds": round(duration_seconds, 3) if duration_seconds is not None else None,
        "result": result,
        "evidence": evidence or {},
    }
    path = _events_path(bug_id, root)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event_payload, ensure_ascii=False, sort_keys=True) + "\n")
    return event_payload


def _parse_utc_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _event_phase(event: dict[str, Any]) -> str:
    raw = str(event.get("event") or event.get("state") or "unknown")
    if raw.startswith("state:"):
        raw = raw.split(":", 1)[1]
    if raw.startswith("command:"):
        return raw.split(":", 1)[1] or "command"
    if raw in {"active_worktree_decision"}:
        return "active_worktree_guard"
    return raw or "unknown"


def _read_events(bug_id: str, root: Path | None = None) -> list[dict[str, Any]]:
    path = _events_path(bug_id, root)
    if not path.exists():
        return []
    events: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            events.append(payload)
    return events


def _prior_postmortem_paths(bug_id: str, roots: list[Path]) -> list[Path]:
    candidates: list[Path] = []
    known_roots = roots + [REPO_ROOT, _canonical_root()]
    for root in known_roots:
        candidates.extend(
            [
                root / WORKFLOW_ROOT / bug_id / "postmortem.json",
                root / WORKFLOW_ROOT / bug_id / "postmortem-pre-cleanup.json",
            ]
        )
    worktree_root = _default_worktree_root()
    if worktree_root.exists():
        candidates.extend(worktree_root.glob(f"*/{WORKFLOW_ROOT.as_posix()}/{bug_id}/postmortem*.json"))
    return _unique_existing_paths(candidates)


def _load_prior_postmortem(bug_id: str, roots: list[Path]) -> dict[str, Any] | None:
    paths = _prior_postmortem_paths(bug_id, roots)
    if not paths:
        return None
    def sort_key(path: Path) -> tuple[float, float, str]:
        try:
            payload = _load_json(path)
            timing = payload.get("timing_summary") if isinstance(payload, dict) else {}
            event_count = float((timing or {}).get("event_count") or 0)
            known_duration = float((timing or {}).get("known_duration_seconds") or 0)
            return (event_count, known_duration, str(path))
        except OSError:
            return (0.0, 0.0, str(path))
        except WorkflowError:
            return (0.0, 0.0, str(path))

    path = sorted(paths, key=sort_key)[-1]
    try:
        payload = _load_json(path)
    except WorkflowError:
        return None
    payload = dict(payload)
    payload.setdefault("schema_version", "aistock_issue_workflow_postmortem_v1")
    payload["workflow_gate"] = payload.get("workflow_gate") or "artifact_fallback"
    payload["artifact_fallback"] = {
        "reason": "workflow_state_missing_or_cleaned",
        "path": str(path),
    }
    return payload


def _workflow_timing_summary(bug_id: str, root: Path | None = None) -> dict[str, Any]:
    events = _read_events(bug_id, root)
    events = sorted(events, key=lambda item: str(item.get("timestamp") or ""))
    phases: dict[str, dict[str, Any]] = {}
    previous_ts: datetime | None = None
    started_at: str | None = None
    ended_at: str | None = None
    known_duration = 0.0
    inferred_duration = 0.0

    for event in events:
        ts = _parse_utc_timestamp(str(event.get("timestamp") or ""))
        if ts:
            started_at = started_at or event.get("timestamp")
            ended_at = event.get("timestamp")
        phase = _event_phase(event)
        bucket = phases.setdefault(
            phase,
            {
                "event_count": 0,
                "known_duration_seconds": 0.0,
                "inferred_since_previous_seconds": 0.0,
                "first_at": event.get("timestamp"),
                "last_at": event.get("timestamp"),
            },
        )
        bucket["event_count"] += 1
        bucket["last_at"] = event.get("timestamp")
        duration = event.get("duration_seconds")
        if isinstance(duration, (int, float)):
            bucket["known_duration_seconds"] = round(float(bucket["known_duration_seconds"]) + float(duration), 3)
            known_duration += float(duration)
        if ts and previous_ts:
            delta = max(0.0, (ts - previous_ts).total_seconds())
            bucket["inferred_since_previous_seconds"] = round(
                float(bucket["inferred_since_previous_seconds"]) + delta,
                3,
            )
            inferred_duration += delta
        if ts:
            previous_ts = ts

    queue_seconds = _phase_seconds(phases, "discovered")
    context_seconds = _phase_seconds(phases, "context_ready")
    active_fix_seconds = _phase_seconds(phases, "fix_in_progress") + _phase_seconds(phases, "fix_applied") + context_seconds
    local_validation_seconds = _phase_seconds(phases, "validation_planned") + _phase_seconds(phases, "validation_running") + _phase_seconds(phases, "validation_passed")
    pr_ci_seconds = _phase_seconds(phases, "pr_opened") + _phase_seconds(phases, "ci_running") + _phase_seconds(phases, "ci_green") + _phase_seconds(phases, "gh_pr_create")
    merge_aftercare_seconds = sum(_phase_seconds(phases, phase) for phase in ("merged", "close_synced", "cleanup_done", "complete", "close_sync_apply", "close_sync_persisted"))

    return {
        "schema_version": "aistock_issue_workflow_timing_summary_v1",
        "bug_id": bug_id,
        "event_count": len(events),
        "started_at": started_at,
        "ended_at": ended_at,
        "known_duration_seconds": round(known_duration, 3),
        "inferred_elapsed_seconds": round(inferred_duration, 3),
        "phases": phases,
        "queue_seconds": round(queue_seconds, 3) if queue_seconds else None,
        "active_fix_seconds": round(active_fix_seconds, 3) if active_fix_seconds else None,
        "local_validation_seconds": round(local_validation_seconds, 3) if local_validation_seconds else None,
        "pr_ci_seconds": round(pr_ci_seconds, 3) if pr_ci_seconds else None,
        "merge_aftercare_seconds": round(merge_aftercare_seconds, 3) if merge_aftercare_seconds else None,
        "code_repair_seconds": round(active_fix_seconds, 3) if active_fix_seconds else None,
        "notes": [
            "known_duration_seconds comes from command-level telemetry when available",
            "inferred_elapsed_seconds is wall-clock distance between recorded events and may include human/CI wait time",
            "code_repair_seconds is intentionally not guessed unless the agent records explicit repair events",
        ],
    }




def _augment_timing_with_issue_record(timing: dict[str, Any], state: dict[str, Any], root: Path) -> dict[str, Any]:
    source_path = _state_issue_json_path(root, state)
    if not source_path or not source_path.exists():
        return timing
    try:
        record = _load_json(source_path)
    except WorkflowError:
        return timing
    created_at = _parse_utc_timestamp(str(record.get("created_at") or record.get("first_seen_at") or ""))
    started_at = _parse_utc_timestamp(str(timing.get("started_at") or ""))
    if created_at and started_at and started_at > created_at:
        timing = dict(timing)
        queue_seconds = round((started_at - created_at).total_seconds(), 3)
        existing = float(timing.get("queue_seconds") or 0)
        timing["queue_seconds"] = max(existing, queue_seconds)
        timing["issue_created_at"] = record.get("created_at") or record.get("first_seen_at")
        timing["active_work_started_at"] = timing.get("started_at")
        timing.setdefault("notes", []).append(
            "queue_seconds uses BUG created_at to first workflow event when available, so queue time is not hidden."
        )
    return timing


def _phase_seconds(phases: dict[str, Any], phase: str) -> float:
    item = phases.get(phase)
    if not isinstance(item, dict):
        return 0.0
    return max(float(item.get("known_duration_seconds") or 0), float(item.get("inferred_since_previous_seconds") or 0))


def _phase_cost_table(timing: dict[str, Any]) -> list[dict[str, Any]]:
    phases = timing.get("phases") if isinstance(timing, dict) else {}
    if not isinstance(phases, dict):
        return []
    rows: list[dict[str, Any]] = []
    for phase, item in sorted(phases.items()):
        if not isinstance(item, dict):
            continue
        known = round(float(item.get("known_duration_seconds") or 0), 3)
        inferred = round(float(item.get("inferred_since_previous_seconds") or 0), 3)
        rows.append(
            {
                "phase": str(phase),
                "event_count": int(item.get("event_count") or 0),
                "known_seconds": known,
                "inferred_seconds": inferred,
                "dominant_seconds": max(known, inferred),
            }
        )
    return rows


def _h6_summary(timing: dict[str, Any], context_metrics: dict[str, Any], artifact_metrics: dict[str, Any]) -> dict[str, Any]:
    phase_rows = _phase_cost_table(timing)
    top_phase = max(phase_rows, key=lambda item: item["dominant_seconds"], default=None)
    context_token_values = [
        int(item.get("estimated_tokens") or 0)
        for item in context_metrics.values()
        if isinstance(item, dict) and item.get("exists") is not False and "estimated_tokens" in item
    ]
    artifact_token_values = [
        int(item.get("estimated_tokens") or 0)
        for item in artifact_metrics.values()
        if isinstance(item, dict) and item.get("exists") is not False and "estimated_tokens" in item
    ]
    context_tokens: int | None = sum(context_token_values) if context_token_values else None
    artifact_tokens: int | None = sum(artifact_token_values) if artifact_token_values else None
    total_tokens: int | None = (
        (context_tokens or 0) + (artifact_tokens or 0)
        if context_tokens is not None or artifact_tokens is not None
        else None
    )
    return {
        "schema_version": "aistock_issue_workflow_h6_summary_v1",
        "event_count": timing.get("event_count"),
        "known_duration_seconds": timing.get("known_duration_seconds"),
        "inferred_elapsed_seconds": timing.get("inferred_elapsed_seconds"),
        "top_phase": top_phase,
        "context_estimated_tokens": context_tokens,
        "artifact_estimated_tokens": artifact_tokens,
        "total_estimated_tokens": total_tokens,
        "token_usage_status": "estimated" if total_tokens is not None else "unknown",
        "queue_seconds": timing.get("queue_seconds"),
        "active_fix_seconds": timing.get("active_fix_seconds"),
        "local_validation_seconds": timing.get("local_validation_seconds"),
        "pr_ci_seconds": timing.get("pr_ci_seconds"),
        "merge_aftercare_seconds": timing.get("merge_aftercare_seconds"),
        "code_repair_seconds": timing.get("code_repair_seconds"),
        "code_repair_note": "active_fix_seconds is derived from workflow state events; exact editor time is recorded only when clients emit explicit repair events",
    }


def _code_intelligence_readiness(code_intel: dict[str, Any] | None) -> dict[str, Any]:
    code_intel = code_intel or {}
    codegraph = code_intel.get("codegraph") if isinstance(code_intel.get("codegraph"), dict) else {}
    freshness = code_intel.get("codegraph_freshness") if isinstance(code_intel.get("codegraph_freshness"), dict) else {}
    latest_freshness = freshness.get("latest") if isinstance(freshness.get("latest"), dict) else {}
    ua = code_intel.get("understand_anything") if isinstance(code_intel.get("understand_anything"), dict) else {}
    bootstrap = code_intel.get("bootstrap_commands") if isinstance(code_intel.get("bootstrap_commands"), dict) else {}
    context = code_intel.get("context") if isinstance(code_intel.get("context"), dict) else {}
    fallback = context.get("fallback") if isinstance(context.get("fallback"), dict) else {}
    fallback_used = bool(
        code_intel.get("fallback_used")
        or fallback.get("used")
        or str(code_intel.get("status") or "").lower() == "fallback"
        or str(codegraph.get("status") or "").lower() in {"unavailable", "missing_index", "stale"}
    )
    codegraph_status = str(codegraph.get("status") or code_intel.get("status") or "unknown")
    fallback_reason = fallback.get("reason") or code_intel.get("fallback_reason")
    if not fallback_reason and fallback_used:
        fallback_reason = "codegraph_" + codegraph_status
    next_command = None
    if fallback_used:
        next_command = codegraph.get("bootstrap_command") or bootstrap.get("codegraph") or "codegraph init -i"
    ua_status = str(ua.get("status") or "unknown")
    ua_next_command = None
    if ua_status == "not_configured":
        ua_next_command = ua.get("configure_command") or bootstrap.get("understand_anything_configure")
    elif not ua.get("graph_exists"):
        ua_next_command = ua.get("generate_graph_command") or bootstrap.get("understand_anything_generate_graph")
    return {
        "schema_version": "aistock_issue_workflow_h7_code_intelligence_readiness_v1",
        "workflow_gate": "warning" if fallback_used else "ready",
        "status": code_intel.get("status") or codegraph_status,
        "codegraph_status": codegraph_status,
        "codegraph_freshness": latest_freshness.get("freshness"),
        "codegraph_freshness_ref": latest_freshness.get("artifact_path"),
        "understand_anything_status": ua_status,
        "understand_anything_graph_exists": ua.get("graph_exists"),
        "understand_anything_next_command": ua_next_command,
        "fallback_used": fallback_used,
        "fallback_reason": fallback_reason,
        "readiness_next_command": next_command,
        "blocking_for_issue_workflow": False,
    }


def _code_intelligence_efficiency_summary(code_intel: dict[str, Any] | None) -> dict[str, Any]:
    """Record graph-first usage without expanding graph artifacts in successful output."""

    code_intel = code_intel or {}
    affected_tests = code_intel.get("affected_tests") if isinstance(code_intel.get("affected_tests"), dict) else {}
    context = code_intel.get("context") if isinstance(code_intel.get("context"), dict) else {}
    fallback = context.get("fallback") if isinstance(context.get("fallback"), dict) else {}
    ua = code_intel.get("understand_anything") if isinstance(code_intel.get("understand_anything"), dict) else {}
    context_ref = code_intel.get("context_ref")
    affected_tests_ref = code_intel.get("affected_tests_ref")
    ua_summary_ref = code_intel.get("understand_anything_summary_ref")
    graph_refs = [item for item in (context_ref, affected_tests_ref, ua_summary_ref) if item]
    fallback_used = bool(
        code_intel.get("fallback_used")
        or fallback.get("used")
        or str(code_intel.get("status") or "").lower() == "fallback"
    )
    fallback_reason = fallback.get("reason") or code_intel.get("fallback_reason")
    if not fallback_reason and fallback_used:
        fallback_reason = "graph_context_fallback"
    broad_scan_avoided = bool(graph_refs) and not fallback_used
    return {
        "schema_version": "aistock_issue_workflow_code_intelligence_efficiency_v1",
        "graph_first_context_used": broad_scan_avoided,
        "codegraph_context_ref": context_ref,
        "affected_tests_ref": affected_tests_ref,
        "understand_anything_summary_ref": ua_summary_ref,
        "affected_tests_count": code_intel.get("affected_tests_count") or len(affected_tests.get("suggested_tests") or []),
        "affected_quality": code_intel.get("affected_quality"),
        "fallback_used": fallback_used,
        "fallback_reason": fallback_reason,
        "broad_scan_avoided": broad_scan_avoided,
        "estimated_broad_scan_tokens_avoided": 8000 if broad_scan_avoided else None,
        "understand_anything_status": ua.get("status"),
        "full_graph_payload_included": False,
    }


def _code_intelligence_hint(root: Path | None = None) -> dict[str, Any]:
    freshness = code_intelligence.latest_codegraph_freshness(root or REPO_ROOT)
    latest = freshness.get("latest") if isinstance(freshness.get("latest"), dict) else {}
    freshness_value = latest.get("freshness") if latest else None
    return {
        "schema_version": "aistock_issue_workflow_code_intelligence_hint_v1",
        "workflow_gate": freshness.get("workflow_gate") or "warning",
        "blocking_for_issue_workflow": False,
        "latest_freshness": freshness_value,
        "artifact_path": latest.get("artifact_path") if latest else None,
        "summary_ref": latest.get("summary_ref") if latest else None,
        "consume_command": "python scripts/code_intelligence_adapter.py latest-freshness",
        "readiness_next_command": None
        if freshness_value == "fresh"
        else "python scripts/code_intelligence_adapter.py freshness --skip-external",
    }


def _update_active_index(bug_id: str, state_payload: dict[str, Any], *, root: Path | None = None) -> dict[str, Any]:
    root = root or REPO_ROOT
    path = _active_index_path(root)
    existing: dict[str, Any] = {}
    if path.exists():
        try:
            existing = _load_json(path)
        except WorkflowError:
            existing = {}
    index = dict(existing.get("active_bugs") or existing)
    key = bug_id.strip().upper()
    if not _state_is_active(state_payload):
        index.pop(key, None)
    else:
        active_worktree = state_payload.get("worktree") or state_payload.get("cwd") or str(root)
        index[key] = {
            "bug_id": key,
            "active_state": state_payload.get("state"),
            "branch": state_payload.get("branch"),
            "planned_branch": state_payload.get("planned_branch"),
            "worktree": active_worktree,
            "planned_worktree": state_payload.get("planned_worktree"),
            "pr_url": state_payload.get("pr_url"),
            "last_event_at": state_payload.get("updated_at"),
            "next_command": _next_command_for_state(key, state_payload),
        }
    payload = {
        "schema_version": "aistock_issue_workflow_active_index_v1",
        "updated_at": _utc_now(),
        "active_bugs": index,
    }
    _write_json(path, payload)
    return payload


def _write_state(
    bug_id: str,
    *,
    state: str,
    root: Path | None = None,
    next_actions: list[str] | None = None,
    stop_reason: str | None = None,
    **fields: Any,
) -> dict[str, Any]:
    root = root or REPO_ROOT
    previous = _load_state(bug_id, root) or {}
    payload = {
        **previous,
        **fields,
        "schema_version": "aistock_issue_workflow_state_v1",
        "bug_id": bug_id,
        "state": state,
        "updated_at": _utc_now(),
        "cwd": str(root.resolve()),
        "next_actions": next_actions or fields.get("next_actions") or [],
    }
    for key, value in fields.items():
        if value is None:
            payload.pop(key, None)
    if stop_reason:
        payload["stop_reason"] = stop_reason
    _write_json(_state_path(bug_id, root), payload)
    _append_event(
        bug_id,
        event=f"state:{state}",
        state=state,
        root=root,
        evidence={key: payload.get(key) for key in ("branch", "worktree", "commit", "pr_url") if payload.get(key)},
    )
    _update_active_index(bug_id, payload, root=root)
    return payload


def _default_worktree_root() -> Path:
    override = os.environ.get("AISTOCK_WORKTREE_ROOT")
    if override:
        return Path(override)
    if REPO_ROOT.parent.name == "AIstock_worktrees":
        return REPO_ROOT.parent
    if REPO_ROOT.name.lower() == "aistock":
        return REPO_ROOT.parent / "AIstock_worktrees"
    return REPO_ROOT.parent / "AIstock_worktrees"


def _bug_files(bugs_root: Path | None = None) -> list[Path]:
    bugs_root = bugs_root or BUGS_ROOT
    return sorted(path for path in bugs_root.glob("*.json") if not path.name.startswith("."))


def _bugs_root(root: Path | None = None) -> Path:
    return (root / "tests" / "aistock_validation" / "bugs") if root else BUGS_ROOT


def _allocator_path(root: Path | None = None) -> Path:
    return _bugs_root(root) / ".bug_id_allocator.json"


def _bug_id_number(value: str | None) -> int | None:
    match = BUG_ID_RE.search(value or "")
    return int(match.group(1)) if match else None


def _unique_existing_paths(paths: list[Path]) -> list[Path]:
    seen: set[str] = set()
    unique: list[Path] = []
    for path in paths:
        try:
            key = str(path.resolve()).lower()
        except OSError:
            key = str(path.absolute()).lower()
        if key in seen:
            continue
        seen.add(key)
        if path.exists():
            unique.append(path)
    return unique


def _bug_id_scan_roots(root: Path | None = None) -> list[Path]:
    repo_root = root or REPO_ROOT
    candidates = [
        _bugs_root(repo_root),
        _bugs_root(REPO_ROOT),
        _bugs_root(_canonical_root()),
    ]
    worktree_root = _default_worktree_root()
    if worktree_root.exists():
        for child in worktree_root.iterdir():
            if child.is_dir():
                candidates.append(_bugs_root(child))
    for item in _parse_worktree_list():
        worktree = item.get("worktree")
        if worktree:
            candidates.append(_bugs_root(Path(worktree)))
    return _unique_existing_paths(candidates)


def _bug_id_reservation_root() -> Path:
    override = os.environ.get("AISTOCK_BUG_ID_RESERVATION_ROOT")
    return Path(override) if override else _default_worktree_root() / ".locks" / "bug-id-reservations"


def _scan_bug_id_reservations() -> list[dict[str, Any]]:
    reservation_root = _bug_id_reservation_root()
    if not reservation_root.exists():
        return []
    sources: list[dict[str, Any]] = []
    for path in sorted(reservation_root.glob("BUG-*.json")):
        number = _bug_id_number(path.name)
        if not number:
            continue
        sources.append(
            {
                "bug_id": f"BUG-{number:03d}",
                "number": number,
                "kind": "reservation",
                "source": str(path),
            }
        )
    return sources


def _scan_bug_registry_ids(root: Path | None = None) -> list[dict[str, Any]]:
    sources: list[dict[str, Any]] = []
    for bugs_root in _bug_id_scan_roots(root):
        allocator = bugs_root / ".bug_id_allocator.json"
        if allocator.exists():
            try:
                payload = _load_json(allocator)
                number = int(payload.get("last_allocated") or 0)
            except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
                raise WorkflowError(f"invalid bug id allocator: {allocator}") from exc
            if number > 0:
                sources.append(
                    {
                        "bug_id": f"BUG-{number:03d}",
                        "number": number,
                        "kind": "allocator",
                        "source": str(allocator),
                    }
                )
        for path in sorted(p for p in bugs_root.glob("*.json") if not p.name.startswith(".")):
            bug_id: str | None = None
            try:
                payload = _load_json(path)
                bug_id = str(payload.get("bug_id") or "")
            except (OSError, json.JSONDecodeError, WorkflowError):
                bug_id = None
            number = _bug_id_number(bug_id) or _bug_id_number(path.name)
            if number:
                sources.append(
                    {
                        "bug_id": f"BUG-{number:03d}",
                        "number": number,
                        "kind": "bug_json",
                        "source": str(path),
                    }
                )
    return sources


def _scan_github_bug_ids(*, limit: int = 1000, timeout: int = 30) -> tuple[list[dict[str, Any]], list[str]]:
    result = _run_command(
        [
            "gh",
            "issue",
            "list",
            "--repo",
            GITHUB_REPO,
            "--state",
            "all",
            "--limit",
            str(limit),
            "--json",
            "number,title,url,state,labels",
        ],
        timeout=timeout,
    )
    if not result.get("ok"):
        message = result.get("stderr") or result.get("stdout") or "gh issue list failed"
        return [], [f"github BUG id scan unavailable: {message}"]
    try:
        issues = json.loads(str(result.get("stdout") or "[]"))
    except json.JSONDecodeError as exc:
        return [], [f"github BUG id scan returned invalid JSON: {exc}"]
    sources: list[dict[str, Any]] = []
    for issue in issues if isinstance(issues, list) else []:
        if not isinstance(issue, dict):
            continue
        title = str(issue.get("title") or "")
        number = _bug_id_number(title)
        if not number:
            continue
        sources.append(
            {
                "bug_id": f"BUG-{number:03d}",
                "number": number,
                "kind": "github_issue",
                "source": issue.get("url") or f"github_issue:{issue.get('number')}",
                "github_issue_number": issue.get("number"),
                "github_state": issue.get("state"),
                "title": title,
                "labels": issue.get("labels") or [],
            }
        )
    if len(issues) >= limit:
        sources.append(
            {
                "bug_id": "BUG-000",
                "number": 0,
                "kind": "github_scan_limit_warning",
                "source": f"gh issue list reached limit {limit}; increase limit if BUG ids exceed this window",
            }
        )
    return sources, []


def _github_bug_issue_for_id(bug_id: str, *, limit: int = 1000, timeout: int = 30) -> tuple[dict[str, Any] | None, list[str]]:
    normalized = bug_id.strip().upper()
    sources, warnings = _scan_github_bug_ids(limit=limit, timeout=timeout)
    matches = [
        item
        for item in sources
        if str(item.get("bug_id") or "").upper() == normalized and item.get("kind") == "github_issue"
    ]
    if not matches:
        return None, warnings
    matches.sort(key=lambda item: int(item.get("github_issue_number") or 0), reverse=True)
    return matches[0], warnings


def _bug_id_allocation_report(
    root: Path | None = None,
    *,
    include_github: bool = False,
    github_required: bool = False,
) -> dict[str, Any]:
    sources = _scan_bug_registry_ids(root)
    sources.extend(_scan_bug_id_reservations())
    warnings: list[str] = []
    if include_github:
        github_sources, github_warnings = _scan_github_bug_ids()
        sources.extend(github_sources)
        warnings.extend(github_warnings)
        if github_required and github_warnings:
            raise WorkflowError("; ".join(github_warnings))
    max_number = max((int(source.get("number") or 0) for source in sources), default=0)
    max_by_kind: dict[str, int] = {}
    for source in sources:
        kind = str(source.get("kind") or "unknown")
        number = int(source.get("number") or 0)
        if number > max_by_kind.get(kind, 0):
            max_by_kind[kind] = number
    allocator_max = max_by_kind.get("allocator", 0)
    observed_max = max(
        max_by_kind.get("bug_json", 0),
        max_by_kind.get("reservation", 0),
        max_by_kind.get("github_issue", 0),
    )
    if allocator_max and observed_max > allocator_max:
        warnings.append(
            "BUG id allocator is behind observed BUG ids: "
            f"allocator=BUG-{allocator_max:03d}, observed=BUG-{observed_max:03d}; "
            f"next allocation will use BUG-{max_number + 1:03d}"
        )
    return {
        "schema_version": "aistock_bug_id_allocation_report_v1",
        "max_number": max_number,
        "next_number": max_number + 1,
        "sources": sources,
        "warnings": warnings,
        "github_scanned": include_github,
        "max_by_kind": max_by_kind,
        "allocator_max_number": allocator_max,
        "observed_max_number": observed_max,
        "github_max_number": max_by_kind.get("github_issue", 0),
    }


def _bug_id_allocation_summary(report: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "aistock_bug_id_allocation_summary_v1",
        "max_number": report.get("max_number"),
        "next_number": report.get("next_number"),
        "allocator_max_number": report.get("allocator_max_number"),
        "observed_max_number": report.get("observed_max_number"),
        "github_max_number": report.get("github_max_number"),
        "github_scanned": report.get("github_scanned"),
        "warnings": report.get("warnings", []),
    }


def _duplicate_bug_id_sources(
    report: dict[str, Any],
    bug_id: str,
    *,
    allowed_github_issue_number: int | str | None = None,
) -> list[dict[str, Any]]:
    number = _bug_id_number(bug_id)
    allowed_issue = str(allowed_github_issue_number) if allowed_github_issue_number is not None else None
    duplicates: list[dict[str, Any]] = []
    for source in report.get("sources", []):
        if int(source.get("number") or 0) != number:
            continue
        kind = source.get("kind")
        if kind == "allocator":
            continue
        if kind == "github_issue" and allowed_issue and str(source.get("github_issue_number")) == allowed_issue:
            continue
        duplicates.append(source)
    return duplicates


def _reserve_bug_id(
    root: Path | None,
    *,
    bug_id: str | None,
    include_github: bool,
    github_required: bool,
    allowed_github_issue_number: int | str | None,
) -> tuple[str, int, dict[str, Any], Path]:
    with _GlobalBugIdAllocatorLock():
        report = _bug_id_allocation_report(root, include_github=include_github, github_required=github_required)
        if bug_id:
            canonical_bug_id = bug_id.strip().upper()
            number = _bug_id_number(canonical_bug_id)
            if not number or not re.fullmatch(r"BUG-\d{3,}", canonical_bug_id):
                raise WorkflowError("--bug-id must match BUG-NNN when provided")
            duplicates = _duplicate_bug_id_sources(
                report,
                canonical_bug_id,
                allowed_github_issue_number=allowed_github_issue_number,
            )
            if duplicates:
                detail = "; ".join(f"{item.get('kind')}:{item.get('source')}" for item in duplicates[:5])
                raise WorkflowError(f"{canonical_bug_id} already exists in global BUG id scan: {detail}")
        else:
            number = int(report["next_number"])
            canonical_bug_id = f"BUG-{number:03d}"
        reservation_root = _bug_id_reservation_root()
        reservation_path = reservation_root / f"{canonical_bug_id}.json"
        if reservation_path.exists():
            raise WorkflowError(f"{canonical_bug_id} is already reserved: {reservation_path}")
        _write_json(
            reservation_path,
            {
                "schema_version": "aistock_bug_id_reservation_v1",
                "bug_id": canonical_bug_id,
                "reserved_at": _utc_now(),
                "reserved_by": "aistock_issue_workflow.py",
                "root": str((root or REPO_ROOT).resolve()),
            },
        )
    return canonical_bug_id, number, report, reservation_path


def _release_bug_id_reservation(path: Path | None) -> None:
    if not path:
        return
    try:
        path.unlink()
    except FileNotFoundError:
        pass


class _GlobalBugIdAllocatorLock:
    def __init__(self, *, timeout: float = 30.0) -> None:
        self.timeout = timeout
        self.path = Path(os.environ.get("AISTOCK_BUG_ID_LOCK_PATH") or (_default_worktree_root() / ".locks" / "bug-id-allocator.lock"))
        self._fd: int | None = None

    def __enter__(self) -> "_GlobalBugIdAllocatorLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        deadline = time.monotonic() + self.timeout
        while True:
            try:
                self._fd = os.open(str(self.path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(self._fd, f"{os.getpid()}\n{_utc_now()}\n".encode("ascii"))
                return self
            except FileExistsError as exc:
                if time.monotonic() >= deadline:
                    raise WorkflowError(f"timed out waiting for global BUG id allocator lock: {self.path}") from exc
                time.sleep(0.1)

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        if self._fd is not None:
            os.close(self._fd)
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass


def _next_bug_id(
    root: Path | None = None,
    *,
    include_github: bool = False,
    github_required: bool = False,
) -> tuple[str, int]:
    report = _bug_id_allocation_report(root, include_github=include_github, github_required=github_required)
    next_number = int(report["next_number"])
    return f"BUG-{next_number:03d}", next_number


def _write_allocator(next_number: int, root: Path | None = None) -> None:
    allocator = _allocator_path(root)
    current = 0
    if allocator.exists():
        try:
            current = int(_load_json(allocator).get("last_allocated") or 0)
        except (TypeError, ValueError) as exc:
            raise WorkflowError(f"invalid bug id allocator: {allocator}") from exc
    if current > next_number:
        next_number = current
    allocator = _allocator_path(root)
    _write_json(
        _allocator_path(root),
        {
            "schema_version": "aistock_bug_id_allocator_v1",
            "last_allocated": next_number,
            "updated_at": _utc_now(),
            "updated_by": "aistock_issue_workflow.py",
        },
    )


def _bug_json_path(record: dict[str, Any], root: Path | None = None) -> Path:
    bug_id = str(record["bug_id"])
    return _bugs_root(root) / f"{_today_compact()}_{bug_id}-{_slug(str(record.get('title') or bug_id))}.json"


def _add_record_allowed_scope(record: dict[str, Any], *paths: str | Path) -> None:
    scope = [str(item).replace("\\", "/").strip("/") for item in flow._as_list(record.get("allowed_write_scope"))]
    for raw_path in paths:
        path = str(raw_path).replace("\\", "/").strip("/")
        if path and path not in scope:
            scope.append(path)
    record["allowed_write_scope"] = scope


def _registry_target_root() -> Path:
    override = os.environ.get("AISTOCK_ISSUE_REGISTRY_ROOT")
    return Path(override) if override else REPO_ROOT


def _registry_worktree_names(*, title: str, module: str, severity: str) -> tuple[str, Path]:
    title_slug = _slug(title, max_len=34)
    module_slug = _slug(module, max_len=22)
    suffix = _short_hash(title, module, severity, _utc_now(), length=6)
    name = f"registry-{module_slug}-{title_slug}-{_today_compact()}-{suffix}"
    branch = f"bug/{name}"
    return branch, _default_worktree_root() / name


def _close_sync_worktree_names(*, bug_id: str) -> tuple[str, Path]:
    name = f"{bug_id.strip().upper()}-close-sync-{_today_compact()}"
    branch = f"chore/{name}"
    return branch, _default_worktree_root() / name


def _close_sync_batch_worktree_names(*, bug_ids: list[str]) -> tuple[str, Path]:
    normalized = [item.strip().upper() for item in bug_ids if item.strip()]
    label = "-".join(normalized[:3])
    if len(normalized) > 3:
        label = f"{label}-plus-{len(normalized) - 3}"
    name = f"{label}-close-sync-batch-{_today_compact()}".strip("-")
    branch = f"chore/{name}"
    return branch, _default_worktree_root() / name


def _maybe_create_registry_worktree(
    *,
    title: str,
    module: str,
    severity: str,
    create: bool,
    dry_run: bool,
) -> dict[str, Any]:
    branch, worktree = _registry_worktree_names(title=title, module=module, severity=severity)
    plan = {
        "create_worktree": create,
        "dry_run": dry_run,
        "branch": branch,
        "worktree": str(worktree),
        "base": "origin/main",
    }
    if not create or dry_run:
        return plan
    if worktree.exists():
        raise WorkflowError(f"target registry worktree already exists: {worktree}")
    _git(["fetch", "origin", "main"])
    _git_worktree_add_new_branch(worktree=worktree, branch=branch)
    plan["created"] = True
    return plan


def _maybe_create_fix_chain_worktree(
    *,
    record: dict[str, Any],
    bug_id: str,
    create: bool,
    dry_run: bool,
    task_slug: str | None = None,
) -> dict[str, Any]:
    branch, worktree = _target_names(record, bug_id, task_slug)
    plan = {
        "create_worktree": create,
        "dry_run": dry_run,
        "branch": branch,
        "worktree": str(worktree),
        "base": "origin/main",
        "registration_strategy": "fix_pr_persists_bug_registration",
    }
    if not create or dry_run:
        return plan
    if worktree.exists():
        raise WorkflowError(f"target fix-chain worktree already exists: {worktree}")
    _git(["fetch", "origin", "main"])
    _git_worktree_add_new_branch(worktree=worktree, branch=branch)
    plan["created"] = True
    return plan


def _git_worktree_add_new_branch(*, worktree: Path, branch: str, base: str = "origin/main") -> None:
    # Keep options before the path; some Git versions otherwise infer `main` from origin/main in linked worktrees.
    _git(["worktree", "add", "-b", branch, str(worktree), base])


def _maybe_create_close_sync_worktree(*, bug_id: str, create: bool, dry_run: bool) -> dict[str, Any]:
    branch, worktree = _close_sync_worktree_names(bug_id=bug_id)
    plan = {
        "create_worktree": create,
        "dry_run": dry_run,
        "branch": branch,
        "worktree": str(worktree),
        "base": "origin/main",
    }
    if not create or dry_run:
        return plan
    _git(["fetch", "origin", "main"])
    if worktree.exists():
        git = _git_snapshot(worktree)
        if not git.get("ok"):
            raise WorkflowError(f"target close-sync worktree is not a git checkout: {worktree}")
        if git.get("dirty"):
            raise WorkflowError(f"target close-sync worktree is dirty: {worktree}")
        plan["reused"] = True
        plan["git"] = git
        return plan
    if _git_ref_exists(branch):
        _git(["worktree", "add", str(worktree), branch])
        plan["reused_branch"] = True
    else:
        _git_worktree_add_new_branch(worktree=worktree, branch=branch)
    plan["created"] = True
    return plan


def _maybe_create_close_sync_batch_worktree(
    *,
    bug_ids: list[str],
    create: bool,
    dry_run: bool,
) -> dict[str, Any]:
    branch, worktree = _close_sync_batch_worktree_names(bug_ids=bug_ids)
    plan = {
        "create_worktree": create,
        "dry_run": dry_run,
        "branch": branch,
        "worktree": str(worktree),
        "base": "origin/main",
        "bug_ids": bug_ids,
    }
    if not create or dry_run:
        return plan
    _git(["fetch", "origin", "main"])
    if worktree.exists():
        git = _git_snapshot(worktree)
        if not git.get("ok"):
            raise WorkflowError(f"target close-sync batch worktree is not a git checkout: {worktree}")
        if git.get("dirty"):
            raise WorkflowError(f"target close-sync batch worktree is dirty: {worktree}")
        plan["reused"] = True
        plan["git"] = git
        return plan
    if _git_ref_exists(branch):
        _git(["worktree", "add", str(worktree), branch])
        plan["reused_branch"] = True
    else:
        _git_worktree_add_new_branch(worktree=worktree, branch=branch)
    plan["created"] = True
    return plan


def _validate_registry_apply_target(target_root: Path) -> dict[str, Any]:
    target = target_root.resolve()
    canonical = _canonical_root().resolve()
    git = _git_snapshot(target_root)
    blocking: list[str] = []
    warnings: list[str] = []

    if not git.get("ok"):
        blocking.append(str(git.get("error") or f"not a git checkout: {target_root}"))
    if target == canonical:
        blocking.append(
            "refusing to write BUG registry files in canonical root; use a task/registry worktree and branch"
        )
    if git.get("branch") == "main":
        blocking.append("refusing to write BUG registry files on main; use a task/registry branch")
    if git.get("dirty"):
        blocking.append(f"registry target is dirty ({git.get('dirty_count')} file(s)); start from a clean task worktree")
    if (
        target_root.name.lower() == "aistock"
        and not _is_inside(target_root, _default_worktree_root())
        and target != canonical
    ):
        warnings.append("registry target does not look like an isolated AIstock worktree")

    return {
        "target_root": str(target),
        "canonical_root": str(canonical),
        "git": git,
        "blocking": blocking,
        "warnings": warnings,
    }


def _validate_close_sync_apply_target(target_root: Path) -> dict[str, Any]:
    guard = _validate_registry_apply_target(target_root)
    blocking = list(guard.get("blocking") or [])
    if blocking:
        guard["blocking"] = [
            item.replace("write BUG registry files", "close-sync BUG registry files")
            for item in blocking
        ]
        guard["next_command_hint"] = (
            "Run close-sync from a clean registry/task worktree, or rerun with "
            "--create-registry-worktree so the wrapper creates an isolated branch."
        )
    return guard


def _dirty_files(root: Path) -> list[str]:
    status = _run_command(["git", "status", "--porcelain=v1"], cwd=root)
    if not status.get("ok"):
        return []
    files: list[str] = []
    for line in str(status.get("stdout") or "").splitlines():
        if not line.strip():
            continue
        path = _parse_git_porcelain_path(line)
        if path:
            files.append(path)
    return files


def _git_ref_has_path(root: Path, ref: str, rel: str) -> bool:
    normalized = rel.replace("\\", "/").strip("/")
    if not normalized:
        return False
    return _run_command(["git", "cat-file", "-e", f"{ref}:{normalized}"], cwd=root).get("ok", False)


def _origin_equivalent_dirty_files(root: Path, files: list[str]) -> list[str]:
    equivalent: list[str] = []
    for rel in files:
        if not rel:
            continue
        if not _git_ref_has_path(root, "origin/main", rel):
            continue
        if _run_command(["git", "diff", "--quiet", "origin/main", "--", rel], cwd=root).get("ok"):
            equivalent.append(rel)
    return equivalent


def _root_sync_safe_with_dirty(root_git: dict[str, Any]) -> bool:
    head = str(root_git.get("head") or "")
    origin_main = str(root_git.get("origin_main") or "")
    return bool(head and origin_main and head == origin_main)


def _github_issue_number_from_url(url: str) -> int | None:
    match = re.search(r"/issues/(\d+)(?:$|[?#])", url.strip())
    return int(match.group(1)) if match else None


def _is_inside(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def _cwd_is_inside(path: str | Path | None) -> bool:
    if not path:
        return False
    try:
        target = Path(path).resolve()
        cwd = Path.cwd().resolve()
    except OSError:
        return False
    return cwd == target or _is_inside(cwd, target)


def _relocate_cwd_before_cleanup(*worktrees: str | Path | None, root: Path | None = None) -> dict[str, Any] | None:
    current_root = root or _canonical_root()
    for worktree in worktrees:
        if _cwd_is_inside(worktree):
            relocation = {
                "from": str(worktree),
                "to": str(current_root),
                "reason": "cleanup_target_contains_current_cwd",
                "relocated": False,
            }
            if not current_root.exists():
                relocation["error"] = f"canonical root missing: {current_root}"
                return relocation
            try:
                os.chdir(current_root)
                relocation["relocated"] = True
            except OSError as exc:
                relocation["error"] = str(exc)
            return relocation
    return None


def _deferred_cleanup_from_safe_cwd_plan(
    *,
    branch: str,
    bug_id: str,
    worktree: str | None,
    pr_url: str | None,
    sync_root: bool,
    root: Path | None = None,
) -> dict[str, Any]:
    canonical_root = root or _canonical_root()
    command = (
        f'cd /d "{canonical_root}" && python scripts/aistock_issue_workflow.py cleanup-after-merge '
        f'--branch "{branch}" --bug-id {bug_id} '
    )
    if worktree:
        command += f'--worktree "{worktree}" '
    if pr_url:
        command += f'--pr-url "{pr_url}" '
    if sync_root:
        command += "--sync-root "
    command += "--apply"
    return {
        "schema_version": "aistock_issue_workflow_deferred_cleanup_v1",
        "workflow_gate": "ready_for_cleanup",
        "branch": branch,
        "bug_id": bug_id,
        "worktree": worktree,
        "pr_url": pr_url,
        "sync_root": sync_root,
        "reason": "source_worktree_contains_invoking_cwd",
        "next_command": command,
        "warnings": [
            "Source worktree cleanup is deferred because the invoking shell may still hold the target cwd on Windows."
        ],
    }


def _canonical_root() -> Path:
    override = os.environ.get("AISTOCK_CANONICAL_ROOT") or os.environ.get("AISTOCK_ROOT")
    if override:
        return Path(override)
    default = Path("F:/Dev/AIstock")
    return default if default.exists() else REPO_ROOT


def _git_snapshot(root: Path) -> dict[str, Any]:
    if not (root / ".git").exists() and not (root / ".git").is_file():
        return {"ok": False, "error": f"not a git checkout: {root}"}
    status = _run_command(["git", "status", "--porcelain=v1", "-b"], cwd=root)
    branch = _run_command(["git", "branch", "--show-current"], cwd=root)
    head = _run_command(["git", "rev-parse", "--short", "HEAD"], cwd=root)
    upstream = _run_command(["git", "rev-parse", "--short", "@{u}"], cwd=root)
    origin_main = _run_command(["git", "rev-parse", "--short", "origin/main"], cwd=root)
    porcelain = status.get("stdout", "")
    dirty_lines = [line for line in porcelain.splitlines()[1:] if line.strip()]
    return {
        "ok": bool(status.get("ok")),
        "branch": branch.get("stdout"),
        "head": head.get("stdout"),
        "upstream": upstream.get("stdout"),
        "origin_main": origin_main.get("stdout"),
        "status": porcelain,
        "dirty": bool(dirty_lines),
        "dirty_count": len(dirty_lines),
    }


def _parse_worktree_list() -> list[dict[str, str]]:
    result = _run_command(["git", "worktree", "list", "--porcelain"], cwd=REPO_ROOT, timeout=30)
    if not result.get("ok"):
        return []
    items: list[dict[str, str]] = []
    current: dict[str, str] = {}
    for line in str(result.get("stdout") or "").splitlines():
        if not line.strip():
            if current:
                items.append(current)
                current = {}
            continue
        key, _, value = line.partition(" ")
        current[key] = value
    if current:
        items.append(current)
    return items


def _branch_for_path(path: Path) -> str | None:
    target = str(path.resolve()).replace("\\", "/").lower()
    for item in _parse_worktree_list():
        worktree = item.get("worktree")
        if worktree and str(Path(worktree).resolve()).replace("\\", "/").lower() == target:
            branch_ref = item.get("branch") or ""
            return branch_ref.removeprefix("refs/heads/") or None
    return None


def _state_issue_json_path(root: Path, state: dict[str, Any]) -> Path | None:
    raw = str(state.get("source_bug_json") or state.get("target_bug_json") or "").strip()
    if not raw:
        return None
    path = Path(raw)
    if not path.is_absolute():
        path = root / path
    return path


def _registry_commit_next_command(worktree: str | Path, bug_id: str, *, continue_fix: bool = False, issue_json: str | None = None) -> str:
    parts = [
        f"cd /d {worktree}",
        "git status --short",
        f"git add {' '.join(COMMITTABLE_BUG_REGISTRY_PATHS)}",
        f'git commit -m "chore(issue): register {bug_id}"',
    ]
    if continue_fix:
        issue_arg = f' --issue-json "{issue_json}"' if issue_json else " --issue-json <BUG_JSON>"
        parts.append(f"python scripts/aistock_issue_workflow.py run --bug-id {bug_id}{issue_arg} --mode plan --create-worktree")
    return " && ".join(parts)


def _commit_bug_registration_in_fix_worktree(root: Path, bug_id: str) -> dict[str, Any]:
    dirty = [
        path for path in _dirty_files(root)
        if path.replace("\\", "/").startswith("tests/aistock_validation/bugs/")
    ]
    if not dirty:
        return {"workflow_gate": "no_changes", "root": str(root), "branch": _branch_for_path(root)}
    unexpected = sorted(
        path for path in _dirty_files(root)
        if not path.replace("\\", "/").startswith("tests/aistock_validation/bugs/")
    )
    if unexpected:
        raise WorkflowError(
            "fix-chain registration worktree has unexpected dirty files outside BUG registry: "
            + ", ".join(unexpected[:10])
        )
    started = time.monotonic()
    actions: list[dict[str, Any]] = []
    add = _run_command(["git", "add", *COMMITTABLE_BUG_REGISTRY_PATHS], cwd=root, timeout=60)
    actions.append({"command": f"git add {' '.join(COMMITTABLE_BUG_REGISTRY_PATHS)}", "result": add})
    if not add.get("ok"):
        raise WorkflowError(add.get("stderr") or add.get("stdout") or "fix-chain registration git add failed")
    commit = _run_command(["git", "commit", "-m", f"chore(issue): register {bug_id}"], cwd=root, timeout=120)
    actions.append({"command": f"git commit -m chore(issue): register {bug_id}", "result": commit})
    if not commit.get("ok") and "nothing to commit" not in f"{commit.get('stdout')}\n{commit.get('stderr')}".lower():
        raise WorkflowError(commit.get("stderr") or commit.get("stdout") or "fix-chain registration git commit failed")
    commit_sha = _run_command(["git", "rev-parse", "--short=12", "HEAD"], cwd=root, timeout=30)
    return {
        "workflow_gate": "committed",
        "root": str(root),
        "branch": _branch_for_path(root),
        "changed_files": dirty,
        "actions": actions,
        "commit": str(commit_sha.get("stdout") or "").strip() if commit_sha.get("ok") else None,
        "duration_seconds": round(time.monotonic() - started, 3),
    }


def _issue_json_path_for_worktree(source_path: Path, target_root: Path) -> Path:
    if _is_inside(source_path, target_root):
        return source_path
    normalized_parts = [part.replace("\\", "/") for part in source_path.parts]
    for index in range(len(normalized_parts) - 2):
        if normalized_parts[index : index + 3] == ["tests", "aistock_validation", "bugs"]:
            return target_root / Path(*source_path.parts[index:])
    try:
        relative = source_path.resolve().relative_to(REPO_ROOT.resolve())
    except ValueError:
        try:
            relative = source_path.resolve().relative_to(_canonical_root().resolve())
        except ValueError:
            relative = Path("tests") / "aistock_validation" / "bugs" / source_path.name
    return target_root / relative


def _ensure_issue_json_in_worktree(source_path: Path, target_root: Path, *, record: dict[str, Any] | None = None) -> Path:
    target = _issue_json_path_for_worktree(source_path, target_root)
    if target.exists():
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    if source_path.exists():
        target.write_text(source_path.read_text(encoding="utf-8"), encoding="utf-8")
    elif record is not None:
        _write_json(target, record)
    else:
        raise WorkflowError(f"BUG JSON source does not exist: {source_path}")
    return target


def _workflow_role_for_state(root: Path, state: dict[str, Any], git: dict[str, Any] | None = None) -> str:
    explicit = str(state.get("workflow_role") or "").strip().lower()
    if explicit:
        return explicit
    if state.get("registry_pr_only"):
        return "registry_intake"
    branch = str(state.get("branch") or (git or {}).get("branch") or _branch_for_path(root) or "")
    worktree_name = Path(str(state.get("worktree") or root)).name
    if branch.startswith("bug/registry-") or worktree_name.startswith("registry-"):
        return "registry_intake"
    return "fix"


def _workflow_state_sort_key(root: Path, state: dict[str, Any]) -> tuple[int, str, str]:
    state_rank = {
        "discovered": 10,
        "context_ready": 20,
        "fix_in_progress": 30,
        "fix_applied": 40,
        "validation_planned": 50,
        "validation_running": 60,
        "validation_passed": 70,
        "pushed": 80,
        "pr_opened": 90,
        "ci_running": 100,
        "ci_green": 110,
        "merged": 120,
        "close_synced": 130,
        "cleanup_done": 140,
        "complete": 150,
    }
    role = _workflow_role_for_state(root, state)
    role_bonus = -100 if role.startswith("registry") else 100
    pr_bonus = 25 if state.get("pr_url") else 0
    worktree_bonus = 10 if state.get("worktree") else 0
    score = role_bonus + pr_bonus + worktree_bonus + state_rank.get(str(state.get("state") or ""), 0)
    return (score, str(state.get("updated_at") or ""), str(root))


def _state_is_active(state: dict[str, Any]) -> bool:
    value = str(state.get("state") or "")
    if not value:
        return False
    if state.get("planned_worktree") and not state.get("worktree"):
        return False
    if value in TERMINAL_WORKFLOW_STATES:
        return False
    return value in ACTIVE_WORKFLOW_STATES or value not in {"fixed", "verified", "wontfix"}


def _active_workflows_for_bug(bug_id: str) -> list[dict[str, Any]]:
    canonical_bug_id = bug_id.strip().upper()
    active: list[dict[str, Any]] = []
    for root in _state_roots_for_bug(canonical_bug_id):
        state = _load_state(canonical_bug_id, root)
        if not state or not _state_is_active(state):
            continue
        worktree = Path(str(state.get("worktree") or root))
        if not worktree.is_absolute():
            worktree = root / worktree
        if state.get("worktree") and not worktree.exists():
            continue
        git = _git_snapshot(worktree) if worktree.exists() else {"ok": False, "error": f"workflow worktree missing: {worktree}"}
        role = _workflow_role_for_state(root, state, git)
        issue_json = _state_issue_json_path(root, state)
        active.append(
            {
                "bug_id": canonical_bug_id,
                "root": str(root),
                "worktree": str(worktree),
                "branch": state.get("branch") or git.get("branch") or _branch_for_path(worktree),
                "workflow_role": role,
                "issue_json": str(issue_json) if issue_json and issue_json.exists() else None,
                "state": state.get("state"),
                "state_path": _repo_rel(_state_path(canonical_bug_id, root), root),
                "dirty": bool(git.get("dirty")),
                "dirty_count": git.get("dirty_count"),
                "git": git,
                "next_command": _next_command_for_state(canonical_bug_id, state),
            }
        )
    return active


def _active_worktree_decision(
    *,
    bug_id: str,
    create_worktree: bool,
    force_new_worktree: bool,
    force_reason: str | None,
) -> dict[str, Any]:
    active = _active_workflows_for_bug(bug_id)
    decision: dict[str, Any] = {
        "bug_id": bug_id,
        "create_worktree_requested": create_worktree,
        "force_new_worktree": force_new_worktree,
        "force_reason": force_reason,
        "active_workflows": active,
        "decision": "create_or_continue",
        "workflow_gate": "ready",
        "blocking": [],
        "warnings": [],
        "next_command": None,
        "rescue_checklist": [],
    }
    if not create_worktree or not active:
        return decision
    registry_active = [item for item in active if str(item.get("workflow_role") or "").startswith("registry")]
    fix_active = [item for item in active if item not in registry_active]
    if registry_active and not fix_active:
        dirty_registry = [item for item in registry_active if item.get("dirty")]
        first_registry = sorted(
            registry_active,
            key=lambda item: (
                1 if item.get("issue_json") else 0,
                str(item.get("state") or ""),
                str(item.get("root") or ""),
            ),
            reverse=True,
        )[0]
        if dirty_registry:
            dirty_first = dirty_registry[0]
            decision["decision"] = "blocked_dirty_registry_intake"
            decision["workflow_gate"] = "blocked"
            decision["blocking"].append(
                f"registry intake worktree is dirty and must be committed before creating a separate fix worktree: {dirty_first.get('worktree')}"
            )
            decision["next_command"] = _registry_commit_next_command(dirty_first.get("worktree") or REPO_ROOT, bug_id)
            decision["rescue_checklist"] = [
                "commit_or_pr_the_registry_intake_branch",
                "do_not_edit_code_in_the_registry_intake_worktree",
                "rerun_with_create_worktree_to_build_the_fix_worktree",
            ]
            return decision
        registry_issue_json = first_registry.get("issue_json")
        if not registry_issue_json:
            decision["decision"] = "blocked_registry_issue_json_missing"
            decision["workflow_gate"] = "blocked"
            decision["blocking"].append("registry intake workflow does not record a readable source_bug_json")
            decision["next_command"] = f"python scripts/aistock_issue_workflow.py run --bug-id {bug_id} --issue-json <BUG_JSON> --mode plan --create-worktree"
            return decision
        decision["decision"] = "create_fix_from_registry_intake"
        decision["workflow_gate"] = "ready"
        decision["registry_intake_workflows"] = registry_active
        decision["registry_issue_json"] = registry_issue_json
        decision["warnings"].append(
            "only registry intake workflow exists; creating a separate fix worktree from latest origin/main with the registry BUG JSON"
        )
        decision["next_command"] = (
            f"python scripts/aistock_issue_workflow.py run --bug-id {bug_id} --issue-json \"{registry_issue_json}\" "
            "--mode plan --create-worktree"
        )
        return decision
    active_for_decision = fix_active or active
    dirty = [item for item in active_for_decision if item.get("dirty")]
    if force_new_worktree:
        if not str(force_reason or "").strip():
            decision["decision"] = "blocked_dirty_active" if dirty else "blocked_requires_force_reason"
            decision["workflow_gate"] = "blocked"
            decision["blocking"].append("--force-new-worktree requires --reason so the exception is auditable")
            return decision
        decision["decision"] = "force_new_worktree"
        decision["workflow_gate"] = "warning"
        decision["warnings"].append("active workflow exists, but --force-new-worktree was supplied with a reason")
        return decision
    first = active_for_decision[0]
    if dirty:
        dirty_first = dirty[0]
        decision["decision"] = "blocked_dirty_active"
        decision["workflow_gate"] = "blocked"
        decision["blocking"].append(f"active workflow worktree is dirty: {dirty_first.get('worktree')}")
        decision["next_command"] = f"python scripts/aistock_issue_workflow.py resume --bug-id {bug_id} --worktree \"{dirty_first.get('worktree')}\""
        decision["rescue_checklist"] = [
            "switch_to_active_worktree",
            "inspect_git_status_without_reset_or_clean",
            "commit_or_stash_task_files_if_they_belong_to_this_issue",
            "rerun_resume_and_continue_existing_workflow",
            "use_force_new_worktree_only_with_an_audited_reason",
        ]
        return decision
    decision["decision"] = "resume_existing"
    decision["workflow_gate"] = "resume"
    decision["next_command"] = f"python scripts/aistock_issue_workflow.py resume --bug-id {bug_id} --worktree \"{first.get('worktree')}\""
    return decision


def _github_issue_url(number: int | str) -> str:
    return f"https://github.com/{GITHUB_REPO}/issues/{number}"


def _extract_run_id_from_issue_body(body: str) -> str | None:
    marker = re.search(r"aistock-issue-on-test-fail:(\d+)", body or "")
    if marker:
        return marker.group(1)
    run_url = re.search(r"github\.com/[^/]+/[^/]+/actions/runs/(\d+)", body or "")
    return run_url.group(1) if run_url else None


def _issue_body_failure_text(body: str) -> str:
    """Keep CI classification focused on failure evidence, not generic checklists."""
    text = str(body or "")
    return re.split(r"\n##\s+(Agent Handoff|Suggested Triage|BUG JSON Linkage|Production Gates)\b", text, maxsplit=1)[0]


def _extract_regression_locator_from_issue_body(body: str, summary: dict[str, Any]) -> dict[str, Any] | None:
    text = str(body or "")
    status_match = re.search(r"last_green_status:\s*`?([A-Za-z0-9_-]+)`?", text)
    range_match = re.search(r"commit_range:\s*`?([0-9a-fA-F.]+)`?", text)
    previous_match = re.search(r"previous_success_run:\s*(https://github\.com/[^\s`]+/actions/runs/(\d+))", text)
    if not (status_match or range_match or previous_match):
        return None
    previous_run = None
    if previous_match:
        previous_run = {"run_url": previous_match.group(1), "run_id": previous_match.group(2)}
    return {
        "schema_version": "aistock_ci_last_green_locator_v1",
        "status": status_match.group(1) if status_match else "found",
        "commit_range": range_match.group(1) if range_match else None,
        "previous_success_run": previous_run,
        "current_run": {
            "workflow": summary.get("workflow"),
            "run_id": str(summary.get("run_id") or ""),
            "run_url": summary.get("run_url"),
            "branch": summary.get("branch"),
            "commit": summary.get("commit"),
        },
        "blocking_for_issue_workflow": False,
        "source": "github_issue_body",
        "warnings": [],
    }


def _merge_issue_body_regression_locator(summary: dict[str, Any], body: str) -> dict[str, Any]:
    locator = _extract_regression_locator_from_issue_body(body, summary)
    if not locator:
        return summary
    current = summary.get("last_green_locator") if isinstance(summary.get("last_green_locator"), dict) else {}
    if current and current.get("status") not in {None, "", "not_found", "unknown", "not_requested"}:
        return summary
    enriched = dict(summary)
    enriched["last_green_locator"] = locator
    notes = list(enriched.get("triage_notes") or [])
    notes.append("last_green_locator recovered from GitHub Issue body")
    enriched["triage_notes"] = notes
    return enriched


def _find_superseding_main_success(summary: dict[str, Any]) -> dict[str, Any] | None:
    workflow = str(summary.get("workflow") or "").strip()
    branch = str(summary.get("branch") or "").strip()
    run_id = str(summary.get("run_id") or "").strip()
    if not workflow or not branch or not run_id.isdigit():
        return None
    result = _run_command(
        [
            "gh",
            "run",
            "list",
            "--repo",
            GITHUB_REPO,
            "--branch",
            branch,
            "--workflow",
            workflow,
            "--limit",
            "20",
            "--json",
            "databaseId,headSha,conclusion,status,url,createdAt,displayTitle",
        ],
        cwd=REPO_ROOT,
        timeout=30,
    )
    if not result.get("ok"):
        return None
    try:
        runs = json.loads(str(result.get("stdout") or "[]"))
    except json.JSONDecodeError:
        return None
    if not isinstance(runs, list):
        return None
    current_id = int(run_id)
    for item in runs:
        if not isinstance(item, dict):
            continue
        candidate_id = str(item.get("databaseId") or "")
        if not candidate_id.isdigit() or int(candidate_id) <= current_id:
            continue
        if str(item.get("status") or "").lower() != "completed":
            continue
        if str(item.get("conclusion") or "").lower() != "success":
            continue
        return {
            "run_id": candidate_id,
            "run_url": item.get("url"),
            "head_sha": item.get("headSha"),
            "created_at": item.get("createdAt"),
            "display_title": item.get("displayTitle"),
            "branch": branch,
            "supersede_scope": "main" if branch == "main" else "same_branch",
        }
    return None


def _superseded_success_phrase(superseding_run: dict[str, Any], workflow: str | None) -> str:
    workflow_name = workflow or "CI"
    branch = str(superseding_run.get("branch") or "").strip()
    if superseding_run.get("supersede_scope") == "same_branch" and branch:
        return f"later successful {workflow_name} run on the same branch {branch}"
    return f"later successful main {workflow_name} run"


def _load_github_issue(issue_number: int | str) -> dict[str, Any]:
    result = _execute_checked(
        [
            "gh",
            "issue",
            "view",
            str(issue_number),
            "--repo",
            GITHUB_REPO,
            "--json",
            "number,title,state,body,url,labels,createdAt,updatedAt",
        ],
        cwd=REPO_ROOT,
        timeout=60,
    )
    try:
        payload = json.loads(str(result.get("stdout") or "{}"))
    except json.JSONDecodeError as exc:
        raise WorkflowError(f"cannot parse GitHub issue #{issue_number}: {exc}") from exc
    if not payload.get("number"):
        raise WorkflowError(f"GitHub issue not found or unreadable: {issue_number}")
    return payload


def _stale_pr_check_for_bug(bug_id: str) -> dict[str, Any]:
    if not (REPO_ROOT / ".git").exists():
        return {"status": "skipped_no_git_checkout", "open_prs": [], "merged_prs": []}
    result_open = _run_command(
        [
            "gh",
            "pr",
            "list",
            "--repo",
            GITHUB_REPO,
            "--state",
            "open",
            "--search",
            f"{bug_id} in:title,body",
            "--json",
            "number,title,url,headRefName,body",
            "--limit",
            "20",
        ],
        cwd=REPO_ROOT,
        timeout=20,
    )
    result_merged = _run_command(
        [
            "gh",
            "pr",
            "list",
            "--repo",
            GITHUB_REPO,
            "--state",
            "merged",
            "--search",
            f"{bug_id} in:title,body",
            "--json",
            "number,title,url,headRefName,mergedAt,body",
            "--limit",
            "20",
        ],
        cwd=REPO_ROOT,
        timeout=20,
    )
    if not result_open.get("ok") and not result_merged.get("ok"):
        return {
            "status": "unavailable",
            "open_prs": [],
            "merged_prs": [],
            "error": result_open.get("stderr") or result_merged.get("stderr") or "gh pr list failed",
        }

    def parse(result: dict[str, Any]) -> list[dict[str, Any]]:
        if not result.get("ok"):
            return []
        try:
            data = json.loads(str(result.get("stdout") or "[]"))
        except json.JSONDecodeError:
            return []
        return data if isinstance(data, list) else []

    open_prs = parse(result_open)
    merged_prs = parse(result_merged)
    cleanup_needed = bool(open_prs and merged_prs)
    return {
        "status": "cleanup_recommended" if cleanup_needed else "checked",
        "open_prs": open_prs,
        "merged_prs": merged_prs,
        "cleanup_plan": [
            "inspect open registry-only PRs for this BUG",
            "close stale registry-only PR if a merged fix PR already resolved the BUG",
        ] if cleanup_needed else [],
    }


def _find_bug_by_github_issue(issue_number: int | str) -> tuple[dict[str, Any], Path] | None:
    target = int(issue_number)
    for path in _bug_files():
        record = _load_json(path)
        if int(record.get("github_issue_number") or 0) == target:
            return record, path
    return None


def _shell_quote(value: str) -> str:
    text = str(value)
    return '"' + text.replace('"', '\\"') + '"'


def _github_issue_label_names(issue: dict[str, Any]) -> list[str]:
    names: list[str] = []
    for item in issue.get("labels") or []:
        if isinstance(item, dict):
            name = str(item.get("name") or "").strip()
        else:
            name = str(item or "").strip()
        if name:
            names.append(name)
    return flow._unique_strings(names)


def _infer_bug_module_from_github_issue(issue: dict[str, Any]) -> str | None:
    for label in _github_issue_label_names(issue):
        lower = label.lower()
        if lower.startswith("module:"):
            value = lower.split(":", 1)[1].strip()
            return value or None
        if lower == "paper-v2":
            return "paper_v2"
        if lower == "qe":
            return "qe"
    return None


def _infer_bug_severity_from_github_issue(issue: dict[str, Any]) -> str | None:
    text = " ".join([str(issue.get("title") or ""), *_github_issue_label_names(issue)])
    match = re.search(r"\bP([0-3])\b", text, flags=re.IGNORECASE)
    return f"P{match.group(1)}" if match else None


def _adopt_bug_command(*, bug_id: str, title: str, module: str | None, severity: str | None, issue_number: Any, issue_url: str) -> str:
    module_arg = module or "<module>"
    severity_arg = severity or "P1"
    return (
        "python scripts/aistock_issue_workflow.py submit-bug "
        f"--bug-id {bug_id} --title {_shell_quote(title)} "
        f"--module {module_arg} --severity {severity_arg} "
        f"--github-issue-number {issue_number} --github-issue-url {issue_url} "
        "--create-fix-worktree --apply"
    )


def _missing_bug_record_recovery_payload(bug_id: str) -> dict[str, Any]:
    normalized = bug_id.strip().upper()
    github_issue, warnings = _github_bug_issue_for_id(normalized)
    if not github_issue:
        return {
            "schema_version": "aistock_issue_workflow_missing_bug_record_v1",
            "generated_at": _utc_now(),
            "bug_id": normalized,
            "workflow_gate": "blocked",
            "reason": "local_bug_json_missing",
            "blocking": [f"BUG record not found: {normalized}"],
            "warnings": warnings,
            "next_command": _adopt_bug_command(
                bug_id=normalized,
                title=f"{normalized} <title>",
                module=None,
                severity=None,
                issue_number="<issue_number>",
                issue_url="<issue_url>",
            ),
        }

    issue_number = github_issue.get("github_issue_number")
    issue_url = str(github_issue.get("source") or _github_issue_url(issue_number))
    issue_title = str(github_issue.get("title") or normalized)
    module = _infer_bug_module_from_github_issue(github_issue)
    severity = _infer_bug_severity_from_github_issue(github_issue)
    return {
        "schema_version": "aistock_issue_workflow_missing_bug_record_v1",
        "generated_at": _utc_now(),
        "bug_id": normalized,
        "workflow_gate": "missing_bug_record",
        "reason": "github_issue_exists_without_local_bug_json",
        "github_issue": {
            "number": issue_number,
            "url": issue_url,
            "state": github_issue.get("github_state"),
            "title": issue_title,
            "labels": _github_issue_label_names(github_issue),
        },
        "inferred_module": module,
        "inferred_severity": severity,
        "blocking": [f"{normalized} exists on GitHub but local BUG JSON is missing from this checkout"],
        "warnings": warnings,
        "next_actions": [
            "adopt_or_reconstruct_bug_json_in_isolated_worktree",
            "avoid_creating_duplicate_bug_id",
        ],
        "next_command": _adopt_bug_command(
            bug_id=normalized,
            title=issue_title,
            module=module,
            severity=severity,
            issue_number=issue_number,
            issue_url=issue_url,
        ),
    }


def find_bug_record(bug_id: str | None = None, issue_json: str | None = None) -> tuple[dict[str, Any], Path]:
    if issue_json:
        path = Path(issue_json)
        record = _load_json(path)
        return record, path
    if not bug_id:
        raise WorkflowError("Either --bug-id or --issue-json is required")
    normalized = bug_id.strip().upper()
    matches: list[tuple[dict[str, Any], Path]] = []
    for path in _bug_files():
        record = _load_json(path)
        if str(record.get("bug_id") or "").upper() == normalized:
            matches.append((record, path))
    if not matches:
        raise WorkflowPayloadError(_missing_bug_record_recovery_payload(normalized))
    if len(matches) > 1:
        raise WorkflowError(f"Multiple BUG records found for {normalized}: {[str(path) for _, path in matches]}")
    return matches[0]


def _find_bug_record_from_active_registry(bug_id: str) -> tuple[dict[str, Any], Path] | None:
    for item in sorted(
        _active_workflows_for_bug(bug_id),
        key=lambda entry: (
            1 if str(entry.get("workflow_role") or "").startswith("registry") else 0,
            1 if entry.get("issue_json") else 0,
            str(entry.get("root") or ""),
        ),
        reverse=True,
    ):
        if not str(item.get("workflow_role") or "").startswith("registry"):
            continue
        issue_json = item.get("issue_json")
        if not issue_json:
            continue
        path = Path(str(issue_json))
        if path.exists():
            return _load_json(path), path
    return None



def _normalize_module_label(module: str | None) -> str:
    module_label = re.sub(r"[^a-zA-Z0-9_.-]+", "_", str(module or "unknown").strip().lower()).strip("_")
    return module_label or "unknown"


def _small_text_blob(parts: Iterable[str | None]) -> str:
    text = ""
    for part in parts:
        if part is None:
            continue
        text += f"\n{part}"
    return text


def _csv_arg(items: Iterable[str]) -> str:
    result = ""
    for item in items:
        if not item:
            continue
        result = item if not result else f"{result},{item}"
    return result


def _is_ui_issue(title: str | None, module: str | None, changed_files: list[str], description: str | None = None) -> bool:
    normalized_paths = [path.replace("\\", "/") for path in changed_files]
    has_frontend_scope = any(path.startswith("frontend/") or "/frontend/" in path for path in normalized_paths)
    has_ui_catalog_scope = any(
        path.startswith("frontend/tests/")
        or path.startswith("tests/aistock_validation/catalog/ui_targets")
        for path in normalized_paths
    )
    if has_frontend_scope or has_ui_catalog_scope:
        return True
    # UI words in workflow scripts or command text (for example statusCheckRollup)
    # must not create false visual-acceptance routes.
    if changed_files and not any(path.startswith(("frontend/", "tests/e2e/", "playwright")) for path in normalized_paths):
        return False
    haystack = _small_text_blob([str(title or ""), str(module or ""), str(description or "")]).lower()
    return any(token.lower() in haystack for token in UI_KEYWORDS)


def _ui_intake_hints(
    *,
    title: str,
    module: str,
    description: str | None,
    changed_files: list[str],
    reproduce_command: str | None,
) -> dict[str, Any] | None:
    if not _is_ui_issue(title, module, changed_files, description):
        return None
    normalized_module = _normalize_module_label(module)
    hint = UI_ROUTE_HINTS.get(normalized_module) or next(
        (value for key, value in UI_ROUTE_HINTS.items() if key in normalized_module),
        None,
    )
    route = None
    search_text = _small_text_blob([title, description or "", *changed_files])
    route_match = re.search(r"(/[a-zA-Z0-9_.~:/?#\[\]@!$&'()*+,;=-]+)", search_text)
    if route_match:
        route = route_match.group(1).rstrip(".,;。；")
    routes = list((hint or {}).get("routes") or [])
    if route and route not in routes:
        routes.insert(0, route)
    scope = flow._unique_strings(list((hint or {}).get("scope") or []) + changed_files)
    verification = flow._unique_strings(list((hint or {}).get("verification") or []) + ["l0"])
    reproduce_missing = not reproduce_command or reproduce_command.strip().lower() in {"n/a", "na", "none"}
    return {
        "schema_version": "aistock_ui_issue_intake_hints_v1",
        "ui_issue": True,
        "scope_source": "inferred_from_module_route_and_changed_files",
        "ui_route": routes[0] if routes else None,
        "ui_routes": routes,
        "ui_component_scope": scope,
        "recommended_verification": verification,
        "reproduce_required": reproduce_missing,
        "reproduce_template": [
            "Open the affected AIstock page/route.",
            "Perform the visible user action that currently fails or looks wrong.",
            "Verify the expected visible state, table, dialog, or form behavior.",
        ],
        "visual_acceptance_required": True,
        "labels": ["bug", "ui", f"module:{normalized_module}", "type:bug"],
    }


def _issue_labels_for_bug(*, module: str, severity: str, ui_hints: dict[str, Any] | None = None) -> list[str]:
    normalized_module = _normalize_module_label(module)
    module_label = f"module:{normalized_module}"
    if normalized_module in {"advisory", "paper_v2_advisory"}:
        module_label = "module:paper_v2"
    labels = ["aistock:bug", "bug", severity.upper(), f"severity:{severity.lower()}", module_label, "status:open"]
    if ui_hints and module_label == "module:paper_v2":
        labels.append("paper-v2")
    return flow._unique_strings(labels)


def _workflow_efficiency_recommendations(record: dict[str, Any], ui_hints: dict[str, Any] | None = None) -> dict[str, Any]:
    module = _normalize_module_label(record.get("module"))
    required = record.get("required_verification") or []
    recs = [
        "Use compact success output; request full JSON only for failures or diagnostics.",
        "Run targeted validation first, then final gates once the patch is stable.",
    ]
    batch_candidate = module in {"validation", "validation.guardrails", "validation_center"} or str(record.get("risk_area") or "") in {"ci_failure_intake", "workflow"}
    if batch_candidate:
        recs.append("Batch compatible workflow/CI/docs changes into one PR with per-issue evidence.")
    if ui_hints:
        recs.append("Use inferred UI route/scope to avoid broad repo scans; validate with frontend tsc and focused E2E when available.")
    if any(str(item).startswith("validation_center_backend") for item in required):
        recs.append("Keep validation_center_backend only when the changed files actually affect Validation Center.")
    return {
        "schema_version": "aistock_workflow_efficiency_recommendations_v1",
        "batch_candidate": batch_candidate,
        "docs_only_merge_with_related_code": True,
        "compact_success_output": True,
        "full_json_on_failure_only": True,
        "recommendations": recs,
    }

def _render_github_issue_body(record: dict[str, Any], candidate: dict[str, Any]) -> str:
    evidence = record.get("evidence_uris") or []
    scope = record.get("allowed_write_scope") or []
    verification = record.get("required_verification") or []
    ui_hints = record.get("ui_intake_hints") if isinstance(record.get("ui_intake_hints"), dict) else None
    lines = [
        f"<!-- aistock-bug:{record.get('bug_id')} -->",
        f"<!-- aistock-candidate:{candidate.get('candidate_id')} -->",
        "",
        "## Summary",
        str(record.get("description") or record.get("title") or ""),
        "",
        "## Expected",
        str(record.get("expected") or "Expected behavior should be restored."),
        "",
        "## Actual",
        str(record.get("actual") or record.get("description") or ""),
        "",
        "## Reproduce",
        f"`{record.get('reproduce_command') or 'n/a'}`",
        "",
    ]
    if ui_hints:
        lines.extend([
            "## UI Intake Hints",
            f"- route: `{ui_hints.get('ui_route') or 'unknown'}`",
            f"- reproduce_required: `{ui_hints.get('reproduce_required')}`",
            "- visual_acceptance_required: `true`",
            "- inferred_scope:",
            *[f"  - `{item}`" for item in ui_hints.get('ui_component_scope') or []],
            "- recommended_verification:",
            *[f"  - `{item}`" for item in ui_hints.get('recommended_verification') or []],
            "",
        ])
    lines.extend([
        "## Scope",
        *[f"- `{item}`" for item in scope or ["triage required"]],
        "",
        "## Required Verification",
        *[f"- `{item}`" for item in verification or ["l0"]],
        "",
        "## Evidence",
        *[f"- {item}" for item in evidence or ["n/a"]],
        "",
        "## Workflow Gates",
        "- production_ddl_gate: `noop`",
        "- production_frontend_dependency_gate: `noop`",
        "- production_backend_dependency_gate: `noop`",
    ])
    return "\n".join(lines)


def build_submit_bug_plan(
    *,
    title: str,
    module: str,
    severity: str,
    description: str | None,
    expected: str | None,
    actual: str | None,
    reproduce_command: str | None,
    evidence_refs: list[str],
    changed_files: list[str],
    plan_key: str | None,
    nox_session: str | None,
    candidate_type: str,
    bug_id: str | None,
    github_issue_number: str | None,
    github_issue_url: str | None,
    create_github: bool,
    apply: bool,
    allow_current_worktree: bool = False,
    create_registry_worktree: bool = False,
    create_fix_worktree: bool = False,
    registry_pr_only: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    effective_apply = apply and not dry_run
    if create_fix_worktree and create_registry_worktree:
        raise WorkflowError("--create-fix-worktree and --create-registry-worktree are mutually exclusive")
    if create_fix_worktree and registry_pr_only:
        raise WorkflowError("--create-fix-worktree cannot be combined with --registry-pr-only")
    if effective_apply and not create_github and not (github_issue_number and github_issue_url):
        raise WorkflowError("--apply requires --create-github or existing --github-issue-number and --github-issue-url")
    registry_worktree_plan = _maybe_create_registry_worktree(
        title=title,
        module=module,
        severity=severity,
        create=create_registry_worktree,
        dry_run=dry_run or not apply,
    )
    registry_root = (
        Path(registry_worktree_plan["worktree"])
        if create_registry_worktree
        else _registry_target_root()
    )
    github_create_root = _canonical_root() if create_fix_worktree else registry_root
    allocation_root = registry_root if registry_root.exists() else _registry_target_root()
    registry_guard = None if (effective_apply and create_fix_worktree) else (
        _validate_registry_apply_target(registry_root) if effective_apply else None
    )
    if registry_guard and registry_guard["blocking"] and not allow_current_worktree:
        raise WorkflowError("; ".join(registry_guard["blocking"]))
    reservation_path: Path | None = None
    include_github_scan = create_github or bool(github_issue_number and github_issue_url)
    canonical_bug_id: str
    allocated_number: int
    allocation_report: dict[str, Any]
    try:
        if effective_apply:
            canonical_bug_id, allocated_number, allocation_report, reservation_path = _reserve_bug_id(
                allocation_root,
                bug_id=bug_id,
                include_github=include_github_scan,
                github_required=create_github,
                allowed_github_issue_number=github_issue_number,
            )
        else:
            allocation_report = _bug_id_allocation_report(
                allocation_root,
                include_github=include_github_scan,
                github_required=False,
            )
            canonical_bug_id = (bug_id or "").strip().upper() or f"BUG-{int(allocation_report['next_number']):03d}"
            number = _bug_id_number(canonical_bug_id)
            if not number or not re.fullmatch(r"BUG-\d{3,}", canonical_bug_id):
                raise WorkflowError("--bug-id must match BUG-NNN when provided")
            duplicates = _duplicate_bug_id_sources(
                allocation_report,
                canonical_bug_id,
                allowed_github_issue_number=github_issue_number,
            )
            if duplicates:
                detail = "; ".join(f"{item.get('kind')}:{item.get('source')}" for item in duplicates[:5])
                raise WorkflowError(f"{canonical_bug_id} already exists in global BUG id scan: {detail}")
            allocated_number = number

        event_args = argparse.Namespace(
            source="manual",
            source_json=None,
            title=title,
            module=module,
            severity_guess=severity,
            actual=actual or description or title,
            plan_key=plan_key,
            nox_session=nox_session,
            reproduce_command=reproduce_command,
            evidence_ref=evidence_refs,
            changed_file=changed_files,
        )
        event = flow.build_failure_event(event_args)
        candidate = flow.candidate_from_event(
            event,
            title=title,
            candidate_type=candidate_type,
            expected=expected,
            actual=actual or description,
        )
        record = flow.promote_candidate_to_bug(
            candidate,
            bug_id=canonical_bug_id,
            github_issue_number=github_issue_number,
            github_issue_url=github_issue_url,
        )
        now = _utc_now()
        record.setdefault("created_at", now)
        record.setdefault("first_seen_at", now)
        record.setdefault("last_seen_at", now)
        record.setdefault("assigned_agent", None)
        record.setdefault("fix_branch", None)
        record.setdefault("fix_commit", None)
        record.setdefault("verification_run_id", None)
        record.setdefault("github_issue_number", None)
        record.setdefault("github_issue_url", None)
        record.setdefault("fixed_at", None)
        record.setdefault("closed_at", None)
        record.setdefault("production_ddl_gate", "noop")
        record.setdefault("production_frontend_dependency_gate", "noop")
        record.setdefault("production_backend_dependency_gate", "noop")
        ui_hints = _ui_intake_hints(
            title=title,
            module=module,
            description=description,
            changed_files=changed_files,
            reproduce_command=reproduce_command,
        )
        if ui_hints:
            record["ui_intake_hints"] = ui_hints
            _add_record_allowed_scope(record, *(ui_hints.get("ui_component_scope") or []))
            record["required_verification"] = flow._unique_strings(
                flow._as_list(record.get("required_verification")) + list(ui_hints.get("recommended_verification") or [])
            )
        record["workflow_efficiency_recommendations"] = _workflow_efficiency_recommendations(record, ui_hints)

        output_dir = registry_root / WORKFLOW_ROOT / canonical_bug_id
        candidate_path = output_dir / "candidate.json"
        github_body_path = output_dir / "github-issue-body.md"
        bug_path = _bug_json_path(record, registry_root)
        _add_record_allowed_scope(
            record,
            _repo_rel(bug_path, registry_root),
            _repo_rel(_allocator_path(registry_root), registry_root),
        )
        github_result: dict[str, Any] | None = None

        if effective_apply and bug_path.exists():
            raise WorkflowError(f"BUG JSON already exists: {bug_path}")

        if create_github and not record.get("github_issue_url") and effective_apply:
            github_body_for_create = github_body_path
            if create_fix_worktree:
                github_body_for_create = Path(tempfile.gettempdir()) / "aistock_issue_workflow" / f"{canonical_bug_id}-github-issue-body.md"
            _write_text(github_body_for_create, _render_github_issue_body(record, candidate))
            github_title = f"{canonical_bug_id} {severity}: {title}"
            result = _execute_checked(
                [
                    "gh",
                    "issue",
                    "create",
                    "--repo",
                    GITHUB_REPO,
                    "--title",
                    github_title,
                    "--body-file",
                    str(github_body_for_create),
                    "--label",
                    _csv_arg(_issue_labels_for_bug(module=module, severity=severity, ui_hints=ui_hints)),
                ],
        cwd=github_create_root,
                timeout=120,
            )
            issue_url = str(result.get("stdout") or "").splitlines()[-1].strip()
            issue_number = _github_issue_number_from_url(issue_url)
            if not issue_url or not issue_number:
                raise WorkflowError(f"cannot parse created GitHub issue URL: {issue_url!r}")
            record["github_issue_url"] = issue_url
            record["github_issue_number"] = issue_number
            github_result = {"created": True, "url": issue_url, "number": issue_number}
        elif create_github and not record.get("github_issue_url"):
            github_result = {"created": False, "planned": True, "body_path": _repo_rel(github_body_path, registry_root)}

        has_linkage = bool(record.get("github_issue_number") and record.get("github_issue_url"))
        if effective_apply and not has_linkage:
            raise WorkflowError("--apply requires --create-github or existing --github-issue-number and --github-issue-url")
        fix_worktree_plan = _maybe_create_fix_chain_worktree(
            record=record,
            bug_id=canonical_bug_id,
            create=create_fix_worktree,
            dry_run=dry_run or not apply,
        )
        fix_chain_root = Path(fix_worktree_plan["worktree"]) if create_fix_worktree else None
        write_root = fix_chain_root if fix_chain_root is not None and effective_apply else registry_root
        write_output_dir = write_root / WORKFLOW_ROOT / canonical_bug_id
        write_candidate_path = write_output_dir / "candidate.json"
        write_github_body_path = write_output_dir / "github-issue-body.md"
        write_bug_path = _issue_json_path_for_worktree(bug_path, write_root) if write_root != registry_root else bug_path
        fix_worktree_guard = _validate_registry_apply_target(write_root) if (effective_apply and create_fix_worktree) else None
        if fix_worktree_guard and fix_worktree_guard["blocking"] and not allow_current_worktree:
            raise WorkflowError("; ".join(fix_worktree_guard["blocking"]))
        if write_root != registry_root:
            _add_record_allowed_scope(record, _repo_rel(write_bug_path, write_root))

        payload = {
        "schema_version": "aistock_issue_workflow_submit_bug_v1",
        "generated_at": now,
        "bug_id": canonical_bug_id,
        "candidate_id": candidate.get("candidate_id"),
        "dry_run": not effective_apply,
        "workflow_gate": "submitted" if effective_apply else ("ready_for_apply" if has_linkage or create_github else "needs_github_sync"),
        "registry_root": str(registry_root),
        "registry_guard": registry_guard or (
            {
                "target_root": str(registry_root),
                "canonical_root": str(_canonical_root().resolve()),
                "blocking": [],
                "warnings": [
                    "BUG registry will be persisted in the fix worktree/branch"
                    if create_fix_worktree
                    else "registry worktree will be created on apply"
                ],
                "planned": True,
            }
            if (create_fix_worktree or (create_registry_worktree and not registry_root.exists()))
            else _validate_registry_apply_target(registry_root)
        ),
        "registry_worktree_plan": registry_worktree_plan,
        "fix_worktree_plan": fix_worktree_plan,
        "fix_worktree_guard": fix_worktree_guard,
        "registration_strategy": "fix_pr_persists_bug_registration" if create_fix_worktree else "registry_intake_then_fix_worktree",
        "candidate_path": _repo_rel(candidate_path, registry_root),
        "github_issue_body_path": _repo_rel(github_body_path, registry_root),
        "bug_json_path": _repo_rel(bug_path, registry_root),
        "github": github_result or {
            "created": False,
            "number": record.get("github_issue_number"),
            "url": record.get("github_issue_url"),
        },
        "record": record,
        "ui_intake_hints": ui_hints,
        "workflow_efficiency_recommendations": record.get("workflow_efficiency_recommendations"),
        "github_issue_labels": _issue_labels_for_bug(module=module, severity=severity, ui_hints=ui_hints),
        "registry_pr_only": registry_pr_only,
        "stale_pr_check": _stale_pr_check_for_bug(canonical_bug_id) if effective_apply else {"status": "not_applicable_before_apply"},
        "bug_id_allocation": {
            "allocator_root": str(allocation_root),
            "global_max_number": allocation_report.get("max_number"),
            "github_scanned": allocation_report.get("github_scanned"),
            "warnings": allocation_report.get("warnings", []),
            "reservation_path": str(reservation_path) if reservation_path else None,
        },
        "next_agent_steps": [
            "switch_to_fix_worktree_and_continue_fix" if create_fix_worktree else "switch_to_registry_worktree",
            "commit_registry_only_pr_without_fix" if registry_pr_only else (
                "continue_fix_in_same_branch" if create_fix_worktree else "commit_registry_files_then_continue_fix_worktree"
            ),
            "do_not_write_bug_json_in_canonical_root",
        ] if effective_apply else [
            "create_or_switch_to_clean_registry_worktree",
            "rerun_submit_bug_with_github_linkage",
        ],
        "next_command": (
            f"cd /d {fix_chain_root} && python scripts/aistock_issue_workflow.py run --bug-id {canonical_bug_id} "
            f"--issue-json \"{_repo_rel(write_bug_path, write_root)}\" --mode plan"
            if effective_apply and create_fix_worktree and fix_chain_root
            else _registry_commit_next_command(
                registry_root,
                canonical_bug_id,
                continue_fix=not registry_pr_only,
                issue_json=_repo_rel(bug_path, registry_root),
            )
        )
        if effective_apply
        else (
            f"python scripts/aistock_issue_workflow.py submit-bug --title \"{title}\" --module {module} "
            f"--severity {severity} --create-github --create-registry-worktree --apply"
        ),
        }

        if effective_apply:
            _write_json(write_candidate_path, {"event": event, "candidate": candidate})
            _write_text(write_github_body_path, _render_github_issue_body(record, candidate))
            _write_json(write_bug_path, record)
            _write_allocator(allocated_number, write_root)
            fix_registration_commit = (
                _commit_bug_registration_in_fix_worktree(write_root, canonical_bug_id)
                if create_fix_worktree and write_root != registry_root
                else None
            )
            _write_state(
                canonical_bug_id,
                state="discovered",
                root=write_root,
                branch=_branch_for_path(write_root),
                workflow_role="fix" if create_fix_worktree else "registry_intake",
                registry_pr_only=registry_pr_only,
                source_bug_json=_repo_rel(write_bug_path, write_root),
                target_bug_json=_repo_rel(write_bug_path, write_root),
                candidate_path=_repo_rel(write_candidate_path, write_root),
                github_issue_number=record.get("github_issue_number"),
                github_issue_url=record.get("github_issue_url"),
                worktree=str(write_root) if create_fix_worktree else None,
                fix_chain_registration_commit=fix_registration_commit,
                next_actions=(
                    ["run_issue_workflow_plan_in_current_fix_worktree", "read_context_pack", "fix", "validate", "create_pr"]
                    if create_fix_worktree
                    else ["run_issue_workflow_plan", "create_worktree", "read_context_pack"]
                ),
            )
            payload["state_path"] = _repo_rel(_state_path(canonical_bug_id, write_root), write_root)
            payload["events_path"] = _repo_rel(_events_path(canonical_bug_id, write_root), write_root)
            payload["candidate_path"] = _repo_rel(write_candidate_path, write_root)
            payload["github_issue_body_path"] = _repo_rel(write_github_body_path, write_root)
            payload["bug_json_path"] = _repo_rel(write_bug_path, write_root)
            if fix_registration_commit:
                payload["fix_registration_commit"] = fix_registration_commit
            payload["fix_chain"] = {
                "registry_pr_required": registry_pr_only,
                "continue_to_fix_in_same_workflow": not registry_pr_only,
                "run_next_command": payload["next_command"],
                "next_command": (
                    None
                    if registry_pr_only
                    else (
                        f"cd /d {fix_chain_root} && python scripts/aistock_issue_workflow.py run --bug-id {canonical_bug_id} "
                        f"--issue-json \"{_repo_rel(write_bug_path, write_root)}\" --mode plan"
                        if create_fix_worktree and fix_chain_root
                        else (
                            f"python scripts/aistock_issue_workflow.py run --bug-id {canonical_bug_id} "
                            f"--issue-json \"{_repo_rel(bug_path, registry_root)}\" --mode plan --create-worktree"
                        )
                    )
                ),
                "default_path": (
                    "registry_pr_only"
                    if registry_pr_only
                    else ("single_fix_branch_registration_and_fix" if create_fix_worktree else "commit_registry_then_create_fix_worktree")
                ),
                "stop_reason": "registry_pr_only_requested" if registry_pr_only else None,
                "note": (
                    "User explicitly requested registry-only tracking; stop after the registry PR."
                    if registry_pr_only
                    else (
                        "BUG registration has been committed in the fix branch; continue coding and include that BUG JSON in the fix PR."
                        if create_fix_worktree
                        else "BUG registration seeds a registry-intake state. Commit the BUG JSON, then create a separate fix worktree from latest origin/main using the registry BUG JSON until it lands on main."
                    )
                ),
            }
        return payload
    except Exception:
        _release_bug_id_reservation(reservation_path)
        raise


def _require_github_linkage(record: dict[str, Any], *, allow_missing: bool = False) -> list[str]:
    missing = [key for key in ("github_issue_number", "github_issue_url") if not record.get(key)]
    if missing and not allow_missing:
        bug_id = record.get("bug_id") or "<unknown>"
        raise WorkflowError(f"{bug_id} missing GitHub linkage: {', '.join(missing)}")
    return missing


def _require_fixable_status(record: dict[str, Any], *, allow_closed: bool = False) -> str:
    status = str(record.get("status") or "").strip()
    if not allow_closed and status not in ALLOWED_FIX_STATUSES:
        bug_id = record.get("bug_id") or "<unknown>"
        raise WorkflowError(f"{bug_id} status is {status!r}; only {sorted(ALLOWED_FIX_STATUSES)} are fixable by default")
    return status


def _target_names(record: dict[str, Any], bug_id: str, task_slug: str | None = None) -> tuple[str, Path]:
    title_slug = _slug(task_slug or str(record.get("title") or bug_id), max_len=48)
    today = _today_compact()
    branch = f"bug/{bug_id}-{title_slug}-{today}"
    worktree = _default_worktree_root() / f"{bug_id}-{title_slug}-{today}"
    return branch, worktree


def _batch_target_names(batch_id: str, module: str, task_slug: str | None = None) -> tuple[str, Path]:
    batch_slug = _slug(task_slug or f"{batch_id}-{module}", max_len=56)
    today = _today_compact()
    branch = f"bug/{batch_slug}-{today}"
    worktree = _default_worktree_root() / f"{batch_slug}-{today}"
    return branch, worktree


def _maybe_create_worktree(
    *,
    record: dict[str, Any],
    bug_id: str,
    source_bug_json: Path,
    create: bool,
    dry_run: bool,
    task_slug: str | None,
) -> dict[str, Any]:
    branch, worktree = _target_names(record, bug_id, task_slug)
    plan = {
        "create_worktree": create,
        "dry_run": dry_run,
        "branch": branch,
        "worktree": str(worktree),
        "base": "origin/main",
    }
    if not create or dry_run:
        return plan
    if worktree.exists():
        raise WorkflowError(f"target worktree already exists: {worktree}")
    _git(["fetch", "origin", "main"])
    _git_worktree_add_new_branch(worktree=worktree, branch=branch)
    copied_bug_json = _ensure_issue_json_in_worktree(source_bug_json, worktree, record=record)
    plan["seeded_issue_json"] = _repo_rel(copied_bug_json, worktree)
    plan["created"] = True
    return plan


def _actual_and_planned_worktree(worktree_plan: dict[str, Any]) -> tuple[str | None, str | None]:
    worktree = str(worktree_plan.get("worktree") or "").strip() or None
    if worktree_plan.get("created"):
        return worktree, None
    return None, worktree


def _maybe_create_named_worktree(
    *,
    branch: str,
    worktree: Path,
    create: bool,
    dry_run: bool,
) -> dict[str, Any]:
    plan = {
        "create_worktree": create,
        "dry_run": dry_run,
        "branch": branch,
        "worktree": str(worktree),
        "base": "origin/main",
    }
    if not create or dry_run:
        return plan
    if worktree.exists():
        raise WorkflowError(f"target worktree already exists: {worktree}")
    _git(["fetch", "origin", "main"])
    _git_worktree_add_new_branch(worktree=worktree, branch=branch)
    plan["created"] = True
    return plan


def _bug_path_for_target(original_path: Path, target_root: Path) -> Path:
    try:
        relative = original_path.resolve().relative_to(REPO_ROOT.resolve())
        return target_root / relative
    except Exception:
        return original_path


def _issue_query(record: dict[str, Any], changed_files: list[str] | None = None) -> str:
    parts = [
        str(record.get("bug_id") or record.get("candidate_id") or "AIstock issue"),
        str(record.get("title") or ""),
        str(record.get("module") or record.get("module_guess") or ""),
        str(record.get("description") or record.get("actual") or ""),
    ]
    if changed_files:
        parts.extend(changed_files[:8])
    return " ".join(part for part in parts if part).strip() or "AIstock issue"


def _build_code_intelligence_summary(
    *,
    item_id: str,
    record: dict[str, Any],
    changed_files: list[str] | None,
    root: Path,
) -> dict[str, Any]:
    return code_intelligence.build_summary(
        item_id=item_id,
        query=_issue_query(record, changed_files),
        changed_files=changed_files or [],
        module=str(record.get("module") or "").strip() or None,
        root=root,
        skip_external=False,
    )


def _compact_code_intelligence_for_task_card(code_intelligence_summary: dict[str, Any]) -> dict[str, Any]:
    ua = code_intelligence_summary.get("understand_anything")
    ua_summary = code_intelligence_summary.get("understand_anything_summary")
    return {
        "provider": code_intelligence_summary.get("provider"),
        "status": code_intelligence_summary.get("status"),
        "context_ref": code_intelligence_summary.get("context_ref"),
        "manifest_ref": code_intelligence_summary.get("manifest_ref"),
        "affected_tests_ref": code_intelligence_summary.get("affected_tests_ref"),
        "affected_tests_count": code_intelligence_summary.get("affected_tests_count"),
        "affected_quality": code_intelligence_summary.get("affected_quality"),
        "latest_freshness": code_intelligence_summary.get("latest_freshness"),
        "latest_freshness_ref": code_intelligence_summary.get("latest_freshness_ref"),
        "consume_command": code_intelligence_summary.get("consume_command"),
        "verify_command": code_intelligence_summary.get("verify_command"),
        "stale_metadata_warning": bool(code_intelligence_summary.get("stale_metadata_warning")),
        "context_quality": (
            ((code_intelligence_summary.get("context") or {}).get("context_quality") or {}).get("quality")
            if isinstance(code_intelligence_summary.get("context"), dict)
            else None
        ),
        "noisy_context_warning": bool(
            ((code_intelligence_summary.get("context") or {}).get("context_quality") or {}).get("noisy_context_warning")
        )
        if isinstance(code_intelligence_summary.get("context"), dict)
        else False,
        "fallback_used": bool(code_intelligence_summary.get("fallback_used")),
        "fallback_reason": code_intelligence_summary.get("fallback_reason"),
        "understand_anything_status": (ua or {}).get("status") if isinstance(ua, dict) else None,
        "understand_anything_summary_ref": code_intelligence_summary.get("understand_anything_summary_ref"),
        "understand_anything_nodes_used": (ua_summary or {}).get("nodes_used") if isinstance(ua_summary, dict) else None,
        "understand_anything_graph_exists": (ua_summary or {}).get("graph_exists") if isinstance(ua_summary, dict) else None,
        "understand_anything_generate_graph_command": (ua or {}).get("generate_graph_command") if isinstance(ua, dict) else None,
        "blocking_for_issue_workflow": False,
    }


def build_task_card(
    *,
    bug_id: str,
    record: dict[str, Any],
    root: Path,
    branch: str | None,
    planned_branch: str | None,
    worktree: str | None,
    planned_worktree: str | None,
    context_pack_json_path: Path,
    context_pack_md_path: Path,
    fix_ready_path: Path,
    state_path: Path,
    events_path: Path,
    fix_ready: dict[str, Any],
    context_pack: dict[str, Any],
    code_intelligence_summary: dict[str, Any],
    fast_path: dict[str, Any],
) -> dict[str, Any]:
    validation = fix_ready.get("validation_selection") if isinstance(fix_ready.get("validation_selection"), dict) else {}
    code_intel = _compact_code_intelligence_for_task_card(code_intelligence_summary)
    return {
        "schema_version": "aistock_agent_task_card_v1",
        "generated_at": _utc_now(),
        "task_card_id": f"TC-{bug_id}-{flow._stable_hash(record.get('github_issue_number'), record.get('title'), record.get('module'))}",
        "agent_neutral": True,
        "supported_clients": ["Codex", "Claude Code", "Cursor", "CLI"],
        "bug_id": bug_id,
        "github_issue": {
            "number": record.get("github_issue_number"),
            "url": record.get("github_issue_url"),
        },
        "module": record.get("module"),
        "severity": record.get("severity"),
        "risk_level": fix_ready.get("risk_level"),
        "branch": branch,
        "planned_branch": planned_branch,
        "worktree": worktree,
        "planned_worktree": planned_worktree,
        "artifact_refs": {
            "context_pack_json": _repo_rel(context_pack_json_path, root),
            "context_pack_md": _repo_rel(context_pack_md_path, root),
            "fix_ready_json": _repo_rel(fix_ready_path, root),
            "state_json": _repo_rel(state_path, root),
            "events_jsonl": _repo_rel(events_path, root),
        },
        "problem": {
            "title": record.get("title"),
            "statement": context_pack.get("problem_statement"),
            "reproduce_command": context_pack.get("reproduce_command"),
        },
        "allowed_write_scope": fix_ready.get("allowed_write_scope") or [],
        "required_verification": fix_ready.get("required_verification") or validation.get("required_plans") or [],
        "recommended_verification": fix_ready.get("recommended_verification") or validation.get("recommended_plans") or [],
        "production_gates": validation.get("production_gates") or _production_gates_payload(),
        "code_intelligence": code_intel,
        "fast_path": _pick(fast_path or {}, "task_tier", "module", "workflow_gate", "required_validation", "recommended_validation"),
        "stop_conditions": [
            "missing GitHub linkage",
            "scope expansion required outside allowed_write_scope",
            "required validation cannot run",
            "production runtime or DB action requested without explicit approval",
        ],
        "next_client_steps": [
            "switch_to_worktree_if_created",
            "read task-card.md first, then context-pack.md only when needed",
            "read Code Intelligence refs before rg; broad scans require a scoped miss reason",
            "edit only files under allowed_write_scope or stop for scope expansion",
            "run finish --plan-only before reporting the issue fixed",
        ],
        "token_budget": {
            "context_pack_target_tokens": (context_pack.get("token_budget") or {}).get("target_tokens"),
            "task_card_target_tokens": 2000,
            "large_graph_payload_inlined": False,
        },
        "blocking_for_issue_workflow": False,
    }


def render_task_card_markdown(task_card: dict[str, Any]) -> str:
    artifacts = task_card.get("artifact_refs") if isinstance(task_card.get("artifact_refs"), dict) else {}
    github_issue = task_card.get("github_issue") if isinstance(task_card.get("github_issue"), dict) else {}
    code_intel = task_card.get("code_intelligence") if isinstance(task_card.get("code_intelligence"), dict) else {}
    lines = [
        f"# AIstock Agent Task Card {task_card.get('bug_id')}",
        "",
        f"- task_card_id: `{task_card.get('task_card_id')}`",
        f"- clients: `{', '.join(task_card.get('supported_clients') or [])}`",
        f"- module: `{task_card.get('module') or 'unknown'}`",
        f"- risk_level: `{task_card.get('risk_level') or 'unknown'}`",
        f"- github_issue: {github_issue.get('url') or 'missing'}",
        f"- branch: `{task_card.get('branch') or task_card.get('planned_branch') or 'not_created'}`",
        f"- worktree: `{task_card.get('worktree') or task_card.get('planned_worktree') or 'not_created'}`",
        "",
        "## Start Here",
        *[f"- {item}" for item in task_card.get("next_client_steps") or []],
        "",
        "## Problem",
        f"- title: {((task_card.get('problem') or {}).get('title') if isinstance(task_card.get('problem'), dict) else None) or 'n/a'}",
        f"- reproduce: `{((task_card.get('problem') or {}).get('reproduce_command') if isinstance(task_card.get('problem'), dict) else None) or 'n/a'}`",
        "",
        "## Allowed Write Scope",
        *[f"- `{item}`" for item in task_card.get("allowed_write_scope") or ["triage_only_until_scope_is_set"]],
        "",
        "## Required Verification",
        *[f"- `{item}`" for item in task_card.get("required_verification") or ["l0"]],
        "",
        "## Artifacts",
        *[f"- {key}: `{value}`" for key, value in artifacts.items() if value],
        "",
        "## Code Intelligence",
        f"- status: `{code_intel.get('status') or 'unknown'}`",
        f"- context_ref: `{code_intel.get('context_ref') or 'not_generated'}`",
        f"- affected_tests_ref: `{code_intel.get('affected_tests_ref') or 'not_generated'}`",
        f"- affected_tests_count: `{code_intel.get('affected_tests_count', 0)}`",
        f"- latest_freshness: `{code_intel.get('latest_freshness') or 'not_available'}`",
        f"- latest_freshness_ref: `{code_intel.get('latest_freshness_ref') or 'not_available'}`",
        f"- consume_command: `{code_intel.get('consume_command') or 'python scripts/code_intelligence_adapter.py latest-freshness --refresh-if-stale'}`",
        f"- verify_command: `{code_intel.get('verify_command') or 'python scripts/code_intelligence_adapter.py verify-clients'}`",
        f"- context_quality: `{code_intel.get('context_quality') or 'unknown'}`",
        f"- stale_metadata_warning: `{str(bool(code_intel.get('stale_metadata_warning'))).lower()}`",
        f"- noisy_context_warning: `{str(bool(code_intel.get('noisy_context_warning'))).lower()}`",
        f"- understand_anything_summary_ref: `{code_intel.get('understand_anything_summary_ref') or 'not_generated'}`",
        f"- understand_anything_status: `{code_intel.get('understand_anything_status') or 'unknown'}`",
        f"- understand_anything_graph_exists: `{str(bool(code_intel.get('understand_anything_graph_exists'))).lower()}`",
        f"- understand_anything_nodes_used: `{code_intel.get('understand_anything_nodes_used', 0)}`",
        f"- fallback_used: `{str(bool(code_intel.get('fallback_used'))).lower()}`",
        f"- blocking_for_issue_workflow: `{str(bool(code_intel.get('blocking_for_issue_workflow'))).lower()}`",
        f"- ua_generate_graph_command: `{code_intel.get('understand_anything_generate_graph_command') or 'not_required'}`",
        "",
        "## Production Gates",
        *[f"- {key}: `{value}`" for key, value in sorted((task_card.get("production_gates") or {}).items())],
        "",
        "Large graph payloads are intentionally not inlined. Use artifact refs only when needed.",
    ]
    return "\n".join(lines)


def _batch_code_intelligence_query(records: list[dict[str, Any]], changed_files: list[str] | None = None) -> str:
    parts = ["AIstock batch issue"]
    parts.extend(
        " ".join(
            str(value)
            for value in (
                record.get("bug_id"),
                record.get("title"),
                record.get("module"),
                record.get("description") or record.get("actual"),
            )
            if value
        )
        for record in records
    )
    if changed_files:
        parts.extend(changed_files[:12])
    return " ".join(part for part in parts if part).strip() or "AIstock batch issue"


def _record_scope(record: dict[str, Any]) -> list[str]:
    return flow._unique_strings(
        flow._as_list(record.get("allowed_write_scope")) or flow._as_list(record.get("suggested_scope"))
    )


def _record_required_verification(record: dict[str, Any]) -> list[str]:
    return flow._unique_strings(
        flow._as_list(record.get("required_verification")) or flow._as_list(record.get("suggested_validation"))
    )


def _path_matches_scope(path: str, scope: list[str]) -> bool:
    normalized = path.replace("\\", "/").lstrip("./")
    for item in scope:
        allowed = str(item).replace("\\", "/").lstrip("./").rstrip("/")
        if normalized and (
            normalized == allowed
            or normalized.startswith(allowed + "/")
            or flow._pattern_matches(allowed, normalized)
            or flow._pattern_matches(allowed.rstrip("/") + "/**", normalized)
        ):
            return True
    return False


def _batch_scope_check(changed_files: list[str], allowed_scope: list[str]) -> dict[str, Any]:
    changed = _normalize_changed_files(changed_files)
    if not changed:
        return {
            "schema_version": "aistock_batch_scope_check_v1",
            "status": "skipped_no_changed_files",
            "allowed_scope": allowed_scope,
            "changed_files": [],
            "violations": [],
        }
    violations = [path for path in changed if not _path_matches_scope(path, allowed_scope)]
    return {
        "schema_version": "aistock_batch_scope_check_v1",
        "status": "passed" if not violations else "failed",
        "allowed_scope": allowed_scope,
        "changed_files": changed,
        "violations": violations,
    }


def _batch_validation_selector(
    records: list[dict[str, Any]],
    *,
    module: str,
    changed_files: list[str] | None = None,
) -> dict[str, Any]:
    per_issue: dict[str, dict[str, Any]] = {}
    blocking: list[str] = []
    warnings: list[str] = []
    shared_scope = flow._unique_strings(path for record in records for path in _record_scope(record))
    selector_files = _normalize_changed_files(changed_files) or shared_scope
    selected = flow.select_validation(selector_files, module=module)
    required_plans = flow._unique_strings(selected.get("required_plans") or [])
    required_set = set(required_plans)

    for record in records:
        bug_id = str(record.get("bug_id") or record.get("candidate_id") or record.get("title"))
        scope = _record_scope(record)
        required = _record_required_verification(record)
        missing_required = [plan for plan in required if plan and plan not in required_set]
        if not scope:
            blocking.append(f"{bug_id} has no allowed_write_scope/suggested_scope for safe batching")
        if missing_required:
            blocking.append(f"{bug_id} required verification not covered by shared selector: {missing_required}")
        per_issue[bug_id] = {
            "allowed_write_scope": scope,
            "required_verification": required,
            "missing_required_plans": missing_required,
            "covered_by_shared_validation": not missing_required,
        }

    gates = selected.get("production_gates") or {}
    special_gates = {key: value for key, value in gates.items() if value != "noop"}
    if special_gates:
        blocking.append(f"batch includes production/dependency gates and must be split or handled explicitly: {special_gates}")
    if len(shared_scope) > 20:
        warnings.append("batch shared scope is broad; consider splitting before PR if review becomes unclear")

    return {
        "schema_version": "aistock_batch_validation_selector_v1",
        "module": module,
        "shared_files": shared_scope,
        "selector_files": selector_files,
        "selected_validation": selected,
        "required_plans": required_plans,
        "recommended_plans": flow._unique_strings(selected.get("recommended_plans") or []),
        "production_gates": gates,
        "per_issue": per_issue,
        "blocking": blocking,
        "warnings": warnings,
        "workflow_gate": "compatible" if not blocking else "blocked",
    }


def _build_batch_code_intelligence_summary(
    *,
    batch_id: str,
    records: list[dict[str, Any]],
    changed_files: list[str] | None,
    root: Path,
) -> dict[str, Any]:
    return code_intelligence.build_summary(
        item_id=batch_id,
        query=_batch_code_intelligence_query(records, changed_files),
        changed_files=changed_files or [],
        module=str(records[0].get("module") or "").strip() if records else None,
        root=root,
        skip_external=False,
    )


def _normalize_changed_files(changed_files: list[str] | None) -> list[str]:
    return flow._unique_strings(
        [
            str(path).replace("\\", "/").lstrip("./")
            for path in changed_files or []
            if str(path).strip()
        ]
    )


def _path_in_prefixes(path: str, prefixes: tuple[str, ...]) -> bool:
    normalized = path.replace("\\", "/").lstrip("./")
    return any(normalized == prefix.rstrip("/") or normalized.startswith(prefix) for prefix in prefixes)


def _tier_ge(left: str, right: str) -> bool:
    return FAST_PATH_TIER_ORDER[left] >= FAST_PATH_TIER_ORDER[right]


def _bump_fast_path_tier(current: str, candidate: str) -> str:
    return candidate if _tier_ge(candidate, current) else current


def _plans_to_commands(plan_keys: list[str]) -> list[str]:
    plans = flow._plans_by_key()
    commands: list[str] = []
    for key in plan_keys:
        plan = plans.get(key) or {}
        session = str(plan.get("nox_session") or "").strip()
        if session:
            commands.append(f"python -m nox -s {session}")
        elif key:
            commands.append(f"python -m nox -s {key}")
    return flow._unique_strings(commands)


def _file_category(path: str) -> str:
    normalized = path.replace("\\", "/").lstrip("./")
    if normalized in FAST_PATH_WORKFLOW_FILES:
        return "workflow"
    if _path_in_prefixes(normalized, FAST_PATH_REGISTRY_PREFIXES):
        return "bug_registry"
    if _path_in_prefixes(normalized, FAST_PATH_CATALOG_PREFIXES):
        return "validation_catalog"
    if normalized.startswith("docs/"):
        return "docs"
    if normalized.startswith(".codex/") or normalized.startswith(".claude/"):
        return "client_wrapper"
    if normalized in FAST_PATH_DEPENDENCY_FILES or normalized.endswith((".sql",)):
        return "production_gate_sensitive"
    if "/migrations/" in normalized:
        return "production_gate_sensitive"
    if normalized.startswith("frontend/"):
        return "frontend"
    if normalized.startswith("backend/"):
        return "backend"
    if normalized.startswith("tests/"):
        return "tests"
    if normalized.startswith("scripts/"):
        return "scripts"
    return "other"


def _infer_fast_path_tier(
    *,
    record: dict[str, Any] | None,
    changed_files: list[str],
    validation: dict[str, Any],
    ownership: dict[str, Any],
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    tier = "T0"
    categories = {_file_category(path) for path in changed_files}
    metadata_only = bool(categories) and categories <= {"docs", "client_wrapper", "bug_registry"} and not record
    severity_parts = str((record or {}).get("severity") or "").upper().split()
    severity = severity_parts[0] if severity_parts else ""
    module = str((record or {}).get("module") or "").strip()

    if record:
        tier = _bump_fast_path_tier(tier, "T1")
        reasons.append("linked BUG/GitHub issue requires standard issue traceability")
    if severity in {"P0", "P1"}:
        tier = _bump_fast_path_tier(tier, "T1")
        reasons.append(f"{severity} issue keeps at least T1 validation and evidence")
    if not changed_files:
        reasons.append("no changed files supplied; plan uses issue metadata and l0 fallback")
    if metadata_only:
        reasons.append("docs/client/registry-only scope can stay T0")
    if categories & {"workflow", "validation_catalog", "backend", "frontend", "scripts", "tests"}:
        tier = _bump_fast_path_tier(tier, "T1")
        reasons.append("code, workflow, test, or validation catalog files require T1")
    if categories & {"production_gate_sensitive"}:
        tier = _bump_fast_path_tier(tier, "T2")
        reasons.append("dependency, migration, or DDL-sensitive files require expanded gate checks")
    matched_rules = ownership.get("matched_rules") or []
    primary_modules = flow._unique_strings([item.get("primary_module") for item in matched_rules if item.get("primary_module")])
    if not metadata_only and len(primary_modules) > 1:
        tier = _bump_fast_path_tier(tier, "T2")
        reasons.append("multiple primary modules require shared validation planning")
    risk_levels = {str(item).lower() for item in ownership.get("risk_levels") or []}
    if "critical" in risk_levels:
        tier = _bump_fast_path_tier(tier, "T2")
        reasons.append("critical ownership risk requires T2")
    if module in {"paper_v2", "strategy_package", "research_assistant"} and changed_files:
        tier = _bump_fast_path_tier(tier, "T2")
        reasons.append(f"{module} is high-risk product scope; avoid T0 shortcut")
    if any(path.startswith("docs/architecture/") for path in changed_files):
        tier = _bump_fast_path_tier(tier, "T3")
        reasons.append("architecture/design documents require T3 design review context")
    return tier, flow._unique_strings(reasons)


def build_fast_path_plan(
    *,
    bug_id: str | None,
    issue_json: str | None,
    changed_files: list[str] | None,
    module: str | None = None,
    allow_missing_linkage: bool = False,
    allow_closed: bool = False,
) -> dict[str, Any]:
    changed = _normalize_changed_files(changed_files)
    record: dict[str, Any] | None = None
    source_path: Path | None = None
    canonical_bug_id: str | None = bug_id.strip().upper() if bug_id else None
    missing_linkage: list[str] = []
    status: str | None = None
    if bug_id or issue_json:
        record, source_path = find_bug_record(bug_id=bug_id, issue_json=issue_json)
        canonical_bug_id = str(record.get("bug_id") or canonical_bug_id or source_path.stem).upper()
        missing_linkage = _require_github_linkage(record, allow_missing=allow_missing_linkage)
        status = _require_fixable_status(record, allow_closed=allow_closed)
        module = str(record.get("module") or module or "").strip() or None

    validation = flow.select_validation(changed, module=module)
    ownership = validation.get("ownership") or flow.match_changed_files(changed)
    tier, reasons = _infer_fast_path_tier(
        record=record,
        changed_files=changed,
        validation=validation,
        ownership=ownership,
    )
    required = [str(item) for item in validation.get("required_plans") or []]
    recommended = [str(item) for item in validation.get("recommended_plans") or []]
    context_strategy = {
        "primary_sources": [
            "current BUG/GitHub issue body" if record else "explicit user request",
            "tmp/issue_workflow/<BUG>/context-pack.md when a BUG is linked",
            "changed-file ownership and test plan catalogs",
            "CodeGraph/Understand Anything refs only when available and relevant",
        ],
        "avoid_by_default": [
            "archived standards",
            "old design notes",
            "full module restart plans",
            "full logs unless triage requires the failing excerpt",
        ],
        "max_initial_files": 4 if tier in {"T0", "T1"} else 8,
    }
    if tier == "T0":
        context_strategy["goal"] = "metadata-only or docs/registry fast path; do not load module history"
    elif tier == "T1":
        context_strategy["goal"] = "single issue context pack plus targeted code snippets"
    elif tier == "T2":
        context_strategy["goal"] = "shared same-module or multi-impact context with selected validation"
    else:
        context_strategy["goal"] = "design/architecture review with broader acceptance evidence"

    stop_conditions = [
        "doctor reports workflow_gate=blocked",
        "required validation cannot run",
        "production runtime, production DB, or DDL action is needed without explicit user approval",
    ]
    if record:
        stop_conditions.extend(
            [
                "BUG JSON lacks github_issue_number or github_issue_url",
                "fix requires files outside allowed_write_scope",
            ]
        )

    if canonical_bug_id:
        next_command = f"python scripts/aistock_issue_workflow.py run --bug-id {canonical_bug_id} --mode plan --create-worktree"
    else:
        next_command = "python scripts/aistock_issue_workflow.py doctor"

    return {
        "schema_version": "aistock_issue_workflow_fast_path_v1",
        "generated_at": _utc_now(),
        "workflow_gate": "planned",
        "bug_id": canonical_bug_id,
        "module": module,
        "status": status,
        "source_bug_json": _repo_rel(source_path) if source_path else None,
        "missing_github_linkage": missing_linkage,
        "changed_files": changed,
        "file_categories": {path: _file_category(path) for path in changed},
        "task_tier": tier,
        "tier_reasons": reasons,
        "context_strategy": context_strategy,
        "validation": validation,
        "code_intelligence_hint": _code_intelligence_hint(REPO_ROOT),
        "required_validation": required,
        "recommended_validation": recommended,
        "required_commands": _plans_to_commands(required),
        "recommended_commands": _plans_to_commands(recommended),
        "production_gates": validation.get("production_gates") or {},
        "estimated_workflow_steps": _estimated_fast_path_steps(tier, has_bug=bool(record)),
        "stop_conditions": stop_conditions,
        "next_command": next_command,
    }


def _estimated_fast_path_steps(tier: str, *, has_bug: bool) -> list[str]:
    if tier == "T0":
        return [
            "doctor once",
            "targeted diff or metadata edit",
            "changed-file lint or l0 only when code/catalog changed",
            "commit and PR evidence",
        ]
    if tier == "T1":
        return [
            "doctor once",
            "run plan/create worktree and read compact Context Pack",
            "targeted fix within allowed_write_scope",
            "finish plan-only, selected validation, PR automation",
        ]
    if tier == "T2":
        return [
            "doctor once",
            "run-p0/start-batch when issues share module and validation",
            "shared context/code-intelligence refs",
            "selected module validation plus per-issue evidence",
        ]
    return [
        "doctor once",
        "design/architecture doc and acceptance matrix",
        "implementation with broader scope review",
        "full required validation and production gates",
    ]


def build_workflow_smoke_plan(
    *,
    bug_id: str | None = None,
    issue_json: str | None = None,
    changed_files: list[str] | None = None,
    module: str | None = None,
) -> dict[str, Any]:
    synthetic_record = False
    cleanup_paths: list[Path] = []
    if not bug_id and not issue_json:
        synthetic_record = True
        smoke_bug_id = "BUG-000"
        smoke_dir = REPO_ROOT / WORKFLOW_ROOT / "smoke"
        issue_path = smoke_dir / "synthetic-BUG-000.json"
        record = {
            "bug_id": smoke_bug_id,
            "title": "Workflow smoke synthetic issue",
            "module": module or "validation.guardrails",
            "severity": "P2",
            "status": "open",
            "description": "Synthetic issue used to dry-run the workflow state machine.",
            "reproduce_command": "python scripts/aistock_issue_workflow.py workflow-smoke",
            "allowed_write_scope": ["scripts/aistock_issue_workflow.py", "backend/tests/scripts/test_aistock_issue_workflow.py"],
            "required_verification": ["l0"],
            "github_issue_number": 999999,
            "github_issue_url": "https://github.com/licong01-cloud/AIstock/issues/999999",
        }
        _write_json(issue_path, record)
        cleanup_paths.append(issue_path)
        issue_json = str(issue_path)
        bug_id = smoke_bug_id

    changed = _normalize_changed_files(changed_files) or ["scripts/aistock_issue_workflow.py"]
    blocking: list[str] = []
    warnings: list[str] = []
    dirty_before = _git_status_paths(REPO_ROOT)
    doctor_payload: dict[str, Any] | None = None
    fast_path: dict[str, Any] | None = None
    start: dict[str, Any] | None = None
    finish: dict[str, Any] | None = None
    postmortem_preview: dict[str, Any] | None = None
    try:
        doctor_payload = build_doctor_report(skip_external=True)
        fast_path = build_fast_path_plan(
            bug_id=bug_id,
            issue_json=issue_json,
            changed_files=changed,
            module=module,
            allow_missing_linkage=True,
            allow_closed=True,
        )
        start = build_start_plan(
            bug_id=bug_id,
            issue_json=issue_json,
            changed_files=changed,
            create_worktree=True,
            dry_run=True,
            task_slug="workflow-smoke",
            allow_missing_linkage=True,
            allow_closed=True,
        )
        finish = build_finish_plan(
            bug_id=bug_id,
            issue_json=issue_json,
            changed_files=changed,
            base="origin/main",
            head="HEAD",
            validation_evidence=[],
            plan_only=True,
            allow_missing_evidence=False,
        )
        postmortem_preview = {
            "schema_version": "aistock_issue_workflow_smoke_postmortem_preview_v1",
            "bug_id": start["bug_id"],
            "timing_summary": _workflow_timing_summary(start["bug_id"], root=REPO_ROOT),
            "state_path": start.get("state_path"),
            "finish_plan_path": (finish or {}).get("artifact_metrics", {}).get("finish_plan", {}).get("path"),
            "stale_pr_check": "skipped_in_smoke_to_avoid_external_github_reads",
        }
    except Exception as exc:
        blocking.append(str(exc))
    dirty_after = _git_status_paths(REPO_ROOT)
    before_paths = {row["path"] for row in dirty_before}
    after_paths = {row["path"] for row in dirty_after}
    new_paths = sorted(after_paths - before_paths)
    allowed_prefixes = (
        f"{WORKFLOW_ROOT.as_posix()}/{(bug_id or 'BUG-000').upper()}",
        f"{WORKFLOW_ROOT.as_posix()}/smoke",
    )
    unexpected = [
        path
        for path in new_paths
        if not any(path == prefix or path.startswith(prefix + "/") for prefix in allowed_prefixes)
    ]
    if unexpected:
        blocking.append(f"workflow smoke created unexpected git-status paths: {unexpected}")
    if synthetic_record:
        warnings.append("used synthetic BUG-000 record under ignored tmp/issue_workflow/smoke")
    return {
        "schema_version": "aistock_issue_workflow_smoke_v1",
        "generated_at": _utc_now(),
        "workflow_gate": "passed" if not blocking else "blocked",
        "blocking": blocking,
        "warnings": warnings,
        "dry_run": True,
        "synthetic_record": synthetic_record,
        "changed_files": changed,
        "dirty_paths_before": dirty_before,
        "dirty_paths_after": dirty_after,
        "new_dirty_paths": new_paths,
        "unexpected_dirty_paths": unexpected,
        "client_manifest": (doctor_payload or {}).get("client_manifest"),
        "restart_recommended": (doctor_payload or {}).get("restart_recommended"),
        "h7_code_intelligence": (doctor_payload or {}).get("h7_code_intelligence"),
        "fast_path": fast_path,
        "start": start,
        "finish": finish,
        "postmortem_preview": postmortem_preview,
        "cleanup_paths": [_repo_rel(path) for path in cleanup_paths],
        "production_gates": (fast_path or {}).get("production_gates") or _production_gates_payload(),
        "next_command": f"python scripts/aistock_issue_workflow.py fast-path --bug-id {bug_id} --changed-file <path>"
        if bug_id
        else "python scripts/aistock_issue_workflow.py doctor",
    }


def _smoke_nightly_status_payload() -> dict[str, Any]:
    return {
        "statuses": {
            "runnerPreflight": "success",
            "drSnapshot": "success",
            "drValidate": "success",
            "nightlyL3": "failure",
            "paperV2Live": "skipped",
            "codeIntelligence": "success",
        },
        "run_id": "999999999",
        "run_url": "https://github.com/licong01-cloud/AIstock/actions/runs/999999999",
    }


def _smoke_batch_record(
    bug_id: str,
    issue_number: int,
    *,
    title: str,
) -> dict[str, Any]:
    return {
        "schema_version": "aistock_validation_bug_v1",
        "bug_id": bug_id,
        "title": title,
        "module": "validation.guardrails",
        "severity": "P1",
        "status": "open",
        "description": "Synthetic same-module batch workflow smoke record.",
        "actual": "Batch handoff needs a compact no-root-pollution smoke gate.",
        "expected": "Batch start/finish can produce per-issue context and closure evidence under ignored workflow artifacts.",
        "reproduce_command": "python scripts/aistock_issue_workflow.py batch-workflow-smoke",
        "allowed_write_scope": ["scripts/aistock_issue_workflow.py"],
        "required_verification": ["l0"],
        "closure_requirements": [
            "Generate per-issue Context Packs.",
            "Generate a batch finish plan and PR body.",
            "Keep synthetic artifacts ignored.",
        ],
        "non_goals": ["Do not touch production runtime services."],
        "github_issue_number": issue_number,
        "github_issue_url": f"https://github.example/issues/{issue_number}",
        "production_ddl_gate": "noop",
        "production_frontend_dependency_gate": "noop",
        "production_backend_dependency_gate": "noop",
    }


def build_batch_workflow_smoke_plan() -> dict[str, Any]:
    """Dry-run same-module batch start/finish without GitHub or tracked writes."""
    bug_ids = ["BUG-000", "BUG-001"]
    smoke_root = REPO_ROOT / WORKFLOW_ROOT / "batch-smoke"
    issue_dir = smoke_root / "synthetic-bugs"
    records = [
        _smoke_batch_record(bug_ids[0], 9000, title="Synthetic batch smoke one"),
        _smoke_batch_record(bug_ids[1], 9001, title="Synthetic batch smoke two"),
    ]
    issue_paths = [issue_dir / f"{record['bug_id']}.json" for record in records]
    blocking: list[str] = []
    warnings: list[str] = []
    dirty_before = _git_status_paths(REPO_ROOT)
    start_payload: dict[str, Any] | None = None
    finish_payload: dict[str, Any] | None = None

    try:
        for path, record in zip(issue_paths, records, strict=True):
            _write_json(path, record)
        start_payload = build_start_batch_plan(
            bug_ids=bug_ids,
            create_worktree=False,
            dry_run=False,
            task_slug="batch-smoke",
            allow_missing_linkage=False,
            allow_closed=False,
            record_pairs=list(zip(records, issue_paths, strict=True)),
        )
        finish_payload = build_finish_batch_plan(
            batch_id=str(start_payload["batch_id"]),
            bug_ids=[],
            changed_files=["scripts/aistock_issue_workflow.py"],
            base="origin/main",
            head="HEAD",
            validation_evidence=["batch-workflow-smoke synthetic validation -> passed"],
            issue_commit=["BUG-000=synthetic-shared-pr", "BUG-001=synthetic-shared-pr"],
            plan_only=False,
            allow_missing_evidence=False,
            record_pairs=list(zip(records, issue_paths, strict=True)),
        )
    except Exception as exc:
        blocking.append(str(exc))

    batch_id = str((start_payload or {}).get("batch_id") or "BATCH-smoke")
    output_dir = REPO_ROOT / WORKFLOW_ROOT / batch_id
    artifacts = {
        "batch_plan": output_dir / "batch-plan.json",
        "batch_state": output_dir / "batch-state.json",
        "finish_plan": output_dir / "finish-plan.json",
        "pr_body": output_dir / "pr-body.md",
    }
    for label, path in artifacts.items():
        if not path.exists():
            blocking.append(f"missing batch smoke artifact: {label}={_repo_rel(path)}")

    batch_state = _load_json(artifacts["batch_state"]) if artifacts["batch_state"].exists() else {}
    finish_plan = _load_json(artifacts["finish_plan"]) if artifacts["finish_plan"].exists() else {}
    pr_body = artifacts["pr_body"].read_text(encoding="utf-8", errors="replace") if artifacts["pr_body"].exists() else ""
    context_dir = Path(str(batch_state.get("context_dir") or ""))
    fix_ready_dir = Path(str(batch_state.get("fix_ready_dir") or ""))
    for bug_id in bug_ids:
        if context_dir and not (REPO_ROOT / context_dir / f"{bug_id}.md").exists():
            blocking.append(f"missing per-issue context markdown for {bug_id}")
        if fix_ready_dir and not (REPO_ROOT / fix_ready_dir / f"{bug_id}.json").exists():
            blocking.append(f"missing per-issue fix-ready JSON for {bug_id}")
        if bug_id not in str(finish_plan.get("per_issue_closure_map") or ""):
            blocking.append(f"finish plan missing per-issue closure map for {bug_id}")
        expected_issue = 9000 if bug_id == "BUG-000" else 9001
        if f"Closes #{expected_issue}" not in pr_body:
            blocking.append(f"batch PR body missing closing keyword for {bug_id}")

    if (finish_payload or {}).get("workflow_gate") != "ready_for_pr":
        blocking.append("batch finish smoke did not reach ready_for_pr")
    if (finish_payload or {}).get("scope_check", {}).get("status") != "passed":
        blocking.append("batch finish scope check did not pass")

    dirty_after = _git_status_paths(REPO_ROOT)
    before_paths = {row["path"] for row in dirty_before}
    new_paths = sorted({row["path"] for row in dirty_after} - before_paths)
    allowed_prefixes = (
        f"{WORKFLOW_ROOT.as_posix()}/batch-smoke",
        f"{WORKFLOW_ROOT.as_posix()}/{batch_id}",
    )
    unexpected = [
        path
        for path in new_paths
        if not any(path == prefix or path.startswith(prefix + "/") for prefix in allowed_prefixes)
    ]
    if unexpected:
        blocking.append(f"batch workflow smoke created unexpected git-status paths: {unexpected}")
    if not dirty_before and not dirty_after:
        warnings.append("git status stayed clean; tmp/issue_workflow batch smoke artifacts are ignored as expected")

    return {
        "schema_version": "aistock_batch_workflow_smoke_v1",
        "generated_at": _utc_now(),
        "workflow_gate": "passed" if not blocking else "blocked",
        "blocking": blocking,
        "warnings": warnings,
        "dry_run": True,
        "github_writes": False,
        "bug_ids": bug_ids,
        "batch_id": batch_id,
        "artifacts": {key: _repo_rel(path) for key, path in artifacts.items()},
        "start": start_payload,
        "finish": finish_payload,
        "dirty_paths_before": dirty_before,
        "dirty_paths_after": dirty_after,
        "new_dirty_paths": new_paths,
        "unexpected_dirty_paths": unexpected,
        "production_gates": _production_gates_payload(),
        "next_command": "python scripts/aistock_issue_workflow.py start-batch --bug-id BUG-XXX --bug-id BUG-YYY --create-worktree",
    }


def _path_under_repo_tmp_validation(path: Path) -> bool:
    try:
        rel = path.resolve().relative_to(REPO_ROOT.resolve()).as_posix()
    except ValueError:
        return False
    return rel.startswith("tmp/validation/")


def build_nightly_intake_smoke_plan() -> dict[str, Any]:
    """Dry-run Nightly failure intake artifacts without GitHub or tracked writes."""
    smoke_root = REPO_ROOT / "tmp" / "validation" / "nightly_failure_issue" / "smoke"
    status_path = smoke_root / "status.json"
    summary_path = smoke_root / "summary.json"
    markdown_path = smoke_root / "body.md"
    context_path = smoke_root / "context-pack.json"
    context_md_path = smoke_root / "context-pack.md"
    issue_payload_path = smoke_root / "github-issue-payload.json"
    artifacts = {
        "status": status_path,
        "summary": summary_path,
        "markdown": markdown_path,
        "context": context_path,
        "context_markdown": context_md_path,
        "github_issue_payload": issue_payload_path,
    }
    blocking: list[str] = []
    warnings: list[str] = []
    dirty_before = _git_status_paths(REPO_ROOT)
    stdout_payload: dict[str, Any] = {}
    try:
        _write_json(status_path, _smoke_nightly_status_payload())
        stdout_buffer = io.StringIO()
        with contextlib.redirect_stdout(stdout_buffer):
            exit_code = ci_failure_summary.main(
                [
                    "--nightly-status-json",
                    str(status_path),
                    "--run-id",
                    "999999999",
                    "--run-url",
                    "https://github.com/licong01-cloud/AIstock/actions/runs/999999999",
                    "--source-name",
                    "AIstock Nightly",
                    "--output",
                    str(summary_path),
                    "--markdown-output",
                    str(markdown_path),
                    "--context-output",
                    str(context_path),
                    "--context-markdown-output",
                    str(context_md_path),
                    "--github-issue-payload-output",
                    str(issue_payload_path),
                    "--stdout-format",
                    "compact",
                ]
            )
        if exit_code != 0:
            blocking.append(f"Nightly intake summary command exited with {exit_code}")
        stdout_payload = json.loads(stdout_buffer.getvalue() or "{}")
    except Exception as exc:
        blocking.append(str(exc))

    for label, path in artifacts.items():
        if not path.exists():
            blocking.append(f"missing Nightly intake artifact: {label}={_repo_rel(path)}")
        if not _path_under_repo_tmp_validation(path):
            blocking.append(f"Nightly intake artifact is outside tmp/validation: {label}={path}")

    candidate_history = stdout_payload.get("artifacts", {}).get("candidate_history") if isinstance(stdout_payload, dict) else None
    candidate_history_path = Path(candidate_history) if candidate_history else None
    if not candidate_history_path:
        blocking.append("Nightly intake smoke did not persist compact candidate history")
    elif "tests/aistock_validation/history" in candidate_history_path.as_posix():
        blocking.append(f"candidate history used tracked history path: {candidate_history_path}")
    elif not _path_under_repo_tmp_validation(candidate_history_path):
        blocking.append(f"candidate history is outside tmp/validation: {candidate_history_path}")

    context_pack = _load_json(context_path) if context_path.exists() else {}
    issue_payload = _load_json(issue_payload_path) if issue_payload_path.exists() else {}
    handoff = context_pack.get("agent_handoff") if isinstance(context_pack.get("agent_handoff"), dict) else {}
    entrypoints = handoff.get("workflow_entrypoints") if isinstance(handoff.get("workflow_entrypoints"), dict) else {}
    body = str(issue_payload.get("body") or "")
    closed_loop_checks = {
        "agent_handoff_section": "## Agent Handoff" in body,
        "triage_entrypoint": "triage-ci-issue" in str(entrypoints.get("triage") or ""),
        "promotion_requires_registry_worktree": "--create-registry-worktree --apply" in str(entrypoints.get("promote") or ""),
        "needs_bug_json_recorded": handoff.get("needs_bug_json") is True,
        "candidate_history_tmp_only": bool(candidate_history_path and _path_under_repo_tmp_validation(candidate_history_path)),
    }
    for label, ok in closed_loop_checks.items():
        if not ok:
            blocking.append(f"Nightly intake closed-loop check failed: {label}")
    if "triage-ci-issue --issue <issue-number>" not in body:
        blocking.append("GitHub Issue payload is missing triage-ci-issue handoff")
    if "promote-ci-issue --issue <issue-number> --create-registry-worktree --apply" not in body:
        blocking.append("GitHub Issue payload is missing promote-ci-issue registry worktree handoff")
    if "triage-ci-issue" not in str(entrypoints.get("triage") or ""):
        blocking.append("Context Pack is missing triage workflow entrypoint")
    if "promote-ci-issue" not in str(entrypoints.get("promote") or ""):
        blocking.append("Context Pack is missing promote workflow entrypoint")

    dirty_after = _git_status_paths(REPO_ROOT)
    before_paths = {row["path"] for row in dirty_before}
    new_paths = sorted({row["path"] for row in dirty_after} - before_paths)
    allowed_prefix = "tmp/validation/nightly_failure_issue/smoke"
    unexpected = [
        path for path in new_paths if not (path == allowed_prefix or path.startswith(allowed_prefix + "/"))
    ]
    if unexpected:
        blocking.append(f"Nightly intake smoke created unexpected git-status paths: {unexpected}")
    if not dirty_before and not dirty_after:
        warnings.append("git status stayed clean; tmp/validation artifacts are ignored as expected")

    return {
        "schema_version": "aistock_nightly_intake_smoke_v1",
        "generated_at": _utc_now(),
        "workflow_gate": "passed" if not blocking else "blocked",
        "blocking": blocking,
        "warnings": warnings,
        "dry_run": True,
        "github_writes": False,
        "production_gates": _production_gates_payload(),
        "artifacts": {key: _repo_rel(path) for key, path in artifacts.items()},
        "candidate_history_path": _repo_rel(candidate_history_path) if candidate_history_path else None,
        "issue_title": issue_payload.get("title"),
        "nightly_failed_stages": stdout_payload.get("nightly_failed_stages") if isinstance(stdout_payload, dict) else [],
        "closed_loop_checks": closed_loop_checks,
        "handoff_entrypoints": entrypoints,
        "dirty_paths_before": dirty_before,
        "dirty_paths_after": dirty_after,
        "new_dirty_paths": new_paths,
        "unexpected_dirty_paths": unexpected,
        "next_command": "python scripts/aistock_issue_workflow.py triage-ci-issue --issue <created-nightly-issue-number>",
    }


def build_start_plan(
    *,
    bug_id: str | None,
    issue_json: str | None,
    changed_files: list[str],
    create_worktree: bool,
    dry_run: bool,
    task_slug: str | None,
    allow_missing_linkage: bool,
    allow_closed: bool,
    active_decision: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not issue_json and active_decision and active_decision.get("registry_issue_json"):
        issue_json = str(active_decision["registry_issue_json"])
    record, source_path = find_bug_record(bug_id=bug_id, issue_json=issue_json)
    canonical_bug_id = str(record.get("bug_id") or bug_id or source_path.stem).upper()
    missing_linkage = _require_github_linkage(record, allow_missing=allow_missing_linkage)
    status = _require_fixable_status(record, allow_closed=allow_closed)
    worktree_plan = _maybe_create_worktree(
        record=record,
        bug_id=canonical_bug_id,
        source_bug_json=source_path,
        create=create_worktree,
        dry_run=dry_run,
        task_slug=task_slug,
    )
    actual_worktree, planned_worktree = _actual_and_planned_worktree(worktree_plan)
    actual_branch = worktree_plan.get("branch") if actual_worktree else None
    planned_branch = worktree_plan.get("branch") if planned_worktree else None
    target_root = Path(actual_worktree) if actual_worktree else REPO_ROOT
    target_bug_path = _issue_json_path_for_worktree(source_path, target_root)
    output_dir = target_root / WORKFLOW_ROOT / canonical_bug_id
    fix_ready = flow.build_fix_ready(record, changed_files)
    context_pack = flow.build_context_pack(record, changed_files)
    code_intelligence_summary = _build_code_intelligence_summary(
        item_id=canonical_bug_id,
        record=record,
        changed_files=changed_files,
        root=target_root,
    )
    fast_path = build_fast_path_plan(
        bug_id=canonical_bug_id,
        issue_json=str(source_path),
        changed_files=changed_files,
        module=record.get("module"),
        allow_missing_linkage=True,
        allow_closed=True,
    )
    context_pack["code_intelligence"] = {
        "provider": code_intelligence_summary.get("provider"),
        "status": code_intelligence_summary.get("status"),
        "context_ref": code_intelligence_summary.get("context_ref"),
        "manifest_ref": code_intelligence_summary.get("manifest_ref"),
        "affected_tests_ref": code_intelligence_summary.get("affected_tests_ref"),
        "fallback_used": code_intelligence_summary.get("fallback_used"),
        "affected_tests_count": code_intelligence_summary.get("affected_tests_count"),
        "affected_quality": code_intelligence_summary.get("affected_quality"),
        "understand_anything_summary_ref": code_intelligence_summary.get("understand_anything_summary_ref"),
        "understand_anything_summary": code_intelligence_summary.get("understand_anything_summary"),
        "understand_anything": code_intelligence_summary.get("understand_anything"),
    }
    fix_ready["code_intelligence"] = context_pack["code_intelligence"]
    fix_ready_path = output_dir / "fix-ready.json"
    context_json_path = output_dir / "context-pack.json"
    context_md_path = output_dir / "context-pack.md"
    task_card_json_path = _task_card_json_path(canonical_bug_id, target_root)
    task_card_md_path = _task_card_md_path(canonical_bug_id, target_root)
    task_card = build_task_card(
        bug_id=canonical_bug_id,
        record=record,
        root=target_root,
        branch=actual_branch,
        planned_branch=planned_branch,
        worktree=actual_worktree,
        planned_worktree=planned_worktree,
        context_pack_json_path=context_json_path,
        context_pack_md_path=context_md_path,
        fix_ready_path=fix_ready_path,
        state_path=_state_path(canonical_bug_id, target_root),
        events_path=_events_path(canonical_bug_id, target_root),
        fix_ready=fix_ready,
        context_pack=context_pack,
        code_intelligence_summary=code_intelligence_summary,
        fast_path=fast_path,
    )
    if not dry_run:
        _write_json(fix_ready_path, fix_ready)
        _write_json(context_json_path, context_pack)
        _write_text(context_md_path, flow.render_context_pack_markdown(context_pack))
        _write_json(task_card_json_path, task_card)
        _write_text(task_card_md_path, render_task_card_markdown(task_card))
        context_metrics = {
            "context_pack_md": _size_and_token_estimate(context_md_path),
            "context_pack_json": _size_and_token_estimate(context_json_path),
            "fix_ready_json": _size_and_token_estimate(fix_ready_path),
            "task_card_md": _size_and_token_estimate(task_card_md_path),
            "task_card_json": _size_and_token_estimate(task_card_json_path),
        }
        _write_state(
            canonical_bug_id,
            state="context_ready",
            root=target_root,
            branch=actual_branch,
            planned_branch=planned_branch,
            workflow_role="fix",
            worktree=actual_worktree,
            planned_worktree=planned_worktree,
            base=worktree_plan.get("base"),
            source_bug_json=_repo_rel(source_path),
            target_bug_json=_repo_rel(target_bug_path, target_root),
            context_pack_md=_repo_rel(context_md_path, target_root),
            context_pack_json=_repo_rel(context_json_path, target_root),
            fix_ready_path=_repo_rel(fix_ready_path, target_root),
            task_card_json=_repo_rel(task_card_json_path, target_root),
            task_card_md=_repo_rel(task_card_md_path, target_root),
            github_issue_number=record.get("github_issue_number"),
            github_issue_url=record.get("github_issue_url"),
            production_gates=fix_ready.get("validation_selection", {}).get("production_gates", {}),
            code_intelligence=context_pack.get("code_intelligence"),
            fast_path=fast_path,
            active_decision=active_decision,
            context_metrics=context_metrics,
            next_actions=[
                "switch_to_worktree_if_created",
                "read_context_pack_md",
                "fix_only_within_allowed_write_scope_or_stop_for_scope_expansion",
                "run_finish_plan_before_reporting_done",
            ],
        )
    else:
        context_metrics = {
            "context_pack_md": {"path": str(context_md_path), "exists": False, "bytes": 0, "estimated_tokens": 0},
            "context_pack_json": {"path": str(context_json_path), "exists": False, "bytes": 0, "estimated_tokens": 0},
            "fix_ready_json": {"path": str(fix_ready_path), "exists": False, "bytes": 0, "estimated_tokens": 0},
            "task_card_md": {"path": str(task_card_md_path), "exists": False, "bytes": 0, "estimated_tokens": 0},
            "task_card_json": {"path": str(task_card_json_path), "exists": False, "bytes": 0, "estimated_tokens": 0},
        }
    return {
        "schema_version": "aistock_issue_workflow_start_v1",
        "generated_at": _utc_now(),
        "bug_id": canonical_bug_id,
        "status": status,
        "module": record.get("module"),
        "source_bug_json": _repo_rel(source_path),
        "target_bug_json": _repo_rel(target_bug_path, target_root),
        "github_issue_number": record.get("github_issue_number"),
        "github_issue_url": record.get("github_issue_url"),
        "missing_github_linkage": missing_linkage,
        "worktree_plan": worktree_plan,
        "fix_ready_path": _repo_rel(fix_ready_path, target_root),
        "context_pack_json": _repo_rel(context_json_path, target_root),
        "context_pack_md": _repo_rel(context_md_path, target_root),
        "task_card_json": _repo_rel(task_card_json_path, target_root),
        "task_card_md": _repo_rel(task_card_md_path, target_root),
        "state_path": _repo_rel(_state_path(canonical_bug_id, target_root), target_root),
        "events_path": _repo_rel(_events_path(canonical_bug_id, target_root), target_root),
        "allowed_write_scope": fix_ready.get("allowed_write_scope") or [],
        "required_verification": fix_ready.get("required_verification") or [],
        "recommended_verification": fix_ready.get("recommended_verification") or [],
        "production_gates": fix_ready.get("validation_selection", {}).get("production_gates", {}),
        "code_intelligence": context_pack.get("code_intelligence"),
        "fast_path": fast_path,
        "active_decision": active_decision,
        "context_metrics": context_metrics,
        "workflow_efficiency_recommendations": record.get("workflow_efficiency_recommendations") or _workflow_efficiency_recommendations(record, record.get("ui_intake_hints") if isinstance(record.get("ui_intake_hints"), dict) else None),
        "ui_intake_hints": record.get("ui_intake_hints"),
        "next_agent_steps": [
            "switch_to_worktree_if_created",
            "read_context_pack_md",
            "fix_only_within_allowed_write_scope_or_stop_for_scope_expansion",
            "use_compact_success_output_and_full_json_only_on_failure",
            "run_finish_plan_before_reporting_done",
        ],
    }


def build_finish_plan(
    *,
    bug_id: str | None,
    issue_json: str | None,
    changed_files: list[str] | None,
    base: str,
    head: str,
    validation_evidence: list[str],
    plan_only: bool,
    allow_missing_evidence: bool,
) -> dict[str, Any]:
    record, source_path = find_bug_record(bug_id=bug_id, issue_json=issue_json)
    canonical_bug_id = str(record.get("bug_id") or bug_id or source_path.stem).upper()
    changed = changed_files if changed_files is not None else flow.changed_files_from_git(base, head)
    validation = flow.select_validation(changed, module=record.get("module"))
    pr_quality = flow.build_pr_quality(base=base, head=head, issue_record=record, changed_files=changed)
    code_intelligence_summary = _build_code_intelligence_summary(
        item_id=canonical_bug_id,
        record=record,
        changed_files=changed,
        root=REPO_ROOT,
    )
    h7_code_intelligence = _code_intelligence_readiness(code_intelligence_summary)
    codegraph_tests = code_intelligence_summary.get("affected_tests", {}).get("suggested_tests") or []
    if codegraph_tests:
        validation["codegraph_suggested_tests"] = codegraph_tests
    evidence = [item for item in validation_evidence if item.strip()]
    closure_ready = bool(evidence) or plan_only or allow_missing_evidence
    output_dir = REPO_ROOT / WORKFLOW_ROOT / canonical_bug_id
    pr_body_path = output_dir / "pr-body.md"
    pr_body = render_pr_body(canonical_bug_id, record, changed, validation, pr_quality, evidence, closure_ready)
    _write_json(output_dir / "finish-plan.json", {
        "bug_id": canonical_bug_id,
        "changed_files": changed,
        "selected_validation": validation,
        "pr_quality": pr_quality,
        "validation_evidence": evidence,
        "closure_ready": closure_ready,
        "code_intelligence": code_intelligence_summary,
        "h7_code_intelligence": h7_code_intelligence,
    })
    _write_text(pr_body_path, pr_body)
    next_state = "validation_passed" if evidence else ("validation_planned" if plan_only else "blocked")
    _write_state(
        canonical_bug_id,
        state=next_state,
        changed_files=changed,
        validation_evidence=evidence,
        pr_body_path=_repo_rel(pr_body_path),
        production_gates=validation.get("production_gates") or {},
        code_intelligence={
            "status": code_intelligence_summary.get("status"),
            "context_ref": code_intelligence_summary.get("context_ref"),
            "affected_tests_ref": code_intelligence_summary.get("affected_tests_ref"),
            "fallback_used": code_intelligence_summary.get("fallback_used"),
            "affected_tests_count": code_intelligence_summary.get("affected_tests_count"),
            "understand_anything_summary_ref": code_intelligence_summary.get("understand_anything_summary_ref"),
            "readiness_next_command": h7_code_intelligence.get("readiness_next_command"),
            "fallback_reason": h7_code_intelligence.get("fallback_reason"),
        },
        stop_reason=None if closure_ready else "validation_evidence_missing",
        next_actions=[
            "commit_only_task_files",
            "push_task_branch",
            "create_pr_from_pr_body",
            "watch_ci_before_merge",
        ] if evidence else ["run_required_validation", "rerun_finish_with_validation_evidence"],
    )
    payload = {
        "schema_version": "aistock_issue_workflow_finish_v1",
        "generated_at": _utc_now(),
        "bug_id": canonical_bug_id,
        "source_bug_json": _repo_rel(source_path),
        "changed_files": changed,
        "fast_path": build_fast_path_plan(
            bug_id=canonical_bug_id,
            issue_json=str(source_path),
            changed_files=changed,
            module=record.get("module"),
            allow_missing_linkage=True,
            allow_closed=True,
        ),
        "required_verification": validation.get("required_plans") or [],
        "recommended_verification": validation.get("recommended_plans") or [],
        "production_gates": validation.get("production_gates") or {},
        "scope_check": pr_quality.get("scope_check"),
        "code_intelligence": {
            "status": code_intelligence_summary.get("status"),
            "context_ref": code_intelligence_summary.get("context_ref"),
            "manifest_ref": code_intelligence_summary.get("manifest_ref"),
            "affected_tests_ref": code_intelligence_summary.get("affected_tests_ref"),
            "fallback_used": code_intelligence_summary.get("fallback_used"),
            "affected_tests_count": code_intelligence_summary.get("affected_tests_count"),
            "affected_quality": code_intelligence_summary.get("affected_quality"),
            "understand_anything_summary_ref": code_intelligence_summary.get("understand_anything_summary_ref"),
        },
        "codegraph_suggested_tests": codegraph_tests,
        "h7_code_intelligence": h7_code_intelligence,
        "validation_evidence": evidence,
        "closure_ready": closure_ready,
        "workflow_gate": "ready_for_pr" if closure_ready else "validation_evidence_missing",
        "pr_body_path": _repo_rel(pr_body_path),
        "state_path": _repo_rel(_state_path(canonical_bug_id)),
        "events_path": _repo_rel(_events_path(canonical_bug_id)),
        "artifact_metrics": {
            "pr_body": _size_and_token_estimate(pr_body_path),
            "finish_plan": _size_and_token_estimate(output_dir / "finish-plan.json"),
        },
    }
    payload["pre_pr_gate"] = _pre_pr_gate(finish=payload, validation_evidence=evidence, root=REPO_ROOT, run_lint=False)
    if not closure_ready:
        payload["error"] = "validation evidence is required unless --plan-only or --allow-missing-evidence is used"
    return payload


def render_pr_body(
    bug_id: str,
    record: dict[str, Any],
    changed_files: list[str],
    validation: dict[str, Any],
    pr_quality: dict[str, Any],
    evidence: list[str],
    closure_ready: bool,
) -> str:
    gates = validation.get("production_gates") or {}
    code_intel = validation.get("h7_code_intelligence") or {}
    lines = [
        f"## {bug_id} issue workflow summary",
        "",
        f"- GitHub Issue: {record.get('github_issue_url') or 'missing'}",
        f"- Module: `{record.get('module') or 'unknown'}`",
        f"- Scope check: `{(pr_quality.get('scope_check') or {}).get('status')}`",
        f"- Closure ready: `{str(closure_ready).lower()}`",
        "",
        "## Changed files",
        *[f"- `{path}`" for path in changed_files or ["none"]],
        "",
        "## Required validation",
        *[f"- `{plan}`" for plan in validation.get("required_plans") or ["l0"]],
        "",
        "## Code intelligence",
        *[f"- CodeGraph suggested test: `{path}`" for path in validation.get("codegraph_suggested_tests") or ["none"]],
        f"- H7 readiness: `{code_intel.get('workflow_gate') or 'unknown'}`",
        f"- fallback_used: `{str(bool(code_intel.get('fallback_used'))).lower()}`",
        f"- readiness_next_command: `{code_intel.get('readiness_next_command') or 'not_required'}`",
        "",
        "## Evidence",
        *[f"- {item}" for item in evidence or ["missing - run required validation before requesting merge"]],
        "",
        "## Production gates",
        f"- production_ddl_gate: `{gates.get('ddl', 'noop')}`",
        f"- production_frontend_dependency_gate: `{gates.get('frontend_dependency', 'noop')}`",
        f"- production_backend_dependency_gate: `{gates.get('backend_dependency', 'noop')}`",
        "",
        f"Closes #{record.get('github_issue_number')}" if record.get("github_issue_number") else "",
    ]
    return "\n".join(line for line in lines if line is not None)


def build_triage_p0(*, include_fixed: bool = False) -> dict[str, Any]:
    accepted_statuses = set(ALLOWED_FIX_STATUSES)
    if include_fixed:
        accepted_statuses.update({"fixed", "verified"})
    items: list[dict[str, Any]] = []
    groups: dict[tuple[str, tuple[str, ...]], list[str]] = {}
    for path in _bug_files():
        record = _load_json(path)
        severity = str(record.get("severity") or "").upper().split()[0]
        status = str(record.get("status") or "")
        if severity != "P0" or status not in accepted_statuses:
            continue
        bug_id = str(record.get("bug_id") or path.stem)
        required = tuple(flow._unique_strings(flow._as_list(record.get("required_verification"))))
        key = (str(record.get("module") or "unknown"), required)
        groups.setdefault(key, []).append(bug_id)
        missing = [field for field in ("github_issue_number", "github_issue_url") if not record.get(field)]
        items.append(
            {
                "bug_id": bug_id,
                "title": record.get("title"),
                "status": status,
                "module": record.get("module"),
                "github_issue_number": record.get("github_issue_number"),
                "github_issue_url": record.get("github_issue_url"),
                "missing_github_linkage": missing,
                "required_verification": list(required),
                "allowed_write_scope": flow._as_list(record.get("allowed_write_scope")),
                "source_bug_json": _repo_rel(path),
            }
        )
    group_payload = []
    for (module, required), bug_ids in sorted(groups.items(), key=lambda item: (item[0][0], item[0][1], item[1])):
        group_payload.append(
            {
                "module": module,
                "required_verification": list(required),
                "bug_ids": bug_ids,
                "can_batch": len(bug_ids) > 1,
                "suggested_branch": f"bug/p0-{_slug(module)}-batch-{_today_compact()}" if len(bug_ids) > 1 else f"bug/{bug_ids[0]}-{_slug(module)}-{_today_compact()}",
            }
        )
    return {
        "schema_version": "aistock_issue_workflow_triage_p0_v1",
        "generated_at": _utc_now(),
        "count": len(items),
        "items": sorted(items, key=lambda item: (str(item.get("module")), str(item.get("bug_id")))),
        "groups": group_payload,
    }


def build_run_p0_plan(*, module: str | None = None, include_fixed: bool = False) -> dict[str, Any]:
    triage = build_triage_p0(include_fixed=include_fixed)
    items = triage["items"]
    groups = triage["groups"]
    if module:
        items = [item for item in items if str(item.get("module") or "") == module]
        groups = [group for group in groups if str(group.get("module") or "") == module]
    recommended = items[0]["bug_id"] if items else None
    return {
        "schema_version": "aistock_issue_workflow_run_p0_v1",
        "generated_at": _utc_now(),
        "module": module,
        "count": len(items),
        "items": items,
        "groups": groups,
        "recommended_first_issue": recommended,
        "next_command": f"python scripts/aistock_issue_workflow.py run --bug-id {recommended} --mode plan --create-worktree"
        if recommended
        else None,
    }


def _records_for_bug_ids(
    bug_ids: list[str],
    *,
    allow_missing_linkage: bool = False,
    allow_closed: bool = False,
) -> list[tuple[dict[str, Any], Path]]:
    normalized = [bug_id.strip().upper() for bug_id in bug_ids if bug_id.strip()]
    if len(normalized) != len(set(normalized)):
        raise WorkflowError(f"duplicate BUG ids in batch: {bug_ids}")
    if len(normalized) < 2:
        raise WorkflowError("batch workflow requires at least two BUG ids")
    records: list[tuple[dict[str, Any], Path]] = []
    for bug_id in normalized:
        record, path = find_bug_record(bug_id=bug_id)
        _require_github_linkage(record, allow_missing=allow_missing_linkage)
        _require_fixable_status(record, allow_closed=allow_closed)
        records.append((record, path))
    return records


def _batch_signature(records: list[dict[str, Any]]) -> dict[str, Any]:
    modules = sorted({str(record.get("module") or "unknown") for record in records})
    if len(modules) != 1:
        raise WorkflowError(f"batch issues must share one module; got {modules}")
    risks = sorted({flow._risk_from_severity(str(record.get("severity") or "P2")) for record in records})
    if len(risks) != 1:
        raise WorkflowError(f"batch issues must share one risk tier; got {risks}")
    verification_signatures = {
        tuple(flow._unique_strings(flow._as_list(record.get("required_verification"))))
        for record in records
    }
    if len(verification_signatures) != 1:
        raise WorkflowError("batch issues must share the same required_verification signature")
    selector = _batch_validation_selector(records, module=modules[0])
    if selector["workflow_gate"] != "compatible":
        raise WorkflowError("; ".join(selector["blocking"]))
    bug_ids = [str(record.get("bug_id")) for record in records]
    batch_id = f"BATCH-{_slug(modules[0], max_len=32)}-{_today_compact()}-{_short_hash(*bug_ids)}"
    return {
        "batch_id": batch_id,
        "module": modules[0],
        "risk_tier": risks[0],
        "bug_ids": bug_ids,
        "required_verification": list(next(iter(verification_signatures))),
        "batch_selector": selector,
    }


def build_start_batch_plan(
    *,
    bug_ids: list[str],
    create_worktree: bool,
    dry_run: bool,
    task_slug: str | None,
    allow_missing_linkage: bool,
    allow_closed: bool,
    record_pairs: list[tuple[dict[str, Any], Path]] | None = None,
) -> dict[str, Any]:
    if record_pairs is None:
        record_pairs = _records_for_bug_ids(
            bug_ids,
            allow_missing_linkage=allow_missing_linkage,
            allow_closed=allow_closed,
        )
    records = [record for record, _ in record_pairs]
    signature = _batch_signature(records)
    batch_plan = flow.build_batch_plan(records)
    batch_plan["batch_id"] = signature["batch_id"]
    batch_plan["batch_selector"] = signature["batch_selector"]
    batch_plan["shared_files"] = signature["batch_selector"]["shared_files"]
    batch_plan["shared_validation"] = signature["batch_selector"]["required_plans"]
    branch, worktree = _batch_target_names(signature["batch_id"], signature["module"], task_slug)
    worktree_plan = _maybe_create_named_worktree(
        branch=branch,
        worktree=worktree,
        create=create_worktree,
        dry_run=dry_run,
    )
    target_root = Path(worktree_plan["worktree"]) if create_worktree and not dry_run else REPO_ROOT
    output_dir = target_root / WORKFLOW_ROOT / signature["batch_id"]
    context_dir = output_dir / "context-packs"
    fix_ready_dir = output_dir / "fix-ready"
    code_intelligence_summary = _build_batch_code_intelligence_summary(
        batch_id=signature["batch_id"],
        records=records,
        changed_files=batch_plan.get("shared_files") or [],
        root=target_root,
    )
    batch_code_intelligence = {
        "provider": code_intelligence_summary.get("provider"),
        "status": code_intelligence_summary.get("status"),
        "context_ref": code_intelligence_summary.get("context_ref"),
        "manifest_ref": code_intelligence_summary.get("manifest_ref"),
        "affected_tests_ref": code_intelligence_summary.get("affected_tests_ref"),
        "fallback_used": code_intelligence_summary.get("fallback_used"),
        "affected_tests_count": code_intelligence_summary.get("affected_tests_count"),
        "affected_quality": code_intelligence_summary.get("affected_quality"),
        "understand_anything_summary_ref": code_intelligence_summary.get("understand_anything_summary_ref"),
        "understand_anything_summary": code_intelligence_summary.get("understand_anything_summary"),
        "understand_anything": code_intelligence_summary.get("understand_anything"),
    }
    batch_plan["code_intelligence"] = batch_code_intelligence
    context_metrics: dict[str, Any] = {}
    if not dry_run:
        for record, source_path in record_pairs:
            bug_id = str(record.get("bug_id"))
            context_pack = flow.build_context_pack(record, [])
            fix_ready = flow.build_fix_ready(record, [])
            context_pack["code_intelligence"] = batch_code_intelligence
            fix_ready["code_intelligence"] = batch_code_intelligence
            _write_json(context_dir / f"{bug_id}.json", context_pack)
            _write_text(context_dir / f"{bug_id}.md", flow.render_context_pack_markdown(context_pack))
            _write_json(fix_ready_dir / f"{bug_id}.json", fix_ready)
            context_metrics[bug_id] = {
                "context_md": _size_and_token_estimate(context_dir / f"{bug_id}.md"),
                "context_json": _size_and_token_estimate(context_dir / f"{bug_id}.json"),
                "fix_ready_json": _size_and_token_estimate(fix_ready_dir / f"{bug_id}.json"),
            }
            target_bug_path = _issue_json_path_for_worktree(source_path, target_root)
            batch_plan.setdefault("source_bug_json", {})[bug_id] = _repo_rel(source_path)
            batch_plan.setdefault("target_bug_json", {})[bug_id] = _repo_rel(target_bug_path, target_root)
        _write_json(output_dir / "batch-plan.json", batch_plan)
        _write_json(output_dir / "batch-state.json", {
            **signature,
            "state": "context_ready",
            "branch": worktree_plan.get("branch"),
            "worktree": worktree_plan.get("worktree"),
            "batch_plan_path": _repo_rel(output_dir / "batch-plan.json", target_root),
            "context_dir": _repo_rel(context_dir, target_root),
            "fix_ready_dir": _repo_rel(fix_ready_dir, target_root),
            "context_metrics": context_metrics,
            "batch_selector": signature["batch_selector"],
            "updated_at": _utc_now(),
        })
        _write_state(
            signature["batch_id"],
            state="context_ready",
            root=target_root,
            branch=worktree_plan.get("branch"),
            worktree=worktree_plan.get("worktree"),
            base=worktree_plan.get("base"),
            batch_id=signature["batch_id"],
            bug_ids=signature["bug_ids"],
            module=signature["module"],
            risk_tier=signature["risk_tier"],
            batch_plan_path=_repo_rel(output_dir / "batch-plan.json", target_root),
            batch_state_path=_repo_rel(output_dir / "batch-state.json", target_root),
            context_dir=_repo_rel(context_dir, target_root),
            fix_ready_dir=_repo_rel(fix_ready_dir, target_root),
            code_intelligence=batch_code_intelligence,
            batch_selector=signature["batch_selector"],
            context_metrics=context_metrics,
            next_actions=[
                "switch_to_batch_worktree_if_created",
                "read_each_context_pack",
                "fix_only_within_shared_scope_or_stop_for_scope_expansion",
                "run_finish_batch_before_reporting_done",
            ],
        )
    return {
        "schema_version": "aistock_issue_workflow_start_batch_v1",
        "generated_at": _utc_now(),
        **signature,
        "worktree_plan": worktree_plan,
        "batch_plan": batch_plan,
        "batch_plan_path": _repo_rel(output_dir / "batch-plan.json", target_root),
        "batch_state_path": _repo_rel(output_dir / "batch-state.json", target_root),
        "context_dir": _repo_rel(context_dir, target_root),
        "fix_ready_dir": _repo_rel(fix_ready_dir, target_root),
        "state_path": _repo_rel(_state_path(signature["batch_id"], target_root), target_root),
        "events_path": _repo_rel(_events_path(signature["batch_id"], target_root), target_root),
        "code_intelligence": batch_code_intelligence,
        "batch_selector": signature["batch_selector"],
        "context_metrics": context_metrics,
        "workflow_gate": "ready_for_batch_fix",
        "next_command": f"python scripts/aistock_issue_workflow.py finish-batch --batch-id {signature['batch_id']} --plan-only",
    }


def _load_batch_state(batch_id: str) -> dict[str, Any] | None:
    path = REPO_ROOT / WORKFLOW_ROOT / batch_id / "batch-state.json"
    if path.exists():
        return _load_json(path)
    state = _load_state(batch_id)
    return state


def _parse_issue_commit_map(entries: list[str] | None) -> dict[str, str]:
    result: dict[str, str] = {}
    for entry in entries or []:
        if "=" not in entry:
            raise WorkflowError(f"--issue-commit must use BUG-XXX=<commit>: {entry}")
        issue, commit = entry.split("=", 1)
        issue = issue.strip().upper()
        commit = commit.strip()
        if not issue or not commit:
            raise WorkflowError(f"--issue-commit must use BUG-XXX=<commit>: {entry}")
        result[issue] = commit
    return result


def render_batch_pr_body(
    batch_id: str,
    records: list[dict[str, Any]],
    changed_files: list[str],
    validation: dict[str, Any],
    evidence: list[str],
    commit_map: dict[str, str],
    code_intelligence_summary: dict[str, Any],
    closure_ready: bool,
) -> str:
    gates = validation.get("production_gates") or {}
    codegraph_tests = code_intelligence_summary.get("affected_tests", {}).get("suggested_tests") or []
    lines = [
        f"## {batch_id} batch issue workflow summary",
        "",
        f"- Module: `{records[0].get('module') if records else 'unknown'}`",
        f"- Closure ready: `{str(closure_ready).lower()}`",
        "",
        "## Issues",
    ]
    for record in records:
        bug_id = str(record.get("bug_id"))
        issue = record.get("github_issue_number")
        url = record.get("github_issue_url") or "missing"
        commit = commit_map.get(bug_id, "shared PR")
        lines.append(f"- `{bug_id}` / #{issue}: {url} / commit: `{commit}`")
    lines.extend([
        "",
        "## Changed files",
        *[f"- `{path}`" for path in changed_files or ["none"]],
        "",
        "## Required validation",
        *[f"- `{plan}`" for plan in validation.get("required_plans") or ["l0"]],
        "",
        "## Code intelligence",
        f"- context_ref: `{code_intelligence_summary.get('context_ref') or 'not_generated'}`",
        f"- affected_tests_ref: `{code_intelligence_summary.get('affected_tests_ref') or 'not_generated'}`",
        f"- fallback_used: `{str(bool(code_intelligence_summary.get('fallback_used'))).lower()}`",
        *[f"- CodeGraph suggested test: `{path}`" for path in codegraph_tests or ["none"]],
        "",
        "## Evidence",
        *[f"- {item}" for item in evidence or ["missing - run required validation before requesting merge"]],
        "",
        "## Production gates",
        f"- production_ddl_gate: `{gates.get('ddl', 'noop')}`",
        f"- production_frontend_dependency_gate: `{gates.get('frontend_dependency', 'noop')}`",
        f"- production_backend_dependency_gate: `{gates.get('backend_dependency', 'noop')}`",
        "",
        "## Per-issue closure map",
    ])
    for record in records:
        bug_id = str(record.get("bug_id"))
        requirements = flow._unique_strings(flow._as_list(record.get("closure_requirements"))) or ["Fix issue-specific behavior."]
        lines.append(f"- `{bug_id}`")
        lines.extend(f"  - {item}" for item in requirements)
    closing = [f"Closes #{record.get('github_issue_number')}" for record in records if record.get("github_issue_number")]
    if closing:
        lines.extend(["", *closing])
    return "\n".join(lines)


def build_finish_batch_plan(
    *,
    batch_id: str | None,
    bug_ids: list[str],
    changed_files: list[str] | None,
    base: str,
    head: str,
    validation_evidence: list[str],
    issue_commit: list[str] | None,
    plan_only: bool,
    allow_missing_evidence: bool,
    record_pairs: list[tuple[dict[str, Any], Path]] | None = None,
) -> dict[str, Any]:
    if not batch_id and not bug_ids and record_pairs is None:
        raise WorkflowError("finish-batch requires --batch-id or at least two --bug-id values")
    if batch_id:
        state = _load_batch_state(batch_id)
        if state and not bug_ids:
            bug_ids = [str(item) for item in state.get("bug_ids") or []]
    if record_pairs is None:
        record_pairs = _records_for_bug_ids(bug_ids, allow_missing_linkage=False, allow_closed=True)
    records = [record for record, _ in record_pairs]
    signature = _batch_signature(records)
    if batch_id and batch_id != signature["batch_id"]:
        signature["batch_id"] = batch_id
    canonical_batch_id = signature["batch_id"]
    changed = changed_files if changed_files is not None else flow.changed_files_from_git(base, head)
    validation = flow.select_validation(changed, module=signature["module"])
    batch_selector = _batch_validation_selector(records, module=signature["module"], changed_files=changed)
    scope_check = _batch_scope_check(changed, batch_selector["shared_files"])
    selector_blocking = list(batch_selector.get("blocking") or [])
    if scope_check["status"] == "failed":
        selector_blocking.append(f"batch changed files exceed shared scope: {scope_check['violations']}")
    code_intelligence_summary = _build_batch_code_intelligence_summary(
        batch_id=canonical_batch_id,
        records=records,
        changed_files=changed,
        root=REPO_ROOT,
    )
    codegraph_tests = code_intelligence_summary.get("affected_tests", {}).get("suggested_tests") or []
    if codegraph_tests:
        validation["codegraph_suggested_tests"] = codegraph_tests
    evidence = [item for item in validation_evidence if item.strip()]
    closure_ready = (bool(evidence) or plan_only or allow_missing_evidence) and not selector_blocking
    commit_map = _parse_issue_commit_map(issue_commit)
    output_dir = REPO_ROOT / WORKFLOW_ROOT / canonical_batch_id
    pr_body_path = output_dir / "pr-body.md"
    finish_plan = {
        "schema_version": "aistock_issue_workflow_finish_batch_v1",
        "generated_at": _utc_now(),
        **signature,
        "batch_id": canonical_batch_id,
        "changed_files": changed,
        "selected_validation": validation,
        "batch_selector": batch_selector,
        "scope_check": scope_check,
        "validation_evidence": evidence,
        "per_issue_commit_map": commit_map,
        "per_issue_closure_map": {
            str(record.get("bug_id")): flow._unique_strings(flow._as_list(record.get("closure_requirements")))
            for record in records
        },
        "code_intelligence": code_intelligence_summary,
        "closure_ready": closure_ready,
        "production_gates": validation.get("production_gates") or {},
        "blocking": selector_blocking,
    }
    _write_json(output_dir / "finish-plan.json", finish_plan)
    if evidence:
        _write_json(output_dir / "validation-evidence.json", {"batch_id": canonical_batch_id, "items": evidence})
    _write_text(
        pr_body_path,
        render_batch_pr_body(
            canonical_batch_id,
            records,
            changed,
            validation,
            evidence,
            commit_map,
            code_intelligence_summary,
            closure_ready,
        ),
    )
    next_state = "validation_passed" if evidence else ("validation_planned" if plan_only else "blocked")
    _write_state(
        canonical_batch_id,
        state=next_state,
        batch_id=canonical_batch_id,
        bug_ids=signature["bug_ids"],
        changed_files=changed,
        validation_evidence=evidence,
        pr_body_path=_repo_rel(pr_body_path),
        production_gates=validation.get("production_gates") or {},
        code_intelligence={
            "status": code_intelligence_summary.get("status"),
            "context_ref": code_intelligence_summary.get("context_ref"),
            "affected_tests_ref": code_intelligence_summary.get("affected_tests_ref"),
            "fallback_used": code_intelligence_summary.get("fallback_used"),
        },
        stop_reason=None if closure_ready else ("; ".join(selector_blocking) if selector_blocking else "validation_evidence_missing"),
        next_actions=[
            "commit_only_batch_files",
            "push_task_branch",
            "create_pr_from_batch_pr_body",
            "watch_ci_before_merge",
        ] if evidence else ["run_required_validation", "rerun_finish_batch_with_validation_evidence"],
    )
    payload = {
        **finish_plan,
        "workflow_gate": "ready_for_pr" if closure_ready else ("blocked" if selector_blocking else "validation_evidence_missing"),
        "required_verification": validation.get("required_plans") or [],
        "recommended_verification": validation.get("recommended_plans") or [],
        "code_intelligence": {
            "status": code_intelligence_summary.get("status"),
            "context_ref": code_intelligence_summary.get("context_ref"),
            "manifest_ref": code_intelligence_summary.get("manifest_ref"),
            "affected_tests_ref": code_intelligence_summary.get("affected_tests_ref"),
            "fallback_used": code_intelligence_summary.get("fallback_used"),
        },
        "codegraph_suggested_tests": codegraph_tests,
        "pr_body_path": _repo_rel(pr_body_path),
        "state_path": _repo_rel(_state_path(canonical_batch_id)),
        "events_path": _repo_rel(_events_path(canonical_batch_id)),
    }
    if not closure_ready:
        payload["error"] = "; ".join(selector_blocking) if selector_blocking else "validation evidence is required unless --plan-only or --allow-missing-evidence is used"
    return payload


def _path_check(path: Path, label: str, *, blocking: bool = True) -> dict[str, Any]:
    return {
        "label": label,
        "path": str(path),
        "exists": path.exists(),
        "blocking": blocking,
    }


def _codex_home() -> Path:
    return Path(os.environ.get("CODEX_HOME") or Path.home() / ".codex")


def _claude_home() -> Path:
    return Path(os.environ.get("CLAUDE_HOME") or Path.home() / ".claude")


def _mcp_config_snapshot() -> dict[str, Any]:
    candidates = [
        _codex_home() / "config.toml",
        _codex_home() / "config.json",
        REPO_ROOT / ".mcp.json",
    ]
    files = []
    stale_paths: list[str] = []
    for path in candidates:
        item = {"path": str(path), "exists": path.exists(), "mentions_aistock_root": False, "mentions_worktree": False}
        if path.exists():
            text = path.read_text(encoding="utf-8", errors="replace")
            normalized = text.replace("\\", "/").lower()
            item["mentions_aistock_root"] = "f:/dev/aistock" in normalized
            item["mentions_worktree"] = "aistock_worktrees" in normalized
            if item["mentions_worktree"]:
                stale_paths.append(str(path))
        files.append(item)
    return {"files": files, "stale_worktree_config_files": stale_paths}


def _client_manifest(codex_home: Path | None = None, claude_home: Path | None = None) -> dict[str, Any]:
    codex_home = codex_home or _codex_home()
    claude_home = claude_home or _claude_home()
    repo_skill = REPO_ROOT / ".codex" / "skills" / "fix-aistock-issue"
    global_skill = codex_home / "skills" / "fix-aistock-issue"
    repo_claude = REPO_ROOT / ".claude" / "commands" / "fix-aistock-issue.md"
    global_claude = claude_home / "commands" / "fix-aistock-issue.md"
    cli = REPO_ROOT / "scripts" / "aistock_issue_workflow.py"
    repo_head = _run_command(["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, timeout=15)
    repo_skill_sha = _sha256_tree(repo_skill)
    global_skill_sha = _sha256_tree(global_skill)
    claude_sha = _sha256_file(repo_claude)
    global_claude_sha = _sha256_file(global_claude)
    cli_sha = _sha256_file(cli)
    codex_status = "missing_global"
    if repo_skill_sha and global_skill_sha:
        codex_status = "current" if repo_skill_sha == global_skill_sha else "stale"
    elif repo_skill_sha and not global_skill_sha:
        codex_status = "missing_global"
    elif not repo_skill_sha:
        codex_status = "missing_repo_skill"
    return {
        "schema_version": "aistock_issue_workflow_client_manifest_v1",
        "repo_commit": repo_head.get("stdout") if repo_head.get("ok") else None,
        "workflow_cli_sha256": cli_sha,
        "codex_skill_sha256": repo_skill_sha,
        "global_codex_skill_sha256": global_skill_sha,
        "claude_command_sha256": claude_sha,
        "global_claude_command_sha256": global_claude_sha,
        "codex_skill_status": codex_status,
        "claude_command_status": "current"
        if claude_sha and global_claude_sha == claude_sha
        else ("stale_global" if claude_sha and global_claude_sha else ("missing_global" if claude_sha else "missing_repo")),
        "paths": {
            "repo_codex_skill": str(repo_skill),
            "global_codex_skill": str(global_skill),
            "claude_command": str(repo_claude),
            "global_claude_command": str(global_claude),
            "workflow_cli": str(cli),
        },
        "restart_recommended": codex_status != "current" or (claude_sha and global_claude_sha != claude_sha),
        "install_client_next_command": f"python {REPO_ROOT / 'scripts' / 'aistock_issue_workflow.py'} install-client --apply",
    }


def build_doctor_report(*, skip_external: bool = False) -> dict[str, Any]:
    canonical_root = _canonical_root()
    codex_skill = _codex_home() / "skills" / "fix-aistock-issue" / "SKILL.md"
    checks = [
        _path_check(REPO_ROOT, "repo_root"),
        _path_check(REPO_ROOT / "scripts" / "aistock_issue_workflow.py", "high_level_cli"),
        _path_check(REPO_ROOT / "scripts" / "issue_flow.py", "lower_level_cli"),
        _path_check(REPO_ROOT / ".codex" / "skills" / "fix-aistock-issue" / "SKILL.md", "repo_codex_skill"),
        _path_check(codex_skill, "global_codex_skill", blocking=False),
        _path_check(REPO_ROOT / ".claude" / "commands" / "fix-aistock-issue.md", "claude_code_command", blocking=False),
        _path_check(REPO_ROOT / "docs" / "standards" / "aistock_development_standard_v1.5_20260523.md", "active_standard"),
        _path_check(REPO_ROOT / "docs" / "architecture" / "aistock_issue_workflow_opensource_cicd_design_v2_20260525.md", "v2_design"),
        _path_check(canonical_root, "canonical_root", blocking=False),
    ]
    blocking = [
        f"missing {item['label']}: {item['path']}"
        for item in checks
        if item["blocking"] and not item["exists"]
    ]
    warnings = [
        f"missing optional {item['label']}: {item['path']}"
        for item in checks
        if not item["blocking"] and not item["exists"]
    ]

    repo_git = _git_snapshot(REPO_ROOT)
    canonical_git = _git_snapshot(canonical_root) if canonical_root.exists() else {"ok": False, "error": "canonical root missing"}
    if not repo_git.get("ok"):
        blocking.append(str(repo_git.get("error") or "repo git status failed"))
    if canonical_git.get("dirty"):
        warnings.append(f"canonical root has {canonical_git.get('dirty_count')} dirty file(s); root sync must stop")
    if canonical_git.get("branch") == "main" and canonical_git.get("head") != canonical_git.get("origin_main"):
        warnings.append("canonical root main is not equal to origin/main")

    github: dict[str, Any] = {
        "env_repository": os.environ.get("GITHUB_REPOSITORY"),
        "auth": {"ok": None, "skipped": skip_external},
        "repo": {"ok": None, "skipped": skip_external},
    }
    if not skip_external:
        github["auth"] = _run_command(["gh", "auth", "status"], cwd=REPO_ROOT, timeout=20)
        github["repo"] = _run_command(["gh", "repo", "view", "licong01-cloud/AIstock", "--json", "nameWithOwner"], cwd=REPO_ROOT, timeout=20)
        if not github["auth"].get("ok"):
            warnings.append("GitHub CLI auth check failed; MCP may still work, but gh fallback is not ready")
        if not github["repo"].get("ok"):
            warnings.append("GitHub repo check failed for licong01-cloud/AIstock")

    bug_id_allocation = _bug_id_allocation_summary(
        _bug_id_allocation_report(REPO_ROOT, include_github=not skip_external, github_required=False)
    )
    for warning in bug_id_allocation.get("warnings") or []:
        warnings.append(f"bug id allocation: {warning}")

    mcp = _mcp_config_snapshot()
    if mcp["stale_worktree_config_files"]:
        warnings.append("MCP/Codex config mentions AIstock_worktrees; verify it is not a stale server target")

    client_manifest = _client_manifest()
    if client_manifest["codex_skill_status"] in {"stale", "missing_global"}:
        warnings.append("global Codex issue skill is missing or stale; run install-client --apply and restart old client windows")
    elif client_manifest["codex_skill_status"] == "missing_repo_skill":
        blocking.append("repo Codex issue skill is missing")
    if client_manifest["claude_command_status"] == "missing_repo":
        warnings.append("repo Claude Code issue command is missing; Claude can still call the repo CLI directly")
    elif client_manifest["claude_command_status"] in {"missing_global", "stale_global"}:
        warnings.append("global Claude Code issue command is missing or stale; run install-client --apply")

    code_intel = code_intelligence.build_doctor_report(REPO_ROOT, skip_external=skip_external)
    for warning in code_intel.get("warnings") or []:
        warnings.append(f"code intelligence: {warning}")
    for item in code_intel.get("blocking") or []:
        blocking.append(f"code intelligence: {item}")
    h7_code_intelligence = _code_intelligence_readiness(code_intel)

    gate = "blocked" if blocking else ("warning" if warnings else "ready")
    next_command = f"python {REPO_ROOT / 'scripts' / 'aistock_issue_workflow.py'} run --bug-id BUG-XXX --mode plan --create-worktree"
    return {
        "schema_version": "aistock_issue_workflow_doctor_v1",
        "generated_at": _utc_now(),
        "workflow_gate": gate,
        "blocking": blocking,
        "warnings": warnings,
        "checks": checks,
        "repo_git": repo_git,
        "canonical_root": str(canonical_root),
        "canonical_git": canonical_git,
        "github": github,
        "bug_id_allocation": bug_id_allocation,
        "mcp": mcp,
        "code_intelligence": code_intel,
        "h7_code_intelligence": h7_code_intelligence,
        "client_manifest": client_manifest,
        "restart_recommended": client_manifest.get("restart_recommended"),
        "install_client_next_command": client_manifest.get("install_client_next_command"),
        "next_command": next_command,
    }


def build_client_install_plan(
    *,
    apply: bool = False,
    codex_home: str | None = None,
    claude_home: str | None = None,
) -> dict[str, Any]:
    source_skill = REPO_ROOT / ".codex" / "skills" / "fix-aistock-issue"
    source_claude = REPO_ROOT / ".claude" / "commands" / "fix-aistock-issue.md"
    target_home = Path(codex_home) if codex_home else _codex_home()
    target_skill = target_home / "skills" / "fix-aistock-issue"
    target_claude_home = Path(claude_home) if claude_home else _claude_home()
    target_claude = target_claude_home / "commands" / "fix-aistock-issue.md"
    blocking: list[str] = []
    if not source_skill.exists():
        blocking.append(f"missing repo Codex skill: {source_skill}")
    if not source_claude.exists():
        blocking.append(f"missing repo Claude Code command: {source_claude}")
    actions = [
        {
            "action": "sync_global_codex_skill",
            "source": str(source_skill),
            "target": str(target_skill),
            "safe": not blocking,
        },
        {
            "action": "verify_claude_code_command",
            "source": str(source_claude),
            "target": str(target_claude),
            "safe": source_claude.exists(),
        },
    ]
    payload = {
        "schema_version": "aistock_issue_workflow_client_install_v1",
        "generated_at": _utc_now(),
        "dry_run": not apply,
        "workflow_gate": "ready_for_install" if not blocking else "blocked",
        "blocking": blocking,
        "actions": actions,
        "codex_home": str(target_home),
        "claude_home": str(target_claude_home),
        "client_manifest_before": _client_manifest(target_home, target_claude_home),
    }
    if apply:
        if blocking:
            raise WorkflowError("; ".join(blocking))
        if target_skill.exists():
            shutil.rmtree(target_skill)
        target_skill.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(source_skill, target_skill)
        target_claude.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_claude, target_claude)
        payload["workflow_gate"] = "installed"
        payload["dry_run"] = False
        payload["installed"] = [{"target": str(target_skill)}, {"target": str(target_claude)}]
        payload["client_manifest_after"] = _client_manifest(target_home, target_claude_home)
    manifest_path = REPO_ROOT / WORKFLOW_ROOT / "client-manifest.json"
    _write_json(
        manifest_path,
        payload.get("client_manifest_after")
        or payload.get("client_manifest_before")
        or _client_manifest(target_home, target_claude_home),
    )
    payload["client_manifest_path"] = _repo_rel(manifest_path)
    return payload


def _state_roots_for_bug(bug_id: str) -> list[Path]:
    roots: list[Path] = [REPO_ROOT]
    worktree_root = _default_worktree_root()
    if worktree_root.exists():
        for state in worktree_root.glob(f"*/{WORKFLOW_ROOT.as_posix()}/{bug_id}/state.json"):
            roots.append(state.parents[3])
    unique: list[Path] = []
    seen: set[str] = set()
    for root in roots:
        key = str(root.resolve())
        if key not in seen:
            seen.add(key)
            unique.append(root)
    return unique


def build_resume_plan(*, bug_id: str, worktree: str | None = None, events_limit: int = 8) -> dict[str, Any]:
    canonical_bug_id = bug_id.strip().upper()
    roots = [Path(worktree)] if worktree else _state_roots_for_bug(canonical_bug_id)
    candidates = [(root, _load_state(canonical_bug_id, root)) for root in roots]
    candidates = [(root, state) for root, state in candidates if state]
    if not candidates:
        raise WorkflowError(f"No workflow state found for {canonical_bug_id}; run start or run --mode plan first")
    root, state = sorted(candidates, key=lambda item: _workflow_state_sort_key(item[0], item[1]))[-1]
    events_path = _events_path(canonical_bug_id, root)
    events: list[dict[str, Any]] = []
    if events_path.exists():
        lines = events_path.read_text(encoding="utf-8").splitlines()[-events_limit:]
        events = [json.loads(line) for line in lines if line.strip()]
    git = _git_snapshot(root) if root.exists() else {"ok": False, "error": f"workflow root missing: {root}"}
    dirty_stop = bool(git.get("dirty") and state.get("state") == "validation_passed")
    state_worktree = str(state.get("worktree") or "").strip()
    missing_state_worktree = bool(state_worktree and not Path(state_worktree).exists())
    actual_worktree = None if missing_state_worktree else state.get("worktree")
    planned_worktree = state.get("planned_worktree") or (state_worktree if missing_state_worktree else None)
    planned_only = bool(planned_worktree and not actual_worktree)
    stop_conditions = ["commit task files before PR automation"] if dirty_stop else []
    if planned_only:
        stop_conditions.append("planned worktree has not been created; rerun plan with --create-worktree")
    return {
        "schema_version": "aistock_issue_workflow_resume_v1",
        "generated_at": _utc_now(),
        "bug_id": canonical_bug_id,
        "workflow_root": str(root),
        "workflow_git": git,
        "worktree": actual_worktree,
        "planned_worktree": planned_worktree,
        "branch": state.get("branch") or git.get("branch"),
        "planned_branch": state.get("planned_branch"),
        "task_card_json": state.get("task_card_json") or _repo_rel(_task_card_json_path(canonical_bug_id, root), root),
        "task_card_md": state.get("task_card_md") or _repo_rel(_task_card_md_path(canonical_bug_id, root), root),
        "state_path": _repo_rel(_state_path(canonical_bug_id, root), root),
        "events_path": _repo_rel(events_path, root),
        "state": state,
        "recent_events": events,
        "stop_conditions": stop_conditions,
        "next_command": _next_command_for_state(canonical_bug_id, state),
    }


def _workflow_artifacts_enabled() -> bool:
    value = os.environ.get("AISTOCK_WORKFLOW_ARTIFACTS", "").strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    return os.environ.get("AISTOCK_VALIDATION_ARTIFACTS", "").strip().lower() in {"1", "true", "yes", "on"}


def build_postmortem_plan(
    *,
    bug_id: str,
    worktree: str | None = None,
    output_markdown: bool = True,
    persist_artifacts: bool | None = None,
) -> dict[str, Any]:
    canonical_bug_id = bug_id.strip().upper()
    roots = [Path(worktree)] if worktree else _state_roots_for_bug(canonical_bug_id)
    candidates = [(root, _load_state(canonical_bug_id, root)) for root in roots]
    candidates = [(root, state) for root, state in candidates if state]
    if not candidates:
        prior = _load_prior_postmortem(canonical_bug_id, roots)
        if prior:
            return prior
        raise WorkflowError(f"No workflow state found for {canonical_bug_id}; run start or run --mode plan first")
    root, state = sorted(candidates, key=lambda item: _workflow_state_sort_key(item[0], item[1]))[-1]
    events = _read_events(canonical_bug_id, root)
    timing = _augment_timing_with_issue_record(_workflow_timing_summary(canonical_bug_id, root), state, root)
    if state.get("state") == "complete" and (timing.get("event_count") or 0) <= 1:
        prior = _load_prior_postmortem(canonical_bug_id, roots)
        prior_timing = prior.get("timing_summary") if isinstance(prior, dict) else {}
        if prior and (prior_timing.get("event_count") or 0) > (timing.get("event_count") or 0):
            prior["workflow_gate"] = "artifact_fallback"
            prior["artifact_fallback"] = {
                "reason": "prior_postmortem_has_more_phase_evidence_than_cleanup_state",
                "current_event_count": timing.get("event_count") or 0,
                "prior_event_count": prior_timing.get("event_count") or 0,
            }
            return prior
    active = _active_workflows_for_bug(canonical_bug_id)
    duplicate_active_count = max(0, len(active) - 1)
    stale_pr_check = _stale_pr_check_for_bug(canonical_bug_id)
    context_metrics = state.get("context_metrics") or {}
    artifact_metrics = {}
    finish_plan = root / WORKFLOW_ROOT / canonical_bug_id / "finish-plan.json"
    pr_body = root / WORKFLOW_ROOT / canonical_bug_id / "pr-body.md"
    if finish_plan.exists() or pr_body.exists():
        artifact_metrics = {
            "finish_plan": _size_and_token_estimate(finish_plan),
            "pr_body": _size_and_token_estimate(pr_body),
        }
    phase_cost_table = _phase_cost_table(timing)
    h6_summary = _h6_summary(timing, context_metrics, artifact_metrics)
    h7_code_intelligence = _code_intelligence_readiness(state.get("code_intelligence") or {})
    code_intelligence_efficiency = _code_intelligence_efficiency_summary(state.get("code_intelligence") or {})
    payload = {
        "schema_version": "aistock_issue_workflow_postmortem_v1",
        "generated_at": _utc_now(),
        "bug_id": canonical_bug_id,
        "workflow_root": str(root),
        "state": {
            "state": state.get("state"),
            "branch": state.get("branch"),
            "worktree": state.get("worktree") or str(root),
            "pr_url": state.get("pr_url"),
            "commit": state.get("commit"),
            "next_actions": state.get("next_actions") or [],
        },
        "timing_summary": timing,
        "phase_cost_table": phase_cost_table,
        "h6_summary": h6_summary,
        "context_metrics": context_metrics,
        "artifact_metrics": artifact_metrics,
        "h7_code_intelligence": h7_code_intelligence,
        "code_intelligence_efficiency": code_intelligence_efficiency,
        "active_workflows": active,
        "duplicate_active_count": duplicate_active_count,
        "stale_pr_check": stale_pr_check,
        "flow_overhead_estimate": {
            "known_duration_seconds": timing.get("known_duration_seconds"),
            "inferred_elapsed_seconds": timing.get("inferred_elapsed_seconds"),
            "event_count": timing.get("event_count"),
            "context_estimated_tokens": h6_summary.get("context_estimated_tokens"),
            "artifact_estimated_tokens": h6_summary.get("artifact_estimated_tokens"),
            "code_intelligence_broad_scan_avoided": code_intelligence_efficiency.get("broad_scan_avoided"),
            "estimated_broad_scan_tokens_avoided": code_intelligence_efficiency.get("estimated_broad_scan_tokens_avoided"),
            "top_phase": h6_summary.get("top_phase"),
        },
        "production_gates": state.get("production_gates") or {},
        "recent_events": events[-20:],
    }
    if persist_artifacts is None:
        persist_artifacts = _workflow_artifacts_enabled() or str(state.get("state") or "") == "blocked"
    payload["artifact_policy"] = "persisted" if persist_artifacts else "compact_success_no_artifact"
    output_dir = root / WORKFLOW_ROOT / canonical_bug_id
    if output_markdown and persist_artifacts:
        lines = [
            f"# {canonical_bug_id} Workflow Postmortem",
            "",
            f"- State: `{payload['state']['state']}`",
            f"- Branch: `{payload['state']['branch'] or 'unknown'}`",
            f"- PR: {payload['state']['pr_url'] or 'n/a'}",
            f"- Events: `{timing.get('event_count')}`",
            f"- Known command duration: `{timing.get('known_duration_seconds')}s`",
            f"- Inferred elapsed duration: `{timing.get('inferred_elapsed_seconds')}s`",
            f"- Duplicate active workflows: `{duplicate_active_count}`",
            "",
            "## Phase Timing",
            "",
            "| Phase | Events | Known seconds | Inferred seconds |",
            "| --- | ---: | ---: | ---: |",
        ]
        for item in phase_cost_table:
            lines.append(
                f"| `{item.get('phase')}` | {item.get('event_count')} | {item.get('known_seconds')} | {item.get('inferred_seconds')} |"
            )
        lines.extend(
            [
                "",
                "## H6 Cost Summary",
                "",
                f"- Top phase: `{(h6_summary.get('top_phase') or {}).get('phase') or 'none'}`",
                f"- Token usage status: `{h6_summary.get('token_usage_status') or 'unknown'}`",
                f"- Context estimated tokens: `{h6_summary.get('context_estimated_tokens') if h6_summary.get('context_estimated_tokens') is not None else 'unknown'}`",
                f"- Artifact estimated tokens: `{h6_summary.get('artifact_estimated_tokens') if h6_summary.get('artifact_estimated_tokens') is not None else 'unknown'}`",
                f"- Queue seconds: `{h6_summary.get('queue_seconds') or 'not_recorded'}`",
                f"- Active fix seconds: `{h6_summary.get('active_fix_seconds') or 'not_recorded'}`",
                f"- Local validation seconds: `{h6_summary.get('local_validation_seconds') or 'not_recorded'}`",
                f"- PR/CI seconds: `{h6_summary.get('pr_ci_seconds') or 'not_recorded'}`",
                f"- Merge aftercare seconds: `{h6_summary.get('merge_aftercare_seconds') or 'not_recorded'}`",
                f"- Code repair seconds: `{h6_summary.get('code_repair_seconds') or 'not_recorded'}`",
                "",
                "## H7 Code Intelligence",
                "",
                f"- status: `{h7_code_intelligence.get('status') or 'unknown'}`",
                f"- codegraph_status: `{h7_code_intelligence.get('codegraph_status') or 'unknown'}`",
                f"- codegraph_freshness: `{h7_code_intelligence.get('codegraph_freshness') or 'not_available'}`",
                f"- codegraph_freshness_ref: `{h7_code_intelligence.get('codegraph_freshness_ref') or 'not_available'}`",
                f"- understand_anything_status: `{h7_code_intelligence.get('understand_anything_status') or 'unknown'}`",
                f"- understand_anything_graph_exists: `{str(bool(h7_code_intelligence.get('understand_anything_graph_exists'))).lower()}`",
                f"- understand_anything_next_command: `{h7_code_intelligence.get('understand_anything_next_command') or 'not_required'}`",
                f"- fallback_used: `{str(bool(h7_code_intelligence.get('fallback_used'))).lower()}`",
                f"- readiness_next_command: `{h7_code_intelligence.get('readiness_next_command') or 'not_required'}`",
                f"- broad_scan_avoided: `{str(bool(code_intelligence_efficiency.get('broad_scan_avoided'))).lower()}`",
                f"- estimated_broad_scan_tokens_avoided: `{code_intelligence_efficiency.get('estimated_broad_scan_tokens_avoided') if code_intelligence_efficiency.get('estimated_broad_scan_tokens_avoided') is not None else 'unknown'}`",
                "",
                "## Production Gates",
                "",
                *[f"- {key}: `{value}`" for key, value in sorted((payload.get("production_gates") or {}).items())],
            ]
        )
        _write_text(output_dir / "postmortem.md", "\n".join(lines))
        payload["postmortem_md_path"] = _repo_rel(output_dir / "postmortem.md", root)
    if persist_artifacts:
        payload["postmortem_json_path"] = _repo_rel(output_dir / "postmortem.json", root)
        _write_json(output_dir / "postmortem.json", payload)
    return payload


def _classify_ci_issue(summary: dict[str, Any], issue: dict[str, Any]) -> str:
    title = str(issue.get("title") or "").lower()
    evidence_body = _issue_body_failure_text(str(issue.get("body") or "")).lower()
    title_body = f"{title}\n{evidence_body}"
    errors = "\n".join(
        str(item)
        for job in summary.get("failed_jobs") or []
        for item in [job.get("error_signature"), *(job.get("key_log_excerpt") or [])]
        if item
    ).lower()
    infra_signatures = [
        "self-hosted",
        "runner-preflight",
        "runner unavailable",
        "runner availability",
        "no online github actions runner",
        "unable to query github runner health",
        "aistock_runner_health_token",
    ]
    if any(token in title_body or token in errors for token in infra_signatures):
        return "infra_blocker"
    if any(token in title_body for token in ["flaky", "timeout", "network"]):
        return "infra_flaky"
    if any(token in errors for token in ["relation ", "does not exist", "fixture", "test fixture"]):
        return "test_fixture_gap_or_real_regression"
    if summary.get("diagnostic_status") == "complete":
        return "real_regression_candidate"
    return "needs_log_triage"


def build_triage_ci_issue_plan(
    *,
    issue_number: int | str,
    run_id: str | None = None,
    summary_json: str | None = None,
    skip_github_summary: bool = False,
) -> dict[str, Any]:
    issue = _load_github_issue(issue_number)
    linked = _find_bug_by_github_issue(issue["number"])
    body = str(issue.get("body") or "")
    detected_run_id = run_id or _extract_run_id_from_issue_body(body)
    summary: dict[str, Any]
    extraction_errors: list[str] = []
    if summary_json:
        summary = _load_json(Path(summary_json))
    elif detected_run_id and not skip_github_summary:
        summary = ci_failure_summary.summarize_actions_run(
            repo=GITHUB_REPO,
            run_id=detected_run_id,
            run_url=None,
            severity="P1",
        )
    else:
        extraction_errors.append("No Actions run id was found in the GitHub Issue body.")
        summary = ci_failure_summary.finalize_summary(
            {
                "schema_version": "aistock_ci_failure_summary_v1",
                "generated_at": _utc_now(),
                "severity": "P1",
                "workflow": "unknown",
                "run_id": detected_run_id or "",
                "run_url": None,
                "branch": None,
                "commit": None,
                "failed_jobs": [],
                "extraction_errors": extraction_errors,
            }
        )
    summary = _merge_issue_body_regression_locator(summary, body)
    classification = _classify_ci_issue(summary, issue)
    superseding_run = _find_superseding_main_success(summary)
    superseded_action = None
    if superseding_run and linked is None:
        same_branch = superseding_run.get("supersede_scope") == "same_branch"
        classification = "superseded_by_later_branch_success" if same_branch else "superseded_by_later_main_success"
        success_phrase = _superseded_success_phrase(superseding_run, str(summary.get("workflow") or "CI"))
        close_command = (
            f"gh issue close {issue.get('number')} --repo {GITHUB_REPO} --comment "
            f"\"Superseded by {success_phrase}: {superseding_run.get('run_url')}. "
            "No BUG JSON promotion is required.\""
        )
        superseded_action = {
            "workflow_gate": "superseded_by_latest_branch_success" if same_branch else "superseded_by_latest_main_success",
            "reason": (
                "A later successful run of the same workflow on the same branch supersedes this CI failure."
                if same_branch
                else "A later successful default-branch run of the same workflow supersedes this CI failure."
            ),
            "superseding_run": superseding_run,
            "next_command": close_command,
            "production_gates": {
                "production_ddl_gate": "noop",
                "production_frontend_dependency_gate": "noop",
                "production_backend_dependency_gate": "noop",
            },
        }
    module = (summary.get("suspected_modules") or ["validation"])[0]
    first_job = (summary.get("failed_jobs") or [{}])[0]
    failed_test = ((first_job.get("failed_tests") or [None])[0] or "").split("::")[-1]
    suggested_title = (
        f"{module} CI failure requires triage: {failed_test or first_job.get('error_signature') or issue.get('title')}"
    )
    output_dir = REPO_ROOT / WORKFLOW_ROOT / f"ci-issue-{issue.get('number')}"
    github_issue_url = issue.get("url") or _github_issue_url(issue.get("number"))
    failure_event = ci_failure_summary.build_failure_event(
        summary,
        github_issue_number=issue.get("number"),
        github_issue_url=github_issue_url,
    )
    context_pack = ci_failure_summary.build_context_pack(
        summary,
        github_issue_number=issue.get("number"),
        github_issue_url=github_issue_url,
    )
    actionable = classification in ACTIONABLE_CI_CLASSIFICATIONS
    triage_action: str | None = None
    if not actionable and classification not in {"infra_flaky", "infra_blocker", *SUPERSEDED_CI_CLASSIFICATIONS}:
        triage_action = "triage_incomplete_collect_failure_diagnostics_before_bug_promotion"
        failure_event["candidate_status"] = "triage_incomplete"
        context_pack["failure_event"] = failure_event
        handoff = context_pack.get("agent_handoff") if isinstance(context_pack.get("agent_handoff"), dict) else {}
        handoff["needs_bug_json"] = False
        handoff["handoff_mode"] = "triage_only"
        handoff["workflow_entrypoints"] = {
            "triage": f"python scripts/aistock_issue_workflow.py triage-ci-issue --issue {issue.get('number')}",
            "promote": "blocked_until_diagnostic_status_complete",
        }
        handoff["next_commands"] = [f"python scripts/aistock_issue_workflow.py triage-ci-issue --issue {issue.get('number')}"]
        handoff["stop_conditions"] = [
            "Do not promote BUG JSON until CI/Nightly diagnostics identify a concrete code or test failure.",
            "Do not edit source files from this triage-only issue.",
        ]
        context_pack["agent_handoff"] = handoff
    if superseded_action:
        failure_event["candidate_status"] = classification
        failure_event["superseded_action"] = superseded_action
        context_pack["failure_event"] = failure_event
        context_pack["superseded_action"] = superseded_action
        verification_scope = "same branch" if classification == "superseded_by_later_branch_success" else "default-branch"
        context_pack["required_verification"] = [
            f"Verify the superseding {verification_scope} run for the same workflow is successful.",
            "Close the auto-filed GitHub Issue with the superseding run URL; do not promote BUG JSON.",
        ]
        handoff = context_pack.get("agent_handoff") if isinstance(context_pack.get("agent_handoff"), dict) else {}
        handoff["workflow_entrypoints"] = {
            "triage": f"python scripts/aistock_issue_workflow.py triage-ci-issue --issue {issue.get('number')}",
            "close_superseded_issue": superseded_action["next_command"],
        }
        handoff["next_commands"] = [superseded_action["next_command"]]
        handoff["required_verification"] = context_pack["required_verification"]
        handoff["stop_conditions"] = ["Do not promote BUG JSON for this superseded CI failure."]
        context_pack["agent_handoff"] = handoff
    failure_event_path = output_dir / "failure-event.json"
    context_pack_json_path = output_dir / "context-pack.json"
    context_pack_md_path = output_dir / "context-pack.md"
    payload = {
        "schema_version": "aistock_issue_workflow_triage_ci_issue_v1",
        "generated_at": _utc_now(),
        "github_issue": {
            "number": issue.get("number"),
            "url": issue.get("url"),
            "title": issue.get("title"),
            "state": issue.get("state"),
        },
        "detected_run_id": detected_run_id,
        "summary": summary,
        "failure_event": failure_event,
        "failure_event_path": _repo_rel(failure_event_path),
        "context_pack": context_pack,
        "context_pack_json_path": _repo_rel(context_pack_json_path),
        "context_pack_md_path": _repo_rel(context_pack_md_path),
        "classification_recommendation": classification,
        "linked_bug": {"bug_id": linked[0].get("bug_id"), "path": _repo_rel(linked[1])} if linked else None,
        "needs_bug_json": linked is None and actionable,
        "triage_action": triage_action,
        "suggested_bug": {
            "module": module,
            "severity": summary.get("severity") or "P1",
            "title": suggested_title[:180],
            "risk_area": "ci_failure_intake",
            "allowed_write_scope": context_pack.get("allowed_write_scope") or [],
            "required_verification": context_pack.get("required_verification") or [],
        },
        "infra_action": (
            {
                "workflow_gate": "infra_action_required",
                "reason": "CI/Nightly failure is classified as infrastructure, not a code regression.",
                "next_actions": [
                    "restore or register the self-hosted Windows GitHub Actions runner",
                    "verify runner labels include: self-hosted, windows",
                    "configure AISTOCK_RUNNER_HEALTH_TOKEN if runner API access is denied",
                    "rerun the failed workflow after infrastructure is healthy",
                ],
                "production_gates": {
                    "production_ddl_gate": "noop",
                    "production_frontend_dependency_gate": "noop",
                    "production_backend_dependency_gate": "noop",
                },
            }
            if classification in {"infra_flaky", "infra_blocker"}
            else None
        ),
        "superseded_action": superseded_action,
        "next_command": (
            "infra_action_required_no_code_bug"
            if classification in {"infra_flaky", "infra_blocker"} and linked is None
            else (
                superseded_action["next_command"]
                if classification in SUPERSEDED_CI_CLASSIFICATIONS and linked is None and superseded_action
                else (
                    f"python scripts/aistock_issue_workflow.py promote-ci-issue --issue {issue.get('number')} "
                    "--create-registry-worktree --apply"
                    if linked is None and actionable
                    else triage_action
                    if linked is None
                    else f"python scripts/aistock_issue_workflow.py run --bug-id {linked[0].get('bug_id')} --mode plan --create-worktree"
                )
            )
        ),
    }
    _write_json(failure_event_path, failure_event)
    _write_json(context_pack_json_path, context_pack)
    _write_text(context_pack_md_path, ci_failure_summary.render_context_pack_markdown(context_pack))
    _write_json(output_dir / "triage-ci-issue.json", payload)
    return payload


def _list_open_auto_filed_ci_issues(*, limit: int = 50) -> list[dict[str, Any]]:
    result = _execute_checked(
        [
            "gh",
            "issue",
            "list",
            "--repo",
            GITHUB_REPO,
            "--state",
            "open",
            "--label",
            "auto-filed",
            "--limit",
            str(limit),
            "--json",
            "number,title,state,url,labels",
        ],
        cwd=REPO_ROOT,
        timeout=60,
    )
    try:
        issues = json.loads(str(result.get("stdout") or "[]"))
    except json.JSONDecodeError as exc:
        raise WorkflowError(f"cannot parse auto-filed CI issue list: {exc}") from exc
    if not isinstance(issues, list):
        raise WorkflowError("auto-filed CI issue list returned non-list JSON")
    filtered: list[dict[str, Any]] = []
    for issue in issues:
        if not isinstance(issue, dict):
            continue
        labels = {
            str(item.get("name") or "")
            for item in issue.get("labels") or []
            if isinstance(item, dict)
        }
        if "ci" in labels:
            filtered.append(issue)
    return filtered


def _close_superseded_ci_issue(issue_number: int | str, superseding_run: dict[str, Any], workflow: str | None) -> dict[str, Any]:
    success_phrase = _superseded_success_phrase(superseding_run, workflow)
    comment = (
        f"Superseded by {success_phrase}: {superseding_run.get('run_url')}. "
        "No BUG JSON promotion is required. production_ddl_gate=noop; "
        "production_frontend_dependency_gate=noop; production_backend_dependency_gate=noop."
    )
    return _execute_checked(
        [
            "gh",
            "issue",
            "close",
            str(issue_number),
            "--repo",
            GITHUB_REPO,
            "--comment",
            comment,
        ],
        cwd=REPO_ROOT,
        timeout=60,
    )


def _close_infra_ci_issue(issue_number: int | str, infra_action: dict[str, Any], workflow: str | None) -> dict[str, Any]:
    next_actions = infra_action.get("next_actions") if isinstance(infra_action.get("next_actions"), list) else []
    next_action_text = "; ".join(str(item) for item in next_actions[:4])
    comment = (
        f"Closed as infrastructure-only {workflow or 'CI'} failure. "
        f"{infra_action.get('reason') or 'No code BUG JSON promotion is required.'} "
        f"Next actions: {next_action_text or 'restore infrastructure and rerun CI/Nightly'}. "
        "production_ddl_gate=noop; production_frontend_dependency_gate=noop; "
        "production_backend_dependency_gate=noop."
    )
    return _execute_checked(
        [
            "gh",
            "issue",
            "close",
            str(issue_number),
            "--repo",
            GITHUB_REPO,
            "--comment",
            comment,
        ],
        cwd=REPO_ROOT,
        timeout=60,
    )


def build_ci_issue_janitor_plan(
    *,
    issue_numbers: list[int | str] | None = None,
    apply: bool = False,
    limit: int = 50,
    skip_github_summary: bool = False,
) -> dict[str, Any]:
    if issue_numbers:
        issues = [{"number": str(item)} for item in issue_numbers]
        source = "explicit_issues"
    else:
        issues = _list_open_auto_filed_ci_issues(limit=limit)
        source = "open_auto_filed_ci_issues"
    evaluated: list[dict[str, Any]] = []
    closed: list[int | str] = []
    failed: list[dict[str, Any]] = []
    superseded_count = 0
    infra_count = 0
    for item in issues:
        issue_number = item.get("number")
        if issue_number is None:
            continue
        issue_value = int(issue_number) if str(issue_number).isdigit() else issue_number
        entry: dict[str, Any] = {"issue": issue_value, "action": "skip"}
        try:
            triage = build_triage_ci_issue_plan(
                issue_number=issue_number,
                skip_github_summary=skip_github_summary,
            )
        except WorkflowError as exc:
            entry.update({"action": "failed", "reason": str(exc)})
            failed.append(entry)
            evaluated.append(entry)
            continue
        entry["classification"] = triage.get("classification_recommendation")
        entry["linked_bug"] = triage.get("linked_bug")
        entry["github_issue"] = triage.get("github_issue")
        if (
            triage.get("classification_recommendation") in SUPERSEDED_CI_CLASSIFICATIONS
            and not triage.get("linked_bug")
            and isinstance(triage.get("superseded_action"), dict)
        ):
            superseded_count += 1
            entry["action"] = "close_superseded"
            entry["superseding_run"] = triage["superseded_action"].get("superseding_run")
            if apply:
                try:
                    result = _close_superseded_ci_issue(
                        issue_value,
                        triage["superseded_action"].get("superseding_run") or {},
                        (triage.get("summary") or {}).get("workflow"),
                    )
                    entry["close_result"] = _pick(result, "ok", "returncode")
                    closed.append(issue_value)
                except WorkflowError as exc:
                    entry.update({"action": "failed", "reason": str(exc)})
                    failed.append(entry)
        elif (
            triage.get("classification_recommendation") in {"infra_flaky", "infra_blocker"}
            and not triage.get("linked_bug")
            and triage.get("needs_bug_json") is False
            and isinstance(triage.get("infra_action"), dict)
        ):
            infra_count += 1
            entry["action"] = "close_infra"
            entry["infra_action"] = _pick(
                triage["infra_action"],
                "workflow_gate",
                "reason",
                "production_gates",
            )
            actions = triage["infra_action"].get("next_actions")
            if isinstance(actions, list):
                entry["infra_action"]["next_actions"] = actions[:4]
            if apply:
                try:
                    result = _close_infra_ci_issue(
                        issue_value,
                        triage["infra_action"],
                        (triage.get("summary") or {}).get("workflow"),
                    )
                    entry["close_result"] = _pick(result, "ok", "returncode")
                    closed.append(issue_value)
                except WorkflowError as exc:
                    entry.update({"action": "failed", "reason": str(exc)})
                    failed.append(entry)
        else:
            entry["reason"] = "not_superseded_or_infra_or_requires_bug_workflow"
        evaluated.append(entry)
    actionable_count = superseded_count + infra_count
    workflow_gate = "failed" if failed else ("closed" if apply and closed else ("ready_for_apply" if actionable_count else "no_actionable_ci_issues"))
    payload = {
        "schema_version": "aistock_issue_workflow_ci_issue_janitor_v1",
        "generated_at": _utc_now(),
        "workflow_gate": workflow_gate,
        "dry_run": not apply,
        "source": source,
        "limit": limit,
        "scanned_count": len(evaluated),
        "superseded_count": superseded_count,
        "infra_count": infra_count,
        "closed_count": len(closed),
        "skipped_count": len([item for item in evaluated if item.get("action") == "skip"]),
        "failed_count": len(failed),
        "closed_issues": closed,
        "failed_issues": [{"issue": item.get("issue"), "reason": item.get("reason")} for item in failed],
        "issues": evaluated,
        "production_gates": {
            "production_ddl_gate": "noop",
            "production_frontend_dependency_gate": "noop",
            "production_backend_dependency_gate": "noop",
        },
    }
    if not apply and actionable_count:
        issue_args = " ".join(f"--issue {item.get('issue')}" for item in evaluated if item.get("action") in {"close_superseded", "close_infra"})
        limit_arg = "" if issue_args else f" --limit {limit}"
        payload["next_command"] = (
            f"python scripts/aistock_issue_workflow.py ci-issue-janitor {issue_args} --apply"
            if issue_args
            else f"python scripts/aistock_issue_workflow.py ci-issue-janitor{limit_arg} --apply"
        )
    output_dir = REPO_ROOT / WORKFLOW_ROOT / "ci-issue-janitor"
    _write_json(output_dir / "ci-issue-janitor.json", payload)
    return payload


def build_promote_ci_issue_plan(
    *,
    issue_number: int | str,
    apply: bool,
    bug_id: str | None = None,
    summary_json: str | None = None,
    skip_github_summary: bool = False,
    create_registry_worktree: bool = False,
) -> dict[str, Any]:
    triage = build_triage_ci_issue_plan(
        issue_number=issue_number,
        summary_json=summary_json,
        skip_github_summary=skip_github_summary,
    )
    if triage.get("linked_bug"):
        return {
            "schema_version": "aistock_issue_workflow_promote_ci_issue_v1",
            "generated_at": _utc_now(),
            "workflow_gate": "already_linked",
            "dry_run": not apply,
            "triage": triage,
            "next_command": f"python scripts/aistock_issue_workflow.py run --bug-id {triage['linked_bug']['bug_id']} --mode plan --create-worktree",
        }
    if triage.get("classification_recommendation") in {"infra_flaky", "infra_blocker"}:
        return {
            "schema_version": "aistock_issue_workflow_promote_ci_issue_v1",
            "generated_at": _utc_now(),
            "workflow_gate": "blocked_infra_issue_not_code_bug",
            "dry_run": not apply,
            "triage": triage,
            "infra_action": triage.get("infra_action"),
            "next_command": "resolve_infrastructure_then_rerun_triage_ci_issue",
        }
    if triage.get("classification_recommendation") in SUPERSEDED_CI_CLASSIFICATIONS:
        action = triage.get("superseded_action") if isinstance(triage.get("superseded_action"), dict) else {}
        return {
            "schema_version": "aistock_issue_workflow_promote_ci_issue_v1",
            "generated_at": _utc_now(),
            "workflow_gate": "blocked_superseded_issue_not_code_bug",
            "dry_run": not apply,
            "triage": triage,
            "superseded_action": action,
            "next_command": action.get("next_command") or "close_ci_issue_as_superseded_after_review",
        }
    if triage.get("classification_recommendation") not in ACTIONABLE_CI_CLASSIFICATIONS:
        return {
            "schema_version": "aistock_issue_workflow_promote_ci_issue_v1",
            "generated_at": _utc_now(),
            "workflow_gate": "blocked_triage_incomplete_not_code_bug",
            "dry_run": not apply,
            "triage": triage,
            "triage_action": triage.get("triage_action")
            or "triage_incomplete_collect_failure_diagnostics_before_bug_promotion",
            "next_command": triage.get("next_command")
            or "rerun_triage_ci_issue_after_failure_diagnostics_are_available",
        }
    if apply and not create_registry_worktree:
        return {
            "schema_version": "aistock_issue_workflow_promote_ci_issue_v1",
            "generated_at": _utc_now(),
            "workflow_gate": "registry_worktree_required",
            "dry_run": False,
            "triage": triage,
            "blocking": [
                "promote-ci-issue --apply must use --create-registry-worktree so CI/Nightly intake cannot write BUG JSON from canonical root or main"
            ],
            "next_command": (
                f"python scripts/aistock_issue_workflow.py promote-ci-issue --issue {issue_number} "
                "--create-registry-worktree --apply"
            ),
        }
    summary = triage["summary"]
    suggested = triage["suggested_bug"]
    first_job = (summary.get("failed_jobs") or [{}])[0]
    failed_tests = first_job.get("failed_tests") or []
    error_signature = first_job.get("error_signature")
    details = ci_failure_summary.render_issue_markdown(summary, github_issue_number=issue_number)
    changed_files = list(suggested.get("allowed_write_scope") or [])
    evidence_refs = flow._unique_strings(
        [
            str(summary.get("run_url") or ""),
            _github_issue_url(issue_number),
            str(triage.get("failure_event_path") or ""),
            str(triage.get("context_pack_md_path") or ""),
        ]
    )
    plan = build_submit_bug_plan(
        title=suggested["title"],
        module=suggested["module"],
        severity=suggested["severity"],
        description=f"Auto-filed CI issue #{issue_number} requires actionable triage and repair.\n\n{details}",
        expected="CI/Nightly failure issues include enough diagnostic detail to enter the BUG JSON workflow without manual log rediscovery.",
        actual=f"Failure summary: {error_signature or (failed_tests[0] if failed_tests else 'diagnostic extraction incomplete')}",
        reproduce_command=str(summary.get("reproduce_command") or "Inspect linked CI run log."),
        evidence_refs=evidence_refs,
        changed_files=changed_files,
        plan_key="ci_failure_issue_intake",
        nox_session=first_job.get("nox_session"),
        candidate_type="regression",
        bug_id=bug_id,
        github_issue_number=str(issue_number),
        github_issue_url=_github_issue_url(issue_number),
        create_github=False,
        apply=apply,
        create_registry_worktree=create_registry_worktree,
        registry_pr_only=False,
    )
    return {
        "schema_version": "aistock_issue_workflow_promote_ci_issue_v1",
        "generated_at": _utc_now(),
        "workflow_gate": "promoted" if apply else "ready_for_apply",
        "dry_run": not apply,
        "triage": triage,
        "submit_bug": plan,
        "next_command": plan.get("next_command"),
    }


def _next_command_for_state(bug_id: str, state: dict[str, Any]) -> str:
    current = str(state.get("state") or "")
    worktree = str(state.get("worktree") or state.get("cwd") or "").strip()
    state_worktree = str(state.get("worktree") or "").strip()
    has_missing_state_worktree = bool(state_worktree and not Path(state_worktree).exists())
    prefix = f"cd /d {worktree} && " if worktree and not has_missing_state_worktree else ""
    if (state.get("planned_worktree") and not state.get("worktree")) or has_missing_state_worktree:
        return f"python scripts/aistock_issue_workflow.py run --bug-id {bug_id} --mode plan --create-worktree"
    if current in {"context_ready", "fix_in_progress"}:
        return f"{prefix}python scripts/aistock_issue_workflow.py finish --bug-id {bug_id} --plan-only"
    if current in {"validation_planned", "blocked"}:
        return f"{prefix}python scripts/aistock_issue_workflow.py finish --bug-id {bug_id} --validation-evidence \"<command> -> passed\""
    if current == "validation_passed":
        return (
            f"{prefix}git status --short && git add <task files> && git commit -m \"fix: resolve {bug_id}\" && "
            f"python scripts/aistock_issue_workflow.py run --bug-id {bug_id} --mode pr "
            "--validation-evidence \"<command> -> passed\" --push --create-pr"
        )
    if current in {"pr_opened", "ci_running"}:
        pr_url = str(state.get("pr_url") or "<PR_URL>")
        return f"{prefix}python scripts/aistock_issue_workflow.py watch-ci --bug-id {bug_id} --pr-url {pr_url}"
    return f"{prefix}python scripts/aistock_issue_workflow.py run --bug-id {bug_id} --mode plan"


def _execute_checked(args: list[str], *, cwd: Path | None = None, timeout: int = 120) -> dict[str, Any]:
    result = _run_command(args, cwd=cwd, timeout=timeout)
    if not result.get("ok"):
        raise WorkflowError(result.get("stderr") or result.get("stdout") or f"command failed: {' '.join(args)}")
    return result


def _is_reparse_or_symlink(path: Path) -> bool:
    try:
        if path.is_symlink():
            return True
        is_junction = getattr(path, "is_junction", None)
        if callable(is_junction) and is_junction():
            return True
        attrs = getattr(path.stat(follow_symlinks=False), "st_file_attributes", 0)
        return bool(attrs & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0))
    except OSError:
        return False


def _orphan_worktree_dir_profile(path: Path) -> dict[str, Any]:
    sample_limit = 20
    profile: dict[str, Any] = {
        "path": str(path),
        "inside_worktree_root": _is_inside(path, _default_worktree_root()) if path.exists() else False,
        "regular_entries": [],
        "reparse_entries": [],
        "empty_dirs": [],
        "regular_entry_count": 0,
        "reparse_entry_count": 0,
        "empty_dir_count": 0,
        "sample_limit": sample_limit,
        "top_regular_dirs": {},
        "missing": not path.exists(),
    }
    if not path.exists():
        profile["safe_reparse_or_empty_only"] = True
        return profile

    def add_sample(key: str, rel: str) -> None:
        count_key = {
            "regular_entries": "regular_entry_count",
            "reparse_entries": "reparse_entry_count",
            "empty_dirs": "empty_dir_count",
        }[key]
        profile[count_key] += 1
        if len(profile[key]) < sample_limit:
            profile[key].append(rel)
        if key == "regular_entries":
            top = rel.split("/", 1)[0] if "/" in rel else rel
            top_dirs = profile["top_regular_dirs"]
            top_dirs[top] = int(top_dirs.get(top, 0)) + 1

    def scan_dir(directory: Path) -> None:
        try:
            children = sorted(directory.iterdir(), key=lambda item: item.as_posix())
        except OSError:
            add_sample("regular_entries", _repo_rel(directory, path))
            return
        if not children and directory != path:
            add_sample("empty_dirs", _repo_rel(directory, path))
            return
        for child in children:
            rel = _repo_rel(child, path)
            if _is_reparse_or_symlink(child):
                add_sample("reparse_entries", rel)
            elif child.is_dir():
                scan_dir(child)
            else:
                add_sample("regular_entries", rel)

    scan_dir(path)
    profile["regular_entries_truncated"] = profile["regular_entry_count"] > len(profile["regular_entries"])
    profile["reparse_entries_truncated"] = profile["reparse_entry_count"] > len(profile["reparse_entries"])
    profile["empty_dirs_truncated"] = profile["empty_dir_count"] > len(profile["empty_dirs"])
    profile["top_regular_dirs"] = dict(
        sorted(
            profile["top_regular_dirs"].items(),
            key=lambda item: (-int(item[1]), str(item[0])),
        )[:10]
    )
    profile["safe_reparse_or_empty_only"] = bool(profile["inside_worktree_root"]) and not profile["regular_entry_count"]
    return profile


def _orphan_worktree_refusal_message(profile: dict[str, Any]) -> str:
    return (
        "refusing orphan worktree cleanup with regular files: "
        f"count={profile.get('regular_entry_count', 0)} "
        f"samples={profile.get('regular_entries') or []} "
        f"top_dirs={profile.get('top_regular_dirs') or {}} "
        "full file list intentionally omitted; close open processes or delete the orphan directory manually"
    )


def _remove_reparse_or_empty_tree(path: Path) -> dict[str, Any]:
    profile = _orphan_worktree_dir_profile(path)
    if not profile.get("safe_reparse_or_empty_only"):
        raise WorkflowError(_orphan_worktree_refusal_message(profile))
    removed: list[str] = []
    if not path.exists():
        return {"ok": True, "returncode": 0, "stdout": "", "stderr": "", "profile": profile, "removed": removed}

    def remove_children(directory: Path) -> None:
        for child in sorted(directory.iterdir(), key=lambda item: item.as_posix(), reverse=True):
            if _is_reparse_or_symlink(child):
                if child.is_dir():
                    child.rmdir()
                else:
                    child.unlink()
                removed.append(_repo_rel(child, path))
            elif child.is_dir():
                remove_children(child)
                try:
                    child.rmdir()
                    removed.append(_repo_rel(child, path))
                except OSError:
                    pass

    remove_children(path)
    try:
        path.rmdir()
        removed.append(".")
    except PermissionError as exc:
        return {
            "ok": True,
            "returncode": 0,
            "stdout": "",
            "stderr": str(exc),
            "profile": profile,
            "removed": removed,
            "deferred": True,
            "deferred_reason": "empty_directory_locked_by_windows_handle",
        }
    return {
        "ok": True,
        "returncode": 0,
        "stdout": json.dumps({"removed": removed}, ensure_ascii=True),
        "stderr": "",
        "profile": profile,
        "removed": removed,
    }
def _looks_like_reparse_cleanup_failure(result: dict[str, Any]) -> bool:
    text = f"{result.get('stdout') or ''}\n{result.get('stderr') or ''}".lower()
    return any(token in text for token in ("invalid argument", "junction", "reparse", "node_modules"))


def _remove_worktree_with_reparse_fallback(*, root: Path, worktree_path: Path) -> dict[str, Any]:
    result = _run_command(["git", "worktree", "remove", str(worktree_path)], cwd=root, timeout=120)
    payload: dict[str, Any] = {
        "ok": bool(result.get("ok")),
        "primary": result,
        "fallback_used": False,
    }
    if result.get("ok") or not worktree_path.exists():
        return payload
    if not _looks_like_reparse_cleanup_failure(result):
        raise WorkflowError(result.get("stderr") or result.get("stdout") or f"git worktree remove failed: {worktree_path}")
    fallback = _remove_reparse_or_empty_tree(worktree_path)
    prune = _run_command(["git", "worktree", "prune"], cwd=root, timeout=60)
    payload.update(
        {
            "ok": not worktree_path.exists(),
            "fallback_used": True,
            "fallback_reason": "git_worktree_remove_left_reparse_or_empty_tree",
            "fallback": fallback,
            "prune": prune,
        }
    )
    if not payload["ok"]:
        raise WorkflowError(f"worktree reparse fallback did not remove {worktree_path}")
    return payload


def _check_name(item: dict[str, Any]) -> str:
    return str(item.get("name") or item.get("workflowName") or item.get("__typename") or "unknown")


def _classify_pr_checks(checks: list[dict[str, Any]]) -> dict[str, list[str]]:
    failed: list[str] = []
    pending: list[str] = []
    non_blocking: list[str] = []
    passed: list[str] = []
    for item in checks:
        name = _check_name(item)
        status = str(item.get("status") or "").upper()
        conclusion = str(item.get("conclusion") or "").upper()
        if status != "COMPLETED":
            pending.append(name)
        elif conclusion in NON_BLOCKING_CHECK_CONCLUSIONS:
            passed.append(name)
            if conclusion in {"NEUTRAL", "SKIPPED"}:
                non_blocking.append(name)
        elif conclusion:
            failed.append(name)
    return {
        "failed": failed,
        "pending": pending,
        "non_blocking": non_blocking,
        "passed": passed,
    }


def _execute_workflow_command(
    bug_id: str,
    args: list[str],
    *,
    state: str,
    cwd: Path | None = None,
    timeout: int = 120,
    event: str | None = None,
    root: Path | None = None,
    allow_failure: bool = False,
) -> dict[str, Any]:
    started = time.monotonic()
    result = _run_command(args, cwd=cwd, timeout=timeout)
    duration = time.monotonic() - started
    _append_event(
        bug_id,
        event=event or f"command:{args[0]}",
        state=state,
        command=" ".join(args),
        cwd=cwd,
        root=root,
        duration_seconds=duration,
        result="ok" if result.get("ok") else "failed",
        evidence={
            "returncode": result.get("returncode"),
            "stdout_excerpt": str(result.get("stdout") or "")[:1000],
            "stderr_excerpt": str(result.get("stderr") or "")[:1000],
        },
    )
    if not result.get("ok") and not allow_failure:
        raise WorkflowError(result.get("stderr") or result.get("stdout") or f"command failed: {' '.join(args)}")
    return result


def _path_is_artifact(path: str) -> bool:
    normalized = path.replace("\\", "/").strip("/")
    parts = [part for part in normalized.split("/") if part]
    return any(pattern in parts or normalized == pattern or normalized.startswith(pattern + "/") for pattern in ARTIFACT_PATH_PATTERNS)


def _git_status_paths(root: Path) -> list[dict[str, str]]:
    result = _run_command(["git", "status", "--porcelain=v1", "-uall"], cwd=root, timeout=30)
    if not result.get("ok"):
        return []
    rows: list[dict[str, str]] = []
    for line in str(result.get("stdout") or "").splitlines():
        if len(line) < 4:
            continue
        status = line[:2]
        raw_path = _parse_git_porcelain_path(line)
        rows.append({"status": status, "path": raw_path})
    return rows


def _run_changed_file_lint(changed_files: list[str], *, root: Path) -> dict[str, Any]:
    python_files = [path for path in changed_files if path.endswith(".py") and (root / path).exists()]
    if not python_files:
        return {"status": "not_required", "python_files": [], "commands": []}
    candidates = [
        ["ruff", "check", *python_files],
        [sys.executable, "-m", "ruff", "check", *python_files],
    ]
    commands: list[dict[str, Any]] = []
    for command in candidates:
        result = _run_command(command, cwd=root, timeout=120)
        commands.append({"command": " ".join(command), "result": result})
        if result.get("ok"):
            return {"status": "passed", "python_files": python_files, "commands": commands}
        combined = f"{result.get('stdout')}\n{result.get('stderr')}".lower()
        if "no module named ruff" in combined or "not recognized" in combined or "no such file" in combined:
            continue
        return {"status": "failed", "python_files": python_files, "commands": commands}
    return {"status": "unavailable", "python_files": python_files, "commands": commands}


def _pre_pr_gate(
    *,
    finish: dict[str, Any],
    validation_evidence: list[str],
    root: Path,
    run_lint: bool = True,
    require_clean: bool = False,
) -> dict[str, Any]:
    changed_files = [str(item) for item in finish.get("changed_files") or []]
    scope_check = finish.get("scope_check") or {}
    fast_path = finish.get("fast_path") if isinstance(finish.get("fast_path"), dict) else {}
    ownership = fast_path.get("ownership") if isinstance(fast_path.get("ownership"), dict) else {}
    status_rows = _git_status_paths(root)
    artifact_rows = [row for row in status_rows if _path_is_artifact(row["path"])]
    task_dirty_rows = [row for row in status_rows if not _path_is_artifact(row["path"])]
    blocking: list[str] = []
    warnings: list[str] = []
    next_actions: list[str] = []
    if require_clean and task_dirty_rows:
        blocking.append(f"commit_required before push/PR: {[row['path'] for row in task_dirty_rows]}")
        next_actions.append('git add <task files> && git commit -m "fix: resolve issue"')
    elif task_dirty_rows:
        warnings.append(f"uncommitted task file(s) present: {[row['path'] for row in task_dirty_rows]}")
    if not validation_evidence:
        blocking.append("validation evidence is required before PR creation")
    if scope_check.get("status") not in {None, "passed"}:
        blocking.append(f"scope check failed: {scope_check.get('violations') or scope_check.get('status')}")
    if ownership.get("unmapped_count"):
        blocking.append(f"ownership check failed: unmapped={ownership.get('unmapped') or ownership.get('unmapped_count')}")
    if ownership.get("ambiguous_count"):
        blocking.append(f"ownership check failed: ambiguous={ownership.get('ambiguous') or ownership.get('ambiguous_count')}")
    if artifact_rows:
        blocking.append(f"temporary/cache artifacts are present in git status: {[row['path'] for row in artifact_rows]}")
    lint = _run_changed_file_lint(changed_files, root=root) if run_lint else {"status": "skipped", "python_files": []}
    if lint.get("status") == "failed":
        blocking.append("changed-file Ruff lint failed")
    elif lint.get("status") == "unavailable" and lint.get("python_files"):
        warnings.append("Ruff is unavailable; run the required nox/pytest validation before PR")
    return {
        "schema_version": "aistock_issue_workflow_pre_pr_gate_v1",
        "generated_at": _utc_now(),
        "workflow_gate": "passed" if not blocking else "blocked",
        "blocking": blocking,
        "warnings": warnings,
        "changed_files": changed_files,
        "scope_check": scope_check,
        "ownership_check": ownership,
        "artifact_guard": {
            "status": "passed" if not artifact_rows else "failed",
            "artifact_paths": artifact_rows,
            "patterns": list(ARTIFACT_PATH_PATTERNS),
        },
        "dirty_task_files": task_dirty_rows,
        "next_actions": next_actions,
        "lint": lint,
        "validation_evidence_present": bool(validation_evidence),
    }


def _current_branch(root: Path | None = None) -> str:
    branch = _git(["branch", "--show-current"], cwd=root or REPO_ROOT)
    if not branch:
        raise WorkflowError("current branch is empty; cannot create issue PR")
    if branch == "main":
        raise WorkflowError("refusing to create an issue PR from main")
    return branch


def _pr_worktree_guard(root: Path | None = None) -> dict[str, Any]:
    work_root = root or REPO_ROOT
    canonical = _canonical_root().resolve()
    git = _git_snapshot(work_root)
    blocking: list[str] = []
    warnings: list[str] = []
    target = work_root.resolve()
    if not git.get("ok"):
        blocking.append(str(git.get("error") or f"not a git checkout: {work_root}"))
    if target == canonical:
        blocking.append("refusing PR automation from canonical root; continue in the issue worktree")
    if git.get("branch") == "main":
        blocking.append("refusing PR automation from main; continue on the task branch")
    if git.get("dirty"):
        warnings.append(f"task worktree has {git.get('dirty_count')} uncommitted file(s); commit only task files before push/PR")
    return {
        "root": str(target),
        "canonical_root": str(canonical),
        "git": git,
        "blocking": blocking,
        "warnings": warnings,
    }


def _checks_summary_payload(checks_view: dict[str, list[str]]) -> dict[str, Any]:
    return {
        "failed_count": len(checks_view.get("failed") or []),
        "pending_count": len(checks_view.get("pending") or []),
        "passed_count": len(checks_view.get("passed") or []),
        "non_blocking_count": len(checks_view.get("non_blocking") or []),
        "failed": list(checks_view.get("failed") or [])[:5],
        "pending": list(checks_view.get("pending") or [])[:5],
        "non_blocking": list(checks_view.get("non_blocking") or [])[:5],
    }


def _view_pr_checks(pr_url: str) -> dict[str, Any]:
    view = _run_command(["gh", "pr", "view", pr_url, "--json", "statusCheckRollup"], cwd=REPO_ROOT, timeout=60)
    if not view.get("ok"):
        return {"workflow_gate": "checks_unavailable", "check_summary": {"failed_count": 0, "pending_count": 0, "passed_count": 0, "non_blocking_count": 0}, "error": view.get("stderr") or view.get("stdout")}
    try:
        checks = json.loads(str(view.get("stdout") or "{}")).get("statusCheckRollup") or []
    except json.JSONDecodeError as exc:
        return {"workflow_gate": "checks_unavailable", "check_summary": {"failed_count": 0, "pending_count": 0, "passed_count": 0, "non_blocking_count": 0}, "error": str(exc)}
    classified = _classify_pr_checks(checks)
    if not checks:
        gate = "checks_pending"
    elif classified["failed"]:
        gate = "checks_failed"
    elif classified["pending"]:
        gate = "checks_pending"
    else:
        gate = "checks_passed"
    return {"workflow_gate": gate, "check_summary": _checks_summary_payload(classified), "classified": classified, "raw_count": len(checks)}


def _watch_pr_checks_compact(bug_id: str, pr_url: str, *, attempts: int = 3, delay_seconds: int = 10) -> dict[str, Any]:
    attempts_payload: list[dict[str, Any]] = []
    latest = {"workflow_gate": "checks_pending", "check_summary": {"failed_count": 0, "pending_count": 0, "passed_count": 0, "non_blocking_count": 0}}
    for index in range(1, attempts + 1):
        latest = _view_pr_checks(pr_url)
        attempts_payload.append({"attempt": index, "workflow_gate": latest.get("workflow_gate"), "check_summary": latest.get("check_summary")})
        if latest.get("workflow_gate") in {"checks_passed", "checks_failed"}:
            break
        if index < attempts:
            time.sleep(delay_seconds)
    gate = str(latest.get("workflow_gate") or "checks_pending")
    next_actions = ["merge_only_if_user_authorized"] if gate == "checks_passed" else ["rerun_checks_after_github_reports_jobs"]
    if gate == "checks_failed":
        next_actions = ["inspect_failed_ci_jobs", "fix_on_same_task_branch"]
    payload = {
        "schema_version": "aistock_issue_workflow_check_watch_v1",
        "workflow_gate": gate,
        "pr_url": pr_url,
        "attempts": attempts_payload,
        "check_summary": latest.get("check_summary"),
        "next_actions": next_actions,
    }
    _append_event(
        bug_id,
        event="command:gh_pr_checks_compact",
        state="ci_running" if gate == "checks_pending" else ("ci_green" if gate == "checks_passed" else "blocked"),
        command=f"gh pr view {pr_url} --json statusCheckRollup",
        cwd=REPO_ROOT,
        result=gate,
        evidence={"check_summary": payload["check_summary"]},
    )
    return payload


def build_watch_ci_plan(
    *,
    bug_id: str,
    pr_url: str | None = None,
    attempts: int = 1,
    delay_seconds: int = 0,
) -> dict[str, Any]:
    canonical_bug_id = bug_id.strip().upper()
    state = _load_state(canonical_bug_id) or {}
    resolved_pr_url = pr_url or str(state.get("pr_url") or "").strip()
    if not resolved_pr_url:
        raise WorkflowError("--pr-url is required when workflow state does not include a PR URL")
    attempts = max(1, attempts)
    delay_seconds = max(0, delay_seconds)
    ci_watch = _watch_pr_checks_compact(
        canonical_bug_id,
        resolved_pr_url,
        attempts=attempts,
        delay_seconds=delay_seconds,
    )
    gate = str(ci_watch.get("workflow_gate") or "checks_pending")
    check_ok = gate == "checks_passed"
    _write_state(
        canonical_bug_id,
        state="ci_green" if check_ok else ("blocked" if gate == "checks_failed" else "ci_running"),
        pr_url=resolved_pr_url,
        checks=ci_watch.get("check_summary"),
        next_actions=ci_watch.get("next_actions") or [],
        stop_reason=None if check_ok else gate,
    )
    return {
        "schema_version": "aistock_issue_workflow_watch_ci_v1",
        "generated_at": _utc_now(),
        "bug_id": canonical_bug_id,
        "pr_url": resolved_pr_url,
        "workflow_gate": gate,
        "check_summary": ci_watch.get("check_summary"),
        "next_actions": ci_watch.get("next_actions") or [],
        "state": "ci_green" if check_ok else ("blocked" if gate == "checks_failed" else "ci_running"),
    }


def _maybe_create_pr(
    *,
    bug_id: str,
    finish: dict[str, Any],
    push: bool,
    create_pr: bool,
    watch_ci: bool,
    pr_title: str | None,
) -> dict[str, Any]:
    actions: list[dict[str, Any]] = []
    pr_url: str | None = None
    guard = _pr_worktree_guard()
    if guard["blocking"]:
        raise WorkflowError("; ".join(guard["blocking"]))
    branch = _current_branch()
    pre_pr_gate = _pre_pr_gate(
        finish=finish,
        validation_evidence=finish.get("validation_evidence") or [],
        root=REPO_ROOT,
        require_clean=bool(push or create_pr),
    )
    if not (push or create_pr or watch_ci):
        return {
            "branch": branch,
            "dry_run": True,
            "worktree_guard": guard,
            "pre_pr_gate": pre_pr_gate,
            "next_commands": [
                f"git push -u origin {branch}",
                f"gh pr create --base main --head {branch} --title \"{pr_title or bug_id + ' issue workflow fix'}\" --body-file {finish.get('pr_body_path')}",
            ],
        }
    if not finish.get("validation_evidence"):
        raise WorkflowError("validation evidence is required before push/create-pr automation")
    if pre_pr_gate["workflow_gate"] != "passed":
        raise WorkflowError("; ".join(pre_pr_gate["blocking"]))
    if push:
        actions.append(
            {
                "command": f"git push -u origin {branch}",
                "result": _execute_workflow_command(
                    bug_id,
                    ["git", "push", "-u", "origin", branch],
                    state="pushed",
                    cwd=REPO_ROOT,
                    timeout=180,
                    event="command:git_push",
                ),
            }
        )
        _write_state(bug_id, state="pushed", branch=branch, next_actions=["create_pr_from_pr_body"])
    if create_pr:
        title = pr_title or f"{bug_id} issue workflow fix"
        body_path = str(REPO_ROOT / str(finish.get("pr_body_path")))
        result = _execute_workflow_command(
            bug_id,
            ["gh", "pr", "create", "--base", "main", "--head", branch, "--title", title, "--body-file", body_path],
            state="pr_opened",
            cwd=REPO_ROOT,
            timeout=120,
            event="command:gh_pr_create",
        )
        pr_url = str(result.get("stdout") or "").splitlines()[-1].strip()
        actions.append({"command": "gh pr create", "result": result})
        _write_state(bug_id, state="pr_opened", branch=branch, pr_url=pr_url, next_actions=["watch_ci_before_merge"])
    ci_watch: dict[str, Any] | None = None
    if watch_ci:
        if not pr_url:
            raise WorkflowError("--watch-ci requires --create-pr in this Phase 1 wrapper")
        ci_watch = _watch_pr_checks_compact(bug_id, pr_url)
        actions.append({"command": "gh pr view --json statusCheckRollup", "result": {"ok": True, "summary": ci_watch.get("check_summary")}})
        check_ok = ci_watch.get("workflow_gate") == "checks_passed"
        _write_state(
            bug_id,
            state="ci_green" if check_ok else "ci_running",
            branch=branch,
            pr_url=pr_url,
            checks=ci_watch.get("check_summary"),
            next_actions=ci_watch.get("next_actions") or [],
            stop_reason=None if check_ok else str(ci_watch.get("workflow_gate") or "ci_not_green"),
        )
    result_payload = {"branch": branch, "dry_run": False, "pr_url": pr_url, "actions": actions, "worktree_guard": guard, "pre_pr_gate": pre_pr_gate}
    if ci_watch is not None:
        result_payload["ci_watch"] = ci_watch
    return result_payload


def _verify_pr_merged(pr_url: str, *, skip_github_check: bool = False) -> dict[str, Any]:
    if skip_github_check:
        return {"checked": False, "merged": True, "reason": "skip_github_check"}
    result = _run_command(
        ["gh", "pr", "view", pr_url, "--json", "state,mergedAt,mergeCommit,url,headRefName,headRefOid"],
        cwd=REPO_ROOT,
        timeout=30,
    )
    if not result.get("ok"):
        raise WorkflowError(result.get("stderr") or result.get("stdout") or f"cannot inspect PR: {pr_url}")
    try:
        payload = json.loads(str(result.get("stdout") or "{}"))
    except json.JSONDecodeError as exc:
        raise WorkflowError(f"cannot parse gh pr view output for {pr_url}: {exc}") from exc
    merged = payload.get("state") == "MERGED" or bool(payload.get("mergedAt"))
    if not merged:
        raise WorkflowError(f"PR is not merged: {pr_url}")
    return {"checked": True, "merged": True, "pr": payload}


def _merge_commit_from_pr_check(pr_check: dict[str, Any] | None) -> str | None:
    pr = (pr_check or {}).get("pr") or {}
    merge_commit = pr.get("mergeCommit") if isinstance(pr, dict) else None
    if isinstance(merge_commit, dict):
        return str(merge_commit.get("oid") or "") or None
    return str(merge_commit or "") or None


def _pr_head_oid_from_pr_check(pr_check: dict[str, Any] | None) -> str | None:
    pr = (pr_check or {}).get("pr") or {}
    if not isinstance(pr, dict):
        return None
    head_ref_oid = str(pr.get("headRefOid") or "").strip()
    return head_ref_oid or None


def _git_ref_exists(ref: str, *, cwd: Path | None = None) -> bool:
    if not ref:
        return False
    return bool(_run_command(["git", "rev-parse", "--verify", "--quiet", ref], cwd=cwd or REPO_ROOT).get("ok"))


def _registered_worktree_paths(*, cwd: Path | None = None) -> set[Path]:
    result = _run_command(["git", "worktree", "list", "--porcelain"], cwd=cwd or REPO_ROOT, timeout=30)
    if not result.get("ok"):
        return set()
    paths: set[Path] = set()
    for line in str(result.get("stdout") or "").splitlines():
        if not line.startswith("worktree "):
            continue
        raw_path = line.removeprefix("worktree ").strip()
        if not raw_path:
            continue
        try:
            paths.add(Path(raw_path).resolve())
        except OSError:
            paths.add(Path(raw_path))
    return paths


def _path_is_registered_worktree(path: Path, *, cwd: Path | None = None) -> bool:
    try:
        target = path.resolve()
    except OSError:
        target = path
    return target in _registered_worktree_paths(cwd=cwd)


def _registered_worktree_for_branch(branch: str, *, cwd: Path | None = None) -> Path | None:
    if not branch:
        return None
    result = _run_command(["git", "worktree", "list", "--porcelain"], cwd=cwd or REPO_ROOT, timeout=30)
    if not result.get("ok"):
        return None
    current: Path | None = None
    expected_ref = f"refs/heads/{branch}"
    for line in str(result.get("stdout") or "").splitlines():
        if line.startswith("worktree "):
            raw_path = line.removeprefix("worktree ").strip()
            current = Path(raw_path) if raw_path else None
            continue
        if line.startswith("branch ") and current:
            ref = line.removeprefix("branch ").strip()
            if ref in {branch, expected_ref}:
                return current
    return None


def _git_refs_tree_equivalent(left: str, right: str, *, cwd: Path | None = None) -> bool:
    if not left or not right:
        return False
    root = cwd or REPO_ROOT
    if not _git_ref_exists(left, cwd=root) or not _git_ref_exists(right, cwd=root):
        return False
    return bool(_run_command(["git", "diff", "--quiet", left, right], cwd=root).get("ok"))


def _git_changed_files(base: str, head: str, *, cwd: Path | None = None) -> list[str]:
    if not base or not head:
        return []
    result = _run_command(["git", "diff", "--name-only", base, head], cwd=cwd or REPO_ROOT)
    if not result.get("ok"):
        return []
    return [line.strip() for line in str(result.get("stdout") or "").splitlines() if line.strip()]


def _git_merge_base(left: str, right: str, *, cwd: Path | None = None) -> str | None:
    result = _run_command(["git", "merge-base", left, right], cwd=cwd or REPO_ROOT)
    if not result.get("ok"):
        return None
    value = str(result.get("stdout") or "").strip()
    return value or None


def _git_paths_equivalent(left: str, right: str, paths: list[str], *, cwd: Path | None = None) -> bool:
    if not left or not right or not paths:
        return False
    root = cwd or REPO_ROOT
    if not _git_ref_exists(left, cwd=root) or not _git_ref_exists(right, cwd=root):
        return False
    return bool(_run_command(["git", "diff", "--quiet", left, right, "--", *paths], cwd=root).get("ok"))


def _git_squash_head_equivalent_to_ref(
    head_oid: str,
    target_ref: str,
    *,
    target_label: str | None = None,
    cwd: Path | None = None,
) -> dict[str, Any]:
    root = cwd or REPO_ROOT
    base_ref = target_label or target_ref
    payload: dict[str, Any] = {
        "head_ref": head_oid,
        "base_ref": base_ref,
        "target_ref": target_ref,
        "base": None,
        "changed_files": [],
        "verified": False,
        "reason": None,
    }
    if not _git_ref_exists(head_oid, cwd=root) or not _git_ref_exists(target_ref, cwd=root):
        payload["reason"] = "missing_ref"
        return payload
    if _git_refs_tree_equivalent(head_oid, target_ref, cwd=root):
        payload["verified"] = True
        payload["reason"] = "full_tree_equivalent"
        return payload
    base = _git_merge_base(head_oid, target_ref, cwd=root)
    payload["base"] = base
    if not base:
        payload["reason"] = "missing_merge_base"
        return payload
    changed_files = _git_changed_files(base, head_oid, cwd=root)
    payload["changed_files"] = changed_files
    if not changed_files:
        payload["reason"] = "empty_pr_diff"
        return payload
    payload["verified"] = _git_paths_equivalent(head_oid, target_ref, changed_files, cwd=root)
    payload["reason"] = "changed_paths_equivalent" if payload["verified"] else "changed_paths_differ"
    return payload


def _git_squash_head_equivalent_to_origin(head_oid: str, *, cwd: Path | None = None) -> dict[str, Any]:
    return _git_squash_head_equivalent_to_ref(head_oid, "origin/main", cwd=cwd)


def _cleanup_merge_verification(
    branch: str,
    pr_url: str | None,
    merged: bool,
    *,
    cwd: Path | None = None,
) -> dict[str, Any]:
    root = cwd or REPO_ROOT
    payload: dict[str, Any] = {
        "method": "git_merged_branch" if merged else None,
        "verified": bool(merged),
        "squash_merge_verified": False,
        "tree_equivalent_to_origin_main": bool(merged),
        "tree_equivalence_ref": "branch" if merged else None,
        "pr_check": None,
        "path_equivalence": None,
        "merge_commit_path_equivalence": None,
        "origin_path_equivalence": None,
    }
    if merged or not pr_url:
        return payload

    pr_check = _verify_pr_merged(pr_url)
    payload["pr_check"] = pr_check
    if not pr_check.get("merged"):
        return payload

    head_oid = _pr_head_oid_from_pr_check(pr_check)
    merge_commit = _merge_commit_from_pr_check(pr_check)
    if head_oid and merge_commit:
        merge_commit_equivalence = _git_squash_head_equivalent_to_ref(
            head_oid,
            merge_commit,
            target_label="source_pr_merge_commit",
            cwd=root,
        )
    else:
        merge_commit_equivalence = {"verified": False, "reason": "missing_merge_commit" if head_oid else "missing_head_oid"}
    payload["merge_commit_path_equivalence"] = merge_commit_equivalence
    if head_oid and merge_commit_equivalence.get("verified"):
        payload["path_equivalence"] = merge_commit_equivalence
        payload.update(
            {
                "method": f"squash_merge_head_oid_{merge_commit_equivalence.get('reason')}_to_merge_commit",
                "verified": True,
                "squash_merge_verified": True,
                "tree_equivalent_to_origin_main": False,
                "tree_equivalence_ref": head_oid,
                "tree_equivalence_target": merge_commit,
            }
        )
        return payload

    head_equivalence = _git_squash_head_equivalent_to_origin(head_oid, cwd=root) if head_oid else {"verified": False, "reason": "missing_head_oid"}
    payload["origin_path_equivalence"] = head_equivalence
    payload["path_equivalence"] = head_equivalence
    if head_oid and head_equivalence.get("verified"):
        payload.update(
            {
                "method": f"squash_merge_head_oid_{head_equivalence.get('reason')}",
                "verified": True,
                "squash_merge_verified": True,
                "tree_equivalent_to_origin_main": head_equivalence.get("reason") == "full_tree_equivalent",
                "tree_equivalence_ref": head_oid,
            }
        )
        return payload

    if _git_ref_exists(branch, cwd=root) and _git_refs_tree_equivalent(branch, "origin/main", cwd=root):
        payload.update(
            {
                "method": "squash_merge_branch_tree_equivalent",
                "verified": True,
                "squash_merge_verified": True,
                "tree_equivalent_to_origin_main": True,
                "tree_equivalence_ref": branch,
            }
        )
    return payload


def _cleanup_preflight_fetch_origin(root: Path, *, apply: bool) -> dict[str, Any]:
    if not apply:
        return {"status": "skipped", "reason": "dry_run"}
    result = _run_command(["git", "fetch", "origin", "--prune"], cwd=root, timeout=120)
    return {
        "status": "fetched" if result.get("ok") else "failed",
        "command": "git fetch origin --prune",
        "result": result,
    }


def _canonical_bug_record_snapshot(bug_id: str, root: Path | None = None) -> dict[str, Any]:
    canonical_root = root or _canonical_root()
    payload: dict[str, Any] = {
        "bug_id": bug_id,
        "canonical_root": str(canonical_root),
        "persisted": False,
        "path": None,
        "status": None,
    }
    bugs_root = _bugs_root(canonical_root)
    if not bugs_root.exists():
        payload["reason"] = "bugs_root_missing"
        return payload
    for path in _bug_files(bugs_root):
        try:
            record = _load_json(path)
        except WorkflowError:
            continue
        if str(record.get("bug_id") or "").strip().upper() != bug_id:
            continue
        payload.update(
            {
                "persisted": True,
                "path": _repo_rel(path, canonical_root),
                "status": record.get("status"),
                "github_issue_number": record.get("github_issue_number"),
                "github_issue_url": record.get("github_issue_url"),
            }
        )
        return payload
    payload["reason"] = "bug_record_missing"
    return payload


def build_registry_intake_cleanup_plan(
    *,
    bug_id: str,
    apply: bool = False,
    canonical_root: str | None = None,
) -> dict[str, Any]:
    canonical_bug_id = bug_id.strip().upper()
    root = Path(canonical_root) if canonical_root else _canonical_root()
    persisted = _canonical_bug_record_snapshot(canonical_bug_id, root)
    current_cwd = Path.cwd().resolve()
    local_branches = set(_git(["for-each-ref", "--format=%(refname:short)", "refs/heads"], check=False).splitlines())
    merged_refs = set(_git(["branch", "--format=%(refname:short)", "--merged", "origin/main"], check=False).splitlines())
    candidates: list[dict[str, Any]] = []
    warnings: list[str] = []

    for item in _active_workflows_for_bug(canonical_bug_id):
        if not str(item.get("workflow_role") or "").startswith("registry"):
            continue
        worktree_path = Path(str(item.get("worktree") or item.get("root") or ""))
        branch = str(item.get("branch") or "").strip()
        exists = bool(str(worktree_path)) and worktree_path.exists()
        git = item.get("git") if isinstance(item.get("git"), dict) else (_git_snapshot(worktree_path) if exists else {})
        dirty = bool(git.get("dirty")) if git else True
        is_current_cwd = exists and worktree_path.resolve() == current_cwd
        remote_ref = _git(["ls-remote", "--heads", "origin", branch], check=False) if branch else ""
        safe = bool(persisted.get("persisted")) and exists and not dirty and not is_current_cwd
        reason = None
        if not persisted.get("persisted"):
            reason = "canonical_bug_record_missing"
        elif not exists:
            reason = "worktree_missing"
        elif dirty:
            reason = "worktree_dirty"
        elif is_current_cwd:
            reason = "refusing_current_cwd"
        actions = []
        if exists:
            actions.append({"action": "remove_worktree", "worktree": str(worktree_path), "safe": safe})
        if branch and branch in local_branches:
            actions.append(
                {
                    "action": "delete_local_branch",
                    "branch": branch,
                    "safe": safe,
                    "delete_flag": "-d" if branch in merged_refs else "-D",
                }
            )
        if branch and remote_ref:
            actions.append({"action": "delete_remote_branch", "branch": branch, "safe": safe})
        if not safe and reason:
            warnings.append(f"registry intake cleanup skipped for {worktree_path}: {reason}")
        candidates.append(
            {
                "worktree": str(worktree_path) if str(worktree_path) else None,
                "branch": branch or None,
                "issue_json": item.get("issue_json"),
                "dirty": dirty,
                "safe": safe,
                "skip_reason": None if safe else reason,
                "actions": actions,
            }
        )

    safe_candidates = [item for item in candidates if item.get("safe")]
    payload: dict[str, Any] = {
        "schema_version": "aistock_issue_workflow_registry_intake_cleanup_v1",
        "generated_at": _utc_now(),
        "bug_id": canonical_bug_id,
        "canonical_bug_record": persisted,
        "candidates": candidates,
        "warnings": warnings,
        "dry_run": not apply,
        "workflow_gate": "ready_for_cleanup" if safe_candidates else "skipped",
    }
    if not apply or not safe_candidates:
        return payload

    started = time.monotonic()
    applied: list[dict[str, Any]] = []
    for candidate in safe_candidates:
        for action in candidate.get("actions") or []:
            if not action.get("safe"):
                continue
            if action["action"] == "remove_worktree":
                worktree_path = Path(str(action["worktree"]))
                applied.append(
                    {
                        "command": f"git worktree remove {action['worktree']}",
                        "result": _remove_worktree_with_reparse_fallback(root=REPO_ROOT, worktree_path=worktree_path),
                    }
                )
            elif action["action"] == "delete_local_branch":
                flag = str(action.get("delete_flag") or "-d")
                applied.append(
                    {
                        "command": f"git branch {flag} {action['branch']}",
                        "result": _execute_checked(["git", "branch", flag, str(action["branch"])], cwd=REPO_ROOT, timeout=120),
                    }
                )
            elif action["action"] == "delete_remote_branch":
                applied.append(
                    {
                        "command": f"git push origin --delete {action['branch']}",
                        "result": _execute_checked(["git", "push", "origin", "--delete", str(action["branch"])], cwd=REPO_ROOT, timeout=180),
                    }
                )
    payload["applied"] = applied
    payload["duration_seconds"] = round(time.monotonic() - started, 3)
    payload["workflow_gate"] = "cleanup_done"
    payload["dry_run"] = False
    return payload


def _sync_github_issue_after_close(
    record: dict[str, Any],
    evidence_payload: dict[str, Any],
    *,
    root: Path | None = None,
) -> dict[str, Any]:
    root = root or REPO_ROOT
    issue_number = record.get("github_issue_number")
    if not issue_number:
        return {"status": "skipped_missing_issue_number"}
    lines = [
        f"AIstock workflow close-sync persisted to the current registry worktree for `{record.get('bug_id')}`.",
        "",
        f"- PR: {evidence_payload.get('merged_pr') or 'n/a'}",
        f"- Merge commit: `{evidence_payload.get('merge_commit') or 'unknown'}`",
        "- BUG JSON status: `fixed`",
        "- Note: this PR persists registry close-sync metadata; final completion requires this PR to merge into `origin/main`.",
        "",
        "Validation evidence:",
        *[f"- {item}" for item in evidence_payload.get("validation_evidence") or ["n/a"]],
        "",
        "Production gates:",
        *[
            f"- {key}: `{value}`"
            for key, value in sorted((evidence_payload.get("production_gates") or {}).items())
        ],
    ]
    tmp_comment = root / WORKFLOW_ROOT / str(record.get("bug_id") or issue_number) / "github-close-comment.md"
    _write_text(tmp_comment, "\n".join(lines))
    comment = _run_command(
        ["gh", "issue", "comment", str(issue_number), "--repo", GITHUB_REPO, "--body-file", str(tmp_comment)],
        cwd=root,
        timeout=60,
    )
    close = _run_command(["gh", "issue", "close", str(issue_number), "--repo", GITHUB_REPO], cwd=root, timeout=60)
    return {
        "status": "synced" if comment.get("ok") and close.get("ok") else "warning",
        "comment": comment,
        "close": close,
        "comment_path": _repo_rel(tmp_comment, root),
    }


def _production_gates_payload(args: argparse.Namespace | None = None) -> dict[str, str]:
    if args is None:
        return {
            "production_ddl_gate": "noop",
            "production_frontend_dependency_gate": "noop",
            "production_backend_dependency_gate": "noop",
        }
    return {
        "production_ddl_gate": args.production_ddl_gate,
        "production_frontend_dependency_gate": args.production_frontend_dependency_gate,
        "production_backend_dependency_gate": args.production_backend_dependency_gate,
    }


def _merge_pr_if_ready(pr_url: str) -> dict[str, Any]:
    view = _execute_checked(
        ["gh", "pr", "view", pr_url, "--json", "state,mergeStateStatus,statusCheckRollup,url"],
        cwd=REPO_ROOT,
        timeout=60,
    )
    payload = json.loads(str(view.get("stdout") or "{}"))
    check_summary = _classify_pr_checks(payload.get("statusCheckRollup") or [])
    failed = check_summary["failed"]
    pending = check_summary["pending"]
    if payload.get("state") == "MERGED":
        return {"already_merged": True, "view": payload}
    if failed or pending:
        raise WorkflowError(f"PR checks are not green; failed={failed}, pending={pending}")
    result = _run_command(["gh", "pr", "merge", pr_url, "--squash", "--delete-branch"], cwd=REPO_ROOT, timeout=180)
    try:
        verified = _verify_pr_merged(pr_url)
    except WorkflowError as exc:
        if not result.get("ok"):
            raise WorkflowError(
                result.get("stderr") or result.get("stdout") or f"gh pr merge failed before verification: {exc}"
            ) from exc
        raise
    if not result.get("ok"):
        return {
            "already_merged": True,
            "check_summary": check_summary,
            "merge_result": result,
            "verified": verified,
            "recovered_from_local_merge_error": True,
        }
    return {"already_merged": False, "check_summary": check_summary, "merge_result": result, "verified": verified}


def _merge_pr_if_ready_for_bug(bug_id: str, pr_url: str) -> dict[str, Any]:
    view = _execute_workflow_command(
        bug_id,
        ["gh", "pr", "view", pr_url, "--json", "state,mergeStateStatus,statusCheckRollup,url"],
        state="ci_green",
        cwd=REPO_ROOT,
        timeout=60,
        event="command:gh_pr_view_before_merge",
    )
    payload = json.loads(str(view.get("stdout") or "{}"))
    check_summary = _classify_pr_checks(payload.get("statusCheckRollup") or [])
    failed = check_summary["failed"]
    pending = check_summary["pending"]
    if payload.get("state") == "MERGED":
        return {"already_merged": True, "view": payload}
    if failed or pending:
        raise WorkflowError(f"PR checks are not green; failed={failed}, pending={pending}")
    result = _execute_workflow_command(
        bug_id,
        ["gh", "pr", "merge", pr_url, "--squash", "--delete-branch"],
        state="merged",
        cwd=REPO_ROOT,
        timeout=180,
        event="command:gh_pr_merge",
        allow_failure=True,
    )
    try:
        verified = _verify_pr_merged(pr_url)
    except WorkflowError as exc:
        if not result.get("ok"):
            raise WorkflowError(
                result.get("stderr") or result.get("stdout") or f"gh pr merge failed before verification: {exc}"
            ) from exc
        raise
    if not result.get("ok"):
        _append_event(
            bug_id,
            event="merge_remote_verified_after_local_error",
            state="merged",
            result="recovered",
            evidence={
                "pr_url": pr_url,
                "merge_error": result.get("stderr") or result.get("stdout"),
                "merge_commit": _merge_commit_from_pr_check(verified),
            },
        )
        return {
            "already_merged": True,
            "check_summary": check_summary,
            "merge_result": result,
            "verified": verified,
            "recovered_from_local_merge_error": True,
        }
    return {"already_merged": False, "check_summary": check_summary, "merge_result": result, "verified": verified}


def _close_sync_changed_files(close_sync: dict[str, Any]) -> list[str]:
    root = Path(str(close_sync.get("registry_root") or REPO_ROOT))
    updated = str(close_sync.get("updated_bug_json") or close_sync.get("source_bug_json") or "")
    files: list[str] = []
    status = _run_command(["git", "status", "--porcelain=v1", "--", "tests/aistock_validation/bugs"], cwd=root)
    if status.get("ok"):
        for line in str(status.get("stdout") or "").splitlines():
            rel = _parse_git_porcelain_path(line)
            if rel and rel.replace("\\", "/").startswith("tests/aistock_validation/bugs/"):
                files.append(rel.replace("\\", "/"))
    elif updated:
        files.append(updated.replace("\\", "/"))
    return sorted(set(files))


def _open_pr_for_branch(branch: str, *, root: Path) -> dict[str, Any] | None:
    if not branch:
        return None
    existing = _run_command(
        [
            "gh",
            "pr",
            "list",
            "--repo",
            GITHUB_REPO,
            "--head",
            branch,
            "--state",
            "open",
            "--json",
            "number,url,headRefName,title",
            "--limit",
            "1",
        ],
        cwd=root,
        timeout=60,
    )
    if not existing.get("ok"):
        return None
    try:
        rows = json.loads(str(existing.get("stdout") or "[]"))
    except json.JSONDecodeError:
        return None
    return rows[0] if isinstance(rows, list) and rows else None


def _maybe_commit_and_pr_close_sync(
    *,
    bug_id: str,
    close_sync: dict[str, Any],
    validation_evidence: list[str],
) -> dict[str, Any]:
    root = Path(str(close_sync.get("registry_root") or ""))
    branch = ((close_sync.get("registry_worktree_plan") or {}).get("branch") or "")
    if not root.exists() or not branch:
        return {"workflow_gate": "skipped", "reason": "missing_close_sync_worktree"}
    bug_ids = flow._unique_strings(flow._as_list(close_sync.get("bug_ids")) or [bug_id])
    label = str(close_sync.get("batch_id") or bug_id)
    title_label = label if len(bug_ids) > 1 else bug_id
    changed_files = _close_sync_changed_files(close_sync)
    if not changed_files:
        return {"workflow_gate": "no_changes", "root": str(root), "branch": branch}
    dirty = _dirty_files(root)
    unexpected_dirty = sorted(
        path for path in dirty if not path.replace("\\", "/").startswith("tests/aistock_validation/bugs/")
    )
    if unexpected_dirty:
        raise WorkflowError(
            "close-sync worktree has unexpected dirty files outside BUG registry: "
            + ", ".join(unexpected_dirty[:10])
        )
    existing_pr = _open_pr_for_branch(branch, root=root)
    if existing_pr:
        return {
            "workflow_gate": "pr_opened",
            "reason": "existing_open_close_sync_pr_for_branch",
            "root": str(root),
            "branch": branch,
            "changed_files": changed_files,
            "pr_url": existing_pr.get("url"),
            "open_pr": existing_pr,
            "commit": None,
        }

    started = time.monotonic()
    actions: list[dict[str, Any]] = []
    add = _run_command(["git", "add", *changed_files], cwd=root, timeout=60)
    actions.append({"command": f"git add {' '.join(changed_files)}", "result": add})
    if not add.get("ok"):
        raise WorkflowError(add.get("stderr") or add.get("stdout") or "close-sync git add failed")
    commit_message = f"chore(issue): close-sync {title_label} after merge"
    commit = _run_command(["git", "commit", "-m", commit_message], cwd=root, timeout=120)
    actions.append({"command": f"git commit -m {commit_message}", "result": commit})
    if not commit.get("ok") and "nothing to commit" not in f"{commit.get('stdout')}\n{commit.get('stderr')}".lower():
        raise WorkflowError(commit.get("stderr") or commit.get("stdout") or "close-sync git commit failed")
    commit_sha = _run_command(["git", "rev-parse", "--short=12", "HEAD"], cwd=root, timeout=30)

    push = _run_command(["git", "push", "-u", "origin", branch], cwd=root, timeout=180)
    actions.append({"command": f"git push -u origin {branch}", "result": push})
    if not push.get("ok"):
        raise WorkflowError(push.get("stderr") or push.get("stdout") or "close-sync git push failed")

    body_path = root / WORKFLOW_ROOT / label / "close-sync-pr-body.md"
    body_lines = [
        f"## {title_label} close-sync",
        "",
        f"- Source PR: {close_sync.get('merged_pr') or 'n/a'}",
        f"- Merge commit: `{close_sync.get('merge_commit') or 'unknown'}`",
        f"- BUG IDs: `{', '.join(bug_ids)}`",
        "- BUG JSON status: `fixed`",
        "- Note: this PR persists registry close-sync metadata; final completion requires this PR to merge into `origin/main`.",
        "",
        "## Validation",
        *[f"- {item}" for item in validation_evidence or close_sync.get("validation_evidence") or ["n/a"]],
        "",
        "## Production gates",
        *[f"- {key}: `{value}`" for key, value in sorted((close_sync.get("production_gates") or {}).items())],
    ]
    _write_text(body_path, "\n".join(body_lines) + "\n")
    pr = _run_command(
        [
            "gh",
            "pr",
            "create",
            "--repo",
            GITHUB_REPO,
            "--base",
            "main",
            "--head",
            branch,
            "--title",
            f"chore(issue): close-sync {title_label}",
            "--body-file",
            str(body_path),
        ],
        cwd=root,
        timeout=120,
    )
    actions.append({"command": "gh pr create close-sync", "result": pr})
    pr_url = str(pr.get("stdout") or "").splitlines()[-1].strip() if pr.get("ok") else None
    if not pr.get("ok"):
        text = f"{pr.get('stdout')}\n{pr.get('stderr')}"
        if "already exists" not in text.lower():
            raise WorkflowError(pr.get("stderr") or pr.get("stdout") or "close-sync PR create failed")
        existing = _run_command(
            [
                "gh",
                "pr",
                "list",
                "--repo",
                GITHUB_REPO,
                "--head",
                branch,
                "--state",
                "open",
                "--json",
                "url",
                "--limit",
                "1",
            ],
            cwd=root,
            timeout=60,
        )
        if existing.get("ok"):
            try:
                rows = json.loads(str(existing.get("stdout") or "[]"))
                if rows:
                    pr_url = rows[0].get("url")
            except json.JSONDecodeError:
                pr_url = None
    result = {
        "workflow_gate": "pr_opened" if pr.get("ok") or pr_url else "committed",
        "root": str(root),
        "branch": branch,
        "changed_files": changed_files,
        "actions": actions,
        "commit": str(commit_sha.get("stdout") or "").strip() if commit_sha.get("ok") else None,
        "pr_url": pr_url,
        "duration_seconds": round(time.monotonic() - started, 3),
    }
    _append_event(
        bug_id,
        event="close_sync_persisted",
        state="close_synced",
        root=root,
        duration_seconds=result["duration_seconds"],
        evidence={"branch": branch, "commit": result["commit"], "pr_url": pr_url, "changed_files": changed_files},
    )
    return result


def _pr_url_from_create_output(result: dict[str, Any]) -> str | None:
    text = f"{result.get('stdout') or ''}\n{result.get('stderr') or ''}"
    match = re.search(r"https://github\.com/[^\s]+/pull/\d+", text)
    return match.group(0) if match else None


def _pr_number_from_url(pr_url: str | None) -> int | None:
    match = re.search(r"/pull/(\d+)(?:\D*$|$)", str(pr_url or ""))
    return int(match.group(1)) if match else None


def _bug_record_matches_close_sync(
    record: dict[str, Any],
    *,
    source_pr_url: str,
    merge_commit: str | None,
) -> bool:
    status = str(record.get("status") or "").strip().lower()
    record_pr_url = str(record.get("pr_url") or "").strip()
    record_commit = str(record.get("fix_commit") or "").strip()
    expected_commit = str(merge_commit or "").strip()
    if status not in {"fixed", "verified", "closed"}:
        return False
    if record_pr_url != source_pr_url:
        return False
    return not expected_commit or record_commit == expected_commit


def _find_bug_record_in_root(root: Path, bug_id: str) -> tuple[dict[str, Any], Path] | None:
    bugs_root = _bugs_root(root)
    if not bugs_root.exists():
        return None
    normalized = bug_id.strip().upper()
    for path in _bug_files(bugs_root):
        try:
            record = _load_json(path)
        except WorkflowError:
            continue
        if str(record.get("bug_id") or "").strip().upper() == normalized:
            return record, path
    return None


def _fetch_origin_main_for_close_sync(root: Path) -> dict[str, Any]:
    if not (root / ".git").exists() and not (root / ".git").is_file():
        return {"status": "skipped_no_git_checkout"}
    result = _run_command(["git", "fetch", "origin", "main", "--quiet"], cwd=root, timeout=120)
    return {"status": "fetched" if result.get("ok") else "warning", "result": result}


def _find_bug_record_in_git_ref(
    bug_id: str,
    *,
    ref: str = "origin/main",
    cwd: Path | None = None,
) -> tuple[dict[str, Any], str] | None:
    root = cwd or (_canonical_root() if _canonical_root().exists() else REPO_ROOT)
    if not (root / ".git").exists() and not (root / ".git").is_file():
        return None
    grep = _run_command(
        ["git", "grep", "-l", bug_id.strip().upper(), ref, "--", "tests/aistock_validation/bugs"],
        cwd=root,
        timeout=30,
    )
    if not grep.get("ok"):
        return None
    for line in str(grep.get("stdout") or "").splitlines():
        _, _, rel_path = line.partition(":")
        rel_path = rel_path.strip()
        if not rel_path or not rel_path.endswith(".json") or Path(rel_path).name.startswith("."):
            continue
        show = _run_command(["git", "show", f"{ref}:{rel_path}"], cwd=root, timeout=30)
        if not show.get("ok"):
            continue
        try:
            record = json.loads(str(show.get("stdout") or "{}"))
        except json.JSONDecodeError:
            continue
        if isinstance(record, dict) and str(record.get("bug_id") or "").strip().upper() == bug_id.strip().upper():
            return record, f"{ref}:{rel_path}"
    return None


def _close_sync_bug_snapshot(
    *,
    bug_id: str,
    source_pr_url: str,
    merge_commit: str | None,
    issue_json: str | None = None,
) -> dict[str, Any] | None:
    candidates: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add_candidate(record: dict[str, Any], path_label: str, source: str, root: Path | None = None) -> None:
        key = f"{source}:{path_label}"
        if key in seen:
            return
        seen.add(key)
        candidates.append({"record": record, "path": path_label, "source": source, "root": str(root) if root else None})

    if issue_json:
        path = Path(issue_json)
        if path.exists():
            try:
                add_candidate(_load_json(path), str(path), "issue_json", path.parent)
            except WorkflowError:
                pass

    try:
        record, path = find_bug_record(bug_id=bug_id)
        add_candidate(record, _repo_rel(path, REPO_ROOT), "current_worktree", REPO_ROOT)
    except WorkflowError:
        pass

    canonical = _canonical_root()
    if canonical.exists():
        found = _find_bug_record_in_root(canonical, bug_id)
        if found:
            record, path = found
            add_candidate(record, _repo_rel(path, canonical), "canonical_root", canonical)

    for candidate in candidates:
        if _bug_record_matches_close_sync(candidate["record"], source_pr_url=source_pr_url, merge_commit=merge_commit):
            return candidate

    ref_root = canonical if canonical.exists() else REPO_ROOT
    fetch = _fetch_origin_main_for_close_sync(ref_root)
    found_in_ref = _find_bug_record_in_git_ref(bug_id, ref="origin/main", cwd=ref_root)
    if found_in_ref:
        record, path_label = found_in_ref
        if _bug_record_matches_close_sync(record, source_pr_url=source_pr_url, merge_commit=merge_commit):
            return {
                "record": record,
                "path": path_label,
                "source": "origin_main_ref",
                "root": str(ref_root),
                "fetch": fetch,
            }
    return None


def _close_sync_is_complete(
    *,
    bug_id: str,
    source_pr_url: str,
    merge_commit: str | None,
    issue_json: str | None = None,
) -> dict[str, Any] | None:
    """Return a completed close-sync marker when BUG JSON already reflects this source PR."""
    snapshot = _close_sync_bug_snapshot(
        bug_id=bug_id,
        source_pr_url=source_pr_url,
        merge_commit=merge_commit,
        issue_json=issue_json,
    )
    if not snapshot:
        return None
    record = snapshot["record"]
    status = str(record.get("status") or "").strip().lower()
    record_commit = str(record.get("fix_commit") or "").strip()
    expected_commit = str(merge_commit or "").strip()

    marker = {
        "workflow_gate": "already_close_synced",
        "bug_id": bug_id,
        "registry_root": str(snapshot.get("root") or REPO_ROOT),
        "source_bug_json": snapshot["path"],
        "updated_bug_json": snapshot["path"],
        "merged_pr": source_pr_url,
        "merge_commit": record_commit or expected_commit or None,
        "current_status": status,
        "snapshot_source": snapshot["source"],
        "production_gates": {
            "production_backend_dependency_gate": record.get("production_backend_dependency_gate"),
            "production_ddl_gate": record.get("production_ddl_gate"),
            "production_frontend_dependency_gate": record.get("production_frontend_dependency_gate"),
        },
        "reason": "bug_json_already_fixed_for_source_pr",
    }
    if snapshot.get("fetch"):
        marker["origin_main_fetch"] = snapshot["fetch"]
    stale = _stale_pr_check_for_bug(bug_id)
    source_pr_number = _pr_number_from_url(source_pr_url)
    merged_close_sync_prs = []
    open_close_sync_prs = []
    for item in stale.get("merged_prs") or []:
        if _looks_like_close_sync_pr_for_source(
            item,
            source_pr_number=source_pr_number,
            source_pr_url=source_pr_url,
        ):
            merged_close_sync_prs.append(item)
    for item in stale.get("open_prs") or []:
        if _looks_like_close_sync_pr_for_source(
            item,
            source_pr_number=source_pr_number,
            source_pr_url=source_pr_url,
        ):
            open_close_sync_prs.append(item)
    marker["stale_pr_check"] = stale
    marker["merged_close_sync_prs"] = merged_close_sync_prs
    marker["open_close_sync_prs"] = open_close_sync_prs
    if merged_close_sync_prs:
        marker["close_sync_pr"] = merged_close_sync_prs[0]
    if open_close_sync_prs:
        marker["open_close_sync_pr"] = open_close_sync_prs[0]
    return marker


def _looks_like_close_sync_pr_for_source(
    item: dict[str, Any],
    *,
    source_pr_number: int | None,
    source_pr_url: str,
) -> bool:
    number = int(item.get("number") or 0)
    title = str(item.get("title") or "").lower()
    body = str(item.get("body") or "")
    if source_pr_number and number == source_pr_number:
        return False
    if "close-sync" not in title and "close sync" not in title:
        return False
    if source_pr_url and source_pr_url in body:
        return True
    if source_pr_number and re.search(rf"(?:#|/pull/){source_pr_number}\b", body):
        return True
    # Older PR list payloads and tests may not include body. The title and BUG search still identify the retry target.
    return not body.strip()


def _close_sync_pr_in_progress_marker(
    *,
    bug_id: str,
    source_pr_url: str,
    merge_commit: str | None,
) -> dict[str, Any] | None:
    """Return an existing close-sync PR marker even before BUG JSON reaches origin/main."""
    stale = _stale_pr_check_for_bug(bug_id)
    source_pr_number = _pr_number_from_url(source_pr_url)
    open_close_sync_prs = [
        item
        for item in stale.get("open_prs") or []
        if _looks_like_close_sync_pr_for_source(
            item,
            source_pr_number=source_pr_number,
            source_pr_url=source_pr_url,
        )
    ]
    if not open_close_sync_prs:
        return None
    pr = open_close_sync_prs[0]
    return {
        "workflow_gate": "close_sync_pr_open",
        "bug_id": bug_id,
        "registry_root": str(REPO_ROOT),
        "source_bug_json": None,
        "updated_bug_json": None,
        "merged_pr": source_pr_url,
        "merge_commit": merge_commit,
        "current_status": "unknown",
        "snapshot_source": "open_close_sync_pr",
        "reason": "existing_open_close_sync_pr_for_source",
        "stale_pr_check": stale,
        "open_close_sync_prs": open_close_sync_prs,
        "open_close_sync_pr": pr,
    }


def _close_sync_commit_already_merged(close_sync: dict[str, Any]) -> dict[str, Any]:
    pr = close_sync.get("close_sync_pr") or {}
    pr_url = str(pr.get("url") or "").strip()
    return {
        "workflow_gate": "already_merged",
        "reason": "close_sync_already_persisted",
        "branch": pr.get("headRefName"),
        "pr_url": pr_url or None,
        "commit": close_sync.get("merge_commit"),
    }


def _close_sync_commit_existing_open_pr(close_sync: dict[str, Any]) -> dict[str, Any]:
    pr = close_sync.get("open_close_sync_pr") or {}
    pr_url = str(pr.get("url") or "").strip()
    return {
        "workflow_gate": "pr_opened",
        "reason": "existing_open_close_sync_pr",
        "branch": pr.get("headRefName"),
        "pr_url": pr_url or None,
        "commit": close_sync.get("merge_commit"),
    }


def _close_sync_commit_needs_persistence(close_sync: dict[str, Any]) -> dict[str, Any]:
    return {
        "workflow_gate": "blocked",
        "reason": "close_sync_bug_json_fixed_but_not_persisted_to_origin_main",
        "blocking": [
            "BUG JSON is already fixed in the current close-sync snapshot, but no merged or open close-sync PR was found."
        ],
        "commit": close_sync.get("merge_commit"),
    }


def _merge_close_sync_pr_if_ready(
    *,
    bug_id: str,
    close_sync_commit: dict[str, Any],
    auto_merge: bool,
) -> dict[str, Any]:
    pr_url = str(close_sync_commit.get("pr_url") or "").strip()
    if not pr_url:
        return {
            "workflow_gate": "skipped",
            "reason": "missing_close_sync_pr_url",
            "auto_merge": auto_merge,
        }
    if not auto_merge:
        return {
            "workflow_gate": "ready_for_merge",
            "auto_merge": False,
            "pr_url": pr_url,
            "next_command": f"gh pr merge {pr_url} --squash --delete-branch",
        }
    try:
        result = _merge_pr_if_ready_for_bug(bug_id, pr_url)
    except WorkflowError as exc:
        return {
            "workflow_gate": "blocked",
            "auto_merge": True,
            "pr_url": pr_url,
            "blocking": [str(exc)],
            "next_command": (
                f"python scripts/aistock_issue_workflow.py watch-ci --bug-id {bug_id} "
                f"--pr-url {pr_url} --attempts 6 --delay-seconds 30"
            ),
        }
    return {
        "workflow_gate": "merged",
        "auto_merge": True,
        "pr_url": pr_url,
        "merge": result,
        "merge_commit": _merge_commit_from_pr_check(result.get("verified")) if isinstance(result, dict) else None,
    }


def _path_differs_from_canonical_root(path: Path) -> bool:
    canonical = _canonical_root()
    try:
        return path.resolve() != canonical.resolve()
    except OSError:
        return path != canonical


def _close_sync_worktree_for_cleanup(
    *,
    branch: str,
    close_sync_commit: dict[str, Any],
) -> str | None:
    root_text = str(close_sync_commit.get("root") or "").strip()
    if root_text:
        root_path = Path(root_text)
        if _path_differs_from_canonical_root(root_path):
            return str(root_path)
    discovered = _registered_worktree_for_branch(branch, cwd=_canonical_root()) if branch else None
    if discovered and _path_differs_from_canonical_root(discovered):
        return str(discovered)
    return None


def _build_close_sync_cleanup_after_merge_plan(
    *,
    bug_id: str,
    close_sync_commit: dict[str, Any],
    close_sync_pr_merge: dict[str, Any],
    cleanup: bool,
    apply: bool,
    sync_root: bool = False,
) -> dict[str, Any] | None:
    if not cleanup or close_sync_pr_merge.get("workflow_gate") not in {"merged", "already_merged"}:
        return None
    branch = str(close_sync_commit.get("branch") or "").strip()
    worktree = _close_sync_worktree_for_cleanup(
        branch=branch,
        close_sync_commit=close_sync_commit,
    )
    if not branch or not worktree:
        return None
    return build_cleanup_after_merge_plan(
        branch=branch,
        bug_id=bug_id,
        worktree=worktree,
        pr_url=close_sync_commit.get("pr_url") or close_sync_pr_merge.get("pr_url"),
        apply=bool(apply and close_sync_pr_merge.get("auto_merge")),
        sync_root=sync_root,
    )


def build_merge_finalizer_plan(
    *,
    bug_id: str | list[str],
    source_pr_url: str,
    source_branch: str | None,
    source_worktree: str | None,
    validation_evidence: list[str],
    issue_json: str | None = None,
    allow_missing_linkage: bool = False,
    production_gates: dict[str, str] | None = None,
    sync_root: bool = True,
    merge_close_sync_pr: bool = False,
    cleanup: bool = False,
    apply: bool = False,
    source_pr_check: dict[str, Any] | None = None,
) -> dict[str, Any]:
    canonical_bug_ids = flow._unique_strings(
        [str(item).strip().upper() for item in flow._as_list(bug_id) if str(item).strip()]
    )
    if not canonical_bug_ids:
        raise WorkflowError("merge-finalizer requires at least one --bug-id")
    canonical_bug_id = canonical_bug_ids[0]
    batch_mode = len(canonical_bug_ids) > 1
    evidence = [item for item in validation_evidence if item.strip()]
    blocking: list[str] = []
    warnings: list[str] = []
    if apply and not evidence:
        blocking.append("validation evidence is required before merge finalizer apply")
    if cleanup and not source_branch:
        blocking.append("--cleanup requires --source-branch")
    if cleanup and not source_worktree:
        warnings.append("--source-worktree is missing; cleanup can delete branches but cannot remove the task worktree")
    source_cleanup_deferred = bool(cleanup and apply and source_worktree and _cwd_is_inside(source_worktree))
    cleanup_cwd_relocation = None
    if cleanup and apply:
        cleanup_cwd_relocation = _relocate_cwd_before_cleanup(source_worktree)
        if cleanup_cwd_relocation and not cleanup_cwd_relocation.get("relocated"):
            blocking.append(cleanup_cwd_relocation.get("error") or "failed to relocate cwd before cleanup")

    payload: dict[str, Any] = {
        "schema_version": "aistock_issue_workflow_merge_finalizer_v1",
        "generated_at": _utc_now(),
        "bug_id": canonical_bug_id,
        "bug_ids": canonical_bug_ids,
        "batch_mode": batch_mode,
        "source_pr_url": source_pr_url,
        "source_branch": source_branch,
        "source_worktree": source_worktree,
        "merge_close_sync_pr": merge_close_sync_pr,
        "cleanup_requested": cleanup,
        "sync_root": sync_root,
        "dry_run": not apply,
        "blocking": blocking,
        "warnings": warnings,
        "production_gates": production_gates or _production_gates_payload(),
        "cleanup_cwd_relocation": cleanup_cwd_relocation,
        "source_cleanup_deferred": source_cleanup_deferred,
    }
    if blocking:
        payload["workflow_gate"] = "blocked"
        return payload
    if not apply:
        payload["workflow_gate"] = "ready_for_apply"
        bug_args = " ".join(f"--bug-id {item}" for item in canonical_bug_ids)
        payload["next_command"] = (
            f"python scripts/aistock_issue_workflow.py merge-finalizer {bug_args} "
            f"--source-pr-url {source_pr_url} --validation-evidence \"<command> -> passed\" --apply"
        )
        return payload

    source_pr_check = source_pr_check or _verify_pr_merged(source_pr_url)
    merge_commit = _merge_commit_from_pr_check(source_pr_check)
    if batch_mode:
        incomplete_bug_ids: list[str] = []
        complete_markers: list[dict[str, Any]] = []
        for item in canonical_bug_ids:
            marker = _close_sync_is_complete(
                bug_id=item,
                source_pr_url=source_pr_url,
                merge_commit=merge_commit,
                issue_json=issue_json if item == canonical_bug_id else None,
            )
            if not marker:
                marker = _close_sync_pr_in_progress_marker(
                    bug_id=item,
                    source_pr_url=source_pr_url,
                    merge_commit=merge_commit,
                )
            if marker and (marker.get("close_sync_pr") or marker.get("snapshot_source") == "origin_main_ref"):
                complete_markers.append(marker)
            elif marker and marker.get("open_close_sync_pr"):
                complete_markers.append(marker)
            else:
                incomplete_bug_ids.append(item)
        if not incomplete_bug_ids and complete_markers:
            close_sync = dict(complete_markers[0])
            close_sync["bug_ids"] = canonical_bug_ids
            if close_sync.get("open_close_sync_pr"):
                close_sync_commit = _close_sync_commit_existing_open_pr(close_sync)
                close_sync_pr_merge = _merge_close_sync_pr_if_ready(
                    bug_id=canonical_bug_id,
                    close_sync_commit=close_sync_commit,
                    auto_merge=merge_close_sync_pr,
                )
            else:
                close_sync_commit = _close_sync_commit_already_merged(close_sync)
                close_sync_pr_merge = {
                    "workflow_gate": "already_merged",
                    "auto_merge": merge_close_sync_pr,
                    "pr_url": close_sync_commit.get("pr_url"),
                    "reason": "batch_close_sync_pr_or_bug_json_already_persisted",
                }
        else:
            close_sync = build_close_sync_batch_plan(
                bug_ids=canonical_bug_ids,
                pr_url=source_pr_url,
                apply=True,
                allow_missing_linkage=allow_missing_linkage,
                validation_evidence=evidence,
                merge_commit=merge_commit,
                production_gates=production_gates or _production_gates_payload(),
                create_registry_worktree=True,
            )
            close_sync_commit = _maybe_commit_and_pr_close_sync(
                bug_id=canonical_bug_id,
                close_sync=close_sync,
                validation_evidence=evidence,
            )
            close_sync_pr_merge = _merge_close_sync_pr_if_ready(
                bug_id=canonical_bug_id,
                close_sync_commit=close_sync_commit,
                auto_merge=merge_close_sync_pr,
            )
    else:
        close_sync = _close_sync_is_complete(
            bug_id=canonical_bug_id,
            source_pr_url=source_pr_url,
            merge_commit=merge_commit,
            issue_json=issue_json,
        )
        if not close_sync:
            close_sync = _close_sync_pr_in_progress_marker(
                bug_id=canonical_bug_id,
                source_pr_url=source_pr_url,
                merge_commit=merge_commit,
            )
        if close_sync:
            if close_sync.get("close_sync_pr") or close_sync.get("snapshot_source") == "origin_main_ref":
                close_sync_commit = _close_sync_commit_already_merged(close_sync)
                close_sync_pr_merge = {
                    "workflow_gate": "already_merged",
                    "auto_merge": merge_close_sync_pr,
                    "pr_url": close_sync_commit.get("pr_url"),
                    "reason": "close_sync_pr_or_bug_json_already_persisted",
                }
            elif close_sync.get("open_close_sync_pr"):
                close_sync_commit = _close_sync_commit_existing_open_pr(close_sync)
                close_sync_pr_merge = _merge_close_sync_pr_if_ready(
                    bug_id=canonical_bug_id,
                    close_sync_commit=close_sync_commit,
                    auto_merge=merge_close_sync_pr,
                )
            else:
                close_sync_commit = _close_sync_commit_needs_persistence(close_sync)
                close_sync_pr_merge = {
                    "workflow_gate": "blocked",
                    "auto_merge": merge_close_sync_pr,
                    "blocking": close_sync_commit.get("blocking") or [],
                    "reason": close_sync_commit.get("reason"),
                }
        else:
            close_sync = build_close_sync_plan(
                bug_id=canonical_bug_id,
                issue_json=issue_json,
                pr_url=source_pr_url,
                apply=True,
                allow_missing_linkage=allow_missing_linkage,
                validation_evidence=evidence,
                merge_commit=merge_commit,
                production_gates=production_gates or _production_gates_payload(),
                create_registry_worktree=True,
            )
            close_sync_commit = _maybe_commit_and_pr_close_sync(
                bug_id=canonical_bug_id,
                close_sync=close_sync,
                validation_evidence=evidence,
            )
            close_sync_pr_merge = _merge_close_sync_pr_if_ready(
                bug_id=canonical_bug_id,
                close_sync_commit=close_sync_commit,
                auto_merge=merge_close_sync_pr,
            )

    cleanup_plan = None
    if cleanup and source_branch:
        if source_cleanup_deferred:
            cleanup_plan = _deferred_cleanup_from_safe_cwd_plan(
                branch=source_branch,
                bug_id=canonical_bug_id,
                worktree=source_worktree,
                pr_url=source_pr_url,
                sync_root=sync_root,
            )
        else:
            cleanup_plan = build_cleanup_after_merge_plan(
                branch=source_branch,
                bug_id=canonical_bug_id,
                worktree=source_worktree,
                pr_url=source_pr_url,
                apply=merge_close_sync_pr,
                sync_root=sync_root,
            )
    close_sync_cleanup_sync_root = bool(
        sync_root
        and close_sync_pr_merge.get("workflow_gate") in {"merged", "already_merged"}
        and (
            cleanup_plan is None
            or cleanup_plan.get("workflow_gate") != "cleanup_done"
            or not cleanup_plan.get("sync_root")
        )
    )
    close_sync_cleanup_plan = _build_close_sync_cleanup_after_merge_plan(
        bug_id=canonical_bug_id,
        close_sync_commit=close_sync_commit,
        close_sync_pr_merge=close_sync_pr_merge,
        cleanup=cleanup,
        apply=apply,
        sync_root=close_sync_cleanup_sync_root,
    )
    try:
        postmortem = build_postmortem_plan(bug_id=canonical_bug_id)
    except WorkflowError as exc:
        postmortem = {
            "schema_version": "aistock_issue_workflow_postmortem_v1",
            "workflow_gate": "skipped",
            "reason": str(exc),
        }
    final_blocking = []
    if close_sync_commit.get("workflow_gate") == "blocked":
        final_blocking.extend(close_sync_commit.get("blocking") or [])
    if close_sync_pr_merge.get("workflow_gate") == "blocked":
        final_blocking.extend(close_sync_pr_merge.get("blocking") or [])
    if cleanup_plan and cleanup_plan.get("workflow_gate") == "blocked":
        final_blocking.extend(cleanup_plan.get("blocking") or [])
    if close_sync_cleanup_plan and close_sync_cleanup_plan.get("workflow_gate") == "blocked":
        final_blocking.extend(close_sync_cleanup_plan.get("blocking") or [])

    cleanup_complete = bool(cleanup_plan and cleanup_plan.get("workflow_gate") == "cleanup_done")
    close_sync_cleanup_complete = (
        close_sync_cleanup_plan is None or close_sync_cleanup_plan.get("workflow_gate") == "cleanup_done"
    )

    payload.update(
        {
            "workflow_gate": "blocked" if final_blocking else (
                "complete" if cleanup_complete and close_sync_cleanup_complete else "close_sync_persisted"
            ),
            "blocking": final_blocking,
            "source_pr_check": source_pr_check,
            "source_merge_commit": merge_commit,
            "close_sync": close_sync,
            "close_sync_commit": close_sync_commit,
            "close_sync_pr_merge": close_sync_pr_merge,
            "cleanup": cleanup_plan,
            "close_sync_cleanup": close_sync_cleanup_plan,
            "postmortem": postmortem,
            "next_actions": [],
            "next_commands": [],
        }
    )
    if close_sync_pr_merge.get("workflow_gate") == "ready_for_merge":
        payload["next_actions"].append("merge_close_sync_pr_after_checks_are_green")
    if cleanup_plan is None and source_branch:
        payload["next_actions"].append("run_cleanup_after_merge")
    if cleanup_plan and cleanup_plan.get("workflow_gate") == "ready_for_cleanup":
        payload["next_actions"].append("rerun_cleanup_after_merge_with_apply")
        if cleanup_plan.get("next_command"):
            payload["next_commands"].append(cleanup_plan["next_command"])
    if close_sync_cleanup_plan and close_sync_cleanup_plan.get("workflow_gate") == "ready_for_cleanup":
        payload["next_actions"].append("rerun_close_sync_cleanup_after_merge_with_apply")
        if close_sync_cleanup_plan.get("next_command"):
            payload["next_commands"].append(close_sync_cleanup_plan["next_command"])
    for state_bug_id in canonical_bug_ids:
        _write_state(
            state_bug_id,
            state="complete" if payload["workflow_gate"] == "complete" else "close_synced",
            pr_url=source_pr_url,
            commit=merge_commit,
            close_sync=close_sync,
            close_sync_commit=close_sync_commit,
            close_sync_pr_merge=close_sync_pr_merge,
            cleanup_plan=cleanup_plan,
            close_sync_cleanup_plan=close_sync_cleanup_plan,
            postmortem=postmortem if state_bug_id == canonical_bug_id else None,
            next_actions=payload["next_actions"],
        )
    return payload


def build_run_plan(
    *,
    bug_id: str,
    mode: str,
    issue_json: str | None,
    changed_files: list[str],
    create_worktree: bool,
    dry_run: bool,
    validation_evidence: list[str],
    task_slug: str | None,
    allow_missing_linkage: bool,
    allow_closed: bool,
    base: str,
    head: str,
    push: bool = False,
    create_pr: bool = False,
    watch_ci: bool = False,
    pr_title: str | None = None,
    force_new_worktree: bool = False,
    force_reason: str | None = None,
    pr_url: str | None = None,
    merge: bool = False,
    sync_root: bool = False,
    branch: str | None = None,
    worktree: str | None = None,
    production_gates: dict[str, str] | None = None,
) -> dict[str, Any]:
    canonical_bug_id = bug_id.strip().upper()
    if mode in {"plan", "fix"}:
        if not issue_json:
            active_registry_record = _find_bug_record_from_active_registry(canonical_bug_id)
            if active_registry_record:
                _, active_registry_issue_path = active_registry_record
                issue_json = str(active_registry_issue_path)
        active_decision = _active_worktree_decision(
            bug_id=canonical_bug_id,
            create_worktree=create_worktree,
            force_new_worktree=force_new_worktree,
            force_reason=force_reason,
        )
        if active_decision["workflow_gate"] in {"resume", "blocked"}:
            event_root = Path(str((active_decision.get("active_workflows") or [{}])[0].get("root") or REPO_ROOT))
            _append_event(
                canonical_bug_id,
                event="active_worktree_decision",
                state="blocked" if active_decision["workflow_gate"] == "blocked" else "context_ready",
                result=str(active_decision["decision"]),
                evidence=active_decision,
                root=event_root,
            )
            return {
                "schema_version": "aistock_issue_workflow_run_v1",
                "generated_at": _utc_now(),
                "bug_id": canonical_bug_id,
                "mode": mode,
                "workflow_gate": active_decision["workflow_gate"],
                "active_decision": active_decision,
                "blocking": active_decision["blocking"],
                "warnings": active_decision["warnings"],
                "next_command": active_decision["next_command"],
            }
        start = build_start_plan(
            bug_id=canonical_bug_id,
            issue_json=issue_json,
            changed_files=changed_files,
            create_worktree=create_worktree,
            dry_run=dry_run,
            task_slug=task_slug,
            allow_missing_linkage=allow_missing_linkage,
            allow_closed=allow_closed,
            active_decision=active_decision,
        )
        gate = "ready_for_fix" if mode == "fix" else "planned"
        return {
            "schema_version": "aistock_issue_workflow_run_v1",
            "generated_at": _utc_now(),
            "bug_id": start["bug_id"],
            "mode": mode,
            "workflow_gate": gate,
            "start": start,
            "next_command": f"python scripts/aistock_issue_workflow.py resume --bug-id {start['bug_id']}",
        }
    if mode == "pr":
        finish = build_finish_plan(
            bug_id=canonical_bug_id,
            issue_json=issue_json,
            changed_files=changed_files or None,
            base=base,
            head=head,
            validation_evidence=validation_evidence,
            plan_only=not validation_evidence,
            allow_missing_evidence=False,
        )
        state_name = "validation_passed" if finish.get("validation_evidence") else "validation_planned"
        _write_state(
            canonical_bug_id,
            state=state_name,
            pr_body_path=finish.get("pr_body_path"),
            production_gates=finish.get("production_gates"),
            next_actions=["create_pr_from_pr_body", "watch_ci_before_merge"] if finish.get("validation_evidence") else ["run_required_validation"],
        )
        pr_automation = _maybe_create_pr(
            bug_id=canonical_bug_id,
            finish=finish,
            push=push,
            create_pr=create_pr,
            watch_ci=watch_ci,
            pr_title=pr_title,
        )
        return {
            "schema_version": "aistock_issue_workflow_run_v1",
            "generated_at": _utc_now(),
            "bug_id": canonical_bug_id,
            "mode": mode,
            "workflow_gate": "ready_for_pr" if finish.get("validation_evidence") else "validation_required",
            "finish": finish,
            "pr_automation": pr_automation,
            "next_command": f"gh pr create --body-file {finish.get('pr_body_path')} --fill"
            if finish.get("validation_evidence")
            else f"python scripts/aistock_issue_workflow.py finish --bug-id {canonical_bug_id} --validation-evidence \"<command> -> passed\"",
        }
    if mode == "merge":
        if not pr_url:
            raise WorkflowError("run --mode merge requires --pr-url")
        if not merge:
            return {
                "schema_version": "aistock_issue_workflow_run_v1",
                "generated_at": _utc_now(),
                "bug_id": canonical_bug_id,
                "mode": mode,
                "workflow_gate": "merge_requires_explicit_flag",
                "next_command": f"python scripts/aistock_issue_workflow.py run --bug-id {canonical_bug_id} --mode merge --pr-url {pr_url} --merge",
            }
        merge_result = _merge_pr_if_ready_for_bug(canonical_bug_id, pr_url)
        finalizer = build_merge_finalizer_plan(
            bug_id=canonical_bug_id,
            source_pr_url=pr_url,
            source_branch=branch,
            source_worktree=worktree,
            validation_evidence=validation_evidence,
            issue_json=issue_json,
            allow_missing_linkage=allow_missing_linkage,
            production_gates=production_gates or _production_gates_payload(),
            sync_root=sync_root,
            merge_close_sync_pr=False,
            cleanup=bool(branch),
            apply=True,
            source_pr_check=merge_result.get("verified") if isinstance(merge_result, dict) else None,
        )
        _write_state(
            canonical_bug_id,
            state="close_synced",
            pr_url=pr_url,
            commit=finalizer.get("source_merge_commit"),
            merge=merge_result,
            finalizer=finalizer,
            close_sync=finalizer.get("close_sync"),
            close_sync_commit=finalizer.get("close_sync_commit"),
            cleanup_plan=finalizer.get("cleanup"),
            next_actions=["merge_close_sync_pr_when_ready", "run_cleanup_after_merge_apply_when_ready"],
        )
        return {
            "schema_version": "aistock_issue_workflow_run_v1",
            "generated_at": _utc_now(),
            "bug_id": canonical_bug_id,
            "mode": mode,
            "workflow_gate": "merged_close_synced",
            "merge": merge_result,
            "finalizer": finalizer,
            "close_sync": finalizer.get("close_sync"),
            "close_sync_commit": finalizer.get("close_sync_commit"),
            "cleanup": finalizer.get("cleanup"),
        }
    raise WorkflowError(f"Unsupported run mode for Phase 1: {mode}")


def build_close_sync_plan(
    *,
    bug_id: str | None,
    issue_json: str | None,
    pr_url: str | None,
    apply: bool,
    allow_missing_linkage: bool,
    validation_evidence: list[str] | None = None,
    merge_commit: str | None = None,
    production_gates: dict[str, str] | None = None,
    skip_github_check: bool = False,
    create_registry_worktree: bool = False,
    allow_current_worktree: bool = False,
) -> dict[str, Any]:
    record, source_path = find_bug_record(bug_id=bug_id, issue_json=issue_json)
    canonical_bug_id = str(record.get("bug_id") or bug_id or source_path.stem).upper()
    missing_linkage = _require_github_linkage(record, allow_missing=allow_missing_linkage)
    status = str(record.get("status") or "").strip()
    if status not in flow.VALID_BUG_STATUSES:
        raise WorkflowError(f"{canonical_bug_id} has invalid status for close/sync: {status!r}")
    evidence = [item for item in validation_evidence or [] if item.strip()]
    gates = production_gates or _production_gates_payload()
    registry_worktree_plan = _maybe_create_close_sync_worktree(
        bug_id=canonical_bug_id,
        create=create_registry_worktree,
        dry_run=not apply,
    )
    close_sync_root = Path(registry_worktree_plan["worktree"]) if create_registry_worktree else REPO_ROOT
    if create_registry_worktree and apply:
        rel_source = source_path.resolve().relative_to(REPO_ROOT.resolve())
        target_source = close_sync_root / rel_source
        if not target_source.exists():
            raise WorkflowError(f"BUG JSON does not exist in close-sync worktree: {target_source}")
        record = _load_json(target_source)
        source_path = target_source
    apply_guard = _validate_close_sync_apply_target(close_sync_root) if apply else None
    if apply_guard and apply_guard["blocking"] and not allow_current_worktree:
        raise WorkflowError("; ".join(apply_guard["blocking"]))
    output_dir = close_sync_root / WORKFLOW_ROOT / canonical_bug_id
    workflow_gate = "ready_for_apply" if pr_url and evidence else ("missing_validation_evidence" if pr_url else "missing_pr_url")
    payload = {
        "schema_version": "aistock_issue_workflow_close_sync_v1",
        "generated_at": _utc_now(),
        "bug_id": canonical_bug_id,
        "source_bug_json": _repo_rel(source_path, close_sync_root),
        "registry_root": str(close_sync_root),
        "registry_worktree_plan": registry_worktree_plan,
        "apply_guard": apply_guard,
        "current_status": status,
        "github_issue_number": record.get("github_issue_number"),
        "github_issue_url": record.get("github_issue_url"),
        "missing_github_linkage": missing_linkage,
        "merged_pr": pr_url,
        "merge_commit": merge_commit,
        "validation_evidence": evidence,
        "production_gates": gates,
        "dry_run": not apply,
        "workflow_gate": workflow_gate,
        "required_checks": [
            "closure_requirements_completed",
            "validation_evidence_attached",
            "BUG_JSON_and_GitHub_issue_status_aligned",
            "production_gates_reported",
        ],
        "next_agent_steps": [
            "verify_closure_requirements_item_by_item",
            "run_close_sync_apply_from_clean_registry_worktree",
            "sync_github_issue_status_with_gh_or_mcp",
            "record_final_production_gates",
        ],
    }
    _write_json(output_dir / "close-sync-plan.json", payload)
    if apply:
        if not pr_url:
            raise WorkflowError("close-sync --apply requires --pr-url")
        if not evidence:
            raise WorkflowError("close-sync --apply requires at least one --validation-evidence")
        started = time.monotonic()
        pr_check = _verify_pr_merged(pr_url, skip_github_check=skip_github_check)
        merge_commit = merge_commit or _merge_commit_from_pr_check(pr_check)
        updated = dict(record)
        updated.update(
            {
                "status": "fixed",
                "fixed_at": _utc_now(),
                "fix_commit": merge_commit,
                "pr_url": pr_url,
                "validation_evidence": evidence,
                **gates,
            }
        )
        _write_json(source_path, updated)
        evidence_payload = {
            **payload,
            "workflow_gate": "close_synced",
            "dry_run": False,
            "pr_check": pr_check,
            "merge_commit": merge_commit,
            "updated_bug_json": _repo_rel(source_path, close_sync_root),
        }
        github_sync = (
            {"status": "skipped_github_check_disabled"}
            if skip_github_check
            else _sync_github_issue_after_close(updated, evidence_payload, root=close_sync_root)
        )
        evidence_payload["github_issue_sync"] = github_sync
        _write_json(output_dir / "close-sync-evidence.json", evidence_payload)
        timing = _workflow_timing_summary(canonical_bug_id, root=close_sync_root)
        _write_state(
            canonical_bug_id,
            state="close_synced",
            root=close_sync_root,
            pr_url=pr_url,
            commit=merge_commit,
            validation_evidence=evidence,
            production_gates=gates,
            github_issue_sync=github_sync,
            timing_summary=timing,
            next_actions=["sync_local_main", "cleanup_after_merge"],
        )
        _append_event(
            canonical_bug_id,
            event="close_sync_apply",
            state="close_synced",
            root=close_sync_root,
            duration_seconds=time.monotonic() - started,
            evidence={
                "pr_url": pr_url,
                "merge_commit": merge_commit,
                "github_issue_sync_status": github_sync.get("status"),
            },
        )
        evidence_payload["timing_summary"] = _workflow_timing_summary(canonical_bug_id, root=close_sync_root)
        return evidence_payload
    return payload


def build_close_sync_batch_plan(
    *,
    bug_ids: list[str],
    pr_url: str | None,
    apply: bool,
    allow_missing_linkage: bool,
    validation_evidence: list[str] | None = None,
    merge_commit: str | None = None,
    production_gates: dict[str, str] | None = None,
    skip_github_check: bool = False,
    create_registry_worktree: bool = False,
    allow_current_worktree: bool = False,
) -> dict[str, Any]:
    canonical_bug_ids = flow._unique_strings([item.strip().upper() for item in bug_ids if item.strip()])
    if not canonical_bug_ids:
        raise WorkflowError("close-sync-batch requires at least one --bug-id")
    evidence = [item for item in validation_evidence or [] if item.strip()]
    gates = production_gates or _production_gates_payload()
    records: list[dict[str, Any]] = []
    source_paths: list[Path] = []
    for item in canonical_bug_ids:
        record, source_path = find_bug_record(bug_id=item, issue_json=None)
        missing = _require_github_linkage(record, allow_missing=allow_missing_linkage)
        status = str(record.get("status") or "").strip()
        if status not in flow.VALID_BUG_STATUSES:
            raise WorkflowError(f"{item} has invalid status for close/sync: {status!r}")
        records.append({"bug_id": item, "record": record, "source_path": source_path, "missing_github_linkage": missing})
        source_paths.append(source_path)
    registry_worktree_plan = _maybe_create_close_sync_batch_worktree(
        bug_ids=canonical_bug_ids,
        create=create_registry_worktree,
        dry_run=not apply,
    )
    close_sync_root = Path(registry_worktree_plan["worktree"]) if create_registry_worktree else REPO_ROOT
    target_pairs: list[tuple[str, dict[str, Any], Path, list[str]]] = []
    if create_registry_worktree and apply:
        for item, record, source_path in [(row["bug_id"], row["record"], row["source_path"]) for row in records]:
            rel_source = source_path.resolve().relative_to(REPO_ROOT.resolve())
            target_source = close_sync_root / rel_source
            if not target_source.exists():
                raise WorkflowError(f"BUG JSON does not exist in close-sync batch worktree: {target_source}")
            target_record = _load_json(target_source)
            target_pairs.append((item, target_record, target_source, []))
    else:
        target_pairs = [
            (row["bug_id"], row["record"], row["source_path"], row["missing_github_linkage"])
            for row in records
        ]
    apply_guard = _validate_close_sync_apply_target(close_sync_root) if apply else None
    if apply_guard and apply_guard["blocking"] and not allow_current_worktree:
        raise WorkflowError("; ".join(apply_guard["blocking"]))
    batch_id = "-".join(canonical_bug_ids)
    output_dir = close_sync_root / WORKFLOW_ROOT / batch_id
    workflow_gate = "ready_for_apply" if pr_url and evidence else ("missing_validation_evidence" if pr_url else "missing_pr_url")
    payload: dict[str, Any] = {
        "schema_version": "aistock_issue_workflow_close_sync_batch_v1",
        "generated_at": _utc_now(),
        "batch_id": batch_id,
        "bug_ids": canonical_bug_ids,
        "source_bug_jsons": [_repo_rel(path, close_sync_root) for path in source_paths],
        "registry_root": str(close_sync_root),
        "registry_worktree_plan": registry_worktree_plan,
        "apply_guard": apply_guard,
        "merged_pr": pr_url,
        "merge_commit": merge_commit,
        "validation_evidence": evidence,
        "production_gates": gates,
        "dry_run": not apply,
        "workflow_gate": workflow_gate,
        "per_issue": [
            {
                "bug_id": item,
                "github_issue_number": record.get("github_issue_number"),
                "github_issue_url": record.get("github_issue_url"),
                "source_bug_json": _repo_rel(path, close_sync_root),
                "missing_github_linkage": missing,
            }
            for item, record, path, missing in target_pairs
        ],
        "next_agent_steps": [
            "verify_shared_source_pr_covers_each_bug",
            "run_close_sync_batch_apply_from_clean_registry_worktree",
            "sync_each_github_issue_status",
            "persist_one_close_sync_pr_for_the_batch",
        ],
    }
    _write_json(output_dir / "close-sync-batch-plan.json", payload)
    if not apply:
        return payload
    if not pr_url:
        raise WorkflowError("close-sync-batch --apply requires --pr-url")
    if not evidence:
        raise WorkflowError("close-sync-batch --apply requires at least one --validation-evidence")
    started = time.monotonic()
    pr_check = _verify_pr_merged(pr_url, skip_github_check=skip_github_check)
    merge_commit = merge_commit or _merge_commit_from_pr_check(pr_check)
    updated_paths: list[str] = []
    github_syncs: dict[str, Any] = {}
    for item, record, source_path, _missing in target_pairs:
        updated = dict(record)
        updated.update(
            {
                "status": "fixed",
                "fixed_at": _utc_now(),
                "fix_commit": merge_commit,
                "pr_url": pr_url,
                "validation_evidence": evidence,
                **gates,
            }
        )
        _write_json(source_path, updated)
        updated_paths.append(_repo_rel(source_path, close_sync_root))
        issue_evidence = {
            **payload,
            "workflow_gate": "close_synced",
            "bug_id": item,
            "merge_commit": merge_commit,
            "updated_bug_json": _repo_rel(source_path, close_sync_root),
        }
        github_syncs[item] = (
            {"status": "skipped_github_check_disabled"}
            if skip_github_check
            else _sync_github_issue_after_close(updated, issue_evidence, root=close_sync_root)
        )
        _write_state(
            item,
            state="close_synced",
            root=close_sync_root,
            pr_url=pr_url,
            commit=merge_commit,
            validation_evidence=evidence,
            production_gates=gates,
            github_issue_sync=github_syncs[item],
            next_actions=["persist_batch_close_sync_pr", "sync_local_main", "cleanup_after_merge"],
        )
        _append_event(
            item,
            event="close_sync_batch_apply",
            state="close_synced",
            root=close_sync_root,
            duration_seconds=0.0,
            evidence={"batch_id": batch_id, "pr_url": pr_url, "merge_commit": merge_commit},
        )
    evidence_payload = {
        **payload,
        "workflow_gate": "close_synced",
        "dry_run": False,
        "pr_check": pr_check,
        "merge_commit": merge_commit,
        "updated_bug_jsons": updated_paths,
        "github_issue_sync": github_syncs,
        "duration_seconds": round(time.monotonic() - started, 3),
    }
    _write_json(output_dir / "close-sync-batch-evidence.json", evidence_payload)
    return evidence_payload


def build_cleanup_after_merge_plan(
    *,
    branch: str,
    bug_id: str | None = None,
    worktree: str | None = None,
    pr_url: str | None = None,
    apply: bool = False,
    sync_root: bool = False,
    canonical_root: str | None = None,
) -> dict[str, Any]:
    root = Path(canonical_root) if canonical_root else _canonical_root()
    pre_cleanup_fetch = _cleanup_preflight_fetch_origin(root, apply=apply)
    current_branch = _git(["branch", "--show-current"], cwd=root, check=False)
    local_branches = set(
        _git(["for-each-ref", "--format=%(refname:short)", "refs/heads"], cwd=root, check=False).splitlines()
    )
    remote_ref = _git(["ls-remote", "--heads", "origin", branch], cwd=root, check=False)
    merged_refs = set(_git(["branch", "--format=%(refname:short)", "--merged", "origin/main"], cwd=root, check=False).splitlines())
    merged = branch in merged_refs
    merge_verification = _cleanup_merge_verification(branch, pr_url, merged, cwd=root)
    squash_merge_verified = bool(merge_verification["squash_merge_verified"])
    pr_check = merge_verification["pr_check"]
    tree_equivalent = bool(merge_verification["tree_equivalent_to_origin_main"])
    merge_verified = bool(merge_verification["verified"])
    worktree_path = Path(worktree) if worktree else None
    worktree_clean = True
    worktree_registered = False
    worktree_exists = bool(worktree_path and worktree_path.exists())
    worktree_empty = False
    worktree_is_current_cwd = False
    worktree_orphan_profile: dict[str, Any] | None = None
    if worktree_path and worktree_path.exists():
        try:
            current_cwd = Path.cwd().resolve()
            resolved_worktree = worktree_path.resolve()
            worktree_is_current_cwd = current_cwd == resolved_worktree or resolved_worktree in current_cwd.parents
        except OSError:
            worktree_is_current_cwd = False
        try:
            worktree_empty = not any(worktree_path.iterdir())
        except OSError:
            worktree_empty = False
        status_result = _run_command(["git", "status", "--porcelain=v1"], cwd=worktree_path)
        worktree_branch = _git(["branch", "--show-current"], cwd=worktree_path, check=False) if status_result.get("ok") else ""
        worktree_registered = _path_is_registered_worktree(worktree_path, cwd=root) or (
            bool(status_result.get("ok")) and worktree_branch == branch
        )
        worktree_clean = bool(status_result.get("ok")) and status_result.get("stdout") == ""
        if not worktree_registered:
            worktree_orphan_profile = _orphan_worktree_dir_profile(worktree_path)
    root_git = _git_snapshot(root) if root.exists() else {"ok": False, "error": "canonical root missing"}
    root_dirty_files = _dirty_files(root) if root.exists() else []
    origin_equivalent_dirty_files = _origin_equivalent_dirty_files(root, root_dirty_files) if root_dirty_files else []
    unrelated_root_dirty_files = sorted(set(root_dirty_files) - set(origin_equivalent_dirty_files))
    root_sync_safe_with_dirty = _root_sync_safe_with_dirty(root_git)
    blocking: list[str] = []
    warnings: list[str] = []
    if pre_cleanup_fetch.get("status") == "failed":
        result = pre_cleanup_fetch.get("result") if isinstance(pre_cleanup_fetch.get("result"), dict) else {}
        detail = result.get("stderr") or result.get("stdout") or "unknown error"
        blocking.append(f"failed to refresh origin/main before cleanup: {detail}")
    if branch == current_branch and apply:
        blocking.append("refusing to cleanup the currently checked-out branch")
    if not merge_verified:
        blocking.append(f"branch is not merged into origin/main: {branch}")
    if worktree_path and worktree_exists and worktree_is_current_cwd and apply and not root.exists():
        blocking.append(f"refusing to remove the current working directory because canonical root is unavailable: {worktree_path}")
    if (
        worktree_path
        and worktree_exists
        and not worktree_registered
        and not worktree_empty
        and not (worktree_orphan_profile or {}).get("safe_reparse_or_empty_only")
    ):
        blocking.append(f"worktree path exists but is not a registered git worktree: {worktree_path}")
    if worktree_path and worktree_registered and not worktree_clean:
        blocking.append(f"worktree is dirty: {worktree_path}")
    if sync_root:
        if not root.exists():
            blocking.append(f"canonical root missing: {root}")
        elif root_git.get("branch") != "main":
            blocking.append(f"canonical root is not on main: {root_git.get('branch')}")
        elif root_git.get("dirty") and origin_equivalent_dirty_files and not unrelated_root_dirty_files:
            warnings.append("canonical root has only origin/main-equivalent dirty file(s); cleanup apply can restore them safely")
        elif root_git.get("dirty") and root_sync_safe_with_dirty:
            if unrelated_root_dirty_files:
                warnings.append(
                    "canonical root has unrelated dirty file(s); sync is already at origin/main and cleanup will ignore them"
                )
            if origin_equivalent_dirty_files:
                warnings.append("canonical root has origin/main-equivalent dirty file(s); cleanup apply can restore them safely")
        elif root_git.get("dirty"):
            blocking.append(f"canonical root is dirty and not synced to origin/main: {root}")
    actions = []
    if sync_root:
        actions.append({"action": "sync_root_main", "root": str(root), "safe": not any("canonical root" in item for item in blocking)})
    if worktree_path and worktree_exists and worktree_registered:
        if worktree_is_current_cwd:
            actions.append({"action": "relocate_current_cwd", "root": str(root), "safe": root.exists()})
        actions.append({"action": "remove_worktree", "worktree": str(worktree_path), "safe": merge_verified and worktree_clean})
    elif worktree_path and worktree_exists and (worktree_empty or (worktree_orphan_profile or {}).get("safe_reparse_or_empty_only")):
        if worktree_is_current_cwd:
            actions.append({"action": "relocate_current_cwd", "root": str(root), "safe": root.exists()})
        actions.append({
            "action": "remove_orphan_worktree_dir",
            "worktree": str(worktree_path),
            "safe": merge_verified and bool((worktree_orphan_profile or {"safe_reparse_or_empty_only": worktree_empty}).get("safe_reparse_or_empty_only")),
        })
    if branch in local_branches:
        actions.append({"action": "delete_local_branch", "branch": branch, "safe": merge_verified})
    if remote_ref:
        actions.append({"action": "delete_remote_branch", "branch": branch, "safe": merge_verified})
    payload = {
        "schema_version": "aistock_issue_workflow_cleanup_v1",
        "generated_at": _utc_now(),
        "branch": branch,
        "worktree": str(worktree_path) if worktree_path else None,
        "canonical_root": str(root),
        "sync_root": sync_root,
        "merged_into_origin_main": merged,
        "merge_verification": merge_verification,
        "squash_merge_verified": squash_merge_verified,
        "tree_equivalent_to_origin_main": tree_equivalent,
        "pr_check": pr_check,
        "worktree_clean": worktree_clean,
        "worktree_exists": worktree_exists,
        "worktree_registered": worktree_registered,
        "worktree_empty": worktree_empty,
        "worktree_orphan_profile": worktree_orphan_profile,
        "worktree_is_current_cwd": worktree_is_current_cwd,
        "pre_cleanup_fetch": pre_cleanup_fetch,
        "root_git": root_git,
        "root_dirty_files": root_dirty_files,
        "origin_equivalent_dirty_files": origin_equivalent_dirty_files,
        "unrelated_root_dirty_files": unrelated_root_dirty_files,
        "root_sync_safe_with_dirty": root_sync_safe_with_dirty,
        "blocking": blocking,
        "warnings": warnings,
        "actions": actions,
        "dry_run": not apply,
        "workflow_gate": "ready_for_cleanup" if not blocking else "blocked",
    }
    if bug_id:
        payload["registry_intake_cleanup"] = build_registry_intake_cleanup_plan(
            bug_id=bug_id,
            apply=False,
            canonical_root=str(root),
        )
    output_dir = REPO_ROOT / WORKFLOW_ROOT / "cleanup"
    _write_json(output_dir / f"{_slug(branch)}-cleanup-plan.json", payload)
    if apply:
        if blocking:
            raise WorkflowError("; ".join(blocking))
        started = time.monotonic()
        applied: list[dict[str, Any]] = []
        if pre_cleanup_fetch.get("status") == "fetched":
            applied.append(
                {
                    "command": pre_cleanup_fetch.get("command") or "git fetch origin --prune",
                    "phase": "pre_cleanup_verification",
                    "result": pre_cleanup_fetch.get("result"),
                }
            )
        if sync_root:
            if pre_cleanup_fetch.get("status") != "fetched":
                applied.append({"command": "git fetch origin --prune", "result": _execute_checked(["git", "fetch", "origin", "--prune"], cwd=root, timeout=120)})
            if origin_equivalent_dirty_files and not unrelated_root_dirty_files:
                applied.append(
                    {
                        "command": "git restore origin/main-equivalent dirty file(s)",
                        "result": _execute_checked(
                            ["git", "restore", "--source=origin/main", "--", *origin_equivalent_dirty_files],
                            cwd=root,
                            timeout=120,
                        ),
                    }
                )
            if root_sync_safe_with_dirty:
                applied.append(
                    {
                        "command": "skip git merge --ff-only origin/main; canonical root already synced and dirty files are unrelated",
                        "result": {"ok": True, "stdout": "", "stderr": "", "returncode": 0},
                    }
                )
            else:
                applied.append({"command": "git merge --ff-only origin/main", "result": _execute_checked(["git", "merge", "--ff-only", "origin/main"], cwd=root, timeout=120)})
        if worktree_path and worktree_path.exists() and worktree_is_current_cwd:
            os.chdir(root)
            applied.append(
                {
                    "command": f"chdir {root} before removing current worktree",
                    "result": {"ok": True, "stdout": "", "stderr": "", "returncode": 0},
                }
            )
        if worktree_path and worktree_path.exists() and worktree_registered:
            applied.append({"command": f"git worktree remove {worktree_path}", "result": _remove_worktree_with_reparse_fallback(root=root, worktree_path=worktree_path)})
        elif worktree_path and worktree_path.exists() and (worktree_empty or (worktree_orphan_profile or {}).get("safe_reparse_or_empty_only")):
            removed = _remove_reparse_or_empty_tree(worktree_path)
            applied.append(
                {
                    "command": f"remove orphan worktree dir {worktree_path}",
                    "result": removed,
                }
            )
            if removed.get("deferred"):
                payload.setdefault("warnings", []).append(
                    f"deferred empty worktree directory cleanup: {worktree_path}"
                )
                payload["deferred_cleanup"] = {
                    "schema_version": "aistock_issue_workflow_deferred_cleanup_v1",
                    "worktree": str(worktree_path),
                    "reason": removed.get("deferred_reason"),
                    "safe_to_retry": True,
                    "profile": removed.get("profile"),
                }
        if branch in local_branches:
            delete_flag = "-d" if merged else "-D"
            applied.append({"command": f"git branch {delete_flag} {branch}", "result": _execute_checked(["git", "branch", delete_flag, branch], cwd=root, timeout=120)})
        if remote_ref:
            applied.append({"command": f"git push origin --delete {branch}", "result": _execute_checked(["git", "push", "origin", "--delete", branch], cwd=root, timeout=180)})
        if bug_id:
            registry_cleanup = build_registry_intake_cleanup_plan(
                bug_id=bug_id,
                apply=True,
                canonical_root=str(root),
            )
            payload["registry_intake_cleanup"] = registry_cleanup
            if registry_cleanup.get("warnings"):
                payload["warnings"].extend(registry_cleanup.get("warnings") or [])
        payload["applied"] = applied
        payload["workflow_gate"] = "cleanup_done"
        payload["dry_run"] = False
        payload["duration_seconds"] = round(time.monotonic() - started, 3)
        _write_json(output_dir / f"{_slug(branch)}-cleanup-evidence.json", payload)
    return payload


def cmd_start(args: argparse.Namespace) -> int:
    payload = build_start_plan(
        bug_id=args.bug_id,
        issue_json=args.issue_json,
        changed_files=list(args.changed_file or []),
        create_worktree=args.create_worktree,
        dry_run=args.dry_run,
        task_slug=args.task_slug,
        allow_missing_linkage=args.allow_missing_linkage,
        allow_closed=args.allow_closed,
        active_decision=None,
    )
    _emit_args(payload, args)
    return 0


def cmd_finish(args: argparse.Namespace) -> int:
    payload = build_finish_plan(
        bug_id=args.bug_id,
        issue_json=args.issue_json,
        changed_files=list(args.changed_file) if args.changed_file else None,
        base=args.base,
        head=args.head,
        validation_evidence=list(args.validation_evidence or []),
        plan_only=args.plan_only,
        allow_missing_evidence=args.allow_missing_evidence,
    )
    _emit_args(payload, args)
    return 0 if payload.get("closure_ready") else 2


def cmd_triage_p0(args: argparse.Namespace) -> int:
    payload = build_triage_p0(include_fixed=args.include_fixed)
    _emit_args(payload, args)
    return 0


def cmd_run_p0(args: argparse.Namespace) -> int:
    payload = build_run_p0_plan(module=args.module, include_fixed=args.include_fixed)
    _emit_args(payload, args)
    return 0


def cmd_start_batch(args: argparse.Namespace) -> int:
    payload = build_start_batch_plan(
        bug_ids=list(args.bug_id or []),
        create_worktree=args.create_worktree,
        dry_run=args.dry_run,
        task_slug=args.task_slug,
        allow_missing_linkage=args.allow_missing_linkage,
        allow_closed=args.allow_closed,
    )
    _emit_args(payload, args)
    return 0


def cmd_finish_batch(args: argparse.Namespace) -> int:
    payload = build_finish_batch_plan(
        batch_id=args.batch_id,
        bug_ids=list(args.bug_id or []),
        changed_files=list(args.changed_file) if args.changed_file else None,
        base=args.base,
        head=args.head,
        validation_evidence=list(args.validation_evidence or []),
        issue_commit=list(args.issue_commit or []),
        plan_only=args.plan_only,
        allow_missing_evidence=args.allow_missing_evidence,
    )
    _emit_args(payload, args)
    return 0 if payload.get("closure_ready") else 2


def cmd_fast_path(args: argparse.Namespace) -> int:
    payload = build_fast_path_plan(
        bug_id=args.bug_id,
        issue_json=args.issue_json,
        changed_files=list(args.changed_file or []),
        module=args.module,
        allow_missing_linkage=args.allow_missing_linkage,
        allow_closed=args.allow_closed,
    )
    _emit_args(payload, args)
    return 0


def cmd_workflow_smoke(args: argparse.Namespace) -> int:
    payload = build_workflow_smoke_plan(
        bug_id=args.bug_id,
        issue_json=args.issue_json,
        changed_files=list(args.changed_file or []),
        module=args.module,
    )
    _emit_args(payload, args)
    return 0 if payload.get("workflow_gate") == "passed" else 2


def cmd_nightly_intake_smoke(args: argparse.Namespace) -> int:
    payload = build_nightly_intake_smoke_plan()
    _emit_args(payload, args)
    return 0 if payload.get("workflow_gate") == "passed" else 2


def cmd_batch_workflow_smoke(args: argparse.Namespace) -> int:
    payload = build_batch_workflow_smoke_plan()
    _emit_args(payload, args)
    return 0 if payload.get("workflow_gate") == "passed" else 2


def cmd_doctor(args: argparse.Namespace) -> int:
    payload = build_doctor_report(skip_external=args.skip_external)
    _emit_args(payload, args)
    return 0 if payload.get("workflow_gate") != "blocked" else 2


def cmd_submit_bug(args: argparse.Namespace) -> int:
    payload = build_submit_bug_plan(
        title=args.title,
        module=args.module,
        severity=args.severity,
        description=args.description,
        expected=args.expected,
        actual=args.actual,
        reproduce_command=args.reproduce_command,
        evidence_refs=list(args.evidence_ref or []),
        changed_files=list(args.changed_file or []),
        plan_key=args.plan_key,
        nox_session=args.nox_session,
        candidate_type=args.candidate_type,
        bug_id=args.bug_id,
        github_issue_number=args.github_issue_number,
        github_issue_url=args.github_issue_url,
        create_github=args.create_github,
        apply=args.apply,
        allow_current_worktree=args.allow_current_worktree,
        create_registry_worktree=args.create_registry_worktree,
        create_fix_worktree=args.create_fix_worktree,
        registry_pr_only=args.registry_pr_only,
        dry_run=args.dry_run,
    )
    _emit_args(payload, args)
    return 0 if payload.get("workflow_gate") in {"ready_for_apply", "submitted"} else 2


def cmd_install_client(args: argparse.Namespace) -> int:
    payload = build_client_install_plan(
        apply=args.apply,
        codex_home=args.codex_home,
        claude_home=args.claude_home,
    )
    _emit_args(payload, args)
    return 0 if payload.get("workflow_gate") in {"ready_for_install", "installed"} else 2


def cmd_triage_ci_issue(args: argparse.Namespace) -> int:
    payload = build_triage_ci_issue_plan(
        issue_number=args.issue,
        run_id=args.run_id,
        summary_json=args.summary_json,
        skip_github_summary=args.skip_github_summary,
    )
    _emit_args(payload, args)
    return 0


def cmd_ci_issue_janitor(args: argparse.Namespace) -> int:
    payload = build_ci_issue_janitor_plan(
        issue_numbers=list(args.issue or []),
        apply=args.apply,
        limit=args.limit,
        skip_github_summary=args.skip_github_summary,
    )
    _emit_args(payload, args)
    return 0 if payload.get("workflow_gate") in {"ready_for_apply", "closed", "no_actionable_ci_issues"} else 2


def cmd_promote_ci_issue(args: argparse.Namespace) -> int:
    payload = build_promote_ci_issue_plan(
        issue_number=args.issue,
        apply=args.apply,
        bug_id=args.bug_id,
        summary_json=args.summary_json,
        skip_github_summary=args.skip_github_summary,
        create_registry_worktree=args.create_registry_worktree,
    )
    _emit_args(payload, args)
    return 0 if payload.get("workflow_gate") in {"ready_for_apply", "promoted", "already_linked"} else 2


def cmd_run(args: argparse.Namespace) -> int:
    payload = build_run_plan(
        bug_id=args.bug_id,
        mode=args.mode,
        issue_json=args.issue_json,
        changed_files=list(args.changed_file or []),
        create_worktree=args.create_worktree,
        dry_run=args.dry_run,
        validation_evidence=list(args.validation_evidence or []),
        task_slug=args.task_slug,
        allow_missing_linkage=args.allow_missing_linkage,
        allow_closed=args.allow_closed,
        base=args.base,
        head=args.head,
        push=args.push,
        create_pr=args.create_pr,
        watch_ci=args.watch_ci,
        pr_title=args.pr_title,
        force_new_worktree=args.force_new_worktree,
        force_reason=args.reason,
        pr_url=args.pr_url,
        merge=args.merge,
        sync_root=args.sync_root,
        branch=args.branch,
        worktree=args.worktree,
        production_gates=_production_gates_payload(args),
    )
    _emit_args(payload, args)
    return 0 if payload.get("workflow_gate") not in {"validation_evidence_missing", "blocked"} else 2


def cmd_resume(args: argparse.Namespace) -> int:
    payload = build_resume_plan(bug_id=args.bug_id, worktree=args.worktree, events_limit=args.events_limit)
    _emit_args(payload, args)
    return 0


def cmd_postmortem(args: argparse.Namespace) -> int:
    payload = build_postmortem_plan(
        bug_id=args.bug_id,
        worktree=args.worktree,
        output_markdown=not args.no_markdown,
        persist_artifacts=args.persist_artifacts,
    )
    _emit_args(payload, args)
    return 0


def cmd_watch_ci(args: argparse.Namespace) -> int:
    payload = build_watch_ci_plan(
        bug_id=args.bug_id,
        pr_url=args.pr_url,
        attempts=args.attempts,
        delay_seconds=args.delay_seconds,
    )
    _emit_args(payload, args)
    return 0 if payload.get("workflow_gate") in {"checks_passed", "checks_pending"} else 2


def cmd_close_sync(args: argparse.Namespace) -> int:
    payload = build_close_sync_plan(
        bug_id=args.bug_id,
        issue_json=args.issue_json,
        pr_url=args.pr_url,
        apply=args.apply,
        allow_missing_linkage=args.allow_missing_linkage,
        validation_evidence=list(args.validation_evidence or []),
        merge_commit=args.merge_commit,
        production_gates=_production_gates_payload(args),
        skip_github_check=args.skip_github_check,
        create_registry_worktree=args.create_registry_worktree,
        allow_current_worktree=args.allow_current_worktree,
    )
    _emit_args(payload, args)
    return 0 if payload.get("workflow_gate") in {"ready_for_apply", "close_synced"} else 2


def cmd_close_sync_batch(args: argparse.Namespace) -> int:
    payload = build_close_sync_batch_plan(
        bug_ids=list(args.bug_id or []),
        pr_url=args.pr_url,
        apply=args.apply,
        allow_missing_linkage=args.allow_missing_linkage,
        validation_evidence=list(args.validation_evidence or []),
        merge_commit=args.merge_commit,
        production_gates=_production_gates_payload(args),
        skip_github_check=args.skip_github_check,
        create_registry_worktree=args.create_registry_worktree,
        allow_current_worktree=args.allow_current_worktree,
    )
    if args.apply and args.create_pr:
        payload["close_sync_commit"] = _maybe_commit_and_pr_close_sync(
            bug_id=payload["bug_ids"][0],
            close_sync=payload,
            validation_evidence=list(args.validation_evidence or []),
        )
    _emit_args(payload, args)
    return 0 if payload.get("workflow_gate") in {"ready_for_apply", "close_synced"} else 2


def cmd_cleanup_after_merge(args: argparse.Namespace) -> int:
    payload = build_cleanup_after_merge_plan(
        branch=args.branch,
        bug_id=args.bug_id,
        worktree=args.worktree,
        pr_url=args.pr_url,
        apply=args.apply,
        sync_root=args.sync_root,
        canonical_root=args.canonical_root,
    )
    if payload.get("workflow_gate") == "cleanup_done" and args.bug_id:
        bug_id = args.bug_id.strip().upper()
        try:
            pre_cleanup_postmortem = build_postmortem_plan(bug_id=bug_id, output_markdown=False)
            if _workflow_artifacts_enabled():
                pre_cleanup_path = REPO_ROOT / WORKFLOW_ROOT / bug_id / "postmortem-pre-cleanup.json"
                _write_json(pre_cleanup_path, pre_cleanup_postmortem)
                payload["pre_cleanup_postmortem_path"] = _repo_rel(pre_cleanup_path)
            else:
                payload["pre_cleanup_postmortem"] = {
                    "artifact_policy": "compact_success_no_artifact",
                    "timing_summary": pre_cleanup_postmortem.get("timing_summary"),
                    "h6_summary": pre_cleanup_postmortem.get("h6_summary"),
                }
        except WorkflowError as exc:
            payload.setdefault("warnings", []).append(f"pre-cleanup postmortem skipped: {exc}")
        cleanup_evidence = {
            key: payload.get(key)
            for key in (
                "schema_version",
                "branch",
                "worktree",
                "canonical_root",
                "sync_root",
                "workflow_gate",
                "actions",
                "applied",
                "duration_seconds",
            )
        }
        state = _write_state(
            bug_id,
            state="complete",
            root=REPO_ROOT,
            cleanup_evidence=cleanup_evidence,
            next_actions=[],
        )
        payload["complete_state"] = state
    _emit_args(payload, args)
    return 0 if payload.get("workflow_gate") in {"ready_for_cleanup", "cleanup_done"} else 2


def cmd_merge_finalizer(args: argparse.Namespace) -> int:
    payload = build_merge_finalizer_plan(
        bug_id=args.bug_id,
        source_pr_url=args.source_pr_url,
        source_branch=args.source_branch,
        source_worktree=args.source_worktree,
        validation_evidence=list(args.validation_evidence or []),
        issue_json=args.issue_json,
        allow_missing_linkage=args.allow_missing_linkage,
        production_gates=_production_gates_payload(args),
        sync_root=args.sync_root,
        merge_close_sync_pr=args.merge_close_sync_pr,
        cleanup=args.cleanup,
        apply=args.apply,
    )
    _emit_args(payload, args)
    return 0 if payload.get("workflow_gate") not in {"blocked"} else 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="AIstock high-level issue-fix workflow orchestrator.")
    sub = parser.add_subparsers(dest="command", required=True)

    def add_output_options(command_parser: argparse.ArgumentParser) -> None:
        command_parser.add_argument("--output")
        command_parser.add_argument(
            "--output-format",
            choices=OUTPUT_FORMAT_CHOICES,
            default="compact",
            help="Stdout format. Default compact keeps success output short; --output still writes the full JSON artifact.",
        )
    doctor = sub.add_parser("doctor", help="Check repo, GitHub, MCP, and client-entry readiness.")
    doctor.add_argument("--skip-external", action="store_true", help="Skip gh/network-style checks for offline tests.")
    add_output_options(doctor)
    doctor.set_defaults(func=cmd_doctor)

    submit_bug = sub.add_parser("submit-bug", help="Create a normalized BUG candidate and optionally sync GitHub/BUG JSON.")
    submit_bug.add_argument("--title", required=True)
    submit_bug.add_argument("--module", required=True)
    submit_bug.add_argument("--severity", default="P2", choices=["P0", "P1", "P2", "P3"])
    submit_bug.add_argument("--description")
    submit_bug.add_argument("--expected")
    submit_bug.add_argument("--actual")
    submit_bug.add_argument("--reproduce-command")
    submit_bug.add_argument("--evidence-ref", action="append")
    submit_bug.add_argument("--changed-file", action="append")
    submit_bug.add_argument("--plan-key")
    submit_bug.add_argument("--nox-session")
    submit_bug.add_argument("--candidate-type", default="bug", choices=["bug", "regression", "infra_failure", "flaky"])
    submit_bug.add_argument("--bug-id", help="Use an already reserved BUG-NNN id instead of the allocator.")
    submit_bug.add_argument("--github-issue-number")
    submit_bug.add_argument("--github-issue-url")
    submit_bug.add_argument("--create-github", action="store_true", help="Use gh to create the linked GitHub Issue when --apply is set.")
    submit_bug.add_argument("--apply", action="store_true", help="Write candidate/BUG JSON and update allocator after GitHub linkage exists.")
    submit_bug.add_argument("--create-registry-worktree", action="store_true", help="Create a clean registry worktree/branch from origin/main before writing BUG JSON.")
    submit_bug.add_argument("--create-fix-worktree", action="store_true", help="Fast-chain BUG registration into a new fix worktree/branch so the fix PR persists the BUG JSON.")
    submit_bug.add_argument("--registry-pr-only", action="store_true", help="Stop after a registry-only BUG PR; normal workflows continue directly to fix.")
    submit_bug.add_argument("--dry-run", action="store_true", help="Plan registry worktree creation without writing files or creating a worktree.")
    submit_bug.add_argument(
        "--allow-current-worktree",
        action="store_true",
        help="Override the registry guard for emergency/manual use. Normal agent workflows must not use this on canonical main.",
    )
    add_output_options(submit_bug)
    submit_bug.set_defaults(func=cmd_submit_bug)

    install_client = sub.add_parser("install-client", help="Install or dry-run developer-client entry wrappers.")
    install_client.add_argument("--apply", action="store_true")
    install_client.add_argument("--codex-home")
    install_client.add_argument("--claude-home")
    add_output_options(install_client)
    install_client.set_defaults(func=cmd_install_client)

    triage_ci = sub.add_parser("triage-ci-issue", help="Summarize and classify an auto-filed CI/Nightly GitHub Issue.")
    triage_ci.add_argument("--issue", required=True, help="GitHub Issue number to triage.")
    triage_ci.add_argument("--run-id", help="Override or provide the Actions run id.")
    triage_ci.add_argument("--summary-json", help="Use an existing CI failure summary JSON instead of querying Actions.")
    triage_ci.add_argument("--skip-github-summary", action="store_true", help="Do not query Actions logs; emit a partial triage summary.")
    add_output_options(triage_ci)
    triage_ci.set_defaults(func=cmd_triage_ci_issue)

    ci_janitor = sub.add_parser(
        "ci-issue-janitor",
        help="Dry-run or close auto-filed CI issues already superseded by a later successful main run or classified as infra-only.",
    )
    ci_janitor.add_argument("--issue", action="append", help="Limit janitor to a specific GitHub Issue number; repeatable.")
    ci_janitor.add_argument("--limit", type=int, default=50, help="Maximum open auto-filed CI issues to scan when --issue is omitted.")
    ci_janitor.add_argument("--skip-github-summary", action="store_true", help="Do not query Actions logs; useful for tests only.")
    ci_janitor.add_argument(
        "--apply",
        action="store_true",
        help="Close only unlinked issues classified as superseded_by_later_main_success or infra-only.",
    )
    add_output_options(ci_janitor)
    ci_janitor.set_defaults(func=cmd_ci_issue_janitor)

    promote_ci = sub.add_parser("promote-ci-issue", help="Promote a triaged CI GitHub Issue into the BUG JSON workflow.")
    promote_ci.add_argument("--issue", required=True, help="GitHub Issue number to promote.")
    promote_ci.add_argument("--bug-id", help="Use an already reserved BUG-NNN id.")
    promote_ci.add_argument("--summary-json", help="Use an existing CI failure summary JSON instead of querying Actions.")
    promote_ci.add_argument("--skip-github-summary", action="store_true", help="Do not query Actions logs; promote with partial diagnostics.")
    promote_ci.add_argument(
        "--create-registry-worktree",
        action="store_true",
        help="Create a clean registry worktree before writing BUG JSON; required for normal Nightly/CI promotion.",
    )
    promote_ci.add_argument("--apply", action="store_true")
    add_output_options(promote_ci)
    promote_ci.set_defaults(func=cmd_promote_ci_issue)

    run = sub.add_parser("run", help="Run the Phase 1 issue workflow state machine for one BUG.")
    run.add_argument("--bug-id", required=True)
    run.add_argument("--issue-json")
    run.add_argument("--mode", choices=["plan", "fix", "pr", "merge"], default="plan")
    run.add_argument("--changed-file", action="append")
    run.add_argument("--create-worktree", action="store_true")
    run.add_argument("--dry-run", action="store_true")
    run.add_argument("--task-slug")
    run.add_argument("--allow-missing-linkage", action="store_true")
    run.add_argument("--allow-closed", action="store_true")
    run.add_argument("--base", default="origin/main")
    run.add_argument("--head", default="HEAD")
    run.add_argument("--validation-evidence", action="append")
    run.add_argument("--push", action="store_true", help="Push the current task branch after validation evidence exists.")
    run.add_argument("--create-pr", action="store_true", help="Create a GitHub PR from the generated PR body after validation evidence exists.")
    run.add_argument("--watch-ci", action="store_true", help="Watch GitHub PR checks after creating the PR.")
    run.add_argument("--pr-title")
    run.add_argument("--force-new-worktree", action="store_true", help="Override the single-active-workflow guard; requires --reason.")
    run.add_argument("--reason", help="Auditable reason for --force-new-worktree.")
    run.add_argument("--pr-url", help="Merged or merge-ready PR URL for --mode merge.")
    run.add_argument("--merge", action="store_true", help="Explicitly authorize --mode merge to merge the PR when checks are green.")
    run.add_argument("--branch", help="Task branch for post-merge cleanup planning.")
    run.add_argument("--worktree", help="Task worktree for post-merge cleanup planning.")
    run.add_argument("--sync-root", action="store_true", help="Plan canonical root fast-forward after merge.")
    run.add_argument("--production-ddl-gate", default="noop")
    run.add_argument("--production-frontend-dependency-gate", default="noop")
    run.add_argument("--production-backend-dependency-gate", default="noop")
    add_output_options(run)
    run.set_defaults(func=cmd_run)

    resume = sub.add_parser("resume", help="Resume a BUG workflow from state.json/events.jsonl.")
    resume.add_argument("--bug-id", required=True)
    resume.add_argument("--worktree")
    resume.add_argument("--events-limit", type=int, default=8)
    add_output_options(resume)
    resume.set_defaults(func=cmd_resume)

    postmortem = sub.add_parser("postmortem", help="Summarize workflow timing, context cost, active-worktree, and cleanup evidence.")
    postmortem.add_argument("--bug-id", required=True)
    postmortem.add_argument("--worktree")
    postmortem.add_argument("--no-markdown", action="store_true")
    postmortem.add_argument(
        "--persist-artifacts",
        action="store_true",
        help="Write postmortem JSON/Markdown artifacts. Defaults to compact stdout only on successful workflows.",
    )
    add_output_options(postmortem)
    postmortem.set_defaults(func=cmd_postmortem)

    watch_ci = sub.add_parser("watch-ci", help="Refresh compact GitHub PR check state for an existing BUG workflow.")
    watch_ci.add_argument("--bug-id", required=True)
    watch_ci.add_argument("--pr-url", help="PR URL; defaults to state.json pr_url when present.")
    watch_ci.add_argument("--attempts", type=int, default=1)
    watch_ci.add_argument("--delay-seconds", type=int, default=0)
    add_output_options(watch_ci)
    watch_ci.set_defaults(func=cmd_watch_ci)

    start = sub.add_parser("start", help="Prepare a BUG fix workflow and context pack.")
    start.add_argument("--bug-id")
    start.add_argument("--issue-json")
    start.add_argument("--changed-file", action="append")
    start.add_argument("--create-worktree", action="store_true")
    start.add_argument("--dry-run", action="store_true")
    start.add_argument("--task-slug")
    start.add_argument("--allow-missing-linkage", action="store_true")
    start.add_argument("--allow-closed", action="store_true")
    add_output_options(start)
    start.set_defaults(func=cmd_start)

    finish = sub.add_parser("finish", help="Select validation and generate a PR-ready finish plan.")
    finish.add_argument("--bug-id")
    finish.add_argument("--issue-json")
    finish.add_argument("--changed-file", action="append")
    finish.add_argument("--base", default="origin/main")
    finish.add_argument("--head", default="HEAD")
    finish.add_argument("--validation-evidence", action="append")
    finish.add_argument("--plan-only", action="store_true")
    finish.add_argument("--allow-missing-evidence", action="store_true")
    add_output_options(finish)
    finish.set_defaults(func=cmd_finish)

    triage = sub.add_parser("triage-p0", help="List and group open/in-progress P0 BUG records.")
    triage.add_argument("--include-fixed", action="store_true")
    add_output_options(triage)
    triage.set_defaults(func=cmd_triage_p0)

    run_p0 = sub.add_parser("run-p0", help="Plan current P0 handling and recommend the next issue command.")
    run_p0.add_argument("--module")
    run_p0.add_argument("--include-fixed", action="store_true")
    add_output_options(run_p0)
    run_p0.set_defaults(func=cmd_run_p0)

    start_batch = sub.add_parser("start-batch", help="Prepare a same-module batch BUG workflow and context packs.")
    start_batch.add_argument("--bug-id", action="append", required=True)
    start_batch.add_argument("--create-worktree", action="store_true")
    start_batch.add_argument("--dry-run", action="store_true")
    start_batch.add_argument("--task-slug")
    start_batch.add_argument("--allow-missing-linkage", action="store_true")
    start_batch.add_argument("--allow-closed", action="store_true")
    add_output_options(start_batch)
    start_batch.set_defaults(func=cmd_start_batch)

    finish_batch = sub.add_parser("finish-batch", help="Select validation and generate a PR-ready batch finish plan.")
    finish_batch.add_argument("--batch-id")
    finish_batch.add_argument("--bug-id", action="append")
    finish_batch.add_argument("--changed-file", action="append")
    finish_batch.add_argument("--base", default="origin/main")
    finish_batch.add_argument("--head", default="HEAD")
    finish_batch.add_argument("--validation-evidence", action="append")
    finish_batch.add_argument("--issue-commit", action="append", help="Per-issue commit map entry, e.g. BUG-123=<sha>.")
    finish_batch.add_argument("--plan-only", action="store_true")
    finish_batch.add_argument("--allow-missing-evidence", action="store_true")
    add_output_options(finish_batch)
    finish_batch.set_defaults(func=cmd_finish_batch)

    fast_path = sub.add_parser("fast-path", help="Plan the lightest safe T0/T1/T2/T3 issue workflow path.")
    fast_path.add_argument("--bug-id")
    fast_path.add_argument("--issue-json")
    fast_path.add_argument("--changed-file", action="append")
    fast_path.add_argument("--module")
    fast_path.add_argument("--allow-missing-linkage", action="store_true")
    fast_path.add_argument("--allow-closed", action="store_true")
    add_output_options(fast_path)
    fast_path.set_defaults(func=cmd_fast_path)

    workflow_smoke = sub.add_parser("workflow-smoke", help="Dry-run the issue workflow chain without GitHub/PR/DB writes.")
    workflow_smoke.add_argument("--bug-id")
    workflow_smoke.add_argument("--issue-json")
    workflow_smoke.add_argument("--changed-file", action="append")
    workflow_smoke.add_argument("--module")
    add_output_options(workflow_smoke)
    workflow_smoke.set_defaults(func=cmd_workflow_smoke)

    nightly_smoke = sub.add_parser(
        "nightly-intake-smoke",
        help="Dry-run Nightly failure issue context generation without GitHub, DB, runtime, or tracked root writes.",
    )
    add_output_options(nightly_smoke)
    nightly_smoke.set_defaults(func=cmd_nightly_intake_smoke)

    batch_smoke = sub.add_parser(
        "batch-workflow-smoke",
        help="Dry-run same-module batch start/finish without GitHub, DB, runtime, or tracked root writes.",
    )
    add_output_options(batch_smoke)
    batch_smoke.set_defaults(func=cmd_batch_workflow_smoke)

    close = sub.add_parser("close-sync", help="Prepare a dry-run close/sync plan after PR merge.")
    close.add_argument("--bug-id")
    close.add_argument("--issue-json")
    close.add_argument("--pr-url")
    close.add_argument("--apply", action="store_true")
    close.add_argument("--allow-missing-linkage", action="store_true")
    close.add_argument("--validation-evidence", action="append")
    close.add_argument("--merge-commit")
    close.add_argument("--production-ddl-gate", default="noop")
    close.add_argument("--production-frontend-dependency-gate", default="noop")
    close.add_argument("--production-backend-dependency-gate", default="noop")
    close.add_argument("--skip-github-check", action="store_true")
    close.add_argument("--create-registry-worktree", action="store_true")
    close.add_argument(
        "--allow-current-worktree",
        action="store_true",
        help="Override close-sync root/main guard for tests or audited recovery only.",
    )
    add_output_options(close)
    close.set_defaults(func=cmd_close_sync)

    close_batch = sub.add_parser("close-sync-batch", help="Close-sync multiple BUG JSON records in one registry worktree/PR.")
    close_batch.add_argument("--bug-id", action="append", required=True)
    close_batch.add_argument("--pr-url")
    close_batch.add_argument("--apply", action="store_true")
    close_batch.add_argument("--allow-missing-linkage", action="store_true")
    close_batch.add_argument("--validation-evidence", action="append")
    close_batch.add_argument("--merge-commit")
    close_batch.add_argument("--production-ddl-gate", default="noop")
    close_batch.add_argument("--production-frontend-dependency-gate", default="noop")
    close_batch.add_argument("--production-backend-dependency-gate", default="noop")
    close_batch.add_argument("--skip-github-check", action="store_true")
    close_batch.add_argument("--create-registry-worktree", action="store_true")
    close_batch.add_argument("--create-pr", action="store_true", help="Commit, push, and open one close-sync PR for the batch after --apply.")
    close_batch.add_argument(
        "--allow-current-worktree",
        action="store_true",
        help="Override close-sync root/main guard for tests or audited recovery only.",
    )
    add_output_options(close_batch)
    close_batch.set_defaults(func=cmd_close_sync_batch)

    cleanup = sub.add_parser("cleanup-after-merge", help="Safely sync root and clean merged issue worktrees/branches.")
    cleanup.add_argument("--branch", required=True)
    cleanup.add_argument("--bug-id", help="Mark the BUG workflow complete after successful cleanup.")
    cleanup.add_argument("--worktree")
    cleanup.add_argument("--pr-url", help="Merged PR URL used to verify squash-merged branch cleanup.")
    cleanup.add_argument("--sync-root", action="store_true")
    cleanup.add_argument("--canonical-root")
    cleanup.add_argument("--apply", action="store_true")
    add_output_options(cleanup)
    cleanup.set_defaults(func=cmd_cleanup_after_merge)

    finalizer = sub.add_parser("merge-finalizer", help="Finalize a merged issue PR through close-sync, optional close-sync PR merge, cleanup, and postmortem.")
    finalizer.add_argument("--bug-id", action="append", required=True)
    finalizer.add_argument("--issue-json")
    finalizer.add_argument("--source-pr-url", required=True, help="Merged source/fix PR URL.")
    finalizer.add_argument("--source-branch", help="Source/fix PR branch for cleanup.")
    finalizer.add_argument("--source-worktree", help="Source/fix worktree for cleanup.")
    finalizer.add_argument("--validation-evidence", action="append")
    finalizer.add_argument("--allow-missing-linkage", action="store_true")
    finalizer.add_argument("--sync-root", action="store_true", help="Fast-forward the canonical root during cleanup.")
    finalizer.add_argument("--merge-close-sync-pr", action="store_true", help="Also merge the generated close-sync PR when checks are green.")
    finalizer.add_argument("--cleanup", action="store_true", help="Run cleanup-after-merge after close-sync is persisted.")
    finalizer.add_argument("--apply", action="store_true")
    finalizer.add_argument("--production-ddl-gate", default="noop")
    finalizer.add_argument("--production-frontend-dependency-gate", default="noop")
    finalizer.add_argument("--production-backend-dependency-gate", default="noop")
    add_output_options(finalizer)
    finalizer.set_defaults(func=cmd_merge_finalizer)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except WorkflowPayloadError as exc:
        _emit_args(exc.payload, args)
        return 2
    except WorkflowError as exc:
        print(f"aistock_issue_workflow error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

