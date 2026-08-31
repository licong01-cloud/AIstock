from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import issue_flow as flow  # noqa: E402

BUG_REGISTRY_PREFIX = "tests/aistock_validation/bugs/"
CLOSE_SYNC_STATUSES = {
    "fixed",
    "fixed_source_pending_user_restart",
    "closed",
    "verified",
}
WORKFLOW_BUG_METADATA_STATUSES = {
    "open",
    "in_progress",
    "triaged",
    *CLOSE_SYNC_STATUSES,
}
DOCS_ONLY_PREFIXES = ("docs/",)
DOCS_ONLY_ROOT_FILES = {"README.md", "AGENTS.md", "AGENTS.override.md"}
DOCS_FAST_PREFIXES = (
    "docs/architecture/",
    "docs/analysis/",
    "docs/design/",
    "docs/handoff/",
    "docs/operations/",
    "docs/process/",
)
DOCS_FAST_ROOT_FILES = {"README.md"}
DOCS_FAST_OPERATIONS_FILE_PREFIX = "docs/operations_"
DOCS_CONTROLLED_PREFIXES = ("docs/standards/", ".codex/", ".claude/")
DOCS_CONTROLLED_FILES = {"docs/codex_project_memory.md", "AGENTS.md", "AGENTS.override.md"}
# Backwards-compatible names used by the existing GitHub workflows.
DOCS_LIGHT_EXCLUDED_PREFIXES = DOCS_CONTROLLED_PREFIXES[:1]
DOCS_LIGHT_EXCLUDED_FILES = DOCS_CONTROLLED_FILES
WORKFLOW_VALIDATION_FAST_LANE_FILES = {
    "AGENTS.md",
    ".github/workflows/issue-auto-link.yml",
    ".github/workflows/issue-on-guardrail-fail.yml",
    ".github/workflows/issue-on-test-fail.yml",
    ".github/workflows/codeql.yml",
    ".github/workflows/code-intelligence-refresh.yml",
    ".github/workflows/nightly.yml",
    ".github/workflows/dependency-update-validate.yml",
    ".github/workflows/pr-quality.yml",
    ".github/workflows/semgrep.yml",
    ".github/workflows/test.yml",
    ".github/requirements/pr-quality.txt",
    ".github/requirements/semgrep.txt",
    ".claude/commands/aistock-docs-handoff.md",
    ".claude/commands/aistock-feature-workflow.md",
    ".claude/commands/aistock-issue-doctor.md",
    ".claude/commands/aistock-merge-aftercare.md",
    ".claude/commands/aistock-readonly-triage.md",
    ".claude/commands/aistock-task-router.md",
    ".claude/commands/aistock-validation-delegation.md",
    ".claude/commands/fix-aistock-issue.md",
    ".codex/skills/aistock-docs-handoff/SKILL.md",
    ".codex/skills/aistock-merge-aftercare/SKILL.md",
    ".codex/skills/aistock-readonly-triage/SKILL.md",
    ".codex/skills/aistock-task-router/SKILL.md",
    ".codex/skills/aistock-validation-delegation/SKILL.md",
    ".codex/skills/aistock-validation-delegation/agents/openai.yaml",
    ".codex/skills/fix-aistock-issue/SKILL.md",
    ".codex/skills/verify-aistock-feature/SKILL.md",
    ".codex/skills/verify-aistock-feature/scripts/scan_quality_guardrails.py",
    "backend/tests/scripts/test_aistock_issue_workflow.py",
    "backend/tests/scripts/test_aistock_runner_health.py",
    "backend/tests/scripts/test_aistock_mcp_github_issue_tools.py",
    "backend/tests/scripts/test_aistock_feature_workflow.py",
    "backend/tests/scripts/test_bug_registry_metadata_check.py",
    "backend/tests/scripts/test_ci_change_classifier.py",
    "backend/tests/scripts/test_ci_changed_files.py",
    "backend/tests/scripts/test_ci_environment_verify.py",
    "backend/tests/scripts/test_ci_workflow_policy_scan.py",
    "backend/tests/scripts/test_configure_aistock_github_runner.py",
    "backend/tests/scripts/test_ci_failure_issue_summary.py",
    "backend/tests/scripts/test_prepare_self_hosted_workspace.py",
    "backend/tests/scripts/test_code_intelligence_adapter.py",
    "backend/tests/scripts/test_issue_flow.py",
    "backend/tests/scripts/test_issue_flow_pr_quality.py",
    "backend/tests/scripts/test_llm_provider_adapter.py",
    "backend/tests/scripts/test_nightly_session_runner.py",
    "backend/tests/scripts/test_nightly_adaptive_scheduler.py",
    "backend/tests/scripts/test_nightly_design_drift_audit.py",
    "backend/tests/scripts/test_nightly_silent_degradation_audit.py",
    "backend/tests/scripts/test_validate_changed_requirements.py",
    "backend/tests/scripts/test_verify_aistock_feature_guardrail_scan.py",
    "backend/tests/test_aistock_guardrail_scan.py",
    "configs/validation/llm_triage.yaml",
    "configs/validation/design_drift_audit.yaml",
    "configs/validation/silent_degradation_audit.yaml",
    "docs/architecture/aistock_pr_quality_p0p1_evidence_gate_design_20260602.md",
    "docs/architecture/aistock_issue_workflow_efficiency_hardening_design_v2_2_20260529.md",
    "docs/codex_project_memory.md",
    "docs/standards/README.md",
    "docs/standards/aistock_development_standard_v1.5_20260523.md",
    "docs/standards/aistock_development_standard_v1.5_20260523.yaml",
    "docs/standards/aistock_runtime_targets_v1.yaml",
    "docs/standards/aistock_issue_workflow_quickstart.md",
    "docs/operations/validation_llm_guarded_rollout_runbook_20260609.md",
    "prompt_packs/validation_llm/evaluation_cases/historical_failure_fixtures.json",
    "prompt_packs/validation_llm/issue_draft.prompt.yml",
    "prompt_packs/validation_llm/nightly_scheduler.prompt.yml",
    "prompt_packs/validation_llm/result_interpreter.prompt.yml",
    "prompt_packs/validation_llm/test_plan_advisor.prompt.yml",
    "prompt_packs/validation_llm/triage_failure.prompt.yml",
    "prompt_packs/validation_llm/design_drift_audit.prompt.yml",
    "prompt_packs/validation_llm/silent_degradation_audit.prompt.yml",
    "scripts/aistock_issue_workflow.py",
    "scripts/aistock_runner_health.py",
    "backend/tests/scripts/test_aistock_issue_workflow_fast.py",
    "scripts/aistock_bug_id_allocator.py",
    "scripts/aistock_mcp_server.py",
    "scripts/aistock_feature_workflow.py",
    "scripts/aistock_validation_catalog_integrity.py",
    "scripts/aistock_guardrail_scan.py",
    "scripts/bug_registry_metadata_check.py",
    "scripts/ci_change_classifier.py",
    "scripts/ci_changed_files.py",
    "scripts/ci_environment_verify.py",
    "scripts/ci_failure_issue_summary.py",
    "scripts/ci/prepare_self_hosted_workspace.py",
    "scripts/ci_workflow_policy_scan.py",
    "scripts/configure_aistock_github_runner.ps1",
    "scripts/code_intelligence_adapter.py",
    "scripts/issue_flow.py",
    "scripts/llm_provider_adapter.py",
    "scripts/nightly_session_runner.py",
    "scripts/nightly_adaptive_scheduler.py",
    "scripts/nightly_design_drift_audit.py",
    "scripts/nightly_silent_degradation_audit.py",
    "scripts/validate_changed_requirements.py",
    "noxfile.py",
    ".pre-commit-config.yaml",
    ".semgrep.yml",
    "ruff.toml",
    ".github/renovate.json",
}
WORKFLOW_VALIDATION_FAST_LANE_PREFIXES: tuple[str, ...] = ()
WORKFLOW_TEST_TARGETS_BY_FILE: dict[str, tuple[str, ...]] = {
    ".github/workflows/test.yml": ("backend/tests/scripts/test_ci_change_classifier.py",),
    ".github/workflows/pr-quality.yml": ("backend/tests/scripts/test_issue_flow_pr_quality.py",),
    ".github/workflows/semgrep.yml": ("backend/tests/scripts/test_ci_change_classifier.py",),
    "scripts/aistock_issue_workflow.py": ("backend/tests/scripts/test_aistock_issue_workflow_fast.py",),
    "scripts/aistock_runner_health.py": ("backend/tests/scripts/test_aistock_runner_health.py",),
    "backend/tests/scripts/test_aistock_issue_workflow_fast.py": (
        "backend/tests/scripts/test_aistock_issue_workflow_fast.py",
    ),
    "scripts/aistock_bug_id_allocator.py": (
        "backend/tests/scripts/test_aistock_issue_workflow_fast.py",
        "backend/tests/scripts/test_aistock_mcp_github_issue_tools.py",
    ),
    "scripts/aistock_mcp_server.py": ("backend/tests/scripts/test_aistock_mcp_github_issue_tools.py",),
    "scripts/aistock_feature_workflow.py": ("backend/tests/scripts/test_aistock_feature_workflow.py",),
    "scripts/aistock_guardrail_scan.py": ("backend/tests/test_aistock_guardrail_scan.py",),
    "docs/standards/aistock_development_standard_v1.5_20260523.md": ("backend/tests/test_aistock_guardrail_scan.py",),
    "docs/standards/aistock_development_standard_v1.5_20260523.yaml": ("backend/tests/test_aistock_guardrail_scan.py",),
    "docs/standards/aistock_runtime_targets_v1.yaml": (
        "backend/tests/test_aistock_guardrail_scan.py",
        "backend/tests/scripts/test_aistock_issue_workflow.py",
    ),
    "scripts/bug_registry_metadata_check.py": ("backend/tests/scripts/test_bug_registry_metadata_check.py",),
    "scripts/ci_change_classifier.py": ("backend/tests/scripts/test_ci_change_classifier.py",),
    "scripts/ci_changed_files.py": ("backend/tests/scripts/test_ci_changed_files.py",),
    "scripts/ci_environment_verify.py": ("backend/tests/scripts/test_ci_environment_verify.py",),
    "scripts/ci_failure_issue_summary.py": ("backend/tests/scripts/test_ci_failure_issue_summary.py",),
    "scripts/ci/prepare_self_hosted_workspace.py": ("backend/tests/scripts/test_prepare_self_hosted_workspace.py",),
    "scripts/ci_workflow_policy_scan.py": ("backend/tests/scripts/test_ci_workflow_policy_scan.py",),
    "scripts/configure_aistock_github_runner.ps1": ("backend/tests/scripts/test_configure_aistock_github_runner.py",),
    "scripts/code_intelligence_adapter.py": ("backend/tests/scripts/test_code_intelligence_adapter.py",),
    "scripts/issue_flow.py": (
        "backend/tests/scripts/test_issue_flow.py",
        "backend/tests/scripts/test_issue_flow_pr_quality.py",
    ),
    "scripts/llm_provider_adapter.py": ("backend/tests/scripts/test_llm_provider_adapter.py",),
    "scripts/nightly_session_runner.py": ("backend/tests/scripts/test_nightly_session_runner.py",),
    "scripts/nightly_adaptive_scheduler.py": ("backend/tests/scripts/test_nightly_adaptive_scheduler.py",),
    "scripts/nightly_design_drift_audit.py": ("backend/tests/scripts/test_nightly_design_drift_audit.py",),
    "scripts/nightly_silent_degradation_audit.py": ("backend/tests/scripts/test_nightly_silent_degradation_audit.py",),
    "scripts/validate_changed_requirements.py": ("backend/tests/scripts/test_validate_changed_requirements.py",),
    "noxfile.py": ("backend/tests/test_noxfile_validation_env.py",),
}
WORKFLOW_AUTHORITY_PREFIXES = (".codex/skills/", ".claude/commands/")
WORKFLOW_AUTHORITY_FILES = {
    "docs/codex_project_memory.md",
    "docs/standards/README.md",
    "docs/standards/aistock_issue_workflow_quickstart.md",
}
CATALOG_VALIDATION_FILES = {
    "noxfile.py",
    "backend/services/validation/catalog_integrity.py",
    "backend/services/validation/file_ownership.py",
    "backend/services/validation/module_registry.py",
    "backend/services/validation/plan_catalog.py",
    "backend/services/validation/ui_target_catalog.py",
    "backend/tests/test_validation_catalog_integrity.py",
    "backend/tests/test_validation_module_ownership.py",
    "backend/tests/test_noxfile_validation_env.py",
    "backend/tests/test_validation_ui_target_catalog.py",
    "scripts/aistock_module_ownership_scan.py",
}
CATALOG_VALIDATION_PREFIXES = ("tests/aistock_validation/catalog/",)
SHARED_PLAN_KEYS = {"l0", "guardrail_changed_files", "validation_module_registry_l0"}
PROMPT_EVALUATION_PATH_PREFIXES = ("prompt_packs/validation_llm/",)
PROMPT_EVALUATION_FILES = {
    "configs/validation/llm_triage.yaml",
    "scripts/llm_provider_adapter.py",
}
DIRECT_BACKEND_PLAN_KEYS_BY_FILE = {
    "backend/db/pg_pool.py": ("platform_api_backend",),
    "backend/tests/test_pg_pool_audit.py": ("platform_api_backend",),
    "backend/tests/test_validation_ui_target_catalog.py": ("validation_center_backend",),
    "scripts/advisory_p0k_build_training_request.py": ("advisory_modeling_backend",),
    "scripts/wsl/advisory_p0k_train.py": ("advisory_modeling_backend",),
    "scripts/advisory_p0l_build_training_request.py": ("advisory_modeling_backend",),
    "scripts/wsl/advisory_p0l_train.py": ("advisory_modeling_backend",),
    "scripts/advisory_n1_tier1_oracle_learnability.py": ("advisory_modeling_backend",),
    "scripts/advisory_strategy_package_alpha_audit.py": ("advisory_modeling_backend",),
}
FRONTEND_PATH_PREFIXES = ("frontend/src/", "frontend/tests/", "frontend/e2e/")
FRONTEND_FILES = {
    "frontend/package.json",
    "frontend/package-lock.json",
    "frontend/playwright.config.ts",
    "frontend/playwright.paper-v2.config.ts",
    "frontend/tsconfig.json",
    "frontend/next.config.mjs",
}
GO_PATH_PREFIXES = ("tdx-api-main/",)
GO_FILES = {"tdx-api-main/go.mod", "tdx-api-main/go.sum"}
CODE_SUFFIXES = (".py", ".pyi", ".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs", ".go", ".sql", ".sh", ".ps1")
CODE_ROOT_FILES = {"noxfile.py", "pyproject.toml", "pytest.ini"}


def _normalize_path(value: str) -> str:
    normalized = value.replace("\\", "/").strip()
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized


def _load_changed_files(args: argparse.Namespace) -> list[str]:
    files: list[str] = []
    for item in args.changed_file or []:
        files.append(_normalize_path(item))
    if args.changed_files_file:
        path = Path(args.changed_files_file)
        if path.exists():
            files.extend(_normalize_path(line) for line in path.read_text(encoding="utf-8").splitlines())
    seen: set[str] = set()
    result: list[str] = []
    for item in files:
        if item and item not in seen:
            result.append(item)
            seen.add(item)
    return result


def _read_bug_payload(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _bug_status(path: Path) -> str | None:
    payload = _read_bug_payload(path)
    if payload is None:
        return None
    status = payload.get("status")
    return str(status).strip().lower() if status is not None else None


def _workflow_validation_fast_lane(path: str) -> bool:
    return (
        path in WORKFLOW_VALIDATION_FAST_LANE_FILES
        or path.startswith(WORKFLOW_VALIDATION_FAST_LANE_PREFIXES)
        or _is_docs_fast_path(path)
    )


def _prompt_evaluation_required(path: str) -> bool:
    return path in PROMPT_EVALUATION_FILES or path.startswith(PROMPT_EVALUATION_PATH_PREFIXES)


def _catalog_validation_required(path: str) -> bool:
    return path in CATALOG_VALIDATION_FILES or path.startswith(CATALOG_VALIDATION_PREFIXES)


def _workflow_test_targets(paths: list[str]) -> list[str]:
    targets: list[str] = []
    for path in paths:
        path_targets = list(WORKFLOW_TEST_TARGETS_BY_FILE.get(path, ()))
        if (
            path.startswith("backend/tests/")
            and path.endswith(".py")
            and not (
                path == "backend/tests/scripts/test_aistock_issue_workflow.py"
                and "scripts/aistock_issue_workflow.py" in paths
            )
        ):
            path_targets.append(path)
        if path in WORKFLOW_AUTHORITY_FILES or path.startswith(WORKFLOW_AUTHORITY_PREFIXES):
            path_targets.extend(
                [
                    "backend/tests/scripts/test_issue_flow.py",
                    "backend/tests/scripts/test_aistock_issue_workflow_fast.py",
                ]
            )
        for target in path_targets:
            if target not in targets:
                targets.append(target)
    return targets


def _plan_requires_dev_db(plan: dict[str, Any]) -> bool:
    """Return whether a validation plan must use the existing DEV database.

    CI must never create a disposable database.  Plans that write database
    state or explicitly declare a database resource are therefore routed out
    of the ordinary backend matrix and reported as DEV-DB work instead.
    """
    if bool(plan.get("requires_dev_db")):
        return True
    if bool(plan.get("writes_database")):
        return True
    resource_policy = plan.get("resource_policy")
    if isinstance(resource_policy, str) and any(
        token in resource_policy.strip().lower() for token in ("postgres", "timescale", "database", "dev_db", "_db")
    ):
        return True
    if isinstance(resource_policy, dict):
        if bool(resource_policy.get("ddl_idempotency_real_postgres")):
            return True
        resource_types = resource_policy.get("resource_types") or []
        if isinstance(resource_types, str):
            resource_types = [resource_types]
        for resource_type in resource_types:
            normalized = str(resource_type).strip().lower()
            if "postgres" in normalized or normalized.endswith("database") or normalized.endswith("_db"):
                return True
    return False


def _backend_sessions_from_selection(selection: dict[str, Any], plans: dict[str, dict[str, Any]]) -> list[str]:
    sessions: list[str] = []
    for plan_key in selection.get("required_plans") or []:
        plan = plans.get(str(plan_key)) or {}
        session = str(plan.get("nox_session") or "").strip()
        ci_enabled = bool(plan.get("ci_enabled", plan.get("enabled", True)))
        runner_enabled = bool(plan.get("runner_enabled", True))
        if (
            plan.get("ci_lane") != "backend"
            or not ci_enabled
            or not runner_enabled
            or _plan_requires_dev_db(plan)
            or not session
            or session in sessions
        ):
            continue
        sessions.append(session)
    return sessions


def _dev_db_plan_keys(selection: dict[str, Any], plans: dict[str, dict[str, Any]]) -> list[str]:
    plan_keys: list[str] = []
    for plan_key in selection.get("required_plans") or []:
        normalized = str(plan_key)
        plan = plans.get(normalized) or {}
        if _plan_requires_dev_db(plan) and normalized not in plan_keys:
            plan_keys.append(normalized)
    return plan_keys


def _plan_routing(plan_key: str, plan: dict[str, Any]) -> dict[str, Any]:
    """Return the runner and database contract for a selected plan."""
    requires_dev_db = _plan_requires_dev_db(plan)
    runner_kind = (
        "windows_ai_stock_ci"
        if requires_dev_db or plan.get("ci_lane") in {"backend", "frontend", "go"}
        else "hosted_static"
    )
    return {
        "plan_key": plan_key,
        "runner_kind": runner_kind,
        "requires_dev_db": requires_dev_db,
        "environment_fingerprint_ref": "AIstock-CI" if runner_kind == "windows_ai_stock_ci" else None,
        "install_forbidden": True,
    }


def _catalog_backend_selection(paths: list[str]) -> dict[str, Any]:
    plans = flow._plans_by_key()
    selection = flow.select_validation(paths)
    selected_plan_keys: list[str] = []
    dev_db_plan_keys: list[str] = []
    frontend_test_targets: list[str] = []
    mapped_files: list[str] = []
    unmapped_files: list[str] = []
    for path in paths:
        path_selection = flow.select_validation([path])
        required_plans = [str(item) for item in path_selection.get("required_plans") or []]
        if _is_frontend_path(path):
            required_plans.extend(
                str(plan_key)
                for plan_key in path_selection.get("recommended_plans") or []
                if (plans.get(str(plan_key)) or {}).get("ci_lane") == "frontend"
            )
        for plan_key in DIRECT_BACKEND_PLAN_KEYS_BY_FILE.get(path, ()):
            if plan_key not in required_plans:
                required_plans.append(plan_key)
        for plan_key in required_plans:
            if plan_key not in selected_plan_keys:
                selected_plan_keys.append(plan_key)
            plan = plans.get(plan_key) or {}
            if _plan_requires_dev_db(plan) and plan_key not in dev_db_plan_keys:
                dev_db_plan_keys.append(plan_key)
            target = str(plan.get("frontend_test_path") or "").strip()
            if plan.get("ci_lane") == "frontend" and target and target not in frontend_test_targets:
                frontend_test_targets.append(target)
        has_related_deferred_plan = any(
            plan_key not in SHARED_PLAN_KEYS
            and bool((plans.get(plan_key) or {}).get("enabled", True))
            and (
                bool((plans.get(plan_key) or {}).get("runner_enabled", True))
                or _plan_requires_dev_db(plans.get(plan_key) or {})
            )
            for plan_key in required_plans
        )
        if _backend_sessions_from_selection({"required_plans": required_plans}, plans) or has_related_deferred_plan:
            mapped_files.append(path)
        elif _is_code_path(path):
            unmapped_files.append(path)
    sessions = _backend_sessions_from_selection({"required_plans": selected_plan_keys}, plans)
    dev_db_plan_keys = _dev_db_plan_keys({"required_plans": selected_plan_keys}, plans)
    return {
        "selected_plan_keys": selected_plan_keys,
        "backend_sessions": sessions,
        "dev_db_plan_keys": dev_db_plan_keys,
        "frontend_test_targets": frontend_test_targets,
        "mapped_files": mapped_files,
        "unmapped_code_files": unmapped_files,
        "impacted_modules": selection.get("impacted_modules") or [],
        "required_plans": selected_plan_keys,
    }


def _is_frontend_path(path: str) -> bool:
    return path in FRONTEND_FILES or path.startswith(FRONTEND_PATH_PREFIXES)


def _is_go_path(path: str) -> bool:
    return path in GO_FILES or (path.startswith(GO_PATH_PREFIXES) and path.endswith(".go"))


def _is_code_path(path: str) -> bool:
    if path in CODE_ROOT_FILES:
        return True
    if path.startswith(".github/workflows/"):
        return True
    if path.startswith(("backend/", "frontend/", "scripts/", "tests/", "rl_execution/", "tdx-api-main/")):
        return path.endswith(CODE_SUFFIXES) or Path(path).name in {
            "package.json",
            "package-lock.json",
            "go.mod",
            "go.sum",
        }
    return False


def _codeql_language(path: str) -> str | None:
    suffix = Path(path).suffix.lower()
    if suffix == ".py":
        return "python"
    if suffix in {".js", ".jsx", ".ts", ".tsx"}:
        return "javascript-typescript"
    return None


def _is_test_source_path(path: str) -> bool:
    normalized = path.replace("\\", "/").lower()
    parts = normalized.split("/")
    name = parts[-1]
    return (
        any(part in {"test", "tests", "__tests__", "e2e"} for part in parts[:-1])
        or name.startswith("test_")
        or name.endswith("_test.py")
        or ".test." in name
        or ".spec." in name
    )


def _codeql_languages(paths: list[str], *, exclude_test_sources: bool) -> list[str]:
    observed = {
        language
        for path in paths
        if not (exclude_test_sources and _is_test_source_path(path))
        if (language := _codeql_language(path)) is not None
    }
    return [language for language in ("python", "javascript-typescript") if language in observed]


def _is_bug_registry_metadata_path(path: str) -> bool:
    return path.startswith(BUG_REGISTRY_PREFIX)


def _is_docs_path(path: str) -> bool:
    return path.startswith(DOCS_ONLY_PREFIXES) or path in DOCS_ONLY_ROOT_FILES


def _is_docs_controlled_path(path: str) -> bool:
    return path in DOCS_CONTROLLED_FILES or path.startswith(DOCS_CONTROLLED_PREFIXES)


def _is_docs_fast_path(path: str) -> bool:
    if path in DOCS_FAST_ROOT_FILES:
        return True
    if path.startswith(DOCS_FAST_OPERATIONS_FILE_PREFIX):
        return True
    return path.startswith(DOCS_FAST_PREFIXES)


def _is_docs_lite_path(path: str) -> bool:
    return _is_docs_fast_path(path)


def _docs_fast_tier(paths: list[str], added_files: list[str] | None = None) -> str | None:
    if not paths or not all(_is_docs_fast_path(path) for path in paths):
        return None
    added = set(added_files or [])
    return "docs_fast_new" if any(path in added for path in paths) else "docs_fast_update"


def _docs_lite_kind(paths: list[str]) -> str:
    if any(path.startswith("docs/architecture/") for path in paths):
        return "design_docs_only"
    if any(path.startswith("docs/operations/") or path.startswith("docs/operations_") for path in paths):
        return "operations_docs_only"
    if any(path.startswith("docs/analysis/") for path in paths):
        return "analysis_docs_only"
    return "documentation_only"


def _workflow_bug_metadata_file(rel_path: str, *, repo_root: Path) -> bool:
    if Path(rel_path).name.startswith(".") or not rel_path.endswith(".json"):
        return False
    payload = _read_bug_payload(repo_root / rel_path)
    if not payload:
        return False
    status = str(payload.get("status") or "").strip().lower()
    module = str(payload.get("module") or payload.get("affected_module") or "").strip().lower()
    if status not in WORKFLOW_BUG_METADATA_STATUSES or module not in {"validation", "validation_llm_pipeline"}:
        return False
    allowed_scope = payload.get("allowed_write_scope") or payload.get("suggested_scope") or []
    if not isinstance(allowed_scope, list) or not allowed_scope:
        return False
    for raw_path in allowed_scope:
        scope_path = _normalize_path(str(raw_path))
        if not scope_path:
            continue
        if _is_bug_registry_metadata_path(scope_path):
            continue
        if not _workflow_validation_fast_lane(scope_path):
            return False
    return True


def classify_changed_files(
    changed_files: list[str],
    *,
    repo_root: Path | None = None,
    added_files: list[str] | None = None,
) -> dict[str, Any]:
    repo_root = repo_root or Path.cwd()
    normalized = [_normalize_path(item) for item in changed_files if _normalize_path(item)]
    normalized_added = [_normalize_path(item) for item in (added_files or []) if _normalize_path(item)]
    reasons: list[str] = []
    blocking: list[str] = []
    bug_registry_files = [path for path in normalized if path.startswith(BUG_REGISTRY_PREFIX)]
    non_bug_registry_files = [path for path in normalized if not path.startswith(BUG_REGISTRY_PREFIX)]
    prompt_evaluation_files = [path for path in normalized if _prompt_evaluation_required(path)]
    catalog_validation_required = any(_catalog_validation_required(path) for path in normalized)
    docs_lite_only = bool(normalized) and all(_is_docs_lite_path(path) for path in normalized)
    docs_fast_tier = _docs_fast_tier(normalized, normalized_added)
    docs_controlled_only = bool(normalized) and all(_is_docs_controlled_path(path) for path in normalized)
    docs_controlled_required = any(_is_docs_controlled_path(path) for path in normalized)
    docs_only = bool(normalized) and all(_is_docs_path(path) for path in normalized)
    if not normalized:
        blocking.append("no changed files detected; refusing to invent an unrelated test matrix")
    if non_bug_registry_files:
        reasons.append("non-registry files changed; check fast-lane allowlist before skipping backend matrix")
    if not bug_registry_files:
        reasons.append("no BUG registry metadata file changed")

    metadata_statuses: dict[str, str | None] = {}
    workflow_bug_metadata_files: list[str] = []
    allocator_files: list[str] = []
    metadata_only = bool(normalized) and not non_bug_registry_files and bool(bug_registry_files)
    close_sync_metadata_only = metadata_only
    for rel_path in bug_registry_files:
        path = repo_root / rel_path
        if Path(rel_path).name.startswith("."):
            allocator_files.append(rel_path)
            close_sync_metadata_only = False
            reasons.append(f"allocator or hidden registry metadata changed: {rel_path}")
            continue
        if not rel_path.endswith(".json"):
            close_sync_metadata_only = False
            reasons.append(f"non-json BUG registry file changed: {rel_path}")
            continue
        status = _bug_status(path)
        metadata_statuses[rel_path] = status
        if status not in CLOSE_SYNC_STATUSES:
            close_sync_metadata_only = False
            reasons.append(f"BUG registry status is not close-sync metadata: {rel_path} status={status or 'unknown'}")
        if _workflow_bug_metadata_file(rel_path, repo_root=repo_root):
            workflow_bug_metadata_files.append(rel_path)

    if close_sync_metadata_only:
        reasons.append("only close-sync BUG JSON metadata changed; backend matrix can be skipped")

    workflow_fast_files = [
        path for path in non_bug_registry_files if _workflow_validation_fast_lane(path) and not _is_docs_fast_path(path)
    ]
    workflow_test_targets = _workflow_test_targets(workflow_fast_files)
    frontend_files = [path for path in non_bug_registry_files if _is_frontend_path(path)]
    go_files = [path for path in non_bug_registry_files if _is_go_path(path)]
    business_files = [
        path
        for path in non_bug_registry_files
        if path not in workflow_fast_files
        and path not in go_files
        and not _is_docs_path(path)
        and not _catalog_validation_required(path)
    ]
    catalog_selection = _catalog_backend_selection(business_files)
    selected_plan_keys = catalog_selection["selected_plan_keys"]
    backend_sessions = catalog_selection["backend_sessions"]
    dev_db_plan_keys = catalog_selection["dev_db_plan_keys"]
    frontend_test_targets = catalog_selection["frontend_test_targets"]
    mapped_backend_files = catalog_selection["mapped_files"]
    unmapped_code_files = catalog_selection["unmapped_code_files"]
    if unmapped_code_files:
        blocking.append(
            "unmapped executable code must declare a direct CI test mapping: " + ", ".join(unmapped_code_files)
        )

    workflow_validation_required = bool(workflow_test_targets)
    workflow_validation_only = (
        bool(normalized)
        and bool(workflow_fast_files)
        and all(
            path in workflow_fast_files or _catalog_validation_required(path) or _is_docs_path(path)
            for path in non_bug_registry_files
        )
        and not business_files
        and not frontend_files
        and not go_files
    )
    if workflow_validation_only:
        reasons.append(
            "only workflow/validation fast-lane files changed; run focused workflow validation instead of backend matrix"
        )
    if docs_lite_only:
        reasons.append(
            f"only ordinary documentation files changed; {docs_fast_tier or 'docs_fast'} uses diff/version-change review only"
        )
    if docs_controlled_required:
        reasons.append("controlled documentation or client instructions changed; keep normal workflow guardrails")
    elif docs_only:
        reasons.append(
            "documentation files changed but include standards or agent instructions; keep normal guardrails"
        )
    if prompt_evaluation_files:
        reasons.append("validation LLM prompt/config/provider files changed; run prompt evaluation gate")
    if backend_sessions:
        reasons.append("backend code matched direct nox sessions: " + ", ".join(backend_sessions))
    if dev_db_plan_keys:
        reasons.append("database validation must use the existing DEV database: " + ", ".join(dev_db_plan_keys))
    if frontend_files:
        reasons.append("frontend code changed; run the single frontend type/lint gate")
    if go_files:
        reasons.append("TDX Go code changed; run the Go unit-test gate")

    backend_required = bool(backend_sessions) and not docs_lite_only and not close_sync_metadata_only
    dev_db_required = bool(dev_db_plan_keys) and not docs_lite_only and not close_sync_metadata_only
    frontend_required = bool(frontend_files) and not docs_lite_only
    go_required = bool(go_files) and not docs_lite_only
    classification = "full_ci_required"
    if blocking:
        classification = "unmapped_code_blocked"
    elif docs_lite_only:
        classification = docs_fast_tier or _docs_lite_kind(normalized)
    elif close_sync_metadata_only:
        classification = "close_sync_metadata_only"
    elif metadata_only:
        classification = "bug_registry_metadata_only"
    elif docs_controlled_only:
        classification = "docs_controlled"
    elif workflow_validation_only:
        classification = "workflow_validation_only"
    elif dev_db_required and not backend_required and not frontend_required and not go_required:
        classification = "dev_db_validation_required"
    elif catalog_validation_required and not business_files and not frontend_files and not go_files:
        classification = "catalog_validation_only"
    elif frontend_required and not backend_required and not go_required:
        classification = "frontend_ci_required"
    elif go_required and not backend_required and not frontend_required:
        classification = "go_ci_required"
    elif backend_required or frontend_required or go_required:
        classification = "targeted_ci_required"
    plans = flow._plans_by_key()
    plan_routing = [_plan_routing(plan_key, plans.get(plan_key) or {}) for plan_key in selected_plan_keys]
    runner_kind = (
        "windows_ai_stock_ci"
        if backend_required or frontend_required or go_required or dev_db_required
        else "hosted_static"
    )
    codeql_languages = _codeql_languages(normalized, exclude_test_sources=False)
    codeql_pr_languages = _codeql_languages(normalized, exclude_test_sources=True)
    return {
        "schema_version": "aistock_ci_change_classifier_v1",
        "changed_files": normalized,
        "changed_file_count": len(normalized),
        "bug_registry_files": bug_registry_files,
        "non_bug_registry_files": non_bug_registry_files,
        "metadata_statuses": metadata_statuses,
        "metadata_only": metadata_only,
        "docs_only": docs_only,
        "docs_lite_only": docs_lite_only,
        "docs_fast_tier": docs_fast_tier,
        "docs_fast_required": docs_lite_only,
        "docs_controlled_only": docs_controlled_only,
        "docs_controlled_required": docs_controlled_required,
        "close_sync_metadata_only": close_sync_metadata_only,
        "workflow_bug_metadata_files": workflow_bug_metadata_files,
        "workflow_fast_files": workflow_fast_files,
        "workflow_validation_only": workflow_validation_only,
        "workflow_validation_required": workflow_validation_required,
        "workflow_test_targets": workflow_test_targets,
        "docs_lite_required": docs_lite_only,
        "prompt_evaluation_files": prompt_evaluation_files,
        "prompt_evaluation_required": bool(prompt_evaluation_files),
        "backend_required": backend_required,
        "backend_sessions": backend_sessions,
        "dev_db_required": dev_db_required,
        "dev_db_plan_keys": dev_db_plan_keys,
        "runner_kind": runner_kind,
        "plan_routing": plan_routing,
        "environment_fingerprint_ref": "AIstock-CI" if runner_kind == "windows_ai_stock_ci" else None,
        "install_forbidden": True,
        "backend_plan_keys": catalog_selection["required_plans"],
        "selected_plan_keys": selected_plan_keys,
        "catalog_impacted_modules": catalog_selection["impacted_modules"],
        "mapped_backend_files": mapped_backend_files,
        "frontend_required": frontend_required,
        "frontend_test_targets": frontend_test_targets,
        "frontend_files": frontend_files,
        "go_required": go_required,
        "go_files": go_files,
        "codeql_languages": codeql_languages,
        "codeql_pr_languages": codeql_pr_languages,
        "codeql_pr_test_only": bool(codeql_languages) and not codeql_pr_languages,
        "unmapped_code_files": unmapped_code_files,
        "obsolete_surface_removal": False,
        "nightly_deferred_verification": {
            "required": False,
            "reason": None,
        },
        "static_gate_required": not docs_lite_only,
        "catalog_validation_required": catalog_validation_required,
        "pr_quality_required": True,
        "classification": classification,
        "reasons": reasons,
        "blocking": blocking,
        "workflow_gate": "blocked" if blocking else "passed",
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_github_output(path: str, payload: dict[str, Any]) -> None:
    lines = [
        f"backend_required={str(payload['backend_required']).lower()}",
        f"backend_sessions={json.dumps(payload['backend_sessions'])}",
        f"dev_db_required={str(payload['dev_db_required']).lower()}",
        f"dev_db_plan_keys={json.dumps(payload['dev_db_plan_keys'])}",
        f"runner_kind={payload['runner_kind']}",
        f"plan_routing={json.dumps(payload['plan_routing'])}",
        f"environment_fingerprint_ref={payload['environment_fingerprint_ref'] or 'not_applicable'}",
        f"install_forbidden={str(payload['install_forbidden']).lower()}",
        f"frontend_required={str(payload['frontend_required']).lower()}",
        f"frontend_test_targets={json.dumps(payload['frontend_test_targets'])}",
        f"go_required={str(payload['go_required']).lower()}",
        f"unmapped_code_files={json.dumps(payload['unmapped_code_files'])}",
        f"close_sync_metadata_only={str(payload['close_sync_metadata_only']).lower()}",
        f"workflow_validation_required={str(payload['workflow_validation_required']).lower()}",
        f"workflow_test_targets={json.dumps(payload['workflow_test_targets'])}",
        f"docs_lite_required={str(payload['docs_lite_required']).lower()}",
        f"docs_fast_required={str(payload['docs_fast_required']).lower()}",
        f"docs_fast_tier={payload['docs_fast_tier'] or ''}",
        f"docs_controlled_required={str(payload['docs_controlled_required']).lower()}",
        f"static_gate_required={str(payload['static_gate_required']).lower()}",
        f"catalog_validation_required={str(payload['catalog_validation_required']).lower()}",
        f"prompt_evaluation_required={str(payload['prompt_evaluation_required']).lower()}",
        f"classification={payload['classification']}",
    ]
    with open(path, "a", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Classify AIstock CI changed files for safe fast lanes.")
    parser.add_argument("--changed-file", action="append", default=[])
    parser.add_argument("--added-file", action="append", default=[])
    parser.add_argument("--changed-files-file")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--output-json")
    parser.add_argument("--github-output", default=os.environ.get("GITHUB_OUTPUT"))
    args = parser.parse_args(argv)

    payload = classify_changed_files(
        _load_changed_files(args), repo_root=Path(args.repo_root), added_files=args.added_file
    )
    if args.output_json:
        _write_json(Path(args.output_json), payload)
    if args.github_output:
        _write_github_output(args.github_output, payload)
    print(
        json.dumps(
            {
                "workflow_gate": payload["workflow_gate"],
                "classification": payload["classification"],
                "backend_required": payload["backend_required"],
                "backend_sessions": payload["backend_sessions"],
                "dev_db_required": payload["dev_db_required"],
                "dev_db_plan_keys": payload["dev_db_plan_keys"],
                "runner_kind": payload["runner_kind"],
                "plan_routing": payload["plan_routing"],
                "environment_fingerprint_ref": payload["environment_fingerprint_ref"],
                "install_forbidden": payload["install_forbidden"],
                "frontend_required": payload["frontend_required"],
                "frontend_test_targets": payload["frontend_test_targets"],
                "go_required": payload["go_required"],
                "unmapped_code_files": payload["unmapped_code_files"],
                "workflow_validation_required": payload["workflow_validation_required"],
                "workflow_test_targets": payload["workflow_test_targets"],
                "docs_lite_required": payload["docs_lite_required"],
                "docs_fast_required": payload["docs_fast_required"],
                "docs_fast_tier": payload["docs_fast_tier"],
                "docs_controlled_required": payload["docs_controlled_required"],
                "static_gate_required": payload["static_gate_required"],
                "catalog_validation_required": payload["catalog_validation_required"],
                "prompt_evaluation_required": payload["prompt_evaluation_required"],
                "changed_file_count": payload["changed_file_count"],
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 2 if payload["workflow_gate"] == "blocked" else 0


if __name__ == "__main__":
    raise SystemExit(main())
