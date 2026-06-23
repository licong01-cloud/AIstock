from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

BUG_REGISTRY_PREFIX = "tests/aistock_validation/bugs/"
CLOSE_SYNC_STATUSES = {"fixed", "closed", "verified"}
WORKFLOW_BUG_METADATA_STATUSES = {"open", "in_progress", "triaged", *CLOSE_SYNC_STATUSES}
DOCS_ONLY_PREFIXES = ("docs/",)
DOCS_ONLY_ROOT_FILES = {"README.md", "AGENTS.md", "AGENTS.override.md"}
DOCS_FAST_PREFIXES = (
    "docs/architecture/",
    "docs/analysis/",
    "docs/design/",
    "docs/handoff/",
    "docs/operations/",
)
DOCS_FAST_ROOT_FILES = {"README.md"}
DOCS_FAST_OPERATIONS_FILE_PREFIX = "docs/operations_"
DOCS_CONTROLLED_PREFIXES = ("docs/standards/", ".codex/", ".claude/")
DOCS_CONTROLLED_FILES = {"docs/codex_project_memory.md", "AGENTS.md", "AGENTS.override.md"}
# Backwards-compatible names used by the existing GitHub workflows.
DOCS_LIGHT_EXCLUDED_PREFIXES = DOCS_CONTROLLED_PREFIXES[:1]
DOCS_LIGHT_EXCLUDED_FILES = DOCS_CONTROLLED_FILES
WORKFLOW_VALIDATION_FAST_LANE_FILES = {
    ".github/workflows/issue-auto-link.yml",
    ".github/workflows/issue-on-test-fail.yml",
    ".github/workflows/nightly.yml",
    ".github/workflows/dependency-update-validate.yml",
    ".github/workflows/pr-quality.yml",
    ".github/workflows/semgrep.yml",
    ".github/workflows/test.yml",
    ".github/requirements/pr-quality.txt",
    ".github/requirements/semgrep.txt",
    ".claude/commands/fix-aistock-issue.md",
    ".codex/skills/fix-aistock-issue/SKILL.md",
    ".codex/skills/verify-aistock-feature/scripts/scan_quality_guardrails.py",
    "backend/tests/scripts/test_aistock_issue_workflow.py",
    "backend/tests/scripts/test_bug_registry_metadata_check.py",
    "backend/tests/scripts/test_ci_change_classifier.py",
    "backend/tests/scripts/test_ci_failure_issue_summary.py",
    "backend/tests/scripts/test_code_intelligence_adapter.py",
    "backend/tests/scripts/test_issue_flow.py",
    "backend/tests/scripts/test_llm_provider_adapter.py",
    "backend/tests/scripts/test_nightly_adaptive_scheduler.py",
    "backend/tests/scripts/test_nightly_design_drift_audit.py",
    "backend/tests/scripts/test_verify_aistock_feature_guardrail_scan.py",
    "backend/tests/test_aistock_guardrail_scan.py",
    "configs/validation/llm_triage.yaml",
    "configs/validation/design_drift_audit.yaml",
    "docs/architecture/aistock_pr_quality_p0p1_evidence_gate_design_20260602.md",
    "docs/architecture/aistock_issue_workflow_efficiency_hardening_design_v2_2_20260529.md",
    "docs/standards/aistock_issue_workflow_quickstart.md",
    "docs/operations/validation_llm_guarded_rollout_runbook_20260609.md",
    "prompt_packs/validation_llm/evaluation_cases/historical_failure_fixtures.json",
    "prompt_packs/validation_llm/issue_draft.prompt.yml",
    "prompt_packs/validation_llm/nightly_scheduler.prompt.yml",
    "prompt_packs/validation_llm/result_interpreter.prompt.yml",
    "prompt_packs/validation_llm/test_plan_advisor.prompt.yml",
    "prompt_packs/validation_llm/triage_failure.prompt.yml",
    "prompt_packs/validation_llm/design_drift_audit.prompt.yml",
    "scripts/aistock_issue_workflow.py",
    "scripts/aistock_guardrail_scan.py",
    "scripts/bug_registry_metadata_check.py",
    "scripts/ci_change_classifier.py",
    "scripts/ci_failure_issue_summary.py",
    "scripts/code_intelligence_adapter.py",
    "scripts/issue_flow.py",
    "scripts/llm_provider_adapter.py",
    "scripts/nightly_adaptive_scheduler.py",
    "scripts/nightly_design_drift_audit.py",
}
PROMPT_EVALUATION_PATH_PREFIXES = ("prompt_packs/validation_llm/",)
PROMPT_EVALUATION_FILES = {
    "configs/validation/llm_triage.yaml",
    "scripts/llm_provider_adapter.py",
}


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
    return path in WORKFLOW_VALIDATION_FAST_LANE_FILES


def _prompt_evaluation_required(path: str) -> bool:
    return path in PROMPT_EVALUATION_FILES or path.startswith(PROMPT_EVALUATION_PATH_PREFIXES)


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
    if status not in WORKFLOW_BUG_METADATA_STATUSES or module != "validation":
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
    docs_lite_only = bool(normalized) and all(_is_docs_lite_path(path) for path in normalized)
    docs_fast_tier = _docs_fast_tier(normalized, normalized_added)
    docs_controlled_only = bool(normalized) and all(_is_docs_controlled_path(path) for path in normalized)
    docs_controlled_required = any(_is_docs_controlled_path(path) for path in normalized)
    docs_only = bool(normalized) and all(_is_docs_path(path) for path in normalized)

    if not normalized:
        reasons.append("no changed files detected; keep full backend CI")
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
        reasons.append("only fixed/closed/verified BUG JSON metadata changed; backend matrix can be skipped")

    workflow_non_registry_only = (
        bool(non_bug_registry_files)
        and all(_workflow_validation_fast_lane(path) for path in non_bug_registry_files)
    )
    workflow_registry_metadata_only = (
        not bug_registry_files
        or (
            bool(workflow_bug_metadata_files)
            and all(path in workflow_bug_metadata_files or path in allocator_files for path in bug_registry_files)
        )
    )
    workflow_validation_only = (
        bool(normalized)
        and workflow_non_registry_only
        and workflow_registry_metadata_only
    )
    if workflow_validation_only:
        reasons.append("only workflow/validation fast-lane files changed; run focused workflow validation instead of backend matrix")
    if docs_lite_only:
        reasons.append(f"only ordinary documentation files changed; {docs_fast_tier or 'docs_fast'} uses diff/version-change review only")
    if docs_controlled_required:
        reasons.append("controlled documentation or client instructions changed; keep normal workflow guardrails")
    elif docs_only:
        reasons.append("documentation files changed but include standards or agent instructions; keep normal guardrails")
    if prompt_evaluation_files:
        reasons.append("validation LLM prompt/config/provider files changed; run prompt evaluation gate")

    backend_required = not (close_sync_metadata_only or workflow_validation_only or docs_lite_only)
    classification = "full_ci_required"
    if docs_lite_only:
        classification = docs_fast_tier or _docs_lite_kind(normalized)
    elif close_sync_metadata_only:
        classification = "close_sync_metadata_only"
    elif workflow_validation_only:
        classification = "workflow_validation_only"
    elif docs_controlled_only:
        classification = "docs_controlled"
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
        "workflow_validation_only": workflow_validation_only,
        "workflow_validation_required": workflow_validation_only,
        "docs_lite_required": docs_lite_only,
        "prompt_evaluation_files": prompt_evaluation_files,
        "prompt_evaluation_required": bool(prompt_evaluation_files),
        "backend_required": backend_required,
        "static_gate_required": not docs_lite_only,
        "pr_quality_required": True,
        "classification": classification,
        "reasons": reasons,
        "blocking": blocking,
        "workflow_gate": "passed",
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_github_output(path: str, payload: dict[str, Any]) -> None:
    lines = [
        f"backend_required={str(payload['backend_required']).lower()}",
        f"close_sync_metadata_only={str(payload['close_sync_metadata_only']).lower()}",
        f"workflow_validation_required={str(payload['workflow_validation_required']).lower()}",
        f"docs_lite_required={str(payload['docs_lite_required']).lower()}",
        f"docs_fast_required={str(payload['docs_fast_required']).lower()}",
        f"docs_fast_tier={payload['docs_fast_tier'] or ''}",
        f"docs_controlled_required={str(payload['docs_controlled_required']).lower()}",
        f"static_gate_required={str(payload['static_gate_required']).lower()}",
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

    payload = classify_changed_files(_load_changed_files(args), repo_root=Path(args.repo_root), added_files=args.added_file)
    if args.output_json:
        _write_json(Path(args.output_json), payload)
    if args.github_output:
        _write_github_output(args.github_output, payload)
    print(json.dumps({
        "workflow_gate": payload["workflow_gate"],
        "classification": payload["classification"],
        "backend_required": payload["backend_required"],
        "workflow_validation_required": payload["workflow_validation_required"],
        "docs_lite_required": payload["docs_lite_required"],
        "docs_fast_required": payload["docs_fast_required"],
        "docs_fast_tier": payload["docs_fast_tier"],
        "docs_controlled_required": payload["docs_controlled_required"],
        "static_gate_required": payload["static_gate_required"],
        "prompt_evaluation_required": payload["prompt_evaluation_required"],
        "changed_file_count": payload["changed_file_count"],
    }, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
