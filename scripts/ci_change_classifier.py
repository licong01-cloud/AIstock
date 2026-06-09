from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

BUG_REGISTRY_PREFIX = "tests/aistock_validation/bugs/"
CLOSE_SYNC_STATUSES = {"fixed", "closed", "verified"}
WORKFLOW_BUG_METADATA_STATUSES = {"open", "in_progress", "triaged", *CLOSE_SYNC_STATUSES}
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
    "backend/tests/scripts/test_aistock_issue_workflow.py",
    "backend/tests/scripts/test_ci_change_classifier.py",
    "backend/tests/scripts/test_ci_failure_issue_summary.py",
    "backend/tests/scripts/test_code_intelligence_adapter.py",
    "backend/tests/scripts/test_issue_flow.py",
    "backend/tests/scripts/test_llm_provider_adapter.py",
    "backend/tests/scripts/test_nightly_adaptive_scheduler.py",
    "configs/validation/llm_triage.yaml",
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
    "scripts/aistock_issue_workflow.py",
    "scripts/ci_change_classifier.py",
    "scripts/ci_failure_issue_summary.py",
    "scripts/code_intelligence_adapter.py",
    "scripts/issue_flow.py",
    "scripts/llm_provider_adapter.py",
    "scripts/nightly_adaptive_scheduler.py",
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


def _is_bug_registry_metadata_path(path: str) -> bool:
    return path.startswith(BUG_REGISTRY_PREFIX)


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


def classify_changed_files(changed_files: list[str], *, repo_root: Path | None = None) -> dict[str, Any]:
    repo_root = repo_root or Path.cwd()
    normalized = [_normalize_path(item) for item in changed_files if _normalize_path(item)]
    reasons: list[str] = []
    blocking: list[str] = []
    bug_registry_files = [path for path in normalized if path.startswith(BUG_REGISTRY_PREFIX)]
    non_bug_registry_files = [path for path in normalized if not path.startswith(BUG_REGISTRY_PREFIX)]

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

    backend_required = not (close_sync_metadata_only or workflow_validation_only)
    classification = "full_ci_required"
    if close_sync_metadata_only:
        classification = "close_sync_metadata_only"
    elif workflow_validation_only:
        classification = "workflow_validation_only"
    return {
        "schema_version": "aistock_ci_change_classifier_v1",
        "changed_files": normalized,
        "changed_file_count": len(normalized),
        "bug_registry_files": bug_registry_files,
        "non_bug_registry_files": non_bug_registry_files,
        "metadata_statuses": metadata_statuses,
        "metadata_only": metadata_only,
        "close_sync_metadata_only": close_sync_metadata_only,
        "workflow_bug_metadata_files": workflow_bug_metadata_files,
        "workflow_validation_only": workflow_validation_only,
        "workflow_validation_required": workflow_validation_only,
        "backend_required": backend_required,
        "static_gate_required": True,
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
        f"classification={payload['classification']}",
    ]
    with open(path, "a", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Classify AIstock CI changed files for safe fast lanes.")
    parser.add_argument("--changed-file", action="append", default=[])
    parser.add_argument("--changed-files-file")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--output-json")
    parser.add_argument("--github-output", default=os.environ.get("GITHUB_OUTPUT"))
    args = parser.parse_args(argv)

    payload = classify_changed_files(_load_changed_files(args), repo_root=Path(args.repo_root))
    if args.output_json:
        _write_json(Path(args.output_json), payload)
    if args.github_output:
        _write_github_output(args.github_output, payload)
    print(json.dumps({
        "workflow_gate": payload["workflow_gate"],
        "classification": payload["classification"],
        "backend_required": payload["backend_required"],
        "workflow_validation_required": payload["workflow_validation_required"],
        "changed_file_count": payload["changed_file_count"],
    }, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
