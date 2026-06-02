from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

BUG_REGISTRY_PREFIX = "tests/aistock_validation/bugs/"
CLOSE_SYNC_STATUSES = {"fixed", "closed", "verified"}
WORKFLOW_VALIDATION_FAST_LANE_FILES = {
    ".github/workflows/issue-auto-link.yml",
    ".github/workflows/pr-quality.yml",
    ".github/workflows/semgrep.yml",
    ".github/workflows/test.yml",
    "backend/tests/scripts/test_aistock_issue_workflow.py",
    "backend/tests/scripts/test_ci_change_classifier.py",
    "backend/tests/scripts/test_ci_failure_issue_summary.py",
    "backend/tests/scripts/test_code_intelligence_adapter.py",
    "backend/tests/scripts/test_issue_flow.py",
    "docs/architecture/aistock_issue_workflow_efficiency_hardening_design_v2_2_20260529.md",
    "docs/standards/aistock_issue_workflow_quickstart.md",
    "scripts/aistock_issue_workflow.py",
    "scripts/ci_change_classifier.py",
    "scripts/ci_failure_issue_summary.py",
    "scripts/code_intelligence_adapter.py",
    "scripts/issue_flow.py",
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


def _bug_status(path: Path) -> str | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return None
    status = payload.get("status")
    return str(status).strip().lower() if status is not None else None


def _workflow_validation_fast_lane(path: str) -> bool:
    return path in WORKFLOW_VALIDATION_FAST_LANE_FILES


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
        reasons.append("non-registry files changed; backend matrix remains required")
    if not bug_registry_files:
        reasons.append("no BUG registry metadata file changed")

    metadata_statuses: dict[str, str | None] = {}
    metadata_only = bool(normalized) and not non_bug_registry_files and bool(bug_registry_files)
    close_sync_metadata_only = metadata_only
    for rel_path in bug_registry_files:
        path = repo_root / rel_path
        if Path(rel_path).name.startswith("."):
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

    if close_sync_metadata_only:
        reasons.append("only fixed/closed/verified BUG JSON metadata changed; backend matrix can be skipped")

    workflow_validation_only = (
        bool(normalized)
        and not bug_registry_files
        and all(_workflow_validation_fast_lane(path) for path in non_bug_registry_files)
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
