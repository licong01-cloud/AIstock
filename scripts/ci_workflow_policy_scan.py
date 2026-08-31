"""Fail-closed policy scan for CI workflows.

The repository's CI lanes run on pre-provisioned Windows environments.  This
small stdlib-only scanner intentionally checks workflow source rather than
running any project command: dependency installation and disposable database
services must be rejected before a workflow can be merged.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Iterable


DEFAULT_WORKFLOW_ROOT = Path(".github/workflows")
DEFAULT_NOX_PATH = Path("noxfile.py")
WINDOWS_PR_WORKFLOWS = {
    "test.yml",
    "pr-quality.yml",
    "codeql.yml",
    "semgrep.yml",
    "dependency-update-validate.yml",
}
WINDOWS_PR_WORKFLOW_RUNNER_LABEL = {
    "test.yml": "aistock-ci",
    "pr-quality.yml": "aistock-ci",
    "codeql.yml": "aistock-ci-security",
    "semgrep.yml": "aistock-ci",
    "dependency-update-validate.yml": "aistock-ci",
}
SUPERSEDED_RUN_WORKFLOWS = {
    "test.yml",
    "pr-quality.yml",
    "codeql.yml",
    "semgrep.yml",
    "dependency-update-validate.yml",
}
BASE_FETCH_RETRY_WORKFLOWS = {
    "test.yml",
    "codeql.yml",
    "semgrep.yml",
    "dependency-update-validate.yml",
    "pr-quality.yml",
}
PR_ONLY_QUALITY_WORKFLOWS = {"test.yml", "semgrep.yml"}
STABLE_MERGE_QUALITY_CONTEXTS = (
    "CI verdict",
    "CodeQL verdict",
    "AIstock Semgrep guardrails",
    "Context, scope, and open-source tooling dry-run",
)
_INSTALL_RE = re.compile(
    r"\b(?:python\s+-m\s+)?pip(?:\d+(?:\.\d+)?)?\s+install\b"
    r"|\bnpm\s+(?:ci|install)\b"
    r"|\b(?:conda|mamba)\s+install\b"
    r"|\bgo\s+mod\s+download\b",
    re.IGNORECASE,
)
_DB_CREATE_RE = re.compile(
    r"\b(?:docker\s+(?:run|compose\s+(?:up|run))|docker-compose\s+up)\b[^\n]*"
    r"\b(?:postgres|timescale)\b",
    re.IGNORECASE,
)
_SETUP_ACTION_RE = re.compile(r"^\s*(?:-\s*)?uses:\s*actions/setup-(?:python|node|go|miniconda)@", re.IGNORECASE)
_SERVICES_RE = re.compile(r"^\s*(?:-\s*)?services\s*:", re.IGNORECASE)
_DB_IMAGE_RE = re.compile(r"^\s*image:\s*[^#\n]*(?:postgres|timescale)", re.IGNORECASE)
_CI_DDL_DML_RE = re.compile(
    r"\b(?:psql|sqlcmd)\b[^\n]*(?:\bcreate\s+(?:database|table|index)\b|\balter\s+table\b|\bdrop\s+|\binsert\s+into\b|\bupdate\s+|\bdelete\s+from\b|\btruncate\s+)",
    re.IGNORECASE,
)


def scan_workflow_text(text: str, path: str = "workflow.yml") -> list[dict[str, str]]:
    findings: list[dict[str, str]] = []
    for line_number, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.rstrip()
        stripped = line.lstrip()
        if not stripped or stripped.startswith("#"):
            continue
        reason: str | None = None
        if _SERVICES_RE.search(line):
            reason = "CI workflow services are prohibited; use the existing DEV database lane"
        elif _DB_IMAGE_RE.search(line):
            reason = "disposable postgres/timescale image is prohibited in CI"
        elif _SETUP_ACTION_RE.search(line):
            reason = "setup-* actions install mutable toolchains; use a prebuilt runner"
        elif _INSTALL_RE.search(line):
            reason = "dependency installation is prohibited in CI"
        elif _DB_CREATE_RE.search(line):
            reason = "creating a postgres/timescale container is prohibited in CI"
        elif _CI_DDL_DML_RE.search(line):
            reason = "DDL/DML execution is prohibited in CI workflows"
        if reason:
            findings.append({"path": path, "line": str(line_number), "reason": reason, "text": line.strip()})
    return findings


def scan_workflows(paths: Iterable[Path]) -> list[dict[str, str]]:
    findings: list[dict[str, str]] = []
    for path in paths:
        findings.extend(scan_workflow_text(path.read_text(encoding="utf-8"), path.as_posix()))
    return findings


def scan_nox_text(text: str, path: str = "noxfile.py") -> list[dict[str, str]]:
    """Reject CI-visible dependency bootstrap hidden behind a Nox session.

    Local validation may retain an explicit ``npm ci`` fallback, but the
    helper must fail closed when GitHub Actions (or the CI install marker) is
    active.  Workflow-only regex scanning cannot see this code path.
    """

    if "npm" not in text or re.search(r"npm[\"']\s*,\s*[\"']ci", text, re.IGNORECASE) is None:
        return []
    required_markers = ("_ci_dependency_install_forbidden", "session.error")
    if all(marker in text for marker in required_markers):
        return []
    return [
        {
            "path": path,
            "line": "1",
            "reason": "Nox contains npm ci without a CI fail-closed dependency guard",
            "text": "npm ci",
        }
    ]


def scan_environment_contracts(paths: Iterable[Path]) -> list[dict[str, str]]:
    """Check the machine-readable runner contract for PR validation lanes."""

    findings: list[dict[str, str]] = []
    for path in paths:
        if path.name not in WINDOWS_PR_WORKFLOWS:
            continue
        text = path.read_text(encoding="utf-8")
        expected_label = WINDOWS_PR_WORKFLOW_RUNNER_LABEL[path.name]
        runner_re = re.compile(
            rf"runs-on:\s*\[self-hosted,\s*Windows,\s*{re.escape(expected_label)}\]",
            re.IGNORECASE,
        )
        dynamic_windows_runner = (
            path.name == "test.yml"
            and f'fromJSON(\'["self-hosted","Windows","{expected_label}"]\')' in text
            and "ubuntu-latest" in text
            and "close-sync-" in text
        )
        if not runner_re.search(text) and not dynamic_windows_runner:
            findings.append(
                {
                    "path": path.as_posix(),
                    "line": "1",
                    "reason": "PR validation workflow must use its prebuilt Windows runner role",
                    "text": expected_label,
                }
            )
        if path.name == "test.yml":
            required = ("AISTOCK_CI_ENV_NAME: AIstock-CI", "install_forbidden", "dev_db_required")
            missing = [marker for marker in required if marker not in text]
            if missing:
                findings.append(
                    {
                        "path": path.as_posix(),
                        "line": "1",
                        "reason": "test.yml is missing required environment/DB/install contract markers",
                        "text": ", ".join(missing),
                    }
                )
    return findings


def build_contract_evidence(
    paths: Iterable[Path],
    *,
    nox_path: Path = DEFAULT_NOX_PATH,
    classifier_path: Path = Path("scripts/ci_change_classifier.py"),
    environment_verify_path: Path = Path("scripts/ci_environment_verify.py"),
    changed_files_path: Path = Path("scripts/ci_changed_files.py"),
    workspace_prepare_path: Path = Path("scripts/ci/prepare_self_hosted_workspace.py"),
    issue_workflow_path: Path = Path("scripts/aistock_issue_workflow.py"),
    nightly_scheduler_path: Path = Path("scripts/nightly_adaptive_scheduler.py"),
    nightly_session_runner_path: Path = Path("scripts/nightly_session_runner.py"),
) -> dict[str, bool]:
    """Return the exact evidence booleans named by the machine standard."""

    path_list = list(paths)
    workflow_text = {path.name: path.read_text(encoding="utf-8") for path in path_list}
    pr_texts = [workflow_text[name] for name in sorted(WINDOWS_PR_WORKFLOWS) if name in workflow_text]
    pr_combined = "\n".join(pr_texts)
    test_text = workflow_text.get("test.yml", "")
    pr_quality_text = workflow_text.get("pr-quality.yml", "")
    codeql_text = workflow_text.get("codeql.yml", "")
    nightly_text = workflow_text.get("nightly.yml", "")
    code_intelligence_refresh_text = workflow_text.get("code-intelligence-refresh.yml", "")
    nightly_runner_re = re.compile(
        r"runs-on:\s*\[self-hosted,\s*Windows,\s*([a-z0-9-]+)\]",
        re.IGNORECASE,
    )
    nightly_runner_labels = [
        match.group(1)
        for line in nightly_text.splitlines()
        if not line.lstrip().startswith("#")
        if (match := nightly_runner_re.search(line))
    ]
    nox_text = nox_path.read_text(encoding="utf-8") if nox_path.exists() else ""
    classifier_text = classifier_path.read_text(encoding="utf-8") if classifier_path.exists() else ""
    environment_verify_text = (
        environment_verify_path.read_text(encoding="utf-8") if environment_verify_path.exists() else ""
    )
    changed_files_text = changed_files_path.read_text(encoding="utf-8") if changed_files_path.exists() else ""
    workspace_prepare_text = workspace_prepare_path.read_text(encoding="utf-8") if workspace_prepare_path.exists() else ""
    nightly_scheduler_text = nightly_scheduler_path.read_text(encoding="utf-8") if nightly_scheduler_path.exists() else ""
    nightly_session_runner_text = (
        nightly_session_runner_path.read_text(encoding="utf-8") if nightly_session_runner_path.exists() else ""
    )
    ci_preparation_match = re.search(
        r"(?ms)^  ci-verdict:\n(?P<body>.*?)(?=^  [a-z0-9-]+:\n|\Z)",
        test_text,
    )
    ci_preparation_text = ci_preparation_match.group("body") if ci_preparation_match else ""
    ci_verdict_text = ci_preparation_text
    issue_workflow_text = issue_workflow_path.read_text(encoding="utf-8") if issue_workflow_path.exists() else ""
    workflow_findings = scan_workflows(path_list)
    combined_workflow_text = "\n".join(workflow_text.values())
    reasons = {item["reason"] for item in workflow_findings}
    def uses_expected_runner(name: str) -> bool:
        expected_label = WINDOWS_PR_WORKFLOW_RUNNER_LABEL[name]
        literal_runner = bool(
            re.search(
                rf"runs-on:\s*\[self-hosted,\s*Windows,\s*{re.escape(expected_label)}\]",
                workflow_text.get(name, ""),
                re.IGNORECASE,
            )
        )
        dynamic_windows_runner = (
            name == "test.yml"
            and f'fromJSON(\'["self-hosted","Windows","{expected_label}"]\')'
            in workflow_text.get(name, "")
            and "ubuntu-latest" in workflow_text.get(name, "")
            and "close-sync-" in workflow_text.get(name, "")
        )
        return literal_runner or dynamic_windows_runner

    evidence = {
        "windows_self_hosted_runner": len(pr_texts) == len(WINDOWS_PR_WORKFLOWS)
        and all(uses_expected_runner(name) for name in WINDOWS_PR_WORKFLOWS),
        "prebuilt_aistock_ci_environment": len(pr_texts) == len(WINDOWS_PR_WORKFLOWS)
        and all("aistock-ci" in text.casefold() for text in pr_texts),
        "environment_fingerprint_match": len(pr_texts) == len(WINDOWS_PR_WORKFLOWS)
        and all("ci_environment_verify.py" in text for text in pr_texts),
        "no_setup_actions": "setup-* actions install mutable toolchains; use a prebuilt runner" not in reasons,
        "no_dependency_install_commands": "dependency installation is prohibited in CI" not in reasons,
        "nox_ci_install_fail_closed_guard": bool(nox_text) and not scan_nox_text(nox_text, nox_path.as_posix()),
        "no_linux_or_production_environment_fallback": (
            len(pr_texts) == len(WINDOWS_PR_WORKFLOWS)
            and all("conda run -n aistock" not in text.casefold() for text in pr_texts)
            and all(
                "ubuntu-" not in workflow_text.get(name, "").casefold()
                for name in WINDOWS_PR_WORKFLOWS - {"test.yml"}
            )
            and test_text.casefold().count("ubuntu-latest") == 1
            and "github_hosted_metadata" in test_text
            and "scripts/bug_registry_metadata_check.py" in test_text
            and "--close-sync-only" in test_text
        ),
        "windows_git_bash_shell": len(pr_texts) == len(WINDOWS_PR_WORKFLOWS)
        and all("shell: bash" in text.casefold() for text in pr_texts),
        "pr_quality_no_external_report_action_dependency": "actions/upload-artifact@" not in pr_quality_text
        and "actions/github-script@" not in pr_quality_text,
        "superseded_pr_runs_cancel_in_progress": all(
            "concurrency:" in workflow_text.get(name, "")
            and "cancel-in-progress: true" in workflow_text.get(name, "")
            and "github.event.pull_request.number" in workflow_text.get(name, "")
            for name in SUPERSEDED_RUN_WORKFLOWS
        ),
        "bounded_pr_base_fetch_retry": all(
            "--prepare-pr-merge-base-only" in workflow_text.get(name, "")
            for name in BASE_FETCH_RETRY_WORKFLOWS
        )
        and "for index in range(max(1, int(attempts)))" in changed_files_text
        and '"--no-write-fetch-head"' in changed_files_text
        and 'f"--deepen={max(1, int(deepen_by))}"' in changed_files_text
        and '"merge-base"' in changed_files_text
        and "pinned PR base/head history preparation failed" in changed_files_text,
        "non_security_quality_lanes_run_once_on_pull_request": all(
            "pull_request:" in workflow_text.get(name, "")
            and not re.search(r"(?m)^\s{2}push:\s*$", workflow_text.get(name, ""))
            for name in PR_ONLY_QUALITY_WORKFLOWS
        ),
        "merge_quality_contexts_are_change_scoped": (
            "  pull_request:\n    branches: [main]" in codeql_text
            and bool(re.search(r"(?m)^  codeql-verdict:\s*$", codeql_text))
            and "name: CodeQL verdict" in codeql_text
            and "if: always()" in codeql_text
            and "  pull_request:\n    branches: [main]" in workflow_text.get("semgrep.yml", "")
            and "name: AIstock Semgrep guardrails" in workflow_text.get("semgrep.yml", "")
            and "name: Context, scope, and open-source tooling dry-run" in pr_quality_text
            and all(
                "github.event_name != 'pull_request'" in workflow_text.get(name, "")
                and "startsWith(github.head_ref, 'chore/BUG-')" in workflow_text.get(name, "")
                and "contains(github.head_ref, '-close-sync-')" in workflow_text.get(name, "")
                for name in ("pr-quality.yml", "semgrep.yml", "codeql.yml")
            )
            and "github_hosted_metadata" in test_text
            and "ubuntu-latest" in test_text
            and "name: CI verdict" in test_text
            and not re.search(r"(?m)^  classify-changes:\s*$", test_text)
            and "scripts/bug_registry_metadata_check.py" in test_text
            and "_merge_quality_contexts_for_head_ref" in issue_workflow_text
            and all(f'"{context}"' in issue_workflow_text for context in STABLE_MERGE_QUALITY_CONTEXTS)
        ),
        "codeql_default_branch_security_scan_preserved": bool(
            re.search(r"(?m)^\s{2}push:\s*$", workflow_text.get("codeql.yml", ""))
            and "branches: [main]" in workflow_text.get("codeql.yml", "")
        ),
        "codeql_uses_hash_verified_prebuilt_bundle": (
            "AISTOCK_CI_CODEQL_BUNDLE_REQUIRED: '1'" in workflow_text.get("codeql.yml", "")
            and "AISTOCK_CI_CODEQL_BUNDLE_SHA256:" in workflow_text.get("codeql.yml", "")
            and "_work\\_tool\\CodeQL\\2.26.3\\x64\\codeql" in workflow_text.get("codeql.yml", "")
            and "prebuilt CodeQL bundle SHA-256 mismatch" in environment_verify_text
            and "database create" in workflow_text.get("codeql.yml", "")
            and "database analyze" in workflow_text.get("codeql.yml", "")
            and "github upload-results" in workflow_text.get("codeql.yml", "")
        ),
        "codeql_remote_action_download_is_eliminated": (
            "github/codeql-action/" not in codeql_text
            and "actions/checkout@" not in codeql_text
            and "uses:" not in codeql_text
            and "- name: Run CodeQL CLI analysis\n        id: codeql_analysis\n"
            in codeql_text
        ),
        "codeql_reuses_single_security_runner_allocation": (
            codeql_text.count("runs-on: [self-hosted, Windows, aistock-ci-security]") == 1
            and not re.search(r"(?m)^  (?:docs-lite|analyze):\s*$", codeql_text)
            and "strategy:" not in codeql_text
            and "matrix:" not in codeql_text
            and codeql_text.count("Prepare exact local workspace (no remote actions)") == 1
            and "CODEQL_LANGUAGES: ${{ steps.fast_lane.outputs.languages }}" in codeql_text
            and "foreach ($language in $languages)" in codeql_text
            and "CLASSIFIER_RESULT: ${{ steps.fast_lane.outcome }}" in codeql_text
            and "ANALYZE_RESULT: ${{ steps.codeql_analysis.outcome }}" in codeql_text
        ),
        "codeql_exact_local_workspace_fetch_is_bounded": (
            codeql_text.count("Prepare exact local workspace (no remote actions)") == 1
            and codeql_text.count("--no-write-fetch-head") == 1
            and codeql_text.count("exact workspace source fetch failed after 3 attempts") == 1
            and codeql_text.count("refs/aistock-ci/codeql-") == 1
            and codeql_text.count("update-ref -d $cacheRef") == 1
            and codeql_text.count('$env:GIT_CONFIG_KEY_0 = "core.longpaths"') == 1
            and "git -C $source fetch --no-tags --depth=1" not in codeql_text
        ),
        "codeql_pr_test_only_analysis_is_skipped_without_weakening_main_push": (
            "codeql_pr_languages" in classifier_text
            and "codeql_pr_test_only" in classifier_text
            and "LANGUAGE_FIELD=" in workflow_text.get("codeql.yml", "")
            and "pull_request_test_only" in workflow_text.get("codeql.yml", "")
            and "codeql_languages" in workflow_text.get("codeql.yml", "")
            and "github.event_name" in workflow_text.get("codeql.yml", "")
        ),
        "code_intelligence_refresh_is_scheduled_or_manual_only": (
            "schedule:" in code_intelligence_refresh_text
            and "workflow_dispatch:" in code_intelligence_refresh_text
            and not re.search(r"(?m)^\s{2}push:\s*$", code_intelligence_refresh_text)
        ),
        "code_intelligence_refresh_has_no_external_artifact_action_dependency": (
            "actions/upload-artifact@" not in code_intelligence_refresh_text
            and "actions/download-artifact@" not in code_intelligence_refresh_text
        ),
        "javascript_actions_use_approved_native_node24_majors": all(
            set(re.findall(rf"{re.escape(prefix)}v\d+", combined_workflow_text)) == {expected}
            for prefix, expected in {
                "actions/checkout@": "actions/checkout@v7",
                "actions/upload-artifact@": "actions/upload-artifact@v7",
                "actions/download-artifact@": "actions/download-artifact@v8",
                "actions/github-script@": "actions/github-script@v9",
            }.items()
        ),
        "pr_ci_no_separate_failure_publisher_job": "failure-bug-register:" not in test_text
        and "The failed job logs are the authoritative PR evidence" in test_text,
        "pr_ci_no_external_artifact_action_dependency": "actions/upload-artifact@" not in test_text
        and "actions/download-artifact@" not in test_text,
        "pr_ci_static_gate_reuses_classifier_checkout": bool(ci_preparation_text)
        and not re.search(r"(?m)^  (?:static-gate|docs-lite):\s*$", test_text)
        and ci_preparation_text.count("actions/checkout@v7") == 1
        and ci_preparation_text.count("ci_environment_verify.py") == 1
        and "Classify CI lane" in ci_preparation_text
        and "scripts/bug_registry_metadata_check.py" in ci_preparation_text
        and "nox -s l0 -- changed files" in ci_preparation_text,
        "pr_ci_selected_lanes_reuse_ci_verdict_runner": bool(ci_verdict_text)
        and test_text.count("runs-on:") == 1
        and not any(
            re.search(rf"(?m)^  {re.escape(job)}:\s*$", test_text)
            for job in ("classify-changes", "backend-tests", "frontend-quality", "tdx-go-tests", "prompt-evaluation")
        )
        and not re.search(r"(?m)^  workflow-validation-tests:\s*$", test_text)
        and ci_verdict_text.count("actions/checkout@v7") == 1
        and ci_verdict_text.count("ci_environment_verify.py") == 1
        and "id: backend_validation" in ci_verdict_text
        and "id: frontend_validation" in ci_verdict_text
        and "id: go_validation" in ci_verdict_text
        and "id: prompt_validation" in ci_verdict_text
        and "id: workflow_validation" in ci_verdict_text
        and "id: workflow_policy" in ci_verdict_text
        and "BACKEND_RESULT: ${{ steps.backend_validation.outcome }}" in ci_verdict_text
        and "FRONTEND_RESULT: ${{ steps.frontend_validation.outcome }}" in ci_verdict_text
        and "GO_RESULT: ${{ steps.go_validation.outcome }}" in ci_verdict_text
        and "PROMPT_RESULT: ${{ steps.prompt_validation.outcome }}" in ci_verdict_text
        and "WORKFLOW_TEST_RESULT: ${{ steps.workflow_validation.outcome }}" in ci_verdict_text
        and "WORKFLOW_POLICY_RESULT: ${{ steps.workflow_policy.outcome }}" in ci_verdict_text
        and "workflow_validation=${WORKFLOW_TEST_RESULT}" in ci_verdict_text
        and "workflow_policy=${WORKFLOW_POLICY_RESULT}" in ci_verdict_text,
        "pr_workflows_no_external_report_action_dependency": all(
            marker not in pr_combined
            for marker in ("actions/upload-artifact@", "actions/download-artifact@", "actions/github-script@")
        ),
        "no_workflow_services": "CI workflow services are prohibited; use the existing DEV database lane" not in reasons,
        "no_postgres_or_timescaledb_container_creation": "creating a postgres/timescale container is prohibited in CI" not in reasons
        and "disposable postgres/timescale image is prohibited in CI" not in reasons,
        "classifier_dev_db_required_output": "dev_db_required" in test_text and "dev_db_required" in classifier_text,
        "existing_dev_database_lane_reference": "external_DEV_validation_required" in test_text
        and "existing DEV database" in classifier_text,
        "no_ci_ddl_or_dml": "DDL/DML execution is prohibited in CI workflows" not in reasons,
        "no_sqlite_substitution_for_real_database_contract": "sqlite" not in pr_combined.casefold(),
        "nightly_dr_operational_lane_is_explicit_and_does_not_create_or_start_database": (
            "AISTOCK_DR_OPERATIONAL_LANE: 'existing_authorized_target_only'" in nightly_text
            and "docker run" not in nightly_text.casefold()
            and "docker compose up" not in nightly_text.casefold()
        ),
        "nightly_l3_uses_prebuilt_aistock_ci_and_linked_frontend_dependencies": (
            '--frontend-node-modules-source "${env:AISTOCK_SELF_HOSTED_SOURCE}/frontend/node_modules"' in nightly_text
            and "Verify prebuilt AIstock-CI and frontend dependencies" in nightly_text
            and "conda run -n AIstock-CI python scripts/ci_environment_verify.py" in nightly_text
            and "frontend/node_modules/@playwright/test/cli.js" in nightly_text
            and "frontend/node_modules/typescript/bin/tsc" in nightly_text
            and "frontend/node_modules/next/dist/bin/next" in nightly_text
            and "dependency installation is prohibited in Nightly" in nightly_text
            and '"node_modules/@playwright/test/cli.js"' in nox_text
            and '"node_modules/typescript/bin/tsc"' in nox_text
            and '"node_modules/next/dist/bin/next"' in nox_text
            and "conda run -n AIstock-CI python scripts/nightly_adaptive_scheduler.py" in nightly_text
            and "conda run -n AIstock-CI python scripts/nightly_session_runner.py" in nightly_text
            and "conda run -n AIstock python scripts/nightly_session_runner.py" not in nightly_text
        ),
        "self_hosted_workspace_frontend_link_is_lockfile_verified_and_cleanup_safe": (
            "def _materialize_frontend_node_modules" in workspace_prepare_text
            and "REQUIRED_FRONTEND_ENTRYPOINTS" in workspace_prepare_text
            and "frontend_lock_mismatch" in workspace_prepare_text
            and "source_lock_sha256 != destination_lock_sha256" in workspace_prepare_text
            and '"mklink", "/J"' in workspace_prepare_text
            and "frontend_link_readback_failed" in workspace_prepare_text
            and "FILE_ATTRIBUTE_REPARSE_POINT" in workspace_prepare_text
            and "child.rmdir()" in workspace_prepare_text
        ),
        "nightly_retry_receipt_is_repo_scoped_bound_and_fail_closed": (
            "Select prior durable Nightly L3 receipt" in nightly_text
            and nightly_text.index("Select prior durable Nightly L3 receipt")
            < nightly_text.index("  dr-snapshot:")
            and 'prior_runs_tsv=$(gh run list \\' in nightly_text
            and '--repo "${GITHUB_REPOSITORY}"' in nightly_text
            and "--workflow nightly.yml" in nightly_text
            and "--status completed" in nightly_text
            and "--jq '.[] | [.databaseId, .headSha] | @tsv'" in nightly_text
            and "while IFS=$'\\t' read -r candidate_run_id candidate_head" in nightly_text
            and 'gh run download "${candidate_run_id}" \\' in nightly_text
            and 'if ! prior_runs_tsv=$(gh run list \\' in nightly_text
            and "No prior scheduled Nightly run with a durable session receipt is available" in nightly_text
            and "if-no-files-found: error" in nightly_text
            and "nightly-l3-retry-source-${{ github.run_id }}" in nightly_text
            and "RETRY_SOURCE_RUN_ID: ${{ needs.runner-preflight.outputs.retry_run_id }}" in nightly_text
            and "RETRY_SOURCE_HEAD: ${{ needs.runner-preflight.outputs.retry_source_head }}" in nightly_text
            and 'if (-not $retryRunId -or -not $watermark)' in nightly_text
            and "Runner preflight Nightly receipt artifact is incomplete" in nightly_text
            and "Receipt-bound scheduled Nightly watermark is unavailable locally" in nightly_text
            and '"--retry-source-head", $watermark' in nightly_text
            and "source_head != expected_head" in nightly_scheduler_text
            and 'if ($env:FULL_NIGHTLY_RUN -eq "true")' in nightly_text
            and "ConvertFrom-Json -ErrorAction Stop" not in nightly_text
            and "$priorRuns" not in nightly_text
            and "using explicit full-run fallback" not in nightly_text
            and nightly_text.count('$extraArgs += "--full-run"') == 1
        ),
        "nightly_retries_failed_or_missing_sessions_plus_new_impact": (
            '"retry_sessions": retry_sessions' in nightly_scheduler_text
            and '"retry_failed_sessions_from_receipt": True' in nightly_scheduler_text
            and "for session in retry_sessions" in nightly_scheduler_text
            and "session not in observed_sessions" in nightly_scheduler_text
            and '"--retry-results-json", $retryResults' in nightly_text
            and '"--retry-plan-json", $retryPlan' in nightly_text
        ),
        "nightly_change_scoped_l0_uses_explicit_receipt_paths": (
            'CHANGE_SCOPED_SESSIONS = frozenset({"l0"})' in nightly_session_runner_text
            and 'payload.get("session_positional_args")' in nightly_session_runner_text
            and 'AISTOCK_NIGHTLY_SESSION_ARGS_FILE' in nightly_session_runner_text
            and "NamedTemporaryFile" in nightly_session_runner_text
            and '"session_positional_args": session_positional_args' in nightly_scheduler_text
        ),
        "bounded_dual_runner_roles": (
            uses_expected_runner("codeql.yml")
            and "runs-on: [self-hosted, Windows, aistock-ci-security]" in code_intelligence_refresh_text
            and bool(nightly_runner_labels)
            and {label.casefold() for label in nightly_runner_labels}
            == {"aistock-ci", "aistock-ci-security"}
        ),
        "policy_evidence_remains_one_scanner_step": (
            combined_workflow_text.count("python scripts/ci_workflow_policy_scan.py") == 1
            and "Enforce CI workflow policy" in test_text
            and "policy-evidence:" not in combined_workflow_text
        ),
    }
    evidence["policy_scanner_environment_and_nox_contract_checks"] = all(evidence.values())
    return evidence


def scan_repository(workflow_root: Path = DEFAULT_WORKFLOW_ROOT, nox_path: Path = DEFAULT_NOX_PATH) -> list[dict[str, str]]:
    """Scan workflow YAML plus the Nox dependency boundary and runner contract."""

    paths = sorted(path for path in workflow_root.glob("*.yml"))
    findings = scan_workflows(paths)
    findings.extend(scan_environment_contracts(paths))
    if nox_path.exists():
        findings.extend(scan_nox_text(nox_path.read_text(encoding="utf-8"), nox_path.as_posix()))
    else:
        findings.append(
            {
                "path": nox_path.as_posix(),
                "line": "1",
                "reason": "Nox validation contract file is missing",
                "text": nox_path.as_posix(),
            }
        )
    evidence = build_contract_evidence(paths, nox_path=nox_path)
    for key, passed in evidence.items():
        if not passed:
            findings.append(
                {
                    "path": "ci-contract",
                    "line": "1",
                    "reason": f"required CI contract evidence failed: {key}",
                    "text": key,
                }
            )
    return findings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workflow-root", type=Path, default=DEFAULT_WORKFLOW_ROOT)
    parser.add_argument("--nox-path", type=Path, default=DEFAULT_NOX_PATH)
    parser.add_argument("--output-json", type=Path)
    args = parser.parse_args(argv)
    paths = sorted(path for path in args.workflow_root.glob("*.yml"))
    findings = scan_repository(args.workflow_root, args.nox_path)
    evidence = build_contract_evidence(paths, nox_path=args.nox_path)
    payload = {
        "schema_version": "aistock_ci_workflow_policy_receipt_v1",
        "workflow_count": len(paths),
        "nox_checked": args.nox_path.as_posix(),
        "contract_evidence": evidence,
        "status": "pass" if not findings else "blocked",
        "findings": findings,
    }
    encoded = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output_json:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    return 0 if not findings else 1


if __name__ == "__main__":
    raise SystemExit(main())
