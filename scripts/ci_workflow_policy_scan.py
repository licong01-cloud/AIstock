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
SUPERSEDED_RUN_WORKFLOWS = {
    "test.yml",
    "pr-quality.yml",
    "codeql.yml",
    "semgrep.yml",
    "dependency-update-validate.yml",
}
BASE_FETCH_RETRY_WORKFLOWS = {"test.yml", "codeql.yml", "semgrep.yml", "dependency-update-validate.yml"}
PR_ONLY_QUALITY_WORKFLOWS = {"test.yml", "semgrep.yml"}
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
        if not re.search(r"runs-on:\s*\[self-hosted,\s*Windows,\s*aistock-ci\]", text, re.IGNORECASE):
            findings.append(
                {
                    "path": path.as_posix(),
                    "line": "1",
                    "reason": "PR validation workflow must use the prebuilt Windows AIstock-CI runner",
                    "text": "runs-on",
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
) -> dict[str, bool]:
    """Return the exact evidence booleans named by the machine standard."""

    path_list = list(paths)
    workflow_text = {path.name: path.read_text(encoding="utf-8") for path in path_list}
    pr_texts = [workflow_text[name] for name in sorted(WINDOWS_PR_WORKFLOWS) if name in workflow_text]
    pr_combined = "\n".join(pr_texts)
    test_text = workflow_text.get("test.yml", "")
    pr_quality_text = workflow_text.get("pr-quality.yml", "")
    nightly_text = workflow_text.get("nightly.yml", "")
    code_intelligence_refresh_text = workflow_text.get("code-intelligence-refresh.yml", "")
    nox_text = nox_path.read_text(encoding="utf-8") if nox_path.exists() else ""
    classifier_text = classifier_path.read_text(encoding="utf-8") if classifier_path.exists() else ""
    environment_verify_text = (
        environment_verify_path.read_text(encoding="utf-8") if environment_verify_path.exists() else ""
    )
    workflow_findings = scan_workflows(path_list)
    reasons = {item["reason"] for item in workflow_findings}
    windows_runner_re = re.compile(r"runs-on:\s*\[self-hosted,\s*Windows,\s*aistock-ci\]", re.IGNORECASE)
    evidence = {
        "windows_self_hosted_runner": len(pr_texts) == len(WINDOWS_PR_WORKFLOWS)
        and all(windows_runner_re.search(text) for text in pr_texts),
        "prebuilt_aistock_ci_environment": len(pr_texts) == len(WINDOWS_PR_WORKFLOWS)
        and all("aistock-ci" in text.casefold() for text in pr_texts),
        "environment_fingerprint_match": len(pr_texts) == len(WINDOWS_PR_WORKFLOWS)
        and all("ci_environment_verify.py" in text for text in pr_texts),
        "no_setup_actions": "setup-* actions install mutable toolchains; use a prebuilt runner" not in reasons,
        "no_dependency_install_commands": "dependency installation is prohibited in CI" not in reasons,
        "nox_ci_install_fail_closed_guard": bool(nox_text) and not scan_nox_text(nox_text, nox_path.as_posix()),
        "no_linux_or_production_environment_fallback": len(pr_texts) == len(WINDOWS_PR_WORKFLOWS)
        and all("ubuntu-" not in text.casefold() and "conda run -n aistock" not in text.casefold() for text in pr_texts),
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
            "base fetch failed after 3 attempts" in workflow_text.get(name, "")
            for name in BASE_FETCH_RETRY_WORKFLOWS
        ),
        "non_security_quality_lanes_run_once_on_pull_request": all(
            "pull_request:" in workflow_text.get(name, "")
            and not re.search(r"(?m)^\s{2}push:\s*$", workflow_text.get(name, ""))
            for name in PR_ONLY_QUALITY_WORKFLOWS
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
            "github/codeql-action/" not in workflow_text.get("codeql.yml", "")
            and "actions/checkout@" not in workflow_text.get("codeql.yml", "")
            and "uses:" not in workflow_text.get("codeql.yml", "")
            and "- name: Run CodeQL CLI analysis\n        timeout-minutes: 20\n"
            in workflow_text.get("codeql.yml", "")
        ),
        "codeql_exact_local_workspace_fetch_is_bounded": (
            workflow_text.get("codeql.yml", "").count("Prepare exact local workspace (no remote actions)") == 2
            and workflow_text.get("codeql.yml", "").count("--no-write-fetch-head") == 2
            and workflow_text.get("codeql.yml", "").count("exact workspace source fetch failed after 3 attempts") == 2
            and workflow_text.get("codeql.yml", "").count("refs/aistock-ci/codeql-") == 2
            and workflow_text.get("codeql.yml", "").count("update-ref -d $cacheRef") == 2
            and workflow_text.get("codeql.yml", "").count('$env:GIT_CONFIG_KEY_0 = "core.longpaths"') == 2
            and "git -C $source fetch --no-tags --depth=1" not in workflow_text.get("codeql.yml", "")
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
        "pr_ci_no_separate_failure_publisher_job": "failure-bug-register:" not in test_text
        and "The failed job logs are the authoritative PR evidence" in test_text,
        "pr_ci_no_external_artifact_action_dependency": "actions/upload-artifact@" not in test_text
        and "actions/download-artifact@" not in test_text,
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
