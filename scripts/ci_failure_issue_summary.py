from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

DEFAULT_REPO = "licong01-cloud/AIstock"
CANDIDATE_HISTORY_SCHEMA = "aistock_ci_failure_candidate_history_v1"
NIGHTLY_STATUS_KEYS = (
    "runner_preflight",
    "dr_snapshot",
    "dr_validate",
    "nightly_l3",
    "paper_v2_live",
    "code_intelligence",
)
NIGHTLY_STATUS_ALIASES = {
    "runner_preflight": ("runnerPreflight", "runner-preflight", "runner_preflight"),
    "dr_snapshot": ("drSnapshot", "dr-snapshot", "dr_snapshot"),
    "dr_validate": ("drValidate", "dr-validate", "dr_validate"),
    "nightly_l3": ("nightlyL3", "nightly-l3", "nightly_l3"),
    "paper_v2_live": ("paperV2Live", "paper-v2-live", "paper_v2_live"),
    "code_intelligence": ("codeIntelligence", "code-intelligence", "code_intelligence", "code-intelligence-weekly"),
}
NIGHTLY_FAILURE_STATUSES = {"failure", "cancelled", "timed_out", "timed-out", "startup_failure", "action_required"}
LOGS_NOT_READY_REASON = "actions_run_still_in_progress_logs_unavailable"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _run(args: list[str], *, cwd: Path | None = None, timeout: int = 120) -> dict[str, Any]:
    try:
        proc = subprocess.run(
            args,
            cwd=str(cwd) if cwd else None,
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            timeout=timeout,
            check=False,
        )
        return {
            "ok": proc.returncode == 0,
            "returncode": proc.returncode,
            "stdout": proc.stdout,
            "stderr": proc.stderr,
        }
    except Exception as exc:
        return {"ok": False, "returncode": None, "stdout": "", "stderr": str(exc)}


def _clean_log_line(line: str) -> str:
    """Normalize `gh run view --log` rows into message text."""
    line = line.lstrip("\ufeff")
    if "\t" in line:
        parts = line.split("\t")
        if len(parts) >= 3:
            line = parts[-1]
    line = re.sub(r"^\d{4}-\d{2}-\d{2}T[0-9:.]+Z\s*", "", line)
    return line.rstrip()


def normalize_log(text: str) -> list[str]:
    return [_clean_log_line(line) for line in text.splitlines()]


def _first_match(lines: list[str], patterns: list[str]) -> str | None:
    for line in lines:
        for pattern in patterns:
            match = re.search(pattern, line)
            if match:
                return match.group(1) if match.groups() else line.strip()
    return None


def _unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        cleaned = value.strip()
        if cleaned and cleaned not in seen:
            result.append(cleaned)
            seen.add(cleaned)
    return result


def _status_value(value: Any) -> str:
    return str(value or "unknown").strip().lower() or "unknown"


def _session_from_job_name(job_name: str | None) -> str | None:
    text = str(job_name or "")
    match = re.search(r"\(([A-Za-z0-9_.-]+)\)", text)
    return match.group(1) if match else None


def _logs_not_ready_error(value: Any) -> bool:
    text = str(value or "").lower()
    return "run" in text and "still in progress" in text and "log" in text


def _diagnostic_retry_command(run_id: str) -> str:
    run_arg = run_id or "<run-id>"
    return (
        f"python scripts/ci_failure_issue_summary.py --repo {DEFAULT_REPO} --run-id {run_arg} "
        "--wait-for-completion --wait-attempts 2 --wait-seconds 15 "
        "--log-attempts 3 --log-wait-seconds 10 "
        "--output tmp/validation/ci_failure_issue/summary.json "
        "--markdown-output tmp/validation/ci_failure_issue/body.md "
        "--context-output tmp/validation/ci_failure_issue/context-pack.json "
        "--context-markdown-output tmp/validation/ci_failure_issue/context-pack.md "
        "--github-issue-payload-output tmp/validation/ci_failure_issue/github-issue-payload.json "
        "--stdout-format compact"
    )


def _issue_creation_policy(summary: dict[str, Any]) -> dict[str, Any]:
    diagnostic_status = summary.get("diagnostic_status") or "partial"
    errors = summary.get("extraction_errors") or []
    failed_jobs = summary.get("failed_jobs") or []
    has_actionable_failure = any(job.get("failed_tests") or job.get("error_signature") for job in failed_jobs)
    has_manual_summary = bool(str(summary.get("manual_summary") or "").strip())
    run_id = str(summary.get("run_id") or "").strip()
    if diagnostic_status == "deferred":
        return {
            "allowed": False,
            "reason": LOGS_NOT_READY_REASON if any(_logs_not_ready_error(error) for error in errors) else "diagnostics_not_actionable",
            "next_command": _diagnostic_retry_command(run_id),
        }
    if diagnostic_status == "partial" and has_manual_summary:
        return {
            "allowed": True,
            "reason": "manual_summary_triage",
            "next_command": None,
        }
    if diagnostic_status == "partial" and not has_actionable_failure:
        return {
            "allowed": False,
            "reason": "diagnostics_not_actionable",
            "next_command": _diagnostic_retry_command(run_id),
        }
    return {
        "allowed": True,
        "reason": "ready" if diagnostic_status == "complete" else "partial_but_actionable",
        "next_command": None,
    }


def _llm_guarded_rollout_gate(
    summary: dict[str, Any],
    *,
    mode: str | None = None,
    opt_in: bool | None = None,
) -> dict[str, Any]:
    try:
        root = Path(__file__).resolve().parents[1]
        if str(root) not in sys.path:
            sys.path.insert(0, str(root))
        from scripts import llm_provider_adapter

        config = llm_provider_adapter.load_config()
        advice = summary.get("llm_triage_advice") if isinstance(summary.get("llm_triage_advice"), dict) else {}
        llm_workflow_gate = str(advice.get("workflow_gate") or "ready")
        return llm_provider_adapter.build_guarded_rollout_gate(
            "github_models",
            config,
            mode=mode,
            opt_in=opt_in,
            module=(summary.get("suspected_modules") or [None])[0],
            issue_sections=[
                "Failure Summary",
                "Regression Locator",
                "Agent Handoff",
                "Token Policy",
                "Production Gates",
            ],
            deterministic_issue_allowed=(summary.get("issue_creation_policy") or {}).get("allowed") is not False,
            llm_workflow_gate=llm_workflow_gate,
        )
    except Exception as exc:
        return {
            "schema_version": "aistock_validation_llm_guarded_rollout_v1",
            "workflow_gate": "warning",
            "auto_file_allowed": False,
            "llm_can_enhance_issue": False,
            "llm_enhancement_allowed": False,
            "deterministic_issue_creation_unaffected": True,
            "fallback": "deterministic_issue_workflow",
            "rejection_reasons": ["guarded_rollout_gate_unavailable"],
            "fallback_reason": str(exc),
            "llm_invocation_evidence": {
                "schema_version": "aistock_llm_invocation_evidence_v1",
                "provider": "github_models",
                "model": "unknown",
                "invoked": False,
                "reason": "guarded_rollout_gate_unavailable_no_network",
                "redaction_applied": True,
            },
        }


def _infra_action_reason(summary: dict[str, Any]) -> str | None:
    statuses = summary.get("nightly_statuses") if isinstance(summary.get("nightly_statuses"), dict) else {}
    if statuses.get("runner_preflight") == "failure":
        return "self_hosted_runner_unavailable"
    text = "\n".join(
        [
            str(summary.get("issue_title") or ""),
            str(summary.get("manual_summary") or ""),
            *[
                str(item)
                for job in summary.get("failed_jobs") or []
                for item in [job.get("error_signature"), *(job.get("key_log_excerpt") or [])]
                if item
            ],
        ]
    ).lower()
    infra_tokens = [
        "self-hosted windows runner unavailable",
        "no online github actions runner",
        "unable to query github runner health",
        "aistock_runner_health_token",
    ]
    if any(token in text for token in infra_tokens):
        return "runner_health_infrastructure"
    return None


def _handoff_mode(summary: dict[str, Any]) -> dict[str, Any]:
    infra_reason = _infra_action_reason(summary)
    if infra_reason:
        return {
            "mode": "infra_action_only",
            "needs_bug_json": False,
            "reason": infra_reason,
            "infra_action": {
                "workflow_gate": "infra_action_required",
                "next_actions": [
                    "Restore or register the self-hosted Windows GitHub Actions runner.",
                    "Verify runner labels include: self-hosted, windows.",
                    "Configure AISTOCK_RUNNER_HEALTH_TOKEN if runner API access is denied.",
                    "Rerun the failed workflow after infrastructure is healthy.",
                ],
            },
        }
    issue_policy = summary.get("issue_creation_policy") if isinstance(summary.get("issue_creation_policy"), dict) else {}
    if summary.get("diagnostic_status") != "complete" and issue_policy.get("allowed") is False:
        return {
            "mode": "triage_only",
            "needs_bug_json": False,
            "reason": issue_policy.get("reason") or "triage_required_before_bug_promotion",
        }
    suspected_files = summary.get("suspected_files") or []
    if summary.get("diagnostic_status") != "complete" and not suspected_files:
        return {
            "mode": "triage_only",
            "needs_bug_json": False,
            "reason": "triage_required_before_bug_promotion",
        }
    return {"mode": "bug_promotion", "needs_bug_json": True, "reason": "code_or_test_failure"}


def _infer_module(job_name: str, nox_session: str | None, failed_tests: list[str]) -> str | None:
    haystack = " ".join([job_name, nox_session or "", *failed_tests]).lower()
    module_patterns = [
        ("paper_v2", ["paper_v2", "paper-v2", "paper_trading_v2", "selection_center", "strategy_package"]),
        ("validation", ["validation_center", "validation", "guardrail", "catalog"]),
        ("qe_archive", ["qe_archive"]),
        ("qe_data_contract", ["qe_data_contract"]),
        ("model_registry", ["model_registry"]),
        ("market_regime_label", ["market_regime_label"]),
        ("rl_execution", ["rl_execution"]),
    ]
    for module, needles in module_patterns:
        if any(needle in haystack for needle in needles):
            return module
    return None


def _module_files(module: str | None, failed_tests: list[str]) -> list[str]:
    files = [test.split("::", 1)[0] for test in failed_tests if "::" in test]
    if module == "paper_v2":
        files.extend(
            [
                "backend/services/paper_trading_v2",
                "backend/tests/paper_trading_v2",
                "backend/tests/selection_center",
                "backend/tests/strategy_package",
            ]
        )
    elif module == "validation":
        files.extend(
            [
                ".github/workflows/issue-on-test-fail.yml",
                "scripts/ci_failure_issue_summary.py",
                "scripts/aistock_issue_workflow.py",
                "backend/tests/scripts",
            ]
        )
    return _unique(files)


def _short_sha(value: Any) -> str | None:
    text = str(value or "").strip()
    return text[:12] if text else None


def _run_id_number(value: Any) -> int | None:
    try:
        return int(str(value or ""))
    except ValueError:
        return None


def _last_green_payload(summary: dict[str, Any], *, status: str, previous_success: dict[str, Any] | None = None, warning: str | None = None) -> dict[str, Any]:
    previous_success = previous_success or {}
    previous_sha = previous_success.get("headSha") or previous_success.get("commit")
    current_sha = summary.get("commit")
    commit_range = f"{_short_sha(previous_sha)}..{_short_sha(current_sha)}" if previous_sha and current_sha else None
    warnings = [warning] if warning else []
    return {
        "schema_version": "aistock_ci_last_green_locator_v1",
        "status": status,
        "blocking_for_issue_workflow": False,
        "current_run": {
            "run_id": str(summary.get("run_id") or ""),
            "run_url": summary.get("run_url"),
            "workflow": summary.get("workflow"),
            "branch": summary.get("branch"),
            "commit": current_sha,
        },
        "previous_success_run": {
            "run_id": str(previous_success.get("databaseId") or previous_success.get("run_id") or "") or None,
            "run_url": previous_success.get("url") or previous_success.get("run_url"),
            "workflow": previous_success.get("workflowName") or previous_success.get("workflow"),
            "branch": previous_success.get("headBranch") or previous_success.get("branch"),
            "commit": previous_sha,
            "created_at": previous_success.get("createdAt") or previous_success.get("created_at"),
        }
        if previous_success
        else None,
        "commit_range": commit_range,
        "suspected_files": summary.get("suspected_files") or [],
        "warnings": warnings,
    }


def locate_last_green_run(
    summary: dict[str, Any],
    *,
    repo: str,
    run_provider: Callable[..., dict[str, Any]] = _run,
) -> dict[str, Any]:
    """Find the most recent successful run for the same branch/workflow without making it a gate."""
    branch = summary.get("branch")
    workflow = summary.get("workflow")
    current_run_id = str(summary.get("run_id") or "")
    if not branch or not workflow:
        return _last_green_payload(summary, status="unavailable", warning="missing branch or workflow")
    result = run_provider(
        [
            "gh",
            "run",
            "list",
            "--repo",
            repo,
            "--branch",
            str(branch),
            "--limit",
            "50",
            "--json",
            "databaseId,workflowName,headSha,headBranch,conclusion,status,url,createdAt,displayTitle",
        ],
        timeout=120,
    )
    if not result.get("ok"):
        return _last_green_payload(
            summary,
            status="unavailable",
            warning=str(result.get("stderr") or result.get("stdout") or "gh run list failed")[:240],
        )
    try:
        runs = json.loads(str(result.get("stdout") or "[]"))
    except json.JSONDecodeError as exc:
        return _last_green_payload(summary, status="unavailable", warning=f"invalid gh run list JSON: {exc}")
    if not isinstance(runs, list):
        return _last_green_payload(summary, status="unavailable", warning="gh run list JSON root was not a list")
    current_number = _run_id_number(current_run_id)
    for run in runs:
        if not isinstance(run, dict):
            continue
        if str(run.get("workflowName") or "") != str(workflow):
            continue
        if str(run.get("headBranch") or "") != str(branch):
            continue
        if str(run.get("conclusion") or "").lower() != "success":
            continue
        run_number = _run_id_number(run.get("databaseId"))
        if current_number is not None and run_number is not None and run_number >= current_number:
            continue
        if str(run.get("databaseId") or "") == current_run_id:
            continue
        return _last_green_payload(summary, status="found", previous_success=run)
    return _last_green_payload(summary, status="not_found", warning="no previous successful run found in the last 50 branch runs")


def parse_job_log(log_text: str, *, job_name: str = "", job_url: str | None = None) -> dict[str, Any]:
    lines = normalize_log(log_text)
    joined = "\n".join(lines)
    nox_session = _first_match(lines, [r"nox > Running session ([A-Za-z0-9_.-]+)", r"nox -s ([A-Za-z0-9_.-]+)"])
    nox_session = nox_session or _session_from_job_name(job_name)
    command = _first_match(
        lines,
        [
            r"nox > (python -m pytest .*)",
            r"nox > Command (python -m pytest .*?) failed",
            r"##\[command\](.*python.*pytest.*)",
        ],
    )
    failed_step = _first_match(lines, [r"nox > Session ([A-Za-z0-9_.-]+) failed"])
    failed_tests = _unique(re.findall(r"FAILED\s+([^\s]+::[^\s]+)", joined))
    pytest_summary = _first_match(
        lines,
        [
            r"(\d+\s+failed,\s+.*?in\s+[0-9.]+s)",
            r"(\d+\s+failed,\s+.*?deselected(?:\s+in\s+[0-9.]+s)?)",
        ],
    )
    error_signature = None
    docker_exit_code = _first_match(lines, [r"Docker pull failed with exit code ([0-9]+)"])
    for pattern in [r"FAILED\s+[^\s]+::[^\s]+\s+-\s+(.+)", r"\b(E\s+assert\s+.+)", r"(assert\s+.+)"]:
        match = re.search(pattern, joined)
        if match:
            error_signature = match.group(1)
            break
    if not error_signature and docker_exit_code:
        error_signature = f"Docker pull failed with exit code {docker_exit_code}"
    error_signature = error_signature or _first_match(
        lines,
        [
            r"(AssertionError(?::.*)?)",
            r"(##\[error\].+)",
            r"(Error:\s+.+)",
        ],
    )
    if error_signature:
        error_signature = re.sub(r"^E\s+", "", error_signature).strip()

    excerpt_candidates: list[str] = []
    excerpt_patterns = [
        "FAILED ",
        "AssertionError",
        "assert ",
        "##[error]",
        "Traceback",
        " relation ",
        "ERROR:",
        "FATAL:",
        "Command python -m pytest",
        "Session ",
    ]
    for line in lines:
        stripped = line.strip()
        if any(pattern in stripped for pattern in excerpt_patterns):
            excerpt_candidates.append(stripped)
    unique_excerpts = _unique(excerpt_candidates)
    key_log_excerpt = unique_excerpts[:12]
    module = _infer_module(job_name, nox_session, failed_tests)
    return {
        "job_name": job_name,
        "job_url": job_url,
        "failed_step": failed_step or nox_session,
        "command": command,
        "nox_session": nox_session,
        "pytest_summary": pytest_summary,
        "failed_tests": failed_tests,
        "error_signature": error_signature,
        "key_log_excerpt": key_log_excerpt,
        "key_log_excerpt_omitted_count": max(0, len(unique_excerpts) - len(key_log_excerpt)),
        "suspected_module": module,
        "suspected_files": _module_files(module, failed_tests),
    }


def _fingerprint_source(summary: dict[str, Any]) -> str:
    failed = summary.get("failed_jobs") or []
    first = failed[0] if failed else {}
    first_test = (first.get("failed_tests") or [None])[0]
    return "|".join(
        str(part or "")
        for part in [
            summary.get("workflow"),
            summary.get("branch"),
            first.get("job_name"),
            first.get("nox_session"),
            first_test or first.get("error_signature"),
        ]
    )


def finalize_summary(summary: dict[str, Any]) -> dict[str, Any]:
    failed_jobs = summary.get("failed_jobs") or []
    diagnostic_status = "partial"
    errors = summary.get("extraction_errors") or []
    has_actionable_failure = any(job.get("failed_tests") or job.get("error_signature") for job in failed_jobs)
    if summary.get("defer_issue_creation") or (errors and any(_logs_not_ready_error(error) for error in errors) and not has_actionable_failure):
        diagnostic_status = "deferred"
    elif errors and not has_actionable_failure:
        diagnostic_status = "partial"
    elif failed_jobs and any(job.get("failed_tests") or job.get("error_signature") or job.get("command") for job in failed_jobs):
        diagnostic_status = "complete"
    elif summary.get("extraction_errors"):
        diagnostic_status = "partial"
    summary["diagnostic_status"] = diagnostic_status
    source = _fingerprint_source(summary)
    summary["fingerprint_source"] = source
    summary["fingerprint"] = "ci-" + hashlib.sha256(source.encode("utf-8")).hexdigest()[:16]
    modules = _unique([str(job.get("suspected_module")) for job in failed_jobs if job.get("suspected_module")])
    summary["suspected_modules"] = modules
    files: list[str] = []
    for job in failed_jobs:
        files.extend(job.get("suspected_files") or [])
    summary["suspected_files"] = _unique(files)
    first_job = failed_jobs[0] if failed_jobs else {}
    first_test = ((first_job.get("failed_tests") or [None])[0] or "").split("::")[-1]
    nox_session = first_job.get("nox_session") or _session_from_job_name(first_job.get("job_name")) or "ci"
    branch = summary.get("branch") or "unknown"
    failure_name = first_test or first_job.get("error_signature") or summary.get("manual_summary") or "diagnostic extraction incomplete"
    summary["issue_title"] = f"[{summary.get('severity') or 'P1'}][{nox_session}] {branch} CI failed: {failure_name}"[:240]
    if first_job.get("failed_tests"):
        summary["reproduce_command"] = f"python -m pytest {first_job['failed_tests'][0]} -q -p no:cacheprovider"
    elif nox_session and nox_session != "ci":
        summary["reproduce_command"] = f"python -m nox -s {nox_session}"
    else:
        summary["reproduce_command"] = summary.get("run_url") or "Inspect the linked CI run log."
    summary.setdefault("production_ddl_gate", "noop")
    summary.setdefault("production_frontend_dependency_gate", "noop")
    summary.setdefault("production_backend_dependency_gate", "noop")
    summary.setdefault("last_green_locator", _last_green_payload(summary, status="not_requested"))
    summary["issue_creation_policy"] = _issue_creation_policy(summary)
    summary["failure_event"] = build_failure_event(summary)
    summary["agent_handoff"] = build_agent_handoff(summary)
    return summary


def _github_issue_url(issue_number: int | str | None, repo: str = DEFAULT_REPO) -> str | None:
    if issue_number in {None, ""}:
        return None
    return f"https://github.com/{repo}/issues/{issue_number}"


def _first_failed_job(summary: dict[str, Any]) -> dict[str, Any]:
    failed_jobs = summary.get("failed_jobs") or []
    return failed_jobs[0] if failed_jobs else {}


def _all_failed_tests(summary: dict[str, Any]) -> list[str]:
    return _unique(
        [
            str(test)
            for job in summary.get("failed_jobs") or []
            for test in (job.get("failed_tests") or [])
            if test
        ]
    )


def _all_error_signatures(summary: dict[str, Any]) -> list[str]:
    return _unique(
        [
            str(item)
            for job in summary.get("failed_jobs") or []
            for item in [job.get("error_signature"), job.get("pytest_summary")]
            if item
        ]
    )


def _primary_failure(summary: dict[str, Any]) -> str:
    policy = summary.get("issue_creation_policy") if isinstance(summary.get("issue_creation_policy"), dict) else {}
    if policy.get("reason") == LOGS_NOT_READY_REASON:
        return "CI run is still in progress; Actions logs are not ready"
    first_job = _first_failed_job(summary)
    failed_tests = first_job.get("failed_tests") or []
    if failed_tests:
        return str(failed_tests[0])
    if first_job.get("error_signature"):
        return str(first_job["error_signature"])
    if summary.get("manual_summary"):
        return str(summary["manual_summary"])
    return "diagnostic extraction incomplete"


def _safe_command(command: str | None) -> str | None:
    if not command:
        return None
    command = command.strip()
    if not command or command.startswith("http") or command.startswith("Inspect "):
        return None
    return command


def build_failure_event(
    summary: dict[str, Any],
    *,
    github_issue_number: int | str | None = None,
    github_issue_url: str | None = None,
) -> dict[str, Any]:
    """Return an agent-neutral FailureEvent seed without embedding full logs."""
    first_job = _first_failed_job(summary)
    issue_url = github_issue_url or _github_issue_url(github_issue_number)
    evidence_refs = _unique([str(item) for item in [summary.get("run_url"), issue_url] if item])
    modules = summary.get("suspected_modules") or []
    fingerprint = str(summary.get("fingerprint") or "ci-unknown")
    return {
        "schema_version": "aistock_failure_event_v1",
        "event_id": f"FE-CI-{fingerprint.replace('ci-', '')[:16]}",
        "source": "github_actions",
        "timestamp": summary.get("generated_at") or _utc_now(),
        "repo": DEFAULT_REPO,
        "branch": summary.get("branch"),
        "commit": summary.get("commit"),
        "workflow": summary.get("workflow"),
        "run_id": str(summary.get("run_id") or ""),
        "run_url": summary.get("run_url"),
        "github_issue_number": str(github_issue_number) if github_issue_number else None,
        "github_issue_url": issue_url,
        "plan_key": "ci_failure_issue_intake",
        "nox_session": first_job.get("nox_session"),
        "module_guess": modules[0] if modules else (first_job.get("suspected_module") or "validation"),
        "severity_guess": summary.get("severity") or "P1",
        "diagnostic_status": summary.get("diagnostic_status") or "partial",
        "normalized_error": _primary_failure(summary),
        "failed_tests": _all_failed_tests(summary),
        "error_signatures": _all_error_signatures(summary),
        "fingerprint": fingerprint,
        "reproduce_command": summary.get("reproduce_command") or "Inspect the linked CI run log.",
        "evidence_refs": evidence_refs,
        "changed_files": summary.get("suspected_files") or [],
        "candidate_status": "new",
        "last_green_locator": summary.get("last_green_locator"),
        "log_policy": {
            "full_log_embedded": False,
            "full_log_ref": summary.get("run_url"),
            "reason": "Keep GitHub Issues and Context Packs compact; use the Actions run URL for full logs.",
        },
    }


def build_agent_handoff(
    summary: dict[str, Any],
    *,
    github_issue_number: int | str | None = None,
) -> dict[str, Any]:
    issue_arg = str(github_issue_number) if github_issue_number else "<issue-number>"
    issue_policy = summary.get("issue_creation_policy") if isinstance(summary.get("issue_creation_policy"), dict) else {}
    handoff_mode = _handoff_mode(summary)
    reproduce = _safe_command(str(summary.get("reproduce_command") or ""))
    required_verification = [
        item
        for item in [
            reproduce,
            (
                "Restore infrastructure and rerun CI/Nightly; do not promote to BUG JSON unless triage changes classification."
                if not handoff_mode["needs_bug_json"]
                else "Run the validation plan selected after BUG JSON promotion."
            ),
            (
                "Keep GitHub Issue comments and rerun evidence synchronized."
                if not handoff_mode["needs_bug_json"]
                else "Keep GitHub Issue, BUG JSON, PR, and validation evidence synchronized."
            ),
        ]
        if item
    ]
    suspected_files = list(summary.get("suspected_files") or [])
    stop_conditions = []
    if summary.get("diagnostic_status") != "complete":
        if issue_policy.get("allowed") is False:
            stop_conditions.append(
                "Diagnostic extraction is deferred because the Actions run is still in progress; rerun summary generation after completion."
            )
        else:
            stop_conditions.append("Diagnostic extraction is partial; inspect the linked CI run before assigning fix scope.")
    if not suspected_files:
        stop_conditions.append("No suspected files were extracted; run triage before editing code.")
    if not handoff_mode["needs_bug_json"]:
        if handoff_mode["mode"] == "infra_action_only":
            stop_conditions.append("Infrastructure-only issue: do not run promote-ci-issue or edit code unless triage changes classification.")
        else:
            stop_conditions.append("Triage-only CI issue: do not run promote-ci-issue or edit code until triage identifies a concrete code/test failure.")
    workflow_entrypoints = {
        "triage": f"python scripts/aistock_issue_workflow.py triage-ci-issue --issue {issue_arg}",
    }
    next_commands = [workflow_entrypoints["triage"]]
    if handoff_mode["needs_bug_json"]:
        workflow_entrypoints["promote"] = (
            f"python scripts/aistock_issue_workflow.py promote-ci-issue --issue {issue_arg} "
            "--create-registry-worktree --apply"
        )
        workflow_entrypoints["fix_after_promotion"] = (
            "python scripts/aistock_issue_workflow.py run --bug-id <BUG-ID> --mode plan --create-worktree"
        )
        next_commands.extend([workflow_entrypoints["promote"], workflow_entrypoints["fix_after_promotion"]])
    else:
        workflow_entrypoints["promote"] = "not_applicable_infra_action_only"
        workflow_entrypoints["fix_after_promotion"] = "not_applicable_infra_action_only"
    return {
        "schema_version": "aistock_ci_failure_agent_handoff_v1",
        "intended_clients": ["Codex", "Claude Code", "Cursor", "generic CLI/IDE agent"],
        "handoff_mode": handoff_mode["mode"],
        "needs_bug_json": handoff_mode["needs_bug_json"],
        "infra_action": handoff_mode.get("infra_action"),
        "workflow_entrypoints": workflow_entrypoints,
        "next_commands": next_commands,
        "allowed_write_scope": suspected_files,
        "required_verification": required_verification,
        "regression_locator": summary.get("last_green_locator"),
        "issue_creation_policy": issue_policy,
        "token_budget": {
            "target_tokens": 4000,
            "max_tokens": 8000,
            "full_logs_included": False,
            "full_docs_allowed": False,
            "full_log_ref": summary.get("run_url"),
        },
        "stop_conditions": stop_conditions,
        "production_gates": {
            "production_ddl_gate": summary.get("production_ddl_gate") or "noop",
            "production_frontend_dependency_gate": summary.get("production_frontend_dependency_gate") or "noop",
            "production_backend_dependency_gate": summary.get("production_backend_dependency_gate") or "noop",
        },
    }


def build_context_pack(
    summary: dict[str, Any],
    *,
    github_issue_number: int | str | None = None,
    github_issue_url: str | None = None,
) -> dict[str, Any]:
    event = build_failure_event(
        summary,
        github_issue_number=github_issue_number,
        github_issue_url=github_issue_url,
    )
    handoff = build_agent_handoff(summary, github_issue_number=github_issue_number)
    failed_jobs = []
    for job in summary.get("failed_jobs") or []:
        failed_jobs.append(
            {
                "job_name": job.get("job_name"),
                "job_url": job.get("job_url"),
                "nox_session": job.get("nox_session"),
                "command": job.get("command"),
                "failed_tests": job.get("failed_tests") or [],
                "error_signature": job.get("error_signature"),
                "key_log_excerpt": job.get("key_log_excerpt") or [],
                "key_log_excerpt_omitted_count": job.get("key_log_excerpt_omitted_count") or 0,
            }
        )
    return {
        "schema_version": "aistock_ci_failure_context_pack_v1",
        "pack_id": f"CP-CI-{str(summary.get('fingerprint') or 'unknown').replace('ci-', '')[:16]}",
        "phase": "ci_failure_intake",
        "task_tier": "T1",
        "module": event["module_guess"],
        "severity": event["severity_guess"],
        "diagnostic_status": event["diagnostic_status"],
        "problem_statement": _primary_failure(summary),
        "issue_creation_policy": handoff.get("issue_creation_policy") or {},
        "github_issue_number": str(github_issue_number) if github_issue_number else None,
        "github_issue_url": github_issue_url or _github_issue_url(github_issue_number),
        "failure_event": event,
        "agent_handoff": handoff,
        "llm_triage_advice": summary.get("llm_triage_advice"),
        "llm_guarded_rollout_gate": summary.get("llm_guarded_rollout_gate"),
        "reproduce_command": summary.get("reproduce_command") or "Inspect the linked CI run log.",
        "allowed_write_scope": handoff["allowed_write_scope"],
        "required_verification": handoff["required_verification"],
        "evidence_refs": event["evidence_refs"],
        "failed_jobs": failed_jobs,
        "last_green_locator": summary.get("last_green_locator"),
        "extraction_errors": summary.get("extraction_errors") or [],
        "token_budget": handoff["token_budget"],
        "omitted_details": {
            "full_job_logs": "Not embedded. Use run_url/full_log_ref for complete logs.",
            "historical_design_docs": "Not embedded. Load only if the promoted BUG scope requires them.",
        },
    }


def _llm_triage_lines(payload: dict[str, Any]) -> list[str]:
    if not payload:
        return []
    evidence = payload.get("llm_invocation_evidence") if isinstance(payload.get("llm_invocation_evidence"), dict) else {}
    return [
        "",
        "## LLM Triage Advice",
        "",
        f"- provider: `{payload.get('provider') or 'unknown'}`",
        f"- model: `{payload.get('model') or evidence.get('model') or 'unknown'}`",
        f"- workflow_gate: `{payload.get('workflow_gate') or 'unknown'}`",
        f"- invoked: `{evidence.get('invoked')}`",
        f"- input_policy: `{evidence.get('input_policy') or 'compact_failure_event_plus_code_intelligence_refs_only'}`",
    ]


def render_context_pack_markdown(context_pack: dict[str, Any]) -> str:
    handoff = context_pack.get("agent_handoff") or {}
    lines = [
        "# AIstock CI Failure Context Pack",
        "",
        "## Problem",
        "",
        f"- Module: `{context_pack.get('module') or 'unknown'}`",
        f"- Severity: `{context_pack.get('severity') or 'unknown'}`",
        f"- Diagnostic status: `{context_pack.get('diagnostic_status') or 'partial'}`",
        f"- Problem: `{context_pack.get('problem_statement') or 'unknown'}`",
        "",
        "## Regression Locator",
        "",
        f"- last_green_status: `{(context_pack.get('last_green_locator') or {}).get('status') or 'not_requested'}`",
        f"- commit_range: `{(context_pack.get('last_green_locator') or {}).get('commit_range') or 'unknown'}`",
        "",
        "## Agent Handoff",
        "",
    ]
    policy = handoff.get("issue_creation_policy") if isinstance(handoff.get("issue_creation_policy"), dict) else {}
    infra_action = handoff.get("infra_action") if isinstance(handoff.get("infra_action"), dict) else {}
    if policy:
        lines.append(f"- issue_creation_allowed: `{policy.get('allowed')}`")
        lines.append(f"- issue_creation_reason: `{policy.get('reason') or 'unknown'}`")
        if policy.get("next_command"):
            lines.append(f"- issue_creation_next_command: `{policy.get('next_command')}`")
    lines.append(f"- handoff_mode: `{handoff.get('handoff_mode') or 'bug_promotion'}`")
    lines.append(f"- needs_bug_json: `{handoff.get('needs_bug_json')}`")
    lines.extend(_llm_triage_lines(context_pack.get("llm_triage_advice") if isinstance(context_pack.get("llm_triage_advice"), dict) else {}))
    if infra_action:
        lines.append(f"- infra_action: `{infra_action.get('workflow_gate')}`")
    for command in handoff.get("next_commands") or []:
        lines.append(f"- `{command}`")
    lines.extend(
        [
            "",
            "## Scope",
            "",
            *[f"- `{item}`" for item in context_pack.get("allowed_write_scope") or ["triage required before editing"]],
            "",
            "## Required Verification",
            "",
            *[f"- `{item}`" for item in context_pack.get("required_verification") or ["selected validation after promotion"]],
            "",
            "## Evidence",
            "",
            *[f"- {item}" for item in context_pack.get("evidence_refs") or ["n/a"]],
            "",
            "## Token Budget",
            "",
            f"- target_tokens: `{(context_pack.get('token_budget') or {}).get('target_tokens')}`",
            f"- max_tokens: `{(context_pack.get('token_budget') or {}).get('max_tokens')}`",
            "- full_logs_included: `False`",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def build_github_issue_payload(summary: dict[str, Any], *, repo: str = DEFAULT_REPO) -> dict[str, Any]:
    """Build the GitHub issue payload used by CI registrar workflows."""
    policy = summary.get("issue_creation_policy") if isinstance(summary.get("issue_creation_policy"), dict) else {}
    if policy.get("allowed") is False:
        raise ValueError(
            f"CI failure issue is not actionable yet: {policy.get('reason')}; "
            f"rerun after completion with `{policy.get('next_command')}`"
        )
    severity = summary.get("severity") or "P1"
    if severity not in {"P0", "P1"}:
        raise ValueError("Only P0/P1 auto-file behavior is allowed.")
    if "llm_guarded_rollout_gate" not in summary:
        summary["llm_guarded_rollout_gate"] = _llm_guarded_rollout_gate(summary)
    rollout_gate = summary.get("llm_guarded_rollout_gate") if isinstance(summary.get("llm_guarded_rollout_gate"), dict) else {}
    fingerprint = summary.get("fingerprint") or f"run-{summary.get('run_id') or 'unknown'}"
    run_id = str(summary.get("run_id") or "")
    nightly_marker = None
    if summary.get("nightly_fingerprint"):
        nightly_marker = f"<!-- aistock-nightly-failure:{summary['nightly_fingerprint']} -->"
    marker = f"<!-- aistock-ci-failure-fingerprint:{fingerprint} -->"
    run_marker = f"<!-- aistock-issue-on-test-fail:{run_id} -->"
    detail_body = [item for item in [nightly_marker, marker, run_marker, f"<!-- aistock-ci-diagnostic-status:{summary.get('diagnostic_status') or 'partial'} -->"] if item]
    detail_body.extend(["", render_issue_markdown(summary).strip()])
    module_label_allowlist = {
        "module:validation",
        "module:validation.center",
        "module:paper_v2",
        "module:paper_v2_selection_center",
        "module:qe_archive",
        "module:qe",
        "module:quantevolver",
        "module:strategy_package",
        "module:research_assistant",
        "module:simulation_runtime",
        "module:rl_execution",
        "module:scripts",
    }
    module_labels = [
        label
        for module in summary.get("suspected_modules") or []
        for label in [f"module:{module}"]
        if label in module_label_allowlist
    ]
    labels = _unique(
        [
            "bug",
            "ci",
            "auto-filed",
            str(severity),
            *(
                [
                    "aistock:bug",
                    f"severity:{str(severity).lower()}",
                    "source:nightly",
                    "module:validation.runner",
                ]
                if summary.get("nightly_statuses")
                else []
            ),
            "status:open" if summary.get("diagnostic_status") == "complete" else "risk:observability",
            *module_labels,
        ]
    )
    return {
        "schema_version": "aistock_ci_failure_github_issue_payload_v1",
        "repo": repo,
        "title": summary.get("issue_title")
        or f"[{severity}] {summary.get('workflow') or 'AIstock CI'} failed on {summary.get('branch') or 'unknown'}",
        "body": "\n".join(detail_body).rstrip() + "\n",
        "labels": labels,
        "dedupe": {
            "fingerprint": fingerprint,
            "marker": marker,
            "nightly_marker": nightly_marker,
            "run_marker": run_marker,
            "search_query": f"repo:{repo} is:issue in:body {nightly_marker or marker}",
        },
        "recurrence_comment": render_recurrence_comment(summary),
        "llm_enhancement": {
            "allowed": bool(rollout_gate.get("llm_enhancement_allowed")),
            "workflow_gate": rollout_gate.get("workflow_gate") or "unknown",
            "mode": rollout_gate.get("mode") or "unknown",
            "fallback": rollout_gate.get("fallback") or "deterministic_issue_workflow",
            "deterministic_issue_creation_unaffected": rollout_gate.get("deterministic_issue_creation_unaffected") is not False,
        },
    }


def _dedupe_marker(summary: dict[str, Any]) -> str:
    fingerprint = summary.get("fingerprint") or f"run-{summary.get('run_id') or 'unknown'}"
    return f"<!-- aistock-ci-failure-fingerprint:{fingerprint} -->"


def render_recurrence_comment(summary: dict[str, Any]) -> str:
    fingerprint = summary.get("fingerprint") or "unknown"
    failed_jobs = summary.get("failed_jobs") or []
    failed_job_names = [str(job.get("job_name")) for job in failed_jobs if job.get("job_name")]
    lines = [
        f"### Recurrence observed for fingerprint {fingerprint}",
        "",
        f"- Latest run: {summary.get('run_url') or summary.get('run_id') or 'unknown'}",
        f"- Branch: {summary.get('branch') or 'unknown'}",
        f"- Commit: {summary.get('commit') or 'unknown'}",
        f"- Diagnostic status: {summary.get('diagnostic_status') or 'partial'}",
        f"- Failed jobs: {', '.join(failed_job_names) or 'unknown'}",
    ]
    statuses = summary.get("nightly_statuses") if isinstance(summary.get("nightly_statuses"), dict) else None
    if statuses:
        lines.append(
            "- Nightly statuses: "
            + ", ".join(f"{key}={statuses.get(key) or 'unknown'}" for key in NIGHTLY_STATUS_KEYS)
        )
    return "\n".join(lines)


def summarize_manual(args: argparse.Namespace) -> dict[str, Any]:
    return finalize_summary(
        {
            "schema_version": "aistock_ci_failure_summary_v1",
            "generated_at": _utc_now(),
            "severity": args.severity,
            "workflow": args.source_name or "manual dispatch",
            "run_id": str(args.run_id or ""),
            "run_url": args.run_url,
            "branch": args.branch,
            "commit": args.commit,
            "manual_summary": args.manual_summary,
            "failed_jobs": [],
            "extraction_errors": ["manual dispatch did not include machine-readable job logs"],
        }
    )


def _nightly_statuses_from_payload(payload: dict[str, Any]) -> dict[str, str]:
    nested = payload.get("statuses") if isinstance(payload.get("statuses"), dict) else {}
    statuses: dict[str, str] = {}
    for key in NIGHTLY_STATUS_KEYS:
        value: Any = None
        for alias in NIGHTLY_STATUS_ALIASES[key]:
            if alias in payload:
                value = payload[alias]
                break
            if alias in nested:
                value = nested[alias]
                break
        statuses[key] = _status_value(value)
    return statuses


def _nightly_failed_keys(statuses: dict[str, str]) -> list[str]:
    failed: list[str] = []
    for key in NIGHTLY_STATUS_KEYS:
        status = _status_value(statuses.get(key))
        if status in NIGHTLY_FAILURE_STATUSES or (status not in {"success", "skipped", "neutral", "unknown"} and "fail" in status):
            failed.append(key)
    return failed


def _nightly_actionable_failed_keys(statuses: dict[str, str]) -> list[str]:
    return [key for key in _nightly_failed_keys(statuses) if key != "code_intelligence"]


def _nightly_fingerprint(statuses: dict[str, str]) -> str:
    if statuses.get("runner_preflight") == "failure":
        return "runner-preflight-unavailable"
    return "nightly-" + "-".join(statuses.get(key, "unknown") for key in NIGHTLY_STATUS_KEYS)


def _nightly_job_from_statuses(statuses: dict[str, str], *, run_url: str | None = None) -> dict[str, Any]:
    failed_keys = _nightly_failed_keys(statuses)
    if statuses.get("runner_preflight") == "failure":
        error = "self-hosted Windows runner unavailable"
        module = "validation"
        files = [".github/workflows/nightly.yml", "scripts/aistock_runner_health.py"]
    elif "nightly_l3" in failed_keys:
        error = "Nightly failed: " + ", ".join(f"{key}={statuses.get(key)}" for key in failed_keys)
        module = "paper_v2"
        files = [
            ".github/workflows/nightly.yml",
            "noxfile.py",
            "scripts/aistock_data_quality_smoke.py",
            "scripts/aistock_validate.py",
        ]
    elif failed_keys:
        error = "Nightly failed: " + ", ".join(f"{key}={statuses.get(key)}" for key in failed_keys)
        module = "validation"
        files = [".github/workflows/nightly.yml"]
    else:
        error = "Nightly status did not contain a failing stage"
        module = "validation"
        files = [".github/workflows/nightly.yml"]
    return {
        "job_name": "AIstock Nightly status",
        "job_url": run_url,
        "failed_step": failed_keys[0] if failed_keys else "nightly_status",
        "command": "gh run view <run-id> --workflow nightly.yml",
        "nox_session": None,
        "pytest_summary": None,
        "failed_tests": [],
        "error_signature": error,
        "key_log_excerpt": [f"{key}: {statuses.get(key)}" for key in NIGHTLY_STATUS_KEYS],
        "key_log_excerpt_omitted_count": 0,
        "suspected_module": module,
        "suspected_files": files,
    }


def _build_nightly_llm_triage_advice(summary: dict[str, Any], *, provider: str = "github_models") -> dict[str, Any]:
    """Attach schema-checked LLM triage advice without performing a network call."""

    try:
        root = Path(__file__).resolve().parents[1]
        if str(root) not in sys.path:
            sys.path.insert(0, str(root))
        from scripts import llm_provider_adapter

        config = llm_provider_adapter.load_config()
        advice = llm_provider_adapter.build_triage_quality_smoke(provider, config)
        test_plan_advice = llm_provider_adapter.build_test_plan_advice(
            provider,
            config,
            changed_files=list(summary.get("suspected_files") or []),
            module=(summary.get("suspected_modules") or [None])[0],
        )
    except Exception as exc:
        return {
            "schema_version": "aistock_deepseek_triage_advice_v1",
            "provider": provider,
            "workflow_gate": "warning",
            "blocking_for_issue_creation": False,
            "fallback_used": True,
            "fallback_reason": str(exc),
            "llm_invocation_evidence": {
                "schema_version": "aistock_llm_invocation_evidence_v1",
                "provider": provider,
                "model": "unknown",
                "invoked": False,
                "reason": "triage_advice_unavailable",
                "redaction_applied": True,
            },
        }
    advice = dict(advice)
    advice["workflow_gate"] = "ready"
    advice["blocking_for_issue_creation"] = False
    advice["failure_event_ref"] = (summary.get("failure_event") or {}).get("event_id")
    advice["source_fingerprint"] = summary.get("fingerprint")
    advice["code_intelligence_input_policy"] = {
        "required": True,
        "full_repo_scan_allowed": False,
        "full_logs_included": False,
    }
    advice["test_plan_advice_gate"] = {
        "schema_version": test_plan_advice["schema_version"],
        "workflow_gate": test_plan_advice["deterministic_gate"]["workflow_gate"],
        "advised_plan_count": len(test_plan_advice["test_plan_advice"]),
        "allowed_plan_count": len([item for item in test_plan_advice["test_plan_advice"] if item["allowed"]]),
        "validation_select_compatible": test_plan_advice["deterministic_gate"]["validation_select_compatible"],
        "workspace_path_allowed": test_plan_advice["deterministic_gate"]["workspace_path_allowed"],
        "shell_commands_allowed": test_plan_advice["deterministic_gate"]["shell_commands_allowed"],
        "llm_invoked": test_plan_advice["llm_invocation_evidence"]["invoked"],
        "plans": [
            {
                "plan_key": item["plan_key"],
                "allowed": item["allowed"],
                "nox_session": item.get("nox_session"),
                "rejection_reasons": item.get("rejection_reasons") or [],
            }
            for item in test_plan_advice["test_plan_advice"]
        ],
    }
    return advice


def summarize_nightly_status(
    payload: dict[str, Any],
    *,
    repo: str = DEFAULT_REPO,
    run_id: str | None = None,
    run_url: str | None = None,
    severity: str = "P1",
    branch: str | None = None,
    commit: str | None = None,
) -> dict[str, Any]:
    statuses = _nightly_statuses_from_payload(payload)
    effective_run_id = str(run_id or payload.get("run_id") or payload.get("runId") or "")
    effective_run_url = run_url or payload.get("run_url") or payload.get("runUrl")
    effective_branch = branch or payload.get("branch") or payload.get("headBranch") or "main"
    effective_commit = commit or payload.get("commit") or payload.get("headSha")
    fingerprint = _nightly_fingerprint(statuses)
    runner_failed = statuses.get("runner_preflight") == "failure"
    failed_keys = _nightly_failed_keys(statuses)
    actionable_failed_keys = _nightly_actionable_failed_keys(statuses)
    code_intelligence_only = bool(failed_keys) and not actionable_failed_keys
    title = (
        "P1 Nightly blocked: self-hosted Windows runner unavailable"
        if runner_failed
        else "P1 Nightly failed: "
        + " ".join(
            [
                f"runner={statuses.get('runner_preflight')}",
                f"dr={statuses.get('dr_snapshot')}/{statuses.get('dr_validate')}",
                f"l3={statuses.get('nightly_l3')}",
                f"live={statuses.get('paper_v2_live')}",
                f"code={statuses.get('code_intelligence')}",
            ]
        )
    )
    job = _nightly_job_from_statuses(statuses, run_url=effective_run_url)
    summary = finalize_summary(
        {
            "schema_version": "aistock_ci_failure_summary_v1",
            "generated_at": _utc_now(),
            "severity": severity,
            "workflow": "AIstock Nightly L3 + DR",
            "source_name": "AIstock Nightly",
            "run_id": effective_run_id,
            "run_url": effective_run_url,
            "branch": effective_branch,
            "commit": effective_commit,
            "manual_summary": title,
            "failed_jobs": [job] if failed_keys else [],
            "extraction_errors": (
                ["code intelligence warning-only stage failed; no actionable Nightly issue payload required"]
                if code_intelligence_only
                else ([] if failed_keys else ["nightly status payload did not include a failing stage"])
            ),
            "nightly_statuses": statuses,
            "nightly_fingerprint": fingerprint,
            "nightly_failed_stages": failed_keys,
            "defer_issue_creation": code_intelligence_only,
        }
    )
    summary["fingerprint_source"] = fingerprint
    summary["fingerprint"] = "ci-" + hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()[:16]
    summary["issue_title"] = title[:240]
    summary["reproduce_command"] = f"gh run view {effective_run_id} --repo {repo}" if effective_run_id else "Inspect the linked Nightly run."
    summary["last_green_locator"] = _last_green_payload(summary, status="not_requested")
    summary["failure_event"] = build_failure_event(summary)
    summary["agent_handoff"] = build_agent_handoff(summary)
    summary["llm_triage_advice"] = _build_nightly_llm_triage_advice(summary)
    return summary


def summarize_actions_run(
    *,
    repo: str,
    run_id: str,
    run_url: str | None = None,
    severity: str = "P1",
    log_provider: Callable[[str, str], str] | None = None,
    wait_for_completion: bool = False,
    wait_attempts: int = 1,
    wait_seconds: float = 20.0,
    log_attempts: int = 1,
    log_wait_seconds: float = 10.0,
) -> dict[str, Any]:
    run_view_args = [
        "gh",
        "run",
        "view",
        str(run_id),
        "--repo",
        repo,
        "--json",
        "databaseId,name,workflowName,displayTitle,event,headBranch,headSha,status,conclusion,url,jobs",
    ]
    attempts = max(1, wait_attempts if wait_for_completion else 1)
    result: dict[str, Any] = {"ok": False, "stdout": "", "stderr": "run view not attempted"}
    run_payload: dict[str, Any] = {}
    for attempt in range(attempts):
        result = _run(run_view_args, timeout=120)
        if result.get("ok"):
            run_payload = json.loads(str(result.get("stdout") or "{}"))
            if not wait_for_completion or _status_value(run_payload.get("status")) == "completed":
                break
        if wait_for_completion and attempt + 1 < attempts:
            time.sleep(max(0.0, wait_seconds))
    extraction_errors: list[str] = []
    if not result.get("ok"):
        extraction_errors.append(result.get("stderr") or result.get("stdout") or "gh run view failed")
        return finalize_summary(
            {
                "schema_version": "aistock_ci_failure_summary_v1",
                "generated_at": _utc_now(),
                "severity": severity,
                "workflow": "unknown",
                "run_id": str(run_id),
                "run_url": run_url,
                "branch": None,
                "commit": None,
                "failed_jobs": [],
                "extraction_errors": extraction_errors,
            }
        )
    if wait_for_completion and _status_value(run_payload.get("status")) != "completed":
        extraction_errors.append(
            f"run {run_id} is still in progress after {attempts} check(s); logs will be available when it is complete"
        )
        return finalize_summary(
            {
                "schema_version": "aistock_ci_failure_summary_v1",
                "generated_at": _utc_now(),
                "severity": severity,
                "workflow": run_payload.get("workflowName") or run_payload.get("name"),
                "display_title": run_payload.get("displayTitle"),
                "event": run_payload.get("event"),
                "run_id": str(run_payload.get("databaseId") or run_id),
                "run_url": run_payload.get("url") or run_url,
                "branch": run_payload.get("headBranch"),
                "commit": run_payload.get("headSha"),
                "conclusion": run_payload.get("conclusion"),
                "failed_jobs": [],
                "extraction_errors": extraction_errors,
                "defer_issue_creation": True,
            }
        )
    failed_jobs: list[dict[str, Any]] = []
    for job in run_payload.get("jobs") or []:
        if job.get("conclusion") != "failure":
            continue
        job_id = str(job.get("databaseId") or "")
        log_text = ""
        if log_provider is not None:
            log_text = log_provider(str(run_id), job_id)
        else:
            log_result: dict[str, Any] = {"ok": False, "stdout": "", "stderr": "log fetch not attempted"}
            for attempt in range(max(1, log_attempts)):
                log_result = _run(["gh", "run", "view", str(run_id), "--repo", repo, "--job", job_id, "--log"], timeout=180)
                if log_result.get("ok") or not _logs_not_ready_error(log_result.get("stderr") or log_result.get("stdout")):
                    break
                if attempt + 1 < max(1, log_attempts):
                    time.sleep(max(0.0, log_wait_seconds))
            if log_result.get("ok"):
                log_text = str(log_result.get("stdout") or "")
            else:
                extraction_errors.append(f"{job.get('name')}: {log_result.get('stderr') or log_result.get('stdout')}")
        parsed = parse_job_log(log_text, job_name=str(job.get("name") or ""), job_url=job.get("url"))
        parsed["conclusion"] = job.get("conclusion")
        parsed["database_id"] = job.get("databaseId")
        failed_jobs.append(parsed)
    finalized = finalize_summary(
        {
            "schema_version": "aistock_ci_failure_summary_v1",
            "generated_at": _utc_now(),
            "severity": severity,
            "workflow": run_payload.get("workflowName") or run_payload.get("name"),
            "display_title": run_payload.get("displayTitle"),
            "event": run_payload.get("event"),
            "run_id": str(run_payload.get("databaseId") or run_id),
            "run_url": run_payload.get("url") or run_url,
            "branch": run_payload.get("headBranch"),
            "commit": run_payload.get("headSha"),
            "conclusion": run_payload.get("conclusion"),
            "failed_jobs": failed_jobs,
            "extraction_errors": extraction_errors,
        }
    )
    finalized["last_green_locator"] = locate_last_green_run(finalized, repo=repo)
    finalized["failure_event"] = build_failure_event(finalized)
    finalized["agent_handoff"] = build_agent_handoff(finalized)
    return finalized


def render_issue_markdown(summary: dict[str, Any], *, github_issue_number: int | str | None = None) -> str:
    handoff = build_agent_handoff(summary, github_issue_number=github_issue_number)
    lines = [
        "## Failure Summary",
        "",
        f"- Diagnostic status: `{summary.get('diagnostic_status')}`",
        f"- Workflow/source: `{summary.get('workflow') or 'unknown'}`",
        f"- Run: {summary.get('run_url') or summary.get('run_id') or 'n/a'}",
        f"- Branch: `{summary.get('branch') or 'unknown'}`",
        f"- Commit: `{summary.get('commit') or 'unknown'}`",
        f"- Fingerprint: `{summary.get('fingerprint')}`",
        "",
    ]
    locator = summary.get("last_green_locator") or {}
    lines.extend(
        [
            "## Regression Locator",
            "",
            f"- last_green_status: `{locator.get('status') or 'not_requested'}`",
            f"- commit_range: `{locator.get('commit_range') or 'unknown'}`",
        ]
    )
    previous = locator.get("previous_success_run") if isinstance(locator.get("previous_success_run"), dict) else None
    if previous:
        lines.append(f"- previous_success_run: {previous.get('run_url') or previous.get('run_id') or 'n/a'}")
    if locator.get("warnings"):
        lines.extend([f"- warning: `{item}`" for item in locator.get("warnings") or []])
    statuses = summary.get("nightly_statuses") if isinstance(summary.get("nightly_statuses"), dict) else None
    if statuses:
        lines.extend(["", "## Nightly Statuses", ""])
        for key in NIGHTLY_STATUS_KEYS:
            lines.append(f"- {key}: `{statuses.get(key) or 'unknown'}`")
    llm_advice = summary.get("llm_triage_advice") if isinstance(summary.get("llm_triage_advice"), dict) else {}
    llm_evidence = llm_advice.get("llm_invocation_evidence") if isinstance(llm_advice.get("llm_invocation_evidence"), dict) else {}
    if llm_advice:
        lines.extend(
            [
                "",
                "## LLM Triage Advice",
                "",
                f"- provider: `{llm_advice.get('provider') or 'unknown'}`",
                f"- model: `{llm_advice.get('model') or llm_evidence.get('model') or 'unknown'}`",
                f"- workflow_gate: `{llm_advice.get('workflow_gate') or 'unknown'}`",
                f"- invoked: `{llm_evidence.get('invoked')}`",
                f"- input_policy: `{llm_evidence.get('input_policy') or 'compact_failure_event_plus_code_intelligence_refs_only'}`",
            ]
        )
    rollout_gate = summary.get("llm_guarded_rollout_gate") if isinstance(summary.get("llm_guarded_rollout_gate"), dict) else {}
    if rollout_gate:
        lines.extend(
            [
                "",
                "## LLM Guarded Rollout",
                "",
                f"- mode: `{rollout_gate.get('mode') or 'unknown'}`",
                f"- workflow_gate: `{rollout_gate.get('workflow_gate') or 'unknown'}`",
                f"- auto_file_allowed: `{rollout_gate.get('auto_file_allowed')}`",
                f"- llm_can_enhance_issue: `{rollout_gate.get('llm_can_enhance_issue')}`",
                f"- fallback: `{rollout_gate.get('fallback') or 'deterministic_issue_workflow'}`",
            ]
        )
    policy = summary.get("issue_creation_policy") if isinstance(summary.get("issue_creation_policy"), dict) else {}
    if policy:
        lines.extend(
            [
                "",
                "## Issue Creation Policy",
                "",
                f"- allowed: `{policy.get('allowed')}`",
                f"- reason: `{policy.get('reason') or 'unknown'}`",
            ]
        )
        if policy.get("next_command"):
            lines.append(f"- next_command: `{policy.get('next_command')}`")
    lines.extend(
        [
            "",
            "## Failed Jobs",
            "",
            "| Job | Nox/session | Command | Result |",
            "| --- | --- | --- | --- |",
        ]
    )
    failed_jobs = summary.get("failed_jobs") or []
    if not failed_jobs:
        lines.append("| n/a | n/a | n/a | diagnostic extraction did not find failed jobs |")
    for job in failed_jobs:
        result = job.get("pytest_summary") or job.get("error_signature") or job.get("conclusion") or "failure"
        lines.append(
            f"| `{job.get('job_name') or 'unknown'}` | `{job.get('nox_session') or job.get('failed_step') or 'unknown'}` | "
            f"`{job.get('command') or 'see job log'}` | `{result}` |"
        )
    lines.extend(["", "## Failed Tests / Errors", ""])
    if not failed_jobs:
        lines.append("- No failed test was extracted. Inspect the linked run log.")
    for job in failed_jobs:
        for test in job.get("failed_tests") or []:
            lines.append(f"- Failed test: `{test}`")
        if job.get("error_signature"):
            lines.append(f"- Error signature: `{job['error_signature']}`")
        for excerpt in job.get("key_log_excerpt") or []:
            lines.append(f"- Log excerpt: `{excerpt}`")
        if job.get("key_log_excerpt_omitted_count"):
            lines.append(
                f"- Additional matching log lines omitted from issue body: `{job['key_log_excerpt_omitted_count']}`; use the run URL for full logs."
            )
    if summary.get("extraction_errors"):
        lines.extend(["", "## Extraction Errors", ""])
        for error in summary.get("extraction_errors") or []:
            lines.append(f"- `{str(error)}`")
    infra_action = handoff.get("infra_action") if isinstance(handoff.get("infra_action"), dict) else {}
    handoff_prelude = [
        f"- handoff_mode: `{handoff.get('handoff_mode') or 'bug_promotion'}`",
        f"- needs_bug_json: `{handoff.get('needs_bug_json')}`",
    ]
    if infra_action:
        handoff_prelude.append(f"- infra_action: `{infra_action.get('workflow_gate')}`")
        handoff_prelude.extend([f"- {item}" for item in infra_action.get("next_actions") or []])
    bug_linkage_line = (
        "- BUG ID: not applicable for infra-only issue unless `triage-ci-issue` later reclassifies it."
        if handoff.get("needs_bug_json") is False
        else "- BUG ID: pending until promoted by `aistock_issue_workflow.py promote-ci-issue`."
    )
    lines.extend(
        [
            "",
            "## Agent Handoff",
            "",
            "Run these commands from a clean AIstock worktree. They are agent-neutral and work for Codex, Claude Code, Cursor, or a human operator.",
            "",
            *handoff_prelude,
            "",
            "```bash",
            *handoff["next_commands"],
            "```",
            "",
            "Token policy: use this issue and the generated Context Pack first; do not scan full logs or historical design docs unless the triage output says they are needed.",
            "",
            "## Reproduce",
            "",
            "```bash",
            str(summary.get("reproduce_command") or "Inspect the linked CI run log."),
            "```",
            "",
            "## Suggested Triage",
            "",
            "- [ ] real_regression",
            "- [ ] test_fixture_gap",
            "- [ ] infra_flaky",
            "- [ ] duplicate",
            "- [ ] expected_failure",
            "",
            "## BUG JSON Linkage",
            "",
            bug_linkage_line,
            "",
            "## Production Gates",
            "",
            f"- production_ddl_gate: `{summary.get('production_ddl_gate')}`",
            f"- production_frontend_dependency_gate: `{summary.get('production_frontend_dependency_gate')}`",
            f"- production_backend_dependency_gate: `{summary.get('production_backend_dependency_gate')}`",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def _write_json(path: str | None, payload: dict[str, Any]) -> None:
    if not path:
        return
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_text(path: str | None, text: str) -> None:
    if not path:
        return
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _default_candidate_history_dir() -> Path:
    return _repo_root() / "tmp" / "validation" / "ci_failure_issue" / "candidate_history"


def _safe_filename(value: Any, *, fallback: str = "candidate") -> str:
    text = re.sub(r"[^A-Za-z0-9_.-]+", "-", str(value or "").strip()).strip(".-")
    return text[:120] or fallback


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _candidate_history_path(summary: dict[str, Any], history_dir: str | None) -> Path | None:
    if not history_dir:
        return None
    source = "nightly" if summary.get("nightly_statuses") else "ci"
    fingerprint = _safe_filename(summary.get("fingerprint") or summary.get("nightly_fingerprint") or summary.get("run_id"))
    return Path(history_dir) / source / f"{fingerprint}.json"


def _auto_candidate_history_dir(output_path: str | None) -> str | None:
    if not output_path:
        return None
    normalized = Path(output_path).as_posix()
    if "/tmp/validation/ci_failure_issue/" in f"/{normalized}" or "/tmp/validation/nightly_failure_issue/" in f"/{normalized}":
        return str(Path(output_path).parent / "candidate_history")
    return None


def _build_candidate_history(summary: dict[str, Any], *, existing: dict[str, Any] | None = None) -> dict[str, Any]:
    fingerprint = str(summary.get("fingerprint") or "ci-unknown")
    event = summary.get("failure_event") if isinstance(summary.get("failure_event"), dict) else build_failure_event(summary)
    handoff = summary.get("agent_handoff") if isinstance(summary.get("agent_handoff"), dict) else build_agent_handoff(summary)
    existing = existing or {}
    observed_run_ids = _unique(
        [
            *[str(item) for item in existing.get("observed_run_ids") or [] if item],
            *[str(item) for item in [summary.get("run_id")] if item],
        ]
    )
    existing_run_count = int(existing.get("run_count") or 0)
    run_count = max(existing_run_count, len(observed_run_ids), 1)
    if summary.get("run_id") and str(summary["run_id"]) not in set(str(item) for item in existing.get("observed_run_ids") or []):
        run_count = max(run_count, existing_run_count + 1 if existing_run_count else 1)
    created_at = existing.get("created_at") or summary.get("generated_at") or _utc_now()
    module = event.get("module_guess") or (summary.get("suspected_modules") or ["validation"])[0]
    return {
        "schema_version": CANDIDATE_HISTORY_SCHEMA,
        "created_at": created_at,
        "last_seen_at": summary.get("generated_at") or _utc_now(),
        "run_count": run_count,
        "observed_run_ids": observed_run_ids,
        "candidate": {
            "candidate_id": existing.get("candidate", {}).get("candidate_id") if isinstance(existing.get("candidate"), dict) else None,
            "title": summary.get("issue_title") or _primary_failure(summary),
            "module": module,
            "severity": str(summary.get("severity") or "P1").upper(),
            "status": "new",
            "fingerprint": fingerprint,
            "dedupe_key": _dedupe_marker(summary),
            "allowed_write_scope": handoff.get("allowed_write_scope") or summary.get("suspected_files") or [],
            "required_validation": handoff.get("required_verification") or [],
            "evidence": event.get("evidence_refs") or [],
        },
        "failure_event": event,
        "agent_handoff": handoff,
        "source_summary": {
            "diagnostic_status": summary.get("diagnostic_status"),
            "issue_creation_policy": summary.get("issue_creation_policy") or {},
            "workflow": summary.get("workflow"),
            "run_id": summary.get("run_id"),
            "run_url": summary.get("run_url"),
            "branch": summary.get("branch"),
            "commit": summary.get("commit"),
            "nightly_failed_stages": summary.get("nightly_failed_stages") or [],
            "fingerprint": fingerprint,
        },
        "log_policy": {
            "full_log_embedded": False,
            "full_log_ref": summary.get("run_url"),
            "reason": "Candidate history stores compact handoff metadata only; use the run URL for full logs.",
        },
    }


def persist_candidate_history(summary: dict[str, Any], *, history_dir: str | None) -> Path | None:
    target = _candidate_history_path(summary, history_dir)
    if target is None:
        return None
    existing = _read_json_object(target)
    payload = _build_candidate_history(summary, existing=existing)
    candidate = payload.get("candidate")
    if isinstance(candidate, dict) and not candidate.get("candidate_id"):
        candidate["candidate_id"] = f"CAND-CI-{str(payload['source_summary']['fingerprint']).replace('ci-', '')[:16]}"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return target


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Summarize AIstock CI/Nightly failures for actionable GitHub Issues.")
    parser.add_argument("--repo", default=DEFAULT_REPO)
    parser.add_argument("--run-id")
    parser.add_argument("--run-url")
    parser.add_argument("--wait-for-completion", action="store_true", help="Poll the Actions run before extracting logs.")
    parser.add_argument("--wait-attempts", type=int, default=1, help="Number of run-status checks when --wait-for-completion is set.")
    parser.add_argument("--wait-seconds", type=float, default=20.0, help="Delay between run-status checks.")
    parser.add_argument("--log-attempts", type=int, default=1, help="Number of failed-job log fetch attempts.")
    parser.add_argument("--log-wait-seconds", type=float, default=10.0, help="Delay between failed-job log fetch attempts.")
    parser.add_argument("--severity", default="P1", choices=["P0", "P1", "P2", "P3"])
    parser.add_argument("--manual-summary")
    parser.add_argument("--nightly-status-json", help="Parse compact Nightly job result JSON instead of Actions logs.")
    parser.add_argument("--source-name")
    parser.add_argument("--branch")
    parser.add_argument("--commit")
    parser.add_argument("--log-file", help="Parse one local job log fixture instead of calling gh.")
    parser.add_argument("--job-name", default="")
    parser.add_argument("--output")
    parser.add_argument("--markdown-output")
    parser.add_argument("--context-output")
    parser.add_argument("--context-markdown-output")
    parser.add_argument("--github-issue-payload-output")
    parser.add_argument("--llm-triage-mode", choices=["off", "warning_only", "opt_in_auto_file"])
    parser.add_argument("--llm-auto-file-opt-in", action="store_true", default=None)
    parser.add_argument(
        "--candidate-history-dir",
        help=(
            "Persist compact candidate handoff JSON under this stable directory. "
            "If omitted, CI/Nightly outputs under tmp/validation write candidate history next to the output artifact; "
            "other invocations do not persist candidate history."
        ),
    )
    parser.add_argument("--no-candidate-history", action="store_true", help="Do not persist compact candidate history JSON.")
    parser.add_argument("--stdout-format", choices=["full-json", "compact"], default="full-json")
    return parser


def build_stdout_payload(summary: dict[str, Any], args: argparse.Namespace, *, candidate_history_path: Path | None = None) -> dict[str, Any]:
    policy = summary.get("issue_creation_policy") if isinstance(summary.get("issue_creation_policy"), dict) else {}
    github_issue_payload_ref = None if policy.get("allowed") is False else args.github_issue_payload_output
    if args.stdout_format == "full-json":
        payload = dict(summary)
        if candidate_history_path:
            payload["candidate_history_path"] = str(candidate_history_path)
        return payload
    return {
        "schema_version": "aistock_ci_failure_summary_compact_v1",
        "diagnostic_status": summary.get("diagnostic_status"),
        "severity": summary.get("severity"),
        "issue_title": summary.get("issue_title"),
        "fingerprint": summary.get("fingerprint"),
        "nightly_failed_stages": summary.get("nightly_failed_stages") or [],
        "issue_creation_policy": summary.get("issue_creation_policy") or {},
        "llm_triage": {
            "provider": (summary.get("llm_triage_advice") or {}).get("provider")
            if isinstance(summary.get("llm_triage_advice"), dict)
            else None,
            "workflow_gate": (summary.get("llm_triage_advice") or {}).get("workflow_gate")
            if isinstance(summary.get("llm_triage_advice"), dict)
            else None,
            "invoked": (
                ((summary.get("llm_triage_advice") or {}).get("llm_invocation_evidence") or {}).get("invoked")
                if isinstance(summary.get("llm_triage_advice"), dict)
                else None
            ),
        },
        "llm_guarded_rollout": {
            "workflow_gate": (summary.get("llm_guarded_rollout_gate") or {}).get("workflow_gate")
            if isinstance(summary.get("llm_guarded_rollout_gate"), dict)
            else None,
            "auto_file_allowed": (summary.get("llm_guarded_rollout_gate") or {}).get("auto_file_allowed")
            if isinstance(summary.get("llm_guarded_rollout_gate"), dict)
            else None,
            "fallback": (summary.get("llm_guarded_rollout_gate") or {}).get("fallback")
            if isinstance(summary.get("llm_guarded_rollout_gate"), dict)
            else None,
        },
        "suspected_modules": summary.get("suspected_modules") or [],
        "suspected_files_count": len(summary.get("suspected_files") or []),
        "failed_jobs_count": len(summary.get("failed_jobs") or []),
        "artifacts": {
            "summary": args.output,
            "markdown": args.markdown_output,
            "context": args.context_output,
            "context_markdown": args.context_markdown_output,
            "github_issue_payload": github_issue_payload_ref,
            "candidate_history": str(candidate_history_path) if candidate_history_path else None,
        },
    }


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.log_file:
        log_text = Path(args.log_file).read_text(encoding="utf-8")
        job = parse_job_log(log_text, job_name=args.job_name)
        summary = finalize_summary(
            {
                "schema_version": "aistock_ci_failure_summary_v1",
                "generated_at": _utc_now(),
                "severity": args.severity,
                "workflow": args.source_name or "local log fixture",
                "run_id": str(args.run_id or ""),
                "run_url": args.run_url,
                "branch": args.branch,
                "commit": args.commit,
                "failed_jobs": [job],
                "extraction_errors": [],
            }
        )
    elif args.manual_summary:
        summary = summarize_manual(args)
    elif args.nightly_status_json:
        payload = json.loads(Path(args.nightly_status_json).read_text(encoding="utf-8-sig"))
        if not isinstance(payload, dict):
            raise SystemExit("--nightly-status-json must contain a JSON object")
        summary = summarize_nightly_status(
            payload,
            repo=args.repo,
            run_id=args.run_id,
            run_url=args.run_url,
            severity=args.severity,
            branch=args.branch,
            commit=args.commit,
        )
    elif args.run_id:
        summary = summarize_actions_run(
            repo=args.repo,
            run_id=str(args.run_id),
            run_url=args.run_url,
            severity=args.severity,
            wait_for_completion=args.wait_for_completion,
            wait_attempts=args.wait_attempts,
            wait_seconds=args.wait_seconds,
            log_attempts=args.log_attempts,
            log_wait_seconds=args.log_wait_seconds,
        )
    else:
        raise SystemExit("--run-id, --manual-summary, --nightly-status-json, or --log-file is required")

    if "llm_guarded_rollout_gate" not in summary or args.llm_triage_mode or args.llm_auto_file_opt_in:
        summary["llm_guarded_rollout_gate"] = _llm_guarded_rollout_gate(
            summary,
            mode=args.llm_triage_mode,
            opt_in=args.llm_auto_file_opt_in if args.llm_auto_file_opt_in else None,
        )
    _write_json(args.output, summary)
    _write_text(args.markdown_output, render_issue_markdown(summary))
    context_pack = build_context_pack(summary)
    _write_json(args.context_output, context_pack)
    _write_text(args.context_markdown_output, render_context_pack_markdown(context_pack))
    policy = summary.get("issue_creation_policy") if isinstance(summary.get("issue_creation_policy"), dict) else {}
    if args.github_issue_payload_output and policy.get("allowed") is not False:
        _write_json(args.github_issue_payload_output, build_github_issue_payload(summary, repo=args.repo))
    elif args.github_issue_payload_output:
        try:
            Path(args.github_issue_payload_output).unlink()
        except FileNotFoundError:
            pass
    history_dir = None if args.no_candidate_history else (args.candidate_history_dir or _auto_candidate_history_dir(args.output))
    candidate_history_path = persist_candidate_history(summary, history_dir=history_dir)
    sys.stdout.write(json.dumps(build_stdout_payload(summary, args, candidate_history_path=candidate_history_path), ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

