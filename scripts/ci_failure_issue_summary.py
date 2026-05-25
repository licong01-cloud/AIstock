from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


DEFAULT_REPO = "licong01-cloud/AIstock"


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


def parse_job_log(log_text: str, *, job_name: str = "", job_url: str | None = None) -> dict[str, Any]:
    lines = normalize_log(log_text)
    joined = "\n".join(lines)
    nox_session = _first_match(lines, [r"nox > Running session ([A-Za-z0-9_.-]+)", r"nox -s ([A-Za-z0-9_.-]+)"])
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
    for pattern in [r"FAILED\s+[^\s]+::[^\s]+\s+-\s+(.+)", r"\b(E\s+assert\s+.+)", r"(assert\s+.+)"]:
        match = re.search(pattern, joined)
        if match:
            error_signature = match.group(1)
            break
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
    key_log_excerpt = _unique(excerpt_candidates)[:12]
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
    if failed_jobs and any(job.get("failed_tests") or job.get("error_signature") or job.get("command") for job in failed_jobs):
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
    nox_session = first_job.get("nox_session") or first_job.get("job_name") or "ci"
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
    return summary


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


def summarize_actions_run(
    *,
    repo: str,
    run_id: str,
    run_url: str | None = None,
    severity: str = "P1",
    log_provider: Callable[[str, str], str] | None = None,
) -> dict[str, Any]:
    result = _run(
        [
            "gh",
            "run",
            "view",
            str(run_id),
            "--repo",
            repo,
            "--json",
            "databaseId,name,workflowName,displayTitle,event,headBranch,headSha,status,conclusion,url,jobs",
        ],
        timeout=120,
    )
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
    run_payload = json.loads(str(result.get("stdout") or "{}"))
    failed_jobs: list[dict[str, Any]] = []
    for job in run_payload.get("jobs") or []:
        if job.get("conclusion") != "failure":
            continue
        job_id = str(job.get("databaseId") or "")
        log_text = ""
        if log_provider is not None:
            log_text = log_provider(str(run_id), job_id)
        else:
            log_result = _run(["gh", "run", "view", str(run_id), "--repo", repo, "--job", job_id, "--log"], timeout=180)
            if log_result.get("ok"):
                log_text = str(log_result.get("stdout") or "")
            else:
                extraction_errors.append(f"{job.get('name')}: {log_result.get('stderr') or log_result.get('stdout')}")
        parsed = parse_job_log(log_text, job_name=str(job.get("name") or ""), job_url=job.get("url"))
        parsed["conclusion"] = job.get("conclusion")
        parsed["database_id"] = job.get("databaseId")
        failed_jobs.append(parsed)
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
            "failed_jobs": failed_jobs,
            "extraction_errors": extraction_errors,
        }
    )


def render_issue_markdown(summary: dict[str, Any]) -> str:
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
        "## Failed Jobs",
        "",
        "| Job | Nox/session | Command | Result |",
        "| --- | --- | --- | --- |",
    ]
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
            lines.append(f"- Log excerpt: `{excerpt[:500]}`")
    if summary.get("extraction_errors"):
        lines.extend(["", "## Extraction Errors", ""])
        for error in summary.get("extraction_errors") or []:
            lines.append(f"- `{str(error)[:500]}`")
    lines.extend(
        [
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
            "- BUG ID: pending until promoted by `aistock_issue_workflow.py promote-ci-issue`.",
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Summarize AIstock CI/Nightly failures for actionable GitHub Issues.")
    parser.add_argument("--repo", default=DEFAULT_REPO)
    parser.add_argument("--run-id")
    parser.add_argument("--run-url")
    parser.add_argument("--severity", default="P1", choices=["P0", "P1", "P2", "P3"])
    parser.add_argument("--manual-summary")
    parser.add_argument("--source-name")
    parser.add_argument("--branch")
    parser.add_argument("--commit")
    parser.add_argument("--log-file", help="Parse one local job log fixture instead of calling gh.")
    parser.add_argument("--job-name", default="")
    parser.add_argument("--output")
    parser.add_argument("--markdown-output")
    return parser


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
    elif args.run_id:
        summary = summarize_actions_run(repo=args.repo, run_id=str(args.run_id), run_url=args.run_url, severity=args.severity)
    else:
        raise SystemExit("--run-id, --manual-summary, or --log-file is required")

    _write_json(args.output, summary)
    _write_text(args.markdown_output, render_issue_markdown(summary))
    sys.stdout.write(json.dumps(summary, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
