from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

DEFAULT_REPO = "licong01-cloud/AIstock"
DEFAULT_WORKFLOW = "nightly.yml"
SCHEMA_VERSION = "aistock_runner_health_v1"
GITHUB_API = "https://api.github.com"
MAX_QUEUED_RUNS_TO_INSPECT = 5


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _read_json(path: str | Path) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def resolve_github_token() -> tuple[str | None, str]:
    """Resolve a GitHub token for local and Actions runner-health checks."""
    for name in ("AISTOCK_RUNNER_HEALTH_TOKEN", "GH_TOKEN", "GITHUB_TOKEN"):
        value = os.environ.get(name)
        if value:
            return value, name
    try:
        proc = subprocess.run(
            ["gh", "auth", "token"],
            text=True,
            capture_output=True,
            check=False,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return None, f"gh_auth_error:{type(exc).__name__}"
    token = proc.stdout.strip()
    if proc.returncode == 0 and token:
        return token, "gh_auth_token"
    return None, "missing"


def _github_get(path: str, *, token: str | None, timeout_seconds: int = 30) -> Any:
    request = urllib.request.Request(f"{GITHUB_API}{path}")
    request.add_header("Accept", "application/vnd.github+json")
    request.add_header("X-GitHub-Api-Version", "2022-11-28")
    request.add_header("User-Agent", "AIstock-runner-health")
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"GitHub API {path} failed: HTTP {exc.code}: {body}") from exc
    except Exception as exc:  # pragma: no cover - network failures vary by host
        raise RuntimeError(f"GitHub API {path} failed: {exc}") from exc


def _label_names(runner: dict[str, Any]) -> set[str]:
    return {str(item.get("name") or "").lower() for item in runner.get("labels") or []}


def _job_label_names(job: dict[str, Any]) -> set[str]:
    labels: set[str] = set()
    for item in job.get("labels") or []:
        value = item.get("name") if isinstance(item, dict) else item
        if value:
            labels.add(str(value).lower())
    return labels


def _runner_summary(runner: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": runner.get("id"),
        "name": runner.get("name"),
        "os": runner.get("os"),
        "status": runner.get("status"),
        "busy": runner.get("busy"),
        "labels": sorted(_label_names(runner)),
    }


def _matching_runners(runners: list[dict[str, Any]], required_labels: list[str]) -> list[dict[str, Any]]:
    required = {label.lower() for label in required_labels}
    matches: list[dict[str, Any]] = []
    for runner in runners:
        labels = _label_names(runner)
        if str(runner.get("status") or "").lower() == "online" and required.issubset(labels):
            matches.append(_runner_summary(runner))
    return matches


def _runner_role_matches(
    runners: list[dict[str, Any]],
    required_roles: Mapping[str, list[str]],
) -> dict[str, list[dict[str, Any]]]:
    return {
        role: _matching_runners(runners, labels)
        for role, labels in required_roles.items()
    }


def _stale_queued_runs(runs: list[dict[str, Any]], *, stale_minutes: int, now: datetime) -> list[dict[str, Any]]:
    stale: list[dict[str, Any]] = []
    for run in runs:
        if str(run.get("status") or "").lower() not in {"queued", "waiting", "pending", "requested"}:
            continue
        created_at = _parse_time(str(run.get("created_at") or ""))
        age_minutes = None
        if created_at:
            age_minutes = max(0.0, (now - created_at).total_seconds() / 60.0)
        if age_minutes is None or age_minutes >= stale_minutes:
            stale.append(
                {
                    "run_id": run.get("id") or run.get("databaseId"),
                    "status": run.get("status"),
                    "created_at": run.get("created_at") or run.get("createdAt"),
                    "age_minutes": round(age_minutes, 1) if age_minutes is not None else None,
                    "url": run.get("html_url") or run.get("url"),
                    "head_branch": run.get("head_branch") or run.get("headBranch"),
                    "head_sha": run.get("head_sha") or run.get("headSha"),
                }
            )
    return stale


def _queued_job_summaries(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    queued_statuses = {"queued", "waiting", "pending", "requested"}
    summaries: list[dict[str, Any]] = []
    for job in payload.get("jobs") or []:
        status = str(job.get("status") or "").lower()
        if status not in queued_statuses:
            continue
        summaries.append(
            {
                "job_id": job.get("id") or job.get("databaseId"),
                "name": job.get("name"),
                "status": status,
                "labels": sorted(_job_label_names(job)),
                "url": job.get("html_url") or job.get("url"),
            }
        )
    return summaries


def _job_payload_for_run(
    run_id: int | str,
    *,
    repo: str,
    token: str | None,
    jobs_payloads: Mapping[int | str, Mapping[str, Any]] | None,
    fetch_jobs: bool,
) -> Mapping[str, Any]:
    if jobs_payloads is not None:
        return jobs_payloads.get(run_id) or jobs_payloads.get(str(run_id)) or {"jobs": []}
    if not fetch_jobs:
        return {"jobs": []}
    return _github_get(
        f"/repos/{repo}/actions/runs/{run_id}/jobs?filter=all&per_page=100",
        token=token,
        timeout_seconds=10,
    )


def _annotate_stale_runs_with_jobs(
    stale_runs: list[dict[str, Any]],
    *,
    repo: str,
    required_labels: list[str],
    token: str | None,
    jobs_payloads: Mapping[int | str, Mapping[str, Any]] | None,
    fetch_jobs: bool,
    errors: list[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    required = {label.lower() for label in required_labels}
    matching: list[dict[str, Any]] = []
    other_role: list[dict[str, Any]] = []
    for run in stale_runs:
        run_id = run.get("run_id")
        try:
            jobs_payload = _job_payload_for_run(
                run_id,
                repo=repo,
                token=token,
                jobs_payloads=jobs_payloads,
                fetch_jobs=fetch_jobs,
            )
        except Exception as exc:
            errors.append(str(exc))
            run["queued_jobs"] = []
            run["queue_role_match"] = "unknown"
            matching.append(run)
            continue
        queued_jobs = _queued_job_summaries(jobs_payload)
        run["queued_jobs"] = queued_jobs
        jobs_with_labels = [job for job in queued_jobs if job.get("labels")]
        matching_jobs = [job for job in jobs_with_labels if required.issubset(set(job["labels"]))]
        if matching_jobs:
            run["queue_role_match"] = "matching"
            run["matching_queued_jobs"] = matching_jobs
            matching.append(run)
        elif jobs_with_labels:
            run["queue_role_match"] = "other_role"
            other_role.append(run)
        else:
            # A workflow can be queued by concurrency before GitHub creates a job.
            # Preserve the old fail-closed behavior when no job labels exist.
            run["queue_role_match"] = "unknown"
            matching.append(run)
    return matching, other_role


def build_runner_health_report(
    *,
    repo: str = DEFAULT_REPO,
    workflow: str = DEFAULT_WORKFLOW,
    required_labels: list[str] | None = None,
    required_roles: Mapping[str, list[str]] | None = None,
    stale_queued_minutes: int = 10,
    runners_payload: dict[str, Any] | None = None,
    runs_payload: dict[str, Any] | None = None,
    jobs_payloads: Mapping[int | str, Mapping[str, Any]] | None = None,
    token: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    required = required_labels or ["self-hosted", "windows"]
    current_time = now or datetime.now(timezone.utc)
    errors: list[str] = []
    if runners_payload is None:
        try:
            runners_payload = _github_get(f"/repos/{repo}/actions/runners?per_page=100", token=token)
        except Exception as exc:
            runners_payload = {"total_count": None, "runners": []}
            errors.append(str(exc))
    fetch_jobs = runs_payload is None
    if runs_payload is None:
        try:
            runs_payload = _github_get(
                f"/repos/{repo}/actions/workflows/{workflow}/runs?status=queued&per_page={MAX_QUEUED_RUNS_TO_INSPECT}",
                token=token,
            )
        except Exception as exc:
            runs_payload = {"workflow_runs": []}
            errors.append(str(exc))

    runners = list(runners_payload.get("runners") or [])
    matching = _matching_runners(runners, required)
    role_matches = _runner_role_matches(runners, required_roles or {})
    queued_runs = list(runs_payload.get("workflow_runs") or runs_payload.get("runs") or [])
    stale_runs = _stale_queued_runs(queued_runs, stale_minutes=stale_queued_minutes, now=current_time)
    matching_stale_runs, other_role_stale_runs = _annotate_stale_runs_with_jobs(
        stale_runs,
        repo=repo,
        required_labels=required,
        token=token,
        jobs_payloads=jobs_payloads,
        fetch_jobs=fetch_jobs,
        errors=errors,
    )
    idle_matching = [runner for runner in matching if not bool(runner.get("busy"))]
    online_but_not_accepting_work = bool(matching_stale_runs and idle_matching)
    blocking: list[str] = []
    warnings: list[str] = []
    if errors:
        blocking.append("unable to query GitHub runner health")
        if any("HTTP 403" in item or "HTTP 401" in item for item in errors):
            warnings.append(
                "runner API requires a token with repository Administration read permission; "
                "configure AISTOCK_RUNNER_HEALTH_TOKEN when GITHUB_TOKEN is insufficient"
            )
    if not matching:
        blocking.append(
            "no online GitHub Actions runner matches required labels: " + ", ".join(required)
        )
    for role, matches in role_matches.items():
        if not matches:
            blocking.append(
                f"no online GitHub Actions runner matches role {role}: "
                + ", ".join((required_roles or {})[role])
            )
    if role_matches and all(role_matches.values()):
        role_runner_ids = {
            int(runner["id"])
            for matches in role_matches.values()
            for runner in matches
            if runner.get("id") is not None
        }
        if len(role_runner_ids) < len(role_matches):
            blocking.append("runner roles do not provide distinct online capacity")
    if stale_runs:
        warnings.append(f"{len(stale_runs)} queued {workflow} run(s) exceed {stale_queued_minutes} minutes")
    if other_role_stale_runs:
        warnings.append(
            f"{len(other_role_stale_runs)} stale queued {workflow} run(s) belong to other runner roles"
        )
    if online_but_not_accepting_work:
        blocking.append(
            "online idle runner matches required labels but queued work exceeds "
            f"{stale_queued_minutes} minutes; inspect a stuck self-update or listener"
        )
    gate = "blocked" if blocking else "ready"
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "repo": repo,
        "workflow": workflow,
        "required_labels": required,
        "workflow_gate": gate,
        "blocking": blocking,
        "warnings": warnings,
        "errors": errors,
        "all_runners_count": runners_payload.get("total_count", len(runners)),
        "all_runners": [_runner_summary(runner) for runner in runners],
        "online_matching_runners": matching,
        "online_but_not_accepting_work": online_but_not_accepting_work,
        "runner_roles": role_matches,
        "stale_queued_runs": stale_runs,
        "matching_stale_queued_runs": matching_stale_runs,
        "other_role_stale_queued_runs": other_role_stale_runs,
        "next_actions": _next_actions(gate, required),
        "production_gates": {
            "production_ddl_gate": "noop",
            "production_frontend_dependency_gate": "noop",
            "production_backend_dependency_gate": "noop",
        },
    }


def _next_actions(gate: str, required_labels: list[str]) -> list[str]:
    if gate == "ready":
        return ["continue AIstock Nightly L3 + DR on the matching self-hosted runner"]
    return [
        "configure AISTOCK_RUNNER_HEALTH_TOKEN with repository Administration read permission if runner API access is denied",
        "inspect runner self-update state and the supervised listener before restarting or re-registering it",
        "start or register the AIstock self-hosted Windows GitHub Actions runner with automatic updates disabled",
        "verify runner labels include: " + ", ".join(required_labels),
        "rerun AIstock Nightly L3 + DR after the runner is online",
    ]


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# AIstock Runner Health",
        "",
        f"- repo: `{report.get('repo')}`",
        f"- workflow: `{report.get('workflow')}`",
        f"- workflow_gate: `{report.get('workflow_gate')}`",
        f"- required_labels: `{', '.join(report.get('required_labels') or [])}`",
        f"- all_runners_count: `{report.get('all_runners_count')}`",
        f"- online_matching_runners: `{len(report.get('online_matching_runners') or [])}`",
        "",
        "## Blocking",
    ]
    blocking = report.get("blocking") or []
    lines.extend([f"- {item}" for item in blocking] or ["- none"])
    lines.extend(["", "## Warnings"])
    warnings = report.get("warnings") or []
    lines.extend([f"- {item}" for item in warnings] or ["- none"])
    lines.extend(["", "## Matching Runners"])
    matches = report.get("online_matching_runners") or []
    if matches:
        for runner in matches:
            lines.append(f"- `{runner.get('name')}` labels={runner.get('labels')} busy={runner.get('busy')}")
    else:
        lines.append("- none")
    lines.extend(["", "## Runner Roles"])
    roles = report.get("runner_roles") or {}
    if roles:
        for role, role_matches in roles.items():
            names = [str(item.get("name")) for item in role_matches]
            lines.append(f"- `{role}`: `{', '.join(names) if names else 'missing'}`")
    else:
        lines.append("- not requested")
    lines.extend(["", "## Stale Queued Runs"])
    stale = report.get("stale_queued_runs") or []
    if stale:
        for run in stale:
            lines.append(
                f"- run `{run.get('run_id')}` status={run.get('status')} age_minutes={run.get('age_minutes')} url={run.get('url')}"
            )
    else:
        lines.append("- none")
    lines.extend(["", "## Next Actions"])
    lines.extend([f"- {item}" for item in report.get("next_actions") or []])
    lines.extend(["", "## Production Gates"])
    for key, value in (report.get("production_gates") or {}).items():
        lines.append(f"- {key}: `{value}`")
    return "\n".join(lines) + "\n"


def _write_outputs(report: dict[str, Any], *, output_json: str | None, output_md: str | None) -> None:
    if output_json:
        path = Path(output_json)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    if output_md:
        path = Path(output_md)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(render_markdown(report), encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Check AIstock GitHub Actions self-hosted runner readiness.")
    sub = parser.add_subparsers(dest="command", required=True)
    doctor = sub.add_parser("doctor", help="Check runner availability for a workflow.")
    doctor.add_argument("--repo", default=DEFAULT_REPO)
    doctor.add_argument("--workflow", default=DEFAULT_WORKFLOW)
    doctor.add_argument("--required-label", action="append", default=[])
    doctor.add_argument(
        "--required-role",
        action="append",
        default=[],
        metavar="ROLE=LABEL,LABEL",
        help="Require one distinct online runner per named role.",
    )
    doctor.add_argument("--stale-queued-minutes", type=int, default=10)
    doctor.add_argument("--runners-json", help="Use a local runners API payload for tests/offline dry-runs.")
    doctor.add_argument("--runs-json", help="Use a local workflow-runs API payload for tests/offline dry-runs.")
    doctor.add_argument(
        "--jobs-json",
        help="Use a run-id to jobs API payload mapping for deterministic role-aware queue checks.",
    )
    doctor.add_argument("--output-json")
    doctor.add_argument("--output-md")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    token, token_source = resolve_github_token()
    required = args.required_label or ["self-hosted", "windows"]
    required_roles: dict[str, list[str]] = {}
    for raw in args.required_role:
        role, separator, labels_text = raw.partition("=")
        labels = [item.strip() for item in labels_text.split(",") if item.strip()]
        if not separator or not role.strip() or not labels:
            raise SystemExit(f"invalid --required-role value: {raw}")
        required_roles[role.strip()] = labels
    runners_payload = _read_json(args.runners_json) if args.runners_json else None
    runs_payload = _read_json(args.runs_json) if args.runs_json else None
    jobs_payloads = _read_json(args.jobs_json) if args.jobs_json else None
    report = build_runner_health_report(
        repo=args.repo,
        workflow=args.workflow,
        required_labels=required,
        required_roles=required_roles,
        stale_queued_minutes=args.stale_queued_minutes,
        runners_payload=runners_payload,
        runs_payload=runs_payload,
        jobs_payloads=jobs_payloads,
        token=token,
    )
    report["token_source"] = token_source
    _write_outputs(report, output_json=args.output_json, output_md=args.output_md)
    print(json.dumps(report, indent=2, ensure_ascii=True))
    if report["workflow_gate"] != "ready" and os.environ.get("GITHUB_ACTIONS"):
        message = "; ".join(report.get("blocking") or ["runner health blocked"])
        print(f"::error::{message}", file=sys.stderr)
    return 0 if report["workflow_gate"] == "ready" else 2


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
