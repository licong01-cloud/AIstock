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
from typing import Any

DEFAULT_REPO = "licong01-cloud/AIstock"
DEFAULT_WORKFLOW = "nightly.yml"
SCHEMA_VERSION = "aistock_runner_health_v1"
GITHUB_API = "https://api.github.com"


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
    except Exception:
        return None, "missing"
    token = proc.stdout.strip()
    if proc.returncode == 0 and token:
        return token, "gh_auth_token"
    return None, "missing"


def _github_get(path: str, *, token: str | None) -> Any:
    request = urllib.request.Request(f"{GITHUB_API}{path}")
    request.add_header("Accept", "application/vnd.github+json")
    request.add_header("X-GitHub-Api-Version", "2022-11-28")
    request.add_header("User-Agent", "AIstock-runner-health")
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"GitHub API {path} failed: HTTP {exc.code}: {body}") from exc
    except Exception as exc:  # pragma: no cover - network failures vary by host
        raise RuntimeError(f"GitHub API {path} failed: {exc}") from exc


def _label_names(runner: dict[str, Any]) -> set[str]:
    return {str(item.get("name") or "").lower() for item in runner.get("labels") or []}


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


def build_runner_health_report(
    *,
    repo: str = DEFAULT_REPO,
    workflow: str = DEFAULT_WORKFLOW,
    required_labels: list[str] | None = None,
    stale_queued_minutes: int = 30,
    runners_payload: dict[str, Any] | None = None,
    runs_payload: dict[str, Any] | None = None,
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
    if runs_payload is None:
        try:
            runs_payload = _github_get(
                f"/repos/{repo}/actions/workflows/{workflow}/runs?status=queued&per_page=20",
                token=token,
            )
        except Exception as exc:
            runs_payload = {"workflow_runs": []}
            errors.append(str(exc))

    runners = list(runners_payload.get("runners") or [])
    matching = _matching_runners(runners, required)
    queued_runs = list(runs_payload.get("workflow_runs") or runs_payload.get("runs") or [])
    stale_runs = _stale_queued_runs(queued_runs, stale_minutes=stale_queued_minutes, now=current_time)
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
    if stale_runs:
        warnings.append(f"{len(stale_runs)} queued {workflow} run(s) exceed {stale_queued_minutes} minutes")
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
        "stale_queued_runs": stale_runs,
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
        "start or register the AIstock self-hosted Windows GitHub Actions runner",
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
    doctor.add_argument("--stale-queued-minutes", type=int, default=30)
    doctor.add_argument("--runners-json", help="Use a local runners API payload for tests/offline dry-runs.")
    doctor.add_argument("--runs-json", help="Use a local workflow-runs API payload for tests/offline dry-runs.")
    doctor.add_argument("--output-json")
    doctor.add_argument("--output-md")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    token, token_source = resolve_github_token()
    required = args.required_label or ["self-hosted", "windows"]
    runners_payload = _read_json(args.runners_json) if args.runners_json else None
    runs_payload = _read_json(args.runs_json) if args.runs_json else None
    report = build_runner_health_report(
        repo=args.repo,
        workflow=args.workflow,
        required_labels=required,
        stale_queued_minutes=args.stale_queued_minutes,
        runners_payload=runners_payload,
        runs_payload=runs_payload,
        token=token,
    )
    report["token_source"] = token_source
    _write_outputs(report, output_json=args.output_json, output_md=args.output_md)
    print(json.dumps(report, indent=2, ensure_ascii=True))
    if report["workflow_gate"] != "ready" and os.environ.get("GITHUB_ACTIONS"):
        message = "; ".join(report.get("blocking") or ["runner health blocked"])
        print(f"::error::{message}")
    return 0 if report["workflow_gate"] == "ready" else 2


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
