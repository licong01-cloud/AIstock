from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import llm_provider_adapter  # noqa: E402

REPORT_SCHEMA_VERSION = "aistock_nightly_adaptive_scheduler_report_v1"
FAILURE_STATUSES = {"failure", "cancelled", "timed_out", "timed-out", "startup_failure", "action_required"}
STATUS_KEY_ALIASES = {
    "runner_preflight": {"runnerPreflight", "runner-preflight", "runner_preflight"},
    "dr_snapshot": {"drSnapshot", "dr-snapshot", "dr_snapshot"},
    "dr_validate": {"drValidate", "dr-validate", "dr_validate"},
    "nightly_l3": {"nightlyL3", "nightly-l3", "nightly_l3"},
    "paper_v2_live": {"paperV2Live", "paper-v2-live", "paper_v2_live"},
    "code_intelligence": {"codeIntelligence", "code-intelligence", "code_intelligence", "code-intelligence-weekly"},
}
STATUS_FAILURE_MODULES = {
    "runner_preflight": ["validation.runner"],
    "dr_snapshot": ["validation.dr"],
    "dr_validate": ["validation.dr"],
    "nightly_l3": ["paper_v2_l3", "qe_archive_l3", "qe_read_l3"],
    "paper_v2_live": ["paper_v2_live"],
    "code_intelligence": ["validation.runner"],
}


def _split_csv(values: list[str] | None) -> list[str]:
    result: list[str] = []
    for value in values or []:
        result.extend(item.strip() for item in str(value).split(",") if item.strip())
    return result


def _normalize_path(value: str) -> str:
    text = str(value or "").strip().replace("\\", "/")
    while text.startswith("./"):
        text = text[2:]
    return text


def _unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        item = str(value or "").strip()
        if item and item not in seen:
            seen.add(item)
            result.append(item)
    return result


def _read_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _git_changed_files(base_ref: str | None, *, root: Path) -> list[str]:
    if not base_ref:
        return []
    proc = subprocess.run(
        ["git", "diff", "--name-only", str(base_ref), "HEAD"],
        cwd=str(root),
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        return []
    return [_normalize_path(line) for line in proc.stdout.splitlines() if _normalize_path(line)]


def collect_changed_files(
    *,
    changed_files: list[str] | None,
    changed_files_file: Path | None,
    base_ref: str | None,
    root: Path,
) -> list[str]:
    collected = [_normalize_path(item) for item in _split_csv(changed_files)]
    if changed_files_file and changed_files_file.exists():
        collected.extend(_normalize_path(line) for line in changed_files_file.read_text(encoding="utf-8").splitlines())
    collected.extend(_git_changed_files(base_ref, root=root))
    return _unique([item for item in collected if item])


def _canonical_status_key(raw_key: str) -> str | None:
    for canonical, aliases in STATUS_KEY_ALIASES.items():
        if raw_key in aliases:
            return canonical
    return None


def collect_statuses(*, status_json: Path | None, inline_statuses: list[str] | None) -> dict[str, str]:
    payload = _read_json(status_json)
    source = payload.get("statuses") if isinstance(payload.get("statuses"), dict) else payload
    statuses: dict[str, str] = {}
    if isinstance(source, dict):
        for raw_key, raw_value in source.items():
            canonical = _canonical_status_key(str(raw_key))
            if canonical:
                statuses[canonical] = str(raw_value or "unknown").strip().lower() or "unknown"
    for item in inline_statuses or []:
        if "=" not in item:
            continue
        raw_key, raw_value = item.split("=", 1)
        canonical = _canonical_status_key(raw_key.strip())
        if canonical:
            statuses[canonical] = raw_value.strip().lower() or "unknown"
    return statuses


def recent_failure_modules_from_statuses(statuses: dict[str, str]) -> list[str]:
    modules: list[str] = []
    for key, value in statuses.items():
        if value.strip().lower() in FAILURE_STATUSES:
            modules.extend(STATUS_FAILURE_MODULES.get(key, []))
    return _unique(modules)


def normalize_codegraph_freshness(value: str | None) -> str:
    raw = str(value or "unknown").strip().lower()
    if raw == "fresh":
        return "fresh"
    if raw in {"missing", "missing_index"}:
        return "missing"
    if raw in {"stale", "unavailable", "status_check_failed", "unverified", "incomplete_index"}:
        return "stale"
    return "unknown"


def codegraph_freshness_from_artifact(path: Path | None, explicit: str | None) -> dict[str, Any]:
    payload = _read_json(path)
    raw = explicit or payload.get("freshness") or "missing"
    freshness = normalize_codegraph_freshness(str(raw))
    return {
        "freshness": freshness,
        "source": "explicit" if explicit else ("artifact" if payload else "missing_artifact"),
        "artifact_path": str(path) if path else None,
        "raw_freshness": raw,
    }


def build_report(
    *,
    provider: str,
    config_path: Path,
    changed_files: list[str],
    statuses: dict[str, str],
    codegraph: dict[str, Any],
    resource_budget_seconds: int,
    workspace_path: str | None = None,
    invoke_llm: bool = False,
    fallback_on_llm_error: bool = True,
) -> dict[str, Any]:
    config = llm_provider_adapter.load_config(config_path)
    recent_failure_modules = recent_failure_modules_from_statuses(statuses)
    advice = llm_provider_adapter.build_nightly_scheduler_advice(
        provider,
        config,
        changed_files=changed_files,
        recent_failure_modules=recent_failure_modules,
        codegraph_freshness=str(codegraph["freshness"]),
        resource_budget_seconds=resource_budget_seconds,
        workspace_path=workspace_path,
        invoke_llm=invoke_llm,
        fallback_on_llm_error=fallback_on_llm_error,
    )
    gate = advice["deterministic_gate"]
    allowed = [item for item in advice["queue"] if item.get("allowed")]
    deferred = [item for item in advice["queue"] if not item.get("allowed")]
    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "workflow_gate": gate["workflow_gate"],
        "provider": advice["provider"],
        "model": advice["model"],
        "execution_mode": "warning_only_advice",
        "execute": False,
        "input_refs": {
            "changed_files": changed_files,
            "nightly_statuses": statuses,
            "recent_failure_modules": recent_failure_modules,
            "codegraph": codegraph,
        },
        "queue_summary": {
            "queue_count": len(advice["queue"]),
            "allowed_plan_keys": [str(item["plan_key"]) for item in allowed],
            "deferred_plan_keys": [str(item["plan_key"]) for item in deferred],
            "deferred_reasons": {
                str(item["plan_key"]): item.get("deferred_reason")
                for item in deferred
                if item.get("deferred_reason")
            },
            "resource_budget_seconds": advice["resource_budget_seconds"],
        },
        "queue": advice["queue"],
        "llm_invoked": bool((advice.get("llm_advice") or {}).get("advisory_only")),
        "issue_creation_policy": {
            "allowed": False,
            "reason": "adaptive_scheduler_warning_mode_never_creates_issue",
        },
        "test_plan_advice_gate": advice["test_plan_advice_gate"],
        "workspace_gate": advice["workspace_gate"],
        "production_gates": gate["production_gates"],
        "shell_commands_allowed": False,
        "production_actions_allowed": False,
    }


def render_markdown(report: dict[str, Any]) -> str:
    queue = report["queue_summary"]
    codegraph = report["input_refs"]["codegraph"]
    lines = [
        "# Nightly Adaptive Scheduler",
        "",
        f"- workflow_gate: `{report['workflow_gate']}`",
        f"- execution_mode: `{report['execution_mode']}`",
        f"- provider: `{report['provider']}`",
        f"- model: `{report['model']}`",
        f"- codegraph_freshness: `{codegraph.get('freshness')}`",
        f"- queue_count: `{queue['queue_count']}`",
        f"- allowed_plan_keys: `{', '.join(queue['allowed_plan_keys']) or 'none'}`",
        f"- deferred_plan_keys: `{', '.join(queue['deferred_plan_keys']) or 'none'}`",
        f"- resource_budget_seconds: `{queue['resource_budget_seconds']}`",
        "- issue_creation: `disabled_warning_mode`",
        "",
        "This warning-only job emits plan keys and gate evidence only. It does not create GitHub Issues, run shell commands, touch production services, or write BUG JSON.",
        "",
    ]
    return "\n".join(lines)


def public_scheduler_report(report: dict[str, Any]) -> dict[str, Any]:
    queue = report.get("queue_summary") if isinstance(report.get("queue_summary"), dict) else {}
    input_refs = report.get("input_refs") if isinstance(report.get("input_refs"), dict) else {}
    return {
        "schema_version": report.get("schema_version"),
        "workflow_gate": report.get("workflow_gate"),
        "execution_mode": report.get("execution_mode"),
        "provider": report.get("provider"),
        "model": report.get("model"),
        "input_refs": {
            "changed_files": input_refs.get("changed_files") or [],
            "statuses": input_refs.get("statuses") or {},
            "codegraph": input_refs.get("codegraph") or {},
        },
        "queue_summary": {
            "queue_count": queue.get("queue_count", 0),
            "allowed_plan_keys": queue.get("allowed_plan_keys") or [],
            "deferred_plan_keys": queue.get("deferred_plan_keys") or [],
            "deferred_reasons": queue.get("deferred_reasons") or {},
            "resource_budget_seconds": queue.get("resource_budget_seconds"),
        },
        "queue": report.get("queue") or [],
        "llm_invoked": bool(report.get("llm_invoked")),
        "issue_creation_policy": report.get("issue_creation_policy") or {},
        "test_plan_advice_gate": report.get("test_plan_advice_gate") or {},
        "workspace_gate": report.get("workspace_gate") or {},
        "production_gates": report.get("production_gates") or {},
        "shell_commands_allowed": False,
        "production_actions_allowed": False,
        "error": report.get("error"),
    }


def _write_json(path: Path | None, public_report: dict[str, Any]) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(public_report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    path.write_text(serialized, encoding="utf-8")


def _write_text(path: Path | None, public_markdown: str) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(public_markdown, encoding="utf-8")


def _print_compact(report: dict[str, Any], *, as_json: bool, output: Path | None) -> None:
    queue = report["queue_summary"]
    compact = {
        "check": "nightly-adaptive-scheduler",
        "workflow_gate": report["workflow_gate"],
        "execution_mode": report["execution_mode"],
        "queue_count": queue["queue_count"],
        "allowed_plan_count": len(queue["allowed_plan_keys"]),
        "deferred_plan_count": len(queue["deferred_plan_keys"]),
        "codegraph_freshness": report["input_refs"]["codegraph"]["freshness"],
        "llm_invoked": bool(report.get("llm_invoked")),
        "artifact": str(output) if output else None,
    }
    if as_json:
        print(json.dumps(compact, ensure_ascii=False, sort_keys=True))
        return
    print(
        "nightly-adaptive-scheduler: "
        f"workflow_gate={compact['workflow_gate']} "
        f"execution_mode={compact['execution_mode']} "
        f"queue_count={compact['queue_count']} "
        f"allowed={compact['allowed_plan_count']} "
        f"deferred={compact['deferred_plan_count']} "
        f"codegraph={compact['codegraph_freshness']} "
        f"artifact={compact['artifact'] or 'none'}"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build AIstock warning-only nightly adaptive scheduler advice.")
    parser.add_argument("--provider", choices=["deterministic", "github_models", "deepseek_api"], default=None)
    parser.add_argument("--config", default=str(llm_provider_adapter.DEFAULT_CONFIG_PATH))
    parser.add_argument("--changed-file", action="append", default=None)
    parser.add_argument("--changed-files-file")
    parser.add_argument("--base-ref")
    parser.add_argument("--status-json")
    parser.add_argument("--status", action="append", default=None, help="Nightly status as key=value; may be repeated.")
    parser.add_argument("--codegraph-freshness-json")
    parser.add_argument("--codegraph-freshness")
    parser.add_argument("--resource-budget-seconds", type=int, default=900)
    parser.add_argument("--workspace-path")
    parser.add_argument(
        "--invoke-llm",
        action="store_true",
        help="Call the configured provider for advisory JSON. Deterministic queue gates remain authoritative.",
    )
    parser.add_argument(
        "--fail-on-llm-error",
        action="store_true",
        help="Fail instead of falling back to deterministic advice if live LLM invocation errors.",
    )
    parser.add_argument("--output")
    parser.add_argument("--markdown-output")
    parser.add_argument("--json", action="store_true", help="Emit compact JSON stdout.")
    parser.add_argument("--fail-on-blocked", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    output = Path(args.output) if args.output else None
    markdown_output = Path(args.markdown_output) if args.markdown_output else None
    provider = args.provider or str(llm_provider_adapter.load_config(Path(args.config)).get("default_provider") or "deterministic")
    try:
        changed_files = collect_changed_files(
            changed_files=args.changed_file,
            changed_files_file=Path(args.changed_files_file) if args.changed_files_file else None,
            base_ref=args.base_ref,
            root=ROOT,
        )
        statuses = collect_statuses(
            status_json=Path(args.status_json) if args.status_json else None,
            inline_statuses=args.status,
        )
        codegraph = codegraph_freshness_from_artifact(
            Path(args.codegraph_freshness_json) if args.codegraph_freshness_json else None,
            args.codegraph_freshness,
        )
        report = build_report(
            provider=provider,
            config_path=Path(args.config),
            changed_files=changed_files,
            statuses=statuses,
            codegraph=codegraph,
            resource_budget_seconds=args.resource_budget_seconds,
            workspace_path=args.workspace_path,
            invoke_llm=args.invoke_llm,
            fallback_on_llm_error=not args.fail_on_llm_error,
        )
    except Exception as exc:
        report = {
            "schema_version": REPORT_SCHEMA_VERSION,
            "workflow_gate": "blocked",
            "execution_mode": "warning_only_advice",
            "error": llm_provider_adapter.redact_secret_text(str(exc)) if hasattr(llm_provider_adapter, "redact_secret_text") else str(exc),
            "queue_summary": {
                "queue_count": 0,
                "allowed_plan_keys": [],
                "deferred_plan_keys": [],
                "deferred_reasons": {},
                "resource_budget_seconds": args.resource_budget_seconds,
            },
            "input_refs": {"codegraph": {"freshness": "unknown"}},
            "llm_invoked": False,
            "production_gates": {
                "production_ddl_gate": "noop",
                "production_frontend_dependency_gate": "noop",
                "production_backend_dependency_gate": "noop",
            },
        }
    _write_json(output, public_scheduler_report(report))
    _write_text(markdown_output, render_markdown(report))
    _print_compact(report, as_json=args.json, output=output)
    return 2 if args.fail_on_blocked and report.get("workflow_gate") == "blocked" else 0


if __name__ == "__main__":
    raise SystemExit(main())
