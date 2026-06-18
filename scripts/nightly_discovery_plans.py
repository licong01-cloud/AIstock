from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


ROOT = Path(__file__).resolve().parents[1]
PLAN_RESULT_SCHEMA_VERSION = "aistock_nightly_discovery_plan_result_v1"
SUITE_RESULT_SCHEMA_VERSION = "aistock_nightly_discovery_suite_v1"
DISCOVERY_PLAN_KEYS = (
    "validation_discovery_issue_intake_readonly",
    "workflow_discovery_root_clean_guard",
    "code_intelligence_discovery_affected_tests_quality",
    "validation_center_discovery_run_record_integrity",
)
PRODUCTION_GATES = {
    "production_ddl_gate": "noop",
    "production_frontend_dependency_gate": "noop",
    "production_backend_dependency_gate": "noop",
}
READONLY_SIDE_EFFECTS = {
    "readonly": True,
    "writes_database": False,
    "writes_business_state": False,
    "production_actions_allowed": False,
    "shell_commands_allowed": False,
}
ALLOWED_ROOT_DIRTY_PREFIXES = ("tmp/validation/",)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def repo_path(path: Path, *, root: Path = ROOT) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def read_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def write_json(path: Path | None, payload: dict[str, Any]) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    payload["public_artifact"] = True
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def stable_id(*parts: str) -> str:
    digest = hashlib.sha256("::".join(parts).encode("utf-8", errors="replace")).hexdigest()
    return digest[:12]


def anomaly_schema(plan_key: str) -> dict[str, Any]:
    return {
        "schema_version": "aistock_nightly_discovery_anomaly_v1",
        "plan_key": plan_key,
        "required_fields": [
            "anomaly_id",
            "type",
            "severity",
            "title",
            "evidence_refs",
            "dedupe_key",
            "suggested_module",
            "candidate",
            "next_action",
        ],
        "candidate_semantics": "candidate=true means Phase 4 may normalize it into a draft; this plan never creates issues.",
    }


def make_anomaly(
    *,
    plan_key: str,
    anomaly_type: str,
    severity: str,
    title: str,
    evidence_refs: list[str],
    suggested_module: str = "validation.runner",
    candidate: bool = True,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    dedupe_key = f"{plan_key}:{anomaly_type}:{'|'.join(evidence_refs)}"
    return {
        "anomaly_id": f"AD-{stable_id(dedupe_key)}",
        "type": anomaly_type,
        "severity": severity,
        "title": title[:240],
        "evidence_refs": evidence_refs[:8],
        "dedupe_key": stable_id(dedupe_key),
        "suggested_module": suggested_module,
        "candidate": candidate,
        "next_action": "phase4_candidate_quality_gate",
        "details": details or {},
    }


def base_result(plan_key: str, *, root: Path) -> dict[str, Any]:
    return {
        "schema_version": PLAN_RESULT_SCHEMA_VERSION,
        "plan_key": plan_key,
        "generated_at": utc_now(),
        "root": str(root),
        "status": "completed",
        "readonly": True,
        "anomaly_schema": anomaly_schema(plan_key),
        "anomalies": [],
        "summary": {
            "inspected_count": 0,
            "anomaly_count": 0,
            "candidate_count": 0,
            "no_candidate_reason": None,
        },
        "side_effects": READONLY_SIDE_EFFECTS,
        "production_gates": PRODUCTION_GATES,
    }


def finalize_result(result: dict[str, Any]) -> dict[str, Any]:
    anomalies = result.get("anomalies") if isinstance(result.get("anomalies"), list) else []
    summary = result.setdefault("summary", {})
    summary["anomaly_count"] = len(anomalies)
    summary["candidate_count"] = len([item for item in anomalies if isinstance(item, dict) and item.get("candidate")])
    if not anomalies:
        summary["no_candidate_reason"] = summary.get("no_candidate_reason") or "no_anomalies_detected"
    return result


def discover_issue_intake_readonly(root: Path, *, limit: int = 120, **_: Any) -> dict[str, Any]:
    plan_key = "validation_discovery_issue_intake_readonly"
    result = base_result(plan_key, root=root)
    bug_dir = root / "tests" / "aistock_validation" / "bugs"
    bug_files = sorted(
        [path for path in bug_dir.glob("*.json") if not path.name.startswith(".")],
        key=lambda path: path.name,
        reverse=True,
    )[:limit]
    result["summary"]["inspected_count"] = len(bug_files)
    open_statuses = {"open", "in_progress", "fixed"}
    for path in bug_files:
        payload = read_json(path)
        if payload.get("schema_version") != "aistock_validation_bug_v1":
            continue
        bug_id = str(payload.get("bug_id") or path.stem)
        status = str(payload.get("status") or "").strip().lower()
        if status not in open_statuses:
            continue
        rel = repo_path(path, root=root)
        if not payload.get("github_issue_number") or not payload.get("github_issue_url"):
            result["anomalies"].append(
                make_anomaly(
                    plan_key=plan_key,
                    anomaly_type="bug_missing_github_linkage",
                    severity="P1",
                    title=f"{bug_id} lacks GitHub Issue linkage",
                    evidence_refs=[rel],
                    details={"bug_id": bug_id, "status": status},
                )
            )
        description = str(payload.get("description") or "").strip()
        if len(description) < 40:
            result["anomalies"].append(
                make_anomaly(
                    plan_key=plan_key,
                    anomaly_type="bug_description_too_short",
                    severity="P2",
                    title=f"{bug_id} issue intake description is too short",
                    evidence_refs=[rel],
                    details={"bug_id": bug_id, "description_length": len(description)},
                )
            )
        if status in {"open", "in_progress"} and not payload.get("required_verification"):
            result["anomalies"].append(
                make_anomaly(
                    plan_key=plan_key,
                    anomaly_type="bug_missing_required_verification",
                    severity="P2",
                    title=f"{bug_id} lacks required verification",
                    evidence_refs=[rel],
                    details={"bug_id": bug_id, "status": status},
                )
            )
        if status == "fixed" and (not payload.get("fix_commit") or not payload.get("pr_url")):
            result["anomalies"].append(
                make_anomaly(
                    plan_key=plan_key,
                    anomaly_type="fixed_bug_missing_pr_or_commit",
                    severity="P1",
                    title=f"{bug_id} is fixed but lacks PR or commit evidence",
                    evidence_refs=[rel],
                    details={"bug_id": bug_id, "status": status},
                )
            )
    return finalize_result(result)


def _git_status_lines(root: Path) -> list[str]:
    proc = subprocess.run(
        ["git", "-C", str(root), "status", "--porcelain=v1", "--untracked-files=all"],
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        return []
    return [line.rstrip() for line in proc.stdout.splitlines() if line.strip()]


def _status_path(line: str) -> str:
    path = line[3:].strip() if len(line) > 3 else line.strip()
    if " -> " in path:
        path = path.split(" -> ", 1)[1].strip()
    return path.replace("\\", "/").strip('"')


def discover_root_clean_guard(root: Path, **_: Any) -> dict[str, Any]:
    plan_key = "workflow_discovery_root_clean_guard"
    result = base_result(plan_key, root=root)
    status_lines = _git_status_lines(root)
    unexpected = [
        line for line in status_lines if not _status_path(line).startswith(ALLOWED_ROOT_DIRTY_PREFIXES)
    ]
    result["summary"]["inspected_count"] = len(status_lines)
    for line in unexpected[:40]:
        path = _status_path(line)
        status_code = line[:2].strip() or "unknown"
        result["anomalies"].append(
            make_anomaly(
                plan_key=plan_key,
                anomaly_type="unexpected_root_dirty_path",
                severity="P1" if status_code != "??" else "P2",
                title=f"Unexpected dirty path in nightly workspace: {path}",
                evidence_refs=[path],
                details={"git_status": status_code, "status_line": line},
            )
        )
    return finalize_result(result)


def _affected_tests_from_payload(payload: dict[str, Any]) -> list[str]:
    direct = payload.get("affected_tests")
    if isinstance(direct, list):
        return [str(item) for item in direct if str(item).strip()]
    fallback = payload.get("test_discovery_fallback")
    if isinstance(fallback, dict):
        matched = fallback.get("matched_tests")
        if isinstance(matched, dict):
            tests: list[str] = []
            for values in matched.values():
                if isinstance(values, list):
                    tests.extend(str(item) for item in values if str(item).strip())
            return sorted(set(tests))
        if isinstance(matched, list):
            return [str(item) for item in matched if str(item).strip()]
    return []


def discover_affected_tests_quality(
    root: Path,
    *,
    code_intelligence_json: Path | None = None,
    **_: Any,
) -> dict[str, Any]:
    plan_key = "code_intelligence_discovery_affected_tests_quality"
    result = base_result(plan_key, root=root)
    payload = read_json(code_intelligence_json)
    if not payload:
        result["anomalies"].append(
            make_anomaly(
                plan_key=plan_key,
                anomaly_type="code_intelligence_artifact_missing",
                severity="P2",
                title="Code intelligence summary artifact is missing or unreadable",
                evidence_refs=[str(code_intelligence_json or "missing")],
                details={"artifact": str(code_intelligence_json) if code_intelligence_json else None},
            )
        )
        return finalize_result(result)

    changed = payload.get("changed_files")
    if not isinstance(changed, list):
        input_refs = payload.get("input_refs") if isinstance(payload.get("input_refs"), dict) else {}
        changed = input_refs.get("changed_files") if isinstance(input_refs.get("changed_files"), list) else []
    affected_tests = _affected_tests_from_payload(payload)
    result["summary"]["inspected_count"] = len(changed) + len(affected_tests)
    if changed and not affected_tests:
        result["anomalies"].append(
            make_anomaly(
                plan_key=plan_key,
                anomaly_type="changed_files_without_affected_tests",
                severity="P1",
                title="Changed files have no affected-test mapping",
                evidence_refs=[str(code_intelligence_json)],
                details={"changed_files_count": len(changed)},
            )
        )
    if len(affected_tests) > 60:
        result["anomalies"].append(
            make_anomaly(
                plan_key=plan_key,
                anomaly_type="affected_tests_too_broad",
                severity="P2",
                title="Affected-test mapping is too broad for nightly discovery",
                evidence_refs=[str(code_intelligence_json)],
                details={"affected_tests_count": len(affected_tests), "threshold": 60},
            )
        )
    refs = payload.get("refs") if isinstance(payload.get("refs"), dict) else {}
    codegraph_ref = refs.get("codegraph_context") or refs.get("codegraph_context_md")
    if not codegraph_ref and not payload.get("codegraph"):
        result["anomalies"].append(
            make_anomaly(
                plan_key=plan_key,
                anomaly_type="codegraph_ref_missing",
                severity="P2",
                title="Code intelligence artifact lacks CodeGraph reference",
                evidence_refs=[str(code_intelligence_json)],
                details={"expected_ref": "codegraph_context"},
            )
        )
    return finalize_result(result)


def discover_run_record_integrity(root: Path, *, history_limit: int = 40, **_: Any) -> dict[str, Any]:
    plan_key = "validation_center_discovery_run_record_integrity"
    result = base_result(plan_key, root=root)
    history_root = root / "tests" / "aistock_validation" / "history"
    records = sorted(history_root.rglob("*.md"), key=lambda path: path.stat().st_mtime, reverse=True)[:history_limit]
    result["summary"]["inspected_count"] = len(records)
    for path in records:
        rel = repo_path(path, root=root)
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            text = ""
        if not text.strip():
            result["anomalies"].append(
                make_anomaly(
                    plan_key=plan_key,
                    anomaly_type="run_record_empty",
                    severity="P2",
                    title=f"Validation history record is empty: {rel}",
                    evidence_refs=[rel],
                    suggested_module="tests.validation_history",
                )
            )
            continue
        missing_gates = [
            gate for gate in PRODUCTION_GATES if gate not in text
        ]
        if missing_gates:
            result["anomalies"].append(
                make_anomaly(
                    plan_key=plan_key,
                    anomaly_type="run_record_missing_production_gates",
                    severity="P2",
                    title=f"Validation history record lacks production gates: {rel}",
                    evidence_refs=[rel],
                    suggested_module="tests.validation_history",
                    details={"missing_gates": missing_gates},
                )
            )
        if "passed" not in text.lower() and "failed" not in text.lower() and "status" not in text.lower():
            result["anomalies"].append(
                make_anomaly(
                    plan_key=plan_key,
                    anomaly_type="run_record_missing_result_status",
                    severity="P3",
                    title=f"Validation history record lacks an explicit result status: {rel}",
                    evidence_refs=[rel],
                    suggested_module="tests.validation_history",
                )
            )
    return finalize_result(result)


PLAN_RUNNERS: dict[str, Callable[..., dict[str, Any]]] = {
    "validation_discovery_issue_intake_readonly": discover_issue_intake_readonly,
    "workflow_discovery_root_clean_guard": discover_root_clean_guard,
    "code_intelligence_discovery_affected_tests_quality": discover_affected_tests_quality,
    "validation_center_discovery_run_record_integrity": discover_run_record_integrity,
}


def run_plan(
    plan_key: str,
    *,
    root: Path = ROOT,
    output: Path | None = None,
    code_intelligence_json: Path | None = None,
    history_limit: int = 40,
) -> dict[str, Any]:
    if plan_key not in PLAN_RUNNERS:
        raise ValueError(f"unknown discovery plan: {plan_key}")
    result = PLAN_RUNNERS[plan_key](
        root=root,
        code_intelligence_json=code_intelligence_json,
        history_limit=history_limit,
    )
    write_json(output, result)
    return result


def selected_plan_keys(path: Path | None) -> list[str]:
    payload = read_json(path)
    keys = payload.get("selected_plan_keys")
    if isinstance(keys, list):
        return [str(item) for item in keys if str(item).strip()]
    selected = payload.get("selected_plans")
    if isinstance(selected, list):
        return [
            str(item.get("plan_key"))
            for item in selected
            if isinstance(item, dict) and str(item.get("plan_key") or "").strip()
        ]
    return []


def run_selected(
    *,
    selected_plans: Path | None,
    output_dir: Path,
    root: Path = ROOT,
    default_plan_keys: list[str] | None = None,
    code_intelligence_json: Path | None = None,
    history_limit: int = 40,
) -> dict[str, Any]:
    selected_keys = selected_plan_keys(selected_plans)
    discovery_keys = [key for key in selected_keys if key in DISCOVERY_PLAN_KEYS]
    if not discovery_keys:
        discovery_keys = [key for key in default_plan_keys or [] if key in DISCOVERY_PLAN_KEYS]
    discovery_keys = list(dict.fromkeys(discovery_keys))
    output_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, Any]] = []
    for plan_key in discovery_keys:
        result = run_plan(
            plan_key,
            root=root,
            output=output_dir / f"{plan_key}.json",
            code_intelligence_json=code_intelligence_json,
            history_limit=history_limit,
        )
        results.append(
            {
                "plan_key": plan_key,
                "status": result.get("status"),
                "anomaly_count": result.get("summary", {}).get("anomaly_count", 0),
                "artifact": (output_dir / f"{plan_key}.json").as_posix(),
            }
        )
    skipped = [key for key in selected_keys if key not in DISCOVERY_PLAN_KEYS]
    manifest = {
        "schema_version": SUITE_RESULT_SCHEMA_VERSION,
        "generated_at": utc_now(),
        "selected_plan_keys": selected_keys,
        "executed_plan_keys": discovery_keys,
        "skipped_plan_keys": skipped,
        "results": results,
        "summary": {
            "executed_count": len(results),
            "anomaly_count": sum(int(item.get("anomaly_count") or 0) for item in results),
            "readonly": True,
            "no_candidate_reason": "no_discovery_plans_selected" if not results else None,
        },
        "side_effects": READONLY_SIDE_EFFECTS,
        "production_gates": PRODUCTION_GATES,
    }
    write_json(output_dir / "manifest.json", manifest)
    return manifest


def _compact_result(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": result.get("schema_version"),
        "plan_key": result.get("plan_key"),
        "status": result.get("status"),
        "anomaly_count": result.get("summary", {}).get("anomaly_count", 0),
        "candidate_count": result.get("summary", {}).get("candidate_count", 0),
        "readonly": True,
    }


def _print_success(check: str, payload: dict[str, Any], *, as_json: bool) -> None:
    compact = {"check": check, **payload}
    if as_json:
        print(json.dumps(compact, ensure_ascii=False, sort_keys=True))
        return
    parts = [f"PASS {check}"]
    for key, value in compact.items():
        if key == "check":
            continue
        parts.append(f"{key}={value}")
    print(" ".join(parts))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run AIstock Nightly readonly active-discovery plans.")
    parser.add_argument("--json", action="store_true", default=False, help="Emit compact JSON stdout.")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run")
    run.add_argument("--plan-key", required=True, choices=DISCOVERY_PLAN_KEYS)
    run.add_argument("--root", default=str(ROOT))
    run.add_argument("--output")
    run.add_argument("--code-intelligence-json")
    run.add_argument("--history-limit", type=int, default=40)

    selected = sub.add_parser("run-selected")
    selected.add_argument("--selected-plans")
    selected.add_argument("--output-dir", required=True)
    selected.add_argument("--root", default=str(ROOT))
    selected.add_argument("--default-plan-key", action="append", default=[])
    selected.add_argument("--code-intelligence-json")
    selected.add_argument("--history-limit", type=int, default=40)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    root = Path(args.root).resolve()
    if args.command == "run":
        output = Path(args.output) if args.output else None
        result = run_plan(
            args.plan_key,
            root=root,
            output=output,
            code_intelligence_json=Path(args.code_intelligence_json) if args.code_intelligence_json else None,
            history_limit=args.history_limit,
        )
        compact = _compact_result(result)
        if output:
            compact["artifact"] = str(output)
        _print_success("nightly-discovery-plan", compact, as_json=args.json)
        return 0
    if args.command == "run-selected":
        output_dir = Path(args.output_dir)
        manifest = run_selected(
            selected_plans=Path(args.selected_plans) if args.selected_plans else None,
            output_dir=output_dir,
            root=root,
            default_plan_keys=args.default_plan_key,
            code_intelligence_json=Path(args.code_intelligence_json) if args.code_intelligence_json else None,
            history_limit=args.history_limit,
        )
        _print_success(
            "nightly-discovery-selected",
            {
                "schema_version": manifest["schema_version"],
                "executed_count": manifest["summary"]["executed_count"],
                "anomaly_count": manifest["summary"]["anomaly_count"],
                "artifact": str(output_dir / "manifest.json"),
            },
            as_json=args.json,
        )
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

