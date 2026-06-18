from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "aistock_discovery_input_pack_v1"
ROTATION_SCHEMA_VERSION = "aistock_nightly_discovery_rotation_v1"
DISCOVERY_STATS_SCHEMA_VERSION = "aistock_nightly_discovery_statistics_v1"
DIFF_HEADER_PREFIXES = ("--- ", "+++ ", "@@", "diff --git ", "index ")
BOM_MOJIBAKE_PREFIX_REPLACEMENTS = {
    "\u9518\u7e1c": "a",
    "\u9518\u7e1d": "b",
    "\u9518\u7e1e": "c",
    "\u9518\u7e1f": "d",
    "\u9518\u7e20": "e",
    "\u9518\u7e21": "f",
    "\u9518\u7e22": "g",
    "\u9518\u7e23": "h",
    "\u9518\u7e24": "i",
    "\u9518\u7e25": "j",
    "\u9518\u7e26": "k",
    "\u9518\u7e27": "l",
    "\u9518\u7e28": "m",
    "\u9518\u7e29": "n",
    "\u9518\u7e2a": "o",
    "\u9518\u7e2b": "p",
    "\u9518\u7e2c": "q",
    "\u9518\u7e2d": "r",
    "\u9518\u7e2e": "s",
    "\u9518\u7e2f": "t",
    "\u9518\u7e30": "u",
    "\u9518\u7e31": "v",
    "\u9518\u7e32": "w",
    "\u9518\u7e33": "x",
    "\u9518\u7e34": "y",
    "\u9518\u7e35": "z",
    "\u9518\ufffd": "",
}
NOISE_TOKENS = {
    "changes",
    "changes:",
    "files",
    "files:",
    "changed files",
    "changed-files",
    "--- changes ---",
    "no changes",
}
READONLY_DISCOVERY_PLAN_KEYS = (
    "validation_discovery_issue_intake_readonly",
    "workflow_discovery_root_clean_guard",
    "code_intelligence_discovery_affected_tests_quality",
    "validation_center_discovery_run_record_integrity",
    "validation_semantic_drift_discovery_readonly",
)
BASELINE_DISCOVERY_PLAN_KEYS = (
    "workflow_discovery_root_clean_guard",
    "validation_discovery_issue_intake_readonly",
)
WEEKLY_ROTATION_FOCI = {
    0: {
        "focus_key": "workflow_validation",
        "focus_label": "issue workflow / Validation Center",
        "focus_modules": ["issue_workflow", "validation_center"],
        "preferred_plan_keys": [
            "validation_discovery_issue_intake_readonly",
            "validation_semantic_drift_discovery_readonly",
            "validation_center_discovery_run_record_integrity",
            "workflow_discovery_root_clean_guard",
        ],
    },
    1: {
        "focus_key": "paper_v2_readonly",
        "focus_label": "Paper v2 read-only live / simulation state",
        "focus_modules": ["paper_v2"],
        "preferred_plan_keys": [
            "validation_semantic_drift_discovery_readonly",
            "code_intelligence_discovery_affected_tests_quality",
            "validation_center_discovery_run_record_integrity",
            "workflow_discovery_root_clean_guard",
        ],
    },
    2: {
        "focus_key": "qe_archive_metrics",
        "focus_label": "QE archive / factor cache / experiment metrics",
        "focus_modules": ["qe"],
        "preferred_plan_keys": [
            "validation_semantic_drift_discovery_readonly",
            "code_intelligence_discovery_affected_tests_quality",
            "validation_center_discovery_run_record_integrity",
            "validation_discovery_issue_intake_readonly",
        ],
    },
    3: {
        "focus_key": "research_assistant_mcp",
        "focus_label": "Research Assistant / MCP evidence",
        "focus_modules": ["research_assistant", "mcp"],
        "preferred_plan_keys": [
            "validation_semantic_drift_discovery_readonly",
            "code_intelligence_discovery_affected_tests_quality",
            "validation_discovery_issue_intake_readonly",
            "workflow_discovery_root_clean_guard",
        ],
    },
    4: {
        "focus_key": "code_intelligence_llm",
        "focus_label": "CodeGraph / Understand Anything / LLM prompt quality",
        "focus_modules": ["code_intelligence", "llm_prompt_quality"],
        "preferred_plan_keys": [
            "validation_semantic_drift_discovery_readonly",
            "code_intelligence_discovery_affected_tests_quality",
            "validation_center_discovery_run_record_integrity",
            "workflow_discovery_root_clean_guard",
        ],
    },
    5: {
        "focus_key": "bug_replay_close_sync",
        "focus_label": "historical bug replay / close-sync integrity",
        "focus_modules": ["issue_workflow", "close_sync"],
        "preferred_plan_keys": [
            "validation_discovery_issue_intake_readonly",
            "validation_semantic_drift_discovery_readonly",
            "workflow_discovery_root_clean_guard",
            "validation_center_discovery_run_record_integrity",
        ],
    },
    6: {
        "focus_key": "ops_retention",
        "focus_label": "long-cycle data integrity / DR / retention",
        "focus_modules": ["validation_center", "ops"],
        "preferred_plan_keys": [
            "validation_semantic_drift_discovery_readonly",
            "validation_center_discovery_run_record_integrity",
            "workflow_discovery_root_clean_guard",
            "code_intelligence_discovery_affected_tests_quality",
        ],
    },
}
CHANGED_MODULE_RULES = (
    {
        "module": "issue_workflow",
        "prefixes": (
            "scripts/aistock_issue_workflow.py",
            "scripts/issue_flow.py",
            "tests/aistock_validation/bugs/",
            "docs/standards/aistock_issue",
        ),
        "plan_keys": (
            "validation_discovery_issue_intake_readonly",
            "workflow_discovery_root_clean_guard",
        ),
    },
    {
        "module": "validation_center",
        "prefixes": (
            "backend/services/validation/",
            "backend/routers/validation",
            "frontend/src/app/validation-center/",
            "frontend/src/components/validation/",
            "tests/aistock_validation/",
        ),
        "plan_keys": (
            "validation_center_discovery_run_record_integrity",
            "validation_discovery_issue_intake_readonly",
        ),
    },
    {
        "module": "validation_workflow",
        "prefixes": (
            ".github/workflows/",
            "noxfile.py",
            "tests/aistock_validation/catalog/",
        ),
        "plan_keys": (
            "workflow_discovery_root_clean_guard",
            "validation_center_discovery_run_record_integrity",
        ),
    },
    {
        "module": "code_intelligence",
        "prefixes": (
            "scripts/code_intelligence",
            "scripts/nightly_discovery",
            "scripts/llm_provider_adapter.py",
            "prompt_packs/validation_llm/",
        ),
        "plan_keys": (
            "validation_semantic_drift_discovery_readonly",
            "code_intelligence_discovery_affected_tests_quality",
            "validation_center_discovery_run_record_integrity",
        ),
    },
    {
        "module": "paper_v2",
        "prefixes": ("backend/services/paper", "frontend/src/app/paper", "frontend/src/components/paper"),
        "plan_keys": ("code_intelligence_discovery_affected_tests_quality",),
    },
    {
        "module": "qe",
        "prefixes": ("backend/services/qe", "frontend/src/app/quantevolver", "tests/qe", "scripts/qe"),
        "plan_keys": ("code_intelligence_discovery_affected_tests_quality",),
    },
    {
        "module": "research_assistant",
        "prefixes": ("backend/services/research_assistant", "frontend/src/app/research-assistant", "tests/research-assistant"),
        "plan_keys": ("code_intelligence_discovery_affected_tests_quality",),
    },
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def write_json(path: Path | None, payload: dict[str, Any]) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def read_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def split_csv(values: list[str] | None) -> list[str]:
    result: list[str] = []
    for value in values or []:
        result.extend(item.strip() for item in str(value).split(",") if item.strip())
    return result


def normalize_repo_path(value: str | Path) -> str:
    text = str(value or "").strip().lstrip("\ufeff").replace("\\", "/")
    for prefix, replacement in BOM_MOJIBAKE_PREFIX_REPLACEMENTS.items():
        if text.startswith(prefix):
            text = replacement + text.removeprefix(prefix)
            break
    text = re.sub(r"\s+", " ", text).strip()
    while text.startswith("./"):
        text = text[2:]
    if text.startswith("a/") or text.startswith("b/"):
        text = text[2:]
    return text


def is_probable_repo_path(value: str) -> bool:
    text = normalize_repo_path(value)
    if not text or "\x00" in text:
        return False
    lowered = text.lower().strip()
    if lowered in NOISE_TOKENS:
        return False
    if lowered.startswith(("http://", "https://")):
        return False
    if any(text.startswith(prefix) for prefix in DIFF_HEADER_PREFIXES):
        return False
    if text.startswith(("/", "\\")):
        return False
    first_part = text.split("/", 1)[0]
    if ":" in first_part:
        return False
    if text.startswith(("+", "-")) and not text.startswith(("+.", "-.")):
        return False
    if " " in text and "/" not in text:
        return False
    return bool(Path(text).suffix or "/" in text)


def unique_repo_paths(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        item = normalize_repo_path(value)
        if item and item not in seen and is_probable_repo_path(item):
            seen.add(item)
            result.append(item)
    return result


def read_changed_files_file(path: Path | None) -> list[str]:
    if path is None or not path.exists():
        return []
    try:
        return path.read_text(encoding="utf-8-sig", errors="replace").splitlines()
    except OSError:
        return []


def git_changed_files(base_ref: str | None, *, root: Path = ROOT) -> list[str]:
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
    return proc.stdout.splitlines()


def collect_changed_files(
    *,
    changed_files: list[str] | None = None,
    changed_files_file: Path | None = None,
    base_ref: str | None = None,
    root: Path = ROOT,
) -> list[str]:
    collected: list[str] = []
    collected.extend(split_csv(changed_files))
    collected.extend(read_changed_files_file(changed_files_file))
    collected.extend(git_changed_files(base_ref, root=root))
    return unique_repo_paths(collected)


def parse_run_date(value: str | date | datetime | None = None) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(text).date()
        except ValueError:
            try:
                return date.fromisoformat(text[:10])
            except ValueError:
                pass
    return datetime.now(timezone.utc).date()


def append_unique(target: list[str], values: list[str] | tuple[str, ...]) -> None:
    for value in values:
        text = str(value).strip()
        if text and text not in target:
            target.append(text)


def infer_changed_modules(changed_files: list[str]) -> list[str]:
    modules: list[str] = []
    lowered = [normalize_repo_path(path).lower() for path in changed_files]
    for rule in CHANGED_MODULE_RULES:
        prefixes = tuple(str(prefix).lower() for prefix in rule["prefixes"])
        if any(path.startswith(prefix) for path in lowered for prefix in prefixes):
            append_unique(modules, [str(rule["module"])])
    return modules


def _select_allowed_plan_keys(
    candidates: list[str],
    allowed: set[str],
    *,
    limit: int,
    existing: list[str] | None = None,
) -> list[str]:
    selected: list[str] = []
    seen = set(existing or [])
    for key in candidates:
        if key in allowed and key not in selected and key not in seen:
            selected.append(key)
        if len(selected) >= limit:
            break
    return selected


def build_previous_discovery_feedback(manifest_path: Path | None) -> dict[str, Any]:
    manifest = read_json(manifest_path)
    if not manifest:
        return {
            "schema_version": "aistock_nightly_discovery_feedback_v1",
            "source_manifest": str(manifest_path) if manifest_path else None,
            "signals": [],
            "preferred_plan_keys": [],
            "focus_modules": [],
            "feedback_gate": "missing",
        }

    summary = manifest.get("summary") if isinstance(manifest.get("summary"), dict) else {}
    effectiveness = manifest.get("discovery_effectiveness") if isinstance(manifest.get("discovery_effectiveness"), dict) else {}
    rotation = manifest.get("rotation") if isinstance(manifest.get("rotation"), dict) else {}
    selected = [str(item) for item in rotation.get("selected_plan_keys") or [] if str(item).strip()]
    focus_modules = [str(item) for item in rotation.get("focus_modules") or [] if str(item).strip()]
    candidates = int(summary.get("candidate_count") or effectiveness.get("candidate_count") or 0)
    ready = int(summary.get("issue_payload_ready_count") or effectiveness.get("issue_payload_ready_count") or 0)
    deduped = int(summary.get("deduped_count") or effectiveness.get("deduped_count") or 0)
    rejected = int(summary.get("rejected_count") or effectiveness.get("rejected_count") or 0)
    artifact_only = int(summary.get("artifact_only_count") or effectiveness.get("artifact_only_count") or 0)
    no_candidate_reason = effectiveness.get("no_candidate_reason") or summary.get("no_candidate_reason")

    signals: list[str] = []
    preferred: list[str] = []
    if candidates == 0 and selected:
        signals.append(f"no_candidate:{no_candidate_reason or 'unknown'}")
        append_unique(preferred, selected[:2])
    if candidates and ready == 0:
        signals.append("candidate_without_issue_payload")
        append_unique(preferred, selected[:2])
    if deduped:
        signals.append("deduped_candidates")
        append_unique(preferred, ["validation_discovery_issue_intake_readonly"])
    if candidates and ready == 0:
        append_unique(preferred, ["validation_semantic_drift_discovery_readonly"])
    if rejected or artifact_only:
        signals.append("quality_gate_noise")
        append_unique(preferred, ["code_intelligence_discovery_affected_tests_quality"])
    if not preferred and selected:
        append_unique(preferred, selected[:1])

    return {
        "schema_version": "aistock_nightly_discovery_feedback_v1",
        "source_manifest": str(manifest_path) if manifest_path else None,
        "signals": signals[:8],
        "preferred_plan_keys": preferred[:4],
        "focus_modules": focus_modules[:6],
        "previous_summary": {
            "candidate_count": candidates,
            "issue_payload_ready_count": ready,
            "deduped_count": deduped,
            "rejected_count": rejected,
            "artifact_only_count": artifact_only,
            "no_candidate_reason": no_candidate_reason,
        },
        "feedback_gate": "ready",
    }


def build_rotation_focus(
    *,
    changed_files: list[str],
    allowed_plan_keys: list[str] | None,
    module: str | None,
    feedback: dict[str, Any] | None = None,
    run_date: str | date | datetime | None = None,
    budget_plan_limit: int = 3,
) -> dict[str, Any]:
    day = parse_run_date(run_date)
    focus = WEEKLY_ROTATION_FOCI[day.weekday()]
    allowed = {str(item) for item in allowed_plan_keys or [] if str(item).strip()}
    if not allowed:
        allowed = {"l0", "validation_module_registry_l0", *READONLY_DISCOVERY_PLAN_KEYS}
    changed_modules = infer_changed_modules(changed_files)
    selected: list[str] = []
    reasons: list[dict[str, Any]] = []

    changed_candidates: list[str] = []
    lowered = [normalize_repo_path(path).lower() for path in changed_files]
    for rule in CHANGED_MODULE_RULES:
        prefixes = tuple(str(prefix).lower() for prefix in rule["prefixes"])
        if any(path.startswith(prefix) for path in lowered for prefix in prefixes):
            append_unique(changed_candidates, tuple(str(key) for key in rule["plan_keys"]))
            reasons.append(
                {
                    "reason": "changed_module_priority",
                    "module": rule["module"],
                    "plan_keys": [key for key in rule["plan_keys"] if key in allowed],
                }
            )
    append_unique(selected, _select_allowed_plan_keys(changed_candidates, allowed, limit=budget_plan_limit))

    feedback_candidates = list(feedback.get("preferred_plan_keys") or []) if isinstance(feedback, dict) else []
    feedback_keys = _select_allowed_plan_keys(
        [str(key) for key in feedback_candidates],
        allowed,
        limit=max(budget_plan_limit - len(selected), 0),
        existing=selected,
    )
    append_unique(selected, feedback_keys)
    if feedback_keys:
        reasons.append(
            {
                "reason": "previous_discovery_feedback",
                "plan_keys": feedback_keys,
                "signals": list(feedback.get("signals") or [])[:6],
            }
        )

    if len(selected) < budget_plan_limit:
        rotation_keys = _select_allowed_plan_keys(
            [str(key) for key in focus["preferred_plan_keys"]],
            allowed,
            limit=budget_plan_limit - len(selected),
            existing=selected,
        )
        append_unique(selected, rotation_keys)
        if rotation_keys:
            reasons.append(
                {
                    "reason": "weekly_rotation",
                    "focus_key": focus["focus_key"],
                    "plan_keys": rotation_keys,
                }
            )

    if len(selected) < budget_plan_limit:
        baseline_keys = _select_allowed_plan_keys(
            list(BASELINE_DISCOVERY_PLAN_KEYS),
            allowed,
            limit=budget_plan_limit - len(selected),
            existing=selected,
        )
        append_unique(selected, baseline_keys)
        if baseline_keys:
            reasons.append({"reason": "baseline_safety_net", "plan_keys": baseline_keys})

    feedback_modules = list(feedback.get("focus_modules") or []) if isinstance(feedback, dict) else []
    focus_modules = list(
        dict.fromkeys([*changed_modules, *[str(item) for item in feedback_modules], *[str(item) for item in focus["focus_modules"]]])
    )
    if module and module != "validation" and module not in focus_modules:
        focus_modules.insert(0, module)
    no_candidate_reason = (
        "readonly_rotation_found_no_anomaly_yet" if selected else "no_allowlisted_readonly_discovery_plan_selected"
    )
    return {
        "schema_version": ROTATION_SCHEMA_VERSION,
        "run_date": day.isoformat(),
        "weekday": day.weekday(),
        "focus_key": focus["focus_key"],
        "focus_label": focus["focus_label"],
        "focus_modules": focus_modules[:8],
        "changed_modules": changed_modules,
        "feedback_focus_modules": feedback_modules,
        "selected_plan_keys": selected,
        "selection_reasons": reasons,
        "feedback": feedback or {},
        "budget_plan_limit": budget_plan_limit,
        "readonly_only": True,
        "changed_module_priority_applied": bool(changed_modules),
        "no_candidate_reason": no_candidate_reason,
    }


def build_discovery_statistics(rotation: dict[str, Any]) -> dict[str, Any]:
    selected = list(rotation.get("selected_plan_keys") or [])
    return {
        "schema_version": DISCOVERY_STATS_SCHEMA_VERSION,
        "candidate_count": 0,
        "issue_payload_ready_count": 0,
        "draft_count": 0,
        "deduped_count": 0,
        "artifact_only_count": 0,
        "duplicate_rate": 0.0,
        "confirmed_real_bug_rate": None,
        "noise_rate": None,
        "executed_plan_count": 0,
        "planned_plan_count": len(selected),
        "no_candidate_reason": rotation.get("no_candidate_reason"),
    }


def git_snapshot(root: Path = ROOT) -> dict[str, Any]:
    def run_git(args: list[str]) -> str | None:
        proc = subprocess.run(
            ["git", *args],
            cwd=str(root),
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            check=False,
        )
        return proc.stdout.strip() if proc.returncode == 0 else None

    dirty = run_git(["status", "--porcelain=v1"]) or ""
    return {
        "branch": run_git(["branch", "--show-current"]),
        "head": run_git(["rev-parse", "HEAD"]),
        "origin_main": run_git(["rev-parse", "origin/main"]),
        "dirty": bool(dirty),
        "dirty_count": len([line for line in dirty.splitlines() if line.strip()]),
    }


def build_discovery_input_pack(
    *,
    run_id: str | None = None,
    changed_files: list[str] | None = None,
    changed_files_file: Path | None = None,
    base_ref: str | None = None,
    module: str | None = None,
    allowed_plan_keys: list[str] | None = None,
    codegraph_freshness_json: Path | None = None,
    code_intelligence_json: Path | None = None,
    previous_candidate_manifest: Path | None = None,
    run_date: str | date | datetime | None = None,
    budget_plan_limit: int = 3,
    root: Path = ROOT,
) -> dict[str, Any]:
    changed = collect_changed_files(
        changed_files=changed_files,
        changed_files_file=changed_files_file,
        base_ref=base_ref,
        root=root,
    )
    snapshot = git_snapshot(root)
    commit = snapshot.get("head")
    run = str(run_id or os.environ.get("GITHUB_RUN_ID") or "local")
    artifact_root = Path("tmp") / "validation" / "nightly_discovery" / run
    allowed_keys = allowed_plan_keys or ["l0", "validation_module_registry_l0"]
    feedback = build_previous_discovery_feedback(previous_candidate_manifest)
    rotation = build_rotation_focus(
        changed_files=changed,
        allowed_plan_keys=allowed_keys,
        module=module or "validation",
        feedback=feedback,
        run_date=run_date,
        budget_plan_limit=budget_plan_limit,
    )
    statistics = build_discovery_statistics(rotation)
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": run,
        "generated_at": utc_now(),
        "commit": commit,
        "branch": snapshot.get("branch"),
        "root": str(root),
        "module": module or "validation",
        "changed_files": changed,
        "changed_files_count": len(changed),
        "input_quality": {
            "changed_files_status": "ok" if changed else "empty",
            "noise_filtered": True,
            "path_encoding": "utf-8-sig-safe",
        },
        "recent_failures": [],
        "recent_bug_clusters": [],
        "codegraph_refs": {
            "freshness_json": str(codegraph_freshness_json) if codegraph_freshness_json else None,
            "code_intelligence_json": str(code_intelligence_json) if code_intelligence_json else None,
        },
        "understand_anything_refs": {},
        "allowed_plan_keys": allowed_keys,
        "rotation": rotation,
        "previous_discovery_feedback": feedback,
        "discovery_statistics": statistics,
        "readonly_runtime_targets": [],
        "stop_conditions": [
            "no_production_db_write",
            "no_production_runtime_restart",
            "allowlisted_plans_only",
        ],
        "artifact_refs": {
            "artifact_root": str(artifact_root).replace("\\", "/"),
            "changed_files_txt": str(artifact_root / "nightly-changed-files.txt").replace("\\", "/"),
            "input_pack_json": str(artifact_root / "discovery-input-pack.json").replace("\\", "/"),
        },
        "production_gates": {
            "production_ddl_gate": "noop",
            "production_frontend_dependency_gate": "noop",
            "production_backend_dependency_gate": "noop",
        },
    }


def render_changed_files_text(changed_files: list[str]) -> str:
    return "" if not changed_files else "\n".join(changed_files) + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build AIstock Nightly active-discovery input pack.")
    parser.add_argument("--run-id")
    parser.add_argument("--changed-file", action="append", default=None)
    parser.add_argument("--changed-files-file")
    parser.add_argument("--base-ref")
    parser.add_argument("--module", default="validation")
    parser.add_argument("--allowed-plan-key", action="append", default=None)
    parser.add_argument("--codegraph-freshness-json")
    parser.add_argument("--code-intelligence-json")
    parser.add_argument("--previous-candidate-manifest")
    parser.add_argument("--run-date", help="UTC date for weekly discovery rotation; defaults to today.")
    parser.add_argument("--budget-plan-limit", type=int, default=3)
    parser.add_argument("--root")
    parser.add_argument("--output")
    parser.add_argument("--changed-files-output")
    parser.add_argument("--json", action="store_true", help="Emit compact JSON stdout instead of one-line status.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    root = Path(args.root) if args.root else ROOT
    payload = build_discovery_input_pack(
        run_id=args.run_id,
        changed_files=args.changed_file,
        changed_files_file=Path(args.changed_files_file) if args.changed_files_file else None,
        base_ref=args.base_ref,
        module=args.module,
        allowed_plan_keys=list(args.allowed_plan_key or []) or None,
        codegraph_freshness_json=Path(args.codegraph_freshness_json) if args.codegraph_freshness_json else None,
        code_intelligence_json=Path(args.code_intelligence_json) if args.code_intelligence_json else None,
        previous_candidate_manifest=Path(args.previous_candidate_manifest) if args.previous_candidate_manifest else None,
        run_date=args.run_date,
        budget_plan_limit=args.budget_plan_limit,
        root=root,
    )
    write_json(Path(args.output) if args.output else None, payload)
    if args.changed_files_output:
        output = Path(args.changed_files_output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(render_changed_files_text(payload["changed_files"]), encoding="utf-8")
    if args.json:
        sys.stdout.write(json.dumps(payload, ensure_ascii=True, sort_keys=True) + "\n")
    else:
        sys.stdout.write(
            "PASS discovery-input-pack "
            f"changed_files={payload['changed_files_count']} "
            f"rotation={payload.get('rotation', {}).get('focus_key')} "
            f"selected_plans={len(payload.get('rotation', {}).get('selected_plan_keys') or [])} "
            f"input_pack={payload.get('artifact_refs', {}).get('input_pack_json')} "
            f"changed_files_ref={payload.get('artifact_refs', {}).get('changed_files_txt')}\n"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
