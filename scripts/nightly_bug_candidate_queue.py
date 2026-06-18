from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
QUEUE_SCHEMA_VERSION = "aistock_bug_candidate_queue_v1"
CANDIDATE_SCHEMA_VERSION = "aistock_bug_candidate_v1"
ISSUE_PAYLOAD_SCHEMA_VERSION = "aistock_bug_candidate_github_issue_payload_v1"
DEFAULT_REPO = "licong01-cloud/AIstock"
PRODUCTION_GATES = {
    "production_ddl_gate": "noop",
    "production_frontend_dependency_gate": "noop",
    "production_backend_dependency_gate": "noop",
}
DISCOVERY_REPRODUCE_COMMAND = (
    "python scripts/nightly_discovery_plans.py --json run --plan-key {plan_key}"
)
DISCOVERY_VALIDATION_BY_PLAN = {
    "validation_discovery_issue_intake_readonly": "python -m nox -s validation_discovery_issue_intake_readonly",
    "workflow_discovery_root_clean_guard": "python -m nox -s workflow_discovery_root_clean_guard",
    "code_intelligence_discovery_affected_tests_quality": (
        "python -m nox -s code_intelligence_discovery_affected_tests_quality"
    ),
    "validation_center_discovery_run_record_integrity": (
        "python -m nox -s validation_center_discovery_run_record_integrity"
    ),
}
QUALITY_READY_THRESHOLD = 0.80


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def read_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload["public_artifact"] = True
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def repo_rel(path: Path, *, root: Path = ROOT) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def stable_hash(*parts: Any, length: int = 16) -> str:
    text = "::".join(json.dumps(part, ensure_ascii=False, sort_keys=True) for part in parts)
    return hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()[:length]


def safe_slug(value: Any, *, max_len: int = 80) -> str:
    slug = re.sub(r"[^A-Za-z0-9_.-]+", "-", str(value or "")).strip("-").lower()
    return slug[:max_len] or "candidate"


def resolve_artifact_path(value: Any, *, root: Path) -> Path | None:
    text = str(value or "").strip()
    if not text:
        return None
    path = Path(text)
    return path if path.is_absolute() else root / path


def load_discovery_plan_results(manifest_path: Path, *, root: Path) -> list[dict[str, Any]]:
    manifest = read_json(manifest_path)
    results: list[dict[str, Any]] = []
    for item in manifest.get("results") or []:
        if not isinstance(item, dict):
            continue
        artifact_path = resolve_artifact_path(item.get("artifact"), root=root)
        payload = read_json(artifact_path)
        if payload:
            results.append(payload)
    return results


def load_discovery_manifest(path: Path) -> dict[str, Any]:
    payload = read_json(path)
    return payload if isinstance(payload, dict) else {}


def is_synthetic_anomaly(anomaly: dict[str, Any]) -> bool:
    details = anomaly.get("details") if isinstance(anomaly.get("details"), dict) else {}
    text = " ".join(
        [
            str(anomaly.get("title") or ""),
            str(anomaly.get("type") or ""),
            json.dumps(details, ensure_ascii=False, sort_keys=True),
        ]
    ).lower()
    return bool(anomaly.get("synthetic") or details.get("synthetic") or "synthetic" in text or "smoke fixture" in text)


def ratio(numerator: int, denominator: int) -> float:
    return round(float(numerator) / float(denominator), 4) if denominator else 0.0


def discovery_effectiveness_summary(
    *,
    discovery_manifest: dict[str, Any],
    candidates: list[dict[str, Any]],
    issue_payload_refs: list[str],
) -> dict[str, Any]:
    candidate_count = len(candidates)
    deduped_count = sum(1 for item in candidates if item.get("status") == "deduped")
    artifact_only_count = sum(1 for item in candidates if item.get("status") == "artifact_only")
    rejected_count = sum(1 for item in candidates if item.get("status") == "rejected")
    no_candidate_reason = None
    if candidate_count == 0:
        summary = discovery_manifest.get("summary") if isinstance(discovery_manifest.get("summary"), dict) else {}
        no_candidate_reason = summary.get("no_candidate_reason") or "candidate_quality_gate_found_no_actionable_candidate"
    return {
        "schema_version": "aistock_nightly_discovery_effectiveness_v1",
        "candidate_count": candidate_count,
        "issue_payload_ready_count": len(issue_payload_refs),
        "draft_count": sum(1 for item in candidates if item.get("status") == "draft"),
        "deduped_count": deduped_count,
        "artifact_only_count": artifact_only_count,
        "rejected_count": rejected_count,
        "duplicate_rate": ratio(deduped_count, candidate_count),
        "artifact_only_rate": ratio(artifact_only_count, candidate_count),
        "issue_payload_ready_rate": ratio(len(issue_payload_refs), candidate_count),
        "confirmed_real_bug_count": 0,
        "confirmed_real_bug_rate": None,
        "noise_rate": None,
        "no_candidate_reason": no_candidate_reason,
    }


def normalize_severity(value: Any) -> str:
    severity = str(value or "P2").strip().upper()
    return severity if severity in {"P0", "P1", "P2", "P3"} else "P2"


def confidence_for(anomaly: dict[str, Any], *, evidence_refs: list[str], reproduce: list[str]) -> float:
    details = anomaly.get("details") if isinstance(anomaly.get("details"), dict) else {}
    try:
        explicit = float(details.get("confidence"))
    except (TypeError, ValueError):
        explicit = -1.0
    if 0.0 <= explicit <= 1.0:
        return round(min(explicit, 0.45) if is_synthetic_anomaly(anomaly) else explicit, 2)
    severity = normalize_severity(anomaly.get("severity"))
    score = {"P0": 0.90, "P1": 0.82, "P2": 0.62, "P3": 0.42}.get(severity, 0.62)
    if evidence_refs:
        score += 0.04
    if reproduce:
        score += 0.03
    if str(anomaly.get("title") or "").strip() and str(anomaly.get("type") or "").strip():
        score += 0.02
    if is_synthetic_anomaly(anomaly):
        score = min(score, 0.45)
    return round(max(0.0, min(score, 0.99)), 2)


def normalize_scope(evidence_refs: list[str]) -> list[str]:
    scope: list[str] = []
    for ref in evidence_refs:
        text = str(ref or "").strip().replace("\\", "/")
        if not text or "://" in text or text.startswith(("commit:", "validation_", "AD-", "#")):
            continue
        if text.startswith("file:"):
            text = text[5:]
        if re.match(r"^[A-Za-z]:/", text):
            continue
        if text.startswith((".", "/")):
            continue
        scope.append(text)
    return list(dict.fromkeys(scope))[:12]


def normalize_refs(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item or "").strip()]
    if isinstance(value, tuple):
        return [str(item).strip() for item in value if str(item or "").strip()]
    text = str(value or "").strip()
    return [text] if text else []


def fingerprint_for_anomaly(anomaly: dict[str, Any]) -> str:
    if anomaly.get("dedupe_fingerprint"):
        return str(anomaly["dedupe_fingerprint"])
    evidence_refs = normalize_refs(anomaly.get("evidence_refs"))
    return "nc-" + stable_hash(
        anomaly.get("plan_key"),
        anomaly.get("type"),
        anomaly.get("dedupe_key"),
        evidence_refs,
    )


def normalize_candidate(
    anomaly: dict[str, Any],
    *,
    run_date: str,
    codegraph_refs: list[str],
    ua_refs: list[str],
) -> dict[str, Any]:
    plan_key = str(anomaly.get("plan_key") or "unknown_plan")
    evidence_refs = normalize_refs(anomaly.get("evidence_refs"))
    reproduce = [DISCOVERY_REPRODUCE_COMMAND.format(plan_key=plan_key)]
    details = anomaly.get("details") if isinstance(anomaly.get("details"), dict) else {}
    fingerprint = fingerprint_for_anomaly(anomaly)
    candidate_id = f"NC-{run_date}-{stable_hash(fingerprint, anomaly.get('anomaly_id'), length=10)}"
    suggested_validation = [
        DISCOVERY_VALIDATION_BY_PLAN.get(plan_key, f"python -m nox -s {plan_key}")
    ]
    allowed_scope = normalize_scope(evidence_refs)
    module = str(anomaly.get("suggested_module") or anomaly.get("module") or "validation.runner")
    candidate = {
        "schema_version": CANDIDATE_SCHEMA_VERSION,
        "candidate_id": candidate_id,
        "source": "nightly_discovery",
        "source_plan_key": plan_key,
        "source_anomaly_id": anomaly.get("anomaly_id"),
        "module": module,
        "severity": normalize_severity(anomaly.get("severity")),
        "confidence": 0.0,
        "failure_kind": str(anomaly.get("type") or "nightly_discovery_anomaly"),
        "title": str(anomaly.get("title") or "Nightly discovery anomaly")[:240],
        "summary": details.get("summary") or str(anomaly.get("title") or "Nightly discovery anomaly"),
        "expected": details.get("expected")
        or f"{plan_key} should not report {anomaly.get('type') or 'anomaly'} candidates for a healthy workspace.",
        "actual": details.get("actual") or str(anomaly.get("title") or "Anomaly was detected."),
        "reproduce": reproduce,
        "evidence_refs": evidence_refs,
        "codegraph_refs": codegraph_refs,
        "ua_refs": ua_refs,
        "dedupe_fingerprint": fingerprint,
        "fingerprint": fingerprint,
        "allowed_write_scope": allowed_scope,
        "suggested_validation": suggested_validation,
        "production_gates": dict(PRODUCTION_GATES),
        "created_at": utc_now(),
        "status": "draft",
        "next_command": "review_candidate_then_submit_bug",
        "source_anomaly": {
            "schema_version": anomaly.get("schema_version") or "aistock_nightly_discovery_anomaly_v1",
            "type": anomaly.get("type"),
            "severity": anomaly.get("severity"),
            "dedupe_key": anomaly.get("dedupe_key"),
            "synthetic": is_synthetic_anomaly(anomaly),
        },
    }
    candidate["confidence"] = confidence_for(candidate | anomaly, evidence_refs=evidence_refs, reproduce=reproduce)
    return candidate


def existing_fingerprints(paths: list[Path]) -> set[str]:
    fingerprints: set[str] = set()
    for root in paths:
        if not root.exists():
            continue
        for path in root.rglob("*.json"):
            payload = read_json(path)
            candidate = payload.get("candidate") if isinstance(payload.get("candidate"), dict) else payload
            for key in ("dedupe_fingerprint", "fingerprint"):
                value = candidate.get(key) if isinstance(candidate, dict) else None
                if value:
                    fingerprints.add(str(value))
    return fingerprints


def apply_quality_gate(
    candidate: dict[str, Any],
    *,
    duplicate: bool,
    module_payload_counts: dict[str, int],
    total_payload_count: int,
    max_issue_payloads: int,
    max_issue_payloads_per_module: int,
) -> tuple[dict[str, Any], bool]:
    reasons: list[str] = []
    if duplicate:
        reasons.append("duplicate_fingerprint")
    if candidate["confidence"] < QUALITY_READY_THRESHOLD:
        reasons.append("confidence_below_threshold")
    for field in ("expected", "actual"):
        if not str(candidate.get(field) or "").strip():
            reasons.append(f"missing_{field}")
    if not candidate.get("reproduce"):
        reasons.append("missing_reproduce")
    if not candidate.get("evidence_refs"):
        reasons.append("missing_evidence")
    if not candidate.get("module"):
        reasons.append("missing_module")
    if not candidate.get("allowed_write_scope"):
        reasons.append("missing_allowed_write_scope")
    if any(value != "noop" for value in (candidate.get("production_gates") or {}).values()):
        reasons.append("production_write_required")
    if is_synthetic_anomaly(candidate.get("source_anomaly") or candidate):
        reasons.append("synthetic_anomaly")
    ready = not reasons
    module = str(candidate.get("module") or "unknown")
    if ready and total_payload_count >= max_issue_payloads:
        reasons.append("daily_issue_payload_cap_reached")
        ready = False
    if ready and module_payload_counts.get(module, 0) >= max_issue_payloads_per_module:
        reasons.append("module_issue_payload_cap_reached")
        ready = False
    if duplicate:
        status = "deduped"
    elif "synthetic_anomaly" in reasons:
        status = "artifact_only"
    else:
        status = "draft"
    candidate["status"] = status
    candidate["quality_gate"] = {
        "schema_version": "aistock_bug_candidate_quality_gate_v1",
        "workflow_gate": "ready" if ready else "draft",
        "issue_payload_ready": ready,
        "threshold": QUALITY_READY_THRESHOLD,
        "reasons": reasons,
        "auto_submit_allowed": False,
        "phase": "phase4_draft_queue_only",
    }
    return candidate, ready


def labels_for_candidate(candidate: dict[str, Any]) -> list[str]:
    severity = normalize_severity(candidate.get("severity"))
    module = safe_slug(candidate.get("module"), max_len=60)
    return list(dict.fromkeys([severity, f"severity:{severity.lower()}", f"module:{module}", "nightly-discovery", "needs-triage"]))


def render_issue_body(candidate: dict[str, Any]) -> str:
    gates = candidate.get("production_gates") if isinstance(candidate.get("production_gates"), dict) else PRODUCTION_GATES
    lines = [
        "## Failure / Anomaly Summary",
        "",
        str(candidate.get("summary") or candidate.get("title") or ""),
        "",
        "## Expected",
        "",
        str(candidate.get("expected") or ""),
        "",
        "## Actual",
        "",
        str(candidate.get("actual") or ""),
        "",
        "## Reproduce",
        "",
        *[f"- `{cmd}`" for cmd in candidate.get("reproduce") or []],
        "",
        "## Evidence Refs",
        "",
        *[f"- `{ref}`" for ref in candidate.get("evidence_refs") or []],
        "",
        "## Suggested Validation",
        "",
        *[f"- `{cmd}`" for cmd in candidate.get("suggested_validation") or []],
        "",
        "## CodeGraph / Understand Anything Refs",
        "",
        *[f"- `{ref}`" for ref in (candidate.get("codegraph_refs") or []) + (candidate.get("ua_refs") or [])],
        "",
        "## Production Gates",
        "",
        f"- production_ddl_gate={gates.get('production_ddl_gate')}",
        f"- production_frontend_dependency_gate={gates.get('production_frontend_dependency_gate')}",
        f"- production_backend_dependency_gate={gates.get('production_backend_dependency_gate')}",
        "",
        "## Dedupe Fingerprint",
        "",
        f"`{candidate.get('dedupe_fingerprint')}`",
        f"<!-- aistock-nightly-bug-candidate:{candidate.get('dedupe_fingerprint')} -->",
        "",
        "## Next Step",
        "",
        (
            "Review this draft, then promote it with "
            "`python scripts/aistock_issue_workflow.py promote-nightly-candidate "
            "--issue-payload <this-payload-json> --opt-in-auto-file --create-registry-worktree --apply` "
            "before any fix work."
        ),
    ]
    return "\n".join(lines).rstrip() + "\n"


def build_issue_payload(candidate: dict[str, Any], *, repo: str) -> dict[str, Any]:
    fingerprint = str(candidate.get("dedupe_fingerprint") or candidate.get("fingerprint"))
    marker = f"<!-- aistock-nightly-bug-candidate:{fingerprint} -->"
    return {
        "schema_version": ISSUE_PAYLOAD_SCHEMA_VERSION,
        "mode": "draft_only",
        "repo": repo,
        "candidate_id": candidate.get("candidate_id"),
        "title": f"[{candidate.get('severity')}] {candidate.get('title')}",
        "body": render_issue_body(candidate),
        "labels": labels_for_candidate(candidate),
        "candidate": candidate,
        "dedupe": {
            "fingerprint": fingerprint,
            "marker": marker,
            "search_query": f"repo:{repo} is:issue in:body {marker}",
        },
        "auto_submit_allowed": False,
        "production_gates": candidate.get("production_gates") or PRODUCTION_GATES,
    }


def render_summary_markdown(manifest: dict[str, Any]) -> str:
    summary = manifest.get("summary") if isinstance(manifest.get("summary"), dict) else {}
    effectiveness = manifest.get("discovery_effectiveness") if isinstance(manifest.get("discovery_effectiveness"), dict) else {}
    rotation = manifest.get("rotation") if isinstance(manifest.get("rotation"), dict) else {}
    lines = [
        "## Nightly BugCandidate Queue",
        "",
        f"- workflow_gate: `{manifest.get('workflow_gate')}`",
        f"- rotation_focus: `{rotation.get('focus_key') or 'n/a'}`",
        f"- candidates: `{summary.get('candidate_count', 0)}`",
        f"- issue_payload_drafts: `{summary.get('issue_payload_ready_count', 0)}`",
        f"- drafts: `{summary.get('draft_count', 0)}`",
        f"- deduped: `{summary.get('deduped_count', 0)}`",
        f"- artifact_only: `{summary.get('artifact_only_count', 0)}`",
        f"- duplicate_rate: `{effectiveness.get('duplicate_rate', 0.0)}`",
        f"- issue_payload_ready_rate: `{effectiveness.get('issue_payload_ready_rate', 0.0)}`",
        f"- no_candidate_reason: `{effectiveness.get('no_candidate_reason') or summary.get('no_candidate_reason') or 'n/a'}`",
        "- auto_submit_allowed: `false`",
        "",
        "Detailed JSON stays in the artifact bundle; this summary intentionally stays compact.",
    ]
    return "\n".join(lines) + "\n"


def build_queue(
    *,
    discovery_manifest: Path,
    output_dir: Path,
    root: Path = ROOT,
    existing_queue_dirs: list[Path] | None = None,
    code_intelligence_json: Path | None = None,
    ua_manifest_json: Path | None = None,
    repo: str = DEFAULT_REPO,
    max_issue_payloads: int = 3,
    max_issue_payloads_per_module: int = 1,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    discovery_manifest_payload = load_discovery_manifest(discovery_manifest)
    run_date = utc_now()[:10].replace("-", "")
    code_refs = [repo_rel(code_intelligence_json, root=root)] if code_intelligence_json and code_intelligence_json.exists() else []
    ua_refs = [repo_rel(ua_manifest_json, root=root)] if ua_manifest_json and ua_manifest_json.exists() else []
    prior = existing_fingerprints(existing_queue_dirs or [])
    seen: set[str] = set()
    module_payload_counts: dict[str, int] = {}
    total_payload_count = 0
    candidates: list[dict[str, Any]] = []
    issue_payload_refs: list[str] = []
    for result in load_discovery_plan_results(discovery_manifest, root=root):
        for anomaly in result.get("anomalies") or []:
            if not isinstance(anomaly, dict) or anomaly.get("candidate") is False:
                continue
            anomaly = {**anomaly, "plan_key": anomaly.get("plan_key") or result.get("plan_key")}
            candidate = normalize_candidate(anomaly, run_date=run_date, codegraph_refs=code_refs, ua_refs=ua_refs)
            fingerprint = str(candidate.get("dedupe_fingerprint"))
            duplicate = fingerprint in prior or fingerprint in seen
            candidate, ready = apply_quality_gate(
                candidate,
                duplicate=duplicate,
                module_payload_counts=module_payload_counts,
                total_payload_count=total_payload_count,
                max_issue_payloads=max_issue_payloads,
                max_issue_payloads_per_module=max_issue_payloads_per_module,
            )
            seen.add(fingerprint)
            candidate_path = output_dir / "candidates" / f"{safe_slug(candidate['candidate_id'])}.json"
            write_json(candidate_path, candidate)
            candidate["artifact_path"] = repo_rel(candidate_path, root=root)
            if ready:
                module = str(candidate.get("module") or "unknown")
                total_payload_count += 1
                module_payload_counts[module] = module_payload_counts.get(module, 0) + 1
                payload = build_issue_payload(candidate, repo=repo)
                payload_path = output_dir / "issue-payloads" / f"{safe_slug(candidate['candidate_id'])}.json"
                write_json(payload_path, payload)
                issue_payload_refs.append(repo_rel(payload_path, root=root))
                candidate["github_issue_payload_ref"] = repo_rel(payload_path, root=root)
                write_json(candidate_path, candidate)
            candidates.append(candidate)
    queue_path = output_dir / "candidate-queue.json"
    write_json(
        queue_path,
        {
            "schema_version": QUEUE_SCHEMA_VERSION,
            "generated_at": utc_now(),
            "candidates": candidates,
        },
    )
    effectiveness = discovery_effectiveness_summary(
        discovery_manifest=discovery_manifest_payload,
        candidates=candidates,
        issue_payload_refs=issue_payload_refs,
    )
    summary = dict(effectiveness)
    summary.pop("schema_version", None)
    manifest = {
        "schema_version": QUEUE_SCHEMA_VERSION,
        "generated_at": utc_now(),
        "workflow_gate": "ready",
        "phase": "phase4_draft_queue_only",
        "discovery_manifest": repo_rel(discovery_manifest, root=root),
        "rotation": discovery_manifest_payload.get("rotation") or {},
        "selection_rationale": discovery_manifest_payload.get("selection_rationale"),
        "candidate_queue_ref": repo_rel(queue_path, root=root),
        "issue_payload_refs": issue_payload_refs,
        "summary": summary,
        "discovery_effectiveness": effectiveness,
        "auto_submit_allowed": False,
        "side_effects": {
            "readonly_inputs": True,
            "writes_database": False,
            "writes_business_state": False,
            "github_issue_created": False,
        },
        "production_gates": PRODUCTION_GATES,
    }
    manifest_path = output_dir / "manifest.json"
    write_json(manifest_path, manifest)
    (output_dir / "candidate-summary.md").write_text(render_summary_markdown(manifest), encoding="utf-8")
    manifest["artifact_path"] = repo_rel(manifest_path, root=root)
    return manifest


def print_success(payload: dict[str, Any], *, as_json: bool) -> None:
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    compact = {
        "check": "nightly-bug-candidate-queue",
        "workflow_gate": payload.get("workflow_gate"),
        "candidates": summary.get("candidate_count", 0),
        "issue_payloads": summary.get("issue_payload_ready_count", 0),
        "drafts": summary.get("draft_count", 0),
        "deduped": summary.get("deduped_count", 0),
        "artifact": payload.get("artifact_path"),
    }
    if as_json:
        print(json.dumps(compact, ensure_ascii=False, sort_keys=True))
    else:
        print(
            "PASS nightly-bug-candidate-queue "
            + " ".join(f"{key}={value}" for key, value in compact.items() if key != "check")
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build AIstock Nightly BugCandidate draft queue artifacts.")
    parser.add_argument("--json", action="store_true", default=False, help="Emit compact JSON stdout.")
    sub = parser.add_subparsers(dest="command", required=True)
    build = sub.add_parser("build")
    build.add_argument("--discovery-manifest", required=True)
    build.add_argument("--output-dir", required=True)
    build.add_argument("--root", default=str(ROOT))
    build.add_argument("--existing-queue-dir", action="append", default=[])
    build.add_argument("--code-intelligence-json")
    build.add_argument("--ua-manifest-json")
    build.add_argument("--repo", default=DEFAULT_REPO)
    build.add_argument("--max-issue-payloads", type=int, default=3)
    build.add_argument("--max-issue-payloads-per-module", type=int, default=1)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    root = Path(args.root).resolve()
    if args.command == "build":
        payload = build_queue(
            discovery_manifest=Path(args.discovery_manifest),
            output_dir=Path(args.output_dir),
            root=root,
            existing_queue_dirs=[Path(item) for item in args.existing_queue_dir],
            code_intelligence_json=Path(args.code_intelligence_json) if args.code_intelligence_json else None,
            ua_manifest_json=Path(args.ua_manifest_json) if args.ua_manifest_json else None,
            repo=args.repo,
            max_issue_payloads=args.max_issue_payloads,
            max_issue_payloads_per_module=args.max_issue_payloads_per_module,
        )
        print_success(payload, as_json=args.json)
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
