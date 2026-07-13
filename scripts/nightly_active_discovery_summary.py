from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def read_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _first_int(*sources: dict[str, Any], key: str) -> int:
    for source in sources:
        if key in source and source.get(key) is not None:
            return _safe_int(source.get(key), 0)
    return 0


def build_summary(candidate_manifest: Path | None, discovery_manifest: Path | None) -> dict[str, Any]:
    candidate = read_json(candidate_manifest)
    discovery = read_json(discovery_manifest)
    summary = _mapping(candidate.get("summary"))
    effectiveness = _mapping(candidate.get("discovery_effectiveness"))
    discovery_summary = _mapping(discovery.get("summary"))
    value_metrics = _mapping(candidate.get("value_metrics"))
    feedback = _mapping(value_metrics.get("candidate_feedback"))
    rotation = candidate.get("rotation") if isinstance(candidate.get("rotation"), dict) else {}
    if not rotation and isinstance(discovery.get("rotation"), dict):
        rotation = discovery["rotation"]
    gate = candidate.get("workflow_gate") or ("warning" if not candidate else "ready")
    no_candidate_reason = summary.get("no_candidate_reason") or discovery_summary.get("no_candidate_reason") or "n/a"
    return {
        "workflow_gate": gate,
        "rotation_focus": rotation.get("focus_key") or "n/a",
        "executed_plans": discovery_summary.get("executed_count", 0),
        "anomalies": discovery_summary.get("anomaly_count", 0),
        "candidates": summary.get("candidate_count", 0),
        "high_value_candidates": _first_int(summary, effectiveness, value_metrics, key="high_value_candidate_count")
        or _first_int(summary, effectiveness, value_metrics, key="high_value_candidates"),
        "issue_payload_drafts": summary.get("issue_payload_ready_count", 0),
        "deduped": summary.get("deduped_count", 0),
        "codegraph_refs_used": _safe_int(value_metrics.get("codegraph_refs_used")),
        "ua_refs_used": _safe_int(value_metrics.get("ua_refs_used")),
        "broad_scan_avoided": bool(value_metrics.get("broad_scan_avoided")),
        "llm_advice_changed_plan": bool(value_metrics.get("llm_advice_changed_plan")),
        "feedback_available": bool(value_metrics.get("candidate_feedback_available") or candidate),
        "accepted": _first_int(summary, effectiveness, feedback, key="accepted_count"),
        "rejected": _first_int(summary, effectiveness, feedback, key="rejected_count"),
        "closed": _first_int(summary, effectiveness, feedback, key="closed_count"),
        "no_candidate_reason": no_candidate_reason,
        "artifact": candidate_manifest.as_posix() if candidate_manifest and candidate else "missing",
    }


def render_markdown(summary: dict[str, Any]) -> str:
    return "\n".join(
        [
            "",
            "## Active Discovery",
            "",
            f"- active_discovery: `{summary.get('workflow_gate')}`",
            f"- rotation_focus: `{summary.get('rotation_focus')}`",
            f"- executed_plans: `{summary.get('executed_plans')}`",
            f"- anomalies: `{summary.get('anomalies')}`",
            f"- candidates: `{summary.get('candidates')}`",
            f"- high_value_candidates: `{summary.get('high_value_candidates')}`",
            f"- issue_payload_drafts: `{summary.get('issue_payload_drafts')}`",
            f"- deduped: `{summary.get('deduped')}`",
            f"- graph_refs: `codegraph={summary.get('codegraph_refs_used')}, ua={summary.get('ua_refs_used')}, broad_scan_avoided={str(bool(summary.get('broad_scan_avoided'))).lower()}`",
            f"- llm_advice_changed_plan: `{str(bool(summary.get('llm_advice_changed_plan'))).lower()}`",
            f"- feedback: `available={str(bool(summary.get('feedback_available'))).lower()}, accepted={summary.get('accepted')}, rejected={summary.get('rejected')}, closed={summary.get('closed')}`",
            f"- no_candidate_reason: `{summary.get('no_candidate_reason')}`",
            f"- artifact: `{summary.get('artifact')}`",
        ]
    ) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Render compact Nightly active discovery summary Markdown.")
    parser.add_argument("--candidate-manifest")
    parser.add_argument("--discovery-manifest")
    args = parser.parse_args(argv)
    candidate = Path(args.candidate_manifest) if args.candidate_manifest else None
    discovery = Path(args.discovery_manifest) if args.discovery_manifest else None
    print(render_markdown(build_summary(candidate, discovery)), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
