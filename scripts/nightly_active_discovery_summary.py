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


def build_summary(candidate_manifest: Path | None, discovery_manifest: Path | None) -> dict[str, Any]:
    candidate = read_json(candidate_manifest)
    discovery = read_json(discovery_manifest)
    summary = candidate.get("summary") if isinstance(candidate.get("summary"), dict) else {}
    discovery_summary = discovery.get("summary") if isinstance(discovery.get("summary"), dict) else {}
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
        "issue_payload_drafts": summary.get("issue_payload_ready_count", 0),
        "deduped": summary.get("deduped_count", 0),
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
            f"- issue_payload_drafts: `{summary.get('issue_payload_drafts')}`",
            f"- deduped: `{summary.get('deduped')}`",
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
