from __future__ import annotations

import json
from pathlib import Path

from scripts import nightly_active_discovery_summary as summary


def test_active_discovery_summary_renders_gate_counts_and_reason(tmp_path: Path) -> None:
    candidate = tmp_path / "bug-candidates" / "manifest.json"
    discovery = tmp_path / "discovery-plans" / "manifest.json"
    candidate.parent.mkdir(parents=True)
    discovery.parent.mkdir(parents=True)
    candidate.write_text(
        json.dumps(
            {
                "workflow_gate": "ready",
                "rotation": {"focus_key": "code_intelligence_llm"},
                "summary": {
                    "candidate_count": 3,
                    "high_value_candidate_count": 2,
                    "issue_payload_ready_count": 1,
                    "deduped_count": 1,
                    "accepted_count": 1,
                    "no_candidate_reason": None,
                },
                "value_metrics": {
                    "codegraph_refs_used": 2,
                    "ua_refs_used": 1,
                    "broad_scan_avoided": True,
                    "llm_advice_changed_plan": True,
                    "candidate_feedback_available": True,
                    "candidate_feedback": {"accepted_count": 1, "rejected_count": 0, "closed_count": 0},
                },
            }
        ),
        encoding="utf-8",
    )
    discovery.write_text(
        json.dumps({"summary": {"executed_count": 5, "anomaly_count": 7}}),
        encoding="utf-8",
    )

    payload = summary.build_summary(candidate, discovery)
    markdown = summary.render_markdown(payload)

    assert payload["workflow_gate"] == "ready"
    assert payload["rotation_focus"] == "code_intelligence_llm"
    assert payload["executed_plans"] == 5
    assert payload["candidates"] == 3
    assert payload["high_value_candidates"] == 2
    assert payload["codegraph_refs_used"] == 2
    assert payload["ua_refs_used"] == 1
    assert payload["broad_scan_avoided"] is True
    assert payload["llm_advice_changed_plan"] is True
    assert payload["accepted"] == 1
    assert "- active_discovery: `ready`" in markdown
    assert "- high_value_candidates: `2`" in markdown
    assert "- issue_payload_drafts: `1`" in markdown
    assert "- graph_refs: `codegraph=2, ua=1, broad_scan_avoided=true`" in markdown
    assert "- llm_advice_changed_plan: `true`" in markdown
    assert "- feedback: `available=true, accepted=1, rejected=0, closed=0`" in markdown


def test_active_discovery_summary_explains_missing_artifact(tmp_path: Path) -> None:
    discovery = tmp_path / "discovery-plans" / "manifest.json"
    discovery.parent.mkdir(parents=True)
    discovery.write_text(
        json.dumps(
            {
                "rotation": {"focus_key": "workflow_validation"},
                "summary": {"executed_count": 0, "anomaly_count": 0, "no_candidate_reason": "no_discovery_plans_selected"},
            }
        ),
        encoding="utf-8",
    )

    payload = summary.build_summary(tmp_path / "missing.json", discovery)
    markdown = summary.render_markdown(payload)

    assert payload["workflow_gate"] == "warning"
    assert payload["artifact"] == "missing"
    assert payload["no_candidate_reason"] == "no_discovery_plans_selected"
    assert "- active_discovery: `warning`" in markdown
    assert "- no_candidate_reason: `no_discovery_plans_selected`" in markdown


def test_active_discovery_summary_cli_outputs_compact_markdown(tmp_path: Path, capsys) -> None:
    candidate = tmp_path / "manifest.json"
    candidate.write_text(json.dumps({"workflow_gate": "ready", "summary": {"candidate_count": 1}}), encoding="utf-8")

    exit_code = summary.main(["--candidate-manifest", str(candidate)])
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "## Active Discovery" in output
    assert "- candidates: `1`" in output
