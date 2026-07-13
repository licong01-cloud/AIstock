from __future__ import annotations

import json
from pathlib import Path

import scripts.issue_flow as flow


def _write_bug_record(root: Path, payload: dict[str, object]) -> str:
    rel_path = "tests/aistock_validation/bugs/20260623_BUG-700-pr-quality-gate.json"
    bug_path = root / rel_path
    bug_path.parent.mkdir(parents=True, exist_ok=True)
    bug_path.write_text(json.dumps(payload), encoding="utf-8")
    return rel_path


def test_large_feature_pr_infers_non_t0_tier_and_complexity_scope(monkeypatch) -> None:
    monkeypatch.setenv("AISTOCK_PR_TITLE", "feat(qe): support runtime-first pending tasks")
    monkeypatch.setenv(
        "AISTOCK_PR_BODY",
        "Feature implementation for QE runtime-first pending tasks with API, UI, MCP, and tests.",
    )
    monkeypatch.setattr(
        flow,
        "_git_output",
        lambda args, cwd=flow.REPO_ROOT, check=True: (
            "feature/qe-runtime-first-pending-tasks" if args[:2] == ["branch", "--show-current"] else ""
        ),
    )
    changed_files = [
        "backend/mcp/common.py",
        "backend/mcp/modules/qe_experiment.py",
        "backend/mcp/tool_manifest.py",
        "backend/routers/quantevolver.py",
        "backend/routers/quantevolver_evolution.py",
        "backend/services/quantevolver/payload_summary.py",
        "backend/services/quantevolver/qe_evolution_service.py",
        "backend/tests/mcp/test_domain_modules.py",
        "backend/tests/unified_engine/test_custom_evo_mutation_routes.py",
        "backend/tests/unified_engine/test_qe_runtime_first_pending_routes.py",
        "frontend/src/app/quantevolver/compose/page.tsx",
        "frontend/src/app/quantevolver/evolution/page.tsx",
        "frontend/src/app/quantevolver/experiments/page.tsx",
        "tests/mcp/test_mcp_gateway_cli.py",
        "tests/mcp/test_mcp_inventory_diff.py",
        "tests/mcp/test_mcp_tool_manifest.py",
    ]

    summary = flow.build_pr_quality(base="origin/main", head="HEAD", changed_files=changed_files)

    assert summary["task_tier"] in {"T2", "T3"}
    assert summary["task_tier"] != "T0"
    assert summary["complexity_inference"]["changed_file_count"] == len(changed_files)
    assert "changed_files>=8" in summary["complexity_inference"]["reasons"]
    assert summary["linkage_inference"]["feature_signals"] == ["feature"]
    assert summary["feature_linkage_gate"]["workflow_gate"] == "warning"
    assert summary["feature_linkage_gate"]["warnings"] == ["linked_issue_or_design", "scope_definition"]


def test_validation_select_keeps_indirect_impact_plans_recommended() -> None:
    payload = flow.select_validation(
        [
            "backend/mcp/common.py",
            "backend/mcp/modules/qe_experiment.py",
            "tests/mcp/test_mcp_tool_manifest.py",
        ]
    )

    assert payload["primary_modules"] == ["platform.mcp_gateway"]
    assert payload["required_plans"] == ["l0"]
    assert "research_assistant_backend" not in payload["required_plans"]
    assert "research_pipeline_backend" not in payload["required_plans"]
    promoted = {item["plan_key"] for item in payload["plan_promotions"]}
    assert "research_assistant_backend" in promoted
    assert "research_pipeline_backend" in promoted
    assert "research_assistant_backend" in payload["recommended_plans"]
    assert "research_pipeline_backend" in payload["recommended_plans"]


def test_p0p1_gate_still_blocks_real_bug_json_fix_without_evidence(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setattr(flow, "REPO_ROOT", tmp_path)
    bug_rel_path = _write_bug_record(
        tmp_path,
        {
            "bug_id": "BUG-700",
            "github_issue_number": 1700,
            "severity": "P1",
            "module": "validation",
            "allowed_write_scope": ["scripts/issue_flow.py", "tests/aistock_validation/bugs/**"],
        },
    )
    monkeypatch.setattr(flow, "_git_output", lambda args, cwd=flow.REPO_ROOT, check=True: "fix BUG-700 P1")

    assert flow.main(
        [
            "pr-check",
            "--changed-file",
            "scripts/issue_flow.py",
            "--changed-file",
            bug_rel_path,
            "--enforce-p0-p1-evidence",
        ]
    ) == 2
    summary = json.loads(capsys.readouterr().out)

    gate = summary["p0p1_evidence_gate"]
    assert gate["workflow_gate"] == "blocked"
    assert gate["is_high_risk"] is True
    assert gate["severity"] == "P1"
    assert gate["high_risk_evidence"]["changed_bug_json"] is True
    assert "validation_evidence" in gate["blocking"]
    assert "production_gates" in gate["blocking"]


def test_p0p1_gate_still_blocks_closing_bug_issue_without_evidence(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setattr(flow, "REPO_ROOT", tmp_path)
    monkeypatch.setenv("AISTOCK_PR_TITLE", "fix(validation): P1 evidence gate regression")
    monkeypatch.setenv("AISTOCK_PR_BODY", "Closes #1700")
    monkeypatch.setattr(flow, "_git_output", lambda args, cwd=flow.REPO_ROOT, check=True: "")

    assert flow.main(
        [
            "pr-check",
            "--changed-file",
            "scripts/issue_flow.py",
            "--enforce-p0-p1-evidence",
        ]
    ) == 2
    summary = json.loads(capsys.readouterr().out)

    gate = summary["p0p1_evidence_gate"]
    assert gate["workflow_gate"] == "blocked"
    assert gate["is_high_risk"] is True
    assert gate["high_risk_evidence"]["closes_bug_issue"] is True
    assert gate["high_risk_evidence"]["closing_issue_refs"] == ["#1700"]


def test_p0p1_gate_ignores_reference_only_bug_token_in_feature_pr(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setattr(flow, "REPO_ROOT", tmp_path)
    monkeypatch.setenv("AISTOCK_PR_TITLE", "feat(validation): document P1 workflow predicates")
    monkeypatch.setenv(
        "AISTOCK_PR_BODY",
        "Feature PR references BUG-470 status predicates for terminology only; it does not fix BUG-470.",
    )
    monkeypatch.setattr(flow, "_git_output", lambda args, cwd=flow.REPO_ROOT, check=True: "feature/validation-predicates")

    assert flow.main(
        [
            "pr-check",
            "--changed-file",
            "docs/architecture/validation_predicates.md",
            "--enforce-p0-p1-evidence",
        ]
    ) == 0
    summary = json.loads(capsys.readouterr().out)

    gate = summary["p0p1_evidence_gate"]
    assert "BUG-470" in summary["linked_issues"]
    assert gate["workflow_gate"] == "not_applicable"
    assert gate["is_high_risk"] is False
    assert gate["high_risk_evidence"]["reference_only_bug_id_signals"] is True
    assert gate["blocking"] == []


def test_p0p1_gate_ignores_1523_style_runtime_shadow_bug_reference(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setattr(flow, "REPO_ROOT", tmp_path)
    monkeypatch.setenv("AISTOCK_PR_TITLE", "feat(paper-v2): MiniQMT durable runtime epic")
    monkeypatch.setenv(
        "AISTOCK_PR_BODY",
        "Epic #1501 updates runtime/shadow wiring and reuses BUG-470 status predicates; "
        "this PR does not fix BUG-470.",
    )
    monkeypatch.setattr(flow, "_git_output", lambda args, cwd=flow.REPO_ROOT, check=True: "feature/miniqmt-runtime-epic")

    assert flow.main(
        [
            "pr-check",
            "--changed-file",
            "backend/services/paper_v2/runtime_shadow.py",
            "--enforce-p0-p1-evidence",
        ]
    ) == 0
    summary = json.loads(capsys.readouterr().out)

    gate = summary["p0p1_evidence_gate"]
    assert "BUG-470" in summary["linked_issues"]
    assert gate["workflow_gate"] == "not_applicable"
    assert gate["is_high_risk"] is False
    assert gate["high_risk_evidence"]["changed_bug_json"] is False
    assert gate["high_risk_evidence"]["closes_bug_issue"] is False
