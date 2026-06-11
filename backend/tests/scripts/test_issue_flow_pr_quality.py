from __future__ import annotations

import scripts.issue_flow as flow


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
