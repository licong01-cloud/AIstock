from __future__ import annotations

from backend.services.research_assistant.repository import InMemoryResearchAssistantRepository
from backend.services.research_assistant.service import DialogueIntent, DialogueMode, ResearchAssistantService


EXPECTED_CAPABILITY_KEYS = [
    "local_data.health_overview",
    "local_data.plan_repair",
    "local_data.apply_repair_confirmed",
    "qe.create_experiment_draft",
    "qe.validate_template",
    "qe.materialize_template",
    "qe.run_experiment",
    "qe.analyze_result",
    "issue.sync_github",
    "memory.write_candidate",
    "factor.analyze_library",
    "rdagent.analyze_task",
    "skill_library.reuse",
    "mcp_capability.mcp_orchestration",
    "local_data.mcp_orchestration",
    "qe_experiment.mcp_orchestration",
    "qe_warehouse.mcp_orchestration",
    "validation_issue.mcp_orchestration",
    "research_pipeline.mcp_orchestration",
    "factor_library.mcp_orchestration",
    "factor_metrics.mcp_orchestration",
    "factor_correlation.mcp_orchestration",
    "model_registry.mcp_orchestration",
    "strategy_governance.mcp_orchestration",
    "execution_policy.mcp_orchestration",
    "external_research.mcp_orchestration",
    "stock_analysis.mcp_orchestration",
]

EXPECTED_CAPABILITY_PROFILES = {
    "local_data.apply_repair_confirmed": ("production_sensitive", "production_sensitive", ("APPROVE_RESEARCH_ASSISTANT_ACTION",), ("local_data_management",), 1),
    "qe.create_experiment_draft": ("medium", "draft_only", ("CONFIRM_QE_DRAFT",), (), 1),
    "qe.validate_template": ("medium", "write_nonprod", ("CONFIRM_QE_VALIDATE",), (), 1),
    "qe.materialize_template": ("high", "high_cost_compute", ("CONFIRM_QE_MATERIALIZE", "MATERIALIZE_QE_TEMPLATE"), (), 1),
    "qe.run_experiment": ("high", "high_cost_compute", ("CONFIRM_QE_RUN", "QE_EXPERIMENT_RUN"), (), 1),
    "skill_library.reuse": ("high", "write_nonprod", ("CONFIRM_SKILL_REUSE",), (), 0),
    "external_research.mcp_orchestration": ("medium", "draft_only", (), (), 4),
    "stock_analysis.mcp_orchestration": ("low", "read_only", (), (), 9),
}

EXPECTED_APPROVED_REFS = {
    "aistock-external-research/external_research_fetch_extract",
    "aistock-external-research/external_research_save_evidence",
    "aistock-external-research/external_research_search_papers",
    "aistock-external-research/external_research_search_web",
    "aistock-local-data/local_data_apply_repair_confirmed",
    "aistock-qe/qe_template_create",
    "aistock-qe/qe_template_materialize_confirmed",
    "aistock-qe/qe_template_run_confirmed",
    "aistock-qe/qe_template_create_and_run_confirmed",
    "aistock-stock-analysis/stock_analysis_get_financials",
    "aistock-stock-analysis/stock_analysis_get_fund_flow",
    "aistock-stock-analysis/stock_analysis_get_kline",
    "aistock-stock-analysis/stock_analysis_get_margin_financing",
    "aistock-stock-analysis/stock_analysis_get_quarterly",
    "aistock-stock-analysis/stock_analysis_get_quote",
    "aistock-stock-analysis/stock_analysis_get_technicals",
    "aistock-trading-ops/execution_policy_bind_confirmed",
    "aistock-trading-ops/execution_policy_retire_confirmed",
    "aistock-validation/mcp_github_issue_create",
    "research-assistant/assistant_create_memory_candidate",
}

EXPECTED_MODE_CONFIGS = {
    "dialogue": ("none", False, False, ("root.assistant", "mode.dialogue")),
    "analysis": ("read_only", False, False, ("root.assistant", "mode.analysis")),
    "planning": ("draft_only", False, True, ("root.assistant", "mode.planning", "intent.planning")),
    "preflight": ("preflight", True, True, ("root.assistant", "mode.preflight", "intent.planning")),
    "execution": ("approved_execution", True, True, ("root.assistant", "mode.execution", "intent.planning")),
    "audit": ("read_only", False, True, ("root.assistant", "mode.audit")),
    "recovery": ("read_only", False, False, ("root.assistant", "mode.recovery")),
}


def _service() -> ResearchAssistantService:
    svc = ResearchAssistantService(repository=InMemoryResearchAssistantRepository(), llm_client=object())
    seeded = svc.seed_catalogs()
    assert seeded["seeded"]["runtime_config_activations"] == 0
    assert seeded["seeded"]["capabilities"] == 0
    assert seeded["seeded"]["mcp_tools"] == 376
    return svc


def _ref_strings(capability: dict[str, object]) -> set[str]:
    refs = capability.get("mcp_tool_refs") or []
    assert isinstance(refs, list)
    return {f"{ref['server_key']}/{ref['tool_name']}" for ref in refs}


def test_yaml_runtime_capability_supply_snapshot() -> None:
    svc = _service()
    capabilities = svc._workflow_capabilities()
    by_key = {str(item["capability_key"]): item for item in capabilities}

    assert list(by_key) == EXPECTED_CAPABILITY_KEYS
    assert len(capabilities) == 27
    assert "issue.create_candidate" not in by_key

    for capability_key, (risk, side_effect, confirmations, skill_refs, mcp_ref_count) in EXPECTED_CAPABILITY_PROFILES.items():
        item = by_key[capability_key]
        assert item["risk_level"] == risk
        assert item["side_effect_level"] == side_effect
        assert tuple(item.get("required_confirmations") or []) == confirmations
        assert tuple(item.get("skill_refs") or []) == skill_refs
        assert len(item.get("mcp_tool_refs") or []) == mcp_ref_count

    assert _ref_strings(by_key["external_research.mcp_orchestration"]) == {
        "aistock-external-research/external_research_fetch_extract",
        "aistock-external-research/external_research_save_evidence",
        "aistock-external-research/external_research_search_papers",
        "aistock-external-research/external_research_search_web",
    }
    assert _ref_strings(by_key["stock_analysis.mcp_orchestration"]) == {
        "aistock-external-research/external_research_fetch_extract",
        "aistock-external-research/external_research_search_web",
        "aistock-stock-analysis/stock_analysis_get_financials",
        "aistock-stock-analysis/stock_analysis_get_fund_flow",
        "aistock-stock-analysis/stock_analysis_get_kline",
        "aistock-stock-analysis/stock_analysis_get_margin_financing",
        "aistock-stock-analysis/stock_analysis_get_quarterly",
        "aistock-stock-analysis/stock_analysis_get_quote",
        "aistock-stock-analysis/stock_analysis_get_technicals",
    }


def test_approved_tool_supply_and_mode_gating_snapshot() -> None:
    svc = _service()
    approved_refs = sorted("%s/%s" % item for item in svc._approved_capability_mcp_tool_refs())
    assert len(approved_refs) == 109
    assert EXPECTED_APPROVED_REFS <= set(approved_refs)

    config = svc.active_runtime_config()
    assert config["dialogue_modes"]["default_mode"] == "dialogue"
    assert set(config["dialogue_modes"]["modes"]) == set(EXPECTED_MODE_CONFIGS)
    for mode, (allowed_side_effect, approval_required, show_plan_card, prompt_nodes) in EXPECTED_MODE_CONFIGS.items():
        mode_cfg = config["dialogue_modes"]["modes"][mode]
        assert mode_cfg["allowed_tool_side_effect"] == allowed_side_effect
        assert bool(mode_cfg.get("approval_required")) is approval_required
        assert bool(mode_cfg.get("show_plan_card")) is show_plan_card
        assert tuple(mode_cfg["prompt_nodes"]) == prompt_nodes
        assert mode_cfg["raw_json_main_view"] is False

    tool_counts: dict[str, tuple[str, int, int]] = {}
    manifest_read_only_count = sum(
        1
        for tool in svc._manifest_mcp_catalog_records()
        if str(tool.get("side_effect_level") or "read_only") == "read_only"
    )
    capability_backed_non_read_only_count = sum(
        1
        for tool in svc._capability_backed_mcp_catalog_records()
        if str(tool.get("side_effect_level") or "read_only") != "read_only"
    )
    read_plus_capability_backed_actions = manifest_read_only_count + capability_backed_non_read_only_count
    approved_skill_count = len(svc._approved_skill_function_records())
    assert approved_skill_count == 6
    function_surface_count = read_plus_capability_backed_actions + approved_skill_count
    for mode in DialogueMode:
        decision = svc._decide_dialogue_mode(
            "",
            dialogue_intent=DialogueIntent.GENERAL_CHAT,
            phase="planning",
            allow_execute=mode == DialogueMode.EXECUTION,
            risk_level="high" if mode == DialogueMode.EXECUTION else "medium",
            override=mode.value,
        )
        tools, registry = svc._agentic_function_tools(decision)
        tool_counts[mode.value] = (decision.allowed_tool_side_effect, len(tools), len(registry))

    assert tool_counts == {
        "dialogue": ("none", approved_skill_count, approved_skill_count),
        "analysis": ("read_only", function_surface_count, function_surface_count),
        "planning": ("draft_only", function_surface_count, function_surface_count),
        "preflight": ("preflight", function_surface_count, function_surface_count),
        "execution": ("approved_execution", function_surface_count, function_surface_count),
        "audit": ("read_only", approved_skill_count, approved_skill_count),
        "recovery": ("read_only", approved_skill_count, approved_skill_count),
    }


def test_mode_router_snapshot_for_common_b1_parity_cases() -> None:
    svc = _service()
    cases = [
        ("闲聊一下", DialogueIntent.GENERAL_CHAT, False, "low", None, ("dialogue", "none", "direct_answer_intent", False, False)),
        ("只做分析 国城矿业走势", DialogueIntent.STOCK_ANALYSIS_REQUEST, False, "medium", None, ("analysis", "read_only", "analysis_only_override", False, False)),
        ("帮我规划一个 QE template 草案", DialogueIntent.EXPERIMENT_DRAFT_REQUEST, False, "medium", None, ("planning", "draft_only", "explicit_task_request", False, False)),
        ("执行 QE template run", DialogueIntent.EXPERIMENT_EXECUTION_REQUEST, True, "high", None, ("execution", "approved_execution", "execution_request_requires_existing_proposal", True, True)),
        ("任何问题", DialogueIntent.GENERAL_CHAT, False, "medium", "preflight", ("preflight", "preflight", "user_override", True, True)),
    ]
    for message, intent, allow_execute, risk_level, override, expected in cases:
        decision = svc._decide_dialogue_mode(
            message,
            dialogue_intent=intent,
            phase="planning",
            allow_execute=allow_execute,
            risk_level=risk_level,
            override=override,
        ).as_dict()
        mode, side_effect, reason, requires_tool, requires_approval = expected
        assert decision["mode"] == mode
        assert decision["allowed_tool_side_effect"] == side_effect
        assert decision["mode_reason"] == reason
        assert decision["requires_tool"] is requires_tool
        assert decision["requires_approval"] is requires_approval
