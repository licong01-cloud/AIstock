from __future__ import annotations

from pathlib import Path

from backend.mcp.tool_manifest import MODULE_TOOL_NAMES, TOOL_MANIFEST
from backend.services.research_assistant.mcp_catalog_sync import (
    canonicalize_server_key,
    default_mcp_servers,
    default_mcp_tools,
    function_calling_tools_for_mcp,
    load_catalog,
    mcp_tool_function_name,
)
from backend.services.research_assistant.repository import InMemoryResearchAssistantRepository
from backend.services.research_assistant.service import ResearchAssistantService

_REPO_ROOT = Path(__file__).resolve().parents[3]
_UTF8_CATALOG_FILES = (
    "backend/services/research_assistant/mcp_catalog_sync.py",
    "backend/services/research_assistant/domain_ontology.py",
    "backend/services/research_assistant/tool_router.py",
    "backend/tests/research_assistant/test_mcp_catalog_sync.py",
    "backend/tests/research_assistant/test_natural_language_mcp_routing.py",
)
_MOJIBAKE_SIGNATURES = tuple(
    chr(codepoint)
    for codepoint in (
        0x93C5,
        0x59AF,
        0x7EDB,
        0x9365,
        0x93B5,
        0x93C1,
        0x6960,
        0x6434,
        0x9419,
        0x93AC,
        0xFFFD,
    )
)


def test_research_assistant_catalog_sources_keep_real_utf8_chinese() -> None:
    for relative_path in _UTF8_CATALOG_FILES:
        text = (_REPO_ROOT / relative_path).read_text(encoding="utf-8")
        hits = [signature for signature in _MOJIBAKE_SIGNATURES if signature in text]
        assert not hits, f"{relative_path} contains mojibake signatures: {hits!r}"

    catalog_text = (_REPO_ROOT / "backend/services/research_assistant/mcp_catalog_sync.py").read_text(encoding="utf-8")
    for phrase in ["智能助理", "QE实验与数仓", "因子能力", "因子独立指标", "因子相关性", "模型库", "策略库", "执行策略"]:
        assert phrase in catalog_text


def test_default_catalog_contains_manifest_tools_on_canonical_gateway_servers() -> None:
    catalog = load_catalog()
    assert catalog["catalog_source"] == "gateway_manifest_derived_catalog"
    assert catalog["server_count"] == len(default_mcp_servers())
    assert catalog["tool_count"] == len(TOOL_MANIFEST)
    assert {item["server_key"] for item in default_mcp_servers()} == {
        "aistock-gateway-lite",
        "research-assistant",
        "aistock-research",
        "aistock-local-data",
        "aistock-qlib-data",
        "aistock-validation",
        "aistock-qe",
        "aistock-factor",
        "aistock-trading-ops",
        "aistock-paper-v2-monitor",
        "aistock-paper-v2-stable",
        "aistock-external-research",
        "aistock-stock-analysis",
    }
    assert canonicalize_server_key("aistock-qe-archive") == "aistock-qe"
    assert canonicalize_server_key("aistock-factor-library") == "aistock-factor"
    assert canonicalize_server_key("aistock-execution-policy") == "aistock-trading-ops"

    servers = {item["server_key"]: item for item in default_mcp_servers()}
    assert servers["aistock-qe"]["health_json"]["display_name_zh"] == "QE实验与数仓"
    assert "模型库" in servers["aistock-qe"]["health_json"]["business_aliases_zh"]
    assert servers["aistock-factor"]["health_json"]["display_name_zh"] == "因子能力"
    assert "因子独立指标" in servers["aistock-factor"]["health_json"]["business_aliases_zh"]
    assert servers["aistock-trading-ops"]["health_json"]["display_name_zh"] == "策略与执行治理"
    assert "执行策略" in servers["aistock-trading-ops"]["health_json"]["business_aliases_zh"]
    assert servers["aistock-paper-v2-monitor"]["health_json"]["display_name_zh"] == "模拟盘监控"
    assert "MiniQMT监控" in servers["aistock-paper-v2-monitor"]["health_json"]["business_aliases_zh"]
    assert servers["aistock-paper-v2-stable"]["health_json"]["display_name_zh"] == "模拟盘稳定能力"
    assert "策略包管理" in servers["aistock-paper-v2-stable"]["health_json"]["business_aliases_zh"]
    assert servers["aistock-external-research"]["health_json"]["display_name_zh"] == "External Research"
    assert servers["aistock-stock-analysis"]["health_json"]["domain"] == "stock_analysis"

    tools = default_mcp_tools()
    catalog_tools = [tool for tool in tools if tool["server_key"] == "aistock-gateway-lite"]
    assert len(catalog_tools) == 6
    assert {tool["tool_name"] for tool in catalog_tools} == {
        "mcp_gateway_health",
        "mcp_gateway_list_profiles",
        "mcp_gateway_list_modules",
        "mcp_gateway_list_tools",
        "mcp_gateway_search_tools",
        "mcp_gateway_preflight_tool",
    }
    assert all(tool["risk_level"] == "low" and tool["side_effect_level"] == "read_only" for tool in catalog_tools)

    assert len([tool for tool in tools if tool["server_key"] == "aistock-local-data"]) == len(MODULE_TOOL_NAMES["local_data"])
    qlib_tools = [tool for tool in tools if tool["server_key"] == "aistock-qlib-data"]
    assert len(qlib_tools) == len(MODULE_TOOL_NAMES["qlib_export"])
    assert any(tool["tool_name"] == "qlib_export_list_snapshots" for tool in qlib_tools)
    qe_tools = [tool for tool in tools if tool["server_key"] == "aistock-qe"]
    assert len(qe_tools) == len(MODULE_TOOL_NAMES["model_registry"]) + len(MODULE_TOOL_NAMES["qe_archive"]) + len(MODULE_TOOL_NAMES["qe_experiment"])
    assert any(tool["tool_name"] == "qe_archive_query_run_leaderboard" for tool in qe_tools)
    assert any(tool["tool_name"] == "model_registry_list" for tool in qe_tools)
    factor_tools = [tool for tool in tools if tool["server_key"] == "aistock-factor"]
    assert len(factor_tools) == 25
    assert any(tool["tool_name"] == "factor_library_list" for tool in factor_tools)
    assert any(tool["tool_name"] == "factor_corr_plan" for tool in factor_tools)
    paper_monitor_tools = [tool for tool in tools if tool["server_key"] == "aistock-paper-v2-monitor"]
    assert len(paper_monitor_tools) == 42
    assert any(tool["tool_name"] == "paper_v2_monitoring_running_summary" for tool in paper_monitor_tools)
    assert any(tool["tool_name"] == "qmt_broker_monitoring_get_snapshot" for tool in paper_monitor_tools)
    paper_stable_tools = [tool for tool in tools if tool["server_key"] == "aistock-paper-v2-stable"]
    assert len(paper_stable_tools) == 88
    assert any(tool["tool_name"] == "strategy_packages_list" for tool in paper_stable_tools)
    assert any(tool["tool_name"] == "selection_center_run_confirmed" for tool in paper_stable_tools)
    assert any(tool["tool_name"] == "advisory_create_program_confirmed" for tool in paper_stable_tools)
    external_tools = [tool for tool in tools if tool["server_key"] == "aistock-external-research"]
    assert len(external_tools) == 4
    assert {tool["tool_name"] for tool in external_tools} == {
        "external_research_search_web",
        "external_research_search_papers",
        "external_research_fetch_extract",
        "external_research_save_evidence",
    }
    assert all(
        next(tool for tool in external_tools if tool["tool_name"] == name)["requires_approval"] is False
        for name in ("external_research_search_web", "external_research_search_papers", "external_research_fetch_extract")
    )
    save_evidence = next(tool for tool in external_tools if tool["tool_name"] == "external_research_save_evidence")
    assert save_evidence["side_effect_level"] == "draft_only"
    assert save_evidence["requires_approval"] is True
    stock_analysis_tools = [tool for tool in tools if tool["server_key"] == "aistock-stock-analysis"]
    assert len(stock_analysis_tools) == len(MODULE_TOOL_NAMES["stock_analysis"])
    assert {tool["tool_name"] for tool in stock_analysis_tools} == set(MODULE_TOOL_NAMES["stock_analysis"])
    assert all(tool["side_effect_level"] == "read_only" for tool in stock_analysis_tools)
    assert all("gateway_manifest" in tool["preflight_schema_json"] for tool in tools)
    assert next(tool for tool in catalog_tools if tool["tool_name"] == "mcp_gateway_health")["preflight_schema_json"]["gateway_manifest"]["risk_level"] == "catalog"

    schema_by_tool = {tool["tool_name"]: tool["input_schema_json"] for tool in tools}
    assert "symbol" in schema_by_tool["stock_analysis_get_quote"]["required"]
    assert "status" in schema_by_tool["qe_experiment_list"]["properties"]
    assert "order_by" in schema_by_tool["qe_archive_query_run_leaderboard"]["properties"]
    assert "analysis_date" in schema_by_tool["local_data_get_preset_daily_status"]["properties"]
    assert "limit" in schema_by_tool["local_data_get_preset_daily_status"]["properties"]

    specs, registry = function_calling_tools_for_mcp(
        [
            next(tool for tool in tools if tool["tool_name"] == "stock_analysis_get_quote"),
            next(tool for tool in tools if tool["tool_name"] == "qe_archive_query_run_leaderboard"),
        ]
    )
    stock_function_name = mcp_tool_function_name("stock_analysis_get_quote")
    assert any(spec["function"]["name"] == stock_function_name for spec in specs)
    assert registry[stock_function_name] == {
        "server_key": "aistock-stock-analysis",
        "tool_name": "stock_analysis_get_quote",
    }
    assert "symbol" in next(spec for spec in specs if spec["function"]["name"] == stock_function_name)["function"]["parameters"]["required"]


def test_seed_catalogs_registers_manifest_cache_and_capability_reply_is_humanized() -> None:
    svc = ResearchAssistantService(repository=InMemoryResearchAssistantRepository())
    result = svc.seed_catalogs()
    assert result["seeded"]["mcp_servers"] == len(default_mcp_servers())
    assert result["seeded"]["mcp_tools"] == len(TOOL_MANIFEST)

    tools = svc.repository.list_records("mcp_tools", limit=500)["items"]
    assert len(tools) == len(TOOL_MANIFEST)
    assert any(tool["server_key"] == "aistock-gateway-lite" and tool["tool_name"] == "mcp_gateway_health" for tool in tools)
    assert any(tool["server_key"] == "aistock-factor" and tool["tool_name"] == "factor_library_list" for tool in tools)
    assert any(tool["server_key"] == "aistock-qlib-data" and tool["tool_name"] == "qlib_export_list_snapshots" for tool in tools)
    assert any(tool["server_key"] == "aistock-qe" and tool["tool_name"] == "qe_archive_query_seed_robustness" for tool in tools)
    assert any(tool["server_key"] == "aistock-trading-ops" and tool["tool_name"] == "execution_policy_bind_confirmed" for tool in tools)
    assert any(tool["server_key"] == "aistock-paper-v2-monitor" and tool["tool_name"] == "paper_v2_monitoring_running_summary" for tool in tools)
    assert any(tool["server_key"] == "aistock-paper-v2-stable" and tool["tool_name"] == "strategy_packages_list" for tool in tools)
    assert any(tool["server_key"] == "aistock-external-research" and tool["tool_name"] == "external_research_search_web" for tool in tools)
    assert any(tool["server_key"] == "aistock-stock-analysis" and tool["tool_name"] == "stock_analysis_get_quote" for tool in tools)
    assert not any(tool["server_key"] == "aistock-qe-archive" for tool in tools)
    assert not any(tool["server_key"] == "aistock-factor-library" for tool in tools)
    mcp_capability = svc._workflow_capability_by_key("mcp_capability.mcp_orchestration")
    assert mcp_capability is not None
    assert mcp_capability["mcp_tool_refs"] == [{"server_key": "research-assistant", "tool_name": "assistant_list_mcp_tools"}]
    assert svc.repository.list_records("capabilities", limit=500)["total"] == 0

    catalog = svc._mcp_tool_catalog_snapshot()
    assert catalog["source"] == "gateway_manifest_derived_catalog"
    assert catalog["manifest_tool_count"] == len(TOOL_MANIFEST)
    reply = svc._render_mcp_tool_catalog_reply(catalog)
    for phrase in ["只能", "不具备", "未登记"]:
        assert phrase not in reply
    assert "summary-first" not in reply
    assert "aistock-gateway-lite" not in reply
    assert "aistock-qe" not in reply
    assert "aistock-factor" not in reply
    assert "返回业务结果" in reply
    assert "模型库" in reply
    assert "策略库" in reply
    assert "因子独立指标" in reply
    assert "执行策略" in reply
    assert "External Research" in reply
