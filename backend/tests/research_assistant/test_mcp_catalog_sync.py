from __future__ import annotations

from backend.services.research_assistant.mcp_catalog_sync import default_mcp_servers, default_mcp_tools, load_catalog
from backend.services.research_assistant.repository import InMemoryResearchAssistantRepository
from backend.services.research_assistant.service import ResearchAssistantService


def test_default_catalog_contains_all_current_and_new_mcp_tools() -> None:
    catalog = load_catalog()
    assert catalog["server_count"] == 12
    assert catalog["tool_count"] == 191
    assert {item["server_key"] for item in default_mcp_servers()} == {
        "research-assistant",
        "aistock-research",
        "aistock-local-data",
        "aistock-qe-experiment",
        "aistock-qe-archive",
        "aistock-validation",
        "aistock-factor-library",
        "aistock-factor-metrics",
        "aistock-factor-correlation",
        "aistock-model-registry",
        "aistock-strategy-governance",
        "aistock-execution-policy",
    }
    servers = {item["server_key"]: item for item in default_mcp_servers()}
    assert servers["aistock-model-registry"]["health_json"]["display_name_zh"] == "模型库"
    assert "模型版本" in servers["aistock-model-registry"]["health_json"]["business_aliases_zh"]
    assert servers["aistock-strategy-governance"]["health_json"]["display_name_zh"] == "策略库"
    assert "策略包" in servers["aistock-strategy-governance"]["health_json"]["business_aliases_zh"]
    assert servers["aistock-factor-library"]["health_json"]["display_name_zh"] == "因子库"
    assert servers["aistock-factor-metrics"]["health_json"]["display_name_zh"] == "因子独立指标"
    assert servers["aistock-factor-correlation"]["health_json"]["display_name_zh"] == "因子相关性"
    assert servers["aistock-execution-policy"]["health_json"]["display_name_zh"] == "执行策略库"

    local_data_tools = [tool for tool in default_mcp_tools() if tool["server_key"] == "aistock-local-data"]
    assert len(local_data_tools) == 47


def test_seed_catalogs_registers_all_mcp_tools_and_capability_reply_is_humanized() -> None:
    svc = ResearchAssistantService(repository=InMemoryResearchAssistantRepository())
    result = svc.seed_catalogs()
    assert result["seeded"]["mcp_servers"] == 12
    assert result["seeded"]["mcp_tools"] == 191

    tools = svc.repository.list_records("mcp_tools", limit=300)["items"]
    assert len(tools) == 191
    assert any(tool["server_key"] == "aistock-factor-library" and tool["tool_name"] == "factor_library_list" for tool in tools)
    assert any(tool["server_key"] == "aistock-execution-policy" and tool["tool_name"] == "execution_policy_bind_confirmed" for tool in tools)

    catalog = svc._mcp_tool_catalog_snapshot()
    reply = svc._render_mcp_tool_catalog_reply(catalog)
    for phrase in ["\u53ea\u80fd", "\u4e0d\u5177\u5907", "\u672a\u767b\u8bb0"]:
        assert phrase not in reply
    assert "summary-first" in reply
    assert "aistock-qe-archive" in reply
    assert "aistock-factor-library" in reply
    assert "模型库" in reply
    assert "策略库" in reply
    assert "因子独立指标" in reply
    assert "执行策略库" in reply
