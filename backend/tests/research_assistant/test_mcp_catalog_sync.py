from __future__ import annotations

from pathlib import Path

from backend.services.research_assistant.mcp_catalog_sync import default_mcp_servers, default_mcp_tools, load_catalog
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
        0x93C5,  # UTF-8 Chinese bytes decoded as GBK, e.g. "智能" -> mojibake.
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
    for phrase in ["智能助理", "QE数仓", "因子库", "因子独立指标", "因子相关性", "模型库", "策略库", "执行策略库"]:
        assert phrase in catalog_text


def test_default_catalog_contains_all_current_and_new_mcp_tools() -> None:
    catalog = load_catalog()
    assert catalog["server_count"] == 13
    assert catalog["tool_count"] == 203
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
        "aistock-external-research",
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
    assert servers["aistock-external-research"]["health_json"]["display_name_zh"] == "External Research"

    local_data_tools = [tool for tool in default_mcp_tools() if tool["server_key"] == "aistock-local-data"]
    assert len(local_data_tools) == 47
    qe_archive_tools = [tool for tool in default_mcp_tools() if tool["server_key"] == "aistock-qe-archive"]
    assert len(qe_archive_tools) == 28
    assert any(tool["tool_name"] == "qe_archive_query_run_leaderboard" for tool in qe_archive_tools)
    assert any(tool["tool_name"] == "qe_archive_query_promotion_candidates" for tool in qe_archive_tools)
    external_tools = [tool for tool in default_mcp_tools() if tool["server_key"] == "aistock-external-research"]
    assert len(external_tools) == 4
    assert {tool["tool_name"] for tool in external_tools} == {
        "external_research_search_web",
        "external_research_search_papers",
        "external_research_fetch_extract",
        "external_research_save_evidence",
    }
    assert next(tool for tool in external_tools if tool["tool_name"] == "external_research_save_evidence")["side_effect_level"] == "draft_only"


def test_seed_catalogs_registers_all_mcp_tools_and_capability_reply_is_humanized() -> None:
    svc = ResearchAssistantService(repository=InMemoryResearchAssistantRepository())
    result = svc.seed_catalogs()
    assert result["seeded"]["mcp_servers"] == 13
    assert result["seeded"]["mcp_tools"] == 203

    tools = svc.repository.list_records("mcp_tools", limit=300)["items"]
    assert len(tools) == 203
    assert any(tool["server_key"] == "aistock-factor-library" and tool["tool_name"] == "factor_library_list" for tool in tools)
    assert any(tool["server_key"] == "aistock-qe-archive" and tool["tool_name"] == "qe_archive_query_seed_robustness" for tool in tools)
    assert any(tool["server_key"] == "aistock-execution-policy" and tool["tool_name"] == "execution_policy_bind_confirmed" for tool in tools)
    assert any(tool["server_key"] == "aistock-external-research" and tool["tool_name"] == "external_research_search_web" for tool in tools)

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
    assert "aistock-external-research" in reply
