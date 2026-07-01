from __future__ import annotations

import json
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.mcp.tool_manifest import TOOL_MANIFEST_BY_NAME
from backend.routers import analysis as analysis_router
from backend.services.research_assistant.external_research import ExtractedEvidence, ExternalEvidenceItem
from backend.services.research_assistant.mcp_catalog_sync import default_mcp_tools, gateway_catalog, workflow_capabilities
from backend.services.research_assistant.models import ChatTurnRequest
from backend.services.research_assistant.react_grounding import McpToolCall
from backend.services.research_assistant.repository import InMemoryResearchAssistantRepository
from backend.services.research_assistant.service import LlmCallResult, ResearchAssistantService


STOCK_TOOL_NAMES = {
    "stock_analysis_get_quote",
    "stock_analysis_get_kline",
    "stock_analysis_get_financials",
    "stock_analysis_get_quarterly",
    "stock_analysis_get_margin_financing",
    "stock_analysis_get_fund_flow",
    "stock_analysis_get_technicals",
}


class _FakeStockEvidenceService:
    def __getattr__(self, name: str) -> Any:
        if name.startswith("get_stock_") and name.endswith("_evidence"):
            dataset = name.removeprefix("get_stock_").removesuffix("_evidence")

            def _call(symbol: str, **kwargs: Any) -> dict[str, Any]:
                as_of = str(kwargs.get("analysis_date") or "2026-06-16")
                return {
                    "ok": True,
                    "domain": f"stock_analysis.{dataset}",
                    "summary_first": True,
                    "items": [{"symbol": symbol, "dataset": dataset, "value": 1.23, "date": as_of}],
                    "total": 1,
                    "source": f"fake_{dataset}_source",
                    "source_refs": [f"stock-ref:{dataset}:{symbol}"],
                    "as_of": as_of,
                    "status": "ok",
                    "summary": f"{symbol} {dataset} evidence from fake source",
                    "dataset": dataset,
                    "response_mode": "stock_analysis_evidence_card",
                    "reason_codes": [],
                    "warnings": [],
                }

            return _call
        raise AttributeError(name)


class _FakeExternalResearchProvider:
    def search_web(self, query: str, *, locale: str = "zh-CN", limit: int = 10) -> list[ExternalEvidenceItem]:
        return [
            ExternalEvidenceItem(
                title="600584 company fundamentals",
                summary="Main business, industry position, competition and trend evidence summary.",
                url="https://example.com/600584-fundamentals",
                source="fake_web_search",
                as_of="2026-06-16",
                evidence_ref="external-ref:600584-fundamentals",
                provider="fake_external_research",
                result_type="web",
            )
        ][:limit]

    def fetch_extract(self, url: str, *, max_chars: int = 2000) -> ExtractedEvidence:
        return ExtractedEvidence(
            title="600584 extracted fundamentals",
            url=url,
            source="fake_web_extract",
            as_of="2026-06-16",
            evidence_ref="external-ref:600584-extract",
            provider="fake_external_research",
            extract_summary="Main business and industry competition extract.",
            content_preview="Main business, industry position, competition and trend extract preview.",
            detail_ref={"kind": "fake_extract", "uri": url},
        )


class _StockEvidenceCardLlm:
    def __init__(self) -> None:
        self.plan_calls: list[dict[str, Any]] = []
        self.complete_calls: list[dict[str, Any]] = []

    def complete_tool_plan(self, **kwargs: Any) -> LlmCallResult:
        self.plan_calls.append(kwargs)
        joined = "\n".join(str(message.get("content", "")) for message in kwargs.get("messages", []))
        assert "stock_analysis" in joined
        return LlmCallResult(
            content=json.dumps(
                {
                    "status": "tool_plan",
                    "domain": "stock_analysis",
                    "server_key": "aistock-stock-analysis",
                    "tool_name": "stock_analysis_get_quote",
                    "tool_args": {"symbol": "600584", "period": "1y", "limit": 8},
                    "confidence": 0.93,
                    "reason": "The user asks for an individual-stock evidence card.",
                }
            ),
            provider="fake",
            model="fake-semantic-planner",
            duration_ms=1,
            usage={},
        )

    def complete(self, **kwargs: Any) -> LlmCallResult:
        self.complete_calls.append(kwargs)
        messages = kwargs.get("messages") if isinstance(kwargs.get("messages"), list) else []
        for message in messages:
            if not isinstance(message, dict):
                continue
            try:
                payload = json.loads(str(message.get("content") or ""))
            except json.JSONDecodeError:
                continue
            if not isinstance(payload, dict) or payload.get("type") != "TOOL_RESULT":
                continue
            tool_payload = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
            if tool_payload.get("response_mode") == "stock_analysis_evidence_card":
                symbol = str(tool_payload.get("symbol") or "600584")
                source = str(tool_payload.get("source") or "stock_analysis_read_adapter")
                as_of = str(tool_payload.get("as_of") or "2026-06-16")
                sections = tool_payload.get("sections") if isinstance(tool_payload.get("sections"), list) else []
                datasets = ", ".join(str(section.get("dataset")) for section in sections if isinstance(section, dict))
                return LlmCallResult(
                    content=(
                        f"{symbol} 全方位分析：基本情况看联网基本面与财务证据，近期走势看行情、资金流和技术面，"
                        "未来趋势只给驱动、情景和风险，不预测方向，也不构成投资建议；"
                        f"驱动看行情和资金流，情景看放量/缩量验证，风险是样本窗口短；已覆盖 {datasets}。"
                        f"来源 {source}，截至 {as_of}。"
                    ),
                    provider="fake",
                    model="fake-primary",
                    duration_ms=1,
                    usage={},
                )
        return LlmCallResult(
            content="Understood as an individual-stock evidence-card request.",
            provider="fake",
            model="fake-primary",
            duration_ms=1,
            usage={},
        )


class _NativeStockEvidenceCardLlm(_StockEvidenceCardLlm):
    def complete(self, **kwargs: Any) -> LlmCallResult:
        messages = kwargs.get("messages") if isinstance(kwargs.get("messages"), list) else []
        has_tool_result = any("TOOL_RESULT" in str(message.get("content", "")) for message in messages if isinstance(message, dict))
        if not has_tool_result:
            self.complete_calls.append(kwargs)
            return LlmCallResult(
                content="",
                provider="fake",
                model="fake-primary",
                duration_ms=1,
                usage={},
                tool_calls=[
                    McpToolCall(
                        server_key="aistock-stock-analysis",
                        tool_name="stock_analysis_get_quote",
                        payload_json={"symbol": "000688", "period": "1y", "limit": 8},
                        stable_call_id="native_stock_000688",
                        reason="native_function_call:stock_analysis_get_quote",
                    )
                ],
            )
        result = super().complete(**kwargs)
        if "全方位分析" in result.content:
            return LlmCallResult(
                content=result.content.replace("600584 全方位分析", "国城矿业（000688）全方位分析", 1).replace("000688 全方位分析", "国城矿业（000688）全方位分析", 1),
                provider=result.provider,
                model=result.model,
                duration_ms=result.duration_ms,
                usage=result.usage,
                tool_calls=result.tool_calls,
            )
        return result


class _ExternalResearchNativeToolLlm(_StockEvidenceCardLlm):
    def __init__(self) -> None:
        super().__init__()
        self.first_registry: dict[str, dict[str, str]] = {}
        self.saw_repair_directive = False

    def complete(self, **kwargs: Any) -> LlmCallResult:
        messages = kwargs.get("messages") if isinstance(kwargs.get("messages"), list) else []
        for message in reversed(messages):
            if not isinstance(message, dict):
                continue
            try:
                directive = json.loads(str(message.get("content") or ""))
            except json.JSONDecodeError:
                continue
            if isinstance(directive, dict) and directive.get("type") == "REACT_EVIDENCE_GUARD_REPAIR_DIRECTIVE":
                self.saw_repair_directive = True
                options = directive.get("citation_options") if isinstance(directive.get("citation_options"), list) else []
                citation = options[0] if options and isinstance(options[0], dict) else {}
                source = str(citation.get("source") or "external_research_summary_adapter")
                as_of = str(citation.get("as_of") or "2026-06-16")
                return LlmCallResult(
                    content=f"国城矿业外部研究检索可用：已取得联网资料线索。来源 {source}，截至 {as_of}。",
                    provider="fake",
                    model="fake-primary",
                    duration_ms=1,
                    usage={},
                )
        has_tool_result = any("TOOL_RESULT" in str(message.get("content", "")) for message in messages if isinstance(message, dict))
        if not has_tool_result:
            self.complete_calls.append(kwargs)
            registry = kwargs.get("tool_registry") if isinstance(kwargs.get("tool_registry"), dict) else {}
            self.first_registry = {str(key): dict(value) for key, value in registry.items() if isinstance(value, dict)}
            return LlmCallResult(
                content="",
                provider="fake",
                model="fake-primary",
                duration_ms=1,
                usage={},
                tool_calls=[
                    McpToolCall(
                        server_key="aistock-external-research",
                        tool_name="external_research_search_web",
                        payload_json={"query": "国城矿业 000688 基本情况 近期走势 未来趋势", "limit": 2},
                        stable_call_id="native_external_research_000688",
                        reason="native_function_call:external_research_search_web",
                    )
                ],
            )
        for message in messages:
            if not isinstance(message, dict):
                continue
            try:
                payload = json.loads(str(message.get("content") or ""))
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict) and payload.get("type") == "TOOL_RESULT":
                tool_payload = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
                items = tool_payload.get("items") if isinstance(tool_payload.get("items"), list) else []
                first = items[0] if items and isinstance(items[0], dict) else {}
                source = str(first.get("source") or first.get("url") or "external_research_summary_adapter")
                as_of = str(first.get("as_of") or tool_payload.get("as_of") or "2026-06-16")
                return LlmCallResult(
                    content=(
                        f"Bottom-line: External research is available for Guocheng Mining: {first.get('title')}. "
                        "Future discussion is limited to drivers, scenarios, and risks; "
                        "I do not predict direction and this is not investment advice. "
                        "driver=industry context; scenario=follow-up verification; risk=short evidence window. "
                        f"source {source} as_of {as_of}."
                    ),
                    provider="fake",
                    model="fake-primary",
                    duration_ms=1,
                    usage={},
                )
        return super().complete(**kwargs)


class _Bug413RealStyleStockEvidenceCardLlm(_NativeStockEvidenceCardLlm):
    def __init__(self) -> None:
        super().__init__()
        self.saw_repair_directive = False

    def complete(self, **kwargs: Any) -> LlmCallResult:
        messages = kwargs.get("messages") if isinstance(kwargs.get("messages"), list) else []
        joined = "\n".join(str(message.get("content", "")) for message in messages if isinstance(message, dict))
        has_tool_result = any("TOOL_RESULT" in str(message.get("content", "")) for message in messages if isinstance(message, dict))
        if "REACT_EVIDENCE_GUARD_REPAIR_DIRECTIVE" in joined:
            self.saw_repair_directive = True
            assert "missing_inline_tool_evidence" in joined
            assert "stock-ref:quote:000688" in joined
            assert "2026-06-16" in joined
            return LlmCallResult(
                content=(
                    "国城矿业（000688）全方位分析：基本情况看联网基本面与财务证据，近期走势看行情、资金流和技术面，"
                    "未来趋势只给驱动、情景和风险，不预测方向，也不构成投资建议；"
                    "驱动看行情和资金流，情景看放量/缩量验证，风险是样本窗口短。"
                    "来源 stock-ref:quote:000688，截至 2026-06-16；"
                    "来源 stock-ref:financials:000688，截至 2026-06-16；"
                    "来源 stock-ref:fund_flow:000688，截至 2026-06-16；"
                    "来源 stock-ref:technicals:000688，截至 2026-06-16。"
                ),
                provider="fake",
                model="fake-primary",
                duration_ms=1,
                usage={},
            )
        if has_tool_result:
            self.complete_calls.append(kwargs)
            return LlmCallResult(
                content=(
                    "国城矿业（000688）全方位分析：基本情况看联网基本面与财务证据，近期走势看行情、资金流和技术面，"
                    "未来趋势只给驱动、情景和风险，不预测方向，也不构成投资建议；"
                    "驱动看行情和资金流，情景看放量/缩量验证，风险是样本窗口短。"
                ),
                provider="fake",
                model="fake-primary",
                duration_ms=1,
                usage={},
            )
        return super().complete(**kwargs)


def test_stock_analysis_tools_are_read_only_and_assistant_direct() -> None:
    manifest_tools = {name: TOOL_MANIFEST_BY_NAME[name] for name in STOCK_TOOL_NAMES}
    assert all(entry.risk_level == "read_only" for entry in manifest_tools.values())
    assert all(entry.assistant_usable == "direct_or_catalog" for entry in manifest_tools.values())
    assert all(entry.requires_confirmation is False for entry in manifest_tools.values())

    catalog_tools = {tool["tool_name"]: tool for tool in default_mcp_tools() if tool["tool_name"] in STOCK_TOOL_NAMES}
    assert set(catalog_tools) == STOCK_TOOL_NAMES
    for tool in catalog_tools.values():
        assert tool["server_key"] == "aistock-stock-analysis"
        assert tool["risk_level"] == "low"
        assert tool["side_effect_level"] == "read_only"
        assert tool["requires_approval"] is False

    save_evidence = next(tool for tool in default_mcp_tools() if tool["tool_name"] == "external_research_save_evidence")
    assert save_evidence["side_effect_level"] == "draft_only"
    assert save_evidence["requires_approval"] is True

    catalog = gateway_catalog()
    assert catalog.server_key_to_modules["aistock-stock-analysis"] == ("stock_analysis", "external_research")
    stock_capability = next(item for item in workflow_capabilities() if item["capability_key"] == "stock_analysis.mcp_orchestration")
    refs = {(ref["server_key"], ref["tool_name"]) for ref in stock_capability["mcp_tool_refs"]}
    assert ("aistock-stock-analysis", "stock_analysis_get_quote") in refs
    assert ("aistock-external-research", "external_research_search_web") in refs
    assert ("aistock-external-research", "external_research_fetch_extract") in refs


def test_a2_stock_research_tool_sets_are_provided_and_executable() -> None:
    svc = ResearchAssistantService(repository=InMemoryResearchAssistantRepository(), llm_client=_StockEvidenceCardLlm())
    svc.seed_catalogs()
    mode_decision = svc._decide_dialogue_mode(
        "国城矿业基本情况/近期走势/未来趋势",
        dialogue_intent=svc._classify_dialogue_intent("国城矿业基本情况/近期走势/未来趋势"),
        phase="analysis",
        allow_execute=False,
        risk_level="medium",
        override="analysis",
    )

    _specs, registry = svc._agentic_function_tools(mode_decision)
    available = {
        (str(ref["server_key"]), str(ref["tool_name"]))
        for capability in svc._workflow_capabilities()
        for ref in capability.get("mcp_tool_refs", [])
        if isinstance(ref, dict)
    }
    executable_entries = svc._react_tool_catalog_entries(mode_decision=mode_decision)
    executable = {(tool.server_key, tool.tool_name) for tool in executable_entries}
    read_only_executable = {
        (tool.server_key, tool.tool_name)
        for tool in executable_entries
        if tool.side_effect_level == "read_only"
    }
    manifest_read_only = {
        (str(tool.get("server_key")), str(tool.get("tool_name")))
        for tool in svc._manifest_mcp_catalog_records()
        if str(tool.get("side_effect_level") or "read_only") == "read_only"
    }
    capability_backed_non_read_only = {
        (str(tool.get("server_key")), str(tool.get("tool_name")))
        for tool in svc._manifest_mcp_catalog_records()
        if str(tool.get("side_effect_level") or "read_only") != "read_only"
        and (str(tool.get("server_key")), str(tool.get("tool_name"))) in svc._approved_capability_mcp_tool_refs()
    }
    function_registry = {(item["server_key"], item["tool_name"]) for item in registry.values()}

    required = {
        ("aistock-external-research", "external_research_search_web"),
        ("aistock-external-research", "external_research_fetch_extract"),
        ("aistock-stock-analysis", "stock_analysis_get_quote"),
        ("aistock-stock-analysis", "stock_analysis_get_financials"),
    }
    assert required <= available
    assert required <= function_registry
    assert required <= executable
    assert read_only_executable == manifest_read_only
    assert executable == function_registry == manifest_read_only | capability_backed_non_read_only


def test_stock_analysis_evidence_facade_endpoints_are_read_only_summary_envelopes() -> None:
    app = FastAPI()
    app.include_router(analysis_router.router, prefix="/api/v1")
    app.dependency_overrides[analysis_router.get_stock_analysis_evidence_service] = _FakeStockEvidenceService
    client = TestClient(app)

    paths = [
        "/api/v1/analysis/stock/evidence/quote/600584",
        "/api/v1/analysis/stock/evidence/kline/600584",
        "/api/v1/analysis/stock/evidence/financials/600584",
        "/api/v1/analysis/stock/evidence/quarterly/600584",
        "/api/v1/analysis/stock/evidence/margin-financing/600584",
        "/api/v1/analysis/stock/evidence/fund-flow/600584",
        "/api/v1/analysis/stock/evidence/technicals/600584",
    ]
    for path in paths:
        response = client.get(path)
        assert response.status_code == 200
        payload = response.json()
        assert payload["summary_first"] is True
        assert payload["response_mode"] == "stock_analysis_evidence_card"
        assert payload["as_of"]
        assert payload["source"].startswith("fake_")
        assert payload["source_refs"]
        assert payload["status"] == "ok"


def test_600584_react_smoke_returns_evidence_card_not_blocker() -> None:
    fake_llm = _StockEvidenceCardLlm()
    svc = ResearchAssistantService(repository=InMemoryResearchAssistantRepository(), llm_client=fake_llm)
    svc.seed_catalogs()
    svc.stock_analysis_facade_factory = _FakeStockEvidenceService
    svc.external_research_provider_factory = _FakeExternalResearchProvider

    result = svc.chat_turn(ChatTurnRequest(message="Analyze 600584 and produce a stock evidence card with quote, financials, fund flow, and web fundamentals."))
    text = result["assistant_message"]["content_text"]

    assert "600584" in text
    assert "基本情况" in text
    assert "近期走势" in text
    assert "未来趋势" in text
    assert "行情" in text
    assert "财务" in text
    assert "资金流" in text
    assert "联网基本面" in text
    assert "来源 stock_analysis_read_adapter" in text
    assert "阻断卡" not in text
    assert "XX" not in text
    assert "X%" not in text
    assert "source=" not in text
    assert "as_of=" not in text

    cards = result["cards"]
    assert cards["mcp_execution_result"]["auto_executed"] is True
    assert cards["mcp_summary_result"]["response_mode"] == "stock_analysis_evidence_card"
    assert cards["mcp_summary_result"]["source_refs"]
    assert cards["mcp_summary_result"]["sections"]
    assert fake_llm.plan_calls


def test_bug_403_guocheng_mining_agentic_stock_analysis_uses_native_tool_call() -> None:
    fake_llm = _NativeStockEvidenceCardLlm()
    svc = ResearchAssistantService(repository=InMemoryResearchAssistantRepository(), llm_client=fake_llm)
    svc.seed_catalogs()
    svc.stock_analysis_facade_factory = _FakeStockEvidenceService
    svc.external_research_provider_factory = _FakeExternalResearchProvider

    result = svc.chat_turn(ChatTurnRequest(message="国城矿业 基本情况/近期走势/未来趋势 全方位分析"))
    text = result["assistant_message"]["content_text"]

    assert "国城矿业" in text
    assert "000688" in text
    assert "基本情况" in text
    assert "近期走势" in text
    assert "未来趋势" in text
    assert "行情" in text
    assert "财务" in text
    assert "资金流" in text
    assert "技术" in text
    assert "联网基本面" in text
    assert "来源 stock_analysis_read_adapter" in text
    assert "澄清" not in text
    assert "个股证据卡" not in text
    assert "source=" not in text
    assert "as_of=" not in text
    assert result["cards"]["mcp_summary_result"]["symbol"] == "000688"
    datasets = {section["dataset"] for section in result["cards"]["mcp_summary_result"]["sections"]}
    assert {"quote", "financials", "fund_flow", "technicals", "fundamentals"} <= datasets
    assert result["cards"]["react_grounding"]["tool_call_count"] >= 1


def test_a2_stock_question_can_execute_external_research_native_tool_call() -> None:
    fake_llm = _ExternalResearchNativeToolLlm()
    svc = ResearchAssistantService(repository=InMemoryResearchAssistantRepository(), llm_client=fake_llm)
    svc.seed_catalogs()
    svc.stock_analysis_facade_factory = _FakeStockEvidenceService
    svc.external_research_provider_factory = _FakeExternalResearchProvider

    result = svc.chat_turn(ChatTurnRequest(message="国城矿业 基本情况/近期走势/未来趋势，需要联网资料和行情一起看", dialogue_mode_override="analysis"))
    text = result["assistant_message"]["content_text"]
    registry_pairs = {(item["server_key"], item["tool_name"]) for item in fake_llm.first_registry.values()}

    assert ("aistock-external-research", "external_research_search_web") in registry_pairs
    assert ("aistock-external-research", "external_research_fetch_extract") in registry_pairs
    assert ("aistock-stock-analysis", "stock_analysis_get_quote") in registry_pairs
    assert "example.org/external-research" not in text
    assert fake_llm.saw_repair_directive is False
    assert "capability_not_found" not in text
    assert "KeyError" not in text
    assert result["cards"]["mcp_execution_result"]["status"] == "succeeded"
    executed_pairs = {
        (item["server_key"], item["tool_name"])
        for item in result["cards"]["react_grounding"]["executed_tools"]
    }
    assert ("aistock-external-research", "external_research_search_web") in executed_pairs


def test_bug_529_guocheng_real_style_guard_failure_no_longer_forces_regeneration() -> None:
    fake_llm = _Bug413RealStyleStockEvidenceCardLlm()
    svc = ResearchAssistantService(repository=InMemoryResearchAssistantRepository(), llm_client=fake_llm)
    svc.seed_catalogs()
    svc.stock_analysis_facade_factory = _FakeStockEvidenceService
    svc.external_research_provider_factory = _FakeExternalResearchProvider

    result = svc.chat_turn(ChatTurnRequest(message="给我国城矿业 基本情况/近期走势/未来趋势 全方位分析"))
    text = result["assistant_message"]["content_text"]
    guard = result["cards"]["react_grounding"]["evidence_guard"]

    assert fake_llm.saw_repair_directive is False
    assert guard["allowed"] is True
    assert guard["reason"] == "guard_disabled"
    assert "Insufficient evidence: business reply synthesis did not pass grounding guard" not in text
    assert "000688" in text
    assert "stock-ref:quote:000688" not in text
    assert "2026-06-16" not in text
