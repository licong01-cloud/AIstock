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
        return LlmCallResult(
            content="Understood as an individual-stock evidence-card request.",
            provider="fake",
            model="fake-primary",
            duration_ms=1,
            usage={},
        )


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
    assert "个股证据卡" in text
    assert "行情" in text
    assert "财务摘要" in text
    assert "资金流向" in text
    assert "联网基本面" in text
    assert "external-ref:600584" in text
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
