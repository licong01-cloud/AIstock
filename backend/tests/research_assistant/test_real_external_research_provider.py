from __future__ import annotations

import importlib
from typing import Any

import httpx
import pytest

from backend.routers.external_research import (
    DeterministicExternalResearchProvider,
    SearchWebRequest,
    get_external_research_provider,
    search_web,
    set_external_research_provider,
)
from backend.services.research_assistant.external_research import assert_token_safe
from backend.services.research_assistant.react_grounding import McpToolResult, _external_research_result_is_stub
from backend.services.research_assistant.real_external_research_provider import (
    AGENTSEARCH_WEB_PROVIDER,
    ARXIV_PROVIDER,
    SEMANTIC_SCHOLAR_PROVIDER,
    RealExternalResearchProvider,
)


def _client(handler: Any) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler), base_url="http://mock.local")


def _provider(
    handler: Any,
    *,
    paper_provider: str = SEMANTIC_SCHOLAR_PROVIDER,
    local_extract_allowed_hosts: tuple[str, ...] = (),
) -> RealExternalResearchProvider:
    return RealExternalResearchProvider(
        agentsearch_base_url="http://agentsearch.local",
        paper_provider=paper_provider,
        local_extract_allowed_hosts=local_extract_allowed_hosts,
        http_client=_client(handler),
    )


@pytest.fixture(autouse=True)
def _restore_external_research_provider() -> Any:
    previous = get_external_research_provider()
    try:
        yield
    finally:
        set_external_research_provider(previous)


def _as_external_web_result(payload: dict[str, Any]) -> McpToolResult:
    return McpToolResult(
        server_key="aistock-external-research",
        tool_name="external_research_search_web",
        status="succeeded",
        payload_json=payload,
        executed=True,
        summary="real external research",
    )


def test_real_search_web_agentsearch_result_is_token_safe_and_not_stub() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/search"
        assert request.url.params["q"] == "factor timing"
        assert request.url.params["count"] == "3"
        return httpx.Response(
            200,
            json={
                "results": [
                    {
                        "title": "A-share factor timing survey",
                        "summary": "Recent public analysis discusses factor timing and regime changes.",
                        "url": "https://research.example.net/factor-timing",
                        "source": "research.example.net",
                        "score": 0.91,
                    }
                ]
            },
        )

    provider = _provider(handler)
    items = provider.search_web("factor timing", limit=3)

    assert len(items) == 1
    item = items[0]
    assert item.provider == AGENTSEARCH_WEB_PROVIDER
    assert item.source == "research.example.net"
    assert item.result_type == "web"
    payload = {"items": [item.compact()], "total": 1}
    assert_token_safe(payload)
    assert not _external_research_result_is_stub(_as_external_web_result(payload))


def test_real_search_papers_semantic_scholar_maps_ids_authors_and_summary() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/graph/v1/paper/search"
        return httpx.Response(
            200,
            json={
                "data": [
                    {
                        "paperId": "paper-1",
                        "title": "Factor timing with market regimes",
                        "abstract": "We study market-regime conditioned factor timing.",
                        "tldr": {"text": "Regime states can affect factor timing."},
                        "url": "https://www.semanticscholar.org/paper/paper-1",
                        "publicationDate": "2024-05-06",
                        "authors": [{"name": "Alice"}, {"name": "Bob"}],
                        "externalIds": {"ArXiv": "2405.00001", "DOI": "10.1234/factor.1"},
                    }
                ]
            },
        )

    provider = _provider(handler)
    items = provider.search_papers("factor timing", limit=2)

    assert len(items) == 1
    item = items[0]
    assert item.provider == SEMANTIC_SCHOLAR_PROVIDER
    assert item.result_type == "paper"
    assert item.authors == ("Alice", "Bob")
    assert "Regime states" in item.summary
    assert {"kind": "arxiv_id", "value": "2405.00001"} in item.artifact_refs
    assert {"kind": "doi", "value": "10.1234/factor.1"} in item.artifact_refs
    assert_token_safe({"items": [item.compact()]})


def test_real_search_papers_semantic_scholar_429_falls_back_to_arxiv() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path == "/graph/v1/paper/search":
            return httpx.Response(429, json={"message": "rate limited"})
        if request.url.path == "/api/query":
            return httpx.Response(
                200,
                text="""<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>https://arxiv.org/abs/2405.00001</id>
    <title>Factor timing arXiv fallback</title>
    <summary>Fallback paper summary.</summary>
    <published>2024-05-01T00:00:00Z</published>
    <author><name>Carol</name></author>
  </entry>
</feed>""",
            )
        return httpx.Response(404)

    provider = _provider(handler)
    items = provider.search_papers("factor timing", limit=2)

    assert calls == ["/graph/v1/paper/search", "/api/query"]
    assert len(items) == 1
    assert items[0].provider == ARXIV_PROVIDER
    assert items[0].authors == ("Carol",)
    assert provider.last_failure("search_papers")["reason_code"] == "S2_RATE_LIMIT_FALLBACK"


def test_real_search_papers_arxiv_provider_parses_atom() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/query"
        return httpx.Response(
            200,
            text="""<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>https://arxiv.org/abs/2406.12345</id>
    <title>Alpha signals and execution</title>
    <summary>Paper summary from arXiv.</summary>
    <published>2024-06-01T00:00:00Z</published>
    <author><name>Dana</name></author>
  </entry>
</feed>""",
        )

    provider = _provider(handler, paper_provider=ARXIV_PROVIDER)
    items = provider.search_papers("alpha execution", limit=1)

    assert len(items) == 1
    assert items[0].provider == ARXIV_PROVIDER
    assert items[0].published_at == "2024-06-01"
    assert {"kind": "arxiv_id", "value": "2406.12345"} in items[0].artifact_refs


def test_real_fetch_extract_agentsearch_keeps_full_text_behind_detail_ref() -> None:
    long_text = chr(65) * 6000

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/read"
        assert request.url.params["url"] == "https://research.example.net/article"
        assert request.url.params["max_chars"] == "800"
        return httpx.Response(
            200,
            json={
                "title": "Research page",
                "url": "https://research.example.net/article",
                "source": "research.example.net",
                "content": long_text,
            },
        )

    provider = _provider(handler)
    extract = provider.fetch_extract("https://research.example.net/article", max_chars=800)
    payload = extract.compact(max_preview_chars=800)

    assert extract.provider == "agentsearch_extract"
    assert len(payload["content_preview"]) <= 800
    assert payload["detail_ref"]["inline"] is False
    assert "full_text" not in str(payload)
    assert_token_safe(payload)


def test_real_fetch_extract_agentsearch_503_uses_trafilatura_local_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeTrafilatura:
        @staticmethod
        def extract(*_args: Any, **_kwargs: Any) -> str:
            return "Locally extracted text from article."

    def fake_import(name: str) -> Any:
        if name == "trafilatura":
            return FakeTrafilatura
        return importlib.import_module(name)

    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path == "/read":
            return httpx.Response(503, json={"error": "unavailable"})
        return httpx.Response(200, text="<html><title>Local Article</title><body>Article</body></html>")

    monkeypatch.setattr(importlib, "import_module", fake_import)
    provider = _provider(handler, local_extract_allowed_hosts=("research.example.net",))
    extract = provider.fetch_extract("https://research.example.net/article", max_chars=500)

    assert calls == ["/read", "/article"]
    assert extract.provider == "local_trafilatura_extract"
    assert extract.title == "Local Article"
    assert "Locally extracted text" in extract.content_preview
    assert extract.detail_ref["fallback_from"] == "agentsearch_extract"


def test_real_fetch_extract_trafilatura_fallback_requires_host_allowlist() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path == "/read":
            return httpx.Response(503, json={"error": "unavailable"})
        raise AssertionError("local fallback must not fetch when host is not allow-listed")

    provider = _provider(handler)
    extract = provider.fetch_extract("https://research.example.net/article", max_chars=500)

    assert calls == ["/read"]
    assert extract.provider == "local_trafilatura_extract"
    assert extract.content_preview == ""
    assert extract.detail_ref["reason_code"] == "LOCAL_TRAFILATURA_HOST_NOT_ALLOWED"
    assert provider.last_failure("fetch_extract")["reason_code"] == "LOCAL_TRAFILATURA_HOST_NOT_ALLOWED"


def test_real_fetch_extract_trafilatura_fallback_rejects_internal_hosts() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path == "/read":
            return httpx.Response(503, json={"error": "unavailable"})
        raise AssertionError("local fallback must not fetch internal hosts")

    provider = _provider(handler, local_extract_allowed_hosts=("localhost",))
    extract = provider.fetch_extract("http://localhost/admin", max_chars=500)

    assert calls == ["/read"]
    assert extract.content_preview == ""
    assert extract.detail_ref["reason_code"] == "LOCAL_TRAFILATURA_INTERNAL_HOST_FORBIDDEN"
    assert provider.last_failure("fetch_extract")["reason_code"] == "LOCAL_TRAFILATURA_INTERNAL_HOST_FORBIDDEN"


def test_real_search_web_timeout_returns_empty_with_reason_code() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        raise httpx.TimeoutException("timed out")

    provider = _provider(handler)
    assert provider.search_web("factor timing") == []
    failure = provider.last_failure("search_web")
    assert failure["reason_code"] == "EXTERNAL_RESEARCH_TIMEOUT"
    assert failure["context"]["query"] == "factor timing"


def test_real_search_web_http_5xx_returns_empty_with_reason_code() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"error": "unavailable"})

    provider = _provider(handler)
    assert provider.search_web("factor timing") == []
    assert provider.last_failure("search_web")["reason_code"] == "SEARCH_WEB_HTTP_ERROR"


def test_real_search_papers_json_parse_failure_returns_empty_with_reason_code() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="not-json")

    provider = _provider(handler)
    assert provider.search_papers("factor timing") == []
    assert provider.last_failure("search_papers")["reason_code"] == "SEMANTIC_SCHOLAR_JSON_INVALID"


def test_external_research_router_surfaces_empty_reason_code() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    provider = _provider(handler)
    set_external_research_provider(provider)
    payload = search_web(SearchWebRequest(query="factor timing"))

    assert payload["items"] == []
    assert payload["total"] == 0
    assert payload["reason_codes"] == ["EXTERNAL_RESEARCH_CONNECTION_FAILED"]
    assert "fabricated" in payload["warnings"][0]["message"]


def test_external_research_default_provider_remains_deterministic() -> None:
    assert isinstance(get_external_research_provider(), DeterministicExternalResearchProvider)


def test_main_configure_external_research_provider_default_offline_keeps_stub(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend import main as backend_main

    set_external_research_provider(DeterministicExternalResearchProvider())
    monkeypatch.delenv("RA_EXTERNAL_RESEARCH_PROVIDER", raising=False)
    monkeypatch.delenv("RA_AGENTSEARCH_BASE_URL", raising=False)
    backend_main._configure_external_research_provider()
    assert isinstance(get_external_research_provider(), DeterministicExternalResearchProvider)

    monkeypatch.setenv("RA_EXTERNAL_RESEARCH_PROVIDER", "offline")
    backend_main._configure_external_research_provider()
    assert isinstance(get_external_research_provider(), DeterministicExternalResearchProvider)


def test_main_configure_external_research_provider_real_requires_base_url(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend import main as backend_main

    set_external_research_provider(DeterministicExternalResearchProvider())
    monkeypatch.setenv("RA_EXTERNAL_RESEARCH_PROVIDER", "real")
    monkeypatch.delenv("RA_AGENTSEARCH_BASE_URL", raising=False)
    backend_main._configure_external_research_provider()
    assert isinstance(get_external_research_provider(), DeterministicExternalResearchProvider)


def test_main_configure_external_research_provider_real_injects_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend import main as backend_main

    monkeypatch.setenv("RA_EXTERNAL_RESEARCH_PROVIDER", "real")
    monkeypatch.setenv("RA_AGENTSEARCH_BASE_URL", "http://agentsearch.local")
    monkeypatch.setenv("RA_PAPER_PROVIDER", "arxiv")
    backend_main._configure_external_research_provider()
    provider = get_external_research_provider()
    assert isinstance(provider, RealExternalResearchProvider)
    assert provider.agentsearch_base_url == "http://agentsearch.local"
    assert provider.paper_provider == ARXIV_PROVIDER
