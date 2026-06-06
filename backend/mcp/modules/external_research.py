"""External Research MCP tool wrappers.

Thin gateway module: delegates to /api/v1/external-research/* summary-first
facade and keeps provider/network choices out of the MCP gateway.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from backend.mcp.registry import ModuleRegistry


TOOL_NAMES = (
    "external_research_search_web",
    "external_research_search_papers",
    "external_research_fetch_extract",
    "external_research_save_evidence",
)
TOOL_COUNT = len(TOOL_NAMES)


def _body(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return dict(payload or {})


def register(registry: "ModuleRegistry") -> None:
    client = registry.client("external-research")

    @registry.mcp.tool(name="external_research_search_web")
    def external_research_search_web(query: str, locale: str = "zh-CN", limit: int = 10) -> Any:
        return client.post("/search-web", {"query": query, "locale": locale, "limit": limit})

    @registry.mcp.tool(name="external_research_search_papers")
    def external_research_search_papers(query: str, provider: str | None = None, limit: int = 10) -> Any:
        return client.post("/search-papers", {"query": query, "provider": provider, "limit": limit})

    @registry.mcp.tool(name="external_research_fetch_extract")
    def external_research_fetch_extract(url: str, max_chars: int = 2000) -> Any:
        return client.post("/fetch-extract", {"url": url, "max_chars": max_chars})

    @registry.mcp.tool(name="external_research_save_evidence")
    def external_research_save_evidence(payload: dict[str, Any]) -> Any:
        return client.post("/save-evidence-candidate", _body(payload))

    registry.register_tool_count("external_research", TOOL_COUNT)
