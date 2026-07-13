"""Stock Analysis MCP tool wrappers.

Thin gateway module: delegates only to read-only /api/v1/analysis/stock/evidence/*
facades so ReAct grounding never calls multi-agent stock analysis POST routes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from backend.mcp.registry import ModuleRegistry


TOOL_NAMES = (
    "stock_analysis_get_quote",
    "stock_analysis_get_kline",
    "stock_analysis_get_financials",
    "stock_analysis_get_quarterly",
    "stock_analysis_get_margin_financing",
    "stock_analysis_get_fund_flow",
    "stock_analysis_get_technicals",
)
TOOL_COUNT = len(TOOL_NAMES)


def _fragment(registry: "ModuleRegistry", value: Any, name: str) -> str:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a string or integer path fragment; got {value!r}")
    raw = str(value) if isinstance(value, int) else value
    return registry.sanitize(raw, name)


def register(registry: "ModuleRegistry") -> None:
    client = registry.client("analysis")

    @registry.mcp.tool(name="stock_analysis_get_quote")
    def stock_analysis_get_quote(symbol: str) -> Any:
        return client.get(f"/stock/evidence/quote/{_fragment(registry, symbol, 'symbol')}")

    @registry.mcp.tool(name="stock_analysis_get_kline")
    def stock_analysis_get_kline(symbol: str, period: str = "1y", analysis_date: str | None = None) -> Any:
        return client.get(
            f"/stock/evidence/kline/{_fragment(registry, symbol, 'symbol')}",
            params={"period": period, "analysis_date": analysis_date},
        )

    @registry.mcp.tool(name="stock_analysis_get_financials")
    def stock_analysis_get_financials(symbol: str, analysis_date: str | None = None) -> Any:
        return client.get(f"/stock/evidence/financials/{_fragment(registry, symbol, 'symbol')}", params={"analysis_date": analysis_date})

    @registry.mcp.tool(name="stock_analysis_get_quarterly")
    def stock_analysis_get_quarterly(symbol: str, analysis_date: str | None = None) -> Any:
        return client.get(f"/stock/evidence/quarterly/{_fragment(registry, symbol, 'symbol')}", params={"analysis_date": analysis_date})

    @registry.mcp.tool(name="stock_analysis_get_margin_financing")
    def stock_analysis_get_margin_financing(symbol: str, analysis_date: str | None = None) -> Any:
        return client.get(f"/stock/evidence/margin-financing/{_fragment(registry, symbol, 'symbol')}", params={"analysis_date": analysis_date})

    @registry.mcp.tool(name="stock_analysis_get_fund_flow")
    def stock_analysis_get_fund_flow(symbol: str, analysis_date: str | None = None) -> Any:
        return client.get(f"/stock/evidence/fund-flow/{_fragment(registry, symbol, 'symbol')}", params={"analysis_date": analysis_date})

    @registry.mcp.tool(name="stock_analysis_get_technicals")
    def stock_analysis_get_technicals(symbol: str, period: str = "1y", analysis_date: str | None = None) -> Any:
        return client.get(
            f"/stock/evidence/technicals/{_fragment(registry, symbol, 'symbol')}",
            params={"period": period, "analysis_date": analysis_date},
        )

    registry.register_tool_count("stock_analysis", TOOL_COUNT)
