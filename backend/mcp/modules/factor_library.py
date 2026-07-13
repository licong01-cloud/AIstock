"""Factor Library MCP tool wrappers.

Thin gateway module: validates path fragments/confirmation text and delegates to
/api/v1/factor-library/* summary-first backend facade.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from backend.mcp.registry import ModuleRegistry


def _fragment(registry: "ModuleRegistry", value: Any, name: str) -> str:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a string or integer path fragment; got {value!r}")
    raw = str(value) if isinstance(value, int) else value
    return registry.sanitize(raw, name)


def _body(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return dict(payload or {})


def _confirmed_body(registry: "ModuleRegistry", *, confirm: str | None, expected: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    registry.confirm(confirm, expected, "confirm")
    body = _body(payload)
    body["confirm"] = expected
    return body

REGISTER_FACTOR_CONFIRM = "REGISTER_FACTOR"
DEPRECATE_FACTOR_CONFIRM = "DEPRECATE_FACTOR"

TOOL_NAMES = (
    "factor_library_list",
    "factor_library_search",
    "factor_library_get",
    "factor_library_get_coverage",
    "factor_library_get_metric_summary",
    "factor_library_get_usage_summary",
    "factor_library_plan_register",
    "factor_library_register_confirmed",
    "factor_library_plan_deprecate",
    "factor_library_deprecate_confirmed",
)
TOOL_COUNT = len(TOOL_NAMES)


def register(registry: "ModuleRegistry") -> None:
    client = registry.client("factor-library")

    @registry.mcp.tool(name="factor_library_list")
    def factor_library_list(search: str | None = None, source: str | None = None, is_available: bool | None = None, limit: int = 20, offset: int = 0) -> Any:
        return client.get("/factors", params={"search": search, "source": source, "is_available": is_available, "limit": limit, "offset": offset})

    @registry.mcp.tool(name="factor_library_search")
    def factor_library_search(q: str, limit: int = 20, offset: int = 0) -> Any:
        return client.get("/factors/search", params={"q": q, "limit": limit, "offset": offset})

    @registry.mcp.tool(name="factor_library_get")
    def factor_library_get(factor_name: str, source: str | None = None) -> Any:
        return client.get(f"/factors/{_fragment(registry, factor_name, 'factor_name')}", params={"source": source})

    @registry.mcp.tool(name="factor_library_get_coverage")
    def factor_library_get_coverage(factor_name: str) -> Any:
        return client.get(f"/factors/{_fragment(registry, factor_name, 'factor_name')}/coverage")

    @registry.mcp.tool(name="factor_library_get_metric_summary")
    def factor_library_get_metric_summary(factor_name: str) -> Any:
        """Read 1d metrics plus nullable h20/HAC companion fields for one factor."""

        return client.get(f"/factors/{_fragment(registry, factor_name, 'factor_name')}/metric-summary")

    @registry.mcp.tool(name="factor_library_get_usage_summary")
    def factor_library_get_usage_summary(factor_name: str, limit: int = 20) -> Any:
        return client.get(f"/factors/{_fragment(registry, factor_name, 'factor_name')}/usage-summary", params={"limit": limit})

    @registry.mcp.tool(name="factor_library_plan_register")
    def factor_library_plan_register(payload: dict[str, Any]) -> Any:
        return client.post("/register-plan", _body(payload))

    @registry.mcp.tool(name="factor_library_register_confirmed")
    def factor_library_register_confirmed(payload: dict[str, Any], confirm: str | None = None) -> Any:
        return client.post("/register-confirmed", _confirmed_body(registry, confirm=confirm, expected=REGISTER_FACTOR_CONFIRM, payload=payload))

    @registry.mcp.tool(name="factor_library_plan_deprecate")
    def factor_library_plan_deprecate(payload: dict[str, Any]) -> Any:
        return client.post("/deprecate-plan", _body(payload))

    @registry.mcp.tool(name="factor_library_deprecate_confirmed")
    def factor_library_deprecate_confirmed(payload: dict[str, Any], confirm: str | None = None) -> Any:
        return client.post("/deprecate-confirmed", _confirmed_body(registry, confirm=confirm, expected=DEPRECATE_FACTOR_CONFIRM, payload=payload))

    registry.register_tool_count("factor_library", TOOL_COUNT)
