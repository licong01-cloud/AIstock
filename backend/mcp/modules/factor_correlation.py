"""Factor Correlation MCP tool wrappers."""

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

SUBMIT_FACTOR_CORRELATION_CONFIRM = "SUBMIT_FACTOR_CORRELATION"
TOOL_NAMES = ("factor_corr_plan", "factor_corr_validate_inputs", "factor_corr_submit_confirmed", "factor_corr_get_job", "factor_corr_get_top_pairs", "factor_corr_get_clusters", "factor_corr_suggest_replacements", "factor_corr_get_matrix_ref")
TOOL_COUNT = len(TOOL_NAMES)


def register(registry: "ModuleRegistry") -> None:
    client = registry.client("factor-correlation")

    @registry.mcp.tool(name="factor_corr_plan")
    def factor_corr_plan(payload: dict[str, Any] | None = None) -> Any:
        return client.post("/plan", _body(payload))

    @registry.mcp.tool(name="factor_corr_validate_inputs")
    def factor_corr_validate_inputs(payload: dict[str, Any] | None = None) -> Any:
        return client.post("/validate-inputs", _body(payload))

    @registry.mcp.tool(name="factor_corr_submit_confirmed")
    def factor_corr_submit_confirmed(payload: dict[str, Any], confirm: str | None = None) -> Any:
        return client.post("/submit-confirmed", _confirmed_body(registry, confirm=confirm, expected=SUBMIT_FACTOR_CORRELATION_CONFIRM, payload=payload))

    @registry.mcp.tool(name="factor_corr_get_job")
    def factor_corr_get_job(job_id: str) -> Any:
        return client.get(f"/jobs/{_fragment(registry, job_id, 'job_id')}")

    @registry.mcp.tool(name="factor_corr_get_top_pairs")
    def factor_corr_get_top_pairs(min_abs_corr: float = 0.7, method: str | None = None, limit: int = 20, offset: int = 0) -> Any:
        return client.get("/top-pairs", params={"min_abs_corr": min_abs_corr, "method": method, "limit": limit, "offset": offset})

    @registry.mcp.tool(name="factor_corr_get_clusters")
    def factor_corr_get_clusters(min_abs_corr: float = 0.7, limit: int = 20) -> Any:
        return client.get("/clusters", params={"min_abs_corr": min_abs_corr, "limit": limit})

    @registry.mcp.tool(name="factor_corr_suggest_replacements")
    def factor_corr_suggest_replacements(factor_name: str, max_abs_corr: float = 0.4, limit: int = 20) -> Any:
        return client.get("/suggest-replacements", params={"factor_name": factor_name, "max_abs_corr": max_abs_corr, "limit": limit})

    @registry.mcp.tool(name="factor_corr_get_matrix_ref")
    def factor_corr_get_matrix_ref(as_of_date: str | None = None) -> Any:
        return client.get("/matrix-ref", params={"as_of_date": as_of_date})

    registry.register_tool_count("factor_correlation", TOOL_COUNT)
