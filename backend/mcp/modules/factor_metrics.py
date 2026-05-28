"""Factor Metrics MCP tool wrappers."""

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

SUBMIT_FACTOR_METRICS_CONFIRM = "SUBMIT_FACTOR_METRICS"
TOOL_NAMES = ("factor_metrics_plan", "factor_metrics_validate_inputs", "factor_metrics_submit_confirmed", "factor_metrics_get_job", "factor_metrics_get_result", "factor_metrics_compare_versions", "factor_metrics_export_result_ref")
TOOL_COUNT = len(TOOL_NAMES)


def register(registry: "ModuleRegistry") -> None:
    client = registry.client("factor-metrics")

    @registry.mcp.tool(name="factor_metrics_plan")
    def factor_metrics_plan(payload: dict[str, Any] | None = None) -> Any:
        return client.post("/plan", _body(payload))

    @registry.mcp.tool(name="factor_metrics_validate_inputs")
    def factor_metrics_validate_inputs(payload: dict[str, Any] | None = None) -> Any:
        return client.post("/validate-inputs", _body(payload))

    @registry.mcp.tool(name="factor_metrics_submit_confirmed")
    def factor_metrics_submit_confirmed(payload: dict[str, Any], confirm: str | None = None) -> Any:
        return client.post("/submit-confirmed", _confirmed_body(registry, confirm=confirm, expected=SUBMIT_FACTOR_METRICS_CONFIRM, payload=payload))

    @registry.mcp.tool(name="factor_metrics_get_job")
    def factor_metrics_get_job(job_id: str) -> Any:
        return client.get(f"/jobs/{_fragment(registry, job_id, 'job_id')}")

    @registry.mcp.tool(name="factor_metrics_get_result")
    def factor_metrics_get_result(factor_name: str | None = None, calc_batch_id: str | None = None, eval_window: str | None = None, limit: int = 20, offset: int = 0) -> Any:
        return client.get("/results", params={"factor_name": factor_name, "calc_batch_id": calc_batch_id, "eval_window": eval_window, "limit": limit, "offset": offset})

    @registry.mcp.tool(name="factor_metrics_compare_versions")
    def factor_metrics_compare_versions(factor_name: str, limit: int = 20) -> Any:
        return client.get("/compare-versions", params={"factor_name": factor_name, "limit": limit})

    @registry.mcp.tool(name="factor_metrics_export_result_ref")
    def factor_metrics_export_result_ref(factor_name: str | None = None, calc_batch_id: str | None = None) -> Any:
        return client.get("/export-result-ref", params={"factor_name": factor_name, "calc_batch_id": calc_batch_id})

    registry.register_tool_count("factor_metrics", TOOL_COUNT)
