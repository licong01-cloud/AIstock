"""Strategy Governance MCP tool wrappers."""

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

PROMOTE_STRATEGY_CONFIRM = "PROMOTE_STRATEGY"
RETIRE_STRATEGY_CONFIRM = "RETIRE_STRATEGY"
TOOL_NAMES = ("strategy_governance_list_packages", "strategy_governance_get_package", "strategy_governance_get_health", "strategy_governance_get_selection_readiness", "strategy_governance_get_paper_readiness", "strategy_governance_plan_promotion", "strategy_governance_plan_retirement", "strategy_governance_promote_confirmed", "strategy_governance_retire_confirmed")
TOOL_COUNT = len(TOOL_NAMES)


def register(registry: "ModuleRegistry") -> None:
    client = registry.client("strategy-governance")

    @registry.mcp.tool(name="strategy_governance_list_packages")
    def strategy_governance_list_packages(status: str | None = None, limit: int = 20) -> Any:
        return client.get("/packages", params={"status": status, "limit": limit})

    @registry.mcp.tool(name="strategy_governance_get_package")
    def strategy_governance_get_package(package_id: str) -> Any:
        return client.get(f"/packages/{_fragment(registry, package_id, 'package_id')}")

    @registry.mcp.tool(name="strategy_governance_get_health")
    def strategy_governance_get_health(package_id: str) -> Any:
        return client.get(f"/packages/{_fragment(registry, package_id, 'package_id')}/health")

    @registry.mcp.tool(name="strategy_governance_get_selection_readiness")
    def strategy_governance_get_selection_readiness(package_id: str) -> Any:
        return client.get(f"/packages/{_fragment(registry, package_id, 'package_id')}/selection-readiness")

    @registry.mcp.tool(name="strategy_governance_get_paper_readiness")
    def strategy_governance_get_paper_readiness(package_id: str) -> Any:
        return client.get(f"/packages/{_fragment(registry, package_id, 'package_id')}/paper-readiness")

    @registry.mcp.tool(name="strategy_governance_plan_promotion")
    def strategy_governance_plan_promotion(package_id: str, payload: dict[str, Any] | None = None) -> Any:
        return client.post(f"/packages/{_fragment(registry, package_id, 'package_id')}/promotion-plan", _body(payload))

    @registry.mcp.tool(name="strategy_governance_plan_retirement")
    def strategy_governance_plan_retirement(package_id: str, payload: dict[str, Any] | None = None) -> Any:
        return client.post(f"/packages/{_fragment(registry, package_id, 'package_id')}/retirement-plan", _body(payload))

    @registry.mcp.tool(name="strategy_governance_promote_confirmed")
    def strategy_governance_promote_confirmed(package_id: str, payload: dict[str, Any], confirm: str | None = None) -> Any:
        return client.post(f"/packages/{_fragment(registry, package_id, 'package_id')}/promote-confirmed", _confirmed_body(registry, confirm=confirm, expected=PROMOTE_STRATEGY_CONFIRM, payload=payload))

    @registry.mcp.tool(name="strategy_governance_retire_confirmed")
    def strategy_governance_retire_confirmed(package_id: str, payload: dict[str, Any] | None = None, confirm: str | None = None) -> Any:
        return client.post(f"/packages/{_fragment(registry, package_id, 'package_id')}/retire-confirmed", _confirmed_body(registry, confirm=confirm, expected=RETIRE_STRATEGY_CONFIRM, payload=payload))

    registry.register_tool_count("strategy_governance", TOOL_COUNT)
