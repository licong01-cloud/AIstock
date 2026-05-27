"""Execution Policy MCP tool wrappers."""

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

BIND_EXECUTION_POLICY_CONFIRM = "BIND_EXECUTION_POLICY"
RETIRE_EXECUTION_POLICY_CONFIRM = "RETIRE_EXECUTION_POLICY"
TOOL_NAMES = ("execution_policy_list_algos", "execution_policy_get_algo", "execution_policy_validate_for_strategy", "execution_policy_get_market_state_constraints", "execution_policy_plan_binding", "execution_policy_bind_confirmed", "execution_policy_retire_confirmed")
TOOL_COUNT = len(TOOL_NAMES)


def register(registry: "ModuleRegistry") -> None:
    client = registry.client("execution-policy")

    @registry.mcp.tool(name="execution_policy_list_algos")
    def execution_policy_list_algos() -> Any:
        return client.get("/algos")

    @registry.mcp.tool(name="execution_policy_get_algo")
    def execution_policy_get_algo(algo_code: str) -> Any:
        return client.get(f"/algos/{_fragment(registry, algo_code, 'algo_code')}")

    @registry.mcp.tool(name="execution_policy_validate_for_strategy")
    def execution_policy_validate_for_strategy(payload: dict[str, Any]) -> Any:
        return client.post("/validate-for-strategy", _body(payload))

    @registry.mcp.tool(name="execution_policy_get_market_state_constraints")
    def execution_policy_get_market_state_constraints() -> Any:
        return client.get("/market-state-constraints")

    @registry.mcp.tool(name="execution_policy_plan_binding")
    def execution_policy_plan_binding(payload: dict[str, Any]) -> Any:
        return client.post("/binding-plan", _body(payload))

    @registry.mcp.tool(name="execution_policy_bind_confirmed")
    def execution_policy_bind_confirmed(payload: dict[str, Any], confirm: str | None = None) -> Any:
        return client.post("/bind-confirmed", _confirmed_body(registry, confirm=confirm, expected=BIND_EXECUTION_POLICY_CONFIRM, payload=payload))

    @registry.mcp.tool(name="execution_policy_retire_confirmed")
    def execution_policy_retire_confirmed(payload: dict[str, Any], confirm: str | None = None) -> Any:
        return client.post("/retire-confirmed", _confirmed_body(registry, confirm=confirm, expected=RETIRE_EXECUTION_POLICY_CONFIRM, payload=payload))

    registry.register_tool_count("execution_policy", TOOL_COUNT)
