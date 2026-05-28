"""Model Registry MCP tool wrappers."""

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

REGISTER_MODEL_CONFIRM = "REGISTER_MODEL"
DEPRECATE_MODEL_CONFIRM = "DEPRECATE_MODEL"
TOOL_NAMES = ("model_registry_list", "model_registry_get", "model_registry_compare_trials", "model_registry_get_seed_stability", "model_registry_get_hyperparam_history", "model_registry_get_artifacts", "model_registry_plan_register", "model_registry_register_confirmed", "model_registry_deprecate_confirmed")
TOOL_COUNT = len(TOOL_NAMES)


def register(registry: "ModuleRegistry") -> None:
    client = registry.client("model-registry")

    @registry.mcp.tool(name="model_registry_list")
    def model_registry_list(qe_selectable: bool | None = None, limit: int = 20, offset: int = 0) -> Any:
        return client.get("/models", params={"qe_selectable": qe_selectable, "limit": limit, "offset": offset})

    @registry.mcp.tool(name="model_registry_get")
    def model_registry_get(model_id: str) -> Any:
        return client.get(f"/models/{_fragment(registry, model_id, 'model_id')}")

    @registry.mcp.tool(name="model_registry_compare_trials")
    def model_registry_compare_trials(model_id: str, limit: int = 20, offset: int = 0) -> Any:
        return client.get(f"/models/{_fragment(registry, model_id, 'model_id')}/trials", params={"limit": limit, "offset": offset})

    @registry.mcp.tool(name="model_registry_get_seed_stability")
    def model_registry_get_seed_stability(model_id: str) -> Any:
        return client.get(f"/models/{_fragment(registry, model_id, 'model_id')}/seed-stability")

    @registry.mcp.tool(name="model_registry_get_hyperparam_history")
    def model_registry_get_hyperparam_history(model_id: str) -> Any:
        return client.get(f"/models/{_fragment(registry, model_id, 'model_id')}/hyperparams")

    @registry.mcp.tool(name="model_registry_get_artifacts")
    def model_registry_get_artifacts(model_id: str) -> Any:
        return client.get(f"/models/{_fragment(registry, model_id, 'model_id')}/artifacts")

    @registry.mcp.tool(name="model_registry_plan_register")
    def model_registry_plan_register(payload: dict[str, Any]) -> Any:
        return client.post("/register-plan", _body(payload))

    @registry.mcp.tool(name="model_registry_register_confirmed")
    def model_registry_register_confirmed(payload: dict[str, Any], confirm: str | None = None) -> Any:
        return client.post("/register-confirmed", _confirmed_body(registry, confirm=confirm, expected=REGISTER_MODEL_CONFIRM, payload=payload))

    @registry.mcp.tool(name="model_registry_deprecate_confirmed")
    def model_registry_deprecate_confirmed(payload: dict[str, Any], confirm: str | None = None) -> Any:
        return client.post("/deprecate-confirmed", _confirmed_body(registry, confirm=confirm, expected=DEPRECATE_MODEL_CONFIRM, payload=payload))

    registry.register_tool_count("model_registry", TOOL_COUNT)
