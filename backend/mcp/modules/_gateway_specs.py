"""Shared spec-driven helpers for thin loopback MCP modules."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from backend.mcp.registry import ModuleRegistry


HttpMethod = Literal["GET", "POST", "PATCH", "DELETE"]


@dataclass(frozen=True)
class ToolSpec:
    name: str
    method: HttpMethod
    path: str
    path_params: tuple[str, ...] = ()
    query_defaults: dict[str, Any] = field(default_factory=dict)
    limit_caps: dict[str, int] = field(default_factory=dict)
    confirm_token: str | None = None
    body_updates: dict[str, Any] = field(default_factory=dict)
    doc: str = ""


def _fragment(registry: "ModuleRegistry", value: Any, name: str) -> str:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a string or integer path fragment; got {value!r}")
    raw = str(value) if isinstance(value, int) else value
    return registry.sanitize(raw, name)


def _bounded_value(key: str, value: Any, cap: int) -> int:
    current = int(value)
    if current < 1 or current > cap:
        raise ValueError(f"{key} must be between 1 and {cap}; got {current}")
    return current


def register_spec_tools(
    registry: "ModuleRegistry",
    *,
    module_name: str,
    client_prefix: str,
    specs: tuple[ToolSpec, ...],
) -> None:
    """Register simple MCP wrappers that call loopback FastAPI endpoints."""

    client = registry.client(client_prefix)

    for spec in specs:

        def _make_tool(current: ToolSpec):
            def _tool(payload: dict[str, Any] | None = None) -> Any:
                data = dict(payload or {})
                if current.confirm_token is not None:
                    registry.confirm(data.pop("confirm", None), current.confirm_token, "confirm")
                path_values = {
                    key: _fragment(registry, data.pop(key), key)
                    for key in current.path_params
                    if key in data
                }
                missing = [key for key in current.path_params if key not in path_values]
                if missing:
                    raise ValueError(f"payload missing required path field(s): {missing!r}")
                params: dict[str, Any] = {}
                for key, default in current.query_defaults.items():
                    value = data.pop(key, default)
                    if value is not None and key in current.limit_caps:
                        value = _bounded_value(key, value, current.limit_caps[key])
                    params[key] = value
                path = current.path.format(**path_values)
                body = data.pop("body", data)
                if not isinstance(body, dict):
                    raise ValueError("payload body must be a JSON object when provided")
                if current.body_updates:
                    body = {**body, **current.body_updates}
                if current.method == "GET":
                    return client.get(path, params=params)
                if current.method == "POST":
                    return client.post(path, body, params=params)
                if current.method == "PATCH":
                    return client.request("PATCH", path, json_body=body, params=params)
                if current.method == "DELETE":
                    return client.delete(path, body)
                raise ValueError(f"unsupported method: {current.method}")

            _tool.__name__ = current.name
            _tool.__doc__ = current.doc or f"Call {current.method} {client_prefix}{current.path}"
            return _tool

        registry.mcp.tool(name=spec.name)(_make_tool(spec))

    registry.register_tool_count(module_name, len(specs))
