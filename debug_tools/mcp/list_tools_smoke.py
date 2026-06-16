"""Static MCP server tool-list smoke for gateway-backed AIstock MCP servers.

This diagnostic intentionally does not call the backend. It reads `.mcp.json`,
resolves the selected gateway profile/modules, imports MCP wrapper modules in
process, and reports the registered tool names plus compact input schemas.
"""

from __future__ import annotations

import argparse
import inspect
import json
import sys
from collections.abc import Callable
from importlib import import_module
from pathlib import Path
from types import UnionType
from typing import Any, get_args, get_origin

import httpx

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.mcp.profiles import resolve_modules  # noqa: E402
from backend.mcp.registry import ModuleRegistry  # noqa: E402


class _StaticMCP:
    def __init__(self) -> None:
        self.tools: dict[str, Callable[..., Any]] = {}

    def tool(self, name: str | None = None, **_kwargs: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            self.tools[name or func.__name__] = func
            return func

        return decorator


def _load_config(config_path: Path) -> dict[str, Any]:
    if not config_path.exists():
        raise FileNotFoundError(f"MCP config not found: {config_path}")
    data = json.loads(config_path.read_text(encoding="utf-8-sig"))
    servers = data.get("mcpServers")
    if not isinstance(servers, dict) or not servers:
        raise ValueError(f"MCP config {config_path} must contain non-empty mcpServers")
    return data


def _arg_value(args: list[str], key: str) -> str | None:
    for index, arg in enumerate(args):
        if arg == key and index + 1 < len(args):
            return args[index + 1]
        if arg.startswith(key + "="):
            return arg.split("=", 1)[1]
    return None


def _server_selection(config: dict[str, Any], server: str) -> tuple[str, list[str], str]:
    servers = config["mcpServers"]
    spec = servers.get(server)
    if not isinstance(spec, dict):
        raise KeyError(f"unknown MCP server: {server}")
    args = [str(item) for item in spec.get("args") or []]
    if not any(arg.replace("\\", "/").endswith("scripts/aistock_mcp_gateway.py") for arg in args):
        raise ValueError(f"MCP server {server!r} is not backed by scripts/aistock_mcp_gateway.py")
    profile = _arg_value(args, "--profile") or "research"
    modules_arg = _arg_value(args, "--modules")
    base_url = str((spec.get("env") or {}).get("AISTOCK_MCP_BASE_URL") or "http://127.0.0.1:8001/api/v1")
    return profile, resolve_modules(profile=profile, modules=modules_arg), base_url


def _schema_type(annotation: Any) -> str:
    if annotation is inspect.Signature.empty:
        return "object"
    origin = get_origin(annotation)
    if origin in {UnionType, getattr(__import__("typing"), "Union")}:
        non_none = [item for item in get_args(annotation) if item is not type(None)]
        return _schema_type(non_none[0]) if len(non_none) == 1 else "object"
    if annotation is str:
        return "string"
    if annotation is int:
        return "integer"
    if annotation is float:
        return "number"
    if annotation is bool:
        return "boolean"
    if annotation in {dict, dict[str, Any]} or origin is dict:
        return "object"
    if annotation in {list, list[Any]} or origin is list:
        return "array"
    return "object"


def _schema_for(func: Callable[..., Any]) -> dict[str, Any]:
    signature = inspect.signature(func)
    properties: dict[str, dict[str, str]] = {}
    required: list[str] = []
    for name, parameter in signature.parameters.items():
        if parameter.kind in {inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD}:
            continue
        properties[name] = {"type": _schema_type(parameter.annotation)}
        if parameter.default is inspect.Signature.empty:
            required.append(name)
    schema: dict[str, Any] = {"properties": sorted(properties)}
    if required:
        schema["required"] = required
    return schema


def _introspect(server: str, profile: str, modules: list[str], base_url: str) -> dict[str, Any]:
    mcp = _StaticMCP()
    registry = ModuleRegistry(
        mcp=mcp,
        base_url=base_url,
        env_name="AISTOCK_MCP_BASE_URL",
        server_name=server,
        profile=profile,
        selected_modules=tuple(modules),
        transport=httpx.MockTransport(lambda _request: httpx.Response(599, json={"error": "static smoke only"})),
    )
    for module_name in modules:
        module = import_module(f"backend.mcp.modules.{module_name}")
        register = getattr(module, "register", None)
        if register is None:
            raise RuntimeError(f"backend.mcp.modules.{module_name} does not define register(registry)")
        register(registry)
    return {
        "introspection_mode": "static_in_process",
        "server": server,
        "profile": profile,
        "module": modules[0] if len(modules) == 1 else None,
        "modules": modules,
        "production_8001_touched": False,
        "tool_count": len(mcp.tools),
        "tools": list(mcp.tools),
        "schemas": {name: _schema_for(func) for name, func in mcp.tools.items()},
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", required=True, help="MCP server key from .mcp.json")
    parser.add_argument("--config", default=str(REPO_ROOT / ".mcp.json"), help="Path to MCP JSON config")
    args = parser.parse_args(argv)

    try:
        config = _load_config(Path(args.config))
        profile, modules, base_url = _server_selection(config, args.server)
        payload = _introspect(args.server, profile, modules, base_url)
    except Exception as exc:
        print(json.dumps({"status": "error", "server": args.server, "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 2

    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
