"""Dynamic loader for the phased AIstock MCP gateway."""

from __future__ import annotations

import importlib
import os

import httpx

try:
    from mcp.server.fastmcp import FastMCP
except ImportError as exc:  # pragma: no cover
    raise SystemExit("mcp package is required: pip install mcp") from exc

from .profiles import resolve_modules
from .registry import ModuleRegistry

DEFAULT_BASE_URL = "http://127.0.0.1:8001/api/v1"
DEFAULT_SERVER_NAME = "aistock-research"


def create_gateway(
    *,
    profile: str | None = "research",
    modules: str | list[str] | tuple[str, ...] | None = None,
    base_url: str | None = None,
    env_name: str = "AISTOCK_MCP_BASE_URL",
    timeout: float | None = None,
    unwrap_data: bool = False,
    transport: httpx.BaseTransport | None = None,
    server_name: str = DEFAULT_SERVER_NAME,
) -> tuple[FastMCP, ModuleRegistry]:
    """Create a FastMCP instance and load requested MCP modules.

    Gateway scope is intentionally limited to profile/module resolution,
    dynamic imports, and FastMCP construction. Business services stay behind
    loopback backend APIs and are not imported here.
    """

    selected_modules = resolve_modules(profile=profile, modules=modules)
    mcp = FastMCP(server_name)
    registry = ModuleRegistry(
        mcp=mcp,
        base_url=base_url or os.environ.get(env_name, DEFAULT_BASE_URL),
        env_name=env_name,
        timeout=timeout,
        unwrap_data=unwrap_data,
        transport=transport,
    )

    for module_name in selected_modules:
        module = importlib.import_module(f"backend.mcp.modules.{module_name}")
        register = getattr(module, "register", None)
        if register is None:
            raise RuntimeError(f"backend.mcp.modules.{module_name} does not define register(registry)")
        register(registry)

    return mcp, registry


def run_gateway(
    *,
    profile: str | None = "research",
    modules: str | list[str] | tuple[str, ...] | None = None,
    base_url: str | None = None,
    transport_name: str = "stdio",
) -> None:
    mcp, _registry = create_gateway(profile=profile, modules=modules, base_url=base_url)
    mcp.run(transport=transport_name)
