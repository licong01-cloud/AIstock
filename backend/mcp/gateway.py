"""Dynamic loader for the phased AIstock MCP gateway."""

from __future__ import annotations

import importlib
import os
from dataclasses import asdict
from typing import Any

import httpx

try:
    from mcp.server.fastmcp import FastMCP
except ImportError as exc:  # pragma: no cover
    raise SystemExit("mcp package is required: pip install mcp") from exc

from .common import assert_loopback_url
from .profiles import INITIAL_PROFILES, resolve_modules
from .registry import ModuleRegistry
from .tool_manifest import TOOL_MANIFEST, legacy_tool_count, manifest_for_modules, platform_tool_count, validate_manifest

DEFAULT_BASE_URL = "http://127.0.0.1:8001/api/v1"
DEFAULT_SERVER_NAME = "aistock-gateway"


def create_gateway(
    *,
    profile: str | None = "lite",
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
        server_name=server_name,
        profile="lite" if profile in {None, ""} and modules is None else profile,
        selected_modules=tuple(selected_modules),
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
    profile: str | None = "lite",
    modules: str | list[str] | tuple[str, ...] | None = None,
    base_url: str | None = None,
    transport_name: str = "stdio",
) -> None:
    mcp, _registry = create_gateway(profile=profile, modules=modules, base_url=base_url)
    mcp.run(transport=transport_name)


def list_profiles_payload() -> dict[str, Any]:
    """Return profile metadata without importing all gateway modules."""

    return {
        "default_profile": "lite",
        "profiles": [
            {
                "profile": name,
                "modules": modules,
                "tool_count": len(manifest_for_modules(modules)),
                "default_recommended": name == "lite",
            }
            for name, modules in sorted(INITIAL_PROFILES.items())
        ],
    }


def list_tools_payload(
    *,
    profile: str | None = "lite",
    modules: str | list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    """Return static tool metadata for a profile/module selection."""

    selected_modules = resolve_modules(profile=profile, modules=modules)
    entries = manifest_for_modules(selected_modules)
    return {
        "profile": "lite" if profile in {None, ""} and modules is None else profile,
        "modules": selected_modules,
        "tool_count": len(entries),
        "legacy_tool_count": sum(1 for entry in entries if entry.module != "catalog"),
        "platform_tool_count": sum(1 for entry in entries if entry.module == "catalog"),
        "items": [asdict(entry) for entry in entries],
    }


def self_check_payload(
    *,
    profile: str | None = "lite",
    modules: str | list[str] | tuple[str, ...] | None = None,
    base_url: str | None = None,
    env_name: str = "AISTOCK_MCP_BASE_URL",
    check_backend: bool = False,
) -> dict[str, Any]:
    """Return a structured gateway readiness check."""

    errors: list[str] = []
    warnings: list[str] = []
    base = base_url or os.environ.get(env_name, DEFAULT_BASE_URL)
    try:
        normalized_base = assert_loopback_url(base, env_name=env_name)
    except ValueError as exc:
        normalized_base = base
        errors.append(str(exc))

    try:
        selected_modules = resolve_modules(profile=profile, modules=modules)
    except ValueError as exc:
        selected_modules = []
        errors.append(str(exc))

    manifest_errors = validate_manifest()
    errors.extend(manifest_errors)
    if profile == "full":
        warnings.append("full profile is for controlled validation/debug only and must not be the default client profile")

    backend_status: dict[str, Any] = {"checked": False}
    if check_backend and not errors:
        try:
            client = httpx.Client(base_url=normalized_base, timeout=3.0, trust_env=False)
            try:
                response = client.get("/health")
            finally:
                client.close()
            backend_status = {
                "checked": True,
                "status_code": response.status_code,
                "reachable": response.status_code < 500,
            }
        except Exception as exc:  # pragma: no cover - depends on local runtime
            backend_status = {"checked": True, "reachable": False, "error": str(exc)}

    return {
        "status": "fail" if errors else "pass",
        "profile": "lite" if profile in {None, ""} and modules is None else profile,
        "modules": selected_modules,
        "base_url": normalized_base,
        "tool_count": len(manifest_for_modules(selected_modules)),
        "manifest_tool_count": len(TOOL_MANIFEST),
        "legacy_tool_count": legacy_tool_count(),
        "platform_tool_count": platform_tool_count(),
        "errors": errors,
        "warnings": warnings,
        "backend": backend_status,
    }
