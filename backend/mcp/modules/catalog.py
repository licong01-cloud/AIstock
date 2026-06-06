"""Gateway catalog and preflight tools.

The catalog module is intentionally self-contained and data-only. It exposes
profile/tool metadata without importing business-facing MCP modules or starting
AIstock backend services.
"""

from __future__ import annotations

from dataclasses import asdict
from typing import TYPE_CHECKING, Any

from backend.mcp.profiles import INITIAL_PROFILES, resolve_modules
from backend.mcp.tool_manifest import (
    MODULE_TOOL_NAMES,
    TOOL_MANIFEST,
    TOOL_MANIFEST_BY_NAME,
    ToolManifestEntry,
    legacy_tool_count,
    manifest_for_modules,
    platform_tool_count,
    validate_manifest,
)

if TYPE_CHECKING:
    from backend.mcp.registry import ModuleRegistry


TOOL_NAMES = (
    "mcp_gateway_health",
    "mcp_gateway_list_profiles",
    "mcp_gateway_list_modules",
    "mcp_gateway_list_tools",
    "mcp_gateway_search_tools",
    "mcp_gateway_preflight_tool",
)
TOOL_COUNT = len(TOOL_NAMES)


def _entry_payload(entry: ToolManifestEntry) -> dict[str, Any]:
    return asdict(entry)


def _filter_entries(
    *,
    profile: str | None = None,
    module: str | None = None,
    risk_level: str | None = None,
    search: str | None = None,
) -> list[ToolManifestEntry]:
    if profile:
        modules = resolve_modules(profile=profile)
        entries = list(manifest_for_modules(modules))
    else:
        entries = list(TOOL_MANIFEST)
    if module:
        entries = [entry for entry in entries if entry.module == module]
    if risk_level:
        entries = [entry for entry in entries if entry.risk_level == risk_level]
    if search:
        needle = search.strip().lower()
        entries = [
            entry
            for entry in entries
            if needle in entry.tool_name.lower()
            or needle in entry.module.lower()
            or needle in entry.backend_endpoint.lower()
            or any(needle in tag.lower() for tag in entry.profile_tags)
        ]
    return entries


def register(registry: "ModuleRegistry") -> None:
    """Register data-only catalog tools on the shared gateway."""

    @registry.mcp.tool(name="mcp_gateway_health")
    def mcp_gateway_health() -> dict[str, Any]:
        """Return static gateway/manifest health without touching backend services."""

        manifest_errors = validate_manifest()
        return {
            "status": "pass" if not manifest_errors else "fail",
            "server_name": registry.server_name,
            "profile": registry.profile,
            "modules": registry.selected_modules,
            "tool_counts": registry.tool_counts,
            "registered_tool_count": registry.total_tool_count(),
            "manifest_tool_count": len(TOOL_MANIFEST),
            "legacy_tool_count": legacy_tool_count(),
            "platform_tool_count": platform_tool_count(),
            "manifest_errors": manifest_errors,
        }

    @registry.mcp.tool(name="mcp_gateway_list_profiles")
    def mcp_gateway_list_profiles() -> dict[str, Any]:
        """List configured gateway profiles and their module sets."""

        return {
            "profiles": [
                {
                    "profile": name,
                    "modules": modules,
                    "tool_count": len(manifest_for_modules(modules)),
                    "default_recommended": name == "lite",
                }
                for name, modules in sorted(INITIAL_PROFILES.items())
            ]
        }

    @registry.mcp.tool(name="mcp_gateway_list_modules")
    def mcp_gateway_list_modules() -> dict[str, Any]:
        """List MCP modules and static tool counts."""

        return {
            "modules": [
                {
                    "module": module,
                    "tool_count": len(tool_names),
                    "profile_tags": sorted({tag for entry in manifest_for_modules([module]) for tag in entry.profile_tags}),
                }
                for module, tool_names in sorted(MODULE_TOOL_NAMES.items())
            ]
        }

    @registry.mcp.tool(name="mcp_gateway_list_tools")
    def mcp_gateway_list_tools(
        profile: str | None = None,
        module: str | None = None,
        risk_level: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, Any]:
        """List tools from the static manifest."""

        entries = _filter_entries(profile=profile, module=module, risk_level=risk_level)
        start = max(int(offset), 0)
        end = start + max(int(limit), 0)
        page = entries[start:end]
        return {
            "total": len(entries),
            "limit": limit,
            "offset": offset,
            "items": [_entry_payload(entry) for entry in page],
        }

    @registry.mcp.tool(name="mcp_gateway_search_tools")
    def mcp_gateway_search_tools(
        query: str,
        profile: str | None = None,
        risk_level: str | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        """Search tools by name, module, profile tag, or endpoint hint."""

        if not isinstance(query, str) or not query.strip():
            raise ValueError("query must be a non-empty string")
        entries = _filter_entries(profile=profile, risk_level=risk_level, search=query)
        return {
            "query": query,
            "total": len(entries),
            "items": [_entry_payload(entry) for entry in entries[: max(int(limit), 0)]],
        }

    @registry.mcp.tool(name="mcp_gateway_preflight_tool")
    def mcp_gateway_preflight_tool(tool_name: str) -> dict[str, Any]:
        """Return static risk and confirmation metadata for one tool."""

        entry = TOOL_MANIFEST_BY_NAME.get(tool_name)
        if entry is None:
            raise ValueError(f"unknown MCP tool: {tool_name!r}")
        return {
            "tool": _entry_payload(entry),
            "preflight_required": entry.requires_confirmation or entry.risk_level not in {"read_only", "catalog"},
            "requires_confirmation": entry.requires_confirmation,
            "allowed_without_backend": entry.module == "catalog",
            "recommended_profile_tags": entry.profile_tags,
        }

    registry.register_tool_count("catalog", TOOL_COUNT)
