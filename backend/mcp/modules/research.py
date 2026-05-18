"""Phase 1 placeholder module for the future Research Pipeline MCP tools."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from backend.mcp.registry import ModuleRegistry


TOOL_COUNT = 0


def register(registry: ModuleRegistry) -> None:
    """Register no tools until Phase 2 wires Research Pipeline endpoints."""

    registry.register_tool_count("research", TOOL_COUNT)
