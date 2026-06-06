"""Registration context passed to AIstock MCP modules."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import httpx

from .common import AIstockApiClient, confirm, join_url_path, sanitize_identifier


@dataclass
class ModuleRegistry:
    """Narrow module registration context, not a backend service locator."""

    mcp: Any
    base_url: str
    env_name: str = "AISTOCK_MCP_BASE_URL"
    server_name: str = "aistock-gateway"
    profile: str | None = None
    selected_modules: tuple[str, ...] = ()
    timeout: float | None = None
    unwrap_data: bool = False
    transport: httpx.BaseTransport | None = None
    _tool_counts: dict[str, int] = field(default_factory=dict)

    def client(self, path_prefix: str = "") -> AIstockApiClient:
        return AIstockApiClient(
            join_url_path(self.base_url, path_prefix),
            env_name=self.env_name,
            timeout=self.timeout,
            unwrap_data=self.unwrap_data,
            transport=self.transport,
        )

    def sanitize(self, value: Any, name: str) -> str:
        return sanitize_identifier(value, name)

    def confirm(self, actual: str | None, expected: str, field: str) -> None:
        confirm(actual, expected, field)

    def register_tool_count(self, module_name: str, count: int) -> None:
        if count < 0:
            raise ValueError(f"tool count for {module_name!r} must be >= 0; got {count}")
        self._tool_counts[module_name] = int(count)

    def tool_count(self, module_name: str) -> int:
        return self._tool_counts.get(module_name, 0)

    def total_tool_count(self) -> int:
        return sum(self._tool_counts.values())

    @property
    def tool_counts(self) -> dict[str, int]:
        return dict(self._tool_counts)
