"""Research Assistant Console MCP tool wrappers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from backend.mcp.registry import ModuleRegistry


TOOL_COUNT = 13


def register(registry: "ModuleRegistry") -> None:
    """Register MCP/API-first assistant tools.

    The gateway stays thin and delegates all state transitions to the backend
    Research Assistant API so permissions, approvals and audit events remain
    centralized.
    """

    client = registry.client("research-assistant")

    @registry.mcp.tool(name="assistant_health")
    def assistant_health() -> Any:
        """Check Research Assistant repository/schema health."""

        return client.get("/health")

    @registry.mcp.tool(name="assistant_create_task")
    def assistant_create_task(payload: dict[str, Any]) -> Any:
        """Create a replayable assistant task."""

        return client.post("/tasks", payload)

    @registry.mcp.tool(name="assistant_add_task_event")
    def assistant_add_task_event(task_id: str, payload: dict[str, Any]) -> Any:
        """Append an event to an assistant task."""

        safe_task_id = registry.sanitize(task_id, "task_id")
        return client.post(f"/tasks/{safe_task_id}/events", payload)

    @registry.mcp.tool(name="assistant_chat_turn")
    def assistant_chat_turn(payload: dict[str, Any]) -> Any:
        """Run one LLM-backed assistant conversation turn without executing high-risk actions."""

        return client.post("/chat/turn", payload)

    @registry.mcp.tool(name="assistant_build_prompt_bundle")
    def assistant_build_prompt_bundle(payload: dict[str, Any]) -> Any:
        """Build a tree-selected prompt bundle for a task or conversation turn."""

        return client.post("/prompt-bundles", payload)

    @registry.mcp.tool(name="assistant_list_prompt_nodes")
    def assistant_list_prompt_nodes(
        phase: str | None = None,
        category: str | None = None,
        status: str | None = None,
        search: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Any:
        """List Prompt Tree nodes and their trigger/phase metadata."""

        return client.get(
            "/prompt-nodes",
            params={"phase": phase, "category": category, "status": status, "search": search, "limit": limit, "offset": offset},
        )

    @registry.mcp.tool(name="assistant_create_memory_candidate")
    def assistant_create_memory_candidate(payload: dict[str, Any]) -> Any:
        """Create a draft Memory Ledger item; approval is handled by backend state."""

        body = dict(payload or {})
        body.setdefault("approval_status", "draft")
        return client.post("/memories", body)

    @registry.mcp.tool(name="assistant_build_context_pack")
    def assistant_build_context_pack(payload: dict[str, Any] | None = None) -> Any:
        """Build a deterministic Context Pack from approved ledger memory."""

        return client.post("/context-packs", payload or {})

    @registry.mcp.tool(name="assistant_list_mcp_tools")
    def assistant_list_mcp_tools(
        server_key: str | None = None,
        risk_level: str | None = None,
        search: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Any:
        """List registered MCP tools and risk/preflight metadata."""

        return client.get(
            "/mcp/tools",
            params={"server_key": server_key, "risk_level": risk_level, "search": search, "limit": limit, "offset": offset},
        )

    @registry.mcp.tool(name="assistant_preflight_mcp_tool")
    def assistant_preflight_mcp_tool(payload: dict[str, Any]) -> Any:
        """Run a backend preflight for a registered MCP tool."""

        return client.post("/mcp/preflight", payload)

    @registry.mcp.tool(name="assistant_create_issue_candidate")
    def assistant_create_issue_candidate(payload: dict[str, Any]) -> Any:
        """Create a candidate issue only; formal GitHub issue creation is approval-gated."""

        return client.post("/issue-candidates", payload)

    @registry.mcp.tool(name="assistant_list_approvals")
    def assistant_list_approvals(
        status: str | None = None,
        risk_level: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Any:
        """List pending approval requests."""

        return client.get("/approvals", params={"status": status, "risk_level": risk_level, "limit": limit, "offset": offset})

    @registry.mcp.tool(name="assistant_create_temp_memory")
    def assistant_create_temp_memory(payload: dict[str, Any]) -> Any:
        """Create task-scoped temporary memory for low-cost worker output."""

        return client.post("/temp-memories", payload)

    registry.register_tool_count("research_assistant", TOOL_COUNT)
