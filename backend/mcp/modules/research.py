"""Research Pipeline MCP tool wrappers.

This module intentionally stays thin: tools validate only path fragments and
confirmation tokens, then call the loopback Research Pipeline backend API.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from backend.mcp.registry import ModuleRegistry


TOOL_COUNT = 12
RESEARCH_RUN_STAGE_CONFIRM = "RESEARCH_RUN_STAGE"
RESEARCH_RETRY_STAGE_CONFIRM = "RESEARCH_RETRY_STAGE"
RESEARCH_PROMOTE_CONFIRM = "RESEARCH_PROMOTE"


def register(registry: ModuleRegistry) -> None:
    """Register Research Pipeline tools on the shared MCP gateway."""

    client = registry.client("research-pipeline")

    @registry.mcp.tool(name="research_create_experiment")
    def research_create_experiment(payload: dict[str, Any]) -> Any:
        """Create a Research Pipeline experiment."""

        return client.post("/experiments", payload)

    @registry.mcp.tool(name="research_list_experiments")
    def research_list_experiments(
        status: str | None = None,
        pipeline_type: str | None = None,
        search: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Any:
        """List Research Pipeline experiments."""

        return client.get(
            "/experiments",
            params={
                "status": status,
                "pipeline_type": pipeline_type,
                "search": search,
                "limit": limit,
                "offset": offset,
            },
        )

    @registry.mcp.tool(name="research_get_experiment")
    def research_get_experiment(experiment_id: str) -> Any:
        """Get a Research Pipeline experiment detail record."""

        safe_experiment_id = registry.sanitize(experiment_id, "experiment_id")
        return client.get(f"/experiments/{safe_experiment_id}")

    @registry.mcp.tool(name="research_run_stage")
    def research_run_stage(
        experiment_id: str,
        stage_name: str,
        payload: dict[str, Any] | None = None,
        confirm: str | None = None,
    ) -> Any:
        """Run a stage after explicit confirmation."""

        registry.confirm(confirm, RESEARCH_RUN_STAGE_CONFIRM, "confirm")
        safe_experiment_id = registry.sanitize(experiment_id, "experiment_id")
        safe_stage_name = registry.sanitize(stage_name, "stage_name")
        body = dict(payload or {})
        body.setdefault("confirm", RESEARCH_RUN_STAGE_CONFIRM)
        return client.post(f"/experiments/{safe_experiment_id}/stages/{safe_stage_name}/run", body)

    @registry.mcp.tool(name="research_retry_stage")
    def research_retry_stage(
        experiment_id: str,
        stage_name: str,
        payload: dict[str, Any] | None = None,
        confirm: str | None = None,
    ) -> Any:
        """Retry a stage after explicit confirmation."""

        registry.confirm(confirm, RESEARCH_RETRY_STAGE_CONFIRM, "confirm")
        safe_experiment_id = registry.sanitize(experiment_id, "experiment_id")
        safe_stage_name = registry.sanitize(stage_name, "stage_name")
        body = dict(payload or {})
        body.setdefault("confirm", RESEARCH_RETRY_STAGE_CONFIRM)
        return client.post(f"/experiments/{safe_experiment_id}/stages/{safe_stage_name}/retry", body)

    @registry.mcp.tool(name="research_get_stage_result")
    def research_get_stage_result(experiment_id: str, stage_name: str) -> Any:
        """Get a stage status and attempt history."""

        safe_experiment_id = registry.sanitize(experiment_id, "experiment_id")
        safe_stage_name = registry.sanitize(stage_name, "stage_name")
        return client.get(f"/experiments/{safe_experiment_id}/stages/{safe_stage_name}")

    @registry.mcp.tool(name="research_compare_baseline")
    def research_compare_baseline(experiment_id: str, payload: dict[str, Any] | None = None) -> Any:
        """Run or record a baseline comparison for an experiment."""

        safe_experiment_id = registry.sanitize(experiment_id, "experiment_id")
        return client.post(f"/experiments/{safe_experiment_id}/compare", payload or {})

    @registry.mcp.tool(name="research_list_artifact_refs")
    def research_list_artifact_refs(
        experiment_id: str,
        domain_type: str | None = None,
        status: str | None = None,
        limit: int = 100,
    ) -> Any:
        """List artifact references without claiming asset ownership."""

        safe_experiment_id = registry.sanitize(experiment_id, "experiment_id")
        return client.get(
            f"/experiments/{safe_experiment_id}/artifact-refs",
            params={"domain_type": domain_type, "status": status, "limit": limit},
        )

    @registry.mcp.tool(name="research_get_pipeline_types")
    def research_get_pipeline_types() -> Any:
        """List supported research pipeline types and defaults."""

        return client.get("/pipeline-types")

    @registry.mcp.tool(name="research_create_issue")
    def research_create_issue(payload: dict[str, Any]) -> Any:
        """Create an issue or local bug record from a research finding."""

        return client.post("/issues", payload)

    @registry.mcp.tool(name="research_promote")
    def research_promote(
        experiment_id: str,
        issue_url: str,
        payload: dict[str, Any] | None = None,
        confirm: str | None = None,
    ) -> Any:
        """Request promotion for a validated experiment; no production write occurs here."""

        registry.confirm(confirm, RESEARCH_PROMOTE_CONFIRM, "confirm")
        if not isinstance(issue_url, str) or not issue_url.strip():
            raise ValueError(f"issue_url must be a non-empty string; got {issue_url!r}")
        safe_experiment_id = registry.sanitize(experiment_id, "experiment_id")
        body = dict(payload or {})
        body.setdefault("issue_url", issue_url)
        body.setdefault("confirm", RESEARCH_PROMOTE_CONFIRM)
        return client.post(f"/experiments/{safe_experiment_id}/promote", body)

    @registry.mcp.tool(name="research_reject")
    def research_reject(experiment_id: str, payload: dict[str, Any] | None = None) -> Any:
        """Reject a Research Pipeline experiment."""

        safe_experiment_id = registry.sanitize(experiment_id, "experiment_id")
        return client.post(f"/experiments/{safe_experiment_id}/reject", payload or {})

    registry.register_tool_count("research", TOOL_COUNT)
