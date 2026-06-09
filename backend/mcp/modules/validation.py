"""Validation Center and BUG workflow MCP tools for the unified gateway."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from backend.mcp import legacy_validation_adapter
from backend.mcp.validation_issue_items import compact_issue_item

if TYPE_CHECKING:
    from backend.mcp.registry import ModuleRegistry


TOOL_NAMES = (
    "health",
    "list_plans",
    "get_plan",
    "list_validation_runs",
    "get_validation_run",
    "list_findings",
    "list_bugs",
    "get_bug_agent_context",
    "get_module_quality_summary",
    "start_validation_execution",
    "schedule_validation_from_llm_advice",
    "get_validation_execution_status",
    "get_validation_execution_log",
    "report_bug",
    "mcp_github_issue_list",
    "mcp_github_issue_search",
    "mcp_github_issue_create",
    "assign_bug",
    "update_bug_status",
    "mcp_github_issue_sync_bug",
)
TOOL_COUNT = len(TOOL_NAMES)


def register(registry: "ModuleRegistry") -> None:
    """Register Validation Center and BUG workflow tools on the shared gateway."""

    client = registry.client("validation")

    @registry.mcp.tool(name="health")
    def health() -> Any:
        """Validation Center health probe."""

        return client.get("/health")

    @registry.mcp.tool(name="list_plans")
    def list_plans() -> Any:
        return client.get("/plans")

    @registry.mcp.tool(name="get_plan")
    def get_plan(plan_key: str) -> Any:
        safe = registry.sanitize(plan_key, "plan_key")
        return client.get(f"/plans/{safe}")

    @registry.mcp.tool(name="list_validation_runs")
    def list_validation_runs(
        module: str | None = None,
        level: str | None = None,
        status: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Any:
        return client.get(
            "/runs",
            params={"module": module, "level": level, "status": status, "page": page, "page_size": page_size},
        )

    @registry.mcp.tool(name="get_validation_run")
    def get_validation_run(run_id: str) -> Any:
        safe = registry.sanitize(run_id, "run_id")
        return client.get(f"/runs/{safe}")

    @registry.mcp.tool(name="list_findings")
    def list_findings(
        severity: str | None = None,
        source: str | None = None,
        module: str | None = None,
        status: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Any:
        return client.get(
            "/findings",
            params={
                "severity": severity,
                "source_type": source,
                "module": module,
                "status": status,
                "page": page,
                "page_size": page_size,
            },
        )

    @registry.mcp.tool(name="list_bugs")
    def list_bugs(
        status: str | None = None,
        module: str | None = None,
        severity: str | None = None,
        agent: str | None = None,
        page: int = 1,
        page_size: int = 20,
        compact: bool = True,
    ) -> Any:
        payload = client.get(
            "/bugs",
            params={
                "status": status,
                "module": module,
                "severity": severity,
                "agent": agent,
                "page": page,
                "page_size": page_size,
            },
        )
        if not compact or not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
            return payload
        result = dict(payload)
        result["items"] = [
            compact_issue_item({"source": "bug_json", **item})
            for item in payload["items"]
            if isinstance(item, dict)
        ]
        result["compact"] = True
        return result

    @registry.mcp.tool(name="get_bug_agent_context")
    def get_bug_agent_context(bug_id: str) -> Any:
        safe = registry.sanitize(bug_id, "bug_id")
        return client.get(f"/bugs/{safe}/agent-context")

    @registry.mcp.tool(name="get_module_quality_summary")
    def get_module_quality_summary(module: str | None = None, commit_limit: int = 50) -> Any:
        summary = client.get("/modules/quality-summary", params={"commit_limit": commit_limit, "module": module})
        if module is None or not isinstance(summary, dict):
            return summary
        modules = summary.get("modules") or []
        if not isinstance(modules, list):
            return summary
        result = dict(summary)
        result["modules"] = [item for item in modules if isinstance(item, dict) and str(item.get("module_id") or "") == module]
        result["filter"] = {"module": module}
        return result

    @registry.mcp.tool(name="start_validation_execution")
    def start_validation_execution(
        plan_key: str,
        requested_by: str = "mcp_agent",
        backend_port: int | None = None,
        frontend_port: int | None = None,
        timeout_seconds: int | None = None,
        confirm_text: str | None = None,
        workspace_path: str | None = None,
        expected_branch: str | None = None,
        expected_commit: str | None = None,
    ) -> Any:
        body: dict[str, Any] = {"plan_key": plan_key, "requested_by": requested_by}
        if backend_port is not None:
            body["backend_port"] = backend_port
        if frontend_port is not None:
            body["frontend_port"] = frontend_port
        if timeout_seconds is not None:
            body["timeout_seconds"] = timeout_seconds
        if confirm_text is not None:
            body["confirm_text"] = confirm_text
        if workspace_path is not None:
            body["workspace_path"] = workspace_path
        if expected_branch is not None:
            body["expected_branch"] = expected_branch
        if expected_commit is not None:
            body["expected_commit"] = expected_commit
        return client.post("/executions", json_body=body)

    @registry.mcp.tool(name="schedule_validation_from_llm_advice")
    def schedule_validation_from_llm_advice(
        provider: str = "github_models",
        trigger: str = "manual",
        changed_files: list[str] | None = None,
        recent_failure_modules: list[str] | None = None,
        recent_failure_plan_keys: list[str] | None = None,
        codegraph_freshness: str = "unknown",
        resource_budget_seconds: int = 900,
        workspace_path: str | None = None,
        execute: bool = False,
        requested_by: str = "mcp_agent",
        backend_port: int | None = None,
        frontend_port: int | None = None,
        timeout_seconds: int | None = None,
        expected_branch: str | None = None,
        expected_commit: str | None = None,
        failure_event_ref: str | None = None,
        bug_id: str | None = None,
        github_issue_number: int | None = None,
        github_issue_url: str | None = None,
    ) -> Any:
        body: dict[str, Any] = {
            "provider": provider,
            "trigger": trigger,
            "changed_files": changed_files or [],
            "recent_failure_modules": recent_failure_modules or [],
            "recent_failure_plan_keys": recent_failure_plan_keys or [],
            "codegraph_freshness": codegraph_freshness,
            "resource_budget_seconds": resource_budget_seconds,
            "execute": execute,
            "requested_by": requested_by,
        }
        optional_values = {
            "workspace_path": workspace_path,
            "backend_port": backend_port,
            "frontend_port": frontend_port,
            "timeout_seconds": timeout_seconds,
            "expected_branch": expected_branch,
            "expected_commit": expected_commit,
            "failure_event_ref": failure_event_ref,
            "bug_id": bug_id,
            "github_issue_number": github_issue_number,
            "github_issue_url": github_issue_url,
        }
        body.update({key: value for key, value in optional_values.items() if value is not None})
        return client.post("/llm/schedule", json_body=body)

    @registry.mcp.tool(name="get_validation_execution_status")
    def get_validation_execution_status(execution_id: str) -> Any:
        safe = registry.sanitize(execution_id, "execution_id")
        return client.get(f"/executions/{safe}")

    @registry.mcp.tool(name="get_validation_execution_log")
    def get_validation_execution_log(execution_id: str, tail: int = 100) -> Any:
        if tail < 1 or tail > 2000:
            raise ValueError("tail must be between 1 and 2000")
        safe = registry.sanitize(execution_id, "execution_id")
        return client.get(f"/executions/{safe}/log", params={"tail_lines": tail})

    @registry.mcp.tool(name="report_bug")
    def report_bug(
        title: str,
        severity: str,
        module: str,
        files: list[str],
        reproduce_command: str,
        expected: str,
        actual: str,
        fix_owner: str | None = None,
        related_drawer: str | None = None,
        comments: list[str] | None = None,
    ) -> Any:
        return legacy_validation_adapter.report_bug(
            title=title,
            severity=severity,
            module=module,
            files=files,
            reproduce_command=reproduce_command,
            expected=expected,
            actual=actual,
            fix_owner=fix_owner,
            related_drawer=related_drawer,
            comments=comments,
        )

    @registry.mcp.tool(name="mcp_github_issue_list")
    def mcp_github_issue_list(
        state: str = "open",
        module: str | None = None,
        severity: str | None = None,
        status: str | None = None,
        labels: list[str] | None = None,
        source: str = "local",
        page: int = 1,
        page_size: int = 20,
        compact: bool = True,
    ) -> Any:
        return legacy_validation_adapter.mcp_github_issue_list(
            state=state,
            module=module,
            severity=severity,
            status=status,
            labels=labels,
            source=source,
            page=page,
            page_size=page_size,
            compact=compact,
        )

    @registry.mcp.tool(name="mcp_github_issue_search")
    def mcp_github_issue_search(
        query: str,
        state: str = "all",
        module: str | None = None,
        severity: str | None = None,
        status: str | None = None,
        labels: list[str] | None = None,
        source: str = "local",
        page: int = 1,
        page_size: int = 20,
        compact: bool = True,
    ) -> Any:
        return legacy_validation_adapter.mcp_github_issue_search(
            query=query,
            state=state,
            module=module,
            severity=severity,
            status=status,
            labels=labels,
            source=source,
            page=page,
            page_size=page_size,
            compact=compact,
        )

    @registry.mcp.tool(name="mcp_github_issue_create")
    def mcp_github_issue_create(
        title: str,
        body: str = "",
        severity: str = "P2",
        module: str = "github_issues",
        labels: list[str] | None = None,
        reproduce_command: str | None = None,
        fix_owner: str | None = None,
        create_github: bool = False,
    ) -> Any:
        return legacy_validation_adapter.mcp_github_issue_create(
            title=title,
            body=body,
            severity=severity,
            module=module,
            labels=labels,
            reproduce_command=reproduce_command,
            fix_owner=fix_owner,
            create_github=create_github,
        )

    @registry.mcp.tool(name="assign_bug")
    def assign_bug(
        bug_id: str,
        assigned_agent: str,
        fix_branch: str | None = None,
        actor: str = "mcp_agent",
        note: str | None = None,
        sync_github: bool = False,
    ) -> Any:
        return legacy_validation_adapter.assign_bug(
            bug_id=bug_id,
            assigned_agent=assigned_agent,
            fix_branch=fix_branch,
            actor=actor,
            note=note,
            sync_github=sync_github,
        )

    @registry.mcp.tool(name="update_bug_status")
    def update_bug_status(
        bug_id: str,
        status: str,
        actor: str = "mcp_agent",
        note: str | None = None,
        fix_branch: str | None = None,
        fix_commit: str | None = None,
        verification_run_id: str | None = None,
        sync_github: bool = False,
    ) -> Any:
        return legacy_validation_adapter.update_bug_status(
            bug_id=bug_id,
            status=status,
            actor=actor,
            note=note,
            fix_branch=fix_branch,
            fix_commit=fix_commit,
            verification_run_id=verification_run_id,
            sync_github=sync_github,
        )

    @registry.mcp.tool(name="mcp_github_issue_sync_bug")
    def mcp_github_issue_sync_bug(
        bug_id: str,
        direction: str = "json-to-github",
        apply: bool = False,
        actor: str = "mcp_agent",
    ) -> Any:
        return legacy_validation_adapter.mcp_github_issue_sync_bug(
            bug_id=bug_id,
            direction=direction,
            apply=apply,
            actor=actor,
        )

    registry.register_tool_count("validation", TOOL_COUNT)
