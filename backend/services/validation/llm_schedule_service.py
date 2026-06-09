from __future__ import annotations

from typing import Any

from backend.services.validation.execution_runner import ValidationExecutionRunner, ValidationRunnerError
from backend.services.validation.plan_catalog import (
    ValidationCatalogError,
    ValidationPlanCatalog,
    load_allowed_command_keys_from_source,
)
from scripts import llm_provider_adapter

SCHEDULE_SCHEMA_VERSION = "aistock_scheduler_decision_v1"


class ValidationLlmScheduleError(ValueError):
    """Raised when a validation LLM schedule request fails deterministic gates."""


class ValidationLlmScheduleService:
    """Gate LLM plan suggestions before optionally enqueueing controlled validation runs."""

    def __init__(
        self,
        *,
        plan_catalog: ValidationPlanCatalog | None = None,
        execution_runner: ValidationExecutionRunner | None = None,
    ) -> None:
        self.plan_catalog = plan_catalog or ValidationPlanCatalog()
        self.execution_runner = execution_runner or ValidationExecutionRunner(plan_catalog=self.plan_catalog)

    def _catalog_kwargs_for_advice(self, workspace_path: str | None) -> dict[str, Any]:
        root = self.execution_runner.repo_root
        kwargs: dict[str, Any] = {
            "root": root,
            "catalog_path": self.plan_catalog.catalog_path,
            "allowed_command_keys": self.plan_catalog.allowed_command_keys,
        }
        if not workspace_path:
            return kwargs
        try:
            workspace = self.execution_runner._validate_workspace_path(workspace_path, root)
        except ValidationRunnerError:
            # Dry-run should stay diagnostic-only; execute mode is blocked later.
            return kwargs
        catalog_path = workspace / "tests" / "aistock_validation" / "catalog" / "test_plans.yaml"
        allowlist_source = workspace / "backend" / "services" / "validation" / "plan_catalog.py"
        if not catalog_path.exists() or not allowlist_source.exists():
            return kwargs
        try:
            kwargs["catalog_path"] = catalog_path
            kwargs["allowed_command_keys"] = load_allowed_command_keys_from_source(allowlist_source)
        except ValidationCatalogError as exc:
            raise ValidationLlmScheduleError(str(exc)) from exc
        return kwargs

    def advise_plans(
        self,
        *,
        provider: str = "github_models",
        changed_files: list[str] | None = None,
        recent_failure_modules: list[str] | None = None,
        recent_failure_plan_keys: list[str] | None = None,
        codegraph_freshness: str = "unknown",
        resource_budget_seconds: int = 900,
        workspace_path: str | None = None,
    ) -> dict[str, Any]:
        try:
            return llm_provider_adapter.build_nightly_scheduler_advice(
                provider,
                llm_provider_adapter.load_config(),
                changed_files=changed_files,
                recent_failure_modules=recent_failure_modules,
                recent_failure_plan_keys=recent_failure_plan_keys,
                codegraph_freshness=codegraph_freshness,
                resource_budget_seconds=resource_budget_seconds,
                workspace_path=workspace_path,
                **self._catalog_kwargs_for_advice(workspace_path),
            )
        except llm_provider_adapter.ProviderAdapterError as exc:
            raise ValidationLlmScheduleError(str(exc)) from exc

    def schedule(
        self,
        *,
        provider: str = "github_models",
        changed_files: list[str] | None = None,
        recent_failure_modules: list[str] | None = None,
        recent_failure_plan_keys: list[str] | None = None,
        codegraph_freshness: str = "unknown",
        resource_budget_seconds: int = 900,
        workspace_path: str | None = None,
        execute: bool = False,
        requested_by: str = "llm_schedule_gate",
        backend_port: int | None = None,
        frontend_port: int | None = None,
        timeout_seconds: int | None = None,
        expected_branch: str | None = None,
        expected_commit: str | None = None,
        trigger: str = "manual",
        failure_event_ref: str | None = None,
        bug_id: str | None = None,
        github_issue_number: int | None = None,
        github_issue_url: str | None = None,
    ) -> dict[str, Any]:
        advice = self.advise_plans(
            provider=provider,
            changed_files=changed_files,
            recent_failure_modules=recent_failure_modules,
            recent_failure_plan_keys=recent_failure_plan_keys,
            codegraph_freshness=codegraph_freshness,
            resource_budget_seconds=resource_budget_seconds,
            workspace_path=workspace_path,
        )
        gate = advice["deterministic_gate"]
        if execute and gate.get("workflow_gate") not in {"ready", "warning"}:
            raise ValidationLlmScheduleError(f"LLM schedule gate is not ready: {gate.get('workflow_gate')}")
        runs: list[dict[str, Any]] = []
        run_evidence_links: list[dict[str, Any]] = []
        if execute:
            for item in advice.get("queue") or []:
                if not item.get("allowed"):
                    continue
                try:
                    job = self.execution_runner.start_job(
                        plan_key=str(item["plan_key"]),
                        requested_by=requested_by,
                        backend_port=backend_port,
                        frontend_port=frontend_port,
                        timeout_seconds=timeout_seconds,
                        workspace_path=workspace_path,
                        expected_branch=expected_branch,
                        expected_commit=expected_commit,
                    )
                    runs.append(job)
                    run_evidence_links.append(
                        {
                            "job_id": job.get("job_id"),
                            "plan_key": item.get("plan_key"),
                            "failure_event_ref": failure_event_ref,
                            "bug_id": bug_id,
                            "github_issue_number": github_issue_number,
                            "github_issue_url": github_issue_url,
                            "evidence_path": job.get("evidence_path"),
                            "run_record_path": (job.get("archive") or {}).get("run_record_path")
                            if isinstance(job.get("archive"), dict)
                            else None,
                        }
                    )
                except ValidationRunnerError as exc:
                    raise ValidationLlmScheduleError(str(exc)) from exc
        return {
            "schema_version": SCHEDULE_SCHEMA_VERSION,
            "trigger": trigger,
            "provider": advice["provider"],
            "model": advice["model"],
            "execute": execute,
            "workflow_gate": gate["workflow_gate"],
            "input_refs": {
                "changed_files": advice["changed_files"],
                "recent_failure_modules": advice["recent_failures"]["modules"],
                "recent_failure_plan_keys": advice["recent_failures"]["plan_keys"],
                "failure_event_ref": failure_event_ref,
                "bug_id": bug_id,
                "github_issue_number": github_issue_number,
                "github_issue_url": github_issue_url,
            },
            "budget": {"resource_budget_seconds": advice["resource_budget_seconds"]},
            "queue": advice["queue"],
            "allowed_plan_count": gate["allowed_plan_count"],
            "deferred_plan_count": gate["deferred_plan_count"],
            "runs": runs,
            "run_count": len(runs),
            "run_evidence_links": run_evidence_links,
            "llm_invocation_evidence": advice["llm_invocation_evidence"],
            "test_plan_advice_gate": advice["test_plan_advice_gate"],
            "workspace_gate": advice["workspace_gate"],
            "gate": {
                "allowed_to_schedule_validation": execute and len(runs) > 0,
                "blocking_reasons": [item["deferred_reason"] for item in advice["queue"] if item.get("deferred_reason")],
            },
            "production_gates": gate["production_gates"],
            "shell_commands_allowed": False,
            "production_actions_allowed": False,
        }
