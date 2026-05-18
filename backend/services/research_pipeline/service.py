"""Research Pipeline backend orchestration service."""

from __future__ import annotations

from typing import Any, Protocol

from .models import (
    PIPELINE_TYPES,
    ArtifactRefRecord,
    ComparisonRecord,
    ExperimentRecord,
    ExternalRunLinkRecord,
    PipelineEventRecord,
    StageAttemptRecord,
    StagePlanRecord,
    sanitize_identifier,
    utc_now,
)
from .repository import ResearchPipelineRepository


class ResearchPipelineError(ValueError):
    """Base domain error for Research Pipeline service failures."""


class ResearchPipelineNotFoundError(ResearchPipelineError):
    """Raised when an experiment or stage does not exist."""


class ResearchPipelineConflictError(ResearchPipelineError):
    """Raised when a requested transition conflicts with current state."""


class ResearchPipelineRepositoryProtocol(Protocol):
    def create_experiment(self, record: ExperimentRecord) -> dict[str, Any]: ...
    def list_experiments(self, **kwargs: Any) -> list[dict[str, Any]]: ...
    def get_experiment(self, experiment_id: str) -> dict[str, Any] | None: ...
    def update_experiment(self, experiment_id: str, updates: dict[str, Any]) -> dict[str, Any]: ...
    def create_stage_plan(self, record: StagePlanRecord) -> dict[str, Any]: ...
    def list_stage_plans(self, experiment_id: str) -> list[dict[str, Any]]: ...
    def get_stage_plan(self, experiment_id: str, stage_name: str) -> dict[str, Any] | None: ...
    def update_stage_plan(self, stage_id: str, updates: dict[str, Any]) -> dict[str, Any]: ...
    def create_stage_attempt(self, record: StageAttemptRecord) -> dict[str, Any]: ...
    def list_stage_attempts(self, experiment_id: str, stage_name: str | None = None) -> list[dict[str, Any]]: ...
    def next_attempt_no(self, experiment_id: str, stage_name: str) -> int: ...
    def create_external_run_link(self, record: ExternalRunLinkRecord) -> dict[str, Any]: ...
    def list_external_run_links(self, experiment_id: str) -> list[dict[str, Any]]: ...
    def create_artifact_ref(self, record: ArtifactRefRecord) -> dict[str, Any]: ...
    def list_artifact_refs(self, experiment_id: str, **kwargs: Any) -> list[dict[str, Any]]: ...
    def create_comparison(self, record: ComparisonRecord) -> dict[str, Any]: ...
    def list_comparisons(self, experiment_id: str) -> list[dict[str, Any]]: ...
    def create_pipeline_event(self, record: PipelineEventRecord) -> dict[str, Any]: ...
    def list_pipeline_events(self, experiment_id: str) -> list[dict[str, Any]]: ...


class ResearchPipelineService:
    def __init__(self, repository: ResearchPipelineRepositoryProtocol | None = None) -> None:
        self._repo = repository or ResearchPipelineRepository()

    def get_pipeline_types(self) -> dict[str, Any]:
        return {name: dict(config) for name, config in PIPELINE_TYPES.items()}

    def create_experiment(
        self,
        *,
        pipeline_type: str,
        title: str,
        description: str | None = None,
        criteria_json: dict[str, Any] | None = None,
        baseline_ref_json: dict[str, Any] | None = None,
        metadata_json: dict[str, Any] | None = None,
        created_by: str = "codex",
        stages: list[dict[str, Any] | str] | None = None,
    ) -> dict[str, Any]:
        pipeline_config = PIPELINE_TYPES.get(pipeline_type)
        if pipeline_config is None:
            raise ResearchPipelineError(f"unsupported pipeline_type: {pipeline_type}")
        default_criteria = dict(pipeline_config.get("default_criteria") or {})
        criteria = {**default_criteria, **dict(criteria_json or {})}
        experiment = self._repo.create_experiment(
            ExperimentRecord(
                pipeline_type=pipeline_type,
                title=title,
                description=description,
                criteria_json=criteria,
                baseline_ref_json=baseline_ref_json or {},
                metadata_json=metadata_json or {},
                created_by=created_by,
            )
        )
        planned_stages = stages or list(pipeline_config["stages"])
        for order, stage in enumerate(planned_stages, start=1):
            if isinstance(stage, str):
                stage_name = stage
                planned_config: dict[str, Any] = {}
            else:
                stage_name = str(stage.get("stage_name") or stage.get("name") or "")
                planned_config = dict(stage.get("planned_config_json") or stage.get("config") or {})
            self._repo.create_stage_plan(
                StagePlanRecord(
                    experiment_id=str(experiment["experiment_id"]),
                    stage_name=stage_name,
                    stage_order=order,
                    planned_config_json=planned_config,
                )
            )
        self._record_event(
            str(experiment["experiment_id"]),
            event_type="experiment_created",
            message=f"Research experiment created: {title}",
            payload={"pipeline_type": pipeline_type},
            created_by=created_by,
        )
        return self.get_experiment(str(experiment["experiment_id"]))

    def list_experiments(
        self,
        *,
        status: str | None = None,
        pipeline_type: str | None = None,
        search: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        return self._repo.list_experiments(
            status=status,
            pipeline_type=pipeline_type,
            search=search,
            limit=limit,
            offset=offset,
        )

    def get_experiment(self, experiment_id: str) -> dict[str, Any]:
        experiment_id = sanitize_identifier(experiment_id, "experiment_id")
        experiment = self._repo.get_experiment(experiment_id)
        if not experiment:
            raise ResearchPipelineNotFoundError(f"experiment not found: {experiment_id}")
        return {
            **experiment,
            "stages": self._repo.list_stage_plans(experiment_id),
            "attempts": self._repo.list_stage_attempts(experiment_id),
            "artifact_refs": self._repo.list_artifact_refs(experiment_id),
            "external_run_links": self._repo.list_external_run_links(experiment_id),
            "comparisons": self._repo.list_comparisons(experiment_id),
            "events": self._repo.list_pipeline_events(experiment_id),
        }

    def run_stage(self, experiment_id: str, stage_name: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        return self._start_stage_attempt(
            experiment_id=experiment_id,
            stage_name=stage_name,
            payload=payload or {},
            event_type="stage_run_requested",
        )

    def retry_stage(self, experiment_id: str, stage_name: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        return self._start_stage_attempt(
            experiment_id=experiment_id,
            stage_name=stage_name,
            payload=payload or {},
            event_type="stage_retry_requested",
        )

    def get_stage_result(self, experiment_id: str, stage_name: str) -> dict[str, Any]:
        experiment_id, stage_name = self._safe_stage_key(experiment_id, stage_name)
        stage = self._require_stage(experiment_id, stage_name)
        return {
            "stage": stage,
            "attempts": self._repo.list_stage_attempts(experiment_id, stage_name),
        }

    def compare_baseline(self, experiment_id: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        experiment_id = sanitize_identifier(experiment_id, "experiment_id")
        self._require_experiment(experiment_id)
        data = dict(payload or {})
        verdict = str(data.get("verdict") or "inconclusive")
        comparison = self._repo.create_comparison(
            ComparisonRecord(
                experiment_id=experiment_id,
                stage_attempt_id=data.get("stage_attempt_id"),
                baseline_ref_json=data.get("baseline_ref_json") or data.get("baseline") or {},
                candidate_ref_json=data.get("candidate_ref_json") or data.get("candidate") or {},
                metrics_json=data.get("metrics_json") or data.get("metrics") or {},
                criteria_json=data.get("criteria_json") or {},
                verdict=verdict,  # type: ignore[arg-type]
                reason_md=data.get("reason_md") or data.get("reason"),
                created_by=data.get("created_by") or "codex",
            )
        )
        status_updates = {
            "pass": {"status": "validated", "validated_at": utc_now()},
            "fail": {"status": "stage_failed"},
            "blocked": {"status": "blocked", "blocked_at": utc_now(), "blocked_reason": data.get("reason_md") or data.get("reason")},
        }.get(verdict)
        if status_updates:
            self._repo.update_experiment(experiment_id, status_updates)
        self._record_event(
            experiment_id,
            event_type="comparison_recorded",
            message=f"Comparison recorded with verdict={verdict}",
            payload={"comparison_id": comparison["comparison_id"], "verdict": verdict},
            stage_attempt_id=data.get("stage_attempt_id"),
            created_by=data.get("created_by") or "codex",
        )
        return comparison

    def list_artifact_refs(
        self,
        experiment_id: str,
        *,
        domain_type: str | None = None,
        status: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        experiment_id = sanitize_identifier(experiment_id, "experiment_id")
        self._require_experiment(experiment_id)
        return self._repo.list_artifact_refs(experiment_id, domain_type=domain_type, status=status, limit=limit)

    def record_artifact_ref(self, experiment_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        experiment_id = sanitize_identifier(experiment_id, "experiment_id")
        self._require_experiment(experiment_id)
        return self._repo.create_artifact_ref(ArtifactRefRecord(experiment_id=experiment_id, **payload))

    def promote(self, experiment_id: str, *, issue_url: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        experiment_id = sanitize_identifier(experiment_id, "experiment_id")
        if not issue_url or not issue_url.strip():
            raise ResearchPipelineError("issue_url is required for promotion request")
        self._require_experiment(experiment_id)
        data = dict(payload or {})
        updated = self._repo.update_experiment(
            experiment_id,
            {
                "status": "promotion_requested",
                "issue_url": issue_url,
                "promotion_requested_at": utc_now(),
            },
        )
        event = self._record_event(
            experiment_id,
            event_type="promotion_requested",
            message="Promotion requested; production assets were not modified",
            payload={"issue_url": issue_url, **data},
            created_by=data.get("created_by") or "codex",
        )
        return {"experiment": updated, "promotion_request": event}

    def reject(self, experiment_id: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        experiment_id = sanitize_identifier(experiment_id, "experiment_id")
        self._require_experiment(experiment_id)
        data = dict(payload or {})
        updated = self._repo.update_experiment(
            experiment_id,
            {
                "status": "rejected",
                "rejected_at": utc_now(),
            },
        )
        event = self._record_event(
            experiment_id,
            event_type="experiment_rejected",
            message=data.get("reason") or "Experiment rejected",
            payload=data,
            created_by=data.get("created_by") or "codex",
        )
        return {"experiment": updated, "event": event}

    def create_issue(self, payload: dict[str, Any]) -> dict[str, Any]:
        data = dict(payload or {})
        experiment_id = data.get("experiment_id")
        if experiment_id is not None:
            experiment_id = sanitize_identifier(str(experiment_id), "experiment_id")
            self._require_experiment(experiment_id)
        event = self._repo.create_pipeline_event(
            PipelineEventRecord(
                experiment_id=experiment_id,
                event_type="issue_requested",
                severity=self._issue_event_severity(data.get("severity")),
                message=str(data.get("title") or "Research issue requested"),
                payload_json=data,
                created_by=str(data.get("created_by") or "codex"),
            )
        )
        return {"issue_request": event, "external_issue_created": False}

    def _start_stage_attempt(
        self,
        *,
        experiment_id: str,
        stage_name: str,
        payload: dict[str, Any],
        event_type: str,
    ) -> dict[str, Any]:
        experiment_id, stage_name = self._safe_stage_key(experiment_id, stage_name)
        self._require_experiment(experiment_id)
        stage = self._require_stage(experiment_id, stage_name)
        attempt_no = self._repo.next_attempt_no(experiment_id, stage_name)
        attempt = self._repo.create_stage_attempt(
            StageAttemptRecord(
                stage_id=str(stage["stage_id"]),
                experiment_id=experiment_id,
                stage_name=stage_name,
                attempt_no=attempt_no,
                status="running",
                input_json=payload,
                started_at=utc_now(),
            )
        )
        self._repo.update_stage_plan(str(stage["stage_id"]), {"status": "running", "latest_attempt_no": attempt_no})
        self._repo.update_experiment(experiment_id, {"status": "running"})
        self._record_event(
            experiment_id,
            stage_attempt_id=str(attempt["stage_attempt_id"]),
            event_type=event_type,
            message=f"Stage {stage_name} attempt {attempt_no} requested",
            payload=payload,
            created_by=str(payload.get("created_by") or "codex"),
        )
        return {"stage": self._repo.get_stage_plan(experiment_id, stage_name), "attempt": attempt}

    def _require_experiment(self, experiment_id: str) -> dict[str, Any]:
        experiment = self._repo.get_experiment(experiment_id)
        if not experiment:
            raise ResearchPipelineNotFoundError(f"experiment not found: {experiment_id}")
        return experiment

    def _require_stage(self, experiment_id: str, stage_name: str) -> dict[str, Any]:
        stage = self._repo.get_stage_plan(experiment_id, stage_name)
        if not stage:
            raise ResearchPipelineNotFoundError(f"stage not found: {experiment_id}/{stage_name}")
        return stage

    @staticmethod
    def _safe_stage_key(experiment_id: str, stage_name: str) -> tuple[str, str]:
        return sanitize_identifier(experiment_id, "experiment_id"), sanitize_identifier(stage_name, "stage_name")

    @staticmethod
    def _issue_event_severity(value: Any) -> str:
        normalized = str(value or "medium").strip().lower()
        if normalized in {"debug", "info", "warning", "error"}:
            return normalized
        return {
            "low": "info",
            "minor": "info",
            "medium": "warning",
            "moderate": "warning",
            "high": "error",
            "critical": "error",
            "blocker": "error",
        }.get(normalized, "warning")

    def _record_event(
        self,
        experiment_id: str | None,
        *,
        event_type: str,
        message: str,
        payload: dict[str, Any] | None = None,
        stage_attempt_id: str | None = None,
        created_by: str = "codex",
    ) -> dict[str, Any]:
        return self._repo.create_pipeline_event(
            PipelineEventRecord(
                experiment_id=experiment_id,
                stage_attempt_id=stage_attempt_id,
                event_type=event_type,
                message=message,
                payload_json=payload or {},
                created_by=created_by,
            )
        )
