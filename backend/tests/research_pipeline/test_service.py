from __future__ import annotations

from typing import Any

from backend.services.research_pipeline.models import (
    ArtifactRefRecord,
    ComparisonRecord,
    ExperimentRecord,
    ExternalRunLinkRecord,
    PipelineEventRecord,
    StageAttemptRecord,
    StagePlanRecord,
)
from backend.services.research_pipeline.service import ResearchPipelineService


class FakeResearchPipelineRepository:
    def __init__(self) -> None:
        self.experiments: dict[str, dict[str, Any]] = {}
        self.stages: dict[tuple[str, str], dict[str, Any]] = {}
        self.attempts: list[dict[str, Any]] = []
        self.artifacts: list[dict[str, Any]] = []
        self.links: list[dict[str, Any]] = []
        self.comparisons: list[dict[str, Any]] = []
        self.events: list[dict[str, Any]] = []

    def create_experiment(self, record: ExperimentRecord) -> dict[str, Any]:
        row = record.model_dump()
        self.experiments[row["experiment_id"]] = row
        return row

    def list_experiments(self, **_kwargs: Any) -> list[dict[str, Any]]:
        return list(self.experiments.values())

    def get_experiment(self, experiment_id: str) -> dict[str, Any] | None:
        return self.experiments.get(experiment_id)

    def update_experiment(self, experiment_id: str, updates: dict[str, Any]) -> dict[str, Any]:
        self.experiments[experiment_id].update(updates)
        return self.experiments[experiment_id]

    def create_stage_plan(self, record: StagePlanRecord) -> dict[str, Any]:
        row = record.model_dump()
        self.stages[(row["experiment_id"], row["stage_name"])] = row
        return row

    def list_stage_plans(self, experiment_id: str) -> list[dict[str, Any]]:
        return [row for key, row in self.stages.items() if key[0] == experiment_id]

    def get_stage_plan(self, experiment_id: str, stage_name: str) -> dict[str, Any] | None:
        return self.stages.get((experiment_id, stage_name))

    def update_stage_plan(self, stage_id: str, updates: dict[str, Any]) -> dict[str, Any]:
        for row in self.stages.values():
            if row["stage_id"] == stage_id:
                row.update(updates)
                return row
        raise ValueError(stage_id)

    def create_stage_attempt(self, record: StageAttemptRecord) -> dict[str, Any]:
        row = record.model_dump()
        self.attempts.append(row)
        return row

    def list_stage_attempts(self, experiment_id: str, stage_name: str | None = None) -> list[dict[str, Any]]:
        rows = [row for row in self.attempts if row["experiment_id"] == experiment_id]
        if stage_name is not None:
            rows = [row for row in rows if row["stage_name"] == stage_name]
        return rows

    def next_attempt_no(self, experiment_id: str, stage_name: str) -> int:
        attempts = self.list_stage_attempts(experiment_id, stage_name)
        return max([row["attempt_no"] for row in attempts], default=0) + 1

    def create_external_run_link(self, record: ExternalRunLinkRecord) -> dict[str, Any]:
        row = record.model_dump()
        self.links.append(row)
        return row

    def list_external_run_links(self, experiment_id: str) -> list[dict[str, Any]]:
        return [row for row in self.links if row["experiment_id"] == experiment_id]

    def create_artifact_ref(self, record: ArtifactRefRecord) -> dict[str, Any]:
        row = record.model_dump()
        for existing in self.artifacts:
            if (
                existing["experiment_id"],
                existing["domain_type"],
                existing.get("domain_id"),
                existing.get("artifact_uri"),
                existing.get("artifact_sha256"),
            ) == (
                row["experiment_id"],
                row["domain_type"],
                row.get("domain_id"),
                row.get("artifact_uri"),
                row.get("artifact_sha256"),
            ):
                existing.update(row)
                return existing
        self.artifacts.append(row)
        return row

    def list_artifact_refs(self, experiment_id: str, **kwargs: Any) -> list[dict[str, Any]]:
        rows = [row for row in self.artifacts if row["experiment_id"] == experiment_id]
        if kwargs.get("domain_type"):
            rows = [row for row in rows if row["domain_type"] == kwargs["domain_type"]]
        if kwargs.get("status"):
            rows = [row for row in rows if row["status"] == kwargs["status"]]
        return rows[: kwargs.get("limit", 100)]

    def create_comparison(self, record: ComparisonRecord) -> dict[str, Any]:
        row = record.model_dump()
        self.comparisons.append(row)
        return row

    def list_comparisons(self, experiment_id: str) -> list[dict[str, Any]]:
        return [row for row in self.comparisons if row["experiment_id"] == experiment_id]

    def create_pipeline_event(self, record: PipelineEventRecord) -> dict[str, Any]:
        row = record.model_dump()
        self.events.append(row)
        return row

    def list_pipeline_events(self, experiment_id: str) -> list[dict[str, Any]]:
        return [row for row in self.events if row.get("experiment_id") == experiment_id]


def test_create_experiment_defaults_to_draft_and_stage_plan() -> None:
    repo = FakeResearchPipelineRepository()
    service = ResearchPipelineService(repo)

    experiment = service.create_experiment(pipeline_type="hmm_research", title="HMM gate")

    assert experiment["status"] == "draft"
    assert experiment["pipeline_type"] == "hmm_research"
    assert [stage["stage_name"] for stage in experiment["stages"]] == [
        "artifact_gen",
        "offline_validation",
        "portfolio_simulation",
        "qe_shadow",
    ]
    assert repo.events[-1]["event_type"] == "experiment_created"


def test_run_and_retry_stage_append_attempt_history() -> None:
    repo = FakeResearchPipelineRepository()
    service = ResearchPipelineService(repo)
    experiment = service.create_experiment(pipeline_type="event_signal_research", title="Event signal")
    experiment_id = experiment["experiment_id"]

    first = service.run_stage(experiment_id, "ic_validation", {"reason": "first"})
    second = service.retry_stage(experiment_id, "ic_validation", {"reason": "retry"})

    assert first["attempt"]["attempt_no"] == 1
    assert second["attempt"]["attempt_no"] == 2
    attempts = service.get_stage_result(experiment_id, "ic_validation")["attempts"]
    assert [attempt["attempt_no"] for attempt in attempts] == [1, 2]
    assert repo.experiments[experiment_id]["status"] == "running"


def test_artifact_ref_deduplicates_reference_without_asset_ownership() -> None:
    repo = FakeResearchPipelineRepository()
    service = ResearchPipelineService(repo)
    experiment = service.create_experiment(pipeline_type="hmm_research", title="Artifact refs")
    experiment_id = experiment["experiment_id"]

    payload = {
        "domain_type": "model",
        "domain_id": "model_1",
        "artifact_uri": "models/model_1.pkl",
        "artifact_sha256": "abc123",
        "status": "candidate",
    }
    first = service.record_artifact_ref(experiment_id, payload)
    second = service.record_artifact_ref(experiment_id, {**payload, "status": "validated"})

    assert first["artifact_ref_id"] == second["artifact_ref_id"]
    assert second["status"] == "validated"
    assert len(repo.artifacts) == 1


def test_comparison_verdict_updates_experiment_status() -> None:
    repo = FakeResearchPipelineRepository()
    service = ResearchPipelineService(repo)
    experiment = service.create_experiment(pipeline_type="hmm_research", title="Compare")
    experiment_id = experiment["experiment_id"]

    comparison = service.compare_baseline(experiment_id, {"verdict": "pass", "reason_md": "beats baseline"})

    assert comparison["verdict"] == "pass"
    assert repo.experiments[experiment_id]["status"] == "validated"
    assert repo.events[-1]["event_type"] == "comparison_recorded"


def test_promote_only_records_request() -> None:
    repo = FakeResearchPipelineRepository()
    service = ResearchPipelineService(repo)
    experiment = service.create_experiment(pipeline_type="hmm_research", title="Promote")
    experiment_id = experiment["experiment_id"]

    result = service.promote(experiment_id, issue_url="https://github.com/example/repo/issues/1")

    assert result["experiment"]["status"] == "promotion_requested"
    assert result["promotion_request"]["event_type"] == "promotion_requested"
    assert result["promotion_request"]["payload_json"]["issue_url"].startswith("https://")
    assert len(repo.events) >= 2
