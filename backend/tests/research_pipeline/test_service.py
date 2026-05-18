from __future__ import annotations

from typing import Any

from backend.services.research_pipeline.models import (
    ArtifactRefRecord,
    ComparisonRecord,
    ExperimentRecord,
    ExternalRunLinkRecord,
    PipelineEventRecord,
    BackfillRunRecord,
    BacktestRecord,
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
        self.backtest_records: dict[str, dict[str, Any]] = {}
        self.backfill_runs: dict[str, dict[str, Any]] = {}

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

    def update_stage_attempt(self, stage_attempt_id: str, updates: dict[str, Any]) -> dict[str, Any]:
        for row in self.attempts:
            if row["stage_attempt_id"] == stage_attempt_id:
                row.update(updates)
                return row
        raise ValueError(stage_attempt_id)

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

    def upsert_backtest_record(self, record: BacktestRecord) -> dict[str, Any]:
        row = record.model_dump()
        existing = self.backtest_records.get(row["record_key_sha256"])
        if existing:
            existing.update(row)
            return existing
        self.backtest_records[row["record_key_sha256"]] = row
        return row

    def list_backtest_records(self, experiment_id: str, **kwargs: Any) -> list[dict[str, Any]]:
        rows = [row for row in self.backtest_records.values() if row["experiment_id"] == experiment_id]
        for key in ("research_domain", "dedup_status", "source_task_id", "hmm_config_sig", "non_hmm_config_sig"):
            if kwargs.get(key):
                rows = [row for row in rows if row.get(key) == kwargs[key]]
        if kwargs.get("qe_archive_representative") is not None:
            rows = [row for row in rows if row.get("qe_archive_representative") is bool(kwargs["qe_archive_representative"])]
        offset = int(kwargs.get("offset") or 0)
        limit = int(kwargs.get("limit") or 100)
        return rows[offset : offset + limit]

    def create_backfill_run(self, record: BackfillRunRecord) -> dict[str, Any]:
        row = record.model_dump()
        self.backfill_runs[row["backfill_run_id"]] = row
        return row

    def update_backfill_run(self, backfill_run_id: str, updates: dict[str, Any]) -> dict[str, Any]:
        self.backfill_runs[backfill_run_id].update(updates)
        return self.backfill_runs[backfill_run_id]

    def get_backfill_run(self, backfill_run_id: str) -> dict[str, Any] | None:
        return self.backfill_runs.get(backfill_run_id)

    def list_backfill_runs(self, experiment_id: str, *, limit: int = 50) -> list[dict[str, Any]]:
        rows = [row for row in self.backfill_runs.values() if row["experiment_id"] == experiment_id]
        return rows[:limit]


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
        "backtest_recording",
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


def test_offline_hmm_stage_passes_criteria_records_artifact_and_comparison() -> None:
    repo = FakeResearchPipelineRepository()
    service = ResearchPipelineService(repo)
    experiment = service.create_experiment(
        pipeline_type="hmm_research",
        title="Offline HMM",
        criteria_json={
            "stage_criteria": {
                "offline_validation": {
                    "min_metrics": {"sharpe": 1.0},
                    "max_metrics": {"max_drawdown": 0.2},
                }
            }
        },
        baseline_ref_json={"metrics_json": {"sharpe": 0.8, "max_drawdown": 0.25}},
    )
    experiment_id = experiment["experiment_id"]

    result = service.complete_offline_stage(
        experiment_id,
        "offline_validation",
        {
            "metrics_json": {"sharpe": 1.2, "max_drawdown": 0.12},
            "candidate_ref_json": {
                "domain_type": "hmm_artifact",
                "domain_id": "hmm_candidate_1",
                "artifact_uri": "artifacts/hmm_candidate_1.json",
                "artifact_sha256": "abc123",
            },
            "artifact_status": "validated",
        },
    )

    assert result["attempt"]["status"] == "passed"
    assert result["stage"]["status"] == "passed"
    assert result["comparison"]["verdict"] == "pass"
    assert result["artifact_refs"][0]["domain_type"] == "hmm_artifact"
    assert repo.artifacts[0]["stage_attempt_id"] == result["attempt"]["stage_attempt_id"]
    assert repo.events[-1]["event_type"] == "offline_stage_completed"
    assert any(event["event_type"] == "offline_stage_evaluated" for event in repo.events)


def test_offline_event_signal_stage_failure_preserves_attempt_history() -> None:
    repo = FakeResearchPipelineRepository()
    service = ResearchPipelineService(repo)
    experiment = service.create_experiment(
        pipeline_type="event_signal_research",
        title="Event signal",
        criteria_json={"stage_criteria": {"ic_validation": {"min_metrics": {"rank_ic": 0.03}}}},
    )
    experiment_id = experiment["experiment_id"]

    first = service.complete_offline_stage(
        experiment_id,
        "ic_validation",
        {"metrics_json": {"rank_ic": 0.01}, "candidate_ref_json": {"domain_type": "event_signal", "domain_id": "sig_1"}},
    )
    second = service.complete_offline_stage(
        experiment_id,
        "ic_validation",
        {"metrics_json": {"rank_ic": 0.05}, "candidate_ref_json": {"domain_type": "event_signal", "domain_id": "sig_1"}},
    )

    assert first["attempt"]["attempt_no"] == 1
    assert first["attempt"]["status"] == "failed"
    assert first["comparison"]["verdict"] == "fail"
    assert second["attempt"]["attempt_no"] == 2
    assert second["attempt"]["status"] == "passed"
    assert [attempt["attempt_no"] for attempt in repo.attempts if attempt["stage_name"] == "ic_validation"] == [1, 2]
    assert repo.experiments[experiment_id]["status"] == "validated"


def test_offline_artifact_gen_records_candidate_without_numeric_criteria() -> None:
    repo = FakeResearchPipelineRepository()
    service = ResearchPipelineService(repo)
    experiment = service.create_experiment(pipeline_type="hmm_research", title="Artifact stage")

    result = service.complete_offline_stage(
        experiment["experiment_id"],
        "artifact_gen",
        {
            "candidate_ref_json": {
                "domain_type": "hmm_artifact",
                "domain_id": "hmm_candidate_2",
                "artifact_uri": "artifacts/hmm_candidate_2.json",
            }
        },
    )

    assert result["attempt"]["status"] == "passed"
    assert result["comparison"] is None
    assert result["artifact_refs"][0]["status"] == "validated"


def test_offline_completion_rejects_qe_shadow_until_phase5() -> None:
    repo = FakeResearchPipelineRepository()
    service = ResearchPipelineService(repo)
    experiment = service.create_experiment(pipeline_type="hmm_research", title="No QE in phase4")

    try:
        service.complete_offline_stage(experiment["experiment_id"], "qe_shadow", {"metrics_json": {"sharpe": 1.0}})
    except ValueError as exc:
        assert "offline completion" in str(exc)
    else:
        raise AssertionError("qe_shadow offline completion should be rejected in Phase 4")


def test_backtest_records_and_backfill_runs_are_queryable() -> None:
    repo = FakeResearchPipelineRepository()
    service = ResearchPipelineService(repo)
    experiment = service.create_experiment(pipeline_type="hmm_research", title="HMM timeline")
    experiment_id = experiment["experiment_id"]

    record = repo.upsert_backtest_record(
        BacktestRecord(
            experiment_id=experiment_id,
            source_type="qe_loop",
            source_task_id="task_1",
            source_loop_id="loop_1",
            source_loop_index=1,
            record_key_sha256="key_1",
            non_hmm_config_sig="family_a",
            hmm_config_sig="hmm_a",
            dedup_status="primary",
            qe_archive_representative=True,
            ann=0.42,
        )
    )
    run = repo.create_backfill_run(BackfillRunRecord(experiment_id=experiment_id, counts_json={"would_insert": 1}))

    assert service.list_backtest_records(experiment_id, qe_archive_representative=True) == [record]
    assert service.list_backfill_runs(experiment_id) == [run]
    assert service.get_backfill_run(run["backfill_run_id"]) == run


def test_get_backfill_run_missing_raises_not_found() -> None:
    repo = FakeResearchPipelineRepository()
    service = ResearchPipelineService(repo)

    try:
        service.get_backfill_run("rp_bf_missing")
    except ValueError as exc:
        assert "backfill run not found" in str(exc)
    else:
        raise AssertionError("missing backfill run should raise")
