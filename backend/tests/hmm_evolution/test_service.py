from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from backend.services.hmm_evolution.errors import InvalidSpecError
from backend.services.hmm_evolution.models import (
    CandidateLifecycle,
    CandidateManifest,
    CandidateRecord,
    CandidateSourceType,
    CandidateCoverage,
    CoefficientStats,
    EvaluationPlan,
    EvaluationSpec,
)
from backend.services.hmm_evolution.service import HMMEvolutionService


def _candidate(candidate_id: str = "hmmc_test") -> CandidateRecord:
    manifest = CandidateManifest(
        source_type=CandidateSourceType.CONFIGURED_LOCAL,
        source_ref={"root_alias": "research", "relative_path": "candidate.json"},
        artifact_uri="configured-local://research/candidate.json",
        artifact_sha256="a" * 64,
        size_bytes=100,
        detected_format="hmm_sector_coefficients_legacy_v1",
        coverage=CandidateCoverage(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 2),
            date_count=2,
            sector_count_min=1,
            sector_count_max=1,
            stock_sector_map_count=1,
        ),
        coefficient_stats=CoefficientStats(min=1.0, max=1.0),
    )
    now = datetime.now(timezone.utc)
    return CandidateRecord(
        candidate_id=candidate_id,
        manifest_hash=manifest.manifest_hash,
        display_name="candidate",
        source_type=manifest.source_type,
        source_ref=manifest.source_ref,
        artifact_manifest=manifest,
        algorithm_version=manifest.algorithm_version,
        lifecycle_status=CandidateLifecycle.RESEARCH_ONLY,
        created_by="tester",
        row_version=1,
        created_at=now,
        updated_at=now,
    )


def _plan(candidate: CandidateRecord) -> EvaluationPlan:
    spec = EvaluationSpec(
        base_loop_ref="qe_20260706_013235_bbd4/Loop8",
        window_start=date(2026, 1, 1),
        window_end=date(2026, 3, 31),
        as_of={"policy": "latest_common_completed", "requested_date": None},
        label_horizon_days=20,
        topk=50,
        market_forward_return={"mode": "required", "horizon_trading_days": 10},
    )
    source_manifest = {
        "schema_version": "hmm_evaluation_source_manifest_v1",
        "base_loop_ref": spec.base_loop_ref,
        "resolved_as_of_date": "2026-04-15",
    }
    return EvaluationPlan.build(
        candidate_id=candidate.candidate_id,
        candidate_manifest_hash=candidate.manifest_hash,
        source_manifest=source_manifest,
        evaluation_spec=spec,
        evaluator_version="hmm_offline_evaluator_v1",
        resolved_as_of_date=date(2026, 4, 15),
        universe_id="prediction_artifact_all",
        universe_hash="b" * 64,
    )


class _Repository:
    def __init__(self, candidate: CandidateRecord, status: str = "queued", created: bool = True):
        self.candidate = candidate
        self.status = status
        self.created = created
        self.batch_args = None

    def get_candidate(self, candidate_id: str):
        assert candidate_id == self.candidate.candidate_id
        return self.candidate

    def create_or_get_evaluation(self, **kwargs):
        return {"eval_id": "hmme_1", "status": self.status}, self.created

    def create_or_get_batch(self, **kwargs):
        self.batch_args = kwargs
        return {"batch_id": "hmmb_1", "request_hash": kwargs["request_hash"]}, True


@pytest.mark.parametrize(
    ("status", "created", "expected_item_status"),
    [
        ("queued", True, "queued"),
        ("queued", False, "waiting_shared"),
        ("running", False, "waiting_shared"),
        ("succeeded", False, "reused"),
        ("failed", False, "failed"),
        ("cancelled", False, "cancelled"),
        ("timed_out", False, "timed_out"),
    ],
)
def test_service_maps_shared_evaluation_state_without_resetting_terminal_rows(
    status: str,
    created: bool,
    expected_item_status: str,
) -> None:
    candidate = _candidate()
    repository = _Repository(candidate, status=status, created=created)
    service = HMMEvolutionService(repository)  # type: ignore[arg-type]

    batch, was_created = service.create_batch(
        plans=[_plan(candidate)],
        recommendation_spec={"schema_version": "hmm_recommendation_spec_v1"},
        recommendation_version="hmm_recommendation_v1",
        created_by="tester",
    )

    assert was_created is True
    assert batch["batch_id"] == "hmmb_1"
    assert repository.batch_args["items"][0]["item_status"] == expected_item_status


def test_service_rejects_retired_candidate_without_research_direction_gate() -> None:
    candidate = _candidate().model_copy(
        update={"lifecycle_status": CandidateLifecycle.RETIRED}
    )
    service = HMMEvolutionService(_Repository(candidate))  # type: ignore[arg-type]

    with pytest.raises(InvalidSpecError, match="research_only"):
        service.create_batch(
            plans=[_plan(candidate)],
            recommendation_spec={"schema_version": "hmm_recommendation_spec_v1"},
            recommendation_version="hmm_recommendation_v1",
            created_by="tester",
        )


def test_evaluation_plan_detects_hash_or_watermark_drift() -> None:
    candidate = _candidate()
    plan = _plan(candidate)
    payload = plan.model_dump(mode="python")
    payload["source_manifest_hash"] = "0" * 64
    with pytest.raises(ValueError, match="source_manifest_hash"):
        EvaluationPlan.model_validate(payload)

    payload = plan.model_dump(mode="python")
    payload["resolved_as_of_date"] = date(2025, 12, 31)
    with pytest.raises(ValueError, match="window end"):
        EvaluationPlan.model_validate(payload)
