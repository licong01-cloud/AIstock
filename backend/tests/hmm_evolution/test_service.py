from __future__ import annotations

import asyncio
from datetime import date, datetime, timezone
from types import SimpleNamespace

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


def _candidate(
    candidate_id: str = "hmmc_test",
    *,
    artifact_sha256: str = "a" * 64,
) -> CandidateRecord:
    manifest = CandidateManifest(
        source_type=CandidateSourceType.CONFIGURED_LOCAL,
        source_ref={"root_alias": "research", "relative_path": "candidate.json"},
        artifact_uri="configured-local://research/candidate.json",
        artifact_sha256=artifact_sha256,
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


def _plan(candidate: CandidateRecord, *, topk: int = 50) -> EvaluationPlan:
    spec = EvaluationSpec(
        base_loop_ref="qe_20260706_013235_bbd4/Loop8",
        window_start=date(2026, 1, 1),
        window_end=date(2026, 3, 31),
        as_of={"policy": "latest_common_completed", "requested_date": None},
        label_horizon_days=20,
        topk=topk,
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
        universe_id="filtered_pool_fixture:qe_st_pit_fixture",
        universe_hash="b" * 64,
    )


class _Repository:
    def __init__(self, candidate: CandidateRecord, status: str = "queued", created: bool = True):
        self.candidate = candidate
        self.status = status
        self.created = created
        self.batch_args = None
        self.submission_args = None

    def get_candidate(self, candidate_id: str):
        assert candidate_id == self.candidate.candidate_id
        return self.candidate

    def create_or_get_evaluation(self, **kwargs):
        return {"eval_id": "hmme_1", "status": self.status}, self.created

    def create_or_get_batch(self, **kwargs):
        self.batch_args = kwargs
        return {"batch_id": "hmmb_1", "request_hash": kwargs["request_hash"]}, True

    def create_or_get_submission(self, **kwargs):
        self.submission_args = kwargs
        return {
            "batch_id": "hmmb_submission",
            "request_hash": kwargs["request_hash"],
            "request_payload": kwargs["request_payload"],
            "status": "preparation_queued",
        }, True


class _MultiRepository(_Repository):
    def __init__(self, candidates: list[CandidateRecord]):
        super().__init__(candidates[0])
        self.candidates = {candidate.candidate_id: candidate for candidate in candidates}
        self.request_hashes: list[str] = []
        self.eval_index = 0
        self.materialized = None
        self.heartbeat_version = 2

    def get_candidate(self, candidate_id: str):
        return self.candidates[candidate_id]

    def create_or_get_evaluation(self, **kwargs):
        self.eval_index += 1
        return {"eval_id": f"hmme_{self.eval_index}", "status": "queued"}, True

    def create_or_get_batch(self, **kwargs):
        self.request_hashes.append(kwargs["request_hash"])
        return {
            "batch_id": f"hmmb_{len(self.request_hashes)}",
            "request_hash": kwargs["request_hash"],
        }, True

    def heartbeat_batch_preparation(self, **kwargs):
        self.heartbeat_version += 1
        return {
            "batch_id": kwargs["batch_id"],
            "status": "preparing",
            "fencing_token": kwargs["fencing_token"],
            "row_version": self.heartbeat_version,
        }

    def create_performance_receipt(self, **kwargs):
        return {"receipt_id": f"hmpr_{kwargs['batch_id']}", "row_version": 1}, True

    def merge_performance_receipt_progress(self, **kwargs):
        return {
            "receipt_id": kwargs["receipt_id"],
            "row_version": kwargs["expected_row_version"] + 1,
        }

    def materialize_prepared_batch(self, **kwargs):
        self.materialized = kwargs
        return {"batch_id": kwargs["batch_id"], "status": "queued"}


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


def test_evaluation_spec_nested_identity_is_deeply_immutable() -> None:
    plan = _plan(_candidate())
    original_hash = plan.evaluation_spec.spec_hash

    with pytest.raises(TypeError, match="cannot be mutated"):
        plan.evaluation_spec.as_of["policy"] = "explicit"
    with pytest.raises(TypeError, match="cannot be mutated"):
        plan.evaluation_spec.market_forward_return["mode"] = "disabled"

    assert plan.evaluation_spec.spec_hash == original_hash


def test_evaluation_plan_source_manifest_is_deeply_immutable_and_serializable() -> None:
    candidate = _candidate()
    original = _plan(candidate)
    plan = EvaluationPlan.build(
        candidate_id=candidate.candidate_id,
        candidate_manifest_hash=candidate.manifest_hash,
        source_manifest={
            "schema_version": "hmm_evaluation_source_manifest_v1",
            "artifacts": [{"artifact_name": "pred.pkl", "sha256": "c" * 64}],
        },
        evaluation_spec=original.evaluation_spec,
        evaluator_version=original.evaluator_version,
        resolved_as_of_date=original.resolved_as_of_date,
        universe_id=original.universe_id,
        universe_hash=original.universe_hash,
    )
    original_hash = plan.source_manifest_hash

    with pytest.raises(TypeError, match="cannot be mutated"):
        plan.source_manifest["schema_version"] = "other"
    with pytest.raises(TypeError, match="cannot be mutated"):
        plan.source_manifest["artifacts"][0]["sha256"] = "f" * 64

    assert plan.source_manifest_hash == original_hash
    dumped = plan.model_dump(mode="json")
    assert isinstance(dumped["source_manifest"], dict)
    assert isinstance(dumped["source_manifest"]["artifacts"], list)


def test_candidate_manifest_source_ref_is_deeply_immutable() -> None:
    candidate = _candidate()
    original_hash = candidate.manifest_hash

    with pytest.raises(TypeError, match="cannot be mutated"):
        candidate.artifact_manifest.source_ref["relative_path"] = "other.json"

    assert candidate.artifact_manifest.manifest_hash == original_hash


def test_batch_request_hash_preserves_candidate_to_spec_binding() -> None:
    candidate_a = _candidate("hmmc_a", artifact_sha256="a" * 64)
    candidate_b = _candidate("hmmc_b", artifact_sha256="b" * 64)
    repository = _MultiRepository([candidate_a, candidate_b])
    service = HMMEvolutionService(repository)  # type: ignore[arg-type]

    service.create_batch(
        plans=[_plan(candidate_a, topk=50), _plan(candidate_b, topk=60)],
        recommendation_spec={"schema_version": "hmm_recommendation_spec_v1"},
        recommendation_version="hmm_recommendation_v1",
        created_by="tester",
    )
    service.create_batch(
        plans=[_plan(candidate_a, topk=60), _plan(candidate_b, topk=50)],
        recommendation_spec={"schema_version": "hmm_recommendation_spec_v1"},
        recommendation_version="hmm_recommendation_v1",
        created_by="tester",
    )
    service.create_batch(
        plans=[_plan(candidate_b, topk=60), _plan(candidate_a, topk=50)],
        recommendation_spec={"schema_version": "hmm_recommendation_spec_v1"},
        recommendation_version="hmm_recommendation_v1",
        created_by="tester",
    )

    first, swapped, reordered = repository.request_hashes
    assert first != swapped
    assert first == reordered


def test_prepare_and_create_batch_uses_real_input_adapter_plans() -> None:
    candidate_a = _candidate("hmmc_a", artifact_sha256="a" * 64)
    candidate_b = _candidate("hmmc_b", artifact_sha256="b" * 64)
    repository = _MultiRepository([candidate_a, candidate_b])

    class _InputAdapter:
        async def prepare_batch(self, *, candidates, evaluation_spec, checkpoint=None):
            assert [candidate.candidate_id for candidate in candidates] == ["hmmc_a", "hmmc_b"]
            if checkpoint:
                checkpoint("prepared")
            return SimpleNamespace(
                plans=tuple(_plan(candidate, topk=evaluation_spec.topk) for candidate in candidates)
            )

    service = HMMEvolutionService(  # type: ignore[arg-type]
        repository,
        input_adapter=_InputAdapter(),
    )
    batch, created = asyncio.run(
        service.prepare_and_create_batch(
            candidate_ids=["hmmc_a", "hmmc_b"],
            evaluation_spec=_plan(candidate_a).evaluation_spec,
            recommendation_spec={"schema_version": "hmm_recommendation_spec_v1"},
            recommendation_version="hmm_recommendation_v1",
            created_by="tester",
        )
    )
    assert created is True
    assert batch["batch_id"] == "hmmb_1"
    assert repository.eval_index == 2


def test_submit_batch_persists_receipt_without_preparing_inputs() -> None:
    candidate = _candidate()
    repository = _Repository(candidate)

    class _InputAdapter:
        async def prepare_batch(self, **_kwargs):  # pragma: no cover - API receipt must not prepare.
            raise AssertionError("submission unexpectedly prepared remote inputs")

    service = HMMEvolutionService(repository, input_adapter=_InputAdapter())  # type: ignore[arg-type]
    batch, created = service.submit_batch(
        candidate_ids=[candidate.candidate_id],
        evaluation_spec=_plan(candidate).evaluation_spec,
        recommendation_spec={"schema_version": "hmm_recommendation_spec_v1"},
        recommendation_version="hmm_recommendation_v1",
        created_by="tester",
        idempotency_key="intent-1",
    )

    assert created is True
    assert batch["status"] == "preparation_queued"
    assert repository.submission_args["candidate_count"] == 1
    assert repository.submission_args["request_payload"]["candidate_ids"] == [candidate.candidate_id]


def test_worker_preparation_materializes_claimed_receipt_after_heartbeats() -> None:
    candidate = _candidate()
    repository = _MultiRepository([candidate])

    class _InputAdapter:
        async def prepare_batch(self, *, candidates, evaluation_spec, checkpoint=None):
            assert [item.candidate_id for item in candidates] == [candidate.candidate_id]
            if checkpoint:
                checkpoint("before-freeze")
            return SimpleNamespace(
                plans=(_plan(candidate, topk=evaluation_spec.topk),),
                artifact_source_info={},
            )

    service = HMMEvolutionService(repository, input_adapter=_InputAdapter())  # type: ignore[arg-type]
    result = asyncio.run(
        service.prepare_claimed_submission(
            batch={
                "batch_id": "hmmb_submission",
                "status": "preparing",
                "fencing_token": 3,
                "row_version": 2,
                "request_hash": "c" * 64,
                "candidate_count": 1,
                "request_payload": {
                    "schema_version": "hmm_batch_submission_v1",
                    "candidate_ids": [candidate.candidate_id],
                    "evaluation_spec": _plan(candidate).evaluation_spec.model_dump(mode="json"),
                    "recommendation_spec": {"schema_version": "hmm_recommendation_spec_v1"},
                    "recommendation_version": "hmm_recommendation_v1",
                    "created_by": "tester",
                },
            },
            owner_id="worker-1",
            lease_seconds=90,
        )
    )

    assert result == {"batch_id": "hmmb_submission", "status": "queued"}
    assert repository.materialized["expected_row_version"] == repository.heartbeat_version
    assert len(repository.materialized["plans"]) == 1
