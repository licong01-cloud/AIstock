"""Application service for P1-A candidate registration and durable batch creation."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from typing import Any

from .candidate_artifact import CandidateArtifactResolver
from .errors import InvalidSpecError
from .models import (
    BatchStatus,
    BatchItemStatus,
    CandidateLifecycle,
    CandidatePreview,
    CandidateRecord,
    EvaluationSpec,
    EvaluationPlan,
    EvaluationStatus,
    canonical_json_sha256,
)
from .input_adapter import HMMEvaluationInputAdapter
from .repository import HMMEvolutionRepository


class HMMEvolutionService:
    """Research-only service; it has no trading, QE mutation or approval methods."""

    def __init__(
        self,
        repository: HMMEvolutionRepository,
        *,
        artifact_resolver: CandidateArtifactResolver | None = None,
        input_adapter: HMMEvaluationInputAdapter | None = None,
    ) -> None:
        self._repository = repository
        self._artifact_resolver = artifact_resolver
        self._input_adapter = input_adapter

    def get_candidate(self, candidate_id: str) -> CandidateRecord:
        return self._repository.get_candidate(candidate_id)

    def list_candidates(
        self,
        *,
        lifecycle_status: CandidateLifecycle | None = None,
        source_type: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[CandidateRecord]:
        return self._repository.list_candidates(
            lifecycle_status=lifecycle_status,
            source_type=source_type,
            limit=limit,
            offset=offset,
        )

    def retire_candidate(self, candidate_id: str, *, expected_row_version: int) -> CandidateRecord:
        return self._repository.retire_candidate(
            candidate_id,
            expected_row_version=expected_row_version,
        )

    def get_evaluation(self, eval_id: str) -> dict[str, Any]:
        return self._repository.get_evaluation(eval_id)

    def list_evaluations(
        self,
        *,
        candidate_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        return self._repository.list_evaluations(
            candidate_id=candidate_id,
            limit=limit,
            offset=offset,
        )

    def get_batch(self, batch_id: str) -> dict[str, Any]:
        return self._repository.get_batch(batch_id)

    def list_batches(self, *, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        return self._repository.list_batches(limit=limit, offset=offset)

    def request_batch_cancel(self, *, batch_id: str, requested_by: str) -> dict[str, Any]:
        return self._repository.request_batch_cancel(
            batch_id=batch_id,
            requested_by=requested_by,
        )

    def register_candidate(
        self,
        preview: CandidatePreview,
        *,
        display_name: str,
        description: str | None,
        created_by: str,
    ):
        return self._repository.register_candidate(
            preview,
            display_name=display_name,
            description=description,
            created_by=created_by,
        )

    def preview_existing_snapshot(
        self,
        *,
        snapshot_id: str,
        artifact_name: str,
    ) -> CandidatePreview:
        return self._resolver().preview_existing_snapshot(
            snapshot_id=snapshot_id,
            artifact_name=artifact_name,
        )

    def preview_configured_local(
        self,
        *,
        root_alias: str,
        relative_path: str,
    ) -> CandidatePreview:
        return self._resolver().preview_configured_local(
            root_alias=root_alias,
            relative_path=relative_path,
        )

    async def preview_qe_experiment(
        self,
        *,
        task_id: str,
        loop_name: str,
        relative_path: str,
    ) -> CandidatePreview:
        return await self._resolver().preview_qe_experiment(
            task_id=task_id,
            loop_name=loop_name,
            relative_path=relative_path,
        )

    def create_batch(
        self,
        *,
        plans: Sequence[EvaluationPlan],
        recommendation_spec: Mapping[str, Any],
        recommendation_version: str,
        created_by: str,
        idempotency_key: str | None = None,
    ) -> tuple[dict[str, Any], bool]:
        if not 1 <= len(plans) <= 50:
            raise InvalidSpecError("an HMM evaluation batch must contain 1..50 candidates")
        candidate_ids = [plan.candidate_id for plan in plans]
        if len(candidate_ids) != len(set(candidate_ids)):
            raise InvalidSpecError("an HMM evaluation batch cannot contain duplicate candidates")
        items: list[dict[str, Any]] = []
        for ordinal, plan in enumerate(plans):
            candidate = self._repository.get_candidate(plan.candidate_id)
            if candidate.lifecycle_status is not CandidateLifecycle.RESEARCH_ONLY:
                raise InvalidSpecError(
                    "new evaluations require a research_only candidate",
                    context={
                        "candidate_id": plan.candidate_id,
                        "lifecycle_status": candidate.lifecycle_status.value,
                    },
                )
            if candidate.manifest_hash != plan.candidate_manifest_hash:
                raise InvalidSpecError(
                    "evaluation plan candidate manifest is stale",
                    context={"candidate_id": plan.candidate_id},
                )
            evaluation, created = self._repository.create_or_get_evaluation(
                candidate_id=plan.candidate_id,
                logical_evaluation_key=plan.logical_evaluation_key,
                base_loop_ref=plan.base_loop_ref,
                source_manifest=plan.source_manifest,
                source_manifest_hash=plan.source_manifest_hash,
                candidate_manifest_hash=plan.candidate_manifest_hash,
                evaluation_spec=plan.evaluation_spec.model_dump(mode="json"),
                evaluation_spec_hash=plan.evaluation_spec_hash,
                evaluator_version=plan.evaluator_version,
                as_of_date=plan.resolved_as_of_date,
                window_start=plan.evaluation_spec.window_start,
                window_end=plan.evaluation_spec.window_end,
                label_horizon_days=plan.evaluation_spec.label_horizon_days,
                universe_id=plan.universe_id,
                universe_hash=plan.universe_hash,
                topk=plan.evaluation_spec.topk,
            )
            items.append(
                {
                    "candidate_id": plan.candidate_id,
                    "eval_id": evaluation["eval_id"],
                    "ordinal": ordinal,
                    "item_status": self._item_status(evaluation["status"], created=created),
                }
            )
        request_hash = canonical_json_sha256(
            {
                "evaluations": sorted(
                    (
                        {
                            "candidate_id": plan.candidate_id,
                            "candidate_manifest_hash": plan.candidate_manifest_hash,
                            "logical_evaluation_key": plan.logical_evaluation_key,
                            "source_manifest_hash": plan.source_manifest_hash,
                            "evaluation_spec_hash": plan.evaluation_spec_hash,
                            "evaluator_version": plan.evaluator_version,
                            "universe_id": plan.universe_id,
                            "universe_hash": plan.universe_hash,
                        }
                        for plan in plans
                    ),
                    key=lambda item: (
                        item["candidate_id"],
                        item["logical_evaluation_key"],
                    ),
                ),
                "recommendation_spec": dict(recommendation_spec),
                "recommendation_version": recommendation_version,
            }
        )
        return self._repository.create_or_get_batch(
            request_hash=request_hash,
            items=items,
            recommendation_spec=recommendation_spec,
            recommendation_version=recommendation_version,
            created_by=created_by,
            idempotency_key=idempotency_key,
        )

    async def prepare_and_create_batch(
        self,
        *,
        candidate_ids: Sequence[str],
        evaluation_spec: EvaluationSpec,
        recommendation_spec: Mapping[str, Any],
        recommendation_version: str,
        created_by: str,
        idempotency_key: str | None = None,
    ) -> tuple[dict[str, Any], bool]:
        if self._input_adapter is None:
            raise InvalidSpecError("HMM evaluation input adapter is not configured")
        normalized_ids = [str(candidate_id or "").strip() for candidate_id in candidate_ids]
        if any(not candidate_id for candidate_id in normalized_ids):
            raise InvalidSpecError("candidate IDs must be non-empty")
        if len(normalized_ids) != len(set(normalized_ids)):
            raise InvalidSpecError("an HMM evaluation batch cannot contain duplicate candidates")
        candidates = await asyncio.to_thread(
            lambda: [self._repository.get_candidate(candidate_id) for candidate_id in normalized_ids]
        )
        prepared = await self._input_adapter.prepare_batch(
            candidates=candidates,
            evaluation_spec=evaluation_spec,
        )
        return await asyncio.to_thread(
            self.create_batch,
            plans=prepared.plans,
            recommendation_spec=recommendation_spec,
            recommendation_version=recommendation_version,
            created_by=created_by,
            idempotency_key=idempotency_key,
        )

    def submit_batch(
        self,
        *,
        candidate_ids: Sequence[str],
        evaluation_spec: EvaluationSpec,
        recommendation_spec: Mapping[str, Any],
        recommendation_version: str,
        created_by: str,
        idempotency_key: str | None = None,
    ) -> tuple[dict[str, Any], bool]:
        """Persist a durable receipt without loading QE or market artifacts."""

        normalized_ids = [str(candidate_id or "").strip() for candidate_id in candidate_ids]
        if not 1 <= len(normalized_ids) <= 50:
            raise InvalidSpecError("an HMM evaluation batch must contain 1..50 candidates")
        if any(not candidate_id for candidate_id in normalized_ids):
            raise InvalidSpecError("candidate IDs must be non-empty")
        if len(normalized_ids) != len(set(normalized_ids)):
            raise InvalidSpecError("an HMM evaluation batch cannot contain duplicate candidates")
        for candidate_id in normalized_ids:
            candidate = self._repository.get_candidate(candidate_id)
            if candidate.lifecycle_status is not CandidateLifecycle.RESEARCH_ONLY:
                raise InvalidSpecError(
                    "new evaluations require a research_only candidate",
                    context={
                        "candidate_id": candidate_id,
                        "lifecycle_status": candidate.lifecycle_status.value,
                    },
                )
        actor = str(created_by or "").strip()
        if not actor:
            raise InvalidSpecError("created_by is required")
        request_payload = {
            "schema_version": "hmm_batch_submission_v1",
            "candidate_ids": normalized_ids,
            "evaluation_spec": evaluation_spec.model_dump(mode="json"),
            "recommendation_spec": dict(recommendation_spec),
            "recommendation_version": recommendation_version,
            "created_by": actor,
        }
        request_hash = canonical_json_sha256(request_payload)
        return self._repository.create_or_get_submission(
            request_hash=request_hash,
            request_payload=request_payload,
            candidate_count=len(normalized_ids),
            recommendation_spec=recommendation_spec,
            recommendation_version=recommendation_version,
            created_by=actor,
            idempotency_key=idempotency_key,
        )

    async def prepare_claimed_submission(
        self,
        *,
        batch: Mapping[str, Any],
        owner_id: str,
        lease_seconds: int,
    ) -> dict[str, Any]:
        """Freeze one claimed receipt exactly once and atomically materialize it."""

        if self._input_adapter is None:
            raise InvalidSpecError("HMM evaluation input adapter is not configured")
        if str(batch.get("status")) != BatchStatus.PREPARING.value:
            raise InvalidSpecError(
                "batch submission is not in preparing state",
                context={"batch_id": batch.get("batch_id"), "status": batch.get("status")},
            )
        payload = dict(batch.get("request_payload") or {})
        if payload.get("schema_version") != "hmm_batch_submission_v1":
            raise InvalidSpecError("batch submission payload schema is unsupported")
        candidate_ids = payload.get("candidate_ids")
        if not isinstance(candidate_ids, list):
            raise InvalidSpecError("batch submission candidate_ids must be a list")
        normalized_ids = [str(item or "").strip() for item in candidate_ids]
        if (
            not 1 <= len(normalized_ids) <= 50
            or any(not item for item in normalized_ids)
            or len(normalized_ids) != len(set(normalized_ids))
        ):
            raise InvalidSpecError("batch submission candidate_ids are invalid")
        evaluation_spec = EvaluationSpec.model_validate(payload.get("evaluation_spec"))
        current = dict(batch)

        def checkpoint(_phase: str) -> None:
            nonlocal current
            current = self._repository.heartbeat_batch_preparation(
                batch_id=str(current["batch_id"]),
                owner_id=owner_id,
                fencing_token=int(current["fencing_token"]),
                expected_row_version=int(current["row_version"]),
                lease_seconds=lease_seconds,
            )

        candidates = await asyncio.to_thread(
            lambda: [self._repository.get_candidate(candidate_id) for candidate_id in normalized_ids]
        )
        checkpoint("after_candidate_validation")
        prepared = await self._input_adapter.prepare_batch(
            candidates=candidates,
            evaluation_spec=evaluation_spec,
            checkpoint=checkpoint,
        )
        return await asyncio.to_thread(
            self._repository.materialize_prepared_batch,
            batch_id=str(current["batch_id"]),
            plans=prepared.plans,
            owner_id=owner_id,
            fencing_token=int(current["fencing_token"]),
            expected_row_version=int(current["row_version"]),
        )

    def retry_failed_batch(
        self,
        *,
        batch_id: str,
        created_by: str,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        return self._repository.create_retry_batch(
            batch_id=batch_id,
            created_by=created_by,
            idempotency_key=idempotency_key,
        )

    @staticmethod
    def _item_status(status: str, *, created: bool) -> str:
        if created:
            return BatchItemStatus.QUEUED.value
        if status == EvaluationStatus.SUCCEEDED.value:
            return BatchItemStatus.REUSED.value
        if status in {EvaluationStatus.QUEUED.value, EvaluationStatus.RUNNING.value}:
            return BatchItemStatus.WAITING_SHARED.value
        if status in {
            EvaluationStatus.FAILED.value,
            EvaluationStatus.CANCELLED.value,
            EvaluationStatus.TIMED_OUT.value,
        }:
            return status
        raise InvalidSpecError(
            "evaluation has an unsupported durable status",
            context={"status": status},
        )

    def _resolver(self) -> CandidateArtifactResolver:
        if self._artifact_resolver is None:
            raise InvalidSpecError("candidate artifact resolver is not configured")
        return self._artifact_resolver
