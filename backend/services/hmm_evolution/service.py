"""Application service for P1-A candidate registration and durable batch creation."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from .candidate_artifact import CandidateArtifactResolver
from .errors import InvalidSpecError
from .models import (
    BatchItemStatus,
    CandidateLifecycle,
    CandidatePreview,
    EvaluationPlan,
    EvaluationStatus,
    canonical_json_sha256,
)
from .repository import HMMEvolutionRepository


class HMMEvolutionService:
    """Research-only service; it has no trading, QE mutation or approval methods."""

    def __init__(
        self,
        repository: HMMEvolutionRepository,
        *,
        artifact_resolver: CandidateArtifactResolver | None = None,
    ) -> None:
        self._repository = repository
        self._artifact_resolver = artifact_resolver

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
                "candidate_ids": sorted(candidate_ids),
                "evaluation_spec_hashes": sorted(plan.evaluation_spec_hash for plan in plans),
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
