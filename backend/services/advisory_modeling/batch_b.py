from __future__ import annotations

from collections.abc import Callable
from datetime import date, timedelta
import hashlib
from pathlib import Path
from typing import Any, Literal

from pydantic import Field, model_validator

from backend.services.advisory_historical_range.api_models import (
    ExistingProgramInput,
    HistoricalRangeBuildBridgeRequest,
    HistoricalRangeCommandRequest,
    HistoricalRangeCreateRequest,
    HistoricalRangeRefreshOutcomesRequest,
)
from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.service import HistoricalRangeApplicationService

from .errors import AdvisoryModelingError, REASON_DATASET_SNAPSHOT_NOT_SEALED
from .base_snapshot import RerankerBaseSnapshotReader
from .dataset_spool import RerankerDatasetSpool
from .feature_builder import ShortReboundFeatureBuilderV1, frozen_formula_registry_v1
from .feature_schema import frozen_feature_schema_v1
from .feature_snapshot import (
    RerankerFeatureSnapshotStore,
    materialize_feature_snapshot,
)
from .feature_sources import PostgresFeatureSourceReader, frozen_feature_query_registry_v1
from .identity import FrozenModel
from .label_policy import RankingLabelPolicyV1
from .style_profile import StrategyStyleProfileV1
from .training_export import TrainingExportStore, materialize_training_export
from .training_view import DatasetBuildIntentV1


BATCH_B_CANDIDATE_PREFETCH_PER_PROGRAM = 8


class BatchBDatasetMaterializationRequestV1(FrozenModel):
    schema_version: Literal["advisory_reranker_batch_b_materialization_request_v1"] = (
        "advisory_reranker_batch_b_materialization_request_v1"
    )
    dataset_intent: DatasetBuildIntentV1
    style_profile: StrategyStyleProfileV1
    existing_program: ExistingProgramInput
    request_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @model_validator(mode="after")
    def _identity(self) -> "BatchBDatasetMaterializationRequestV1":
        intent = self.dataset_intent
        profile = self.style_profile
        expected = {
            "style_profile_id": profile.profile_id,
            "style_profile_hash": profile.profile_payload_sha256,
            "package_id": profile.package_id,
            "package_manifest_sha256": profile.package_manifest_sha256,
            "package_asset_closure_hash": profile.package_asset_closure_hash,
            "selection_runtime_semantics_hash": profile.selection_runtime_semantics_hash,
        }
        mismatches = {
            field: {"intent": getattr(intent, field), "profile": value}
            for field, value in expected.items()
            if getattr(intent, field) != value
        }
        if mismatches:
            raise ValueError(f"dataset intent differs from style profile: {mismatches}")
        digest = canonical_json_sha256(self.model_dump(mode="python", exclude={"request_hash"}))
        if self.request_hash is not None and self.request_hash != digest:
            raise ValueError("Batch B request hash differs from canonical payload")
        object.__setattr__(self, "request_hash", digest)
        return self


class BatchBBaseSnapshotResultV1(FrozenModel):
    schema_version: Literal["advisory_reranker_batch_b_base_snapshot_result_v1"] = (
        "advisory_reranker_batch_b_base_snapshot_result_v1"
    )
    batch_id: str
    range_run_id: str
    bridge_operation_id: str
    sealed_snapshot_id: str
    completed_at_label_as_of: date
    request_hash: str = Field(min_length=64, max_length=64)


class BatchBDataQualityReceiptV1(FrozenModel):
    schema_version: Literal["advisory_reranker_batch_b_data_quality_receipt_v1"] = (
        "advisory_reranker_batch_b_data_quality_receipt_v1"
    )
    status: Literal["COMPLETE", "INSUFFICIENT_SAMPLE"]
    feature_row_count: int = Field(ge=1)
    labeled_row_count: int = Field(ge=0)
    eligible_decision_date_count: int = Field(ge=0)
    complete_window_years: tuple[int, ...]
    insufficient_window_years: tuple[int, ...]
    reason_codes: tuple[str, ...]
    receipt_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @model_validator(mode="after")
    def _identity(self) -> "BatchBDataQualityReceiptV1":
        expected_status = "COMPLETE" if self.complete_window_years == (2, 3, 5) else "INSUFFICIENT_SAMPLE"
        if self.status != expected_status:
            raise ValueError("data quality status differs from complete training views")
        if set(self.complete_window_years) & set(self.insufficient_window_years):
            raise ValueError("data quality window sets overlap")
        if tuple(sorted((*self.complete_window_years, *self.insufficient_window_years))) != (2, 3, 5):
            raise ValueError("data quality receipt must classify every 2/3/5-year window")
        digest = canonical_json_sha256(self.model_dump(mode="python", exclude={"receipt_hash"}))
        if self.receipt_hash is not None and self.receipt_hash != digest:
            raise ValueError("data quality receipt hash differs from canonical payload")
        object.__setattr__(self, "receipt_hash", digest)
        return self


class BatchBMaterializationResultV1(FrozenModel):
    schema_version: Literal["advisory_reranker_batch_b_materialization_result_v1"] = (
        "advisory_reranker_batch_b_materialization_result_v1"
    )
    request_hash: str = Field(min_length=64, max_length=64)
    dataset_request_hash: str = Field(min_length=64, max_length=64)
    base_snapshot_id: str
    base_snapshot_content_hash: str = Field(min_length=64, max_length=64)
    feature_snapshot_id: str
    feature_snapshot_hash: str = Field(min_length=64, max_length=64)
    feature_completion_receipt_hash: str = Field(min_length=64, max_length=64)
    training_export_id: str
    training_export_hash: str = Field(min_length=64, max_length=64)
    training_completion_receipt_hash: str = Field(min_length=64, max_length=64)
    data_quality: BatchBDataQualityReceiptV1
    result_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @model_validator(mode="after")
    def _identity(self) -> "BatchBMaterializationResultV1":
        digest = canonical_json_sha256(self.model_dump(mode="python", exclude={"result_hash"}))
        if self.result_hash is not None and self.result_hash != digest:
            raise ValueError("Batch B result hash differs from canonical payload")
        object.__setattr__(self, "result_hash", digest)
        return self


class ImmediateBackgroundTasks:
    """Run the existing durable dispatcher inline for a CLI-owned Batch B operation."""

    def add_task(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        func(*args, **kwargs)


class BatchBHistoricalRangeDriver:
    def __init__(self, *, service: HistoricalRangeApplicationService) -> None:
        self._service = service

    def ensure_sealed_base(
        self,
        *,
        request: BatchBDatasetMaterializationRequestV1,
        max_stable_boundary_cycles: int = 500,
    ) -> BatchBBaseSnapshotResultV1:
        if max_stable_boundary_cycles < 1:
            raise ValueError("max_stable_boundary_cycles must be positive")
        tasks = ImmediateBackgroundTasks()
        key_prefix = f"adv-reranker-batch-b-{str(request.request_hash)[:32]}"
        create = self._service.create_batch(
            HistoricalRangeCreateRequest(
                program_specs=[request.existing_program],
                start_trade_date=request.dataset_intent.decision_date_start,
                end_trade_date=request.dataset_intent.decision_date_end,
            ),
            idempotency_key=f"{key_prefix}-base",
            background_tasks=tasks,
            requested_by="advisory-modeling-batch-b",
        )
        batch_id = str(create["data"]["batch"]["batch_id"])
        batch = self._service.get_batch(batch_id)
        cycles = 0
        while str(batch["status"]) != "COMPLETED":
            if str(batch["status"]) in {"FAILED", "CANCELLED"}:
                raise AdvisoryModelingError(
                    REASON_DATASET_SNAPSHOT_NOT_SEALED,
                    "Historical Range base batch reached a terminal failure",
                    context={"batch_id": batch_id, "batch_status": batch["status"]},
                )
            if str(batch["status"]) == "WAITING_INPUT":
                raise AdvisoryModelingError(
                    REASON_DATASET_SNAPSHOT_NOT_SEALED,
                    "Historical Range base batch is waiting for explicit source data",
                    context={"batch_id": batch_id, "batch_status": batch["status"]},
                )
            if cycles >= max_stable_boundary_cycles:
                raise AdvisoryModelingError(
                    REASON_DATASET_SNAPSHOT_NOT_SEALED,
                    "Historical Range base batch did not reach a stable terminal boundary",
                    context={"batch_id": batch_id, "batch_status": batch["status"]},
                )
            self._service.resume_batch(
                batch_id,
                HistoricalRangeCommandRequest(
                    operation_idempotency_key=f"{key_prefix}-resume-{int(batch['row_version'])}",
                    expected_row_version=int(batch["row_version"]),
                ),
                background_tasks=tasks,
            )
            batch = self._service.get_batch(batch_id)
            cycles += 1
        runs_page = self._service.list_runs(batch_id, limit=10)
        runs = list(runs_page["items"])
        if len(runs) != 1 or str(runs[0]["status"]) != "COMPLETED":
            raise AdvisoryModelingError(
                REASON_DATASET_SNAPSHOT_NOT_SEALED,
                "Batch B exact Existing Program did not produce one completed range run",
                context={"batch_id": batch_id, "run_count": len(runs)},
            )
        range_run_id = str(runs[0]["range_run_id"])
        self._run_operation_until_terminal(
            invoke=lambda expected_version: self._service.refresh_outcomes(
                batch_id,
                HistoricalRangeRefreshOutcomesRequest(
                    operation_idempotency_key=f"{key_prefix}-outcomes",
                    expected_row_version=expected_version,
                    label_as_of_trade_date=request.dataset_intent.final_fit_as_of.date(),
                    range_run_ids=[range_run_id],
                    horizons=[1, 3, 5, 10, 20],
                ),
                background_tasks=tasks,
            ),
            batch_id=batch_id,
            operation_type="REFRESH_OUTCOMES",
            operation_idempotency_key=f"{key_prefix}-outcomes",
            max_cycles=max_stable_boundary_cycles,
        )
        bridge_operation = self._run_operation_until_terminal(
            invoke=lambda expected_version: self._service.build_dataset_bridge(
                batch_id,
                HistoricalRangeBuildBridgeRequest(
                    operation_idempotency_key=f"{key_prefix}-bridge",
                    expected_row_version=expected_version,
                    range_run_ids=[range_run_id],
                    requested_horizons=[1, 3, 5, 10, 20],
                    requested_maturity_statuses=["COMPLETE", "TERMINAL"],
                ),
                background_tasks=tasks,
            ),
            batch_id=batch_id,
            operation_type="BUILD_DATASET_BRIDGE",
            operation_idempotency_key=f"{key_prefix}-bridge",
            max_cycles=max_stable_boundary_cycles,
        )
        operation = self._service.get_operation(bridge_operation)
        receipt = operation.get("bridge_receipt")
        snapshot = operation.get("snapshot")
        if (
            not isinstance(receipt, dict)
            or not isinstance(snapshot, dict)
            or snapshot.get("status") != "SEALED"
            or snapshot.get("snapshot_id") != receipt.get("sealed_snapshot_id")
        ):
            raise AdvisoryModelingError(
                REASON_DATASET_SNAPSHOT_NOT_SEALED,
                "Historical Range bridge completed without one SEALED snapshot receipt",
                context={"batch_id": batch_id, "operation_id": bridge_operation},
            )
        return BatchBBaseSnapshotResultV1(
            batch_id=batch_id,
            range_run_id=range_run_id,
            bridge_operation_id=bridge_operation,
            sealed_snapshot_id=str(snapshot["snapshot_id"]),
            completed_at_label_as_of=request.dataset_intent.final_fit_as_of.date(),
            request_hash=str(request.request_hash),
        )

    def _run_operation_until_terminal(
        self,
        *,
        invoke: Callable[[int], dict[str, Any]],
        batch_id: str,
        operation_type: str,
        operation_idempotency_key: str,
        max_cycles: int,
    ) -> str:
        existing = tuple(
            item
            for item in self._service.list_operations(batch_id, limit=500)["items"]
            if str(item.get("operation_type")) == operation_type
            and str(item.get("operation_idempotency_key")) == operation_idempotency_key
        )
        if len(existing) > 1:
            raise AdvisoryModelingError(
                REASON_DATASET_SNAPSHOT_NOT_SEALED,
                "Batch B found duplicate durable Historical Range operations",
                context={
                    "batch_id": batch_id,
                    "operation_type": operation_type,
                    "operation_count": len(existing),
                },
            )
        if existing:
            operation_id = str(existing[0]["operation_id"])
            operation = self._service.get_operation(operation_id)
            status = str(operation["status"])
            if status == "COMPLETED":
                return operation_id
            raise AdvisoryModelingError(
                REASON_DATASET_SNAPSHOT_NOT_SEALED,
                "Batch B existing Historical Range operation is not reusable",
                context={"operation_id": operation_id, "status": status},
            )
        operation_id = ""
        expected_version = int(self._service.get_batch(batch_id)["row_version"])
        for _ in range(max_cycles):
            response = invoke(expected_version)
            operation_id = str(response["data"]["operation_id"])
            operation = self._service.get_operation(operation_id)
            status = str(operation["status"])
            if status == "COMPLETED":
                return operation_id
            if status == "FAILED":
                raise AdvisoryModelingError(
                    REASON_DATASET_SNAPSHOT_NOT_SEALED,
                    "Batch B Historical Range operation failed",
                    context={"operation_id": operation_id, "error": operation.get("error")},
                )
            if status in {"WAITING_INPUT", "RUNNING"}:
                raise AdvisoryModelingError(
                    REASON_DATASET_SNAPSHOT_NOT_SEALED,
                    "Batch B Historical Range operation stopped at a non-terminal stable boundary",
                    context={"operation_id": operation_id, "status": status},
                )
            if status not in {"QUEUED", "RUNNING", "WAITING_INPUT", "RETRYABLE_FAILED"}:
                raise AdvisoryModelingError(
                    REASON_DATASET_SNAPSHOT_NOT_SEALED,
                    "Batch B Historical Range operation entered an unsupported state",
                    context={"operation_id": operation_id, "status": status},
                )
        raise AdvisoryModelingError(
            REASON_DATASET_SNAPSHOT_NOT_SEALED,
            "Batch B Historical Range operation exceeded stable-boundary cycles",
            context={"operation_id": operation_id},
        )


class BatchBMaterializationService:
    """Execute the complete Batch B vertical slice without starting model training."""

    _BUILDER_FILES = (
        "backend/services/advisory_modeling/base_snapshot.py",
        "backend/services/advisory_modeling/dataset_spool.py",
        "backend/services/advisory_modeling/feature_builder.py",
        "backend/services/advisory_modeling/feature_schema.py",
        "backend/services/advisory_modeling/feature_snapshot.py",
        "backend/services/advisory_modeling/feature_sources.py",
        "backend/services/advisory_modeling/training_export.py",
    )

    def __init__(
        self,
        *,
        historical_driver: BatchBHistoricalRangeDriver,
        base_reader: RerankerBaseSnapshotReader,
        feature_source_reader: PostgresFeatureSourceReader,
    ) -> None:
        self._historical_driver = historical_driver
        self._base_reader = base_reader
        self._feature_source_reader = feature_source_reader

    def execute(
        self,
        *,
        request: BatchBDatasetMaterializationRequestV1,
        repository_root: Path,
        artifact_root: Path,
        spool_root: Path,
    ) -> BatchBMaterializationResultV1:
        repository = repository_root.resolve(strict=True)
        artifact = artifact_root.resolve(strict=True)
        spool_output = spool_root.resolve(strict=True)
        if not repository.is_dir() or not artifact.is_dir() or not spool_output.is_dir():
            raise ValueError("Batch B roots must be existing directories")
        base = self._historical_driver.ensure_sealed_base(request=request)
        operation_id = f"batch-b-{str(request.request_hash)[:24]}"
        with RerankerDatasetSpool(
            output_root=spool_output,
            repository_root=repository,
            artifact_root=artifact,
            operation_id=operation_id,
        ) as spool:
            groups, base_receipt, dataset_request = self._base_reader.read(
                snapshot_id=base.sealed_snapshot_id,
                intent=request.dataset_intent,
                spool=spool,
            )
            query_registry = frozen_feature_query_registry_v1(
                repository_commit=request.dataset_intent.repository_commit
            )
            feature_schema = frozen_feature_schema_v1()
            formula_registry = frozen_formula_registry_v1()
            if (
                dataset_request.feature_schema_hash != feature_schema.feature_schema_hash
                or dataset_request.feature_formula_registry_hash != formula_registry.registry_hash
                or dataset_request.feature_query_registry_hash != query_registry.registry_hash
            ):
                raise AdvisoryModelingError(
                    "MODEL_CONTRACT_NOT_AVAILABLE",
                    "Batch B intent differs from current frozen feature contracts",
                )
            feature_start = dataset_request.decision_date_start - timedelta(days=180)
            source_revisions = self._feature_source_reader.capture(
                registry=query_registry,
                request_semantic_hash=str(dataset_request.request_semantic_hash),
                start_date=feature_start,
                end_date=dataset_request.decision_date_end,
                spool=spool,
            )
            source_set_hash = canonical_json_sha256(
                tuple(str(item.source_revision_hash) for item in source_revisions)
            )
            builder_hash = self._builder_closure_hash(repository)
            builder = ShortReboundFeatureBuilderV1(
                source_spool=spool,
                source_identity=str(dataset_request.request_semantic_hash),
            )
            feature_rows = tuple(
                row
                for group in groups
                for row in builder.build_group(
                    candidates=group,
                    query_registry_hash=str(query_registry.registry_hash),
                    feature_source_revision_set_hash=source_set_hash,
                    builder_code_closure_hash=builder_hash,
                )
            )
            feature_manifest, feature_payload = materialize_feature_snapshot(
                request=dataset_request,
                base_snapshot_id=base_receipt.snapshot_id,
                base_snapshot_content_hash=base_receipt.snapshot_content_hash,
                feature_schema=feature_schema,
                formula_registry=formula_registry,
                query_registry=query_registry,
                source_revisions=source_revisions,
                builder_code_closure_hash=builder_hash,
                rows=feature_rows,
            )
            feature_completion = RerankerFeatureSnapshotStore(
                artifact_root=artifact,
                repository_root=repository,
            ).publish(manifest=feature_manifest, payload_files=feature_payload)
            trading_dates = tuple(
                date.fromisoformat(str(row["cal_date"]))
                for row in spool.iter_rows(
                    source_kind="FEATURE_SOURCE",
                    source_identity=str(dataset_request.request_semantic_hash),
                    logical_role="historical_trading_calendar_window",
                    end_date=dataset_request.decision_date_end.isoformat(),
                )
                if bool(row["is_trading"])
            )
            label_policy = RankingLabelPolicyV1()
            if dataset_request.label_policy_hash != label_policy.label_policy_hash:
                raise AdvisoryModelingError(
                    "MODEL_LABEL_CLOSURE_INCOMPLETE",
                    "Batch B intent differs from the frozen ranking label policy",
                )
            training_manifest, training_payload = materialize_training_export(
                request=dataset_request,
                base_snapshot_id=base_receipt.snapshot_id,
                base_snapshot_content_hash=base_receipt.snapshot_content_hash,
                feature_snapshot=feature_manifest,
                feature_schema=feature_schema,
                feature_rows=feature_rows,
                base_spool=spool,
                trading_dates=trading_dates,
                label_policy=label_policy,
            )
            training_completion = TrainingExportStore(
                artifact_root=artifact,
                repository_root=repository,
            ).publish(manifest=training_manifest, payload_files=training_payload)
            complete = tuple(view.window_years for view in training_manifest.views if view.trainable)
            insufficient = tuple(view.window_years for view in training_manifest.views if not view.trainable)
            quality = BatchBDataQualityReceiptV1(
                status="COMPLETE" if complete == (2, 3, 5) else "INSUFFICIENT_SAMPLE",
                feature_row_count=len(feature_rows),
                labeled_row_count=training_manifest.row_count,
                eligible_decision_date_count=len(training_manifest.split_plan.eligible_decision_dates),
                complete_window_years=complete,
                insufficient_window_years=insufficient,
                reason_codes=() if not insufficient else ("MODEL_DATASET_WINDOW_INSUFFICIENT",),
            )
            return BatchBMaterializationResultV1(
                request_hash=str(request.request_hash),
                dataset_request_hash=str(dataset_request.request_semantic_hash),
                base_snapshot_id=base_receipt.snapshot_id,
                base_snapshot_content_hash=base_receipt.snapshot_content_hash,
                feature_snapshot_id=str(feature_manifest.feature_snapshot_id),
                feature_snapshot_hash=str(feature_manifest.feature_snapshot_hash),
                feature_completion_receipt_hash=str(feature_completion.receipt_hash),
                training_export_id=str(training_manifest.export_id),
                training_export_hash=str(training_manifest.export_hash),
                training_completion_receipt_hash=str(training_completion.receipt_hash),
                data_quality=quality,
            )

    @classmethod
    def _builder_closure_hash(cls, repository_root: Path) -> str:
        files = []
        for relative_path in cls._BUILDER_FILES:
            path = repository_root / relative_path
            payload = path.read_bytes()
            files.append(
                {
                    "relative_path": relative_path,
                    "sha256": hashlib.sha256(payload).hexdigest(),
                    "size_bytes": len(payload),
                }
            )
        return canonical_json_sha256(files)
