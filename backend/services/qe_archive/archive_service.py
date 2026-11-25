"""QE archive payload classification, persistence and asset publication.

Realtime terminal hooks reach this service through the durable outbox worker;
manual/backfill callers use the same classifier and idempotent writer.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, Mapping

from backend.services.model_store.service import ModelStoreService
from backend.services.quantevolver.qe_execution_reservation import (
    PREPARED_ABSOLUTE_TARGET_MODE,
    PREPARED_WORKSPACE_PARAM,
)
from backend.services.quantevolver.qe_run_registry import (
    QE_ARCHIVE_REPLAY_SOURCE_PARAM,
    normalize_archive_replay_source,
    prepared_workspace_loop_index,
)

from .payload_extractor import ExtractedArchivePayload, QEArchivePayloadExtractor
from .repository import QEArchiveRepository
from .asset_lifecycle import (
    QEAssetStatus,
    QEWarehouseStatus,
    attach_lifecycle,
    classify_qe_result,
)
from .asset_publisher import (
    QEArchiveAssetPublishError,
    QEArchiveAssetPublisher,
    asset_publish_manifest,
)
from backend.services.quantevolver.qe_resource_phase_service import (
    RESOURCE_SCHEMA_REASON,
    QEResourcePhaseError,
    QEResourcePhaseService,
)


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ArchivePayloadResult:
    run_id: str
    dry_run: bool
    stats: dict[str, Any]
    extracted: ExtractedArchivePayload


class QEArchiveService:
    """Archive normalized QE payloads through explicit repository calls."""

    def __init__(
        self,
        *,
        repository: QEArchiveRepository | None = None,
        extractor: QEArchivePayloadExtractor | None = None,
        model_store_service: ModelStoreService | None = None,
        resource_phase_service: QEResourcePhaseService | None = None,
        asset_publisher: QEArchiveAssetPublisher | None = None,
    ) -> None:
        self._repository = repository or QEArchiveRepository()
        self._extractor = extractor or QEArchivePayloadExtractor()
        self._model_store_service = model_store_service or ModelStoreService()
        self._asset_publisher = asset_publisher
        self._resource_phase_service = resource_phase_service
        if resource_phase_service is None and (
            repository is None or isinstance(repository, QEArchiveRepository)
        ):
            self._resource_phase_service = QEResourcePhaseService()

    def process_payload(
        self,
        payload: Mapping[str, Any],
        *,
        event_type: str | None = None,
        source_system: str | None = None,
        source_id: str | None = None,
        source_sub_id: str | None = None,
        dry_run: bool = True,
    ) -> ArchivePayloadResult:
        """Extract records and optionally write them to `qe_archive`.

        `dry_run=True` is the safe default for manual validation and future
        backfill previews; production hooks must opt in explicitly.
        """

        prepared_payload, artifact_resolution = self._attach_prediction_store_manifest(payload)
        lifecycle = classify_qe_result(prepared_payload)
        preliminary_extracted = self._extractor.extract(
            attach_lifecycle(prepared_payload, lifecycle),
            event_type=event_type,
            source_system=source_system,
            source_id=source_id,
            source_sub_id=source_sub_id,
        )
        if (
            not dry_run
            and lifecycle.archive_eligible
            and lifecycle.business_identity_sha256
            and lifecycle.result_digest_sha256
            and lifecycle.result_digest_authoritative
        ):
            find_survivor = getattr(self._repository, "find_lifecycle_survivor", None)
            if callable(find_survivor):
                survivor = find_survivor(
                    business_identity_sha256=lifecycle.business_identity_sha256,
                    result_digest_sha256=lifecycle.result_digest_sha256,
                    exclude_run_id=preliminary_extracted.run.run_id,
                )
                if survivor:
                    lifecycle = classify_qe_result(
                        prepared_payload,
                        duplicate_of=str(survivor),
                    )
        if not dry_run and lifecycle.archive_eligible:
            lifecycle = lifecycle.with_persistence(warehouse_status=QEWarehouseStatus.PERSISTED)
        prepared_payload = attach_lifecycle(prepared_payload, lifecycle)
        extracted = self._extractor.extract(
            prepared_payload,
            event_type=event_type,
            source_system=source_system,
            source_id=source_id,
            source_sub_id=source_sub_id,
        )
        stats = dict(extracted.stats)
        stats.update(
            {
                "run_id": extracted.run.run_id,
                "dry_run": dry_run,
                "data_context_count": len(extracted.data_contexts),
                "account_summary_count": 1 if extracted.account_summary else 0,
                "symbol_summary_count": len(extracted.symbol_summaries),
                "trade_count": len(extracted.trades),
                "execution_event_count": len(extracted.execution_events),
                "raw_payload_count": len(extracted.raw_payloads),
                "prediction_store_link": _compact_artifact_resolution(artifact_resolution),
                "asset_lifecycle": lifecycle.as_dict(),
                "not_eligible": not lifecycle.archive_eligible,
            }
        )

        if not dry_run:
            if not lifecycle.archive_eligible:
                stats["written"] = False
                stats["not_eligible"] = True
                return ArchivePayloadResult(
                    run_id=extracted.run.run_id,
                    dry_run=False,
                    stats=stats,
                    extracted=extracted,
                )
            extracted = replace(
                extracted,
                run=replace(
                    extracted.run,
                    archived_at=extracted.run.archived_at or datetime.now(timezone.utc),
                ),
            )
            self._write(extracted)
            publish_manifest = asset_publish_manifest(prepared_payload)
            if publish_manifest:
                try:
                    published = (self._asset_publisher or QEArchiveAssetPublisher()).publish(
                        run_id=extracted.run.run_id,
                        value_class=lifecycle.value_class.value,
                        retention_class=lifecycle.retention_class,
                        manifest=publish_manifest,
                    )
                except QEArchiveAssetPublishError as exc:
                    lifecycle = lifecycle.with_persistence(
                        warehouse_status=QEWarehouseStatus.PERSISTED,
                        asset_status=(
                            QEAssetStatus.PARTIAL
                            if exc.published_artifacts
                            else QEAssetStatus.FAILED
                        ),
                        protected_owner_count=(1 if exc.published_artifacts else 0),
                        asset_reason_code=exc.reason_code,
                    )
                    prepared_payload = attach_lifecycle(prepared_payload, lifecycle)
                    extracted = self._extractor.extract(
                        prepared_payload,
                        event_type=event_type,
                        source_system=source_system,
                        source_id=source_id,
                        source_sub_id=source_sub_id,
                    )
                    extracted = replace(
                        extracted,
                        run=replace(
                            extracted.run,
                            archived_at=extracted.run.archived_at or datetime.now(timezone.utc),
                        ),
                    )
                    self._write(extracted)
                    if exc.published_artifacts:
                        self._repository.upsert_artifact_manifest(
                            extracted.run.run_id,
                            list(exc.published_artifacts),
                            replace_existing=True,
                        )
                    raise
                lifecycle = lifecycle.with_persistence(
                    warehouse_status=QEWarehouseStatus.PERSISTED,
                    asset_status=QEAssetStatus.PUBLISHED,
                    protected_owner_count=1,
                    asset_reason_code="qe_asset_publish_complete",
                )
                prepared_payload = attach_lifecycle(prepared_payload, lifecycle)
                extracted = self._extractor.extract(
                    prepared_payload,
                    event_type=event_type,
                    source_system=source_system,
                    source_id=source_id,
                    source_sub_id=source_sub_id,
                )
                extracted = replace(
                    extracted,
                    run=replace(
                        extracted.run,
                        archived_at=extracted.run.archived_at or datetime.now(timezone.utc),
                    ),
                )
                self._write(extracted)
                self._repository.upsert_artifact_manifest(
                    extracted.run.run_id,
                    list(published.artifacts),
                    replace_existing=True,
                )
                stats["asset_publish"] = {
                    "status": published.status,
                    "published_count": published.published_count,
                    "reused_count": published.reused_count,
                }
                stats["asset_lifecycle"] = lifecycle.as_dict()
            stats["written"] = True
        else:
            stats["written"] = False

        return ArchivePayloadResult(
            run_id=extracted.run.run_id,
            dry_run=dry_run,
            stats=stats,
            extracted=extracted,
        )

    def link_prediction_artifacts_for_run(
        self,
        run: Mapping[str, Any],
        *,
        dry_run: bool = True,
        verify_sha256: bool = True,
    ) -> dict[str, Any]:
        """Idempotently attach an existing Prediction Store manifest to a run.

        This path changes only ``run_artifact`` pointers (and the corresponding
        source URI when rows are written). It never re-uploads blobs or rewrites
        QE metrics, curves, trades, factors, or raw payloads.
        """

        identity = dict(run)
        run_id = str(identity.get("run_id") or "").strip()
        if not run_id:
            raise ValueError("archive artifact linking requires run_id")
        task_id = str(identity.get("task_id") or "").strip() or None
        loop_index = _optional_int(identity.get("loop_index"))
        resolution = self._model_store_service.resolve_archive_manifest(
            run_id=run_id,
            task_id=task_id,
            loop_index=loop_index,
            verify_sha256=verify_sha256,
        )
        manifest = resolution.get("manifest")
        result = {
            "run_id": run_id,
            "task_id": task_id,
            "loop_index": loop_index,
            "resolution_status": resolution.get("status"),
            "selected_run_key": resolution.get("selected_run_key"),
            "artifact_count": int(resolution.get("artifact_count") or 0),
            "errors": list(resolution.get("errors") or []),
            "dry_run": dry_run,
        }
        if not isinstance(manifest, Mapping):
            result["action_status"] = str(resolution.get("status") or "missing")
            result["written_count"] = 0
            return result

        artifact_payload = {
            "run_id": run_id,
            "source_system": identity.get("source_system") or "qe_archive",
            "source_id": task_id or identity.get("experiment_id") or run_id,
            "source_sub_id": identity.get("loop_id") or loop_index,
            "logical_experiment_id": identity.get("logical_experiment_id") or run_id,
            "experiment_id": identity.get("experiment_id") or run_id,
            "task_id": task_id,
            "loop_id": identity.get("loop_id"),
            "loop_index": loop_index,
            "run_type": identity.get("run_type") or "evolution_loop",
            "status": identity.get("status") or "completed",
            "config": {
                "data_context": {
                    "freq": identity.get("freq") or "day",
                    "label_horizon": identity.get("label_horizon"),
                    "limit_suspend_authoritative": False,
                }
            },
            "prediction_store_manifest": dict(manifest),
            "mlflow_artifact_uri": manifest.get("mlflow_artifact_uri") or manifest.get("uri"),
        }
        desired = self._extractor.extract(artifact_payload).artifact_manifest
        existing = self._repository.list_artifact_manifest(run_id)
        if _artifact_records_cover(existing, desired):
            result["action_status"] = (
                "already_linked_partial" if resolution.get("status") == "partial" else "already_linked"
            )
            result["written_count"] = 0
            return result
        if dry_run:
            result["action_status"] = (
                "would_link_partial" if resolution.get("status") == "partial" else "would_link"
            )
            result["written_count"] = 0
            return result

        written = self._repository.upsert_artifact_manifest(run_id, desired, replace_existing=True)
        self._repository.update_run_source_artifact_uri(
            run_id,
            str(manifest.get("mlflow_artifact_uri") or manifest.get("uri") or ""),
        )
        result["action_status"] = "linked_partial" if resolution.get("status") == "partial" else "linked"
        result["written_count"] = written
        return result

    def _attach_prediction_store_manifest(
        self,
        payload: Mapping[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        prepared = dict(payload)
        config = prepared.get("config") or {}
        flags = (config.get("runtime_flags") or {}) if isinstance(config, Mapping) else {}
        if isinstance(flags, Mapping) and PREPARED_WORKSPACE_PARAM in flags:
            return self._attach_prepared_workspace_assets(prepared, flags)
        existing_manifest = _find_prediction_store_manifest(prepared)
        if existing_manifest is not None:
            return prepared, {
                "status": "payload_manifest",
                "selected_run_key": existing_manifest.get("run_key_safe") or existing_manifest.get("run_key"),
                "artifact_count": len(existing_manifest.get("artifacts") or []),
                "errors": [],
            }

        task_id = str(prepared.get("task_id") or prepared.get("source_id") or "").strip() or None
        loop_index = _optional_int(prepared.get("loop_index"))
        run_id = str(prepared.get("run_id") or "").strip()
        if not run_id and not (task_id and loop_index is not None):
            return prepared, {
                "status": "not_applicable",
                "selected_run_key": None,
                "artifact_count": 0,
                "errors": [],
            }
        try:
            resolution = self._model_store_service.resolve_archive_manifest(
                run_id=run_id,
                task_id=task_id,
                loop_index=loop_index,
                verify_sha256=True,
            )
        except Exception as exc:  # pragma: no cover - defensive boundary around optional artifact enrichment.
            logger.exception("Prediction Store manifest resolution failed for run=%s task=%s loop=%s", run_id, task_id, loop_index)
            return prepared, {
                "status": "failed",
                "selected_run_key": None,
                "artifact_count": 0,
                "errors": [f"{type(exc).__name__}: {exc}"],
            }
        manifest = resolution.get("manifest")
        if isinstance(manifest, Mapping):
            prepared["prediction_store_manifest"] = dict(manifest)
            prepared["mlflow_artifact_uri"] = manifest.get("mlflow_artifact_uri") or manifest.get("uri")
        return prepared, resolution

    def _attach_prepared_workspace_assets(
        self, payload: dict[str, Any], flags: Mapping[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Keep replay recorder identity separate from the verified training producer."""
        task, loop_id = payload.get("task_id"), payload.get("loop_id")
        loop = prepared_workspace_loop_index(flags, task_id=task, loop_id=loop_id)
        mode = flags[PREPARED_WORKSPACE_PARAM]["workload"]
        payload["loop_index"] = loop
        for key in ("prediction_store_manifest", "mlflow_artifact_uri", "prediction_replay_lineage"):
            payload.pop(key, None)
        if mode == PREPARED_ABSOLUTE_TARGET_MODE:
            # The account uses frozen targets. It has no prediction producer,
            # and any recorder supplied by the account remains its own.
            payload.pop("prediction_replay_source_ref", None)
            payload["prepared_workspace_asset_resolution"] = "not_applicable"
            return payload, {"status": "not_applicable", "errors": []}
        for key in ("recorder_id", "recorder_experiment_id"):
            payload.pop(key, None)

        def exact_manifest(task_id, index, runner_mode):
            if not task_id or not index:
                return {}, {"status": "workspace_identity_missing", "errors": []}
            try:
                resolution = self._model_store_service.resolve_archive_manifest(
                    run_id="", task_id=task_id, loop_index=index, verify_sha256=True,
                )
            except Exception as exc:  # Optional asset read never changes the completed account.
                logger.warning("Prepared QE asset lookup failed for %s/Loop%s: %s", task_id, index, exc)
                return {}, {"status": "failed", "errors": [str(exc)]}
            manifest = resolution.get("manifest") or {}
            metadata = manifest.get("metadata") or {}
            if manifest and not (
                metadata.get("task_id") == task_id
                and _optional_int(metadata.get("loop_index")) == index
                and metadata.get("mode") == runner_mode
                and metadata.get("recorder_id") and metadata.get("recorder_experiment_id")
            ):
                return {}, {"status": "workspace_manifest_identity_mismatch", "errors": []}
            return manifest, resolution

        runner_mode = "pred_backtest" if mode == "PREDICTION_REPLAY" else "train_only"
        account, resolution = exact_manifest(task, loop, runner_mode)
        payload["prepared_workspace_asset_resolution"] = resolution["status"]
        if not account:
            return payload, resolution
        account_meta = account["metadata"]
        payload["recorder_id"] = account_meta["recorder_id"]
        payload["recorder_experiment_id"] = account_meta["recorder_experiment_id"]
        primary = account
        if mode == "PREDICTION_REPLAY":
            account_pred = _manifest_artifact(account, "prediction")
            lineage: dict[str, Any] = {
                "replay": {
                    "task_id": task, "loop_index": loop,
                    "recorder_id": account_meta["recorder_id"],
                    "recorder_experiment_id": account_meta["recorder_experiment_id"],
                    "artifact_uri": account.get("uri"), "prediction_sha256": account_pred.get("sha256"),
                },
                "producer_status": "not_declared",
            }
            ref = normalize_archive_replay_source(
                payload.get("prediction_replay_source_ref") or flags.get(QE_ARCHIVE_REPLAY_SOURCE_PARAM)
            )
            if ref:
                producer_task, producer_loop = ref["source_task_id"], ref["source_loop_index"]
                source, source_resolution = exact_manifest(producer_task, producer_loop, "train_only")
                source_pred = _manifest_artifact(source, "prediction")
                weights = _manifest_artifact(source, "model_params")
                if (account_pred.get("sha256") == ref["sha256"] == source_pred.get("sha256")
                        and source_pred.get("size_bytes") == ref["size_bytes"] and weights.get("sha256")):
                    primary = source
                    resolution = source_resolution
                    lineage.update(producer_status="verified", producer={
                        "task_id": producer_task, "loop_index": producer_loop,
                        "recorder_id": source["metadata"]["recorder_id"],
                        "recorder_experiment_id": source["metadata"]["recorder_experiment_id"],
                        "artifact_uri": source.get("uri"), "prediction_sha256": ref["sha256"],
                        "model_params_sha256": weights["sha256"],
                    })
                else:
                    lineage["producer_status"] = "unverified"
            payload["prediction_replay_lineage"] = lineage
        payload["prediction_store_manifest"] = primary
        payload["mlflow_artifact_uri"] = primary.get("mlflow_artifact_uri") or primary.get("uri")
        return payload, resolution

    def _write(self, extracted: ExtractedArchivePayload) -> None:
        repo = self._repository
        run_id = repo.upsert_run(extracted.run)
        repo.upsert_run_source(extracted.source)
        repo.upsert_run_config(extracted.config)
        repo.upsert_reproducibility_manifest(extracted.reproducibility_manifest)
        repo.upsert_artifact_manifest(run_id, extracted.artifact_manifest, replace_existing=True)
        for context in extracted.data_contexts:
            repo.upsert_data_context(context)
        if extracted.account_summary is not None:
            repo.upsert_account_summary(extracted.account_summary)
        repo.upsert_metric_batch(extracted.metrics, replace_existing=True)
        repo.replace_run_curves(run_id, extracted.curves)
        repo.replace_run_factors(run_id, extracted.factors)
        repo.replace_run_factor_importance(run_id, extracted.factor_importance)
        repo.replace_run_symbol_summaries(run_id, extracted.symbol_summaries)
        repo.replace_run_trades(run_id, extracted.trades)
        repo.replace_run_execution_events(run_id, extracted.execution_events)
        repo.replace_raw_payloads(run_id, extracted.raw_payloads)
        task_id = getattr(extracted.run, "task_id", None)
        loop_index = getattr(extracted.run, "loop_index", None)
        if task_id and loop_index is not None and self._resource_phase_service is not None:
            try:
                self._resource_phase_service.bind_archive_run(
                    task_id=str(task_id),
                    loop_index=int(loop_index),
                    archive_run_id=run_id,
                    attempt_no=getattr(extracted.run, "attempt_no", None),
                )
            except QEResourcePhaseError as exc:
                if exc.reason_code != RESOURCE_SCHEMA_REASON:
                    raise
                logger.warning(
                    "%s: resource telemetry schema is not deployed; archive run binding skipped for %s",
                    RESOURCE_SCHEMA_REASON,
                    run_id,
                )


def _manifest_artifact(manifest: Mapping[str, Any], artifact_type: str) -> Mapping[str, Any]:
    matches = [entry for entry in (manifest.get("artifacts") or [])
               if isinstance(entry, Mapping) and entry.get("artifact_type") == artifact_type]
    return matches[0] if len(matches) == 1 else {}


def _find_prediction_store_manifest(payload: Mapping[str, Any]) -> Mapping[str, Any] | None:
    candidates: list[Any] = [payload.get("prediction_store_manifest")]
    config = payload.get("config") if isinstance(payload.get("config"), Mapping) else {}
    metrics = payload.get("metrics") if isinstance(payload.get("metrics"), Mapping) else {}
    enhanced = metrics.get("enhanced_metrics") if isinstance(metrics.get("enhanced_metrics"), Mapping) else {}
    candidates.extend(
        [
            config.get("prediction_store_manifest"),
            metrics.get("prediction_store_manifest"),
            enhanced.get("prediction_store_manifest"),
        ]
    )
    return next(
        (
            candidate
            for candidate in candidates
            if isinstance(candidate, Mapping)
            and (candidate.get("artifacts") or candidate.get("uri") or candidate.get("mlflow_artifact_uri"))
        ),
        None,
    )


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _artifact_records_cover(existing: list[Mapping[str, Any]], desired: list[Mapping[str, Any]]) -> bool:
    existing_by_key = {
        (str(item.get("artifact_type") or ""), str(item.get("artifact_name") or "")): item
        for item in existing
    }
    if not desired:
        return False
    for item in desired:
        key = (str(item.get("artifact_type") or ""), str(item.get("artifact_name") or ""))
        current = existing_by_key.get(key)
        if current is None:
            return False
        if str(current.get("artifact_uri") or "") != str(item.get("artifact_uri") or ""):
            return False
        if str(current.get("sha256") or "").lower() != str(item.get("sha256") or "").lower():
            return False
        if int(current.get("size_bytes") or 0) != int(item.get("size_bytes") or 0):
            return False
    return True


def _compact_artifact_resolution(resolution: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "status": resolution.get("status"),
        "selected_run_key": resolution.get("selected_run_key"),
        "artifact_count": int(resolution.get("artifact_count") or 0),
        "errors": list(resolution.get("errors") or []),
    }
