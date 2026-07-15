"""Manual QE archive payload processing service.

This service is intentionally not wired into QE webhooks or FastAPI startup.
Callers must pass payloads that have already been collected from DB/API paths.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, Mapping

from backend.services.model_store.service import ModelStoreService

from .payload_extractor import ExtractedArchivePayload, QEArchivePayloadExtractor
from .repository import QEArchiveRepository
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
    ) -> None:
        self._repository = repository or QEArchiveRepository()
        self._extractor = extractor or QEArchivePayloadExtractor()
        self._model_store_service = model_store_service or ModelStoreService()
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
            }
        )

        if not dry_run:
            extracted = replace(
                extracted,
                run=replace(
                    extracted.run,
                    archived_at=extracted.run.archived_at or datetime.now(timezone.utc),
                ),
            )
            self._write(extracted)
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
