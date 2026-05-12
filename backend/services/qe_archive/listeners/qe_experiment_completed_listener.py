"""QE experiment completion listener for execution model cache sync.

The listener is deliberately opt-in. A completed QE experiment can publish a
thin outbox payload that points to explicit model files, and this listener can
plan the cache sync without touching DB state or model bytes. Applying the sync
requires both payload intent and listener-side permission.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from backend.services.qe_archive.models import ClaimedOutboxEvent
from backend.services.qe_archive.worker import ArchiveWorkerEventResult
from scripts import sync_qe_models_to_aistock_cache as model_sync


QE_EXPERIMENT_COMPLETED_EVENT_TYPE = "qe.experiment.completed"
MODEL_SYNC_SCHEMA_VERSION = "qe.experiment.completed.model_sync.v1"


class QEExperimentCompletedModelSyncError(ValueError):
    """Raised when the outbox payload is not safe to process."""


@dataclass(frozen=True)
class QEExperimentModelSyncRequest:
    source_dir: Path
    cache_root: Path
    algo_code: str
    models: tuple[str, ...]
    expected_hashes: Mapping[str, str]
    overwrite: bool = False
    apply: bool = False
    wsl_distro: str | None = None


class QEExperimentCompletedModelSyncListener:
    """Plan or apply model sync for `qe.experiment.completed` events."""

    event_type = QE_EXPERIMENT_COMPLETED_EVENT_TYPE

    def __init__(self, *, allow_apply: bool = False) -> None:
        self.allow_apply = allow_apply

    def can_handle(self, event: ClaimedOutboxEvent) -> bool:
        payload = dict(event.payload or {})
        return (
            event.event_type == self.event_type
            and payload.get("schema_version") == MODEL_SYNC_SCHEMA_VERSION
            and payload.get("routing_class") == "model_sync"
        )

    def handle(self, event: ClaimedOutboxEvent) -> ArchiveWorkerEventResult:
        if not self.can_handle(event):
            return ArchiveWorkerEventResult(
                success=False,
                error="unsupported qe experiment completed model sync event",
            )

        try:
            request = parse_model_sync_request(event.payload)
            plans = model_sync.build_plan(
                source_dir=request.source_dir,
                cache_root=request.cache_root,
                algo_code=request.algo_code,
                models=request.models,
                expected_hashes=dict(request.expected_hashes),
                overwrite=request.overwrite,
            )
            applied: list[dict[str, object]] = []
            if request.apply:
                if not self.allow_apply:
                    raise QEExperimentCompletedModelSyncError(
                        "payload requested apply but listener allow_apply is false"
                    )
                applied = model_sync.apply_plan(
                    plans,
                    algo_code=request.algo_code,
                    source_dir=request.source_dir,
                    cache_root=request.cache_root,
                )
        except Exception as exc:
            return ArchiveWorkerEventResult(
                success=False,
                run_id=event.source_id,
                error=f"{type(exc).__name__}: {exc}",
            )

        return ArchiveWorkerEventResult(
            success=True,
            run_id=event.source_id,
            stats={
                "mode": "apply" if request.apply else "dry_run",
                "algo_code": request.algo_code,
                "model_count": len(request.models),
                "plans": [model_sync.asdict(plan) for plan in plans],
                "applied": applied,
            },
        )


def parse_model_sync_request(payload: Mapping[str, Any]) -> QEExperimentModelSyncRequest:
    body = payload.get("model_sync")
    if not isinstance(body, Mapping):
        raise QEExperimentCompletedModelSyncError("payload.model_sync must be an object")

    source_dir_raw = str(body.get("source_dir") or "").strip()
    cache_root_raw = str(body.get("cache_root") or "").strip()
    algo_code = str(body.get("algo_code") or "").strip().upper()
    models_raw = body.get("models")
    if not source_dir_raw or not cache_root_raw or not algo_code:
        raise QEExperimentCompletedModelSyncError("model_sync requires source_dir, cache_root, and algo_code")
    if not isinstance(models_raw, list) or not models_raw:
        raise QEExperimentCompletedModelSyncError("model_sync.models must be a non-empty list")

    wsl_distro = body.get("wsl_distro")
    expected_hashes_raw = body.get("expected_sha256") or {}
    if not isinstance(expected_hashes_raw, Mapping):
        raise QEExperimentCompletedModelSyncError("model_sync.expected_sha256 must be an object when provided")
    expected_hashes = model_sync.parse_expected_hashes(
        [f"{name}={value}" for name, value in expected_hashes_raw.items()]
    )
    models = tuple(model_sync._validate_model_name(str(model)) for model in models_raw)

    return QEExperimentModelSyncRequest(
        source_dir=model_sync.normalize_input_path(source_dir_raw, wsl_distro=str(wsl_distro) if wsl_distro else None),
        cache_root=model_sync.normalize_input_path(cache_root_raw, wsl_distro=str(wsl_distro) if wsl_distro else None),
        algo_code=model_sync._validate_algo_code(algo_code),
        models=models,
        expected_hashes=expected_hashes,
        overwrite=_optional_bool(body.get("overwrite"), field_name="model_sync.overwrite"),
        apply=_optional_bool(body.get("apply"), field_name="model_sync.apply"),
        wsl_distro=str(wsl_distro) if wsl_distro else None,
    )


def _optional_bool(value: Any, *, field_name: str) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    raise QEExperimentCompletedModelSyncError(f"{field_name} must be a JSON boolean when provided")


def qe_experiment_completed_model_sync_handler(
    *,
    allow_apply: bool = False,
) -> QEExperimentCompletedModelSyncListener:
    return QEExperimentCompletedModelSyncListener(allow_apply=allow_apply)
