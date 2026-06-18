"""Embedded QE prediction artifact store routes.

These routes are part of the AIstock backend process; P2 does not start an
MLflow server, MinIO, SMB share, or PG-backed MLflow tracking service.
"""

from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse
from psycopg2.extras import Json

from backend.db.pg_pool import get_conn
from backend.services.model_store import ModelStoreService, PredictionArtifactStore, PredictionStoreError
from backend.services.model_store.artifact_store import artifact_uri
from backend.services.qe_archive.repository import QEArchiveRepository


router = APIRouter(prefix="/prediction-store", tags=["prediction-store"])


def get_model_store_service() -> ModelStoreService:
    return ModelStoreService()


@router.get("/health", summary="Prediction artifact store health")
def get_prediction_store_health() -> dict[str, Any]:
    return {"status": "success", "data": get_model_store_service().health()}


@router.post("/artifacts/{run_key}", summary="Upload QE pred.pkl/label.pkl/params.pkl into the AIstock artifact store")
def upload_prediction_artifacts(
    run_key: str,
    pred: UploadFile | None = File(default=None),
    params: UploadFile | None = File(default=None),
    label: UploadFile | None = File(default=None),
    metadata_json: str | None = Form(default=None),
    experiment_id: str | None = Form(default=None),
    task_id: str | None = Form(default=None),
    loop_id: str | None = Form(default=None),
    loop_index: int | None = Form(default=None),
    recorder_id: str | None = Form(default=None),
    recorder_experiment_id: str | None = Form(default=None),
    source_node_id: str | None = Form(default=None),
) -> dict[str, Any]:
    metadata = _parse_metadata_json(metadata_json)
    metadata.update(
        {
            key: value
            for key, value in {
                "experiment_id": experiment_id,
                "task_id": task_id,
                "loop_id": loop_id,
                "loop_index": loop_index,
                "recorder_id": recorder_id,
                "recorder_experiment_id": recorder_experiment_id,
                "source_node_id": source_node_id,
            }.items()
            if value not in (None, "", [], {})
        }
    )
    files: dict[str, tuple[str, Any]] = {}
    if pred is not None:
        files["prediction"] = (pred.filename or "pred.pkl", pred.file)
    if params is not None:
        files["model_params"] = (params.filename or "params.pkl", params.file)
    if label is not None:
        files["label"] = (label.filename or "label.pkl", label.file)
    if not files:
        raise HTTPException(status_code=400, detail="upload requires pred, params, and/or label file")

    try:
        manifest = PredictionArtifactStore().write_artifacts(run_key=run_key, files=files, metadata=metadata)
        warehouse_status = _try_write_archive_pointer(run_key, manifest)
    except PredictionStoreError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"prediction artifact upload failed: {type(exc).__name__}: {exc}") from exc

    return {
        "status": "success",
        "data": {
            "manifest": manifest,
            "mlflow_artifact_uri": manifest.get("mlflow_artifact_uri"),
            "warehouse_write": warehouse_status,
        },
    }


@router.get("/pointers/{run_id}", summary="Get prediction-store pointer by qe_archive run_id")
def get_prediction_store_pointer(
    run_id: str,
    experiment_id: str | None = Query(None),
) -> dict[str, Any]:
    return {"status": "success", "data": get_model_store_service().get_pointer(run_id=run_id, experiment_id=experiment_id)}


@router.get("/pointers/by-experiment/{experiment_id}", summary="Get prediction-store pointer by experiment_id")
def get_prediction_store_pointer_by_experiment(experiment_id: str) -> dict[str, Any]:
    return {"status": "success", "data": get_model_store_service().get_pointer(experiment_id=experiment_id)}


@router.get("/pred/{run_id}", summary="Preview persisted pred.pkl for one run")
def preview_prediction_artifact(
    run_id: str,
    head: int = Query(5, ge=0, le=1000),
) -> dict[str, Any]:
    try:
        return {"status": "success", "data": get_model_store_service().pull_pred(run_id=run_id, head=head)}
    except PredictionStoreError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/params/{run_id}", summary="Get metadata for persisted params.pkl for one run")
def preview_model_params_artifact(run_id: str) -> dict[str, Any]:
    try:
        return {"status": "success", "data": get_model_store_service().pull_params(run_id=run_id)}
    except PredictionStoreError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/label/{run_id}", summary="Get metadata for persisted label.pkl for one run")
def preview_label_artifact(run_id: str) -> dict[str, Any]:
    try:
        return {"status": "success", "data": get_model_store_service().pull_label(run_id=run_id)}
    except PredictionStoreError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/artifacts/{run_id}/{artifact_type}", summary="Download persisted pred.pkl, label.pkl, or params.pkl")
def download_prediction_artifact(run_id: str, artifact_type: str) -> FileResponse:
    normalized = _normalize_artifact_type(artifact_type)
    service = get_model_store_service()
    try:
        if normalized == "prediction":
            path = service.prediction_path(run_id=run_id)
            filename = "pred.pkl"
        elif normalized == "model_params":
            path = service.pull_params_path(run_id=run_id)
            filename = "params.pkl"
        else:
            path = service.label_path(run_id=run_id)
            filename = "label.pkl"
    except PredictionStoreError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return FileResponse(path, media_type="application/octet-stream", filename=filename)


def _parse_metadata_json(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"metadata_json is invalid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise HTTPException(status_code=400, detail="metadata_json must be a JSON object")
    return parsed


def _normalize_artifact_type(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"prediction", "pred", "pred.pkl"}:
        return "prediction"
    if normalized in {"model_params", "params", "params.pkl", "params_pkl"}:
        return "model_params"
    if normalized in {"label", "label.pkl"}:
        return "label"
    raise HTTPException(status_code=400, detail=f"unsupported artifact_type={value!r}")


def _try_write_archive_pointer(run_key: str, manifest: dict[str, Any]) -> dict[str, Any]:
    """Write qe_archive pointers only when the run already exists.

    Runner uploads can happen before archive ingestion creates qe_archive.run.
    That is a valid forward-only state, so it is reported as skipped rather
    than silently pretending the PG pointer was written.
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM qe_archive.run WHERE run_id = %s", (run_key,))
            exists = cur.fetchone() is not None
    if not exists:
        return {
            "status": "skipped_no_qe_archive_run",
            "run_id": run_key,
            "reason": "archive ingestion will attach the manifest after qe_archive.run exists",
        }

    artifacts = [_manifest_item_to_run_artifact(item) for item in manifest.get("artifacts", []) if isinstance(item, dict)]
    repo = QEArchiveRepository()
    written = repo.upsert_artifact_manifest(run_key, artifacts, replace_existing=True) if artifacts else 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE qe_archive.run_source
                SET mlflow_artifact_uri = COALESCE(mlflow_artifact_uri, %s),
                    metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb
                WHERE run_id = %s
                """,
                (
                    manifest.get("mlflow_artifact_uri"),
                    Json({"prediction_store": _source_metadata(manifest)}),
                    run_key,
                ),
            )
            updated_sources = cur.rowcount
    return {
        "status": "written",
        "run_id": run_key,
        "artifact_count": written,
        "updated_run_source_count": updated_sources,
    }


def _manifest_item_to_run_artifact(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "artifact_type": item.get("artifact_type"),
        "artifact_name": item.get("artifact_name") or item.get("artifact_type"),
        "storage_tier": item.get("storage_tier") or "hot",
        "artifact_uri": item.get("uri") or artifact_uri(str(item.get("run_key_safe") or ""), str(item.get("artifact_type") or "")),
        "source_system": "prediction_store",
        "source_uri": item.get("source_api"),
        "source_node_id": item.get("source_node_id"),
        "sha256": item.get("sha256"),
        "size_bytes": item.get("size_bytes"),
        "content_type": item.get("content_type"),
        "collected_status": item.get("collection_status") or "available",
        "collected_at": item.get("created_at"),
        "parser_status": item.get("parser_status") or "not_required",
        "parser_error": (item.get("metadata") or {}).get("parser_error") if isinstance(item.get("metadata"), dict) else None,
        "metadata": item.get("metadata") or {},
    }


def _source_metadata(manifest: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": manifest.get("schema_version"),
        "uri": manifest.get("uri"),
        "updated_at": manifest.get("updated_at"),
        "artifact_count": len(manifest.get("artifacts") or []),
    }
