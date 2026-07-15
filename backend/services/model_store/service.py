"""Model-store read facade for QE prediction artifacts."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping

from psycopg2.extras import RealDictCursor

from backend.db.pg_pool import get_conn
from backend.services.model_store.artifact_store import (
    PredictionArtifactStore,
    PredictionStoreError,
    PredictionStoreNotFound,
    STORE_URI_SCHEME,
    run_uri,
)

PULL_MAX_BYTES_ENV = "AISTOCK_PREDICTION_STORE_PULL_MAX_BYTES"
DEFAULT_PULL_MAX_BYTES = 512 * 1024 * 1024


class ModelStoreService:
    """Join qe_archive pointers with the P2 artifact store."""

    def __init__(self, *, artifact_store: PredictionArtifactStore | None = None, connection_provider=get_conn) -> None:
        self.artifact_store = artifact_store or PredictionArtifactStore()
        self._connection_provider = connection_provider

    def health(self) -> dict[str, Any]:
        return {
            "status": "success",
            "tracking_backend": "deferred_to_m4",
            "mlflow_pg_enabled": False,
            "artifact_store": self.artifact_store.health(),
        }

    def resolve_archive_manifest(
        self,
        *,
        run_id: str,
        task_id: str | None = None,
        loop_index: int | None = None,
        verify_sha256: bool = True,
    ) -> dict[str, Any]:
        """Resolve and validate the Prediction Store manifest for one archive run.

        QE uploads use ``<task_id>_L<loop_index>`` before the archive run id
        exists. Older backfills can instead use the final ``qear_run_*`` key.
        Both identities are therefore deterministic candidates; no directory
        scan or workspace fallback is performed here.
        """

        candidates = _archive_manifest_candidates(run_id=run_id, task_id=task_id, loop_index=loop_index)
        attempts: list[dict[str, Any]] = []
        for candidate in candidates:
            try:
                manifest = self.artifact_store.load_manifest(candidate, missing_ok=True)
            except (PredictionStoreError, OSError) as exc:
                attempts.append(
                    {
                        "run_key": candidate,
                        "status": "corrupt",
                        "errors": [f"{type(exc).__name__}: {exc}"],
                    }
                )
                continue
            if manifest is None:
                attempts.append({"run_key": candidate, "status": "missing", "errors": []})
                continue

            validation = self._validate_archive_manifest(
                candidate,
                manifest,
                expected_task_id=task_id,
                expected_loop_index=loop_index,
                verify_sha256=verify_sha256,
            )
            attempts.append(
                {
                    "run_key": candidate,
                    "status": validation["status"],
                    "artifact_count": len(validation["valid_artifacts"]),
                    "errors": validation["errors"],
                }
            )
            if validation["valid_artifacts"]:
                resolved_manifest = dict(manifest)
                resolved_manifest["artifacts"] = validation["valid_artifacts"]
                return {
                    "status": validation["status"],
                    "run_id": run_id,
                    "selected_run_key": candidate,
                    "candidate_run_keys": candidates,
                    "artifact_count": len(validation["valid_artifacts"]),
                    "errors": validation["errors"],
                    "attempts": attempts,
                    "manifest": resolved_manifest,
                }

        final_status = "corrupt" if any(item["status"] == "corrupt" for item in attempts) else "missing"
        return {
            "status": final_status,
            "run_id": run_id,
            "selected_run_key": None,
            "candidate_run_keys": candidates,
            "artifact_count": 0,
            "errors": [error for item in attempts for error in item.get("errors", [])],
            "attempts": attempts,
            "manifest": None,
        }

    def _validate_archive_manifest(
        self,
        run_key: str,
        manifest: Mapping[str, Any],
        *,
        expected_task_id: str | None,
        expected_loop_index: int | None,
        verify_sha256: bool,
    ) -> dict[str, Any]:
        errors: list[str] = []
        metadata = manifest.get("metadata") if isinstance(manifest.get("metadata"), Mapping) else {}
        manifest_task_id = str(metadata.get("task_id") or "").strip()
        if expected_task_id and manifest_task_id and manifest_task_id != expected_task_id:
            errors.append(
                f"manifest task_id mismatch: expected={expected_task_id!r} actual={manifest_task_id!r}"
            )
        manifest_loop_index = metadata.get("loop_index")
        if expected_loop_index is not None and manifest_loop_index not in (None, ""):
            try:
                normalized_loop_index = int(manifest_loop_index)
            except (TypeError, ValueError):
                errors.append(f"manifest loop_index is invalid: {manifest_loop_index!r}")
            else:
                if normalized_loop_index != int(expected_loop_index):
                    errors.append(
                        "manifest loop_index mismatch: "
                        f"expected={int(expected_loop_index)} actual={normalized_loop_index}"
                    )
        if errors:
            return {"status": "corrupt", "valid_artifacts": [], "errors": errors}

        raw_artifacts = manifest.get("artifacts")
        if not isinstance(raw_artifacts, list) or not raw_artifacts:
            return {
                "status": "corrupt",
                "valid_artifacts": [],
                "errors": ["prediction-store manifest has no artifact entries"],
            }

        valid_artifacts: list[dict[str, Any]] = []
        for index, raw_item in enumerate(raw_artifacts):
            if not isinstance(raw_item, Mapping):
                errors.append(f"artifact[{index}] is not an object")
                continue
            item = dict(raw_item)
            artifact_type = str(item.get("artifact_type") or "").strip()
            artifact_name = str(item.get("artifact_name") or "").strip()
            expected_sha256 = str(item.get("sha256") or "").strip().lower()
            expected_size = item.get("size_bytes")
            item_label = artifact_type or artifact_name or f"artifact[{index}]"
            if not artifact_type or not artifact_name:
                errors.append(f"{item_label}: artifact_type and artifact_name are required")
                continue
            if len(expected_sha256) != 64 or any(ch not in "0123456789abcdef" for ch in expected_sha256):
                errors.append(f"{item_label}: sha256 is missing or invalid")
                continue
            try:
                normalized_size = int(expected_size)
            except (TypeError, ValueError):
                errors.append(f"{item_label}: size_bytes is missing or invalid")
                continue
            try:
                path = self.artifact_store.resolve_artifact_path(
                    str(manifest.get("uri") or run_key),
                    artifact_type=artifact_type,
                )
                actual_size = path.stat().st_size
                if actual_size != normalized_size:
                    raise PredictionStoreError(
                        f"size mismatch expected={normalized_size} actual={actual_size}"
                    )
                if verify_sha256:
                    actual_sha256 = _sha256_file(path)
                    if actual_sha256 != expected_sha256:
                        raise PredictionStoreError(
                            f"sha256 mismatch expected={expected_sha256} actual={actual_sha256}"
                        )
            except (PredictionStoreError, OSError) as exc:
                errors.append(f"{item_label}: {type(exc).__name__}: {exc}")
                continue
            valid_artifacts.append(item)

        if valid_artifacts and errors:
            status = "partial"
        elif valid_artifacts:
            status = "available"
        else:
            status = "corrupt"
        return {"status": status, "valid_artifacts": valid_artifacts, "errors": errors}

    def get_pointer(self, *, run_id: str | None = None, experiment_id: str | None = None) -> dict[str, Any]:
        if not run_id and not experiment_id:
            raise ValueError("run_id or experiment_id is required")

        run = self._find_run(run_id=run_id, experiment_id=experiment_id)
        if not run:
            fallback = self._store_only_pointer(run_id) if run_id else None
            if fallback:
                return fallback
            return {
                "pointer_status": "missing",
                "warehouse_found": False,
                "run_id": run_id,
                "experiment_id": experiment_id,
                "reason": "qe_archive.run row not found",
            }

        source = self._latest_source(str(run["run_id"]))
        artifacts = self._artifacts(str(run["run_id"]))
        store_uri = (source or {}).get("mlflow_artifact_uri") or _first_artifact_run_uri(artifacts)
        manifest = None
        manifest_error = None
        if store_uri:
            try:
                manifest = self.artifact_store.load_manifest(str(store_uri))
            except PredictionStoreNotFound as exc:
                manifest_error = f"not_found: {exc}"
            except PredictionStoreError as exc:
                manifest_error = f"invalid_pointer: {exc}"

        return {
            "pointer_status": "available" if store_uri and manifest_error is None else "missing_or_unreadable",
            "warehouse_found": True,
            "run": dict(run),
            "source": source,
            "artifacts": artifacts,
            "mlflow_artifact_uri": store_uri,
            "prediction_store_manifest": manifest,
            "manifest_error": manifest_error,
        }

    def pull_pred(self, *, run_id: str, head: int | None = 5) -> dict[str, Any]:
        pointer = self.get_pointer(run_id=run_id)
        store_uri = pointer.get("mlflow_artifact_uri")
        if not store_uri:
            raise PredictionStoreNotFound(f"run_id={run_id} has no prediction-store pointer")
        path = self.artifact_store.resolve_artifact_path(str(store_uri), artifact_type="prediction", artifact_name="pred.pkl")
        preview = self._pickle_preview(path, head=head)
        return {
            "run_id": run_id,
            "artifact_type": "prediction",
            "artifact_path": str(path),
            "pointer": pointer,
            **preview,
        }

    def pull_params(self, *, run_id: str) -> dict[str, Any]:
        pointer = self.get_pointer(run_id=run_id)
        store_uri = pointer.get("mlflow_artifact_uri")
        if not store_uri:
            raise PredictionStoreNotFound(f"run_id={run_id} has no prediction-store pointer")
        path = self.artifact_store.resolve_artifact_path(str(store_uri), artifact_type="model_params", artifact_name="params.pkl")
        return {
            "run_id": run_id,
            "artifact_type": "model_params",
            "artifact_path": str(path),
            "size_bytes": path.stat().st_size,
            "pointer": pointer,
        }

    def pull_label(self, *, run_id: str) -> dict[str, Any]:
        pointer = self.get_pointer(run_id=run_id)
        store_uri = pointer.get("mlflow_artifact_uri")
        if not store_uri:
            raise PredictionStoreNotFound(f"run_id={run_id} has no prediction-store pointer")
        path = self.artifact_store.resolve_artifact_path(str(store_uri), artifact_type="label", artifact_name="label.pkl")
        return {
            "run_id": run_id,
            "artifact_type": "label",
            "artifact_path": str(path),
            "size_bytes": path.stat().st_size,
            "pointer": pointer,
        }

    def prediction_path(self, *, run_id: str) -> Path:
        pointer = self.get_pointer(run_id=run_id)
        store_uri = pointer.get("mlflow_artifact_uri")
        if not store_uri:
            raise PredictionStoreNotFound(f"run_id={run_id} has no prediction-store pointer")
        return self.artifact_store.resolve_artifact_path(str(store_uri), artifact_type="prediction", artifact_name="pred.pkl")

    def pull_params_path(self, *, run_id: str) -> Path:
        pointer = self.get_pointer(run_id=run_id)
        store_uri = pointer.get("mlflow_artifact_uri")
        if not store_uri:
            raise PredictionStoreNotFound(f"run_id={run_id} has no prediction-store pointer")
        return self.artifact_store.resolve_artifact_path(str(store_uri), artifact_type="model_params", artifact_name="params.pkl")

    def label_path(self, *, run_id: str) -> Path:
        pointer = self.get_pointer(run_id=run_id)
        store_uri = pointer.get("mlflow_artifact_uri")
        if not store_uri:
            raise PredictionStoreNotFound(f"run_id={run_id} has no prediction-store pointer")
        return self.artifact_store.resolve_artifact_path(str(store_uri), artifact_type="label", artifact_name="label.pkl")

    def _find_run(self, *, run_id: str | None, experiment_id: str | None) -> dict[str, Any] | None:
        where = "r.run_id = %s" if run_id else "r.experiment_id = %s"
        value = run_id or experiment_id
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT r.run_id, r.logical_experiment_id, r.task_id, r.loop_id, r.loop_index,
                           r.experiment_id, r.status, r.completed_at, r.node_id, r.run_type
                    FROM qe_archive.run r
                    WHERE {where}
                    ORDER BY r.completed_at DESC NULLS LAST, r.archived_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    (value,),
                )
                row = cur.fetchone()
        return dict(row) if row else None

    def _latest_source(self, run_id: str) -> dict[str, Any] | None:
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT source_system, source_type, source_id, source_sub_id, source_status,
                           recorder_experiment_id, recorder_id, mlflow_tracking_uri,
                           mlflow_artifact_uri, qlib_recorder_name, node_api_base_url,
                           metadata, created_at
                    FROM qe_archive.run_source
                    WHERE run_id = %s
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (run_id,),
                )
                row = cur.fetchone()
        return dict(row) if row else None

    def _artifacts(self, run_id: str) -> list[dict[str, Any]]:
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT artifact_type, artifact_name, storage_tier, artifact_uri,
                           local_rel_path, source_system, source_uri, source_node_id,
                           sha256, size_bytes, content_type, compression,
                           collected_status, collected_at, parser_status, parser_error,
                           metadata, created_at, updated_at
                    FROM qe_archive.run_artifact
                    WHERE run_id = %s
                    ORDER BY artifact_type ASC, artifact_name ASC, created_at DESC
                    """,
                    (run_id,),
                )
                rows = cur.fetchall()
        return [dict(row) for row in rows]

    def _store_only_pointer(self, run_id: str | None) -> dict[str, Any] | None:
        if not run_id:
            return None
        try:
            manifest = self.artifact_store.load_manifest(run_id)
        except PredictionStoreError:
            return None
        return {
            "pointer_status": "store_only",
            "warehouse_found": False,
            "run_id": run_id,
            "mlflow_artifact_uri": manifest.get("uri"),
            "prediction_store_manifest": manifest,
            "reason": "qe_archive.run row not found; resolved by store run key only",
        }

    def _pickle_preview(self, path: Path, *, head: int | None) -> dict[str, Any]:
        size_bytes = path.stat().st_size
        max_bytes = _pull_max_bytes()
        if size_bytes > max_bytes:
            raise PredictionStoreError(
                f"prediction artifact is too large to load for preview: size_bytes={size_bytes} "
                f"limit={PULL_MAX_BYTES_ENV}={max_bytes}"
            )
        import pandas as pd  # type: ignore

        obj = pd.read_pickle(path)
        if isinstance(obj, pd.Series):
            frame = obj.to_frame(name="score")
        elif isinstance(obj, pd.DataFrame):
            frame = obj
        else:
            frame = pd.DataFrame(obj)
        head_rows = max(0, min(int(head if head is not None else 5), 1000))
        preview = frame.head(head_rows).reset_index()
        return {
            "row_count": int(len(frame)),
            "columns": [str(col) for col in preview.columns],
            "head": json.loads(preview.to_json(orient="records", date_format="iso")) if head_rows else [],
            "head_count": head_rows,
            "size_bytes": size_bytes,
        }


def _pull_max_bytes() -> int:
    raw = (os.getenv(PULL_MAX_BYTES_ENV) or "").strip()
    if not raw:
        return DEFAULT_PULL_MAX_BYTES
    try:
        value = int(raw)
    except ValueError as exc:
        raise PredictionStoreError(f"{PULL_MAX_BYTES_ENV} must be an integer byte limit, got {raw!r}") from exc
    if value <= 0:
        raise PredictionStoreError(f"{PULL_MAX_BYTES_ENV} must be positive, got {value}")
    return value


def _first_artifact_run_uri(artifacts: list[Mapping[str, Any]]) -> str | None:
    for artifact in artifacts:
        uri = str(artifact.get("artifact_uri") or "")
        if uri.startswith(f"{STORE_URI_SCHEME}://"):
            try:
                run_key, _artifact_type = PredictionArtifactStore()._parse_value(uri)  # noqa: SLF001 - URI helper.
            except Exception:
                continue
            return run_uri(run_key)
    return None


def _archive_manifest_candidates(*, run_id: str, task_id: str | None, loop_index: int | None) -> list[str]:
    candidates = [str(run_id or "").strip()]
    if task_id and loop_index is not None:
        candidates.append(f"{str(task_id).strip()}_L{int(loop_index)}")
    return list(dict.fromkeys(candidate for candidate in candidates if candidate))


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()
