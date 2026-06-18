"""Runner-side HTTP uploader for QE prediction-store artifacts.

The helper is copied into QE workspaces with qrun scripts. It is disabled until
an upload URL/base is explicitly configured; once enabled, upload failures are
written to a marker file and raised so pred.pkl is never silently lost.
"""

from __future__ import annotations

import json
import os
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests


UPLOAD_MARKER_FILE = "qe_prediction_store_upload.json"
UPLOAD_URL_ENV = "AISTOCK_PREDICTION_STORE_UPLOAD_URL"
UPLOAD_TIMEOUT_ENV = "AISTOCK_PREDICTION_STORE_UPLOAD_TIMEOUT_SEC"
BASE_URL_ENVS = (
    "AISTOCK_PREDICTION_STORE_BASE_URL",
    "AISTOCK_QE_CALLBACK_BASE_URL",
    "AISTOCK_BACKEND_CALLBACK_BASE_URL",
    "AISTOCK_BACKEND_BASE_URL",
)
DEFAULT_TIMEOUT_SEC = 120.0


def maybe_upload_prediction_artifacts(
    *,
    recorder: Any,
    recorder_ref: dict[str, Any] | None,
    experiment_name: str,
    mode: str,
    config: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    if not _upload_enabled():
        print("[INFO] Prediction-store upload disabled; set AISTOCK_PREDICTION_STORE_UPLOAD_URL or backend base env to enable")
        return None

    recorder_ref = dict(recorder_ref or {})
    info = getattr(recorder, "info", {}) or {}
    recorder_id = str(info.get("id") or info.get("recorder_id") or recorder_ref.get("recorder_id") or "").strip()
    experiment_id = str(info.get("experiment_id") or recorder_ref.get("experiment_id") or "").strip()
    run_key = _resolve_run_key(
        config=config or {},
        recorder_id=recorder_id,
        experiment_name=experiment_name,
    )
    metadata = _metadata(
        config=config or {},
        recorder=recorder,
        recorder_ref=recorder_ref,
        recorder_id=recorder_id,
        experiment_id=experiment_id,
        experiment_name=experiment_name,
        mode=mode,
    )

    try:
        artifacts = _find_artifact_paths(recorder=recorder, recorder_ref=recorder_ref, recorder_id=recorder_id)
        if "prediction" not in artifacts:
            raise RuntimeError(
                "prediction-store upload is enabled but pred.pkl was not found "
                f"for recorder_id={recorder_id or '<missing>'}"
            )
        manifest = _post_artifacts(run_key=run_key, artifacts=artifacts, metadata=metadata)
        marker = {
            "schema_version": "qe_prediction_store_upload_v1",
            "status": "success",
            "run_key": run_key,
            "mode": mode,
            "upload_enabled": True,
            "mlflow_artifact_uri": manifest.get("mlflow_artifact_uri") or manifest.get("uri"),
            "prediction_store_manifest": manifest,
            "uploaded_artifacts": sorted(artifacts),
            "missing_artifacts": [name for name in ("prediction", "model_params", "label") if name not in artifacts],
            "written_at": _utc_now(),
        }
        _write_marker(marker)
        print(
            "[INFO] Prediction-store upload succeeded: "
            f"run_key={run_key} artifacts={sorted(artifacts)} uri={marker['mlflow_artifact_uri']}"
        )
        return marker
    except Exception as exc:
        marker = {
            "schema_version": "qe_prediction_store_upload_v1",
            "status": "failed",
            "run_key": run_key,
            "mode": mode,
            "upload_enabled": True,
            "error": f"{type(exc).__name__}: {exc}",
            "metadata": metadata,
            "written_at": _utc_now(),
        }
        _write_marker(marker)
        raise RuntimeError(f"prediction-store upload failed: {marker['error']}") from exc


def _upload_enabled() -> bool:
    return bool(_env(UPLOAD_URL_ENV) or _first_env(BASE_URL_ENVS))


def _resolve_run_key(*, config: dict[str, Any], recorder_id: str, experiment_name: str) -> str:
    for value in (
        _env("AISTOCK_PREDICTION_STORE_RUN_KEY"),
        _env("QE_ARCHIVE_RUN_ID"),
        _env("QE_RUN_ID"),
        config.get("run_id"),
        config.get("experiment_id"),
        experiment_name,
        recorder_id,
    ):
        text = str(value or "").strip()
        if text:
            return text
    raise RuntimeError("cannot resolve prediction-store run_key")


def _metadata(
    *,
    config: dict[str, Any],
    recorder: Any,
    recorder_ref: dict[str, Any],
    recorder_id: str,
    experiment_id: str,
    experiment_name: str,
    mode: str,
) -> dict[str, Any]:
    return {
        "producer": "qrun_prediction_store_client",
        "mode": mode,
        "cwd": str(Path.cwd()),
        "experiment_name": experiment_name,
        "experiment_id": experiment_id or None,
        "recorder_id": recorder_id or None,
        "recorder_experiment_id": experiment_id or None,
        "task_id": _env("QE_TASK_ID") or config.get("task_id"),
        "loop_id": _env("QE_LOOP_ID") or config.get("loop_id"),
        "loop_index": _env("QE_LOOP_INDEX") or config.get("loop_index"),
        "source_node_id": _source_node_id(),
        "mlflow_tracking_uri": os.environ.get("MLFLOW_TRACKING_URI", ""),
        "recorder_ref": recorder_ref,
        "recorder_artifact_uri": str(
            (getattr(recorder, "info", {}) or {}).get("artifact_uri")
            or getattr(recorder, "artifact_uri", "")
            or ""
        ),
    }


def _find_artifact_paths(*, recorder: Any, recorder_ref: dict[str, Any], recorder_id: str) -> dict[str, Path]:
    artifact_dirs = _candidate_artifact_dirs(recorder=recorder, recorder_ref=recorder_ref, recorder_id=recorder_id)
    result: dict[str, Path] = {}
    for directory in artifact_dirs:
        pred = directory / "pred.pkl"
        if pred.exists() and "prediction" not in result:
            result["prediction"] = pred
        for params_name in ("params.pkl", "params_pkl"):
            params = directory / params_name
            if params.exists() and "model_params" not in result:
                result["model_params"] = params
        label = directory / "label.pkl"
        if label.exists() and "label" not in result:
            result["label"] = label
    return result


def _candidate_artifact_dirs(*, recorder: Any, recorder_ref: dict[str, Any], recorder_id: str) -> list[Path]:
    candidates: list[Path] = []
    info = getattr(recorder, "info", {}) or {}
    for raw in (
        info.get("artifact_uri"),
        getattr(recorder, "artifact_uri", None),
        getattr(recorder, "uri", None),
    ):
        path = _path_from_uri(raw)
        if path:
            candidates.append(path)

    mlrun_roots: list[Path] = []
    for raw in (
        recorder_ref.get("target_mlruns_realpath"),
        os.environ.get("MLFLOW_TRACKING_URI"),
        Path.cwd() / "mlruns",
    ):
        path = _path_from_uri(raw)
        if path:
            mlrun_roots.append(path)
    for root in mlrun_roots:
        if not root.exists():
            continue
        if recorder_id:
            candidates.extend(root.glob(f"**/{recorder_id}/artifacts"))
            candidates.extend(root.glob(f"**/{recorder_id[:8]}*/artifacts"))
        candidates.extend(path.parent for path in root.glob("**/artifacts/pred.pkl"))
        candidates.extend(path.parent for path in root.glob("**/artifacts/params.pkl"))
        candidates.extend(path.parent for path in root.glob("**/artifacts/label.pkl"))

    deduped: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        try:
            resolved = str(candidate.resolve())
        except Exception:
            resolved = str(candidate)
        if resolved in seen:
            continue
        seen.add(resolved)
        deduped.append(candidate)
    return deduped


def _path_from_uri(value: Any) -> Path | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.startswith("file://"):
        parsed = urlparse(text)
        return Path(parsed.path)
    if text.startswith("file:"):
        return Path(text[5:])
    return Path(text)


def _post_artifacts(*, run_key: str, artifacts: dict[str, Path], metadata: dict[str, Any]) -> dict[str, Any]:
    url = _upload_url(run_key)
    data = {
        "metadata_json": json.dumps(metadata, ensure_ascii=False, default=str),
        "experiment_id": metadata.get("experiment_id"),
        "task_id": metadata.get("task_id"),
        "loop_id": metadata.get("loop_id"),
        "loop_index": metadata.get("loop_index"),
        "recorder_id": metadata.get("recorder_id"),
        "recorder_experiment_id": metadata.get("recorder_experiment_id"),
        "source_node_id": metadata.get("source_node_id"),
    }
    with ExitStack() as stack:
        files = {}
        if "prediction" in artifacts:
            f = stack.enter_context(artifacts["prediction"].open("rb"))
            files["pred"] = ("pred.pkl", f, "application/octet-stream")
        if "model_params" in artifacts:
            f = stack.enter_context(artifacts["model_params"].open("rb"))
            files["params"] = ("params.pkl", f, "application/octet-stream")
        if "label" in artifacts:
            f = stack.enter_context(artifacts["label"].open("rb"))
            files["label"] = ("label.pkl", f, "application/octet-stream")
        response = requests.post(url, data={k: v for k, v in data.items() if v not in (None, "")}, files=files, timeout=_timeout())
    if response.status_code >= 400:
        raise RuntimeError(f"HTTP {response.status_code}: {response.text[:500]}")
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"upload endpoint returned non-JSON response: {response.text[:500]!r}") from exc
    data_obj = payload.get("data") if isinstance(payload, dict) else None
    manifest = (data_obj or {}).get("manifest") if isinstance(data_obj, dict) else None
    if not isinstance(manifest, dict):
        raise RuntimeError(f"upload endpoint response missing data.manifest: {payload!r}")
    returned_types = {
        str(item.get("artifact_type") or "")
        for item in manifest.get("artifacts", [])
        if isinstance(item, dict)
    }
    missing_types = sorted(set(artifacts) - returned_types)
    if missing_types:
        raise RuntimeError(f"upload endpoint manifest missing artifact types: {missing_types}")
    return manifest


def _upload_url(run_key: str) -> str:
    full = _env(UPLOAD_URL_ENV)
    if full:
        if "{run_key}" in full:
            return full.format(run_key=run_key)
        return f"{full.rstrip('/')}/{run_key}" if full.rstrip("/").endswith("/artifacts") else full.rstrip("/")
    base = _first_env(BASE_URL_ENVS)
    if not base:
        raise RuntimeError(f"prediction-store upload requires {UPLOAD_URL_ENV} or one of {BASE_URL_ENVS}")
    _validate_remote_base(base)
    base = base.rstrip("/")
    if base.endswith("/api/v1"):
        return f"{base}/prediction-store/artifacts/{run_key}"
    return f"{base}/api/v1/prediction-store/artifacts/{run_key}"


def _validate_remote_base(url: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise RuntimeError(f"prediction-store upload base must be absolute http(s), got {url!r}")
    host = (parsed.hostname or "").lower()
    node_id = _source_node_id()
    if node_id and node_id != "wsl2-5080" and host in {"127.0.0.1", "localhost", "::1"}:
        raise RuntimeError(f"prediction-store upload base {url!r} is localhost but source_node_id={node_id!r} is remote")


def _write_marker(payload: dict[str, Any]) -> None:
    path = Path.cwd() / UPLOAD_MARKER_FILE
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    tmp.replace(path)


def _timeout() -> float:
    raw = _env(UPLOAD_TIMEOUT_ENV)
    if not raw:
        return DEFAULT_TIMEOUT_SEC
    try:
        value = float(raw)
    except ValueError as exc:
        raise RuntimeError(f"{UPLOAD_TIMEOUT_ENV} must be numeric seconds, got {raw!r}") from exc
    if value <= 0:
        raise RuntimeError(f"{UPLOAD_TIMEOUT_ENV} must be positive, got {value}")
    return value


def _source_node_id() -> str | None:
    for name in ("AISTOCK_NODE_ID", "QE_NODE_ID", "COMPUTE_NODE_ID", "RDAGENT_NODE_ID"):
        value = _env(name)
        if value:
            return value
    return None


def _first_env(names: tuple[str, ...]) -> str:
    for name in names:
        value = _env(name)
        if value:
            return value
    return ""


def _env(name: str) -> str:
    return (os.environ.get(name) or "").strip()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
