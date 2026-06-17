"""Fast-disk artifact store for QE prediction and model-parameter pickles.

P2 deliberately keeps MLflow tracking out of PostgreSQL. Qlib still writes
local recorder artifacts, and the runner pushes ``pred.pkl``/``params.pkl`` to
this AIstock-owned store before any workspace cleanup can remove them.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, BinaryIO, Mapping
from urllib.parse import urlparse

STORE_URI_SCHEME = "aistock-prediction-store"
STORE_ROOT_ENV = "AISTOCK_PREDICTION_STORE_ROOT"
METADATA_MAX_BYTES_ENV = "AISTOCK_PREDICTION_STORE_METADATA_MAX_BYTES"
DEFAULT_METADATA_MAX_BYTES = 512 * 1024 * 1024
MANIFEST_SCHEMA_VERSION = "aistock_prediction_store_manifest_v1"

_SAFE_COMPONENT_RE = re.compile(r"[^A-Za-z0-9_.-]+")


class PredictionStoreError(RuntimeError):
    """Raised when a prediction-store operation cannot be completed."""


class PredictionStoreNotFound(PredictionStoreError):
    """Raised when a requested prediction-store artifact is absent."""


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_store_component(value: str, *, field_name: str = "value") -> str:
    text = str(value or "").strip()
    if not text:
        raise PredictionStoreError(f"{field_name} must be non-empty")
    safe = _SAFE_COMPONENT_RE.sub("_", text).strip("._")
    if not safe:
        raise PredictionStoreError(f"{field_name} has no safe filesystem characters: {value!r}")
    return safe[:180]


def default_store_root() -> Path:
    configured = (os.getenv(STORE_ROOT_ENV) or "").strip()
    if configured:
        return Path(configured).expanduser()
    repo_root = Path(__file__).resolve().parents[3]
    return repo_root / "rdagent_assets" / "prediction_store"


def validate_store_root(root: Path) -> Path:
    root = Path(root).expanduser()
    drive = (root.drive or "").upper()
    as_posix = root.as_posix().lower()
    if drive == "E:" or as_posix.startswith("/mnt/e/") or as_posix == "/mnt/e":
        raise PredictionStoreError(
            "AISTOCK prediction artifact store must not use E: HDD; "
            f"set {STORE_ROOT_ENV} to an F:/SSD-backed path, got {root}"
        )
    return root


def run_uri(run_key_safe: str) -> str:
    return f"{STORE_URI_SCHEME}://runs/{safe_store_component(run_key_safe, field_name='run_key')}"


def artifact_uri(run_key_safe: str, artifact_type: str) -> str:
    return f"{run_uri(run_key_safe)}/{safe_store_component(artifact_type, field_name='artifact_type')}"


def parse_prediction_store_uri(uri: str) -> tuple[str, str | None]:
    text = str(uri or "").strip()
    if not text:
        raise PredictionStoreError("prediction-store URI is empty")
    parsed = urlparse(text)
    if parsed.scheme != STORE_URI_SCHEME:
        raise PredictionStoreError(f"unsupported prediction-store URI: {uri!r}")
    if parsed.netloc != "runs":
        raise PredictionStoreError(f"unsupported prediction-store URI authority: {parsed.netloc!r}")
    parts = [part for part in parsed.path.strip("/").split("/") if part]
    if not parts:
        raise PredictionStoreError(f"prediction-store URI missing run key: {uri!r}")
    return parts[0], parts[1] if len(parts) > 1 else None


def _json_safe(value: Any) -> Any:
    try:
        import numpy as np  # type: ignore
        import pandas as pd  # type: ignore
    except Exception:  # pragma: no cover - optional dependency branch.
        np = None  # type: ignore
        pd = None  # type: ignore

    if pd is not None and isinstance(value, pd.Timestamp):
        return value.isoformat()
    if np is not None and isinstance(value, np.generic):
        return _json_safe(value.item())
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(_json_safe(dict(payload)), ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    tmp.replace(path)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise PredictionStoreNotFound(f"prediction-store manifest not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise PredictionStoreError(f"prediction-store manifest is invalid JSON: {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise PredictionStoreError(f"prediction-store manifest must be an object: {path}")
    return data


def _max_metadata_bytes() -> int:
    raw = (os.getenv(METADATA_MAX_BYTES_ENV) or "").strip()
    if not raw:
        return DEFAULT_METADATA_MAX_BYTES
    try:
        parsed = int(raw)
    except ValueError as exc:
        raise PredictionStoreError(f"{METADATA_MAX_BYTES_ENV} must be an integer byte limit, got {raw!r}") from exc
    if parsed <= 0:
        raise PredictionStoreError(f"{METADATA_MAX_BYTES_ENV} must be positive, got {parsed}")
    return parsed


def _first_non_empty(mapping: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = mapping.get(key)
        if value not in (None, "", [], {}):
            return value
    return None


def infer_pickle_metadata(path: Path, *, artifact_type: str) -> dict[str, Any]:
    """Return nullable artifact metadata; parse failures are explicit fields."""

    size_bytes = path.stat().st_size
    max_bytes = _max_metadata_bytes()
    if size_bytes > max_bytes:
        return {
            "row_count": None,
            "symbol_count": None,
            "date_start": None,
            "date_end": None,
            "parser_status": "not_parsed",
            "parser_error": f"skipped: size_bytes={size_bytes} exceeds {METADATA_MAX_BYTES_ENV}={max_bytes}",
        }
    if artifact_type != "prediction":
        return {"parser_status": "not_required"}

    try:
        import pandas as pd  # type: ignore

        obj = pd.read_pickle(path)
        if isinstance(obj, pd.Series):
            frame = obj.to_frame(name="score")
        elif isinstance(obj, pd.DataFrame):
            frame = obj
        else:
            frame = pd.DataFrame(obj)

        row_count = int(len(frame))
        symbols: Any = None
        dates: Any = None
        if isinstance(frame.index, pd.MultiIndex):
            names = [str(name or "").lower() for name in frame.index.names]
            inst_level = next((i for i, name in enumerate(names) if "inst" in name or "symbol" in name), None)
            date_level = next((i for i, name in enumerate(names) if "date" in name or "time" in name), None)
            if inst_level is not None:
                symbols = frame.index.get_level_values(inst_level)
            if date_level is not None:
                dates = pd.to_datetime(frame.index.get_level_values(date_level), errors="coerce")
        else:
            for col in ("instrument", "symbol", "ts_code"):
                if col in frame.columns:
                    symbols = frame[col]
                    break
            for col in ("datetime", "date", "trade_date"):
                if col in frame.columns:
                    dates = pd.to_datetime(frame[col], errors="coerce")
                    break
            if dates is None and isinstance(frame.index, pd.DatetimeIndex):
                dates = pd.to_datetime(frame.index, errors="coerce")

        symbol_count = int(pd.Series(symbols).dropna().astype(str).nunique()) if symbols is not None else None
        date_series = pd.Series(dates).dropna() if dates is not None else pd.Series(dtype="datetime64[ns]")
        return {
            "row_count": row_count,
            "symbol_count": symbol_count,
            "date_start": date_series.min().date().isoformat() if not date_series.empty else None,
            "date_end": date_series.max().date().isoformat() if not date_series.empty else None,
            "parser_status": "parsed",
        }
    except Exception as exc:
        return {
            "row_count": None,
            "symbol_count": None,
            "date_start": None,
            "date_end": None,
            "parser_status": "failed",
            "parser_error": f"{type(exc).__name__}: {exc}",
        }


class PredictionArtifactStore:
    """Content-addressed store for uploaded QE prediction artifacts."""

    def __init__(self, root: str | Path | None = None) -> None:
        self.root = validate_store_root(Path(root) if root is not None else default_store_root())

    def ensure_ready(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        for child in ("runs", "blobs", "tmp"):
            (self.root / child).mkdir(parents=True, exist_ok=True)

    def health(self) -> dict[str, Any]:
        root = self.root
        try:
            usage = shutil.disk_usage(root if root.exists() else root.parent)
            disk = {"total_bytes": usage.total, "used_bytes": usage.used, "free_bytes": usage.free}
        except Exception as exc:
            disk = {"error": f"{type(exc).__name__}: {exc}"}
        return {
            "store_root": str(root),
            "store_root_env": STORE_ROOT_ENV,
            "exists": root.exists(),
            "scheme": STORE_URI_SCHEME,
            "disk": disk,
            "policy": {"forbidden_drive": "E:", "requires_fast_disk": True},
        }

    def run_dir(self, run_key_safe: str) -> Path:
        return self.root / "runs" / safe_store_component(run_key_safe, field_name="run_key")

    def manifest_path(self, run_key_safe: str) -> Path:
        return self.run_dir(run_key_safe) / "manifest.json"

    def blob_path(self, sha256: str) -> Path:
        digest = str(sha256 or "").strip().lower()
        if len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest):
            raise PredictionStoreError(f"invalid sha256 digest: {sha256!r}")
        return self.root / "blobs" / digest[:2] / digest

    def write_artifacts(
        self,
        *,
        run_key: str,
        files: Mapping[str, tuple[str, BinaryIO]],
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not files:
            raise PredictionStoreError("at least one prediction artifact file is required")
        self.ensure_ready()
        run_key_safe = safe_store_component(run_key, field_name="run_key")
        uploaded_at = utc_now_iso()
        previous = self.load_manifest(run_key_safe, missing_ok=True) or {}
        previous_artifacts = {
            str(item.get("artifact_type") or item.get("artifact_name") or ""): dict(item)
            for item in previous.get("artifacts", [])
            if isinstance(item, Mapping)
        }
        artifact_items: dict[str, dict[str, Any]] = dict(previous_artifacts)
        source_node_id = _first_non_empty(metadata or {}, "source_node_id", "node_id", "execution_node_id")

        for artifact_type, (filename, fileobj) in files.items():
            normalized_type = safe_store_component(artifact_type, field_name="artifact_type")
            artifact_name = "pred.pkl" if normalized_type == "prediction" else "params.pkl"
            if filename:
                lower_name = Path(filename).name.lower()
                if lower_name in {"pred.pkl", "params.pkl", "params_pkl"}:
                    artifact_name = "params.pkl" if lower_name == "params_pkl" else lower_name
            sha256, size_bytes = self._write_blob(fileobj)
            blob = self.blob_path(sha256)
            inferred = infer_pickle_metadata(blob, artifact_type=normalized_type)
            item_metadata = {
                "original_filename": Path(filename).name if filename else artifact_name,
                "blob_rel_path": blob.relative_to(self.root).as_posix(),
                "upload_metadata": dict(metadata or {}),
            }
            if inferred.get("parser_error"):
                item_metadata["parser_error"] = inferred.get("parser_error")
            artifact_items[normalized_type] = {
                "schema_version": MANIFEST_SCHEMA_VERSION,
                "artifact_type": normalized_type,
                "artifact_name": artifact_name,
                "uri": artifact_uri(run_key_safe, normalized_type),
                "sha256": sha256,
                "size_bytes": size_bytes,
                "row_count": inferred.get("row_count"),
                "symbol_count": inferred.get("symbol_count"),
                "date_start": inferred.get("date_start"),
                "date_end": inferred.get("date_end"),
                "content_type": "application/python-pickle",
                "source_api": f"/api/v1/prediction-store/artifacts/{run_key_safe}",
                "source_node_id": source_node_id,
                "created_at": uploaded_at,
                "storage_tier": "hot",
                "collection_status": "available",
                "parser_status": inferred.get("parser_status") or "not_required",
                "metadata": item_metadata,
            }

        manifest = {
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "run_key": run_key,
            "run_key_safe": run_key_safe,
            "uri": run_uri(run_key_safe),
            "mlflow_artifact_uri": run_uri(run_key_safe),
            "storage_tier": "hot",
            "created_at": previous.get("created_at") or uploaded_at,
            "updated_at": uploaded_at,
            "metadata": dict(metadata or {}),
            "artifacts": list(artifact_items.values()),
        }
        _atomic_write_json(self.manifest_path(run_key_safe), manifest)
        return manifest

    def _write_blob(self, fileobj: BinaryIO) -> tuple[str, int]:
        tmp_dir = self.root / "tmp"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        hasher = hashlib.sha256()
        size_bytes = 0
        fd, tmp_name = tempfile.mkstemp(prefix="upload_", suffix=".blob", dir=tmp_dir)
        tmp_path = Path(tmp_name)
        try:
            with os.fdopen(fd, "wb") as out:
                while True:
                    chunk = fileobj.read(1024 * 1024)
                    if not chunk:
                        break
                    if isinstance(chunk, str):
                        chunk = chunk.encode("utf-8")
                    out.write(chunk)
                    hasher.update(chunk)
                    size_bytes += len(chunk)
            digest = hasher.hexdigest()
            target = self.blob_path(digest)
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists():
                if target.stat().st_size != size_bytes:
                    raise PredictionStoreError(
                        f"existing blob size mismatch for sha256={digest}: "
                        f"existing={target.stat().st_size} new={size_bytes}"
                    )
                tmp_path.unlink(missing_ok=True)
            else:
                tmp_path.replace(target)
            return digest, size_bytes
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise

    def load_manifest(self, run_key_or_uri: str, *, missing_ok: bool = False) -> dict[str, Any] | None:
        run_key_safe = self._run_key_from_value(run_key_or_uri)
        path = self.manifest_path(run_key_safe)
        if missing_ok and not path.exists():
            return None
        return _read_json(path)

    def resolve_artifact_path(
        self,
        run_key_or_uri: str,
        *,
        artifact_type: str | None = None,
        artifact_name: str | None = None,
    ) -> Path:
        run_key_safe, uri_artifact_type = self._parse_value(run_key_or_uri)
        desired_type = artifact_type or uri_artifact_type
        manifest = self.load_manifest(run_key_safe)
        for item in manifest.get("artifacts", []):
            if not isinstance(item, Mapping):
                continue
            type_match = desired_type and str(item.get("artifact_type")) == desired_type
            name_match = artifact_name and str(item.get("artifact_name")) == artifact_name
            if not (type_match or name_match):
                continue
            metadata = item.get("metadata") if isinstance(item.get("metadata"), Mapping) else {}
            rel_path = metadata.get("blob_rel_path")
            if not rel_path:
                raise PredictionStoreError(f"artifact {desired_type or artifact_name!r} has no blob path in manifest")
            path = (self.root / str(rel_path)).resolve()
            root_resolved = self.root.resolve()
            if root_resolved not in path.parents and path != root_resolved:
                raise PredictionStoreError(f"artifact path escapes store root: {path}")
            if not path.exists():
                raise PredictionStoreNotFound(f"artifact blob missing on disk: {path}")
            return path
        raise PredictionStoreNotFound(
            f"artifact not found for run={run_key_safe} type={artifact_type!r} name={artifact_name!r}"
        )

    def _parse_value(self, value: str) -> tuple[str, str | None]:
        text = str(value or "").strip()
        if text.startswith(f"{STORE_URI_SCHEME}://"):
            return parse_prediction_store_uri(text)
        return safe_store_component(text, field_name="run_key"), None

    def _run_key_from_value(self, value: str) -> str:
        return self._parse_value(value)[0]
