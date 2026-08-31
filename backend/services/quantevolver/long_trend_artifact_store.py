"""Dedicated content-addressed store for QE long-trend evaluation evidence."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from datetime import datetime, timezone
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlparse

from backend.services.quantevolver.long_trend_evaluation_contract import (
    QELongTrendReason,
    canonical_sha256,
)

STORE_ENV = "QE_LONG_TREND_ARTIFACT_STORE_ROOT"
STORE_SCHEME = "aistock-qe-long-trend"
MANIFEST_SCHEMA = "qe_long_trend_artifact_manifest_v1"
ALLOWED_ARTIFACT_SCHEMAS: dict[str, str] = {
    "worker_compact_receipt": "qe_long_trend_worker_compact_v1",
    "published_compact_receipt": "qe_long_trend_published_compact_v1",
    "signal_observations": "qe_long_trend_signal_observation_v1",
    "holding_episodes": "qe_long_trend_holding_episode_v1",
    "worker_terminal_receipt": "qe_long_trend_worker_terminal_v1",
}
_SHA_RE = re.compile(r"^[0-9a-f]{64}$")
_EVALUATION_RE = re.compile(r"^qelt_[0-9a-f]{64}$")


class QELongTrendArtifactStoreError(RuntimeError):
    def __init__(self, message: str, *, reason_code: str):
        super().__init__(message)
        self.reason_code = reason_code


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def default_long_trend_store_root() -> Path:
    configured = str(os.getenv(STORE_ENV) or "").strip()
    if configured:
        return Path(configured).expanduser()
    return Path(__file__).resolve().parents[3] / "rdagent_assets" / "long_trend_evaluation_store"


def evaluation_uri(evaluation_id: str) -> str:
    _validate_evaluation_id(evaluation_id)
    return f"{STORE_SCHEME}://evaluations/{evaluation_id}"


def artifact_uri(evaluation_id: str, artifact_type: str) -> str:
    _validate_artifact_type(artifact_type)
    return f"{evaluation_uri(evaluation_id)}/{artifact_type}"


class QELongTrendArtifactStore:
    def __init__(
        self,
        root: str | Path | None = None,
        *,
        prediction_store_root: str | Path | None = None,
    ) -> None:
        self.root = self._validate_root(Path(root) if root is not None else default_long_trend_store_root())
        if prediction_store_root is None:
            from backend.services.model_store.artifact_store import default_store_root

            prediction_store_root = default_store_root()
        prediction_root = Path(prediction_store_root).expanduser().resolve()
        if self.root.resolve() == prediction_root:
            raise QELongTrendArtifactStoreError(
                "long-trend artifact store must not share the Prediction Store root",
                reason_code=QELongTrendReason.CAS_MANIFEST_CONFLICT.value,
            )

    @staticmethod
    def _validate_root(root: Path) -> Path:
        root = root.expanduser()
        drive = str(root.drive or "").upper()
        posix = root.as_posix().lower()
        if drive == "E:" or posix == "/mnt/e" or posix.startswith("/mnt/e/"):
            raise QELongTrendArtifactStoreError(
                "QE long-trend artifacts cannot be stored on E: HDD",
                reason_code=QELongTrendReason.CAS_MANIFEST_CONFLICT.value,
            )
        return root

    def ensure_ready(self) -> None:
        for name in ("evaluations", "blobs", "tmp"):
            (self.root / name).mkdir(parents=True, exist_ok=True)
        _fsync_directory(self.root)

    def manifest_path(self, evaluation_id: str) -> Path:
        _validate_evaluation_id(evaluation_id)
        return self.root / "evaluations" / evaluation_id / "manifest.json"

    def blob_path(self, sha256: str) -> Path:
        digest = str(sha256 or "").lower()
        if not _SHA_RE.fullmatch(digest):
            raise QELongTrendArtifactStoreError(
                f"invalid blob sha256: {sha256!r}",
                reason_code=QELongTrendReason.ARTIFACT_HASH_MISMATCH.value,
            )
        return self.root / "blobs" / digest[:2] / digest

    def publish(
        self,
        *,
        evaluation_id: str,
        worker_terminal: Mapping[str, Any],
        artifact_files: Mapping[str, Path],
        expected_catalog: Mapping[str, Mapping[str, Any]],
    ) -> dict[str, Any]:
        """Publish worker evidence without loading Parquet rows into memory."""

        _validate_evaluation_id(evaluation_id)
        terminal_schema = str(worker_terminal.get("schema_version") or "")
        if terminal_schema != ALLOWED_ARTIFACT_SCHEMAS["worker_terminal_receipt"]:
            raise QELongTrendArtifactStoreError(
                f"invalid worker terminal schema: {terminal_schema!r}",
                reason_code=QELongTrendReason.ARTIFACT_SCHEMA_MISMATCH.value,
            )
        if str(worker_terminal.get("evaluation_id") or "") != evaluation_id:
            raise QELongTrendArtifactStoreError(
                "worker terminal evaluation identity mismatch",
                reason_code=QELongTrendReason.CAS_MANIFEST_CONFLICT.value,
            )
        required, typed_absence = required_artifact_matrix(worker_terminal)
        supplied = set(artifact_files)
        unknown = sorted(supplied - set(ALLOWED_ARTIFACT_SCHEMAS))
        if unknown:
            raise QELongTrendArtifactStoreError(
                f"unknown long-trend artifacts: {unknown}",
                reason_code=QELongTrendReason.ARTIFACT_SCHEMA_MISMATCH.value,
            )
        missing = sorted(required - supplied)
        if missing:
            raise QELongTrendArtifactStoreError(
                f"worker terminal required artifacts are missing: {missing}",
                reason_code=QELongTrendReason.ARTIFACT_SCHEMA_MISMATCH.value,
            )
        contradictory = sorted(supplied.intersection(typed_absence))
        if contradictory:
            raise QELongTrendArtifactStoreError(
                f"typed-absent artifacts were also supplied: {contradictory}",
                reason_code=QELongTrendReason.ARTIFACT_SCHEMA_MISMATCH.value,
            )
        self.ensure_ready()
        items: list[dict[str, Any]] = []
        for artifact_type in sorted(supplied):
            path = Path(artifact_files[artifact_type]).resolve()
            if not path.is_file() or path.is_symlink():
                raise QELongTrendArtifactStoreError(
                    f"artifact is missing or linked: {artifact_type}",
                    reason_code=QELongTrendReason.ARTIFACT_STREAM_INTERRUPTED.value,
                )
            actual_sha, size = _sha256_file(path)
            catalog = expected_catalog.get(artifact_type)
            if not isinstance(catalog, Mapping):
                raise QELongTrendArtifactStoreError(
                    f"node catalog is missing artifact metadata: {artifact_type}",
                    reason_code=QELongTrendReason.ARTIFACT_SCHEMA_MISMATCH.value,
                )
            expected_sha = str(catalog.get("sha256") or "").lower()
            expected_size = int(catalog.get("size_bytes") or -1)
            if actual_sha != expected_sha or size != expected_size:
                raise QELongTrendArtifactStoreError(
                    f"artifact catalog mismatch for {artifact_type}",
                    reason_code=QELongTrendReason.ARTIFACT_HASH_MISMATCH.value,
                )
            schema_meta = _validate_artifact_schema(path, artifact_type, evaluation_id)
            target = self.blob_path(actual_sha)
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists():
                existing_sha, existing_size = _sha256_file(target)
                if existing_sha != actual_sha or existing_size != size:
                    raise QELongTrendArtifactStoreError(
                        f"existing blob conflicts with content hash for {artifact_type}",
                        reason_code=QELongTrendReason.CAS_MANIFEST_CONFLICT.value,
                    )
            else:
                fd, tmp_name = tempfile.mkstemp(prefix="qelt_publish_", dir=self.root / "tmp")
                tmp = Path(tmp_name)
                try:
                    with os.fdopen(fd, "wb") as out, path.open("rb") as src:
                        shutil.copyfileobj(src, out, length=1024 * 1024)
                        out.flush()
                        os.fsync(out.fileno())
                    tmp_sha, tmp_size = _sha256_file(tmp)
                    if tmp_sha != actual_sha or tmp_size != size:
                        raise QELongTrendArtifactStoreError(
                            f"staged blob changed during publish: {artifact_type}",
                            reason_code=QELongTrendReason.ARTIFACT_HASH_MISMATCH.value,
                        )
                    _atomic_replace(tmp, target)
                    _fsync_directory(target.parent)
                finally:
                    tmp.unlink(missing_ok=True)
            items.append(
                {
                    "artifact_type": artifact_type,
                    "schema_version": ALLOWED_ARTIFACT_SCHEMAS[artifact_type],
                    "uri": artifact_uri(evaluation_id, artifact_type),
                    "sha256": actual_sha,
                    "size_bytes": size,
                    "blob_rel_path": target.relative_to(self.root).as_posix(),
                    **schema_meta,
                }
            )

        content = {
            "schema_version": MANIFEST_SCHEMA,
            "evaluation_id": evaluation_id,
            "worker_terminal_sha256": canonical_sha256(dict(worker_terminal)),
            "identity": {
                key: worker_terminal.get(key)
                for key in (
                    "input_manifest_sha256",
                    "bundle_sha256",
                    "execution_environment_snapshot_id",
                    "execution_environment_manifest_sha256",
                    "attempt_id",
                    "node_id",
                )
            },
            "required_artifacts": sorted(required),
            "typed_absence": typed_absence,
            "artifacts": items,
        }
        manifest_sha = canonical_sha256(content)
        envelope = {
            **content,
            "artifact_manifest_sha256": manifest_sha,
            "uri": evaluation_uri(evaluation_id),
            "published_at": _utc_now(),
        }
        path = self.manifest_path(evaluation_id)
        with _exclusive_file_lock(path.parent / ".publish.lock"):
            if path.exists():
                existing = self.load_manifest(evaluation_id)
                if str(existing.get("artifact_manifest_sha256") or "") != manifest_sha:
                    raise QELongTrendArtifactStoreError(
                        "successful evaluation manifest already exists with different content",
                        reason_code=QELongTrendReason.CAS_MANIFEST_CONFLICT.value,
                    )
                return existing
            _atomic_write_json(path, envelope)
        return envelope

    def load_manifest(self, evaluation_id_or_uri: str) -> dict[str, Any]:
        evaluation_id = _evaluation_id_from_value(evaluation_id_or_uri)
        path = self.manifest_path(evaluation_id)
        if not path.is_file() or path.is_symlink():
            raise QELongTrendArtifactStoreError(
                "stored long-trend manifest is missing or linked",
                reason_code=QELongTrendReason.ARTIFACT_STREAM_INTERRUPTED.value,
            )
        payload = _read_json(path)
        expected = str(payload.get("artifact_manifest_sha256") or "")
        content = {key: value for key, value in payload.items() if key not in {"artifact_manifest_sha256", "uri", "published_at"}}
        if canonical_sha256(content) != expected:
            raise QELongTrendArtifactStoreError(
                "stored long-trend manifest hash is invalid",
                reason_code=QELongTrendReason.CAS_MANIFEST_CONFLICT.value,
            )
        return payload

    def load_json_artifact(self, *, evaluation_id: str, artifact_type: str) -> dict[str, Any]:
        """Read one manifest-bound JSON artifact and re-verify its immutable identity."""

        _validate_evaluation_id(evaluation_id)
        _validate_artifact_type(artifact_type)
        if artifact_type in {"signal_observations", "holding_episodes", "published_compact_receipt"}:
            raise QELongTrendArtifactStoreError(
                f"artifact type is not a manifest JSON receipt: {artifact_type}",
                reason_code=QELongTrendReason.ARTIFACT_SCHEMA_MISMATCH.value,
            )
        manifest = self.load_manifest(evaluation_id)
        matches = [
            item
            for item in manifest.get("artifacts") or []
            if isinstance(item, Mapping) and item.get("artifact_type") == artifact_type
        ]
        if len(matches) != 1:
            raise QELongTrendArtifactStoreError(
                f"manifest must contain exactly one {artifact_type}",
                reason_code=QELongTrendReason.ARTIFACT_SCHEMA_MISMATCH.value,
            )
        item = matches[0]
        path = self.blob_path(str(item.get("sha256") or ""))
        if not path.is_file() or path.is_symlink():
            raise QELongTrendArtifactStoreError(
                f"stored JSON artifact is missing or linked: {artifact_type}",
                reason_code=QELongTrendReason.ARTIFACT_STREAM_INTERRUPTED.value,
            )
        try:
            encoded = path.read_bytes()
        except OSError as exc:
            raise QELongTrendArtifactStoreError(
                f"cannot read stored JSON artifact: {artifact_type}",
                reason_code=QELongTrendReason.ARTIFACT_STREAM_INTERRUPTED.value,
            ) from exc
        actual_sha = hashlib.sha256(encoded).hexdigest()
        actual_size = len(encoded)
        if actual_sha != item.get("sha256") or actual_size != item.get("size_bytes"):
            raise QELongTrendArtifactStoreError(
                f"stored JSON artifact differs from manifest: {artifact_type}",
                reason_code=QELongTrendReason.ARTIFACT_HASH_MISMATCH.value,
            )
        payload = _decode_json_bytes(encoded, artifact_label=artifact_type)
        if (
            payload.get("schema_version") != ALLOWED_ARTIFACT_SCHEMAS[artifact_type]
            or payload.get("evaluation_id") != evaluation_id
        ):
            raise QELongTrendArtifactStoreError(
                f"stored JSON artifact schema or evaluation identity differs: {artifact_type}",
                reason_code=QELongTrendReason.ARTIFACT_SCHEMA_MISMATCH.value,
            )
        return payload

    def load_published_compact_receipt(self, evaluation_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
        """Read and verify the post-manifest compact receipt without exposing its local path."""

        manifest = self.load_manifest(evaluation_id)
        target = self.root / "evaluations" / evaluation_id / "published_compact_receipt.json"
        if not target.is_file() or target.is_symlink():
            raise QELongTrendArtifactStoreError(
                "published compact receipt is missing or linked",
                reason_code=QELongTrendReason.ARTIFACT_STREAM_INTERRUPTED.value,
            )
        try:
            encoded = target.read_bytes()
        except OSError as exc:
            raise QELongTrendArtifactStoreError(
                "cannot read published compact receipt",
                reason_code=QELongTrendReason.ARTIFACT_STREAM_INTERRUPTED.value,
            ) from exc
        digest = hashlib.sha256(encoded).hexdigest()
        payload = _decode_json_bytes(encoded, artifact_label="published_compact_receipt")
        if (
            payload.get("schema_version") != ALLOWED_ARTIFACT_SCHEMAS["published_compact_receipt"]
            or payload.get("evaluation_id") != evaluation_id
            or payload.get("artifact_manifest_uri") != manifest.get("uri")
            or payload.get("artifact_manifest_sha256") != manifest.get("artifact_manifest_sha256")
        ):
            raise QELongTrendArtifactStoreError(
                "published compact receipt differs from the immutable manifest",
                reason_code=QELongTrendReason.CAS_MANIFEST_CONFLICT.value,
            )
        return payload, {
            "artifact_type": "published_compact_receipt",
            "schema_version": ALLOWED_ARTIFACT_SCHEMAS["published_compact_receipt"],
            "uri": artifact_uri(evaluation_id, "published_compact_receipt"),
            "sha256": digest,
            "size_bytes": len(encoded),
        }

    def publish_compact_receipt(
        self,
        *,
        evaluation_id: str,
        receipt: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Persist the deterministic CAS-published receipt beside the manifest."""

        manifest = self.load_manifest(evaluation_id)
        if (
            receipt.get("schema_version") != ALLOWED_ARTIFACT_SCHEMAS["published_compact_receipt"]
            or receipt.get("evaluation_id") != evaluation_id
            or receipt.get("artifact_manifest_sha256") != manifest["artifact_manifest_sha256"]
            or receipt.get("artifact_manifest_uri") != manifest["uri"]
        ):
            raise QELongTrendArtifactStoreError(
                "published compact receipt does not match the immutable manifest",
                reason_code=QELongTrendReason.CAS_MANIFEST_CONFLICT.value,
            )
        encoded = json.dumps(
            dict(receipt),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        digest = hashlib.sha256(encoded).hexdigest()
        target = self.root / "evaluations" / evaluation_id / "published_compact_receipt.json"
        with _exclusive_file_lock(target.parent / ".publish.lock"):
            if target.exists():
                existing = target.read_bytes()
                if hashlib.sha256(existing).hexdigest() != digest:
                    raise QELongTrendArtifactStoreError(
                        "published compact receipt already exists with different content",
                        reason_code=QELongTrendReason.CAS_MANIFEST_CONFLICT.value,
                    )
            else:
                fd, name = tempfile.mkstemp(prefix="published_", suffix=".json", dir=target.parent)
                tmp = Path(name)
                try:
                    with os.fdopen(fd, "wb") as handle:
                        handle.write(encoded)
                        handle.flush()
                        os.fsync(handle.fileno())
                    _atomic_replace(tmp, target)
                    _fsync_directory(target.parent)
                finally:
                    tmp.unlink(missing_ok=True)
        return {
            "artifact_type": "published_compact_receipt",
            "schema_version": ALLOWED_ARTIFACT_SCHEMAS["published_compact_receipt"],
            "uri": artifact_uri(evaluation_id, "published_compact_receipt"),
            "sha256": digest,
            "size_bytes": len(encoded),
        }


def required_artifact_matrix(worker_terminal: Mapping[str, Any]) -> tuple[set[str], dict[str, Any]]:
    """Return the authoritative required/typed-absence artifact contract."""
    return _required_matrix(worker_terminal)


def _required_matrix(worker_terminal: Mapping[str, Any]) -> tuple[set[str], dict[str, Any]]:
    required = {"worker_terminal_receipt", "worker_compact_receipt"}
    typed_absence: dict[str, Any] = {}
    family_status = worker_terminal.get("family_status")
    if not isinstance(family_status, Mapping):
        raise QELongTrendArtifactStoreError(
            "worker terminal family_status must be an object",
            reason_code=QELongTrendReason.ARTIFACT_SCHEMA_MISMATCH.value,
        )
    for family_name, artifact_type in (
        ("signal_path", "signal_observations"),
        ("position_episode", "holding_episodes"),
    ):
        family = family_status.get(family_name)
        if not isinstance(family, Mapping):
            raise QELongTrendArtifactStoreError(
                f"worker terminal is missing family status: {family_name}",
                reason_code=QELongTrendReason.ARTIFACT_SCHEMA_MISMATCH.value,
            )
        status = str(family.get("status") or "")
        if status in {"COMPUTED", "COMPUTED_WITH_LIMITATIONS"}:
            required.add(artifact_type)
        elif status in {"NOT_COMPUTABLE", "NOT_VERIFIABLE"}:
            typed_absence[artifact_type] = {
                "status": status,
                "reason_codes": list(family.get("reason_codes") or []),
            }
        else:
            raise QELongTrendArtifactStoreError(
                f"invalid family status for {family_name}: {status!r}",
                reason_code=QELongTrendReason.ARTIFACT_SCHEMA_MISMATCH.value,
            )
    return required, typed_absence


def _validate_artifact_schema(path: Path, artifact_type: str, evaluation_id: str) -> dict[str, Any]:
    expected_schema = ALLOWED_ARTIFACT_SCHEMAS[artifact_type]
    if artifact_type in {"signal_observations", "holding_episodes"}:
        try:
            import pyarrow.parquet as pq  # type: ignore

            parquet = pq.ParquetFile(path)
            metadata = parquet.metadata
            schema = parquet.schema_arrow
        except Exception as exc:
            raise QELongTrendArtifactStoreError(
                f"cannot validate Parquet footer for {artifact_type}: {type(exc).__name__}: {exc}",
                reason_code=QELongTrendReason.ARTIFACT_SCHEMA_MISMATCH.value,
            ) from exc
        schema_sha = canonical_sha256({"schema": str(schema), "columns": schema.names})
        return {
            "schema_sha256": schema_sha,
            "row_count": int(metadata.num_rows),
            "row_group_count": int(metadata.num_row_groups),
            "columns": list(schema.names),
        }
    payload = _read_json(path)
    if str(payload.get("schema_version") or "") != expected_schema:
        raise QELongTrendArtifactStoreError(
            f"JSON artifact schema mismatch for {artifact_type}",
            reason_code=QELongTrendReason.ARTIFACT_SCHEMA_MISMATCH.value,
        )
    if str(payload.get("evaluation_id") or "") != evaluation_id:
        raise QELongTrendArtifactStoreError(
            f"JSON artifact evaluation identity mismatch for {artifact_type}",
            reason_code=QELongTrendReason.CAS_MANIFEST_CONFLICT.value,
        )
    return {"schema_sha256": canonical_sha256({"schema_version": expected_schema}), "row_count": None}


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix="manifest_", suffix=".json", dir=path.parent)
    tmp = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(dict(payload), handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
            handle.flush()
            os.fsync(handle.fileno())
        _atomic_replace(tmp, path)
        _fsync_directory(path.parent)
    finally:
        tmp.unlink(missing_ok=True)


@contextmanager
def _exclusive_file_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        if handle.tell() == 0 and path.stat().st_size == 0:
            handle.write(b"\0")
            handle.flush()
            os.fsync(handle.fileno())
        handle.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
            try:
                yield
            finally:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise QELongTrendArtifactStoreError(
            f"cannot read long-trend JSON artifact {path}: {exc}",
            reason_code=QELongTrendReason.ARTIFACT_SCHEMA_MISMATCH.value,
        ) from exc
    if not isinstance(payload, dict):
        raise QELongTrendArtifactStoreError(
            f"long-trend JSON artifact must be an object: {path}",
            reason_code=QELongTrendReason.ARTIFACT_SCHEMA_MISMATCH.value,
        )
    return payload


def _decode_json_bytes(encoded: bytes, *, artifact_label: str) -> dict[str, Any]:
    """Decode bytes already bound to a verified digest; never re-read the path."""

    try:
        payload = json.loads(encoded.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise QELongTrendArtifactStoreError(
            f"stored long-trend JSON artifact is invalid: {artifact_label}",
            reason_code=QELongTrendReason.ARTIFACT_SCHEMA_MISMATCH.value,
        ) from exc
    if not isinstance(payload, dict):
        raise QELongTrendArtifactStoreError(
            f"stored long-trend JSON artifact must be an object: {artifact_label}",
            reason_code=QELongTrendReason.ARTIFACT_SCHEMA_MISMATCH.value,
        )
    return payload


def _sha256_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _validate_evaluation_id(value: str) -> None:
    if not _EVALUATION_RE.fullmatch(str(value or "")):
        raise QELongTrendArtifactStoreError(
            f"invalid evaluation_id: {value!r}",
            reason_code=QELongTrendReason.CAS_MANIFEST_CONFLICT.value,
        )


def _validate_artifact_type(value: str) -> None:
    if value not in ALLOWED_ARTIFACT_SCHEMAS:
        raise QELongTrendArtifactStoreError(
            f"unsupported long-trend artifact type: {value!r}",
            reason_code=QELongTrendReason.ARTIFACT_SCHEMA_MISMATCH.value,
        )


def _evaluation_id_from_value(value: str) -> str:
    text = str(value or "").strip()
    if text.startswith(f"{STORE_SCHEME}://"):
        parsed = urlparse(text)
        if parsed.netloc != "evaluations":
            raise QELongTrendArtifactStoreError(
                f"invalid long-trend URI authority: {parsed.netloc!r}",
                reason_code=QELongTrendReason.CAS_MANIFEST_CONFLICT.value,
            )
        parts = [part for part in parsed.path.split("/") if part]
        if len(parts) != 1:
            raise QELongTrendArtifactStoreError(
                f"manifest URI must identify one evaluation: {text!r}",
                reason_code=QELongTrendReason.CAS_MANIFEST_CONFLICT.value,
            )
        text = parts[0]
    _validate_evaluation_id(text)
    return text


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        # Windows does not support FlushFileBuffers on directory handles. Every
        # publication rename is therefore performed by _atomic_replace with
        # MOVEFILE_WRITE_THROUGH; there is no weaker best-effort path here.
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_replace(source: Path, target: Path) -> None:
    if os.name != "nt":
        os.replace(source, target)
        return
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    move_file_ex = kernel32.MoveFileExW
    move_file_ex.argtypes = (wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD)
    move_file_ex.restype = wintypes.BOOL
    movefile_replace_existing = 0x00000001
    movefile_write_through = 0x00000008
    if not move_file_ex(
        str(source),
        str(target),
        movefile_replace_existing | movefile_write_through,
    ):
        error = ctypes.get_last_error()
        raise OSError(error, f"MoveFileExW write-through replace failed: {source} -> {target}")
