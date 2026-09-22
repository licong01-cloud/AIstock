"""Content-addressed storage for StrategyPackage-owned runtime assets."""

from __future__ import annotations

import hashlib
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from backend.services.model_store.artifact_store import safe_store_component, validate_store_root
from backend.services.trading_core.errors import PackageAssetInvalidError


PACKAGE_ASSET_URI_SCHEME = "aistock-package-asset"
PACKAGE_ASSET_ROOT_ENV = "AISTOCK_PACKAGE_ASSET_STORE_ROOT"


@dataclass(frozen=True)
class PackageAssetBlob:
    kind: str
    uri: str
    sha256: str
    size_bytes: int


class PackageAssetStore:
    """Small adapter contract for immutable package asset blobs."""

    def put(self, data: bytes, *, kind: str, sha256: str | None = None) -> PackageAssetBlob:
        raise NotImplementedError

    def get(self, uri: str) -> bytes:
        raise NotImplementedError

    def exists(self, uri: str) -> bool:
        raise NotImplementedError

    def put_file(
        self,
        source_path: str | Path,
        *,
        kind: str,
        sha256: str,
        size_bytes: int | None = None,
    ) -> PackageAssetBlob:
        raise NotImplementedError

    def verify(self, uri: str, *, sha256: str, size_bytes: int) -> PackageAssetBlob:
        raise NotImplementedError


class LocalPackageAssetStore(PackageAssetStore):
    """Local SSD-backed content-addressed store under rdagent_assets/package_assets."""

    def __init__(self, root: str | Path | None = None) -> None:
        configured = (os.getenv(PACKAGE_ASSET_ROOT_ENV) or "").strip()
        if root is None and configured:
            root = configured
        if root is None:
            root = Path(__file__).resolve().parents[3] / "rdagent_assets" / "package_assets"
        self.root = validate_store_root(Path(root))

    def put(self, data: bytes, *, kind: str, sha256: str | None = None) -> PackageAssetBlob:
        payload = _ensure_bytes(data, field_name="data")
        safe_kind = safe_store_component(kind, field_name="kind")
        digest = hashlib.sha256(payload).hexdigest()
        expected = _normalize_sha256(sha256, field_name="sha256") if sha256 else None
        if expected and expected != digest:
            raise PackageAssetInvalidError(
                "strategy package asset sha256 mismatch",
                context={
                    "reason_code": "strategy_package_asset_sha_mismatch",
                    "asset_kind": safe_kind,
                    "expected_sha256": expected,
                    "actual_sha256": digest,
                },
            )
        self._ensure_ready()
        target = self._blob_path(digest)
        if target.exists():
            verified = self.verify(_blob_uri(digest), sha256=digest, size_bytes=len(payload))
            return PackageAssetBlob(
                kind=safe_kind,
                uri=verified.uri,
                sha256=verified.sha256,
                size_bytes=verified.size_bytes,
            )
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            tmp_dir = self.root / "tmp"
            tmp_dir.mkdir(parents=True, exist_ok=True)
            fd, tmp_name = tempfile.mkstemp(prefix="package_asset_", suffix=".blob", dir=tmp_dir)
            tmp_path = Path(tmp_name)
            try:
                with os.fdopen(fd, "wb") as out:
                    out.write(payload)
                    out.flush()
                    os.fsync(out.fileno())
                staged_sha, staged_size = _hash_file(tmp_path)
                if staged_sha != digest or staged_size != len(payload):
                    raise PackageAssetInvalidError(
                        "strategy package staged asset identity mismatch",
                        context={
                            "reason_code": "strategy_package_asset_staging_identity_mismatch",
                            "asset_kind": safe_kind,
                            "expected_sha256": digest,
                            "actual_sha256": staged_sha,
                            "expected_size_bytes": len(payload),
                            "actual_size_bytes": staged_size,
                        },
                    )
                _commit_staged_blob(tmp_path, target)
            except Exception:
                tmp_path.unlink(missing_ok=True)
                raise
        verified = self.verify(_blob_uri(digest), sha256=digest, size_bytes=len(payload))
        return PackageAssetBlob(
            kind=safe_kind,
            uri=verified.uri,
            sha256=verified.sha256,
            size_bytes=verified.size_bytes,
        )

    def get(self, uri: str) -> bytes:
        path = self._path_from_uri(uri)
        if not path.exists():
            raise PackageAssetInvalidError(
                "strategy package asset blob is missing",
                context={"reason_code": "strategy_package_asset_blob_missing", "asset_ref": uri},
            )
        return path.read_bytes()

    def put_file(
        self,
        source_path: str | Path,
        *,
        kind: str,
        sha256: str,
        size_bytes: int | None = None,
    ) -> PackageAssetBlob:
        """Publish one exact ordinary file without loading it into memory."""

        source = Path(source_path)
        safe_kind = safe_store_component(kind, field_name="kind")
        expected_sha = _normalize_sha256(sha256, field_name="sha256")
        expected_size = int(size_bytes) if size_bytes is not None else None
        _assert_publishable_source(source)
        actual_sha, actual_size = _hash_file(source)
        if actual_sha != expected_sha or (expected_size is not None and actual_size != expected_size):
            raise PackageAssetInvalidError(
                "strategy package source asset identity mismatch",
                context={
                    "reason_code": "strategy_package_asset_source_identity_mismatch",
                    "asset_kind": safe_kind,
                    "expected_sha256": expected_sha,
                    "actual_sha256": actual_sha,
                    "expected_size_bytes": expected_size,
                    "actual_size_bytes": actual_size,
                },
            )
        self._ensure_ready()
        target = self._blob_path(expected_sha)
        if target.exists():
            return self.verify(
                _blob_uri(expected_sha),
                sha256=expected_sha,
                size_bytes=actual_size,
            )

        target.parent.mkdir(parents=True, exist_ok=True)
        tmp_dir = self.root / "tmp"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(prefix="package_asset_", suffix=".staging", dir=tmp_dir)
        tmp_path = Path(tmp_name)
        try:
            with source.open("rb") as src, os.fdopen(fd, "wb") as out:
                while chunk := src.read(1024 * 1024):
                    out.write(chunk)
                out.flush()
                os.fsync(out.fileno())
            staged_sha, staged_size = _hash_file(tmp_path)
            if staged_sha != expected_sha or staged_size != actual_size:
                raise PackageAssetInvalidError(
                    "strategy package staged asset identity mismatch",
                    context={
                        "reason_code": "strategy_package_asset_staging_identity_mismatch",
                        "asset_kind": safe_kind,
                        "expected_sha256": expected_sha,
                        "actual_sha256": staged_sha,
                        "expected_size_bytes": actual_size,
                        "actual_size_bytes": staged_size,
                    },
                )
            _commit_staged_blob(tmp_path, target)
            return self.verify(
                _blob_uri(expected_sha),
                sha256=expected_sha,
                size_bytes=actual_size,
            )
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise

    def verify(self, uri: str, *, sha256: str, size_bytes: int) -> PackageAssetBlob:
        expected_sha = _normalize_sha256(sha256, field_name="sha256")
        path = self._path_from_uri(uri)
        if not path.exists():
            raise PackageAssetInvalidError(
                "strategy package asset blob is missing",
                context={"reason_code": "strategy_package_asset_blob_missing", "asset_ref": uri},
            )
        actual_sha, actual_size = _hash_file(path)
        if actual_sha != expected_sha or actual_size != int(size_bytes):
            raise PackageAssetInvalidError(
                "strategy package asset readback identity mismatch",
                context={
                    "reason_code": "strategy_package_asset_readback_mismatch",
                    "asset_ref": uri,
                    "expected_sha256": expected_sha,
                    "actual_sha256": actual_sha,
                    "expected_size_bytes": int(size_bytes),
                    "actual_size_bytes": actual_size,
                },
            )
        return PackageAssetBlob(
            kind="verified_blob",
            uri=uri,
            sha256=actual_sha,
            size_bytes=actual_size,
        )

    def exists(self, uri: str) -> bool:
        try:
            return self._path_from_uri(uri).exists()
        except PackageAssetInvalidError:
            return False

    def _ensure_ready(self) -> None:
        for child in ("blobs", "tmp"):
            (self.root / child).mkdir(parents=True, exist_ok=True)

    def _blob_path(self, digest: str) -> Path:
        normalized = _normalize_sha256(digest, field_name="sha256")
        return self.root / "blobs" / normalized[:2] / normalized

    def _path_from_uri(self, uri: str) -> Path:
        digest = _sha_from_uri(uri)
        path = self._blob_path(digest).resolve()
        root = self.root.resolve()
        if root not in path.parents:
            raise PackageAssetInvalidError(
                "strategy package asset URI escapes store root",
                context={"reason_code": "strategy_package_asset_uri_invalid", "asset_ref": uri},
            )
        return path


class ObjectPackageAssetStore(PackageAssetStore):
    """Placeholder for future object storage; deliberately not silently emulated."""

    def put(self, data: bytes, *, kind: str, sha256: str | None = None) -> PackageAssetBlob:
        raise NotImplementedError("object package asset store is not implemented in Batch 1")

    def get(self, uri: str) -> bytes:
        raise NotImplementedError("object package asset store is not implemented in Batch 1")

    def exists(self, uri: str) -> bool:
        raise NotImplementedError("object package asset store is not implemented in Batch 1")

    def put_file(
        self,
        source_path: str | Path,
        *,
        kind: str,
        sha256: str,
        size_bytes: int | None = None,
    ) -> PackageAssetBlob:
        raise NotImplementedError("object package asset store is not implemented")

    def verify(self, uri: str, *, sha256: str, size_bytes: int) -> PackageAssetBlob:
        raise NotImplementedError("object package asset store is not implemented")


def _blob_uri(digest: str) -> str:
    normalized = _normalize_sha256(digest, field_name="sha256")
    return f"{PACKAGE_ASSET_URI_SCHEME}://blobs/{normalized}"


def _sha_from_uri(uri: str) -> str:
    text = str(uri or "").strip()
    parsed = urlparse(text)
    if parsed.scheme != PACKAGE_ASSET_URI_SCHEME or parsed.netloc != "blobs":
        raise PackageAssetInvalidError(
            "unsupported strategy package asset URI",
            context={"reason_code": "strategy_package_asset_uri_invalid", "asset_ref": uri},
        )
    parts = [part for part in parsed.path.strip("/").split("/") if part]
    if len(parts) != 1:
        raise PackageAssetInvalidError(
            "strategy package asset URI must contain one blob digest",
            context={"reason_code": "strategy_package_asset_uri_invalid", "asset_ref": uri},
        )
    return _normalize_sha256(parts[0], field_name="asset_ref")


def _normalize_sha256(value: str | None, *, field_name: str) -> str:
    digest = str(value or "").strip().lower()
    if len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest):
        raise PackageAssetInvalidError(
            "strategy package asset sha256 is invalid",
            context={"reason_code": "strategy_package_asset_sha_invalid", field_name: value},
        )
    return digest


def _ensure_bytes(value: bytes | bytearray | memoryview, *, field_name: str) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    raise PackageAssetInvalidError(
        "strategy package asset payload must be bytes",
        context={"reason_code": "strategy_package_asset_payload_invalid", "field_name": field_name},
    )


def _assert_publishable_source(path: Path) -> None:
    if path.is_symlink() or not path.exists() or not path.is_file():
        raise PackageAssetInvalidError(
            "strategy package source asset must be an existing ordinary file",
            context={
                "reason_code": "strategy_package_asset_source_type_invalid",
                "source_path": str(path),
            },
        )
    if os.name == "nt":
        attrs = getattr(path.stat(follow_symlinks=False), "st_file_attributes", 0)
        reparse_flag = getattr(__import__("stat"), "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
        if attrs & reparse_flag:
            raise PackageAssetInvalidError(
                "strategy package source asset must not be a reparse point",
                context={
                    "reason_code": "strategy_package_asset_source_type_invalid",
                    "source_path": str(path),
                },
            )


def _hash_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _commit_staged_blob(staged: Path, target: Path) -> None:
    """Atomically create an immutable CAS path without replacing a winner."""

    try:
        os.link(staged, target)
    except FileExistsError:
        # A concurrent publisher won.  The caller performs full readback and
        # rejects a collision instead of overwriting it.
        pass
    finally:
        staged.unlink(missing_ok=True)
