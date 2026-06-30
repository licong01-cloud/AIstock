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
            size = target.stat().st_size
            if size != len(payload):
                raise PackageAssetInvalidError(
                    "existing strategy package asset blob size mismatch",
                    context={
                        "reason_code": "strategy_package_asset_blob_collision",
                        "asset_kind": safe_kind,
                        "sha256": digest,
                        "existing_size_bytes": size,
                        "new_size_bytes": len(payload),
                    },
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
                tmp_path.replace(target)
            except Exception:
                tmp_path.unlink(missing_ok=True)
                raise
        return PackageAssetBlob(kind=safe_kind, uri=_blob_uri(digest), sha256=digest, size_bytes=len(payload))

    def get(self, uri: str) -> bytes:
        path = self._path_from_uri(uri)
        if not path.exists():
            raise PackageAssetInvalidError(
                "strategy package asset blob is missing",
                context={"reason_code": "strategy_package_asset_blob_missing", "asset_ref": uri},
            )
        return path.read_bytes()

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
