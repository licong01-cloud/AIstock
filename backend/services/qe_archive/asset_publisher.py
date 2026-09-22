"""Publish explicitly manifested QE assets to the shared local CAS."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.services.strategy_package.package_asset_store import (
    LocalPackageAssetStore,
    PackageAssetStore,
)
from backend.services.trading_core.errors import PackageAssetInvalidError


class QEArchiveAssetPublishError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        reason_code: str,
        context: Mapping[str, Any] | None = None,
        published_artifacts: Sequence[Mapping[str, Any]] = (),
        published_count: int = 0,
        reused_count: int = 0,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.context = {"reason_code": reason_code, **dict(context or {})}
        self.published_artifacts = tuple(dict(item) for item in published_artifacts)
        self.published_count = int(published_count)
        self.reused_count = int(reused_count)


@dataclass(frozen=True)
class QEArchiveAssetPublishResult:
    status: str
    artifacts: tuple[dict[str, Any], ...]
    published_count: int
    reused_count: int


class QEArchiveAssetPublisher:
    """Publish only files listed by the run-scoped asset manifest.

    Directory discovery, wildcard expansion and workspace copying are
    intentionally absent.  WSL/node1 callers must first materialize the exact
    requested file on the controller through their existing node API.
    """

    def __init__(self, store: PackageAssetStore | None = None) -> None:
        self._store = store or LocalPackageAssetStore()

    def publish(
        self,
        *,
        run_id: str,
        value_class: str,
        retention_class: str,
        manifest: Sequence[Mapping[str, Any]],
    ) -> QEArchiveAssetPublishResult:
        run_id = str(run_id or "").strip()
        if not run_id:
            raise QEArchiveAssetPublishError(
                "QE asset publication requires run_id",
                reason_code="qe_asset_publish_run_id_missing",
            )
        items = [dict(item) for item in manifest]
        if not items:
            return QEArchiveAssetPublishResult(
                status="not_required",
                artifacts=(),
                published_count=0,
                reused_count=0,
            )

        records: list[dict[str, Any]] = []
        published = 0
        reused = 0
        identities: set[tuple[str, str]] = set()
        for item in items:
            try:
                validated = _validate_item(item)
            except QEArchiveAssetPublishError as exc:
                raise QEArchiveAssetPublishError(
                    str(exc),
                    reason_code=exc.reason_code,
                    context=exc.context,
                    published_artifacts=records,
                    published_count=published,
                    reused_count=reused,
                ) from exc
            identity = (validated["artifact_type"], validated["artifact_name"])
            if identity in identities:
                raise QEArchiveAssetPublishError(
                    "QE asset publish manifest contains a duplicate logical identity",
                    reason_code="qe_asset_publish_manifest_duplicate",
                    context={"artifact_type": identity[0], "artifact_name": identity[1]},
                    published_artifacts=records,
                    published_count=published,
                    reused_count=reused,
                )
            identities.add(identity)
            expected_uri = f"aistock-package-asset://blobs/{validated['sha256']}"
            existed = self._store.exists(expected_uri)
            try:
                blob = self._store.put_file(
                    validated["source_path"],
                    kind=validated["logical_role"],
                    sha256=validated["sha256"],
                    size_bytes=validated["size_bytes"],
                )
                self._store.verify(
                    blob.uri,
                    sha256=validated["sha256"],
                    size_bytes=validated["size_bytes"],
                )
            except PackageAssetInvalidError as exc:
                raise QEArchiveAssetPublishError(
                    "QE asset CAS publication failed",
                    reason_code=str(exc.context.get("reason_code") or "qe_asset_publish_failed"),
                    context={
                        "artifact_type": validated["artifact_type"],
                        "artifact_name": validated["artifact_name"],
                        **dict(exc.context),
                    },
                    published_artifacts=records,
                    published_count=published,
                    reused_count=reused,
                ) from exc
            reused += int(existed)
            published += int(not existed)
            now = datetime.now(timezone.utc).isoformat()
            records.append(
                {
                    "run_id": run_id,
                    "artifact_type": validated["artifact_type"],
                    "artifact_name": validated["artifact_name"],
                    "storage_tier": "local_cas",
                    "artifact_uri": blob.uri,
                    "source_system": "qe_asset_lifecycle",
                    "source_uri": Path(validated["source_path"]).as_posix(),
                    "sha256": blob.sha256,
                    "size_bytes": blob.size_bytes,
                    "content_type": validated["media_type"],
                    "collected_status": "available",
                    "parser_status": "not_required",
                    "metadata": {
                        "schema_version": "qe_cas_asset_reference_v1",
                        "logical_role": validated["logical_role"],
                        "producer_identity": validated["producer_identity"],
                        "source_receipt": validated["source_receipt"],
                        "retention_class": retention_class,
                        "protected_by": [f"qe_archive:{run_id}"],
                        "value_class": value_class,
                        "verified_at": now,
                        "availability": "available",
                        "physical_blob_reused": existed,
                    },
                }
            )
        return QEArchiveAssetPublishResult(
            status="published",
            artifacts=tuple(records),
            published_count=published,
            reused_count=reused,
        )


def asset_publish_manifest(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = payload.get("qe_asset_publish_manifest")
    config = payload.get("config")
    if raw is None and isinstance(config, Mapping):
        raw = config.get("qe_asset_publish_manifest")
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes, bytearray)):
        return []
    return [dict(item) for item in raw if isinstance(item, Mapping)]


def _validate_item(item: Mapping[str, Any]) -> dict[str, Any]:
    required = (
        "artifact_type",
        "artifact_name",
        "source_path",
        "sha256",
        "size_bytes",
        "media_type",
        "logical_role",
        "producer_identity",
        "source_receipt",
    )
    missing = [key for key in required if item.get(key) in (None, "", {})]
    if missing:
        raise QEArchiveAssetPublishError(
            "QE asset publish manifest is incomplete",
            reason_code="qe_asset_publish_manifest_incomplete",
            context={"missing": missing},
        )
    digest = str(item["sha256"]).strip().lower()
    if len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest):
        raise QEArchiveAssetPublishError(
            "QE asset publish manifest sha256 is invalid",
            reason_code="qe_asset_publish_sha_invalid",
            context={"artifact_name": item.get("artifact_name")},
        )
    try:
        size = int(item["size_bytes"])
    except (TypeError, ValueError) as exc:
        raise QEArchiveAssetPublishError(
            "QE asset publish manifest size is invalid",
            reason_code="qe_asset_publish_size_invalid",
        ) from exc
    if size < 0:
        raise QEArchiveAssetPublishError(
            "QE asset publish manifest size is invalid",
            reason_code="qe_asset_publish_size_invalid",
        )
    return {
        **dict(item),
        "artifact_type": str(item["artifact_type"]).strip(),
        "artifact_name": str(item["artifact_name"]).strip(),
        "source_path": str(item["source_path"]).strip(),
        "sha256": digest,
        "size_bytes": size,
        "media_type": str(item["media_type"]).strip(),
        "logical_role": str(item["logical_role"]).strip(),
        "producer_identity": str(item["producer_identity"]).strip(),
        "source_receipt": dict(item["source_receipt"]),
    }
