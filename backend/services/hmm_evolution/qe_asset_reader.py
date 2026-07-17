"""Read-only QE task/loop asset catalog and content access."""

from __future__ import annotations

import hashlib
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

import httpx

from .errors import (
    ArtifactHashMismatchError,
    QEAssetCatalogIncompleteError,
    QEAssetUnavailableError,
)
from .models import (
    AssetAccessMode,
    AssetTrustLevel,
    CatalogCompleteness,
    QEAssetCatalog,
    QEAssetEntry,
    QEAssetReadReceipt,
    normalize_asset_path,
)


class WorkspaceReadClient(Protocol):
    async def list_workspace_files(self, task_id: str, loop_id: str) -> Mapping[str, Any]: ...

    async def stat_workspace_file(
        self, task_id: str, loop_id: str, file_path: str
    ) -> Mapping[str, Any]: ...

    async def download_workspace_file_bytes(
        self, task_id: str, loop_id: str, file_path: str
    ) -> bytes: ...


PartialCatalogProvider = Callable[[str, str], Awaitable[Sequence[Mapping[str, Any]]]]


@dataclass(frozen=True)
class QEAssetContent:
    data: bytes
    receipt: QEAssetReadReceipt


class QEExperimentAssetReader:
    """Expose only list/stat/read; mutation methods are deliberately unreachable."""

    def __init__(
        self,
        client: WorkspaceReadClient,
        *,
        partial_catalog_provider: PartialCatalogProvider | None = None,
        max_read_bytes: int = 64 * 1024 * 1024,
    ) -> None:
        if max_read_bytes < 1:
            raise ValueError("max_read_bytes must be positive")
        self._client = client
        self._partial_catalog_provider = partial_catalog_provider
        self._max_read_bytes = max_read_bytes

    async def list_assets(
        self,
        task_id: str,
        loop_name: str,
        *,
        require_complete: bool = False,
    ) -> QEAssetCatalog:
        warnings: list[str] = []
        try:
            payload = await self._client.list_workspace_files(task_id, loop_name)
            catalog = self._catalog_from_payload(task_id, loop_name, payload)
        except (httpx.HTTPError, RuntimeError, ValueError, TypeError) as exc:
            if self._partial_catalog_provider is None:
                raise QEAssetUnavailableError(
                    "QE node does not expose a usable loop asset catalog",
                    context={"task_id": task_id, "loop_name": loop_name, "error": str(exc)},
                ) from exc
            try:
                partial_rows = await self._partial_catalog_provider(task_id, loop_name)
            except Exception as provider_exc:
                raise QEAssetUnavailableError(
                    "QE asset catalog and partial manifest lookup both failed",
                    context={
                        "task_id": task_id,
                        "loop_name": loop_name,
                        "catalog_error": str(exc),
                        "manifest_error": str(provider_exc),
                    },
                ) from provider_exc
            warnings.append("node_complete_catalog_unavailable")
            catalog = QEAssetCatalog(
                task_id=task_id,
                loop_name=loop_name,
                catalog_completeness=CatalogCompleteness.PARTIAL,
                assets=tuple(
                    self._entry_from_mapping(
                        row,
                        catalog_completeness=CatalogCompleteness.PARTIAL,
                    )
                    for row in partial_rows
                ),
                warnings=tuple(warnings),
            )
        if require_complete and catalog.catalog_completeness is not CatalogCompleteness.COMPLETE:
            raise QEAssetCatalogIncompleteError(
                "complete QE loop asset catalog is required for this operation",
                context={"task_id": task_id, "loop_name": loop_name},
            )
        return catalog

    async def stat_asset(
        self,
        task_id: str,
        loop_name: str,
        relative_path: str,
    ) -> QEAssetEntry:
        path = normalize_asset_path(relative_path)
        try:
            payload = await self._client.stat_workspace_file(task_id, loop_name, path)
            return self._entry_from_mapping(payload, fallback_path=path)
        except (httpx.HTTPError, RuntimeError, ValueError, TypeError):
            catalog = await self.list_assets(task_id, loop_name)
            match = next((item for item in catalog.assets if item.relative_path == path), None)
            if match is not None:
                return match
            if catalog.catalog_completeness is CatalogCompleteness.COMPLETE:
                raise QEAssetUnavailableError(
                    "QE asset does not exist in the complete catalog",
                    context={"task_id": task_id, "loop_name": loop_name, "relative_path": path},
                )
            content = await self.read_asset(task_id, loop_name, path, declared_entry=None)
            return QEAssetEntry(
                relative_path=path,
                size_bytes=content.receipt.size_bytes,
                sha256=content.receipt.sha256,
                content_type=content.receipt.content_type,
                source=content.receipt.source,
                trust_level=AssetTrustLevel.UNVERIFIED_EVIDENCE,
                access_mode=AssetAccessMode.INSPECTION_ONLY,
                catalog_completeness=CatalogCompleteness.PARTIAL,
            )

    async def read_asset(
        self,
        task_id: str,
        loop_name: str,
        relative_path: str,
        *,
        declared_entry: QEAssetEntry | None = None,
    ) -> QEAssetContent:
        path = normalize_asset_path(relative_path)
        entry = declared_entry
        if entry is not None and entry.relative_path != path:
            raise ValueError("declared_entry path does not match requested path")
        if entry is not None and entry.size_bytes > self._max_read_bytes:
            raise QEAssetUnavailableError(
                "QE asset exceeds the in-memory read limit; streaming API is required",
                context={"relative_path": path, "size_bytes": entry.size_bytes},
            )
        try:
            data = await self._client.download_workspace_file_bytes(task_id, loop_name, path)
        except (httpx.HTTPError, RuntimeError) as exc:
            raise QEAssetUnavailableError(
                "QE asset read failed",
                context={"task_id": task_id, "loop_name": loop_name, "relative_path": path},
            ) from exc
        if len(data) > self._max_read_bytes:
            raise QEAssetUnavailableError(
                "QE asset exceeds the in-memory read limit; streaming API is required",
                context={"relative_path": path, "size_bytes": len(data)},
            )
        actual_sha256 = hashlib.sha256(data).hexdigest()
        if entry is not None and entry.size_bytes != len(data):
            raise ArtifactHashMismatchError(
                "QE asset size does not match catalog metadata",
                context={
                    "relative_path": path,
                    "expected_size": entry.size_bytes,
                    "actual_size": len(data),
                },
            )
        if entry is not None and entry.sha256 and entry.sha256 != actual_sha256:
            raise ArtifactHashMismatchError(
                "QE asset hash does not match catalog metadata",
                context={
                    "relative_path": path,
                    "expected_sha256": entry.sha256,
                    "actual_sha256": actual_sha256,
                },
            )
        completeness = (
            entry.catalog_completeness
            if entry is not None
            else CatalogCompleteness.PARTIAL
        )
        trust_level = entry.trust_level if entry is not None else AssetTrustLevel.UNVERIFIED_EVIDENCE
        access_mode = entry.access_mode if entry is not None else AssetAccessMode.INSPECTION_ONLY
        receipt = QEAssetReadReceipt(
            task_id=task_id,
            loop_name=loop_name,
            relative_path=path,
            source=entry.source if entry is not None else "qe_workspace_direct_read",
            sha256=actual_sha256,
            size_bytes=len(data),
            content_type=entry.content_type if entry is not None else None,
            trust_level=trust_level,
            access_mode=access_mode,
            catalog_completeness=completeness,
        )
        return QEAssetContent(data=data, receipt=receipt)

    async def read_text(
        self,
        task_id: str,
        loop_name: str,
        relative_path: str,
        *,
        declared_entry: QEAssetEntry | None = None,
    ) -> tuple[str, QEAssetReadReceipt]:
        content = await self.read_asset(
            task_id,
            loop_name,
            relative_path,
            declared_entry=declared_entry,
        )
        try:
            return content.data.decode("utf-8"), content.receipt
        except UnicodeDecodeError as exc:
            raise QEAssetUnavailableError(
                "QE asset is not valid UTF-8 text",
                context={"relative_path": normalize_asset_path(relative_path)},
            ) from exc

    def _catalog_from_payload(
        self,
        task_id: str,
        loop_name: str,
        payload: Mapping[str, Any],
    ) -> QEAssetCatalog:
        rows = payload.get("files")
        if rows is None:
            rows = payload.get("assets")
        if not isinstance(rows, list):
            raise ValueError("QE node asset catalog response has no files/assets list")
        completeness_raw = str(payload.get("catalog_completeness") or "partial").lower()
        completeness = CatalogCompleteness(completeness_raw)
        warnings = tuple(str(item) for item in payload.get("warnings") or ())
        return QEAssetCatalog(
            task_id=task_id,
            loop_name=loop_name,
            catalog_completeness=completeness,
            assets=tuple(
                self._entry_from_mapping(row, catalog_completeness=completeness)
                for row in rows
            ),
            warnings=warnings,
        )

    @staticmethod
    def _entry_from_mapping(
        row: Mapping[str, Any],
        *,
        fallback_path: str | None = None,
        catalog_completeness: CatalogCompleteness | None = None,
    ) -> QEAssetEntry:
        relative_path = row.get("relative_path") or row.get("path") or row.get("filename") or fallback_path
        if relative_path is None:
            raise ValueError("QE asset metadata has no relative path")
        trust_raw = row.get("trust_level") or AssetTrustLevel.UNVERIFIED_EVIDENCE.value
        access_raw = row.get("access_mode") or AssetAccessMode.INSPECTION_ONLY.value
        return QEAssetEntry(
            relative_path=str(relative_path),
            size_bytes=int(row.get("size_bytes", row.get("size", 0))),
            sha256=row.get("sha256"),
            content_type=row.get("content_type"),
            modified_at=row.get("modified_at") or row.get("modified"),
            source=str(row.get("source") or "qe_workspace"),
            trust_level=AssetTrustLevel(str(trust_raw)),
            access_mode=AssetAccessMode(str(access_raw)),
            schema_version=row.get("schema_version"),
            parser_contract=row.get("parser_contract"),
            catalog_completeness=catalog_completeness
            or CatalogCompleteness(str(row.get("catalog_completeness") or "partial")),
        )
