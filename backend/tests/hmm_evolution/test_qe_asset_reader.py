from __future__ import annotations

import asyncio
import hashlib

import pytest

from backend.services.hmm_evolution.errors import (
    ArtifactHashMismatchError,
    QEAssetCatalogIncompleteError,
    QEAssetUnavailableError,
)
from backend.services.hmm_evolution.models import CatalogCompleteness
from backend.services.hmm_evolution.qe_asset_reader import QEExperimentAssetReader
from backend.services.quantevolver.qe_workspace_client import (
    QEWorkspaceCatalogUnavailable,
)


class _CatalogClient:
    def __init__(self, data: bytes = b"hello") -> None:
        self.data = data

    async def list_workspace_files(self, task_id: str, loop_id: str):
        return {
            "catalog_completeness": "complete",
            "files": [
                {
                    "relative_path": "reports/result.json",
                    "size_bytes": len(self.data),
                    "sha256": hashlib.sha256(self.data).hexdigest(),
                    "content_type": "application/json",
                }
            ],
        }

    async def stat_workspace_file(self, task_id: str, loop_id: str, file_path: str):
        return (await self.list_workspace_files(task_id, loop_id))["files"][0]

    async def download_workspace_file_bytes(self, task_id: str, loop_id: str, file_path: str):
        return self.data

    async def cleanup_task_workspace(self, task_id: str):  # pragma: no cover - safety sentinel.
        raise AssertionError("mutation must never be called")


def test_complete_catalog_and_read_receipt_are_hash_verified() -> None:
    reader = QEExperimentAssetReader(_CatalogClient())
    catalog = asyncio.run(reader.list_assets("qe_task", "Loop8", require_complete=True))
    content = asyncio.run(
        reader.read_asset(
            "qe_task",
            "Loop8",
            "reports/result.json",
            declared_entry=catalog.assets[0],
        )
    )

    assert catalog.catalog_completeness is CatalogCompleteness.COMPLETE
    assert catalog.assets[0].catalog_completeness is CatalogCompleteness.COMPLETE
    assert content.data == b"hello"
    assert content.receipt.catalog_completeness is CatalogCompleteness.COMPLETE
    assert content.receipt.sha256 == hashlib.sha256(b"hello").hexdigest()
    assert not hasattr(reader, "cleanup_task_workspace")
    assert not hasattr(reader, "kill_loop")
    assert not hasattr(reader, "create_and_run_loop")


class _NoCatalogClient(_CatalogClient):
    async def list_workspace_files(self, task_id: str, loop_id: str):
        raise QEWorkspaceCatalogUnavailable("node endpoint missing")

    async def stat_workspace_file(self, task_id: str, loop_id: str, file_path: str):
        raise QEWorkspaceCatalogUnavailable("node endpoint missing")


async def _partial_provider(task_id: str, loop_id: str):
    return [
        {
            "relative_path": "pred.pkl",
            "size_bytes": 123,
            "sha256": "a" * 64,
            "source": "prediction_store_manifest",
        }
    ]


def test_partial_manifest_never_masquerades_as_complete_catalog() -> None:
    reader = QEExperimentAssetReader(
        _NoCatalogClient(),
        partial_catalog_provider=_partial_provider,
    )
    catalog = asyncio.run(reader.list_assets("qe_task", "Loop8"))

    assert catalog.catalog_completeness is CatalogCompleteness.PARTIAL
    assert catalog.assets[0].catalog_completeness is CatalogCompleteness.PARTIAL
    assert catalog.warnings == ("node_complete_catalog_unavailable",)
    with pytest.raises(QEAssetCatalogIncompleteError):
        asyncio.run(reader.list_assets("qe_task", "Loop8", require_complete=True))


@pytest.mark.parametrize(
    "payload",
    [
        {"catalog_completeness": "complete"},
        {"catalog_completeness": "complete", "files": "not-a-list"},
        {
            "catalog_completeness": "complete",
            "files": [{"relative_path": "pred.pkl", "size_bytes": "invalid"}],
        },
        {
            "catalog_completeness": "complete",
            "files": [
                {"relative_path": "pred.pkl", "size_bytes": 1},
                {"relative_path": "pred.pkl", "size_bytes": 1},
            ],
        },
    ],
)
def test_malformed_catalog_fails_loud_instead_of_using_partial_fallback(payload) -> None:
    class _MalformedCatalogClient(_CatalogClient):
        async def list_workspace_files(self, task_id: str, loop_id: str):
            return payload

    provider_called = False

    async def provider(task_id: str, loop_id: str):
        nonlocal provider_called
        provider_called = True
        return []

    reader = QEExperimentAssetReader(
        _MalformedCatalogClient(),
        partial_catalog_provider=provider,
    )
    with pytest.raises(QEAssetUnavailableError, match="invalid or unreadable"):
        asyncio.run(reader.list_assets("qe_task", "Loop8"))
    assert provider_called is False


def test_malformed_stat_fails_loud_instead_of_reading_raw_content() -> None:
    class _MalformedStatClient(_CatalogClient):
        async def stat_workspace_file(self, task_id: str, loop_id: str, file_path: str):
            return {"relative_path": file_path, "size_bytes": "invalid"}

        async def download_workspace_file_bytes(
            self, task_id: str, loop_id: str, file_path: str
        ):
            raise AssertionError("malformed stat must not fall back to content download")

    reader = QEExperimentAssetReader(_MalformedStatClient())
    with pytest.raises(QEAssetUnavailableError, match="metadata is invalid"):
        asyncio.run(reader.stat_asset("qe_task", "Loop8", "reports/result.json"))


def test_read_rejects_catalog_hash_mismatch() -> None:
    client = _CatalogClient(data=b"tampered")
    reader = QEExperimentAssetReader(client)
    catalog = asyncio.run(reader.list_assets("qe_task", "Loop8"))
    declared = catalog.assets[0].model_copy(update={"sha256": "0" * 64})

    with pytest.raises(ArtifactHashMismatchError):
        asyncio.run(
            reader.read_asset(
                "qe_task",
                "Loop8",
                "reports/result.json",
                declared_entry=declared,
            )
        )


@pytest.mark.parametrize(
    "path",
    ["../secret", "/etc/passwd", "C:/secret", "dir\\secret", "a/./b"],
)
def test_asset_paths_fail_closed(path: str) -> None:
    reader = QEExperimentAssetReader(_CatalogClient())
    with pytest.raises(Exception, match="path|relative|absolute|unsafe|traversal"):
        asyncio.run(reader.stat_asset("qe_task", "Loop8", path))
