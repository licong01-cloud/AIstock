from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from backend.services.qe_archive.asset_publisher import QEArchiveAssetPublisher
from backend.services.strategy_package.package_asset import (
    StrategyPackageAssetRecord,
    StrategyPackageAssetType,
)
from backend.services.strategy_package.package_asset_store import LocalPackageAssetStore
from backend.services.trading_core.errors import PackageAssetInvalidError


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def test_file_publish_is_atomic_verified_and_deduplicated(tmp_path: Path) -> None:
    root = tmp_path / "cas"
    source_a = tmp_path / "workspace-a" / "model.bin"
    source_b = tmp_path / "workspace-b" / "same-model.bin"
    source_a.parent.mkdir()
    source_b.parent.mkdir()
    content = b"stable-model-weights"
    source_a.write_bytes(content)
    source_b.write_bytes(content)
    store = LocalPackageAssetStore(root)

    first = store.put_file(source_a, kind="model_weight", sha256=_sha(content), size_bytes=len(content))
    second = store.put_file(source_b, kind="model_weight", sha256=_sha(content), size_bytes=len(content))

    assert first.uri == second.uri
    assert first.sha256 == second.sha256
    assert len(list((root / "blobs").rglob(first.sha256))) == 1
    assert list((root / "tmp").iterdir()) == []
    assert store.get(first.uri) == content


def test_existing_corrupt_blob_fails_closed(tmp_path: Path) -> None:
    content = b"authoritative"
    digest = _sha(content)
    root = tmp_path / "cas"
    store = LocalPackageAssetStore(root)
    target = root / "blobs" / digest[:2] / digest
    target.parent.mkdir(parents=True)
    target.write_bytes(b"corrupt-same-size")
    source = tmp_path / "source.bin"
    source.write_bytes(content)

    with pytest.raises(PackageAssetInvalidError) as excinfo:
        store.put_file(source, kind="model_weight", sha256=digest, size_bytes=len(content))
    assert excinfo.value.context["reason_code"] == "strategy_package_asset_readback_mismatch"


def test_archive_and_strategy_package_share_one_blob_after_workspace_removal(tmp_path: Path) -> None:
    content = b"model-and-preprocessor"
    digest = _sha(content)
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    source = workspace / "asset.bin"
    source.write_bytes(content)
    store = LocalPackageAssetStore(tmp_path / "cas")
    publisher = QEArchiveAssetPublisher(store)

    published = publisher.publish(
        run_id="qear_run_1",
        value_class="A",
        retention_class="protected",
        manifest=[
            {
                "artifact_type": "model_weight",
                "artifact_name": "alpha-core-weight",
                "source_path": str(source),
                "sha256": digest,
                "size_bytes": len(content),
                "media_type": "application/octet-stream",
                "logical_role": "model_weight",
                "producer_identity": "qe:task-1:loop-1",
                "source_receipt": {"node_id": "wsl2-5080", "path_sha256": "b" * 64},
            }
        ],
    )
    archive_ref = published.artifacts[0]
    package_ref = StrategyPackageAssetRecord(
        package_id="pkg_1",
        asset_type=StrategyPackageAssetType.MODEL_WEIGHT,
        asset_ref=archive_ref["artifact_uri"],
        asset_sha256=archive_ref["sha256"],
        asset_size_bytes=archive_ref["size_bytes"],
        protected_asset=True,
        metadata={"archive_run_id": "qear_run_1"},
    )
    source.unlink()
    workspace.rmdir()

    assert package_ref.asset_ref == archive_ref["artifact_uri"]
    assert store.get(package_ref.asset_ref) == content
    assert len(list((tmp_path / "cas" / "blobs").rglob(digest))) == 1


def test_symlink_source_is_rejected(tmp_path: Path) -> None:
    target = tmp_path / "target.bin"
    target.write_bytes(b"data")
    link = tmp_path / "link.bin"
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("symlink creation is unavailable")
    store = LocalPackageAssetStore(tmp_path / "cas")
    with pytest.raises(PackageAssetInvalidError) as excinfo:
        store.put_file(link, kind="other", sha256=_sha(b"data"), size_bytes=4)
    assert excinfo.value.context["reason_code"] == "strategy_package_asset_source_type_invalid"
