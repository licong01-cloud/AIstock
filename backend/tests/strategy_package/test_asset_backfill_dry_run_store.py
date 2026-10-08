from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from backend.services.strategy_package.package_asset_store import LocalPackageAssetStore
from backend.services.trading_core.errors import PackageAssetInvalidError
from scripts.strategy_package_asset_backfill import DryRunPackageAssetStore


@pytest.mark.parametrize("origin", ["bytes", "file", "delegate"])
def test_full_contract_materializes_only_caller_temporary_file(tmp_path: Path, origin: str):
    root = tmp_path / "authoritative"
    delegate = LocalPackageAssetStore(root)
    data = b"model-data"
    digest = hashlib.sha256(data).hexdigest()
    source = tmp_path / "source.pkl"
    source.write_bytes(data)
    store = DryRunPackageAssetStore(delegate)
    if origin == "bytes":
        blob = store.put(data, kind="model_weight", sha256=digest)
    elif origin == "file":
        blob = store.put_file(source, kind="model_weight", sha256=digest, size_bytes=len(data))
    else:
        blob = delegate.put(data, kind="model_weight", sha256=digest)
    before = {str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()}
    assert store.exists(blob.uri)
    assert store.get(blob.uri) == data
    assert store.verify(blob.uri, sha256=digest, size_bytes=len(data)).sha256 == digest
    target = tmp_path / "temporary-smoke" / "model.pkl"
    store.materialize_file(blob.uri, target, sha256=digest, size_bytes=len(data))
    assert target.read_bytes() == data
    after = {str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()}
    assert before == after
    assert source.read_bytes() == data


def test_file_source_corruption_is_detected_before_materialization(tmp_path):
    source = tmp_path / "weight.pkl"
    source.write_bytes(b"valid")
    store = DryRunPackageAssetStore(LocalPackageAssetStore(tmp_path / "unused"))
    blob = store.put_file(source, kind="model_weight", sha256=hashlib.sha256(b"valid").hexdigest())
    source.write_bytes(b"other")
    target = tmp_path / "smoke" / "weight.pkl"
    with pytest.raises(PackageAssetInvalidError):
        store.materialize_file(blob.uri, target, sha256=blob.sha256, size_bytes=blob.size_bytes)
    with pytest.raises(PackageAssetInvalidError):
        store.get(blob.uri)
    assert not target.exists()


@pytest.mark.parametrize("sha,size", [("0" * 64, 5), (hashlib.sha256(b"valid").hexdigest(), 4)])
def test_put_file_rejects_wrong_identity_without_store_write(tmp_path, sha, size):
    source = tmp_path / "weight.pkl"
    source.write_bytes(b"valid")
    root = tmp_path / "unused"
    store = DryRunPackageAssetStore(LocalPackageAssetStore(root))
    with pytest.raises(PackageAssetInvalidError):
        store.put_file(source, kind="model_weight", sha256=sha, size_bytes=size)
    assert not root.exists()


def test_materialization_cannot_overwrite_existing_target(tmp_path):
    store = DryRunPackageAssetStore(LocalPackageAssetStore(tmp_path / "unused"))
    blob = store.put(b"valid", kind="model_weight")
    target = tmp_path / "retained.pkl"
    target.write_bytes(b"keep")
    with pytest.raises(FileExistsError):
        store.materialize_file(blob.uri, target, sha256=blob.sha256, size_bytes=5)
    assert target.read_bytes() == b"keep"


def test_temporary_output_removed_when_source_changes_during_stream(tmp_path, monkeypatch):
    store = DryRunPackageAssetStore(LocalPackageAssetStore(tmp_path / "unused"))
    source = tmp_path / "source.pkl"
    source.write_bytes(b"valid")
    blob = store.put_file(source, kind="model_weight", sha256=hashlib.sha256(b"valid").hexdigest())
    actual = store.verify

    def verify_then_change(*args, **kwargs):
        result = actual(*args, **kwargs)
        source.write_bytes(b"other")
        return result

    monkeypatch.setattr(store, "verify", verify_then_change)
    target = tmp_path / "smoke" / "weight.pkl"
    with pytest.raises(PackageAssetInvalidError):
        store.materialize_file(blob.uri, target, sha256=blob.sha256, size_bytes=5)
    assert not target.exists()
