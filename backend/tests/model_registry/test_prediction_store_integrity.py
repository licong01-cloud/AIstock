import hashlib
import io
from pathlib import Path

import pytest

from backend.services.model_store.artifact_store import PredictionArtifactStore, PredictionStoreError


def test_prediction_store_restores_same_size_corruption_from_verified_upload(tmp_path, caplog):
    store = PredictionArtifactStore(root=tmp_path / "store")
    data = b"known-model-bytes"
    digest, size = store._write_blob(io.BytesIO(data))
    target = store.blob_path(digest)
    target.write_bytes(b"x" * size)
    actual = store._write_blob(io.BytesIO(data))
    assert actual == (digest, size)
    assert hashlib.sha256(target.read_bytes()).hexdigest() == digest
    assert "prediction_store_blob_repaired" in caplog.text
    assert list((store.root / "tmp").iterdir()) == []


def test_prediction_store_verified_duplicate_preserves_existing_inode(tmp_path):
    store = PredictionArtifactStore(root=tmp_path / "store")
    digest, size = store._write_blob(io.BytesIO(b"valid"))
    target = store.blob_path(digest)
    before = target.stat()
    assert store._write_blob(io.BytesIO(b"valid")) == (digest, size)
    after = target.stat()
    assert (before.st_dev, before.st_ino, before.st_mtime_ns) == (after.st_dev, after.st_ino, after.st_mtime_ns)


def test_prediction_store_size_corruption_still_fails_without_manifest_write(tmp_path):
    store = PredictionArtifactStore(root=tmp_path / "store")
    digest, _ = store._write_blob(io.BytesIO(b"valid"))
    store.blob_path(digest).write_bytes(b"broken-and-longer")
    with pytest.raises(PredictionStoreError, match="size mismatch"):
        store.write_artifacts(run_key="must_not_publish", files={"model_params": ("params.pkl", io.BytesIO(b"valid"))})
    assert not store.manifest_path("must_not_publish").exists()
    assert list((store.root / "tmp").iterdir()) == []


def test_prediction_store_failed_restore_preserves_target_and_cleans_staging(tmp_path, monkeypatch):
    store = PredictionArtifactStore(root=tmp_path / "store")
    digest, size = store._write_blob(io.BytesIO(b"valid"))
    target = store.blob_path(digest)
    target.write_bytes(b"other")
    original = Path.replace

    def refuse_restore(source, destination):
        if Path(destination) == target:
            raise PermissionError("in-use target")
        return original(source, destination)

    monkeypatch.setattr(Path, "replace", refuse_restore)
    with pytest.raises(PermissionError, match="in-use"):
        store._write_blob(io.BytesIO(b"valid"))
    assert target.read_bytes() == b"other"
    assert target.stat().st_size == size
    assert list((store.root / "tmp").iterdir()) == []


def test_prediction_store_interrupted_upload_never_publishes_partial_blob(tmp_path):
    store = PredictionArtifactStore(root=tmp_path / "store")

    class Interrupted:
        calls = 0

        def read(self, size):
            self.calls += 1
            if self.calls == 1:
                return b"partial"
            raise OSError("upload interrupted")

    with pytest.raises(OSError, match="interrupted"):
        store._write_blob(Interrupted())
    assert not (store.root / "blobs").exists()
    assert list((store.root / "tmp").iterdir()) == []
