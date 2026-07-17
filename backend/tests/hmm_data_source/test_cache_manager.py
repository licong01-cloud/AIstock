"""Security and lifecycle tests for the HMM artifact cache."""

from __future__ import annotations

import hashlib
import json
import os
import pickle
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest

from backend.services.hmm_data_source import ArtifactCacheManager, CacheError


PROVENANCE = {"source": "test_fixture"}


def qe_provenance(payload: bytes, *, row_count: int) -> dict:
    workspace_path = "mlruns/1/abc/artifacts/pred.pkl"
    return {
        "source": "qe_workspace",
        "task_id": "qe_test",
        "loop_name": "Loop1",
        "workspace_path": workspace_path,
        "remote_manifest_path": f"{workspace_path}.manifest.json",
        "remote_schema_version": "qe_dataframe_v1",
        "remote_sha256": hashlib.sha256(payload).hexdigest(),
        "remote_size_bytes": len(payload),
        "remote_row_count": row_count,
        "remote_quality_status": "ok",
    }


@pytest.fixture
def cache_manager(tmp_path):
    return ArtifactCacheManager(str(tmp_path / "cache"), allow_test_fixtures=True)


def test_path_uses_hash_key_and_stays_below_root(cache_manager):
    loop_ref = "qe_20260502_131502_9b54/Loop1"
    path = cache_manager.get_artifact_path(loop_ref, "pred.pkl")

    assert path.parent.name == hashlib.sha256(loop_ref.encode()).hexdigest()
    assert path.name == "pred.pkl"
    assert path.resolve().is_relative_to(cache_manager.cache_dir.resolve())


@pytest.mark.parametrize(
    "loop_ref",
    ("../outside/Loop1", "qe_test/../../outside", "/absolute/Loop1", "qe_test\\Loop1"),
)
def test_rejects_unsafe_loop_refs(cache_manager, loop_ref):
    with pytest.raises(CacheError, match="loop_ref"):
        cache_manager.get_artifact_path(loop_ref, "pred.pkl")


@pytest.mark.parametrize("artifact_name", ("../pred.pkl", "x/pred.pkl", "x\\pred.pkl", "."))
def test_rejects_unsafe_artifact_names(cache_manager, artifact_name):
    with pytest.raises(CacheError, match="artifact_name"):
        cache_manager.get_artifact_path("qe_test/Loop1", artifact_name)


def test_save_requires_explicit_provenance(cache_manager):
    with pytest.raises(CacheError, match="provenance"):
        cache_manager.save_artifact("qe_test/Loop1", "pred.pkl", b"data")


def test_production_cache_rejects_test_fixture_provenance(tmp_path):
    manager = ArtifactCacheManager(str(tmp_path / "cache"))
    with pytest.raises(CacheError, match="test_fixture"):
        manager.save_artifact(
            "qe_test/Loop1", "pred.pkl", b"data", metadata=PROVENANCE
        )


def test_save_and_load_valid_manifest(cache_manager):
    path = cache_manager.save_artifact(
        "qe_test/Loop1", "pred.pkl", b"data", metadata=PROVENANCE
    )

    assert path.exists()
    assert cache_manager.load_artifact("qe_test/Loop1", "pred.pkl") == b"data"
    manifest = cache_manager._load_metadata("qe_test/Loop1", "pred.pkl")
    assert manifest is not None
    assert manifest["schema_version"] == "hmm_artifact_manifest_v1"
    assert manifest["provenance"] == PROVENANCE


def test_artifact_without_manifest_is_not_trusted(cache_manager):
    artifact = cache_manager.get_artifact_path("qe_test/Loop1", "pred.pkl")
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"untrusted")

    assert not cache_manager.is_cached("qe_test/Loop1", "pred.pkl")
    with pytest.raises(CacheError, match="manifest"):
        cache_manager.load_artifact("qe_test/Loop1", "pred.pkl")


def test_checksum_validation_rejects_tampering(cache_manager):
    artifact = cache_manager.save_artifact(
        "qe_test/Loop1", "pred.pkl", b"original", metadata=PROVENANCE
    )
    artifact.write_bytes(b"tampered")

    with pytest.raises(CacheError, match="size|checksum"):
        cache_manager.load_artifact("qe_test/Loop1", "pred.pkl")


def test_manifest_identity_cannot_be_replayed(cache_manager):
    cache_manager.save_artifact(
        "qe_test/Loop1", "pred.pkl", b"data", metadata=PROVENANCE
    )
    manifest_path = cache_manager._manifest_path("qe_test/Loop1", "pred.pkl")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["loop_ref"] = "qe_other/Loop1"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(CacheError, match="identity"):
        cache_manager.load_artifact("qe_test/Loop1", "pred.pkl")


def test_expired_entry_is_not_cached(cache_manager):
    cache_manager.save_artifact(
        "qe_test/Loop1", "pred.pkl", b"data", metadata=PROVENANCE
    )
    manifest_path = cache_manager._manifest_path("qe_test/Loop1", "pred.pkl")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["expires_at"] = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    assert not cache_manager.is_cached("qe_test/Loop1", "pred.pkl")
    with pytest.raises(CacheError, match="expired"):
        cache_manager.load_artifact("qe_test/Loop1", "pred.pkl")


def test_checksum_verification_cannot_be_disabled(cache_manager):
    cache_manager.save_artifact(
        "qe_test/Loop1", "pred.pkl", b"data", metadata=PROVENANCE
    )
    with pytest.raises(CacheError, match="cannot be disabled"):
        cache_manager.load_artifact(
            "qe_test/Loop1", "pred.pkl", verify_checksum=False
        )


def test_rejects_oversized_artifact(tmp_path):
    manager = ArtifactCacheManager(
        str(tmp_path / "cache"),
        max_artifact_bytes=3,
        max_cache_bytes=10,
        allow_test_fixtures=True,
    )
    with pytest.raises(CacheError, match="artifact size"):
        manager.save_artifact(
            "qe_test/Loop1", "pred.pkl", b"four", metadata=PROVENANCE
        )


def test_total_cache_overflow_evicts_oldest_entry(tmp_path):
    manager = ArtifactCacheManager(
        str(tmp_path / "cache"),
        max_artifact_bytes=2048,
        max_cache_bytes=2048,
        allow_test_fixtures=True,
    )
    manager.save_artifact(
        "qe_test/Loop1", "pred.pkl", b"a" * 1024, metadata=PROVENANCE
    )
    manager.save_artifact(
        "qe_test/Loop2", "pred.pkl", b"b" * 1024, metadata=PROVENANCE
    )

    assert not manager.is_cached("qe_test/Loop1", "pred.pkl")
    assert manager.is_cached("qe_test/Loop2", "pred.pkl")


def test_partial_atomic_publish_is_never_loadable(cache_manager, monkeypatch):
    original = cache_manager._atomic_write

    def fail_manifest(path, payload):
        if path.name.endswith(".manifest.json"):
            raise OSError("simulated manifest failure")
        original(path, payload)

    monkeypatch.setattr(cache_manager, "_atomic_write", fail_manifest)
    with pytest.raises(CacheError, match="failed to save"):
        cache_manager.save_artifact(
            "qe_test/Loop1", "pred.pkl", b"data", metadata=PROVENANCE
        )
    assert not cache_manager.is_cached("qe_test/Loop1", "pred.pkl")


def test_concurrent_writers_leave_a_valid_pair(cache_manager):
    payloads = [f"payload-{index}".encode() for index in range(20)]
    with ThreadPoolExecutor(max_workers=8) as executor:
        list(
            executor.map(
                lambda payload: cache_manager.save_artifact(
                    "qe_test/Loop1", "pred.pkl", payload, metadata=PROVENANCE
                ),
                payloads,
            )
        )

    assert cache_manager.load_artifact("qe_test/Loop1", "pred.pkl") in payloads


def test_process_lock_times_out_for_competing_manager(tmp_path):
    cache_dir = tmp_path / "cache"
    first = ArtifactCacheManager(str(cache_dir), allow_test_fixtures=True)
    second = ArtifactCacheManager(
        str(cache_dir), lock_timeout_seconds=0.05, allow_test_fixtures=True
    )
    entry_dir = first._entry_dir("qe_test/Loop1")

    with first._process_lock(entry_dir):
        with pytest.raises(CacheError, match="timed out"):
            with second._process_lock(entry_dir):
                pass


def test_clear_specific_loop_preserves_other_loop(cache_manager):
    cache_manager.save_artifact(
        "qe_test/Loop1", "pred.pkl", b"one", metadata=PROVENANCE
    )
    cache_manager.save_artifact(
        "qe_test/Loop2", "pred.pkl", b"two", metadata=PROVENANCE
    )

    cache_manager.clear_cache("qe_test/Loop1")

    assert not cache_manager.is_cached("qe_test/Loop1", "pred.pkl")
    assert cache_manager.is_cached("qe_test/Loop2", "pred.pkl")


def test_clear_all_preserves_cache_root(cache_manager):
    cache_manager.save_artifact(
        "qe_test/Loop1", "pred.pkl", b"data", metadata=PROVENANCE
    )
    cache_manager.clear_cache()

    assert cache_manager.cache_dir.is_dir()
    assert not any(cache_manager.cache_dir.iterdir())


def test_clear_refuses_reparse_child(cache_manager, tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    sentinel = outside / "sentinel.txt"
    sentinel.write_text("keep", encoding="utf-8")
    link = cache_manager.cache_dir / "unsafe-link"
    try:
        os.symlink(outside, link, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"directory symlink unavailable: {exc}")

    with pytest.raises(CacheError, match="reparse"):
        cache_manager.clear_cache()
    assert sentinel.read_text(encoding="utf-8") == "keep"


def test_cache_info_is_manifest_backed(cache_manager):
    cache_manager.save_artifact(
        "qe_test/Loop1", "pred.pkl", b"x" * 10, metadata=PROVENANCE
    )
    cache_manager.save_artifact(
        "qe_test/Loop1", "label.pkl", b"y" * 20, metadata=PROVENANCE
    )

    info = cache_manager.get_cache_info("qe_test/Loop1")
    all_info = cache_manager.get_cache_info()

    assert info["cached"] is True
    assert info["total_size"] == 30
    assert info["artifacts"]["pred.pkl"]["valid"] is True
    assert all_info["qe_test/Loop1"]["cached"] is True


def test_load_pickle_after_integrity_validation(cache_manager):
    expected = {"key": "value", "items": [1, 2, 3]}
    cache_manager.save_artifact(
        "qe_test/Loop1",
        "pred.pkl",
        pickle.dumps(expected),
        metadata=PROVENANCE,
    )
    assert cache_manager.load_pickle("qe_test/Loop1", "pred.pkl") == expected


def test_remote_row_count_mismatch_invalidates_entry(tmp_path):
    manager = ArtifactCacheManager(str(tmp_path / "cache"))
    payload = pickle.dumps([1, 2])
    manager.save_artifact(
        "qe_test/Loop1",
        "pred.pkl",
        payload,
        metadata=qe_provenance(payload, row_count=3),
    )

    with pytest.raises(CacheError, match="row_count mismatch"):
        manager.load_pickle("qe_test/Loop1", "pred.pkl")
    assert not manager.is_cached("qe_test/Loop1", "pred.pkl")
