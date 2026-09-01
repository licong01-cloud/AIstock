from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from backend.services.dataset_release.cas_store import (
    CASCorruptionError,
    CASHashOnlyMismatch,
    CASStore,
    CASStoreNotInitialized,
)
from backend.services.dataset_release.control_store import ControlStore


def test_cas_requires_explicit_control_store_initialization(tmp_path) -> None:
    root = tmp_path / "control"

    with pytest.raises(CASStoreNotInitialized):
        CASStore(root)

    assert not root.exists()


def test_cas_put_is_content_addressed_idempotent_and_readback_verified(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)

    first = cas.put_json({"z": 1, "a": [2, 3]})
    second = cas.put_json({"a": [2, 3], "z": 1})

    assert first == second
    assert first.relative_path == f"cas/sha256/{first.sha256[:2]}/{first.sha256}"
    assert cas.get_json(first) == {"a": [2, 3], "z": 1}
    assert cas.verify(first) == first


def test_cas_verify_streams_without_read_bytes(tmp_path, monkeypatch) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    reference = cas.put_bytes(b"bounded-stream-verification" * 1000)

    def forbidden_read_bytes(_path):
        raise AssertionError("verify must not allocate the whole CAS blob")

    monkeypatch.setattr(Path, "read_bytes", forbidden_read_bytes)
    verified = cas.verify(reference)
    assert verified.sha256 == reference.sha256
    assert verified.size == reference.size


def test_cas_put_stream_is_bounded_idempotent_and_cleans_failed_temp(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    streamed = cas.put_stream((chunk for chunk in (b"abc", b"def")), max_chunk_bytes=3)
    direct = cas.put_bytes(b"abcdef")
    assert streamed == direct

    def broken_stream():
        yield b"ok"
        raise RuntimeError("injected stream failure")

    with pytest.raises(RuntimeError, match="injected"):
        cas.put_stream(broken_stream(), max_chunk_bytes=3)
    assert list(cas.cas_root.glob(".stream.*.partial")) == []


def test_cas_put_stream_reports_new_versus_reused_bytes(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    first = cas.put_stream_observed((b"source-partition",))
    replay = cas.put_stream_observed((b"source-partition",))
    assert first.reference == replay.reference
    assert first.created is True
    assert replay.created is False

    with pytest.raises(Exception, match="memory bound"):
        cas.put_stream((b"toolarge",), max_chunk_bytes=3)
    assert list(cas.cas_root.glob(".stream.*.partial")) == []


def test_hash_only_stream_reuses_sealed_ref_without_temp_write_or_blob_read(
    tmp_path,
    monkeypatch,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    payload = b"row-1\nrow-2\nrow-3\n"
    reference = cas.put_stream((payload[:7], payload[7:14], payload[14:]))
    destination = store.root / reference.relative_path
    before_files = {path.relative_to(cas.cas_root).as_posix() for path in cas.cas_root.rglob("*") if path.is_file()}
    existing_blob_reads = 0
    original_open = Path.open

    def guarded_open(path, *args, **kwargs):
        nonlocal existing_blob_reads
        if Path(path) == destination:
            existing_blob_reads += 1
        return original_open(path, *args, **kwargs)

    def forbidden_temp(*_args, **_kwargs):
        raise AssertionError("hash-only verification must not create a temp file")

    monkeypatch.setattr(Path, "open", guarded_open)
    monkeypatch.setattr(
        "backend.services.dataset_release.cas_store.tempfile.mkstemp",
        forbidden_temp,
    )
    consumed: list[bytes] = []

    def chunks():
        for chunk in (payload[:7], payload[7:14], payload[14:]):
            consumed.append(chunk)
            yield chunk

    observed = cas.verify_stream_hash_only(
        chunks(),
        expected_digest=reference.sha256,
        expected_size=reference.size,
        expected_relative_path=reference.relative_path,
        expected_codec_identity="fixture-codec-v1",
        observed_codec_identity="fixture-codec-v1",
        max_chunk_bytes=7,
    )

    after_files = {path.relative_to(cas.cas_root).as_posix() for path in cas.cas_root.rglob("*") if path.is_file()}
    assert observed == reference
    assert b"".join(consumed) == payload
    assert existing_blob_reads == 0
    assert after_files == before_files
    assert list(cas.cas_root.rglob("*.partial")) == []


@pytest.mark.parametrize(
    "fresh",
    (
        b"row-1\nchanged\nrow-3\n",
        b"row-1\nrow-3\n",
        b"row-2\nrow-1\nrow-3\n",
    ),
    ids=("changed-row", "deleted-row", "reordered-row"),
)
def test_hash_only_stream_blocks_any_byte_revision_without_partial(
    tmp_path,
    fresh: bytes,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    original = cas.put_stream((b"row-1\nrow-2\nrow-3\n",))

    with pytest.raises(CASHashOnlyMismatch, match="differs"):
        cas.verify_stream_hash_only(
            (fresh,),
            expected_digest=original.sha256,
            expected_size=original.size,
            expected_relative_path=original.relative_path,
            expected_codec_identity="fixture-codec-v1",
            observed_codec_identity="fixture-codec-v1",
        )

    assert list(cas.cas_root.rglob("*.partial")) == []


def test_hash_only_stream_blocks_codec_drift_before_reuse(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    original = cas.put_stream((b"sealed",))

    with pytest.raises(CASHashOnlyMismatch, match="codec identity"):
        cas.verify_stream_hash_only(
            (b"sealed",),
            expected_digest=original.sha256,
            expected_size=original.size,
            expected_relative_path=original.relative_path,
            expected_codec_identity="gzip-v1",
            observed_codec_identity="gzip-v2",
        )

    assert list(cas.cas_root.rglob("*.partial")) == []


def test_hash_only_stream_producer_failure_leaves_no_partial_or_new_blob(
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    original = cas.put_stream((b"complete-stream",))
    before = {path.relative_to(cas.cas_root).as_posix() for path in cas.cas_root.rglob("*") if path.is_file()}

    def broken():
        yield b"complete-"
        raise RuntimeError("injected producer failure")

    with pytest.raises(RuntimeError, match="producer failure"):
        cas.verify_stream_hash_only(
            broken(),
            expected_digest=original.sha256,
            expected_size=original.size,
            expected_relative_path=original.relative_path,
            expected_codec_identity="fixture-codec-v1",
            observed_codec_identity="fixture-codec-v1",
        )

    after = {path.relative_to(cas.cas_root).as_posix() for path in cas.cas_root.rglob("*") if path.is_file()}
    assert after == before
    assert list(cas.cas_root.rglob("*.partial")) == []


def test_concurrent_same_blob_publish_converges_to_one_verified_ref(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    payload = b"same immutable payload" * 2048

    with ThreadPoolExecutor(max_workers=8) as pool:
        refs = list(pool.map(lambda _: cas.put_bytes(payload), range(24)))

    assert len({ref.sha256 for ref in refs}) == 1
    assert len({ref.relative_path for ref in refs}) == 1
    assert cas.get_bytes(refs[0]) == payload
    destination = store.root / refs[0].relative_path
    assert destination.is_file()
    assert list(destination.parent.glob(refs[0].sha256)) == [destination]


def test_existing_tampered_blob_fails_closed_instead_of_replacing(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    ref = cas.put_bytes(b"authoritative")
    destination = store.root / ref.relative_path
    destination.write_bytes(b"tampered")

    with pytest.raises(CASCorruptionError, match="corruption"):
        cas.put_bytes(b"authoritative")
    with pytest.raises(CASCorruptionError, match="mismatch"):
        cas.get_bytes(ref)


def test_cas_rejects_reparse_or_symlink_shard(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    payload = b"link guard"
    digest = __import__("hashlib").sha256(payload).hexdigest()
    outside = tmp_path / "outside"
    outside.mkdir()
    shard = cas.cas_root / digest[:2]
    try:
        shard.symlink_to(outside, target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("symlink creation is unavailable on this host")

    with pytest.raises(Exception, match="symlink|reparse"):
        cas.put_bytes(payload)


def test_cas_json_bounded_read_refuses_large_artifact(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    reference = cas.put_json({"payload": "x" * 128})

    assert cas.get_json_bounded(reference, max_bytes=1024)["payload"] == "x" * 128
    with pytest.raises(CASCorruptionError, match="bounded read limit"):
        cas.get_json_bounded(reference, max_bytes=32)
