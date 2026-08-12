from __future__ import annotations

import os

import pytest

from backend.services.dataset_release import copy_on_write as cow_module
from backend.services.dataset_release.copy_on_write import (
    CopyOnWriteError,
    SourceMerkleChanged,
    UnsafeMutationSet,
    adopt_deferred_writer_outputs,
    adopt_isolated_writer_patch,
    assert_writer_targets_private,
    atomic_write_mutation,
    clone_sealed_tree_for_reuse,
    prepare_copy_on_write_tree,
    tree_merkle,
    verify_source_unchanged,
    writer_target_manifest,
)


def _source_tree(tmp_path):
    root = tmp_path / "source"
    (root / "chunks").mkdir(parents=True)
    (root / "aggregate.h5").write_bytes(b"source-aggregate")
    (root / "chunks" / "sealed.parquet").write_bytes(b"sealed-partition")
    return root


def _source_merkle(source) -> str:
    _files, merkle = tree_merkle(source)
    return merkle


def test_explicit_mutation_is_private_and_hardlink_source_merkle_stays_stable(tmp_path) -> None:
    source = _source_tree(tmp_path)
    target = tmp_path / "target"
    plan = prepare_copy_on_write_tree(
        source,
        target,
        mutation_paths=["aggregate.h5"],
        source_sealed=True,
        expected_source_merkle=_source_merkle(source),
    )

    assert not os.path.samefile(source / "aggregate.h5", target / "aggregate.h5")
    assert os.path.samefile(
        source / "chunks" / "sealed.parquet",
        target / "chunks" / "sealed.parquet",
    )
    assert_writer_targets_private(plan, ["aggregate.h5"])
    receipt = atomic_write_mutation(plan, "aggregate.h5", b"new-aggregate")

    assert (source / "aggregate.h5").read_bytes() == b"source-aggregate"
    assert (target / "aggregate.h5").read_bytes() == b"new-aggregate"
    assert receipt["source_merkle_verification"] == ("deferred_to_writer_target_manifest")
    assert verify_source_unchanged(plan) == plan.source_merkle_before


def test_pure_reuse_clone_has_no_writer_and_preserves_every_hardlink_and_merkle(
    tmp_path,
) -> None:
    source = _source_tree(tmp_path)
    before = _source_merkle(source)

    receipt = clone_sealed_tree_for_reuse(
        source,
        tmp_path / "reused",
        source_sealed=True,
        expected_source_merkle=before,
    )

    assert receipt["writer_targets"] == []
    assert receipt["source_merkle_before"] == before
    assert receipt["source_merkle_after"] == before
    assert receipt["target_merkle"] == before
    assert all(
        os.path.samefile(source / item["relative_path"], tmp_path / "reused" / item["relative_path"])
        for item in receipt["files"]
    )


def test_writer_cannot_target_a_reused_hardlink(tmp_path) -> None:
    source = _source_tree(tmp_path)
    plan = prepare_copy_on_write_tree(
        source,
        tmp_path / "target",
        mutation_paths=["aggregate.h5"],
        source_sealed=True,
        expected_source_merkle=_source_merkle(source),
    )

    with pytest.raises(UnsafeMutationSet, match="absent from mutation set"):
        assert_writer_targets_private(plan, ["chunks/sealed.parquet"])
    with pytest.raises(UnsafeMutationSet, match="absent from mutation set"):
        atomic_write_mutation(plan, "chunks/sealed.parquet", b"unsafe")
    assert (source / "chunks" / "sealed.parquet").read_bytes() == b"sealed-partition"


def test_create_new_target_is_allowlisted_create_if_absent(tmp_path) -> None:
    source = _source_tree(tmp_path)
    plan = prepare_copy_on_write_tree(
        source,
        tmp_path / "target",
        replace_existing_targets=["aggregate.h5"],
        create_new_targets=["features/301999.sz/close.day.bin"],
        source_sealed=True,
        expected_source_merkle=_source_merkle(source),
    )

    receipt = atomic_write_mutation(plan, "features/301999.sz/close.day.bin", b"new-ipo")
    with pytest.raises(UnsafeMutationSet, match="already exists"):
        atomic_write_mutation(plan, "features/301999.sz/close.day.bin", b"overwrite")

    assert receipt["write_mode"] == "create_new_create_if_absent"
    assert (plan.target_root / "features/301999.sz/close.day.bin").read_bytes() == b"new-ipo"
    assert not (source / "features/301999.sz/close.day.bin").exists()
    atomic_write_mutation(plan, "aggregate.h5", b"updated")
    manifest = writer_target_manifest(plan)
    assert manifest["source_merkle_after"] == plan.source_merkle_before


def test_copy_on_write_rejects_unsealed_missing_and_unsafe_mutation_sets(tmp_path) -> None:
    source = _source_tree(tmp_path)
    with pytest.raises(CopyOnWriteError, match="source_sealed"):
        prepare_copy_on_write_tree(
            source,
            tmp_path / "unsealed",
            mutation_paths=["aggregate.h5"],
            source_sealed=False,
            expected_source_merkle=_source_merkle(source),
        )
    with pytest.raises(UnsafeMutationSet, match="absent"):
        prepare_copy_on_write_tree(
            source,
            tmp_path / "missing",
            mutation_paths=["missing.h5"],
            source_sealed=True,
            expected_source_merkle=_source_merkle(source),
        )
    with pytest.raises(UnsafeMutationSet, match="unsafe"):
        prepare_copy_on_write_tree(
            source,
            tmp_path / "traversal",
            mutation_paths=["../aggregate.h5"],
            source_sealed=True,
            expected_source_merkle=_source_merkle(source),
        )


def test_copy_on_write_rejects_symlinked_source_nodes(tmp_path) -> None:
    source = _source_tree(tmp_path)
    try:
        (source / "escape").symlink_to(source / "aggregate.h5")
    except OSError:
        pytest.skip("symlink creation is unavailable on this host")

    with pytest.raises(CopyOnWriteError, match="symlink or reparse"):
        prepare_copy_on_write_tree(
            source,
            tmp_path / "target",
            mutation_paths=["aggregate.h5"],
            source_sealed=True,
            expected_source_merkle=_source_merkle(source),
        )


def test_stale_source_merkle_fails_before_target_creation_or_source_change(
    tmp_path,
) -> None:
    source = _source_tree(tmp_path)
    source_merkle_before = _source_merkle(source)
    source_bytes_before = {
        path.relative_to(source).as_posix(): path.read_bytes() for path in source.rglob("*") if path.is_file()
    }
    target = tmp_path / "target"

    with pytest.raises(SourceMerkleChanged, match="frozen baseline"):
        prepare_copy_on_write_tree(
            source,
            target,
            mutation_paths=["aggregate.h5"],
            source_sealed=True,
            expected_source_merkle="0" * 64,
        )

    assert not target.exists()
    assert _source_merkle(source) == source_merkle_before
    assert {
        path.relative_to(source).as_posix(): path.read_bytes() for path in source.rglob("*") if path.is_file()
    } == source_bytes_before


@pytest.mark.parametrize("attack", ["extra", "delete", "replace_hardlink"])
def test_writer_manifest_rejects_any_undeclared_tree_change(tmp_path, attack) -> None:
    source = _source_tree(tmp_path)
    plan = prepare_copy_on_write_tree(
        source,
        tmp_path / "target",
        replace_existing_targets=["aggregate.h5"],
        source_sealed=True,
        expected_source_merkle=_source_merkle(source),
    )
    atomic_write_mutation(plan, "aggregate.h5", b"new-aggregate")
    if attack == "extra":
        (plan.target_root / "extra.bin").write_bytes(b"undeclared")
        expected = "path set differs"
    elif attack == "delete":
        (plan.target_root / "chunks" / "sealed.parquet").unlink()
        expected = "path set differs"
    else:
        reused = plan.target_root / "chunks" / "sealed.parquet"
        payload = reused.read_bytes()
        reused.unlink()
        reused.write_bytes(payload)
        expected = "no longer the baseline hardlink"

    with pytest.raises(UnsafeMutationSet, match=expected):
        writer_target_manifest(plan)
    assert _source_merkle(source) == plan.source_merkle_before


def test_reuse_clone_does_not_hash_the_target_hardlinks(tmp_path, monkeypatch) -> None:
    source = _source_tree(tmp_path)
    before = _source_merkle(source)
    calls: list[str] = []
    real = cow_module.sha256_file

    def counted(path, *, block_size=1024 * 1024):
        calls.append(str(path))
        return real(path, block_size=block_size)

    monkeypatch.setattr(cow_module, "sha256_file", counted)
    target = tmp_path / "reused"
    clone_sealed_tree_for_reuse(
        source,
        target,
        source_sealed=True,
        expected_source_merkle=before,
    )

    assert len(calls) == 4  # two source files, pre-clone and post-clone
    assert all(str(target) not in value for value in calls)


def test_cow_final_audit_reuses_source_hash_for_unchanged_hardlinks(tmp_path, monkeypatch) -> None:
    source = _source_tree(tmp_path)
    before = _source_merkle(source)
    calls: list[str] = []
    real = cow_module.sha256_file

    def counted(path, *, block_size=1024 * 1024):
        calls.append(str(path))
        return real(path, block_size=block_size)

    monkeypatch.setattr(cow_module, "sha256_file", counted)
    target = tmp_path / "target"
    plan = prepare_copy_on_write_tree(
        source,
        target,
        replace_existing_targets=["aggregate.h5"],
        source_sealed=True,
        expected_source_merkle=before,
    )
    atomic_write_mutation(plan, "aggregate.h5", b"changed")
    receipt = writer_target_manifest(plan)

    source_calls = [value for value in calls if str(source) in value]
    target_calls = [value for value in calls if str(target) in value]
    assert len(source_calls) == 4  # two source files in each of two full audits
    assert target_calls == [str(target / "aggregate.h5")]
    assert receipt["path_set_rule"] == "baseline_union_declared_create_no_delete_v1"


def test_untrusted_writer_only_sees_isolated_patch_and_cannot_change_baseline(
    tmp_path,
) -> None:
    source = _source_tree(tmp_path)
    before = _source_merkle(source)
    plan = prepare_copy_on_write_tree(
        source,
        tmp_path / "candidate",
        replace_existing_targets=["aggregate.h5"],
        create_new_targets=["features/301999.sz/close.day.bin"],
        source_sealed=True,
        expected_source_merkle=before,
    )
    patch = tmp_path / "untrusted-writer-output"
    (patch / "outputs").mkdir(parents=True)
    (patch / "outputs" / "aggregate.h5").write_bytes(b"patched")
    (patch / "outputs" / "new.bin").write_bytes(b"new-ipo")
    # Arbitrary malicious scratch output is outside the trusted adoption map.
    (patch / "extra.bin").write_bytes(b"ignored-malicious-scratch")

    adopted = adopt_isolated_writer_patch(
        plan,
        patch,
        patch_targets={
            "aggregate.h5": "outputs/aggregate.h5",
            "features/301999.sz/close.day.bin": "outputs/new.bin",
        },
    )
    final = writer_target_manifest(plan)

    assert adopted["untrusted_writer_baseline_visibility"] == 0
    assert _source_merkle(source) == before
    assert (source / "aggregate.h5").read_bytes() == b"source-aggregate"
    assert (plan.target_root / "aggregate.h5").read_bytes() == b"patched"
    assert not (plan.target_root / "extra.bin").exists()
    assert final["target_path_count"] == 3


def test_isolated_patch_adoption_rejects_incomplete_or_aliasing_roots(tmp_path) -> None:
    source = _source_tree(tmp_path)
    plan = prepare_copy_on_write_tree(
        source,
        tmp_path / "candidate",
        replace_existing_targets=["aggregate.h5"],
        source_sealed=True,
        expected_source_merkle=_source_merkle(source),
    )
    patch = tmp_path / "patch"
    patch.mkdir()
    (patch / "aggregate.h5").write_bytes(b"patched")

    with pytest.raises(UnsafeMutationSet, match="target set differs"):
        adopt_isolated_writer_patch(plan, patch, patch_targets={})
    with pytest.raises(UnsafeMutationSet, match="overlaps"):
        adopt_isolated_writer_patch(
            plan,
            source,
            patch_targets={"aggregate.h5": "aggregate.h5"},
        )


def test_deferred_replacement_copies_baseline_once_then_atomically_adopts(
    tmp_path,
) -> None:
    source = _source_tree(tmp_path)
    before = _source_merkle(source)
    plan = prepare_copy_on_write_tree(
        source,
        tmp_path / "candidate",
        replace_existing_targets=["aggregate.h5"],
        defer_replace_targets=["aggregate.h5"],
        source_sealed=True,
        expected_source_merkle=before,
    )
    assert not (plan.target_root / "aggregate.h5").exists()
    assert (plan.target_root / "chunks" / "sealed.parquet").is_file()

    private = tmp_path / "private-writer"
    private.mkdir()
    private_output = private / "aggregate.h5"
    private_output.write_bytes(b"source-aggregate-tail")
    receipt = adopt_deferred_writer_outputs(
        plan,
        private,
        patch_targets={"aggregate.h5": "aggregate.h5"},
    )
    final = writer_target_manifest(plan)

    assert not private_output.exists()
    assert (plan.target_root / "aggregate.h5").read_bytes() == b"source-aggregate-tail"
    assert receipt["baseline_copy_count"] == 1
    assert receipt["final_recopy_count"] == 0
    assert receipt["adopted"][0]["adoption"] == "same_volume_atomic_rename"
    assert _source_merkle(source) == before
    assert final["target_path_count"] == 2
