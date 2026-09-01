from __future__ import annotations

import os
from datetime import date

import pytest

from backend.services.dataset_release.contracts import Component, ComponentAction
from backend.services.dataset_release.copy_on_write import (
    SourceMerkleChanged,
    atomic_write_mutation,
    tree_merkle,
    writer_target_manifest,
)
from backend.services.dataset_release.decision import (
    ActionPlan,
    ComponentPlan,
    FrozenReuseEvidence,
)
from backend.services.dataset_release.errors import DecisionError
from backend.services.dataset_release.incremental import (
    MutationScope,
    compile_incremental_plan,
    prepare_incremental_component_tree,
    qfq_denominator_mutation_scope,
    verify_incremental_source_unchanged,
)


def _digest(char: str) -> str:
    return char * 64


def _component_plan(
    component: Component,
    action: ComponentAction,
    *,
    partition_key: str = "2026-07",
    frozen_reuse: FrozenReuseEvidence | None = None,
) -> ComponentPlan:
    return ComponentPlan(
        component=component,
        partition_key=partition_key,
        action=action,
        reason=f"test {action.value.lower()}",
        changed_fingerprints=(),
        invalidation_edges=(),
        estimated_work={},
        frozen_reuse=frozen_reuse,
    )


def _action_plan(
    daily: ComponentPlan,
) -> ActionPlan:
    return ActionPlan(
        actions=(
            daily,
            _component_plan(Component.MINUTE_BIN, ComponentAction.NOOP),
            _component_plan(Component.FACTOR_H5_STATIC, ComponentAction.NOOP),
            _component_plan(Component.DOMESTIC_INDEX_CONTEXT, ComponentAction.NOOP),
        )
    )


def _frozen_reuse(
    manifest_root: str,
    *,
    mutation_set: tuple[str, ...] = ("features/2026-07.bin",),
) -> FrozenReuseEvidence:
    return FrozenReuseEvidence(
        source_release_id="20260630-qe_hmm_full_v1-full-source-candidate",
        source_release_digest=_digest("1"),
        source_attestation_key=_digest("2"),
        artifact_id="daily-bin-2026-07",
        component_partition_key="2026-07",
        manifest_root=manifest_root,
        file_identity=_digest("3"),
        reuse_mode="sealed_component_tree",
        mutation_set=mutation_set,
        compatibility_reason="existing partitions are sealed and compatible",
    )


def _scope(
    *,
    writer_targets: tuple[str, ...] = ("features/2026-07.bin",),
    create_new_targets: tuple[str, ...] = (),
) -> MutationScope:
    return MutationScope(
        component=Component.DAILY_BIN,
        partition_key="2026-07",
        instruments=(),
        start=date(2026, 7, 1),
        end=date(2026, 7, 31),
        chunk_ids=("2026-07",),
        writer_targets=writer_targets,
        invalidation_edges=("daily_source->daily_bin",),
        reason="append the new monthly partition",
        create_new_targets=create_new_targets,
    )


def test_incremental_tree_keeps_baseline_bytes_and_only_copies_writer_targets(
    tmp_path,
) -> None:
    source = tmp_path / "sealed-source"
    (source / "features").mkdir(parents=True)
    (source / "features" / "2026-06.bin").write_bytes(b"sealed-june")
    (source / "features" / "2026-07.bin").write_bytes(b"empty-july-placeholder")
    _files, manifest_root = tree_merkle(source)

    daily = _component_plan(
        Component.DAILY_BIN,
        ComponentAction.INCREMENTAL,
        frozen_reuse=_frozen_reuse(manifest_root),
    )
    execution = compile_incremental_plan(
        _action_plan(daily),
        {(Component.DAILY_BIN, "2026-07"): (_scope(),)},
    )
    daily_execution = execution.component(Component.DAILY_BIN, "2026-07")
    target = tmp_path / "candidate"
    copy_plan = prepare_incremental_component_tree(
        daily_execution,
        source_root=source,
        target_root=target,
        source_file_identity=_digest("3"),
    )

    assert not os.path.samefile(
        source / "features" / "2026-07.bin",
        target / "features" / "2026-07.bin",
    )
    assert os.path.samefile(
        source / "features" / "2026-06.bin",
        target / "features" / "2026-06.bin",
    )
    receipt = atomic_write_mutation(
        copy_plan,
        "features/2026-07.bin",
        b"new-july-partition",
    )

    assert (source / "features" / "2026-07.bin").read_bytes() == (b"empty-july-placeholder")
    assert (target / "features" / "2026-07.bin").read_bytes() == b"new-july-partition"
    assert receipt["source_merkle_verification"] == ("deferred_to_writer_target_manifest")
    assert verify_incremental_source_unchanged(copy_plan) == manifest_root


def test_new_ipo_feature_is_explicit_create_new_without_forcing_full_rebuild(
    tmp_path,
) -> None:
    source = tmp_path / "sealed-source"
    (source / "features" / "000001.sz").mkdir(parents=True)
    existing = source / "features" / "000001.sz" / "close.day.bin"
    existing.write_bytes(b"old-existing")
    _files, manifest_root = tree_merkle(source)
    targets = (
        "features/000001.sz/close.day.bin",
        "features/301999.sz/close.day.bin",
    )
    daily = _component_plan(
        Component.DAILY_BIN,
        ComponentAction.INCREMENTAL,
        frozen_reuse=_frozen_reuse(manifest_root, mutation_set=targets),
    )
    scope = _scope(
        writer_targets=targets,
        create_new_targets=("features/301999.sz/close.day.bin",),
    )
    execution = compile_incremental_plan(
        _action_plan(daily),
        {(Component.DAILY_BIN, "2026-07"): (scope,)},
    )
    component = execution.component(Component.DAILY_BIN, "2026-07")
    assert component.decision.action is ComponentAction.INCREMENTAL
    copy_plan = prepare_incremental_component_tree(
        component,
        source_root=source,
        target_root=tmp_path / "candidate",
        source_file_identity=_digest("3"),
    )

    atomic_write_mutation(copy_plan, targets[0], b"updated-existing")
    created = atomic_write_mutation(copy_plan, targets[1], b"new-ipo")
    manifest = writer_target_manifest(copy_plan)

    assert existing.read_bytes() == b"old-existing"
    assert created["write_mode"] == "create_new_create_if_absent"
    assert manifest["create_new_targets"] == [targets[1]]
    assert [item["relative_path"] for item in manifest["files"]] == list(targets)
    assert not (source / targets[1]).exists()


def test_incremental_action_requires_explicit_mutation_scope() -> None:
    daily = _component_plan(
        Component.DAILY_BIN,
        ComponentAction.INCREMENTAL,
        frozen_reuse=_frozen_reuse(_digest("4")),
    )

    with pytest.raises(DecisionError, match="requires explicit mutation scopes"):
        compile_incremental_plan(_action_plan(daily), {})


def test_incremental_stale_baseline_fails_before_clone(tmp_path) -> None:
    source = tmp_path / "sealed-source"
    (source / "features").mkdir(parents=True)
    source_file = source / "features" / "2026-07.bin"
    source_file.write_bytes(b"sealed-source-bytes")
    _files, source_merkle_before = tree_merkle(source)
    target = tmp_path / "candidate"
    daily = _component_plan(
        Component.DAILY_BIN,
        ComponentAction.INCREMENTAL,
        frozen_reuse=_frozen_reuse(_digest("4")),
    )
    execution = compile_incremental_plan(
        _action_plan(daily),
        {(Component.DAILY_BIN, "2026-07"): (_scope(),)},
    )

    with pytest.raises(SourceMerkleChanged, match="frozen baseline"):
        prepare_incremental_component_tree(
            execution.component(Component.DAILY_BIN, "2026-07"),
            source_root=source,
            target_root=target,
            source_file_identity=_digest("3"),
        )

    assert not target.exists()
    assert source_file.read_bytes() == b"sealed-source-bytes"
    assert tree_merkle(source)[1] == source_merkle_before


def test_incremental_source_identity_drift_fails_before_clone(tmp_path) -> None:
    source = tmp_path / "sealed-source"
    (source / "features").mkdir(parents=True)
    (source / "features" / "2026-07.bin").write_bytes(b"sealed-source-bytes")
    _files, manifest_root = tree_merkle(source)
    target = tmp_path / "candidate"
    daily = _component_plan(
        Component.DAILY_BIN,
        ComponentAction.INCREMENTAL,
        frozen_reuse=_frozen_reuse(manifest_root),
    )
    execution = compile_incremental_plan(
        _action_plan(daily),
        {(Component.DAILY_BIN, "2026-07"): (_scope(),)},
    )

    with pytest.raises(DecisionError, match="file identity differs"):
        prepare_incremental_component_tree(
            execution.component(Component.DAILY_BIN, "2026-07"),
            source_root=source,
            target_root=target,
            source_file_identity=_digest("4"),
        )

    assert not target.exists()


def test_writer_targets_must_exactly_match_frozen_mutation_set() -> None:
    daily = _component_plan(
        Component.DAILY_BIN,
        ComponentAction.INCREMENTAL,
        frozen_reuse=_frozen_reuse(_digest("4")),
    )

    with pytest.raises(DecisionError, match="differ from immutable decision"):
        compile_incremental_plan(
            _action_plan(daily),
            {(Component.DAILY_BIN, "2026-07"): (_scope(writer_targets=("features/unplanned.bin",)),)},
        )


def test_non_mutating_action_rejects_writer_scope() -> None:
    daily = _component_plan(Component.DAILY_BIN, ComponentAction.NOOP)

    with pytest.raises(DecisionError, match="cannot contain writer mutation scopes"):
        compile_incremental_plan(
            _action_plan(daily),
            {(Component.DAILY_BIN, "2026-07"): (_scope(),)},
        )


@pytest.mark.parametrize(
    "action",
    (ComponentAction.RESUME, ComponentAction.FULL_REBUILD),
)
def test_resume_and_full_rebuild_reject_incremental_writer_scopes(action) -> None:
    daily = _component_plan(Component.DAILY_BIN, action)

    with pytest.raises(DecisionError, match="cannot contain writer mutation scopes"):
        compile_incremental_plan(
            _action_plan(daily),
            {(Component.DAILY_BIN, "2026-07"): (_scope(),)},
        )


def test_frozen_baseline_partition_must_match_decision() -> None:
    frozen = _frozen_reuse(_digest("4"))
    daily = _component_plan(
        Component.DAILY_BIN,
        ComponentAction.INCREMENTAL,
        partition_key="2026-08",
        frozen_reuse=frozen,
    )

    with pytest.raises(DecisionError, match="baseline partition differs"):
        compile_incremental_plan(
            _action_plan(daily),
            {
                (Component.DAILY_BIN, "2026-08"): (
                    MutationScope(
                        component=Component.DAILY_BIN,
                        partition_key="2026-08",
                        instruments=(),
                        start=date(2026, 8, 1),
                        end=date(2026, 8, 31),
                        chunk_ids=("2026-08",),
                        writer_targets=("features/2026-07.bin",),
                        invalidation_edges=("daily_source->daily_bin",),
                        reason="fixture",
                    ),
                )
            },
        )


def test_qfq_denominator_change_invalidates_full_instrument_history() -> None:
    scope = qfq_denominator_mutation_scope(
        component=Component.DAILY_BIN,
        partition_key="000001.SZ:full-history",
        instrument="000001.sz",
        dataset_start=date(2018, 1, 1),
        cutoff=date(2026, 7, 31),
        writer_targets=("features/000001.sz.bin",),
    )

    assert scope.instruments == ("000001.SZ",)
    assert scope.start == date(2018, 1, 1)
    assert scope.end == date(2026, 7, 31)
    assert scope.invalidation_edges == ("adj_factor.denominator->qfq_history",)
