from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.services.dataset_release.contracts import Component, ComponentAction
from backend.services.dataset_release.cas_store import CASRef
from backend.services.dataset_release.canonical import canonical_json_bytes
import backend.services.dataset_release.monthly_build_bridge as bridge_module
from backend.services.dataset_release.monthly_build_bridge import (
    MonthlyBuildBridgeError,
    _predecessor_baseline,
    _physical_action_plan,
    _reconcile_action_plan,
    _resolve_ref,
)
from backend.services.dataset_release.decision import ActionPlan, ComponentPlan
from backend.services.dataset_release.monthly_incremental_baseline import (
    MONTHLY_INCREMENTAL_BASELINE_PATH,
    build_monthly_incremental_baseline,
)
from backend.services.dataset_release.monthly_unified import COMPONENTS
from backend.services.dataset_release.resolution_processor import (
    MonthlyResolutionProcessor,
    monthly_build_fingerprints,
)


def _actions(value: str = "COMPONENT_REBUILD") -> dict[str, str]:
    return {component: value for component in COMPONENTS}


def test_bridge_maps_eight_monthly_components_to_four_physical_rebuilds() -> None:
    plan = _physical_action_plan(_actions())

    assert {item.component for item in plan.actions} == set(Component)
    assert {item.action for item in plan.actions} == {ComponentAction.FULL_REBUILD}
    assert len(plan.digest) == 64


def test_bridge_rejects_incomplete_monthly_action_set() -> None:
    actions = _actions()
    actions.pop("sector_context")

    with pytest.raises(MonthlyBuildBridgeError, match="incomplete"):
        _physical_action_plan(actions)


def test_initial_bridge_promotes_unproven_physical_reuse_to_full_rebuild() -> None:
    actions = _actions()
    actions["day"] = "REUSE"

    physical = _physical_action_plan(actions, force_full_rebuild=True)

    assert {item.action for item in physical.actions} == {ComponentAction.FULL_REBUILD}
    assert next(
        item for item in physical.actions if item.component is Component.DAILY_BIN
    ).reason == "monthly_v2_initial_migration:day:REUSE"


def _exact_plan(action: ComponentAction) -> ActionPlan:
    return ActionPlan(
        tuple(
            ComponentPlan(
                component=component,
                partition_key="all",
                action=action,
                reason="exact evidence",
                changed_fingerprints=(),
                invalidation_edges=(),
                estimated_work={},
            )
            for component in Component
        )
    )


def test_reconcile_rejects_source_reuse_when_exact_authority_changed() -> None:
    with pytest.raises(MonthlyBuildBridgeError, match="under-declared"):
        _reconcile_action_plan(_actions("REUSE"), _exact_plan(ComponentAction.INCREMENTAL))


def test_reconcile_allows_contract_incompatibility_to_force_component_rebuild() -> None:
    reconciled = _reconcile_action_plan(
        _actions("REUSE"),
        _exact_plan(ComponentAction.FULL_REBUILD),
    )

    assert {item.action for item in reconciled.actions} == {ComponentAction.FULL_REBUILD}


def test_reconcile_honors_explicit_component_rebuild() -> None:
    reconciled = _reconcile_action_plan(
        _actions("COMPONENT_REBUILD"),
        _exact_plan(ComponentAction.SELECTIVE_REBUILD),
    )

    assert {item.action for item in reconciled.actions} == {ComponentAction.FULL_REBUILD}


def test_predecessor_baseline_requires_manifest_pinned_canonical_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release_root = tmp_path / "releases"
    candidate = release_root / "v1"
    baseline_path = candidate / MONTHLY_INCREMENTAL_BASELINE_PATH
    baseline_path.parent.mkdir(parents=True)
    reference = {
        "sha256": "1" * 64,
        "size": 11,
        "relative_path": f"cas/sha256/11/{'1' * 64}",
    }
    authority = build_monthly_incremental_baseline(
        release_id="release-v1",
        release_digest="2" * 64,
        profile="qe_hmm_full_v2",
        scope="full",
        cutoff="2026-08-31",
        candidate_root_name="v1",
        component_artifact_manifest_ref=reference,
        validation_ref={**reference, "sha256": "6" * 64, "relative_path": f"cas/sha256/66/{'6' * 64}"},
        source_stage_receipt_ref={**reference, "sha256": "7" * 64, "relative_path": f"cas/sha256/77/{'7' * 64}"},
        source_bundle_sha256="8" * 64,
    )
    baseline_path.write_bytes(canonical_json_bytes(authority) + b"\n")
    manifest = {
        "schema_version": "qe_dataset_manifest_v1",
        "release_id": "release-v1",
        "cutoff_trade_date": "2026-08-31",
        "components": {
            "baseline": {
                "path": MONTHLY_INCREMENTAL_BASELINE_PATH.as_posix(),
                "sha256": hashlib.sha256(baseline_path.read_bytes()).hexdigest(),
                "size": baseline_path.stat().st_size,
            }
        },
    }
    manifest["dataset_manifest_sha256"] = hashlib.sha256(
        canonical_json_bytes(manifest)
    ).hexdigest()
    manifest_path = candidate / "qe_dataset_manifest.json"
    manifest_path.write_bytes(canonical_json_bytes(manifest) + b"\n")
    component_manifest = SimpleNamespace(
        profile="qe_hmm_full_v2",
        scope="full",
        cutoff=date(2026, 8, 31),
        candidate_identity="3" * 64,
        artifact_root="4" * 64,
        manifest_root="5" * 64,
        producer_fingerprint="9" * 64,
        artifact_fingerprint="a" * 64,
        validation_fingerprint="b" * 64,
    )
    monkeypatch.setattr(
        bridge_module,
        "load_component_artifact_manifest",
        lambda _cas, value: component_manifest if value == reference else None,
    )
    profile = SimpleNamespace(
        profile="qe_hmm_full_v2",
        candidate_root=str(release_root),
    )
    result = _predecessor_baseline(
        context_plan={
            "predecessor": {
                "candidate_root": str(candidate),
                "release_id": "release-v1",
                "cutoff": "2026-08-31",
                "dataset_manifest_sha256": manifest["dataset_manifest_sha256"],
            },
            "predecessor_manifest_ref": {
                "id": "qe_dataset_manifest.json",
                "sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
                "size": manifest_path.stat().st_size,
                "dataset_manifest_sha256": manifest["dataset_manifest_sha256"],
            },
        },
        profile=profile,
        cas=object(),
    )

    assert result is not None
    assert result.root_relative_path == "v1"
    assert result.authority["baseline_authority_sha256"] == authority[
        "baseline_authority_sha256"
    ]


def test_artifact_reference_rejects_link_in_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "nested" / "bundle.json"
    target.parent.mkdir()
    target.write_bytes(b"{}\n")
    digest = hashlib.sha256(target.read_bytes()).hexdigest()
    monkeypatch.setattr(
        bridge_module,
        "_is_link",
        lambda path: path.name == "nested",
    )

    with pytest.raises(MonthlyBuildBridgeError, match="symlink or junction"):
        _resolve_ref(
            {
                "id": "nested/bundle.json",
                "sha256": digest,
                "size": target.stat().st_size,
            },
            roots=(tmp_path,),
            label="bundle",
        )


def test_resolution_processor_uses_shared_build_fingerprints() -> None:
    profile = SimpleNamespace(
        profile="qe_hmm_full_v2",
        semantic_profile_digest="b" * 64,
        components=tuple(Component),
        qlib_toolchain=SimpleNamespace(digest="c" * 64, dump_script_sha256="d" * 64),
        moneyflow_contract={"unit": "CNY"},
        static_column_count=121,
        qlib_stock_schema_digest="e" * 64,
        index_codes=("000300.SH",),
    )
    expected = monthly_build_fingerprints(profile)
    processor = MonthlyResolutionProcessor(
        profile,
        SimpleNamespace(),
        SimpleNamespace(),
        source_authority=SimpleNamespace(),
    )
    assert set(expected) == {
        "producer_fingerprint",
        "artifact_fingerprint",
        "validation_fingerprint",
    }
    assert all(len(value) == 64 for value in expected.values())
    assert processor.producer_fingerprint == expected["producer_fingerprint"]
    assert processor.artifact_fingerprint == expected["artifact_fingerprint"]
    assert processor.validation_fingerprint == expected["validation_fingerprint"]


def test_initial_bridge_compiles_sealed_source_without_database_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    digest = "a" * 64
    source_stage_ref = CASRef(digest, 7, f"cas/sha256/aa/{digest}")
    bundle = {
        "schema_version": "aistock_monthly_frozen_source_bundle_v1",
        "cutoff": "2026-09-30",
        "source_content_root": "b" * 64,
        "pit_snapshot_digest": "c" * 64,
        "artifact_ready_content_root": "d" * 64,
        "artifact_ready_provenance_root": "e" * 64,
        "source_stage_receipt_ref": source_stage_ref.as_dict(),
    }
    bundle_path = tmp_path / "monthly" / "op" / "frozen-source-bundle.json"
    bundle_path.parent.mkdir(parents=True)
    bundle_path.write_bytes(canonical_json_bytes(bundle) + b"\n")
    bundle_sha = hashlib.sha256(bundle_path.read_bytes()).hexdigest()
    partition = SimpleNamespace(
        spec=SimpleNamespace(identity="trading_calendar:2026-09"),
        as_build_input=lambda: {"dataset": "trading_calendar", "partition_key": "2026-09"},
    )
    generic_ref = CASRef("f" * 64, 9, f"cas/sha256/ff/{'f' * 64}")
    frozen = SimpleNamespace(
        source_content_root=bundle["source_content_root"],
        pit_snapshot_digest=bundle["pit_snapshot_digest"],
        artifact_ready_content_root=bundle["artifact_ready_content_root"],
        artifact_ready_provenance_root=bundle["artifact_ready_provenance_root"],
        artifact_ready_contract_ref=generic_ref,
        source_manifest_ref=generic_ref,
        pit_snapshot_ref=generic_ref,
        provider_receipt_refs=(),
        artifact_ready_derived_source_receipt_refs=(),
        source_cas_usage={"predicted_remaining_new_bytes": 123},
        partitions=(partition,),
    )
    component_refs = {component.value: generic_ref for component in Component}
    artifact_ready = SimpleNamespace(component_manifest_refs=component_refs)

    class _CAS:
        def verify(self, reference):  # type: ignore[no-untyped-def]
            assert reference == source_stage_ref
            return reference

        def get_json_bounded(self, reference, *, max_bytes):  # type: ignore[no-untyped-def]
            assert reference == generic_ref
            assert max_bytes > 0
            return {"effective_partitions": [{"partition": "full"}]}

    monkeypatch.setattr(bridge_module, "load_source_stage_receipt", lambda *args, **kwargs: frozen)
    monkeypatch.setattr(bridge_module, "load_artifact_ready_contract", lambda *args, **kwargs: artifact_ready)
    monkeypatch.setattr(
        bridge_module,
        "monthly_build_fingerprints",
        lambda _profile: {
            "producer_fingerprint": "1" * 64,
            "artifact_fingerprint": "2" * 64,
            "validation_fingerprint": "3" * 64,
        },
    )
    release_root = tmp_path / "releases"
    predecessor_root = release_root / "predecessor"
    predecessor_root.mkdir(parents=True)
    predecessor_manifest = {
        "schema_version": "qe_dataset_manifest_v1",
        "release_id": "qe_hmm_full_v2_20260831",
        "cutoff_trade_date": "2026-08-31",
        "components": {},
    }
    predecessor_manifest["dataset_manifest_sha256"] = hashlib.sha256(
        canonical_json_bytes(predecessor_manifest)
    ).hexdigest()
    predecessor_manifest_path = predecessor_root / "qe_dataset_manifest.json"
    predecessor_manifest_path.write_bytes(canonical_json_bytes(predecessor_manifest) + b"\n")
    predecessor_file_sha = hashlib.sha256(predecessor_manifest_path.read_bytes()).hexdigest()
    profile = SimpleNamespace(
        profile="qe_hmm_full_v2",
        semantic_profile_digest="4" * 64,
        candidate_root=str(release_root),
        candidate_root_id="controller",
        start_date=__import__("datetime").date(2018, 8, 1),
    )

    compiled = bridge_module.compile_initial_monthly_build(
        context_plan={
            "target_cutoff": "2026-09-30",
            "release_id": "qe_hmm_full_v2_20260930",
            "predecessor": {
                "dataset_manifest_sha256": predecessor_manifest[
                    "dataset_manifest_sha256"
                ],
                "candidate_root": str(predecessor_root),
                "release_id": "qe_hmm_full_v2_20260831",
                "cutoff": "2026-08-31",
            },
            "predecessor_manifest_ref": {
                "id": "qe_dataset_manifest.json",
                "sha256": predecessor_file_sha,
                "size": predecessor_manifest_path.stat().st_size,
                "dataset_manifest_sha256": predecessor_manifest[
                    "dataset_manifest_sha256"
                ],
            },
        },
        source_receipt={
            "scope": {"component_actions": _actions("REUSE")},
            "input_refs": [
                {
                    "id": bundle_path.relative_to(tmp_path).as_posix(),
                    "sha256": bundle_sha,
                    "size": bundle_path.stat().st_size,
                }
            ],
        },
        profile=profile,
        cas=_CAS(),
        artifact_roots=(tmp_path,),
    )

    assert compiled.source_bundle_sha256 == bundle_sha
    assert compiled.physical_plan["database_read_performed"] is False
    assert {
        row["action"] for row in compiled.physical_plan["actions"]
    } == {"FULL_REBUILD"}
    assert compiled.physical_plan["build_inputs"]["baseline"] is None
    assert compiled.physical_plan["build_inputs"]["predicted_new_bytes"] > 123
    assert json.loads(bundle_path.read_text(encoding="utf-8")) == bundle
