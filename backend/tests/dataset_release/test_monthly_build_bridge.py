from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.services.dataset_release.contracts import Component, ComponentAction
from backend.services.dataset_release.cas_store import CASRef
from backend.services.dataset_release.canonical import canonical_json_bytes
import backend.services.dataset_release.monthly_build_bridge as bridge_module
from backend.services.dataset_release.monthly_build_bridge import (
    MonthlyBuildBridgeError,
    _physical_action_plan,
    _resolve_ref,
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
    profile = SimpleNamespace(profile="qe_hmm_full_v2", semantic_profile_digest="4" * 64)

    compiled = bridge_module.compile_initial_monthly_build(
        context_plan={
            "target_cutoff": "2026-09-30",
            "predecessor": {"dataset_manifest_sha256": "5" * 64},
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
