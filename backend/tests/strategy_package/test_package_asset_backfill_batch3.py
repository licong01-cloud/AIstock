from __future__ import annotations

import importlib
import pickle
import sys
from pathlib import Path
from types import SimpleNamespace

from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package import package_asset_backfill as backfill_module
from backend.services.strategy_package.models import (
    AlphaCombinationPolicy,
    AlphaMode,
    FactorAsset,
    ModelAsset,
    SourceType,
    StrategyPackageComponentRecord,
)
from backend.services.strategy_package.package_asset import StrategyPackageAssetRecord, StrategyPackageAssetType
from backend.services.strategy_package.package_asset_backfill import (
    STATUS_APPLIED,
    STATUS_PLANNED_FREEZE,
    STATUS_SKIPPED_ALREADY_FROZEN,
    STATUS_UNRECOVERABLE,
    PackageAssetBackfillItem,
    PackageAssetBackfillPlan,
    PackageAssetBackfillService,
)
from backend.services.strategy_package.package_asset_freeze import (
    PackageAssetBytes,
    PackageAssetFreezeService,
    manifest_has_frozen_runtime_assets,
)
from backend.services.strategy_package.package_asset_store import LocalPackageAssetStore
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository
from backend.services.trading_core.errors import DataUnavailableError
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


def _freezer(
    tmp_path: Path,
    *,
    missing_factor: str | None = None,
    model_params: bytes | None = None,
    model_code_files: dict[str, bytes] | None = None,
) -> PackageAssetFreezeService:
    def factor_reader(factor, manifest):  # noqa: ANN001, ANN202
        if missing_factor and factor.factor_name == missing_factor:
            raise DataUnavailableError(
                "factor missing",
                context={
                    "reason_code": "strategy_package_factor_code_missing",
                    "package_id": manifest.package_id,
                    "factor_name": factor.factor_name,
                },
            )
        return PackageAssetBytes(
            f"# factor {factor.factor_name}\nVALUE = '{manifest.package_id}'\n".encode("utf-8"),
            f"unit://factor/{factor.factor_name}.py",
        )

    model_code_files = model_code_files or {}

    def workspace_file(_manifest, rel_path: str):  # noqa: ANN001, ANN202
        rel = rel_path.replace("\\", "/")
        if rel not in model_code_files:
            raise DataUnavailableError(
                "model code missing",
                context={"reason_code": "unit_model_code_missing", "rel_path": rel},
            )
        return PackageAssetBytes(model_code_files[rel], f"unit://workspace/{rel}")

    return PackageAssetFreezeService(
        asset_store=LocalPackageAssetStore(tmp_path / "package_assets"),
        source=SimpleNamespace(workspace_file_bytes=workspace_file),
        conf_yaml_reader=lambda manifest: PackageAssetBytes(b"task: {}\n", f"unit://conf/{manifest.package_id}/conf.yaml"),
        model_params_reader=lambda manifest: PackageAssetBytes(
            model_params if model_params is not None else f"model::{manifest.package_id}".encode("utf-8"),
            f"unit://model/{manifest.package_id}/params.pkl",
        ),
        factor_code_reader=factor_reader,
    )


def _pickled_model_instance_payload(tmp_path: Path) -> bytes:
    module_root = tmp_path / "pickle_model_module"
    module_root.mkdir()
    (module_root / "model.py").write_text("class LSTM_10D_hs64_d02:\n    pass\n", encoding="utf-8")
    sys.path.insert(0, str(module_root))
    try:
        sys.modules.pop("model", None)
        module = importlib.import_module("model")
        return pickle.dumps(module.LSTM_10D_hs64_d02(), protocol=4)
    finally:
        sys.modules.pop("model", None)
        sys.path.remove(str(module_root))


def _single_manifest(name: str):
    base = make_manifest()
    factor = FactorAsset(factor_id=f"factor_{name}", factor_name=f"factor_{name}")
    model = ModelAsset(model_id=f"model_{name}")
    component = base.alpha_components[0].model_copy(
        update={
            "alpha_id": f"alpha_{name}",
            "alpha_name": name,
            "factor_ids": [factor.factor_id],
            "model_id": model.model_id,
            "model_ref": model.model_id,
        }
    )
    return base.model_copy(
        update={
            "package_id": f"pkg_{name}",
            "package_name": f"pkg {name}",
            "source": base.source.model_copy(update={"source_id": f"qe_{name}", "run_id": f"qear_run_{name}"}),
            "alpha_components": [component],
            "alpha_combination_policy": AlphaCombinationPolicy(method="identity", weights={component.alpha_id: 1.0}),
            "factor_set": [factor],
            "model_asset": model,
            "manifest_sha256": None,
        }
    )


def _multi_manifest(name: str, child_a, child_b):  # noqa: ANN001
    base = make_manifest()
    comp_a = child_a.current_manifest().alpha_components[0].model_copy(update={"component_weight": 0.6})
    comp_b = child_b.current_manifest().alpha_components[0].model_copy(update={"component_weight": 0.4})
    return base.model_copy(
        update={
            "package_id": f"pkg_parent_{name}",
            "package_name": f"parent {name}",
            "source": base.source.model_copy(
                update={"source_type": SourceType.MULTI_ALPHA_COMBINE_RUN, "source_id": f"macb_{name}", "run_id": f"macb_{name}"}
            ),
            "alpha_mode": AlphaMode.MULTI_ALPHA,
            "alpha_components": [comp_a, comp_b],
            "alpha_combination_policy": AlphaCombinationPolicy(
                method="weighted_sum",
                weights={comp_a.alpha_id: 0.6, comp_b.alpha_id: 0.4},
            ),
            "factor_set": [*child_a.current_manifest().factor_set, *child_b.current_manifest().factor_set],
            "model_asset": [child_a.current_manifest().model_asset, child_b.current_manifest().model_asset],
            "source_evidence": {
                "multi_alpha": {
                    "legs": [
                        {
                            "leg_id": "leg_a",
                            "child_package_id": child_a.package_id,
                            "child_manifest_sha256": child_a.manifest_sha256,
                            "seed_run_ids": [child_a.run_id],
                        },
                        {
                            "leg_id": "leg_b",
                            "child_package_id": child_b.package_id,
                            "child_manifest_sha256": child_b.manifest_sha256,
                            "seed_run_ids": [child_b.run_id],
                        },
                    ]
                }
            },
            "manifest_sha256": None,
        }
    )


def test_dry_run_apply_and_idempotent_backfill_single_alpha(tmp_path: Path) -> None:
    repo = InMemoryStrategyPackageRepository()
    record = repo.save_manifest(freeze_manifest(_single_manifest("single")))
    old_sha = record.manifest_sha256
    service = PackageAssetBackfillService(repository=repo, asset_freezer=_freezer(tmp_path))

    plan = service.build_plan(package_ids=[record.package_id])

    assert plan.items[0].status == STATUS_PLANNED_FREEZE
    assert repo.get(record.package_id).manifest_sha256 == old_sha
    assert repo.package_assets == {}

    applied = service.apply_plan(plan, operator="unit_test")
    updated = repo.get(record.package_id)

    assert applied.items[0].status == STATUS_APPLIED
    assert updated.manifest_sha256 != old_sha
    assert manifest_has_frozen_runtime_assets(updated.current_manifest())
    assert len(repo.list_package_assets(record.package_id)) == 2
    assert repo.events[-1].reason == "strategy_package_asset_backfill_freeze"
    assert repo.events[-1].context["old_manifest_sha256"] == old_sha
    assert repo.events[-1].context["new_manifest_sha256"] == updated.manifest_sha256

    rerun = service.build_plan(package_ids=[record.package_id])

    assert rerun.items[0].status == STATUS_SKIPPED_ALREADY_FROZEN
    assert rerun.items[0].new_manifest_sha256 == updated.manifest_sha256


def test_plan_report_and_prefix_filter_include_resolution_stats(tmp_path: Path) -> None:
    repo = InMemoryStrategyPackageRepository()
    keep = repo.save_manifest(freeze_manifest(_single_manifest("keep")))
    repo.save_manifest(freeze_manifest(_single_manifest("skip")))
    service = PackageAssetBackfillService(repository=repo, asset_freezer=_freezer(tmp_path))

    plan = service.build_plan(limit=10, package_id_prefix=keep.package_id)
    report = plan.to_report()

    assert [item.package_id for item in plan.items] == [keep.package_id]
    assert report["counts"] == {STATUS_PLANNED_FREEZE: 1}
    assert report["source_resolution"]["resolution_rate"] == 1.0
    assert report["items"][0]["status"] == STATUS_PLANNED_FREEZE


def test_invalid_limit_fails_loud(tmp_path: Path) -> None:
    service = PackageAssetBackfillService(
        repository=InMemoryStrategyPackageRepository(),
        asset_freezer=_freezer(tmp_path),
    )

    try:
        service.build_plan(limit=0)
    except Exception as exc:  # noqa: BLE001 - assert domain context.
        assert getattr(exc, "context", {})["reason_code"] == "strategy_package_asset_backfill_limit_invalid"
    else:  # pragma: no cover - defensive assertion branch.
        raise AssertionError("expected invalid limit failure")


def test_frozen_manifest_with_missing_ledger_rebuilds_package_asset_rows(tmp_path: Path) -> None:
    repo = InMemoryStrategyPackageRepository()
    freezer = _freezer(tmp_path)
    frozen_assets = freezer.freeze_manifest_assets(_single_manifest("ledger"))
    record = repo.save_manifest(frozen_assets.manifest)
    service = PackageAssetBackfillService(repository=repo, asset_freezer=freezer)

    plan = service.build_plan(package_ids=[record.package_id])

    assert plan.items[0].status == STATUS_PLANNED_FREEZE
    assert plan.items[0].old_manifest_sha256 == plan.items[0].new_manifest_sha256 == record.manifest_sha256
    assert repo.list_package_assets(record.package_id) == []

    applied = service.apply_plan(plan, operator="unit_test")

    assert applied.items[0].status == STATUS_APPLIED
    assert repo.get(record.package_id).manifest_sha256 == record.manifest_sha256
    assert len(repo.list_package_assets(record.package_id)) == 2


def test_already_frozen_manifest_with_complete_ledger_is_skipped(tmp_path: Path) -> None:
    repo = InMemoryStrategyPackageRepository()
    frozen_assets = _freezer(tmp_path).freeze_manifest_assets(_single_manifest("done"))
    record = repo.save_manifest_with_assets(frozen_assets.manifest, frozen_assets.assets)
    service = PackageAssetBackfillService(repository=repo, asset_freezer=_freezer(tmp_path))

    plan = service.build_plan(package_ids=[record.package_id])

    assert plan.items[0].status == STATUS_SKIPPED_ALREADY_FROZEN
    assert plan.items[0].asset_count == 2
    assert repo.events[-1].reason == "package_created"


def test_already_frozen_manifest_with_pickled_model_but_missing_code_is_repaired(tmp_path: Path) -> None:
    repo = InMemoryStrategyPackageRepository()
    model_payload = _pickled_model_instance_payload(tmp_path)
    model_py = b"class LSTM_10D_hs64_d02:\n    pass\n"
    freezer = _freezer(tmp_path, model_params=model_payload, model_code_files={"model.py": model_py})
    store = freezer.asset_store
    manifest = _single_manifest("legacy_missing_model_code")
    factor_blob = store.put(b"# factor legacy\nVALUE = 1\n", kind=StrategyPackageAssetType.FACTOR_CODE.value)
    model_blob = store.put(model_payload, kind=StrategyPackageAssetType.MODEL_WEIGHT.value)
    factor = manifest.factor_set[0].model_copy(
        update={
            "asset_ref": factor_blob.uri,
            "sha256": factor_blob.sha256,
            "size_bytes": factor_blob.size_bytes,
            "source_uri": "unit://factor/legacy.py",
        }
    )
    model = manifest.model_asset.model_copy(
        update={
            "asset_ref": model_blob.uri,
            "sha256": model_blob.sha256,
            "size_bytes": model_blob.size_bytes,
            "source_uri": "unit://model/legacy/params.pkl",
            "model_code_required": False,
            "model_code_assets": [],
        }
    )
    broken = freeze_manifest(manifest.model_copy(update={"factor_set": [factor], "model_asset": model, "manifest_sha256": None}))
    record = repo.save_manifest_with_assets(
        broken,
        [
            StrategyPackageAssetRecord(
                package_id=broken.package_id,
                asset_type=StrategyPackageAssetType.FACTOR_CODE,
                asset_ref=factor_blob.uri,
                asset_sha256=factor_blob.sha256,
                asset_size_bytes=factor_blob.size_bytes,
            ),
            StrategyPackageAssetRecord(
                package_id=broken.package_id,
                asset_type=StrategyPackageAssetType.MODEL_WEIGHT,
                asset_ref=model_blob.uri,
                asset_sha256=model_blob.sha256,
                asset_size_bytes=model_blob.size_bytes,
            ),
        ],
    )
    service = PackageAssetBackfillService(repository=repo, asset_freezer=freezer)

    plan = service.build_plan(package_ids=[record.package_id])

    item = plan.items[0]
    assert item.status == STATUS_PLANNED_FREEZE
    assert item.old_manifest_sha256 == record.manifest_sha256
    assert item.new_manifest_sha256 != record.manifest_sha256
    repair = item.context["model_code_repair"]
    assert repair["model_code_asset_count_before"] == 0
    assert repair["model_code_asset_count_after"] == 1
    assert repair["model_code_assets_to_add"][0]["relative_path"] == "model.py"
    assert item.frozen_manifest.model_asset.model_code_required is True
    assert repo.get(record.package_id).manifest_sha256 == record.manifest_sha256


def test_missing_source_is_reported_unrecoverable_without_writes(tmp_path: Path) -> None:
    repo = InMemoryStrategyPackageRepository()
    record = repo.save_manifest(freeze_manifest(_single_manifest("missing")))
    service = PackageAssetBackfillService(
        repository=repo,
        asset_freezer=_freezer(tmp_path, missing_factor="factor_missing"),
    )

    plan = service.build_plan(package_ids=[record.package_id])

    assert plan.items[0].status == STATUS_UNRECOVERABLE
    assert plan.items[0].reason_code == "strategy_package_factor_code_missing"
    assert plan.items[0].context["context"]["factor_name"] == "factor_missing"
    assert repo.get(record.package_id).manifest_sha256 == record.manifest_sha256
    assert repo.package_assets == {}


def test_manifest_drift_is_reported_unrecoverable(tmp_path: Path) -> None:
    repo = InMemoryStrategyPackageRepository()
    record = repo.save_manifest(freeze_manifest(_single_manifest("drift")))
    repo.records[record.package_id] = record.model_copy(update={"manifest_sha256": "0" * 64})
    service = PackageAssetBackfillService(repository=repo, asset_freezer=_freezer(tmp_path))

    plan = service.build_plan()

    assert plan.items[0].package_id == record.package_id
    assert plan.items[0].status == STATUS_UNRECOVERABLE
    assert plan.items[0].reason_code == "strategy_package_manifest_hash_drift"
    assert "drift" in plan.items[0].context


def test_multi_alpha_backfill_recurses_children_and_patches_parent_child_sha(tmp_path: Path) -> None:
    repo = InMemoryStrategyPackageRepository()
    child_a = repo.save_manifest(freeze_manifest(_single_manifest("a")))
    child_b = repo.save_manifest(freeze_manifest(_single_manifest("b")))
    parent = repo.save_manifest(freeze_manifest(_multi_manifest("combo", child_a, child_b)))
    repo.save_components(
        parent.package_id,
        [
            StrategyPackageComponentRecord(
                parent_package_id=parent.package_id,
                child_package_id=child_a.package_id,
                child_manifest_sha256=child_a.manifest_sha256,
                component_weight=0.6,
                position=1,
            ),
            StrategyPackageComponentRecord(
                parent_package_id=parent.package_id,
                child_package_id=child_b.package_id,
                child_manifest_sha256=child_b.manifest_sha256,
                component_weight=0.4,
                position=2,
            ),
        ],
    )
    service = PackageAssetBackfillService(repository=repo, asset_freezer=_freezer(tmp_path))

    plan = service.build_plan(package_ids=[parent.package_id])

    planned_ids = [item.package_id for item in plan.items if item.status == STATUS_PLANNED_FREEZE]
    assert planned_ids == [child_a.package_id, child_b.package_id, parent.package_id]

    applied = service.apply_plan(plan, operator="unit_test")
    child_a_after = repo.get(child_a.package_id)
    child_b_after = repo.get(child_b.package_id)
    parent_after = repo.get(parent.package_id)
    legs = parent_after.current_manifest().source_evidence["multi_alpha"]["legs"]

    assert [item.status for item in applied.items] == [STATUS_APPLIED, STATUS_APPLIED, STATUS_APPLIED]
    assert manifest_has_frozen_runtime_assets(child_a_after.current_manifest())
    assert manifest_has_frozen_runtime_assets(parent_after.current_manifest())
    assert legs[0]["child_manifest_sha256"] == child_a_after.manifest_sha256
    assert legs[1]["child_manifest_sha256"] == child_b_after.manifest_sha256
    assert len(repo.list_package_assets(parent.package_id)) == 4


def test_multi_alpha_missing_component_edge_is_unrecoverable(tmp_path: Path) -> None:
    repo = InMemoryStrategyPackageRepository()
    child_a = repo.save_manifest(freeze_manifest(_single_manifest("missa")))
    child_b = repo.save_manifest(freeze_manifest(_single_manifest("missb")))
    parent = repo.save_manifest(freeze_manifest(_multi_manifest("missing_edges", child_a, child_b)))
    service = PackageAssetBackfillService(repository=repo, asset_freezer=_freezer(tmp_path))

    plan = service.build_plan(package_ids=[parent.package_id])

    assert plan.items[0].status == STATUS_UNRECOVERABLE
    assert plan.items[0].reason_code == "strategy_package_asset_backfill_components_missing"


def test_multi_alpha_missing_child_is_unrecoverable(tmp_path: Path) -> None:
    repo = InMemoryStrategyPackageRepository()
    child_a = repo.save_manifest(freeze_manifest(_single_manifest("childa")))
    child_b = repo.save_manifest(freeze_manifest(_single_manifest("childb")))
    parent = repo.save_manifest(freeze_manifest(_multi_manifest("child_missing", child_a, child_b)))
    repo.save_components(
        parent.package_id,
        [
            StrategyPackageComponentRecord(
                parent_package_id=parent.package_id,
                child_package_id=child_a.package_id,
                child_manifest_sha256=child_a.manifest_sha256,
                component_weight=0.6,
                position=1,
            ),
            StrategyPackageComponentRecord(
                parent_package_id=parent.package_id,
                child_package_id=child_b.package_id,
                child_manifest_sha256=child_b.manifest_sha256,
                component_weight=0.4,
                position=2,
            ),
        ],
    )
    repo.records.pop(child_b.package_id)
    service = PackageAssetBackfillService(repository=repo, asset_freezer=_freezer(tmp_path))

    plan = service.build_plan(package_ids=[parent.package_id])

    assert plan.items[-1].package_id == parent.package_id
    assert plan.items[-1].status == STATUS_UNRECOVERABLE
    assert plan.items[-1].reason_code == "strategy_package_asset_backfill_child_unrecoverable"
    assert plan.items[-1].context["child_failures"][0]["child_package_id"] == child_b.package_id


def test_multi_alpha_parent_evidence_missing_child_entry_is_unrecoverable(tmp_path: Path) -> None:
    repo = InMemoryStrategyPackageRepository()
    child_a = repo.save_manifest(freeze_manifest(_single_manifest("eva")))
    child_b = repo.save_manifest(freeze_manifest(_single_manifest("evb")))
    manifest = _multi_manifest("bad_evidence", child_a, child_b)
    evidence = manifest.source_evidence
    evidence["multi_alpha"]["legs"] = evidence["multi_alpha"]["legs"][:1]
    parent = repo.save_manifest(freeze_manifest(manifest.model_copy(update={"source_evidence": evidence, "manifest_sha256": None})))
    repo.save_components(
        parent.package_id,
        [
            StrategyPackageComponentRecord(
                parent_package_id=parent.package_id,
                child_package_id=child_a.package_id,
                child_manifest_sha256=child_a.manifest_sha256,
                component_weight=0.6,
                position=1,
            ),
            StrategyPackageComponentRecord(
                parent_package_id=parent.package_id,
                child_package_id=child_b.package_id,
                child_manifest_sha256=child_b.manifest_sha256,
                component_weight=0.4,
                position=2,
            ),
        ],
    )
    service = PackageAssetBackfillService(repository=repo, asset_freezer=_freezer(tmp_path))

    plan = service.build_plan(package_ids=[parent.package_id])

    assert plan.items[-1].status == STATUS_UNRECOVERABLE
    assert plan.items[-1].reason_code == "strategy_package_asset_backfill_parent_evidence_missing"
    assert plan.items[-1].context["context"]["missing_child_package_ids"] == [child_b.package_id]


def test_apply_reports_cas_race_without_partial_write(tmp_path: Path) -> None:
    repo = InMemoryStrategyPackageRepository()
    record = repo.save_manifest(freeze_manifest(_single_manifest("race")))
    service = PackageAssetBackfillService(repository=repo, asset_freezer=_freezer(tmp_path))
    plan = service.build_plan(package_ids=[record.package_id])
    repo.records[record.package_id] = repo.records[record.package_id].model_copy(update={"manifest_sha256": "0" * 64})

    applied = service.apply_plan(plan, operator="unit_test")

    assert applied.items[0].status == STATUS_UNRECOVERABLE
    assert applied.items[0].reason_code == "strategy_package_asset_backfill_cas_mismatch"
    assert repo.package_assets == {}
    assert all(event.reason != "strategy_package_asset_backfill_freeze" for event in repo.events)


def test_apply_reports_incomplete_plan_and_keeps_non_planned_items(tmp_path: Path) -> None:
    service = PackageAssetBackfillService(
        repository=InMemoryStrategyPackageRepository(),
        asset_freezer=_freezer(tmp_path),
    )
    planned = PackageAssetBackfillItem(
        package_id="pkg_incomplete",
        package_name="incomplete",
        alpha_mode="single_alpha",
        old_manifest_sha256="a" * 64,
        status=STATUS_PLANNED_FREEZE,
    )
    skipped = PackageAssetBackfillItem(
        package_id="pkg_skipped",
        package_name="skipped",
        alpha_mode="single_alpha",
        old_manifest_sha256="b" * 64,
        status=STATUS_SKIPPED_ALREADY_FROZEN,
    )

    result = service.apply_plan(PackageAssetBackfillPlan(items=[planned, skipped]), operator="unit_test")

    assert result.mode == "apply"
    assert result.items[0].status == STATUS_UNRECOVERABLE
    assert result.items[0].reason_code == "strategy_package_asset_backfill_plan_incomplete"
    assert result.items[1].status == STATUS_SKIPPED_ALREADY_FROZEN


def test_apply_blocks_remaining_planned_items_after_failure(tmp_path: Path) -> None:
    manifest = _single_manifest("blocked")
    frozen_assets = _freezer(tmp_path).freeze_manifest_assets(manifest)
    incomplete = PackageAssetBackfillItem(
        package_id="pkg_incomplete",
        package_name="incomplete",
        alpha_mode="single_alpha",
        old_manifest_sha256="a" * 64,
        status=STATUS_PLANNED_FREEZE,
    )
    second = PackageAssetBackfillItem(
        package_id=manifest.package_id,
        package_name=manifest.package_name,
        alpha_mode="single_alpha",
        old_manifest_sha256="b" * 64,
        status=STATUS_PLANNED_FREEZE,
        frozen_manifest=frozen_assets.manifest,
        assets=frozen_assets.assets,
    )
    service = PackageAssetBackfillService(
        repository=InMemoryStrategyPackageRepository(),
        asset_freezer=_freezer(tmp_path),
    )

    result = service.apply_plan(PackageAssetBackfillPlan(items=[incomplete, second]), operator="unit_test")

    assert result.items[0].reason_code == "strategy_package_asset_backfill_plan_incomplete"
    assert result.items[1].status == STATUS_UNRECOVERABLE
    assert result.items[1].reason_code == "strategy_package_asset_backfill_apply_blocked_after_failure"


def test_merge_conflicts_and_helper_jsonable_are_explicit() -> None:
    left = _single_manifest("conflict").factor_set[0].model_copy(update={"asset_ref": "a", "sha256": "1" * 64})
    right = _single_manifest("conflict").factor_set[0].model_copy(update={"asset_ref": "b", "sha256": "2" * 64})
    left_manifest = _single_manifest("left").model_copy(update={"factor_set": [left], "manifest_sha256": None})
    right_manifest = _single_manifest("right").model_copy(update={"factor_set": [right], "manifest_sha256": None})

    try:
        backfill_module._merge_factor_assets([left_manifest, right_manifest], parent_package_id="parent")  # noqa: SLF001
    except Exception as exc:  # noqa: BLE001 - assert domain context.
        assert getattr(exc, "context", {})["reason_code"] == "strategy_package_asset_backfill_child_factor_conflict"
    else:  # pragma: no cover - defensive assertion branch.
        raise AssertionError("expected conflict")

    assert backfill_module._jsonable({"mode": AlphaMode.SINGLE_ALPHA, "items": {1, 2}})["mode"] == "single_alpha"  # noqa: SLF001


def test_requested_missing_package_is_explicit_unrecoverable(tmp_path: Path) -> None:
    service = PackageAssetBackfillService(
        repository=InMemoryStrategyPackageRepository(),
        asset_freezer=_freezer(tmp_path),
    )

    plan = service.build_plan(package_ids=["pkg_missing"])

    assert plan.items[0].package_id == "pkg_missing"
    assert plan.items[0].status == STATUS_UNRECOVERABLE
    assert plan.items[0].reason_code == "strategy_package_asset_backfill_requested_package_missing"
