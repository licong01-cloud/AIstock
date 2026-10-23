from __future__ import annotations

from copy import deepcopy
import hashlib
import pickle
from types import SimpleNamespace

import pytest

from backend.services.qe_archive.multi_alpha_provenance import SeedProvenance
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import (
    AlphaCombinationPolicy,
    AlphaMode,
    PackageStatus,
    RuntimeAssetManifest,
    SourceType,
)
from backend.services.strategy_package.package_asset_freeze import PackageAssetBytes
from backend.services.strategy_package.package_asset_store import LocalPackageAssetStore
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository
from backend.services.strategy_package.seed_asset_recovery import SeedAssetRecoveryService, semantic_payload
from backend.services.trading_core.errors import DataUnavailableError, StrategyPackageValidationError
from backend.services.trading_core.execution_algo_retirement import require_execution_algo_active
from backend.tests.strategy_package.test_manifest_v1 import make_manifest
from backend.tests.strategy_package.test_multi_alpha_promotion import NoopFrozenRuntimeSelfCheck


def recovery_fixture(tmp_path, counts=(33, 5)):
    """Synthetic governance fixture; never evidence that production models load."""
    source_store = LocalPackageAssetStore(tmp_path / "original")
    stage = LocalPackageAssetStore(tmp_path / "staging")
    base = make_manifest("V25_1_SMALL_CAP")
    factors = []
    for f in base.factor_set:
        blob = source_store.put(f.factor_id.encode(), kind="factor_code")
        factors.append(
            f.model_copy(update={"asset_ref": blob.uri, "sha256": blob.sha256, "size_bytes": blob.size_bytes})
        )
    source = SimpleNamespace()
    source.weights, source.archives, source.manifests, source.provenances = {}, {}, {}, {}
    source.missing_config = set()
    source.config = b"task: {dataset: {kwargs: {handler: {kwargs: {infer_processors: []}}}}}"

    def weight(manifest):
        return source.weights[manifest.source_evidence["seed_run_ids"][0]]

    def config(manifest):
        seed = manifest.source_evidence.get("seed_run_ids", [""])[0]
        if seed in source.missing_config:
            raise DataUnavailableError("original config missing", context={"reason_code": "original_config_missing"})
        return PackageAssetBytes(source.config, "unit-original://conf.yaml")

    def processor(*args, **kwargs):
        raise DataUnavailableError("original dataset missing", context={"reason_code": "original_preprocessor_missing"})

    source.model_params_bytes, source.conf_yaml_bytes = weight, config
    source.fitted_preprocessor_bytes = processor
    source.artifact_store = SimpleNamespace(load_manifest=lambda key: source.archives[key])
    components, legs, models = [], [], []
    for leg_index, count in enumerate(counts):
        leg_id, model_id = f"leg_{leg_index}", f"model_{leg_index}"
        component = base.alpha_components[0].model_copy(
            update={
                "alpha_id": leg_id,
                "model_id": model_id,
                "model_ref": model_id,
                "component_weight": (0.7 if leg_index == 0 else 0.3),
                "score_normalization": "rank",
            }
        )
        component = component.model_copy(
            update={"lineage": component.lineage.model_copy(update={"model_artifact_ref": model_id})}
        )
        components.append(component)
        seeds = [f"qear_run_leg{leg_index}_seed{i}" for i in range(count)]
        provenance = []
        for i, seed in enumerate(seeds):
            p = SeedProvenance(
                seed_ref=seed,
                seed_ref_kind="archive_run_id",
                resolved=True,
                resolve_method="unit_original",
                source_task_id=f"qe_leg_{leg_index}",
                source_loop_id=f"qe_leg_{leg_index}_Loop{i + 1}",
                source_loop_index=i + 1,
                source_experiment_id=f"qe_leg_{leg_index}_L{i + 1}",
                source_run_id=seed,
            )
            provenance.append(p.to_meta())
            source.provenances[seed] = p
            data = pickle.dumps({"original_seed": seed})
            sha = hashlib.sha256(data).hexdigest()
            source.weights[seed] = PackageAssetBytes(data, f"aistock-prediction-store://runs/{seed}/model_params")
            source.archives[seed] = {
                "metadata": {"task_id": p.source_task_id, "loop_index": p.source_loop_index},
                "artifacts": [{"artifact_type": "model_params", "sha256": sha, "size_bytes": len(data)}],
            }
            model = base.model_asset.model_copy(update={"model_id": model_id})
            source.manifests[seed] = freeze_manifest(
                base.model_copy(
                    update={
                        "factor_set": factors,
                        "model_asset": model,
                        "alpha_components": [component.model_copy(update={"component_weight": 1.0})],
                        "source_evidence": {"seed_run_ids": [seed]},
                        "runtime_assets": RuntimeAssetManifest(),
                        "alpha_combination_policy": AlphaCombinationPolicy(method="identity", weights={leg_id: 1.0}),
                        "manifest_sha256": None,
                    }
                )
            )
            if i == 0:
                blob = source_store.put(data, kind="model_weight")
                models.append(model.model_copy(update={"asset_ref": blob.uri, "sha256": sha, "size_bytes": len(data)}))
        legs.append(
            {
                "leg_id": leg_id,
                "model_id": model_id,
                "seed_run_ids": seeds,
                "seed_provenance": provenance,
                "ensemble_method": "mean_by_trade_date_instrument",
                "terminal_weight": component.component_weight,
                "factor_artifact_refs": [f.factor_id for f in factors],
                "runtime_assets": RuntimeAssetManifest().model_dump(mode="json"),
            }
        )
    parent = freeze_manifest(
        base.model_copy(
            update={
                "package_id": "pkg_legacy_unit_recovery",
                "alpha_mode": AlphaMode.MULTI_ALPHA,
                "source": base.source.model_copy(
                    update={"source_type": SourceType.MULTI_ALPHA_COMBINE_RUN, "source_id": "unit_recovery_combine"}
                ),
                "alpha_components": components,
                "factor_set": factors,
                "model_asset": models,
                "runtime_assets": RuntimeAssetManifest(),
                "alpha_combination_policy": AlphaCombinationPolicy(
                    method="ic_weighted", weights={"leg_0": 0.7, "leg_1": 0.3}
                ),
                "source_evidence": {
                    "authority": "parent_package_asset_runtime_authority",
                    "multi_alpha": {
                        "legs": legs,
                        "weighting_scheme": "ic_weighted",
                        "weight_policy": {"lookback": 20},
                        "terminal_weights": {"leg_0": 0.7, "leg_1": 0.3},
                        "per_window_weights": [{"date": "2026-01-01", "weights": [0.6, 0.4]}],
                    },
                },
                "manifest_sha256": None,
            }
        )
    )
    promotion = SimpleNamespace(
        asset_freezer=SimpleNamespace(source=source),
        _resolve_leg_seed_sources=lambda **kw: (source.provenances[kw["seed_run_ids"][0]],),
        _build_parent_leg_asset_manifest=lambda **kw: source.manifests[kw["seed_run_ids"][0]],
    )
    service = SeedAssetRecoveryService(
        promotion=promotion, source_store=source_store, staging_store=stage, self_check=NoopFrozenRuntimeSelfCheck()
    )
    return service, parent, source


def test_38_seeds_preserve_semantics_and_create_immutable_draft(tmp_path):
    service, parent, _ = recovery_fixture(tmp_path)
    original = parent.model_dump(mode="json")
    plan = service.audit(parent, version="1.0.1-seed-assets")
    assert plan.ready, plan.receipts
    assert len(plan.receipts) == len(plan.manifest.model_asset) == 38
    assert len({m.sha256 for m in plan.manifest.model_asset}) == 38
    assert semantic_payload(plan.manifest) == semantic_payload(parent)
    assert plan.manifest.package_status == PackageStatus.DRAFT
    for old, new in zip(
        parent.source_evidence["multi_alpha"]["legs"], plan.manifest.source_evidence["multi_alpha"]["legs"], strict=True
    ):
        assert [x["seed_run_id"] for x in new["seed_assets"]] == old["seed_run_ids"]
    repo = InMemoryStrategyPackageRepository()
    repo.save_manifest(parent)
    first = service.persist(plan, repository=repo, current_parent=parent)
    second = service.persist(plan, repository=repo, current_parent=parent)
    assert first.manifest_sha256 == second.manifest_sha256
    assert repo.get(parent.package_id).manifest.model_dump(mode="json") == original
    assert len(repo.records) == 2


def test_missing_original_config_generates_all_receipts_and_no_partial_write(tmp_path):
    service, parent, source = recovery_fixture(tmp_path)
    source.missing_config.add("qear_run_leg0_seed7")
    plan = service.audit(parent, version="1.0.1")
    assert not plan.ready and plan.manifest is None
    assert len(plan.receipts) == 38
    assert plan.receipts[7]["checks"]["preprocessor"]["reason"] == "original_preprocessor_missing"
    repo = InMemoryStrategyPackageRepository()
    with pytest.raises(StrategyPackageValidationError, match="seed_recovery_incomplete"):
        service.persist(plan, repository=repo, current_parent=parent)
    assert not repo.records


@pytest.mark.parametrize(
    "mutation,reason",
    [
        ("copy_weight", "seed_recovery_weight_source_not_bound"),
        ("archive_hash", "seed_recovery_weight_archive_identity_mismatch"),
        ("provenance", "seed_recovery_provenance_drift"),
        ("factor_order", "seed_recovery_factor_order_drift"),
    ],
)
def test_original_seed_binding_cannot_be_replaced(tmp_path, mutation, reason):
    service, parent, source = recovery_fixture(tmp_path, counts=(2, 1))
    seed = "qear_run_leg0_seed1"
    if mutation == "copy_weight":
        source.weights[seed] = source.weights["qear_run_leg0_seed0"]
    elif mutation == "archive_hash":
        source.archives[seed]["artifacts"][0]["sha256"] = "a" * 64
    elif mutation == "provenance":
        source.provenances[seed] = deepcopy(source.provenances["qear_run_leg0_seed0"])
    else:
        m = source.manifests[seed]
        c = m.alpha_components[0]
        source.manifests[seed] = m.model_copy(
            update={
                "alpha_components": [
                    c.model_copy(
                        update={
                            "lineage": c.lineage.model_copy(
                                update={"factor_artifact_refs": list(reversed(c.lineage.factor_artifact_refs))}
                            )
                        }
                    )
                ]
            }
        )
    plan = service.audit(parent, version="1.0.1")
    assert not plan.ready
    assert reason in {c.get("reason") for c in plan.receipts[1]["checks"].values()}


def test_duplicate_roster_and_same_version_fail_before_asset_reads(tmp_path):
    service, parent, _ = recovery_fixture(tmp_path, counts=(2, 1))
    with pytest.raises(StrategyPackageValidationError, match="new_multi_alpha_version"):
        service.audit(parent, version=parent.package_version)
    evidence = deepcopy(parent.source_evidence)
    evidence["multi_alpha"]["legs"][0]["seed_run_ids"] *= 2
    bad = freeze_manifest(parent.model_copy(update={"source_evidence": evidence, "manifest_sha256": None}))
    with pytest.raises(StrategyPackageValidationError, match="invalid_seed_roster"):
        service.audit(bad, version="1.0.1")


def test_parent_change_or_receipt_tamper_denies_persistence(tmp_path):
    service, parent, _ = recovery_fixture(tmp_path, counts=(2, 1))
    plan = service.audit(parent, version="1.0.1")
    repo = InMemoryStrategyPackageRepository()
    changed = freeze_manifest(parent.model_copy(update={"package_name": "changed", "manifest_sha256": None}))
    with pytest.raises(StrategyPackageValidationError, match="parent_changed"):
        service.persist(plan, repository=repo, current_parent=changed)
    plan.receipts[0]["seed_run_id"] = "copied"
    with pytest.raises(StrategyPackageValidationError, match="invalid_candidate"):
        service.persist(plan, repository=repo, current_parent=parent)
    assert not repo.records


def test_recovery_does_not_bypass_v25_retirement(tmp_path):
    service, parent, _ = recovery_fixture(tmp_path, counts=(2, 1))
    plan = service.audit(parent, version="1.0.1")
    assert plan.ready
    with pytest.raises(Exception) as error:
        require_execution_algo_active(
            plan.manifest.minute_execution_policy.algo_code, operation="multi_alpha_strategy_package_promotion"
        )
    assert "V25_EXECUTION_ALGO_RETIRED" in str(error.value) or "V25_EXECUTION_ALGO_RETIRED" in str(
        getattr(error.value, "context", {})
    )


def test_staging_corruption_blocks_database_write(tmp_path):
    service, parent, _ = recovery_fixture(tmp_path, counts=(2, 1))
    plan = service.audit(parent, version="1.0.1")
    model = plan.manifest.model_asset[0]
    service.store._blob_path(model.sha256).write_bytes(b"corrupted test blob")
    repo = InMemoryStrategyPackageRepository()
    with pytest.raises(Exception) as error:
        service.persist(plan, repository=repo, current_parent=parent)
    assert "mismatch" in str(error.value).lower()
    assert not repo.records


def test_backfill_stays_blocked_and_points_to_versioned_recovery(tmp_path):
    from backend.services.strategy_package.package_asset_backfill import PackageAssetBackfillService

    _, parent, _ = recovery_fixture(tmp_path, counts=(2, 1))
    record = SimpleNamespace(
        manifest=parent,
        package_id=parent.package_id,
        package_name=parent.package_name,
        alpha_mode=parent.alpha_mode,
        manifest_sha256=parent.manifest_sha256,
    )
    item = PackageAssetBackfillService._unrecoverable_item(
        record, reason_code="multi_alpha_parent_leg_runtime_assets_incomplete", context={}
    )
    assert item.status == "unrecoverable" and item.frozen_manifest is None
    assert item.context["asset_recovery"]["in_place_backfill_allowed"] is False
    assert item.context["asset_recovery"]["execution_retirement_checks_unchanged"] is True
