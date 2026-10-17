from __future__ import annotations

import hashlib
from types import SimpleNamespace

from backend.services.multi_alpha.combine_backtest import InMemoryCombineBacktestRepository
from backend.services.qe_archive.multi_alpha_provenance import SeedProvenance
from backend.services.strategy_package.frozen_runtime_self_check import (
    FrozenRuntimeSelfCheckResult,
)
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import (
    Alpha158SchemaAsset,
    AlphaCombinationPolicy,
    RuntimeAssetManifest,
)
from backend.services.strategy_package.multi_alpha_promotion import (
    MULTI_ALPHA_PACKAGE_PROMOTE_CONFIRMATION,
    MultiAlphaPackagePromotionService,
)
from backend.services.strategy_package.package_asset import (
    StrategyPackageAssetRecord,
    StrategyPackageAssetType,
)
from backend.services.strategy_package.repository import (
    InMemoryStrategyPackageRepository,
)
from backend.tests.strategy_package.test_multi_alpha_base_schema import (
    _single_manifest,
)


RUN_ID = "macb_target_two_leg_20260627"
A1_LEG = "a1_plus3_LSTM_h20"
FUND_LEG = "new_FUNDGROWTH_h20"
A1_SEED = "qear_run_a1_seed_42"
FUND_SEED = "qe_new_FUNDGROWTH_L5"
PRED_SHA = "a" * 64


class FakeProvenanceResolver:
    def __init__(self) -> None:
        self.mapping = {
            A1_SEED: SeedProvenance(
                seed_ref=A1_SEED,
                seed_ref_kind="archive_run_id",
                resolved=True,
                resolve_method="unit_fake_archive_run_id_lookup",
                source_experiment_id="qe_exp_a1_seed_42",
                source_run_type="qe_archive_run",
                source_run_id=A1_SEED,
            ),
            FUND_SEED: SeedProvenance(
                seed_ref=FUND_SEED,
                seed_ref_kind="evolution_loop_id",
                resolved=True,
                resolve_method="unit_fake_evolution_loop_id_lookup",
                source_experiment_id="qe_exp_fundgrowth_seed_42",
                source_task_id="qe_new_FUNDGROWTH",
                source_loop_id=FUND_SEED,
                source_loop_index=5,
                source_run_type="qe_evolution_loop",
                source_run_id="qear_run_fundgrowth_seed_42",
            ),
        }

    def resolve_seed(self, seed_ref: str) -> SeedProvenance:
        return self.mapping[seed_ref]


class FakeQESourceResolver:
    def build_from_experiment(
        self,
        experiment_id: str,
        *,
        resolve_runtime_assets: bool = False,
    ):
        del resolve_runtime_assets
        return freeze_manifest(
            _single_manifest_for_parent_leg(
                f"auto_{experiment_id}", run_id=experiment_id
            )
        )

    def build_from_evolution_loop(
        self,
        *,
        qe_task_id: str,
        qe_loop_id: str,
        resolve_runtime_assets: bool = False,
    ):
        del resolve_runtime_assets
        source_key = f"{qe_task_id}:{qe_loop_id}"
        return freeze_manifest(
            _single_manifest_for_parent_leg(
                f"auto_{qe_task_id}_{qe_loop_id}", run_id=source_key
            )
        )


class NoopFrozenRuntimeSelfCheck:
    def assert_manifest_self_contained(self, manifest):  # noqa: ANN001, ANN201
        leg_count = len(manifest.alpha_components)
        return FrozenRuntimeSelfCheckResult(
            package_id=manifest.package_id,
            manifest_sha256=manifest.manifest_sha256 or "",
            origin="package_asset",
            model_kind="multi_alpha_parent",
            model_expected_features=leg_count,
            dynamic_factor_count=leg_count,
            alpha158_alias_count=0,
            factor_order_count=leg_count,
            feature_count_delta=0,
            model_params_path="aistock-package-asset://unit/self-check",
            model_probe_backend="unit_noop",
            leg_results={},
            combined_signal_smoke={
                "schema_version": "multi_alpha_parent_combined_signal_smoke_v1",
                "trade_date": "2024-05-05",
                "instrument": "__self_check__",
                "leg_count": leg_count,
                "combined_score": 1.0,
                "deterministic_replay": True,
            },
        )


class FakeAssetFreezer:
    def freeze_manifest_assets(self, manifest):  # noqa: ANN001, ANN201
        frozen = _with_frozen_assets(manifest, label=manifest.package_name)
        return SimpleNamespace(
            manifest=frozen,
            assets=_asset_records_from_manifest(frozen),
        )


def _single_manifest_for_parent_leg(name: str, *, run_id: str | None = None):
    manifest = _single_manifest(name, run_id=run_id)
    safe = "".join(ch if ch.isalnum() else "_" for ch in name.lower())
    factor_set = [
        factor.model_copy(
            update={
                "factor_id": f"{safe}_{factor.factor_id}",
                "factor_name": f"{safe}_{factor.factor_name}",
            }
        )
        for factor in manifest.factor_set
    ]
    factor_ids = [factor.factor_id for factor in factor_set]
    model_id = f"model_{safe}"
    component = manifest.alpha_components[0].model_copy(
        update={
            "factor_ids": factor_ids,
            "model_id": model_id,
            "model_ref": model_id,
            "lineage": manifest.alpha_components[0].lineage.model_copy(
                update={
                    "factor_artifact_refs": factor_ids,
                    "model_artifact_ref": model_id,
                }
            ),
        }
    )
    return manifest.model_copy(
        update={
            "alpha_components": [component],
            "alpha_combination_policy": AlphaCombinationPolicy(
                method="identity", weights={component.alpha_id: 1.0}
            ),
            "factor_set": factor_set,
            "model_asset": manifest.model_asset.model_copy(
                update={"model_id": model_id, "model_ref": model_id}
            ),
            "manifest_sha256": None,
        }
    )


def _with_frozen_assets(manifest, *, label: str):  # noqa: ANN001, ANN202
    schema_payload = b'{"schema_version":"strategy_package_alpha158_schema_v1"}'
    schema_sha = hashlib.sha256(schema_payload).hexdigest()
    factors = []
    for factor in manifest.factor_set:
        payload = f"{label}:{factor.factor_name}".encode()
        digest = hashlib.sha256(payload).hexdigest()
        factors.append(
            factor.model_copy(
                update={
                    "asset_ref": f"aistock-package-asset://blobs/{digest}?kind=factor_code",
                    "sha256": digest,
                    "size_bytes": len(payload),
                    "source_uri": f"unit://factor/{factor.factor_name}.py",
                }
            )
        )
    model_inputs = (
        manifest.model_asset
        if isinstance(manifest.model_asset, list)
        else [manifest.model_asset]
    )
    models = []
    for model in model_inputs:
        payload = f"{label}:{model.model_id}:model".encode()
        digest = hashlib.sha256(payload).hexdigest()
        models.append(
            model.model_copy(
                update={
                    "asset_ref": f"aistock-package-asset://blobs/{digest}?kind=model_weight",
                    "sha256": digest,
                    "size_bytes": len(payload),
                    "source_uri": "unit://model/params.pkl",
                }
            )
        )
    runtime_assets = RuntimeAssetManifest(
        alpha158=Alpha158SchemaAsset(
            enabled=True,
            aliases=["RESI5"],
            alias_count=1,
            loader_class="qlib.contrib.data.loader.Alpha158DL",
            asset_ref=f"aistock-package-asset://blobs/{schema_sha}",
            sha256=schema_sha,
            size_bytes=len(schema_payload),
            source_uri="unit://conf/alpha158_schema.json",
        )
    )
    return freeze_manifest(
        manifest.model_copy(
            update={
                "factor_set": factors,
                "model_asset": models if isinstance(manifest.model_asset, list) else models[0],
                "runtime_assets": runtime_assets,
                "manifest_sha256": None,
            }
        )
    )


def _asset_records_from_manifest(manifest) -> list[StrategyPackageAssetRecord]:  # noqa: ANN001
    rows = [
        StrategyPackageAssetRecord(
            package_id=manifest.package_id,
            asset_type=StrategyPackageAssetType.FACTOR_CODE,
            asset_ref=factor.asset_ref,
            asset_sha256=factor.sha256,
            asset_size_bytes=factor.size_bytes,
            source_uri=factor.source_uri,
            metadata={"logical_name": factor.factor_name},
        )
        for factor in manifest.factor_set
    ]
    models = (
        manifest.model_asset
        if isinstance(manifest.model_asset, list)
        else [manifest.model_asset]
    )
    rows.extend(
        StrategyPackageAssetRecord(
            package_id=manifest.package_id,
            asset_type=StrategyPackageAssetType.MODEL_WEIGHT,
            asset_ref=model.asset_ref,
            asset_sha256=model.sha256,
            asset_size_bytes=model.size_bytes,
            source_uri=model.source_uri,
            metadata={"logical_name": model.model_id},
        )
        for model in models
    )
    alpha158 = manifest.runtime_assets.alpha158
    rows.append(
        StrategyPackageAssetRecord(
            package_id=manifest.package_id,
            asset_type=StrategyPackageAssetType.FACTOR_SCHEMA,
            asset_ref=alpha158.asset_ref,
            asset_sha256=alpha158.sha256,
            asset_size_bytes=alpha158.size_bytes,
            source_uri=alpha158.source_uri,
            metadata={"logical_name": "alpha158_schema"},
        )
    )
    return rows


def _seed_child(
    repository: InMemoryStrategyPackageRepository,
    name: str,
    leg_id: str,
    seed_run_id: str,
):
    manifest = _single_manifest(name, run_id=seed_run_id)
    component = manifest.alpha_components[0].model_copy(
        update={"alpha_id": leg_id, "alpha_name": leg_id}
    )
    manifest = _with_frozen_assets(
        freeze_manifest(
            manifest.model_copy(
                update={
                    "alpha_components": [component],
                    "alpha_combination_policy": AlphaCombinationPolicy(
                        method="identity", weights={leg_id: 1.0}
                    ),
                    "source_evidence": {"seed_run_ids": [seed_run_id]},
                    "manifest_sha256": None,
                }
            )
        ),
        label=name,
    )
    return repository.save_manifest_with_assets(
        manifest,
        _asset_records_from_manifest(manifest),
    )


def _seed_repos():
    combine_repository = InMemoryCombineBacktestRepository()
    package_repository = InMemoryStrategyPackageRepository()
    child_a1 = _seed_child(package_repository, "a1", A1_LEG, A1_SEED)
    child_fund = _seed_child(
        package_repository, "fund", FUND_LEG, FUND_SEED
    )
    combine_repository.runs[RUN_ID] = {
        "id": RUN_ID,
        "roster_hash": "roster_hash",
        "roster_json": [
            {"leg_id": A1_LEG, "seed_run_ids": [A1_SEED], "metadata": {}},
            {"leg_id": FUND_LEG, "seed_run_ids": [FUND_SEED], "metadata": {}},
        ],
        "oos_start": "2024-07-02",
        "oos_end": "2026-03-10",
        "normalize_method": "zscore",
        "walk_forward_json": {"enabled": True, "window": 60, "min_periods": 2},
        "backtest_config_json": {
            "stock_pool": "V25_1_SMALL_CAP",
            "filtered_pool": "filtered_pool_20260428",
            "label_horizon": 20,
            "execution_algo": "TWAP",
            "n_drop": 2,
            "topk": 50,
        },
        "baseline_leg_id": A1_LEG,
        "status": "succeeded",
        "reason": None,
    }
    combine_repository.scheme_results.append(
        {
            "id": "scheme_icw_1",
            "run_id": RUN_ID,
            "weighting_scheme": "ic_weighted",
            "weights_json": {
                A1_LEG: 0.61,
                FUND_LEG: 0.39,
                "combined_prediction_ref": {
                    "uri": f"aistock-prediction-store://multi-alpha/{RUN_ID}/combined_prediction.pkl",
                    "sha256": PRED_SHA,
                },
            },
            "per_window_weights_json": [],
            "cagr": 1.0715,
            "max_drawdown": -0.1651,
            "sharpe": 2.845,
            "calmar": 6.4886,
            "topk_return_20": 0.0631,
            "topk_hit_rate_20": 0.6471,
            "turnover": 19.2,
            "pred_persisted": True,
            "skipped": False,
            "skipped_reason": None,
        }
    )
    return combine_repository, package_repository, child_a1, child_fund


def _service(combine_repository, package_repository):  # noqa: ANN001, ANN202
    return MultiAlphaPackagePromotionService(
        combine_repository=combine_repository,
        package_repository=package_repository,
        provenance_resolver=FakeProvenanceResolver(),
        source_resolver=FakeQESourceResolver(),
        asset_freezer=FakeAssetFreezer(),
        frozen_runtime_self_check=NoopFrozenRuntimeSelfCheck(),
    )


def _promote(service, child_a1, child_fund):  # noqa: ANN001, ANN202
    del child_a1, child_fund
    return service.promote_from_combine_run(
        combine_backtest_run_id=RUN_ID,
        weighting_scheme="ic_weighted",
        scheme_result_id="scheme_icw_1",
        topk=50,
        secondary_topk=[25],
        package_name="MA2_a1_plus3_LSTM_new_FUNDGROWTH_icw_h20",
        weight_policy={
            "mode": "frozen_backtest_terminal_weights",
            "metric": "rank_ic",
            "lookback_trading_days": 252,
            "min_periods": 60,
            "label_horizon": 20,
            "label_maturity_lag_days": 20,
            "clip_negative_to_zero": True,
        },
        confirmation=MULTI_ALPHA_PACKAGE_PROMOTE_CONFIRMATION,
    )


def test_promotion_contract_freezes_reproducible_parent_package() -> None:
    combine_repo, package_repo, child_a1, child_fund = _seed_repos()
    service = _service(combine_repo, package_repo)

    first = _promote(service, child_a1, child_fund).package
    second = _promote(service, child_a1, child_fund).package

    assert first.package_id == second.package_id
    assert first.manifest_sha256 == second.manifest_sha256
    manifest = first.current_manifest()
    assert {item.alpha_id for item in manifest.alpha_components} == {
        A1_LEG,
        FUND_LEG,
    }
    assert manifest.backtest_context["execution"]["execution_algo"] == "TWAP"
    assert manifest.source_evidence["multi_alpha"]["combine_backtest_run_id"] == RUN_ID
