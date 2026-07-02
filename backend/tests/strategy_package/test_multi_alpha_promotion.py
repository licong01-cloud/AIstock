from __future__ import annotations

import hashlib
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.db import init_trading_core_v2_schema
from backend.routers import strategy_packages as router_module
from backend.services.multi_alpha.combine_backtest import InMemoryCombineBacktestRepository
from backend.services.qe_archive.multi_alpha_provenance import SeedProvenance
from backend.services.strategy_package.asset_eligibility import MULTI_ALPHA_LOCALSIM_DRY_RUN_NOT_REQUIRED
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import AlphaCombinationPolicy, AlphaMode, PackageStatus, SourceType
from backend.services.strategy_package.multi_alpha_promotion import (
    MULTI_ALPHA_PACKAGE_PROMOTE_CONFIRMATION,
    MULTI_ALPHA_PAPER_ADMISSION_BLOCKER,
    MultiAlphaPackagePromotionService,
)
from backend.services.strategy_package.package_asset import StrategyPackageAssetRecord, StrategyPackageAssetType
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository
from backend.services.trading_core.errors import DataUnavailableError, StrategyPackageValidationError
from backend.tests.strategy_package.test_multi_alpha_base_schema import _single_manifest


RUN_ID = "macb_target_two_leg_20260627"
A1_LEG = "a1_plus3_LSTM_h20"
FUND_LEG = "new_FUNDGROWTH_h20"
A1_SEED = "qear_run_a1_seed_42"
FUND_SEED = "qe_new_FUNDGROWTH_L5"
PRED_SHA = "a" * 64
REPO_ROOT = Path(__file__).resolve().parents[3]


class FakeProvenanceResolver:
    def __init__(self, mapping: dict[str, SeedProvenance] | None = None) -> None:
        self.mapping = mapping if mapping is not None else _default_seed_provenance()
        self.calls: list[str] = []

    def resolve_seed(self, seed_ref: str) -> SeedProvenance:
        self.calls.append(seed_ref)
        return self.mapping.get(
            seed_ref,
            SeedProvenance(
                seed_ref=seed_ref,
                seed_ref_kind="unknown",
                resolved=False,
                resolve_method="unit_fake_missing",
                resolve_note="seed not registered in fake provenance resolver",
            ),
        )


class FakeQESourceResolver:
    def __init__(self, fail_on: set[str] | None = None) -> None:
        self.fail_on = fail_on or set()
        self.experiment_calls: list[str] = []
        self.loop_calls: list[tuple[str, str]] = []

    def build_from_experiment(self, experiment_id: str, *, resolve_runtime_assets: bool = False):
        self.experiment_calls.append(experiment_id)
        if experiment_id in self.fail_on:
            raise StrategyPackageValidationError("fake QE experiment unavailable", context={"experiment_id": experiment_id})
        return freeze_manifest(_single_manifest(f"auto_{experiment_id}", run_id=experiment_id))

    def build_from_evolution_loop(
        self,
        *,
        qe_task_id: str,
        qe_loop_id: str,
        resolve_runtime_assets: bool = False,
    ):
        self.loop_calls.append((qe_task_id, qe_loop_id))
        source_key = f"{qe_task_id}:{qe_loop_id}"
        if source_key in self.fail_on:
            raise StrategyPackageValidationError(
                "fake QE loop unavailable",
                context={"qe_task_id": qe_task_id, "qe_loop_id": qe_loop_id},
            )
        return freeze_manifest(_single_manifest(f"auto_{qe_task_id}_{qe_loop_id}", run_id=source_key))


class NoopFrozenRuntimeSelfCheck:
    def assert_manifest_self_contained(self, manifest):  # noqa: ANN001, ANN201
        return None


class FakeAssetFreezer:
    def freeze_manifest_assets(self, manifest):  # noqa: ANN001, ANN201
        frozen = manifest if _manifest_has_assets(manifest) else _with_frozen_assets(manifest, label=manifest.package_name)
        return SimpleNamespace(manifest=frozen, assets=_asset_records_from_manifest(frozen))


def _default_seed_provenance() -> dict[str, SeedProvenance]:
    return {
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
            source_loop_id="qe_new_FUNDGROWTH_L5",
            source_loop_index=5,
            source_run_type="qe_evolution_loop",
            source_run_id="qear_run_fundgrowth_seed_42",
        ),
    }


def _seed_child(repo: InMemoryStrategyPackageRepository, name: str, leg_id: str, seed_run_id: str):
    manifest = _single_manifest(name, run_id=seed_run_id)
    component = manifest.alpha_components[0].model_copy(
        update={
            "alpha_id": leg_id,
            "alpha_name": leg_id,
            "holding_period": "20d",
            "rebalance_frequency": "1day",
        }
    )
    manifest = manifest.model_copy(
        update={
            "alpha_components": [component],
            "alpha_combination_policy": AlphaCombinationPolicy(method="identity", weights={leg_id: 1.0}),
            "source_evidence": {"seed_run_ids": [seed_run_id]},
            "manifest_sha256": None,
        }
    )
    manifest = _with_frozen_assets(freeze_manifest(manifest), label=name)
    child = repo.save_manifest_with_assets(manifest, _asset_records_from_manifest(manifest))
    return child


def _with_frozen_assets(manifest, *, label: str):  # noqa: ANN001, ANN202
    factor_set = [
        factor.model_copy(
            update={
                "asset_ref": f"aistock-package-asset://blobs/{hashlib.sha256(f'{label}:{factor.factor_name}'.encode()).hexdigest()}?kind=factor_code",
                "sha256": hashlib.sha256(f"{label}:{factor.factor_name}".encode()).hexdigest(),
                "size_bytes": len(f"{label}:{factor.factor_name}".encode()),
                "source_uri": f"unit://factor/{factor.factor_name}.py",
            }
        )
        for factor in manifest.factor_set
    ]
    model_input = manifest.model_asset if isinstance(manifest.model_asset, list) else [manifest.model_asset]
    model_assets = []
    for model in model_input:
        model_payload = f"{label}:{model.model_id}:model".encode()
        model_assets.append(
            model.model_copy(
                update={
                    "asset_ref": f"aistock-package-asset://blobs/{hashlib.sha256(model_payload).hexdigest()}?kind=model_weight",
                    "sha256": hashlib.sha256(model_payload).hexdigest(),
                    "size_bytes": len(model_payload),
                    "source_uri": "unit://model/params.pkl",
                }
            )
        )
    model_asset = model_assets if isinstance(manifest.model_asset, list) else model_assets[0]
    return freeze_manifest(manifest.model_copy(update={"factor_set": factor_set, "model_asset": model_asset, "manifest_sha256": None}))


def _manifest_has_assets(manifest) -> bool:  # noqa: ANN001
    if not manifest.factor_set or any(not factor.asset_ref or not factor.sha256 for factor in manifest.factor_set):
        return False
    model_assets = manifest.model_asset if isinstance(manifest.model_asset, list) else [manifest.model_asset]
    return bool(model_assets) and all(asset.asset_ref and asset.sha256 for asset in model_assets)


def _asset_records_from_manifest(manifest) -> list[StrategyPackageAssetRecord]:  # noqa: ANN001
    rows: list[StrategyPackageAssetRecord] = []
    for factor in manifest.factor_set:
        rows.append(
            StrategyPackageAssetRecord(
                package_id=manifest.package_id,
                asset_type=StrategyPackageAssetType.FACTOR_CODE,
                asset_ref=factor.asset_ref,
                asset_sha256=factor.sha256,
                asset_size_bytes=factor.size_bytes,
                source_uri=factor.source_uri,
                metadata={"logical_name": factor.factor_name},
            )
        )
    model_assets = manifest.model_asset if isinstance(manifest.model_asset, list) else [manifest.model_asset]
    for model_asset in model_assets:
        rows.append(
            StrategyPackageAssetRecord(
                package_id=manifest.package_id,
                asset_type=StrategyPackageAssetType.MODEL_WEIGHT,
                asset_ref=model_asset.asset_ref,
                asset_sha256=model_asset.sha256,
                asset_size_bytes=model_asset.size_bytes,
                source_uri=model_asset.source_uri,
                metadata={"logical_name": model_asset.model_id},
            )
        )
    return rows


def _replace_child_with_valid_missing_seed(repo: InMemoryStrategyPackageRepository, child) -> None:
    manifest = freeze_manifest(
        child.manifest.model_copy(
            update={
                "run_id": "different_seed",
                "source_evidence": {"seed_run_ids": ["different_seed"]},
                "backtest_context": {},
                "manifest_sha256": None,
            }
        )
    )
    repo.records[child.package_id] = child.model_copy(
        update={"run_id": "different_seed", "manifest": manifest, "manifest_sha256": manifest.manifest_sha256}
    )


def _seed_repos():
    combine_repo = InMemoryCombineBacktestRepository()
    package_repo = InMemoryStrategyPackageRepository()
    child_a1 = _seed_child(package_repo, "a1", A1_LEG, A1_SEED)
    child_fund = _seed_child(package_repo, "fund", FUND_LEG, FUND_SEED)
    combine_repo.runs[RUN_ID] = {
        "id": RUN_ID,
        "roster_hash": "roster_hash",
        "roster_json": [
            {"leg_id": A1_LEG, "seed_run_ids": [A1_SEED], "metadata": {"family": "plus3"}},
            {"leg_id": FUND_LEG, "seed_run_ids": [FUND_SEED], "metadata": {"family": "fundgrowth"}},
        ],
        "oos_start": "2024-07-02",
        "oos_end": "2026-03-10",
        "normalize_method": "zscore",
        "walk_forward_json": {"enabled": True, "window": 60, "min_periods": 2},
        "backtest_config_json": {
            "stock_pool": "V25_1_SMALL_CAP",
            "filtered_pool": "filtered_pool_20260428",
            "label_horizon": 20,
            "execution_algo": "V25_1_SMALL_CAP",
            "n_drop": 2,
            "topk": 50,
        },
        "baseline_leg_id": A1_LEG,
        "status": "succeeded",
        "reason": None,
        "created_at": "2026-06-27T00:00:00+00:00",
        "updated_at": "2026-06-27T00:00:00+00:00",
    }
    combine_repo.scheme_results.append(
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
            "per_window_weights_json": [
                {"window_start": "2025-01-01", "window_end": "2025-03-31", A1_LEG: 0.61, FUND_LEG: 0.39}
            ],
            "cagr": 1.0715,
            "max_drawdown": -0.1651,
            "sharpe": 2.845,
            "calmar": 6.4886,
            "topk_return_20": 0.0631,
            "topk_hit_rate_20": 0.6471,
            "turnover": 19.2,
            "vs_baseline_sharpe_delta": 0.1,
            "vs_baseline_calmar_delta": 0.2,
            "pred_persisted": True,
            "skipped": False,
            "skipped_reason": None,
        }
    )
    return combine_repo, package_repo, child_a1, child_fund


def _seed_auto_repos():
    combine_repo, package_repo, _child_a1, _child_fund = _seed_repos()
    package_repo.records.clear()
    package_repo.events.clear()
    return combine_repo, package_repo


def _service(combine_repo, package_repo, *, provenance_resolver=None, source_resolver=None, prediction_ref_roots=None, asset_freezer=None):
    return MultiAlphaPackagePromotionService(
        combine_repository=combine_repo,
        package_repository=package_repo,
        provenance_resolver=provenance_resolver,
        source_resolver=source_resolver,
        asset_freezer=asset_freezer or FakeAssetFreezer(),
        frozen_runtime_self_check=NoopFrozenRuntimeSelfCheck(),
        prediction_ref_roots=prediction_ref_roots,
    )


def _weight_policy() -> dict[str, object]:
    return {
        "mode": "frozen_backtest_terminal_weights",
        "metric": "rank_ic",
        "lookback_trading_days": 252,
        "min_periods": 60,
        "label_horizon": 20,
        "label_maturity_lag_days": 20,
        "clip_negative_to_zero": True,
    }


def _request(child_a1, child_fund):
    return {
        "combine_backtest_run_id": RUN_ID,
        "weighting_scheme": "ic_weighted",
        "scheme_result_id": "scheme_icw_1",
        "topk": 50,
        "secondary_topk": [25],
        "package_name": "MA2_a1_plus3_LSTM_new_FUNDGROWTH_icw_h20",
        "component_package_ids": {A1_LEG: child_a1.package_id, FUND_LEG: child_fund.package_id},
        "weight_policy": _weight_policy(),
        "confirmation": MULTI_ALPHA_PACKAGE_PROMOTE_CONFIRMATION,
    }


def _auto_request():
    return {
        "combine_backtest_run_id": RUN_ID,
        "weighting_scheme": "ic_weighted",
        "scheme_result_id": "scheme_icw_1",
        "topk": 50,
        "secondary_topk": [25],
        "package_name": "MA2_a1_plus3_LSTM_new_FUNDGROWTH_icw_h20",
        "weight_policy": _weight_policy(),
        "confirmation": MULTI_ALPHA_PACKAGE_PROMOTE_CONFIRMATION,
    }


def _promote(service, child_a1, child_fund, **overrides):
    payload = _request(child_a1, child_fund)
    payload.update(overrides)
    return service.promote_from_combine_run(**payload)


def _strip_explicit_prediction_ref(combine_repo: InMemoryCombineBacktestRepository) -> None:
    weights_json = dict(combine_repo.scheme_results[0]["weights_json"])
    weights_json.pop("combined_prediction_ref", None)
    weights_json.pop("prediction_ref", None)
    combine_repo.scheme_results[0]["weights_json"] = weights_json
    combine_repo.scheme_results[0].pop("combined_prediction_ref", None)
    combine_repo.scheme_results[0].pop("prediction_ref", None)
    combine_repo.scheme_results[0].pop("combined_prediction_ref_uri", None)
    combine_repo.scheme_results[0].pop("combined_prediction_ref_sha256", None)
    combine_repo.scheme_results[0]["pred_persisted"] = False


def _reason_code(exc: BaseException) -> str | None:
    return getattr(exc, "context", {}).get("reason_code")


def test_promote_target_two_leg_run_freezes_deterministic_multi_alpha_package() -> None:
    combine_repo, package_repo, child_a1, child_fund = _seed_repos()
    service = _service(combine_repo, package_repo)

    first = _promote(service, child_a1, child_fund)
    second = _promote(service, child_a1, child_fund)

    assert first.package.package_id == second.package.package_id
    assert first.package.manifest_sha256 == second.package.manifest_sha256
    assert first.package.alpha_mode == AlphaMode.MULTI_ALPHA
    assert first.package.package_status == PackageStatus.ASSET_VALIDATED
    assert first.package.package_status != PackageStatus.PAPER_ENABLED
    assert first.paper_admission == {"eligible": False, "blocking": [MULTI_ALPHA_PAPER_ADMISSION_BLOCKER]}
    assert first.package.prediction_ref_uri == f"aistock-prediction-store://multi-alpha/{RUN_ID}/combined_prediction.pkl"
    assert first.package.prediction_ref_sha256 == PRED_SHA
    manifest = first.package.manifest
    assert manifest.alpha_combination_policy.method == "ic_weighted"
    assert manifest.source.source_type == SourceType.MULTI_ALPHA_COMBINE_RUN
    assert manifest.source.source_id == RUN_ID
    assert manifest.source.run_id == RUN_ID
    assert manifest.source_evidence["multi_alpha"]["combine_backtest_run_id"] == RUN_ID
    assert manifest.source_evidence["multi_alpha"]["paper_admission"]["blocking"] == [MULTI_ALPHA_PAPER_ADMISSION_BLOCKER]
    assert manifest.backtest_context["daily_strategy"]["topk"] == 50
    assert manifest.backtest_context["daily_strategy"]["secondary_topk"] == [25]
    assert sorted(component.child_package_id for component in first.components) == sorted(
        [child_a1.package_id, child_fund.package_id]
    )
    eligibility = service.package_repository.get(first.package.package_id)
    assert eligibility.package_status == PackageStatus.ASSET_VALIDATED


def test_prediction_ref_can_fall_back_to_local_combine_workspace(tmp_path: Path) -> None:
    combine_repo, package_repo, child_a1, child_fund = _seed_repos()
    _strip_explicit_prediction_ref(combine_repo)
    prediction_file = tmp_path / RUN_ID / "combined_ic_weighted" / "combined_prediction.pkl"
    prediction_payload = b"unit-test-combined-prediction"
    prediction_file.parent.mkdir(parents=True)
    prediction_file.write_bytes(prediction_payload)
    expected_sha = hashlib.sha256(prediction_payload).hexdigest()

    result = _promote(
        _service(combine_repo, package_repo, prediction_ref_roots=[tmp_path]),
        child_a1,
        child_fund,
    )

    assert result.package.prediction_ref_uri == prediction_file.resolve().as_uri()
    assert result.package.prediction_ref_sha256 == expected_sha
    evidence = result.package.current_manifest().source_evidence["multi_alpha"]
    assert evidence["combined_prediction_ref_source"] == "combine_backtest_local_workspace"
    assert evidence["combined_prediction_ref_sha256"] == expected_sha


def test_missing_local_prediction_ref_fails_loud_without_parent_half_package(tmp_path: Path) -> None:
    combine_repo, package_repo, child_a1, child_fund = _seed_repos()
    _strip_explicit_prediction_ref(combine_repo)

    with pytest.raises(StrategyPackageValidationError) as excinfo:
        _promote(
            _service(combine_repo, package_repo, prediction_ref_roots=[tmp_path]),
            child_a1,
            child_fund,
        )

    assert _reason_code(excinfo.value) == "multi_alpha_prediction_ref_missing"
    attempted_paths = excinfo.value.context["attempted_local_prediction_paths"]
    assert str(tmp_path / RUN_ID / "combined_ic_weighted" / "combined_prediction.pkl") in attempted_paths
    assert not any(record.alpha_mode == AlphaMode.MULTI_ALPHA for record in package_repo.records.values())
    assert package_repo.components == {}


def test_backtest_config_strategy_field_fills_stock_pool_and_execution_algo() -> None:
    combine_repo, package_repo, child_a1, child_fund = _seed_repos()
    combine_repo.runs[RUN_ID]["backtest_config_json"] = {
        "strategy": "V25_1_SMALL_CAP",
        "filtered_pool": "filtered_pool_20260428",
        "label_horizon": 20,
        "n_drop": 2,
        "topk": 50,
    }

    result = _promote(_service(combine_repo, package_repo), child_a1, child_fund)

    context = result.package.current_manifest().backtest_context
    assert context["universe"]["stock_pool"] == "V25_1_SMALL_CAP"
    assert context["execution"]["execution_algo"] == "V25_1_SMALL_CAP"


def test_auto_path_materializes_components_and_is_idempotent() -> None:
    combine_repo, package_repo = _seed_auto_repos()
    provenance_resolver = FakeProvenanceResolver()
    source_resolver = FakeQESourceResolver()
    service = _service(
        combine_repo,
        package_repo,
        provenance_resolver=provenance_resolver,
        source_resolver=source_resolver,
    )

    first = service.promote_from_combine_run(**_auto_request())
    second = service.promote_from_combine_run(**_auto_request())

    assert first.package.package_id == second.package.package_id
    assert first.package.manifest_sha256 == second.package.manifest_sha256
    assert first.package.manifest.source.source_type == SourceType.MULTI_ALPHA_COMBINE_RUN
    assert first.package.manifest.source.source_id == RUN_ID
    assert first.package.manifest.source.run_id == RUN_ID
    assert sorted(component.child_package_id for component in first.components) == sorted(
        component.child_package_id for component in second.components
    )
    assert sorted(item["mode"] for item in first.auto_component_materialization) == [
        "auto_created_component_package",
        "auto_created_component_package",
    ]
    assert sorted(item["mode"] for item in second.auto_component_materialization) == [
        "reused_existing_component_package",
        "reused_existing_component_package",
    ]
    assert len([record for record in package_repo.records.values() if record.alpha_mode == AlphaMode.SINGLE_ALPHA]) == 2
    assert len([record for record in package_repo.records.values() if record.alpha_mode == AlphaMode.MULTI_ALPHA]) == 1
    for component in first.components:
        child = package_repo.get(component.child_package_id)
        assert child.source_type == SourceType.MULTI_ALPHA_COMBINE_RUN.value
        assert child.source_id == RUN_ID
        assert child.current_manifest().source_evidence["multi_alpha_component"]["combine_backtest_run_id"] == RUN_ID
    assert source_resolver.experiment_calls == ["qe_exp_a1_seed_42"]
    assert source_resolver.loop_calls == [("qe_new_FUNDGROWTH", "Loop5")]
    for record in package_repo.records.values():
        if record.alpha_mode == AlphaMode.SINGLE_ALPHA:
            assert package_repo.list_package_assets(record.package_id)
            assert all(factor.asset_ref and factor.sha256 for factor in record.current_manifest().factor_set)


def test_explicit_unfrozen_child_is_rejected() -> None:
    combine_repo, package_repo, child_a1, child_fund = _seed_repos()
    legacy_manifest = _single_manifest("legacy", run_id=A1_SEED)
    legacy_record = package_repo.save_manifest(freeze_manifest(legacy_manifest))
    package_repo.records[child_a1.package_id] = legacy_record.model_copy(update={"package_id": child_a1.package_id})

    with pytest.raises(StrategyPackageValidationError) as excinfo:
        _promote(_service(combine_repo, package_repo), child_a1, child_fund)

    assert _reason_code(excinfo.value) == "multi_alpha_child_package_assets_unfrozen"


def test_auto_path_unresolved_seed_fails_without_half_package() -> None:
    combine_repo, package_repo = _seed_auto_repos()
    resolver = FakeProvenanceResolver(
        {
            **_default_seed_provenance(),
            FUND_SEED: SeedProvenance(
                seed_ref=FUND_SEED,
                seed_ref_kind="evolution_loop_id",
                resolved=False,
                resolve_method="unit_fake_missing_loop",
                resolve_note="qe_evolution_loops row missing",
            ),
        }
    )

    with pytest.raises(StrategyPackageValidationError) as excinfo:
        _service(
            combine_repo,
            package_repo,
            provenance_resolver=resolver,
            source_resolver=FakeQESourceResolver(),
        ).promote_from_combine_run(**_auto_request())

    assert _reason_code(excinfo.value) == "multi_alpha_seed_unresolved"
    assert package_repo.records == {}
    assert package_repo.components == {}


def test_auto_path_materialization_failure_fails_without_half_package() -> None:
    combine_repo, package_repo = _seed_auto_repos()

    with pytest.raises(StrategyPackageValidationError) as excinfo:
        _service(
            combine_repo,
            package_repo,
            provenance_resolver=FakeProvenanceResolver(),
            source_resolver=FakeQESourceResolver(fail_on={"qe_new_FUNDGROWTH:Loop5"}),
        ).promote_from_combine_run(**_auto_request())

    assert _reason_code(excinfo.value) == "multi_alpha_component_auto_materialize_failed"
    assert package_repo.records == {}
    assert package_repo.components == {}


def test_explicit_child_manifest_sha_drift_is_rejected() -> None:
    combine_repo, package_repo, child_a1, child_fund = _seed_repos()
    package_repo.records[child_a1.package_id] = child_a1.model_copy(
        update={
            "manifest": child_a1.manifest.model_copy(
                update={"source_evidence": {"seed_run_ids": ["different_seed"]}}
            )
        }
    )

    with pytest.raises(StrategyPackageValidationError) as excinfo:
        _promote(_service(combine_repo, package_repo), child_a1, child_fund)

    assert _reason_code(excinfo.value) == "multi_alpha_child_package_not_frozen"


def test_schema_allows_multi_alpha_combine_source_type() -> None:
    init_ddl = "\n".join(init_trading_core_v2_schema.iter_ddl())
    snapshot_ddl = (REPO_ROOT / "backend/migrations/trading_core_v2_schema.sql").read_text(encoding="utf-8")
    migration_ddl = (
        REPO_ROOT / "backend/migrations/strategy_pkg_multi_alpha_combine_source_type_20260629.sql"
    ).read_text(encoding="utf-8")

    for ddl in (init_ddl, snapshot_ddl, migration_ddl):
        assert "'multi_alpha_combine_run'" in ddl
    assert "package_source_type_check" in init_ddl
    assert "package_source_type_check" in migration_ddl


@pytest.mark.parametrize(
    ("mutator", "expected_reason_code"),
    [
        (lambda repos, children: repos[1].records.pop(children[0].package_id), "multi_alpha_child_package_missing"),
        (
            lambda repos, children: repos[1].records.__setitem__(
                children[0].package_id,
                repos[1].records[children[0].package_id].model_copy(update={"manifest_sha256": ""}),
            ),
            "multi_alpha_child_package_not_frozen",
        ),
        (lambda repos, children: repos[0].scheme_results.clear(), "multi_alpha_scheme_not_succeeded"),
        (
            lambda repos, children: repos[0].runs[RUN_ID].__setitem__(
                "roster_json",
                [{"leg_id": A1_LEG, "seed_run_ids": [A1_SEED]}, {"leg_id": "unexpected_leg", "seed_run_ids": ["seed"]}],
            ),
            "multi_alpha_roster_mismatch",
        ),
        (
            lambda repos, children: _replace_child_with_valid_missing_seed(repos[1], children[0]),
            "multi_alpha_roster_mismatch",
        ),
        (
            lambda repos, children: repos[0].scheme_results[0].__setitem__(
                "weights_json",
                {A1_LEG: 0.61, FUND_LEG: 0.39},
            ),
            "multi_alpha_prediction_ref_missing",
        ),
        (
            lambda repos, children: repos[0].scheme_results[0].__setitem__("sharpe", 0.1),
            "multi_alpha_metrics_below_gate",
        ),
    ],
)
def test_promote_fails_loud_with_reason_codes(mutator, expected_reason_code) -> None:
    combine_repo, package_repo, child_a1, child_fund = _seed_repos()
    mutator((combine_repo, package_repo), (child_a1, child_fund))

    with pytest.raises((StrategyPackageValidationError, DataUnavailableError)) as excinfo:
        _promote(
            _service(combine_repo, package_repo),
            child_a1,
            child_fund,
            promotion_gate={"min_sharpe": 2.0},
        )

    assert _reason_code(excinfo.value) == expected_reason_code


def test_promote_rejects_live_rolling_weight_policy_in_p0() -> None:
    combine_repo, package_repo, child_a1, child_fund = _seed_repos()

    with pytest.raises(StrategyPackageValidationError) as excinfo:
        _promote(
            _service(combine_repo, package_repo),
            child_a1,
            child_fund,
            weight_policy={"mode": "live_rolling_ic_weighted", "metric": "rank_ic"},
        )

    assert _reason_code(excinfo.value) == "multi_alpha_manifest_incomplete"
    assert excinfo.value.context["weight_policy_mode"] == "live_rolling_ic_weighted"


def test_asset_eligibility_warns_multi_alpha_localsim_without_dry_run_and_blocks_minqmt() -> None:
    combine_repo, package_repo, child_a1, child_fund = _seed_repos()
    service = _service(combine_repo, package_repo)
    result = _promote(service, child_a1, child_fund)

    eligibility = service.component_service.repository.get(result.package.package_id)
    eligibility_service = router_module.StrategyPackageAssetEligibilityService()
    local_summary = eligibility_service.summarize(eligibility, broker_backend="local_sim")
    minqmt_summary = eligibility_service.summarize(eligibility, broker_backend="minqmt_sim")

    assert local_summary.eligible is True
    assert MULTI_ALPHA_PAPER_ADMISSION_BLOCKER not in local_summary.blockers
    assert MULTI_ALPHA_PAPER_ADMISSION_BLOCKER in local_summary.warnings
    local_check = next(check for check in local_summary.checks if check.name == MULTI_ALPHA_PAPER_ADMISSION_BLOCKER)
    assert local_check.status == "WARN"
    assert local_check.context["reason_code"] == MULTI_ALPHA_LOCALSIM_DRY_RUN_NOT_REQUIRED
    assert local_check.context["original_blocker"] == MULTI_ALPHA_PAPER_ADMISSION_BLOCKER

    assert minqmt_summary.eligible is False
    assert MULTI_ALPHA_PAPER_ADMISSION_BLOCKER in minqmt_summary.blockers
    minqmt_check = next(check for check in minqmt_summary.checks if check.name == MULTI_ALPHA_PAPER_ADMISSION_BLOCKER)
    assert minqmt_check.status == "FAIL"
    assert minqmt_check.context["reason_code"] == MULTI_ALPHA_PAPER_ADMISSION_BLOCKER


def test_router_endpoint_promotes_and_maps_loud_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    combine_repo, package_repo, child_a1, child_fund = _seed_repos()

    def _factory(*args, **kwargs):  # noqa: ANN001
        return MultiAlphaPackagePromotionService(
            combine_repository=combine_repo,
            package_repository=package_repo,
            asset_freezer=FakeAssetFreezer(),
            frozen_runtime_self_check=NoopFrozenRuntimeSelfCheck(),
        )

    monkeypatch.setattr(router_module, "MultiAlphaPackagePromotionService", _factory)
    app = FastAPI()
    app.include_router(router_module.router)
    client = TestClient(app)

    response = client.post("/strategy-packages/from-multi-alpha-combine-run", json=_request(child_a1, child_fund))

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["ok"] is True
    assert payload["alpha_mode"] == AlphaMode.MULTI_ALPHA.value
    assert payload["paper_admission"]["blocking"] == [MULTI_ALPHA_PAPER_ADMISSION_BLOCKER]

    bad_request = deepcopy(_request(child_a1, child_fund))
    bad_request["combine_backtest_run_id"] = "missing_run"
    failure = client.post("/strategy-packages/from-multi-alpha-combine-run", json=bad_request)

    assert failure.status_code == 404, failure.text
    detail = failure.json()["detail"]
    assert detail["context"]["reason_code"] == "multi_alpha_combine_run_missing"


@pytest.mark.parametrize(
    ("mutator", "expected_reason_code", "expected_status"),
    [
        (lambda repos, children: repos[1].records.pop(children[0].package_id), "multi_alpha_child_package_missing", 404),
        (lambda repos, children: repos[0].scheme_results.clear(), "multi_alpha_scheme_not_succeeded", 400),
        (
            lambda repos, children: repos[0].runs[RUN_ID].__setitem__(
                "roster_json",
                [{"leg_id": A1_LEG, "seed_run_ids": [A1_SEED]}, {"leg_id": "unexpected_leg", "seed_run_ids": ["seed"]}],
            ),
            "multi_alpha_roster_mismatch",
            400,
        ),
        (
            lambda repos, children: _replace_child_with_valid_missing_seed(repos[1], children[0]),
            "multi_alpha_roster_mismatch",
            400,
        ),
    ],
)
def test_router_endpoint_negative_paths_are_loud(
    monkeypatch: pytest.MonkeyPatch,
    mutator,
    expected_reason_code: str,
    expected_status: int,
) -> None:
    combine_repo, package_repo, child_a1, child_fund = _seed_repos()
    mutator((combine_repo, package_repo), (child_a1, child_fund))

    def _factory(*args, **kwargs):  # noqa: ANN001
        return MultiAlphaPackagePromotionService(
            combine_repository=combine_repo,
            package_repository=package_repo,
            frozen_runtime_self_check=NoopFrozenRuntimeSelfCheck(),
        )

    monkeypatch.setattr(router_module, "MultiAlphaPackagePromotionService", _factory)
    app = FastAPI()
    app.include_router(router_module.router)

    response = TestClient(app).post("/strategy-packages/from-multi-alpha-combine-run", json=_request(child_a1, child_fund))

    assert response.status_code == expected_status, response.text
    detail = response.json()["detail"]
    assert detail["context"]["reason_code"] == expected_reason_code
