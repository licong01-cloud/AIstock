"""Real seed assets and inference must match the historical ensemble roster."""

from copy import deepcopy
from dataclasses import replace
from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.strategy_package.frozen_runtime_self_check import FrozenRuntimeSelfCheckService
from backend.services.strategy_package.multi_alpha_live import (
    MultiAlphaLivePredictionProvider,
    _ensemble_seed_frames,
    _multi_alpha_evidence,
    _parent_leg_runtime_slices,
)
from backend.services.strategy_package.package_asset_backfill import PackageAssetBackfillService, STATUS_UNRECOVERABLE
from backend.services.strategy_package.package_asset_freeze import (
    PackageAssetBytes,
    PackageAssetFreezeService,
    manifest_has_frozen_runtime_assets,
)
from backend.services.strategy_package.package_asset_store import LocalPackageAssetStore
from backend.services.trading_core.errors import StrategyPackageValidationError, TradingCoreError
from backend.tests.strategy_package.test_multi_alpha_live_selection import FakeProvider, FakeResolver, TRADE_DATE
from backend.tests.strategy_package.test_multi_alpha_promotion import (
    A1_LEG,
    A1_SEED,
    RUN_ID,
    FakeAssetFreezer,
    FakeQESourceResolver,
    _asset_records_from_manifest,
    _promote,
    _seed_repos,
    _service,
    _single_manifest_for_parent_leg,
)


def _multi_seed_service():
    combine, packages, child_a, child_b = _seed_repos()
    service = _service(combine, packages)

    class IdempotentFreezer(FakeAssetFreezer):
        def freeze_manifest_assets(self, manifest):
            if manifest_has_frozen_runtime_assets(manifest):
                return SimpleNamespace(manifest=manifest, assets=_asset_records_from_manifest(manifest))
            return super().freeze_manifest_assets(manifest)

    service.asset_freezer = IdempotentFreezer()
    second = f"{A1_SEED}_314"
    service.provenance_resolver.mapping[second] = replace(
        service.provenance_resolver.mapping[A1_SEED],
        seed_ref=second,
        source_experiment_id="qe_exp_a1_seed_314",
        source_run_id=second,
    )
    combine.runs[RUN_ID]["roster_json"][0]["seed_run_ids"].append(second)
    return service, child_a, child_b, second


def test_promotion_freezes_each_seed_model_and_its_feature_contract():
    service, child_a, child_b, second = _multi_seed_service()
    manifest = _promote(service, child_a, child_b).package.current_manifest()
    assert len(manifest.model_asset) == 3
    leg = manifest.source_evidence["multi_alpha"]["legs"][0]
    assert leg["leg_id"] == A1_LEG
    assert [seed["seed_run_id"] for seed in leg["seed_assets"]] == [A1_SEED, second]
    models = {model.model_id: model for model in manifest.model_asset}
    for seed in leg["seed_assets"]:
        model = models[seed["model_id"]]
        assert (seed["asset_ref"], seed["sha256"]) == (model.asset_ref, model.sha256)
        assert seed["factor_artifact_refs"]
        assert "alpha158" in seed["runtime_assets"]
    assert len({seed["sha256"] for seed in leg["seed_assets"]}) == 2


def test_seed_ensemble_keeps_the_historical_outer_union():
    day = date(2024, 7, 2)
    frames = {
        "42": pd.DataFrame({"trade_date": [day, day], "instrument": ["A", "B"], "score": [2.0, 4.0]}),
        "314": pd.DataFrame({"trade_date": [day, day], "instrument": ["A", "C"], "score": [6.0, 8.0]}),
    }
    actual = _ensemble_seed_frames(frames, leg_id="leg", model_id="m", package_id="pkg", trade_date=day)
    assert actual.set_index("instrument")["score"].to_dict() == {"A": 4.0, "B": 4.0, "C": 8.0}


@pytest.mark.parametrize("seed_ids", [(A1_SEED, A1_SEED), ()])
def test_invalid_seed_roster_never_materializes_a_partial_ensemble(seed_ids):
    service, _, _, _ = _multi_seed_service()
    with pytest.raises(StrategyPackageValidationError, match="nonempty and unique"):
        service._prepare_parent_leg_asset_plan(
            leg_id=A1_LEG,
            seed_run_ids=seed_ids,
            terminal_weight=0.61,
            run_id=RUN_ID,
        )


def _promoted():
    service, child_a, child_b, second = _multi_seed_service()
    record = _promote(service, child_a, child_b).package
    return service, record, second


def test_live_inference_runs_both_fitted_seeds_not_a_representative_copy():
    _, record, second = _promoted()
    manifest = record.current_manifest()
    leg = _parent_leg_runtime_slices(manifest, evidence=_multi_alpha_evidence(manifest), package_id=record.package_id)[
        0
    ]

    class SeedProvider(FakeProvider):
        def run(self, **kwargs):
            self.scores_by_leg[A1_LEG] = [
                {"symbol": "000001.SZ", "score": 3.0 if kwargs["workspace"].seed_run_id == A1_SEED else 9.0}
            ]
            return super().run(**kwargs)

    resolver = FakeResolver()
    provider = SeedProvider({})
    live = MultiAlphaLivePredictionProvider(
        package_repository=None,
        artifact_repository=None,
        runtime_asset_resolver=resolver,
        live_inference_provider=provider,
    )
    result = live._run_parent_leg_live_inference(
        manifest=manifest,
        leg_slice=leg,
        trade_date=TRADE_DATE,
        cutoff_date=None,
        runtime_config={},
        inference_backend="local",
    )
    assert [call["workspace"].seed_run_id for call in provider.calls] == [A1_SEED, second]
    assert [frame["score"].iloc[0] for frame in result.seed_frames.values()] == [3.0, 9.0]
    assert result.live_result.scores == [{"symbol": "000001.SZ", "score": 6.0}]
    assert len(result.live_result.source_read_receipts) == 8
    assert set(result.live_result.input_context["seed_input_context"]["per_leg_window_lineage"]) == {A1_SEED, second}
    assert len({call["model_asset"].sha256 for call in resolver.load_calls}) == 2
    assert len({call["cache_namespace"] for call in resolver.prepare_calls}) == 2


@pytest.mark.parametrize("damage", ["missing", "duplicate", "wrong_sha", "wrong_factor", "wrong_runtime", "extra"])
def test_seed_binding_damage_is_not_accepted_as_a_valid_frozen_ensemble(damage):
    _, record, _ = _promoted()
    manifest = record.current_manifest()
    evidence = deepcopy(manifest.source_evidence)
    leg = evidence["multi_alpha"]["legs"][0]
    if damage == "missing":
        del leg["seed_assets"]
    elif damage == "duplicate":
        leg["seed_assets"][1] = deepcopy(leg["seed_assets"][0])
    elif damage == "wrong_sha":
        leg["seed_assets"][1]["sha256"] = "0" * 64
    elif damage == "wrong_factor":
        leg["seed_assets"][1]["factor_artifact_refs"] = ["absent"]
    elif damage == "wrong_runtime":
        leg["seed_assets"][1]["runtime_assets"]["alpha158"]["sha256"] = "0" * 64
    else:
        leg["seed_assets"].append(deepcopy(leg["seed_assets"][1]))
    changed = manifest.model_copy(update={"source_evidence": evidence})
    with pytest.raises(TradingCoreError):
        _parent_leg_runtime_slices(changed, evidence=_multi_alpha_evidence(changed), package_id=record.package_id)


def test_self_check_probes_every_seed_feature_contract(tmp_path):
    _, record, _ = _promoted()

    class Resolver:
        def __init__(self):
            self.model_ids = []

        def load_source_for_strategy_package_leg(self, **kwargs):
            self.model_ids.append(kwargs["model_asset"].model_id)
            return SimpleNamespace(
                model_params_origin="package_asset",
                source_workspace_type="strategy_package_asset_store",
                factors=kwargs["factor_set"],
            )

        def prepare_workspace(self, **kwargs):
            factors = [factor.factor_id for factor in kwargs["source"].factors]
            return SimpleNamespace(
                model_params_path=tmp_path / "params.pkl",
                dynamic_factors=factors,
                alpha158_factors=["RESI5"],
                factor_order=[*factors, "RESI5"],
            )

    resolver = Resolver()
    probe_calls = []

    def probe(path):
        probe_calls.append(path)
        return None, "unit", None, 3

    result = FrozenRuntimeSelfCheckService(
        runtime_asset_resolver=resolver, model_loader=probe
    ).assert_manifest_self_contained(record.current_manifest())
    assert len(probe_calls) == len(set(resolver.model_ids)) == 3
    assert result.model_expected_features == result.factor_order_count == 9
    assert len(result.leg_results[A1_LEG]["seed_checks"]) == 2


def test_backfill_does_not_skip_a_legacy_representative_only_ensemble():
    service, record, _ = _promoted()
    manifest = record.current_manifest()
    evidence = deepcopy(manifest.source_evidence)
    del evidence["multi_alpha"]["legs"][0]["seed_assets"]
    invalid = manifest.model_copy(update={"source_evidence": evidence})
    backfill = PackageAssetBackfillService(
        repository=service.package_repository,
        asset_freezer=service.asset_freezer,
        frozen_runtime_self_check=service.frozen_runtime_self_check,
    )
    result = backfill._plan_freeze(record, desired_manifest=invalid)
    assert result.status == STATUS_UNRECOVERABLE
    assert result.reason_code == "multi_alpha_parent_leg_runtime_assets_incomplete"


def test_legacy_single_seed_parent_keeps_its_asset_identity():
    combine, packages, child_a, child_b = _seed_repos()
    record = _promote(_service(combine, packages), child_a, child_b).package
    manifest = record.current_manifest()
    slices = _parent_leg_runtime_slices(
        manifest, evidence=_multi_alpha_evidence(manifest), package_id=record.package_id
    )
    assert len(slices) == 2
    assert all(len(leg.seed_assets) == 1 and leg.seed_assets[0].model_asset == leg.model_asset for leg in slices)


def test_real_cas_retains_same_model_name_seeds_as_distinct_fitted_assets(tmp_path):
    service, child_a, child_b, _ = _multi_seed_service()

    class SharedModelNameResolver(FakeQESourceResolver):
        def build_from_experiment(self, experiment_id, **kwargs):
            return _single_manifest_for_parent_leg("shared_model", run_id=experiment_id)

    service.source_resolver = SharedModelNameResolver()
    store = LocalPackageAssetStore(tmp_path / "assets")
    service.asset_freezer = PackageAssetFreezeService(
        asset_store=store,
        conf_yaml_reader=lambda manifest: PackageAssetBytes(b"task: {}\n", "unit://conf"),
        model_params_reader=lambda manifest: PackageAssetBytes(manifest.package_id.encode(), "unit://model"),
        factor_code_reader=lambda factor, manifest: PackageAssetBytes(
            f"# {factor.factor_id}\n".encode(), "unit://factor"
        ),
    )
    record = _promote(service, child_a, child_b).package
    manifest = record.current_manifest()
    slices = _parent_leg_runtime_slices(
        manifest, evidence=_multi_alpha_evidence(manifest), package_id=record.package_id
    )
    seeds = slices[0].seed_assets
    assert len({seed.model_asset.model_id for seed in seeds}) == 2
    assert len({seed.model_asset.sha256 for seed in seeds}) == 2
    assert seeds[0].factor_set == seeds[1].factor_set
    for model in manifest.model_asset:
        assert (
            store.verify(model.asset_ref, sha256=model.sha256, size_bytes=model.size_bytes).size_bytes
            == model.size_bytes
        )
    assert len(service.package_repository.list_package_assets(record.package_id)) == 7
