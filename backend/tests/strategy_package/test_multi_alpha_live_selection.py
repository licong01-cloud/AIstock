from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from backend.services.strategy_package.live_inference import (
    AUTHORITATIVE_SELECTION_SCOPE,
    DIAGNOSTIC_BACKTEST_SCOPE,
    DIAGNOSTIC_BACKTEST_SOURCE_TYPE,
    LiveInferenceResult,
)
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.multi_alpha_live import (
    LIVE_MULTI_ALPHA_SELECTION_SOURCE_TYPE,
    REASON_COMPONENT_COVERAGE_LOW,
    REASON_DEADLINE_EXCEEDED,
    REASON_LABEL_WINDOW_INSUFFICIENT,
    REASON_PARENT_ALPHA158_SCHEMA_MISSING,
    REASON_PARENT_LEG_FACTOR_REFS_MISSING,
    REASON_PARENT_LEG_INFERENCE_EMPTY,
    REASON_PARENT_LEG_MODEL_ASSET_MISSING,
    REASON_PREDICTION_NOT_AUTHORITATIVE,
    REASON_RUNTIME_NOT_ENABLED,
    REASON_TOPK_RUNTIME_MISMATCH,
    REASON_WEIGHT_ALL_NON_POSITIVE,
    REASON_WEIGHT_UNAVAILABLE,
    MultiAlphaLivePredictionProvider,
    MultiAlphaWeightService,
)
from backend.services.strategy_package.models import RuntimeAssetManifest
from backend.services.strategy_package.runtime import StrategyPackageRuntime
from backend.services.strategy_package.selection_artifact import (
    InMemorySelectionScoreArtifactRepository,
    SelectionScoreArtifact,
    StrategyPackageSelectionArtifactService,
)
from backend.services.trading_core.errors import DataUnavailableError, RuntimeConfigInvalidError, UnsupportedFeatureError
from backend.tests.strategy_package.test_multi_alpha_promotion import (
    A1_LEG,
    A1_SEED,
    FUND_LEG,
    FUND_SEED,
    _promote,
    _seed_repos,
    _service,
)


TRADE_DATE = date(2024, 7, 2)
TRADING_DAYS = [date(2024, 5, 1) + timedelta(days=offset) for offset in range(63)]


class FakeResolver:
    def __init__(self) -> None:
        self.load_calls: list[dict] = []
        self.legacy_load_calls: list[dict] = []
        self.prepare_calls: list[dict] = []

    def load_source_for_strategy_package(self, **kwargs):  # noqa: ANN001, ANN201
        self.legacy_load_calls.append(kwargs)
        raise AssertionError("multi-alpha parent runtime must not call legacy child/package source loader")

    def load_source_for_strategy_package_leg(self, **kwargs):  # noqa: ANN001, ANN201
        self.load_calls.append(kwargs)
        return SimpleNamespace(
            experiment_id=f"{kwargs['package_id']}:{kwargs['leg_id']}",
            model_params_origin="package_asset",
            source_workspace_type="strategy_package_asset_store",
            leg_id=kwargs["leg_id"],
        )

    def prepare_workspace(self, **kwargs):  # noqa: ANN001, ANN201
        self.prepare_calls.append(kwargs)
        artifact_config = kwargs["runtime_config"]["selection_artifact_config"]
        return SimpleNamespace(
            workspace_path=f"workspace/{kwargs['package_id']}/{artifact_config['multi_alpha_leg_id']}",
            seed_run_id=artifact_config["multi_alpha_seed_run_id"],
            leg_id=artifact_config["multi_alpha_leg_id"],
        )


class FakeProvider:
    backend_name = "fake_live"

    def __init__(self, scores_by_leg: dict[str, list[dict]]) -> None:
        self.scores_by_leg = scores_by_leg
        self.calls: list[dict] = []

    def run(self, **kwargs):  # noqa: ANN001, ANN201
        self.calls.append(kwargs)
        leg_id = kwargs["workspace"].leg_id
        return LiveInferenceResult(
            scores=deepcopy(self.scores_by_leg.get(leg_id, [])),
            metadata={"leg_id": leg_id, "seed_run_id": kwargs["workspace"].seed_run_id},
        )


class ChildFailingRepository:
    def __init__(self, delegate) -> None:  # noqa: ANN001
        self.delegate = delegate
        self.get_calls: list[str] = []

    def get(self, package_id: str):  # noqa: ANN201
        self.get_calls.append(package_id)
        if str(package_id).startswith("pkg_mac"):
            raise AssertionError("multi-alpha parent runtime attempted to read legacy child package")
        return self.delegate.get(package_id)

    def __getattr__(self, name: str):  # noqa: ANN204
        return getattr(self.delegate, name)


def _score_rows(*, reverse: bool = False, count: int = 60) -> list[dict]:
    rows = []
    for idx in range(count):
        symbol = f"{idx + 1:06d}.SZ"
        score = float(count - idx if not reverse else idx + 1)
        rows.append({"symbol": symbol, "score": score, "rank": idx + 1})
    return rows


def _runtime_config(*, top_k: int = 25, extra_artifact: dict | None = None) -> dict:
    artifact = {
        "multi_alpha_live_inference_enabled": True,
        "component_coverage_threshold": top_k,
        "trading_days": [item.isoformat() for item in TRADING_DAYS],
    }
    artifact.update(extra_artifact or {})
    return {
        "runtime_profile": {"selection": {"top_k": top_k}},
        "selection_artifact_config": artifact,
    }


def _live_weight_history(*, a1_value: float = 0.12, fund_value: float = 0.04, samples: int = 3) -> list[dict]:
    label_dates = [date(2024, 5, 3), date(2024, 5, 4), date(2024, 5, 5), date(2024, 5, 6)]
    rows = []
    for label_date in label_dates[:samples]:
        rows.append({"leg_id": A1_LEG, "label_date": label_date.isoformat(), "rank_ic": a1_value})
        rows.append({"leg_id": FUND_LEG, "label_date": label_date.isoformat(), "rank_ic": fund_value})
    return rows


def _make_parent(*, live_weight_policy: bool = True):
    _combine_repo, package_repo, child_a1, child_fund = _seed_repos()
    parent = _promote(_service(_combine_repo, package_repo), child_a1, child_fund).package
    if not live_weight_policy:
        return package_repo, parent

    manifest = parent.manifest
    source_evidence = deepcopy(manifest.source_evidence)
    live_policy = {
        "mode": "live_rolling_ic_weighted",
        "metric": "rank_ic",
        "lookback_trading_days": 60,
        "min_periods": 2,
        "label_horizon": 20,
        "settlement_lag_trading_days": 1,
        "clip_negative_to_zero": True,
    }
    source_evidence["multi_alpha"]["weight_policy"] = live_policy
    backtest_context = deepcopy(manifest.backtest_context)
    backtest_context["weight_policy"] = live_policy
    updated = freeze_manifest(
        manifest.model_copy(
            update={
                "package_id": f"{parent.package_id}_live",
                "package_name": f"{parent.package_name}_live",
                "source": manifest.source.model_copy(update={"source_id": f"{manifest.source.source_id}:live"}),
                "source_evidence": source_evidence,
                "backtest_context": backtest_context,
                "manifest_sha256": None,
            }
        )
    )
    live_parent = package_repo.save_manifest(updated)
    return package_repo, live_parent


def _artifact_service(package_repo, provider_scores: dict[str, list[dict]] | None = None):  # noqa: ANN001
    artifact_repo = InMemorySelectionScoreArtifactRepository()
    resolver = FakeResolver()
    provider = FakeProvider(
        provider_scores
        or {
            A1_LEG: _score_rows(reverse=False),
            FUND_LEG: _score_rows(reverse=True),
        }
    )
    service = StrategyPackageSelectionArtifactService(
        package_repository=package_repo,
        artifact_repository=artifact_repo,
        runtime_asset_resolver=resolver,
        live_inference_provider=provider,
    )
    return service, artifact_repo, resolver, provider


def _reason(exc: BaseException) -> str | None:
    return getattr(exc, "context", {}).get("reason_code")


def _generate(service, parent, runtime_config: dict):  # noqa: ANN001
    return service.generate_from_live_inference(
        package_id=parent.package_id,
        trade_date=TRADE_DATE,
        runtime_config=runtime_config,
        include_reference_price=False,
    )


def test_multi_alpha_live_selection_artifact_is_authoritative_and_deterministic() -> None:
    package_repo, parent = _make_parent(live_weight_policy=True)
    runtime_config = _runtime_config(extra_artifact={"multi_alpha_weight_history": _live_weight_history()})
    service, artifact_repo, resolver, provider = _artifact_service(package_repo)

    first = _generate(service, parent, runtime_config)
    second = _generate(service, parent, runtime_config)

    assert first.metadata["source_type"] == LIVE_MULTI_ALPHA_SELECTION_SOURCE_TYPE
    assert first.metadata["authority_scope"] == AUTHORITATIVE_SELECTION_SCOPE
    assert first.metadata["combine_backtest_run_id"]
    assert first.metadata["component_score_artifact_ids"].keys() == {A1_LEG, FUND_LEG}
    assert first.metadata["weight_artifact_id"].startswith("maw_")
    assert first.metadata["component_manifest_sha256"][A1_LEG]
    assert first.metadata["runtime_source"] == "parent_package_asset"
    assert first.metadata["runtime_package_id"] == parent.package_id
    assert first.metadata["model_params_origin"] == "package_asset"
    assert first.metadata["component_artifacts"][A1_LEG]["runtime_source"] == "parent_package_asset"
    assert first.metadata["component_artifacts"][FUND_LEG]["model_params_origin"] == "package_asset"
    assert first.metadata["component_artifacts"][A1_LEG]["child_package_id"] is None
    assert first.metadata["seed_run_ids"] == {A1_LEG: [A1_SEED], FUND_LEG: [FUND_SEED]}
    assert first.metadata["normalization_method"] == "zscore"
    assert first.metadata["final_topk"] == 25
    assert first.metadata["component_candidate_universe_size"] == 60
    assert first.metadata["coverage_threshold"] == 25
    assert first.artifact_sha256 == second.artifact_sha256
    assert first.scores_json == second.scores_json
    assert len(provider.calls) == 4
    assert resolver.legacy_load_calls == []
    assert {call["leg_id"] for call in resolver.load_calls} == {A1_LEG, FUND_LEG}
    assert all(call["package_id"] == parent.package_id for call in resolver.load_calls)
    assert all(call["model_asset"].asset_ref for call in resolver.load_calls)
    assert all(call["factor_set"] for call in resolver.load_calls)
    assert all(str(call["cache_namespace"]).startswith("leg_") for call in resolver.prepare_calls)

    snapshot = StrategyPackageRuntime(artifact_repository=artifact_repo).build_signal_snapshot(
        manifest=parent.current_manifest(),
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        runtime_config=runtime_config,
    )
    assert snapshot.candidates[0].rank == 1
    assert snapshot.candidates[0].component_scores[A1_LEG]["weight"] > snapshot.candidates[0].component_scores[FUND_LEG]["weight"]


def test_multi_alpha_runtime_rejects_non_authoritative_artifact() -> None:
    package_repo, parent = _make_parent(live_weight_policy=False)
    runtime_config = _runtime_config()
    artifact_repo = InMemorySelectionScoreArtifactRepository()
    artifact_repo.save(
        SelectionScoreArtifact(
            package_id=parent.package_id,
            manifest_sha256=parent.manifest_sha256,
            trade_date=TRADE_DATE,
            data_source="DB_HISTORICAL",
            runtime_config_hash=first_multi_alpha_hash(parent.current_manifest(), runtime_config),
            scores_json=[{"symbol": "000001.SZ", "score": 1.0, "rank": 1, "target_weight": 0.04}],
            score_count=1,
            universe_count=1,
            top_score_symbol="000001.SZ",
            metadata={"source_type": DIAGNOSTIC_BACKTEST_SOURCE_TYPE, "authority_scope": DIAGNOSTIC_BACKTEST_SCOPE},
        )
    )

    with pytest.raises(DataUnavailableError) as excinfo:
        StrategyPackageRuntime(artifact_repository=artifact_repo).build_signal_snapshot(
            manifest=parent.current_manifest(),
            trade_date=TRADE_DATE,
            data_source="DB_HISTORICAL",
            runtime_config=runtime_config,
        )

    assert _reason(excinfo.value) == REASON_PREDICTION_NOT_AUTHORITATIVE


def first_multi_alpha_hash(manifest, runtime_config):  # noqa: ANN001, ANN201
    from backend.services.strategy_package.multi_alpha_live import multi_alpha_selection_artifact_runtime_hash

    return multi_alpha_selection_artifact_runtime_hash(manifest, runtime_config)


@pytest.mark.parametrize(
    ("mutator", "expected_reason"),
    [
        (
            lambda _package_repo, parent, _runtime_config: _remove_first_leg_model_asset(parent),
            REASON_PARENT_LEG_MODEL_ASSET_MISSING,
        ),
        (
            lambda _package_repo, parent, _runtime_config: _clear_first_leg_factor_refs(parent),
            REASON_PARENT_LEG_FACTOR_REFS_MISSING,
        ),
        (
            lambda _package_repo, parent, _runtime_config: _remove_parent_alpha158_schema(parent),
            REASON_PARENT_ALPHA158_SCHEMA_MISSING,
        ),
        (
            lambda _package_repo, _parent, runtime_config: runtime_config["selection_artifact_config"].__setitem__(
                "component_coverage_threshold",
                100,
            ),
            REASON_COMPONENT_COVERAGE_LOW,
        ),
    ],
)
def test_multi_alpha_live_selection_negative_manifest_and_coverage(mutator, expected_reason) -> None:  # noqa: ANN001
    package_repo, parent = _make_parent(live_weight_policy=False)
    runtime_config = _runtime_config()
    service, _artifact_repo, _resolver, _provider = _artifact_service(package_repo)
    mutator(package_repo, parent, runtime_config)

    with pytest.raises((DataUnavailableError, RuntimeConfigInvalidError)) as excinfo:
        _generate(service, parent, runtime_config)

    assert _reason(excinfo.value) == expected_reason


def test_multi_alpha_live_selection_fails_loud_when_leg_scores_missing() -> None:
    package_repo, parent = _make_parent(live_weight_policy=False)
    runtime_config = _runtime_config()
    service, _artifact_repo, _resolver, _provider = _artifact_service(package_repo, provider_scores={A1_LEG: _score_rows()})

    with pytest.raises(DataUnavailableError) as excinfo:
        _generate(service, parent, runtime_config)

    assert _reason(excinfo.value) == REASON_PARENT_LEG_INFERENCE_EMPTY


@pytest.mark.parametrize(
    ("history", "expected_reason"),
    [
        ([], REASON_WEIGHT_UNAVAILABLE),
        (_live_weight_history(samples=1), REASON_LABEL_WINDOW_INSUFFICIENT),
        (_live_weight_history(a1_value=-0.1, fund_value=-0.2), REASON_WEIGHT_ALL_NON_POSITIVE),
    ],
)
def test_multi_alpha_live_rolling_weight_failures(history, expected_reason) -> None:  # noqa: ANN001
    package_repo, parent = _make_parent(live_weight_policy=True)
    runtime_config = _runtime_config(extra_artifact={"multi_alpha_weight_history": history})
    service, _artifact_repo, _resolver, _provider = _artifact_service(package_repo)

    with pytest.raises(DataUnavailableError) as excinfo:
        _generate(service, parent, runtime_config)

    assert _reason(excinfo.value) == expected_reason


def test_multi_alpha_topk_mismatch_fails_loud() -> None:
    package_repo, parent = _make_parent(live_weight_policy=False)
    service, _artifact_repo, _resolver, _provider = _artifact_service(package_repo)

    with pytest.raises(RuntimeConfigInvalidError) as excinfo:
        _generate(service, parent, _runtime_config(top_k=10))

    assert _reason(excinfo.value) == REASON_TOPK_RUNTIME_MISMATCH


def test_multi_alpha_deadline_gate_fails_before_inference() -> None:
    package_repo, parent = _make_parent(live_weight_policy=False)
    artifact_repo = InMemorySelectionScoreArtifactRepository()
    resolver = FakeResolver()
    provider = FakeProvider({A1_LEG: _score_rows(), FUND_LEG: _score_rows(reverse=True)})
    gate = MultiAlphaLivePredictionProvider(
        package_repository=package_repo,
        artifact_repository=artifact_repo,
        runtime_asset_resolver=resolver,
        live_inference_provider=provider,
        weight_service=MultiAlphaWeightService(),
        clock=lambda: datetime(2024, 7, 2, 9, 31, tzinfo=timezone.utc),
    )

    with pytest.raises(RuntimeConfigInvalidError) as excinfo:
        gate.generate_artifacts(
            package_id=parent.package_id,
            trade_dates=[TRADE_DATE],
            data_source="DB_HISTORICAL",
            runtime_config=_runtime_config(extra_artifact={"multi_alpha_deadline_at": "2024-07-02T09:30:00+00:00"}),
            include_reference_price=False,
            inference_backend="fake_live",
        )

    assert _reason(excinfo.value) == REASON_DEADLINE_EXCEEDED
    assert provider.calls == []


def test_multi_alpha_runtime_disabled_fails_loud() -> None:
    _package_repo, parent = _make_parent(live_weight_policy=False)
    runtime_config = _runtime_config(extra_artifact={"multi_alpha_live_inference_enabled": False})

    with pytest.raises(UnsupportedFeatureError) as excinfo:
        StrategyPackageRuntime(artifact_repository=InMemorySelectionScoreArtifactRepository()).build_signal_snapshot(
            manifest=parent.current_manifest(),
            trade_date=TRADE_DATE,
            data_source="DB_HISTORICAL",
            runtime_config=runtime_config,
        )

    assert _reason(excinfo.value) == REASON_RUNTIME_NOT_ENABLED


def test_multi_alpha_legacy_child_ref_is_ignored_and_child_repository_is_not_read() -> None:
    package_repo, parent = _make_parent(live_weight_policy=False)
    manifest = parent.manifest
    first = manifest.alpha_components[0]
    updated_component = first.model_copy(
        update={
            "lineage": first.lineage.model_copy(update={"model_artifact_ref": "child_package:pkg_mac_legacy"})
        }
    )
    parent.manifest = manifest.model_copy(update={"alpha_components": [updated_component, *manifest.alpha_components[1:]]})
    sentinel_repo = ChildFailingRepository(package_repo)
    service, _artifact_repo, resolver, provider = _artifact_service(sentinel_repo)

    artifact = _generate(service, parent, _runtime_config())

    assert artifact.status.value == "SUCCEEDED"
    assert sentinel_repo.get_calls == [parent.package_id, parent.package_id]
    assert resolver.legacy_load_calls == []
    assert artifact.metadata["legacy_child_ref_ignored"][A1_LEG] is True
    assert artifact.metadata["component_artifacts"][A1_LEG]["legacy_child_ref_ignored"] is True
    assert artifact.metadata["runtime_source"] == "parent_package_asset"
    assert len(provider.calls) == 2


def test_multi_alpha_runtime_uses_per_leg_alpha158_disabled_mapping() -> None:
    package_repo, parent = _make_parent(live_weight_policy=False)
    manifest = parent.manifest
    source_evidence = deepcopy(manifest.source_evidence)
    for leg in source_evidence["multi_alpha"]["legs"]:
        if leg["leg_id"] == FUND_LEG:
            leg["runtime_assets"] = RuntimeAssetManifest().model_dump(mode="json")
    parent.manifest = manifest.model_copy(update={"source_evidence": source_evidence})
    service, _artifact_repo, resolver, _provider = _artifact_service(package_repo)

    artifact = _generate(service, parent, _runtime_config())

    by_leg = {call["leg_id"]: call["runtime_assets"] for call in resolver.load_calls}
    assert by_leg[A1_LEG].alpha158.enabled is True
    assert by_leg[FUND_LEG].alpha158.enabled is False
    assert artifact.metadata["component_artifacts"][FUND_LEG]["alpha158_schema_sha256"] is None


def _remove_first_leg_model_asset(parent) -> None:  # noqa: ANN001
    manifest = parent.manifest
    first_model_id = manifest.alpha_components[0].model_id
    models = manifest.model_asset if isinstance(manifest.model_asset, list) else [manifest.model_asset]
    parent.manifest = manifest.model_copy(update={"model_asset": [model for model in models if model.model_id != first_model_id]})


def _clear_first_leg_factor_refs(parent) -> None:  # noqa: ANN001
    manifest = parent.manifest
    first = manifest.alpha_components[0]
    updated = first.model_copy(update={"lineage": first.lineage.model_copy(update={"factor_artifact_refs": []})})
    parent.manifest = manifest.model_copy(update={"alpha_components": [updated, *manifest.alpha_components[1:]]})


def _remove_parent_alpha158_schema(parent) -> None:  # noqa: ANN001
    manifest = parent.manifest
    parent.manifest = manifest.model_copy(update={"runtime_assets": None})
