from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from backend.services.strategy_package.live_inference import (
    PREFLIGHT_CHECK_MODEL_PARAMS,
    PREFLIGHT_CHECK_NAMES,
    PREFLIGHT_CHECK_QE_SOURCE,
    PREFLIGHT_STATUS_BLOCKED,
    PREFLIGHT_STATUS_PASS,
    QEExperimentRuntimeAssetResolver,
)
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import (
    Alpha158SchemaAsset,
    AlphaCombinationPolicy,
    AlphaLineage,
    AlphaMode,
    FactorAsset,
    ModelAsset,
    RuntimeAssetManifest,
    SourceType,
)
from backend.services.strategy_package.package_asset_store import LocalPackageAssetStore
from backend.services.trading_core.errors import DataUnavailableError
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


A1_LEG = "a1_plus3_LSTM_h20"
FUND_LEG = "new_FUNDGROWTH_h20"


class _ForbiddenConn:
    def __enter__(self) -> "_ForbiddenConn":
        raise AssertionError("multi-alpha parent preflight must not query QE DB")

    def __exit__(self, *_args: Any) -> None:
        return None


def _put(store: LocalPackageAssetStore, payload: bytes, *, kind: str) -> tuple[str, str]:
    blob = store.put(payload, kind=kind)
    return blob.uri, blob.sha256


def _parent_manifest(store: LocalPackageAssetStore):  # noqa: ANN201
    factor_a_ref, factor_a_sha = _put(
        store,
        b"import pandas as pd\ndef calculate():\n    return pd.DataFrame({'factor_a': [1.0]})\n",
        kind="factor_code",
    )
    factor_b_ref, factor_b_sha = _put(
        store,
        b"import pandas as pd\ndef calculate():\n    return pd.DataFrame({'factor_b': [2.0]})\n",
        kind="factor_code",
    )
    factor_c_ref, factor_c_sha = _put(
        store,
        b"import pandas as pd\ndef calculate():\n    return pd.DataFrame({'factor_c': [3.0]})\n",
        kind="factor_code",
    )
    factor_d_ref, factor_d_sha = _put(
        store,
        b"import pandas as pd\ndef calculate():\n    return pd.DataFrame({'factor_d': [4.0]})\n",
        kind="factor_code",
    )
    model_a_ref, model_a_sha = _put(store, b"model-a-params", kind="model_weight")
    model_b_ref, model_b_sha = _put(store, b"model-b-params", kind="model_weight")
    alpha158_payload = (
        b'{"schema_version":"strategy_package_alpha158_schema_v1",'
        b'"loader_class":"qlib.contrib.data.loader.Alpha158DL",'
        b'"loader_node":{"class":"qlib.contrib.data.loader.Alpha158DL",'
        b'"kwargs":{"config":{"feature":[["Ref($close, 5) / $close"],["RESI5"]]}}},'
        b'"aliases":["RESI5"],"expression_count":1,"alias_count":1,'
        b'"source_conf_relpath":"conf.yaml"}'
    )
    alpha158_ref, alpha158_sha = _put(store, alpha158_payload, kind="factor_schema")

    factors = [
        FactorAsset(
            factor_id="factor_a",
            factor_name="factor_a",
            asset_ref=factor_a_ref,
            sha256=factor_a_sha,
            size_bytes=len(store.get(factor_a_ref)),
        ),
        FactorAsset(
            factor_id="factor_b",
            factor_name="factor_b",
            asset_ref=factor_b_ref,
            sha256=factor_b_sha,
            size_bytes=len(store.get(factor_b_ref)),
        ),
        FactorAsset(
            factor_id="factor_c",
            factor_name="factor_c",
            asset_ref=factor_c_ref,
            sha256=factor_c_sha,
            size_bytes=len(store.get(factor_c_ref)),
        ),
        FactorAsset(
            factor_id="factor_d",
            factor_name="factor_d",
            asset_ref=factor_d_ref,
            sha256=factor_d_sha,
            size_bytes=len(store.get(factor_d_ref)),
        ),
    ]
    model_a = ModelAsset(model_id="model_a", model_ref="model_a", asset_ref=model_a_ref, sha256=model_a_sha, size_bytes=14)
    model_b = ModelAsset(model_id="model_b", model_ref="model_b", asset_ref=model_b_ref, sha256=model_b_sha, size_bytes=14)
    base = make_manifest().model_copy(
        update={
            "manifest_version": "alpha_core_v1",
            "package_name": "unit_multi_alpha_parent_preflight",
            "source": make_manifest().source.model_copy(
                update={
                    "source_type": SourceType.MULTI_ALPHA_COMBINE_RUN,
                    "source_id": "combine_run_unit",
                    "run_id": "combine_run_unit",
                }
            ),
            "alpha_mode": AlphaMode.MULTI_ALPHA,
            "factor_set": factors,
            "model_asset": [model_a, model_b],
            "runtime_assets": RuntimeAssetManifest(
                alpha158=Alpha158SchemaAsset(
                    enabled=True,
                    aliases=["RESI5"],
                    alias_count=1,
                    loader_class="qlib.contrib.data.loader.Alpha158DL",
                    asset_ref=alpha158_ref,
                    sha256=alpha158_sha,
                    size_bytes=len(alpha158_payload),
                )
            ),
            "manifest_sha256": None,
        }
    )
    first = base.alpha_components[0].model_copy(
        update={
            "alpha_id": A1_LEG,
            "alpha_name": A1_LEG,
            "component_weight": 0.6,
            "factor_ids": ["factor_a", "factor_b"],
            "model_id": "model_a",
            "model_ref": "model_a",
            "lineage": AlphaLineage(
                factor_artifact_refs=["factor_a", "factor_b"],
                model_artifact_ref="parent_package_asset:model_id:model_a",
            ),
        }
    )
    second = base.alpha_components[0].model_copy(
        update={
            "alpha_id": FUND_LEG,
            "alpha_name": FUND_LEG,
            "component_weight": 0.4,
            "factor_ids": ["factor_c", "factor_d"],
            "model_id": "model_b",
            "model_ref": "model_b",
            "lineage": AlphaLineage(
                factor_artifact_refs=["factor_c", "factor_d"],
                model_artifact_ref="parent_package_asset:model_id:model_b",
            ),
        }
    )
    return freeze_manifest(
        base.model_copy(
            update={
                "alpha_components": [first, second],
                "alpha_combination_policy": AlphaCombinationPolicy(
                    method="ic_weighted",
                    weights={A1_LEG: 0.6, FUND_LEG: 0.4},
                ),
                "source_evidence": {
                    "authority": "parent_package_asset_runtime_authority",
                    "multi_alpha": {
                        "combine_backtest_run_id": "combine_run_unit",
                        "legs": [
                            {"leg_id": A1_LEG, "seed_run_ids": ["seed_a"], "terminal_weight": 0.6},
                            {"leg_id": FUND_LEG, "seed_run_ids": ["seed_b"], "terminal_weight": 0.4},
                        ],
                        "terminal_weights": {A1_LEG: 0.6, FUND_LEG: 0.4},
                    },
                },
                "manifest_sha256": None,
            }
        )
    )


def _resolver_for_parent(tmp_path: Path) -> tuple[QEExperimentRuntimeAssetResolver, Any]:
    store = LocalPackageAssetStore(tmp_path / "asset_store")
    manifest = _parent_manifest(store)
    return (
        QEExperimentRuntimeAssetResolver(
            conn_factory=lambda: _ForbiddenConn(),
            cache_root=tmp_path / "runtime_cache",
            asset_store=store,
        ),
        manifest,
    )


def test_multi_alpha_parent_preflight_uses_leg_slices_not_single_model_path(tmp_path: Path) -> None:
    resolver, manifest = _resolver_for_parent(tmp_path)

    result = resolver.preflight_for_strategy_package(
        source_type=manifest.source.source_type.value,
        source_id=manifest.source.source_id,
        loop_id=manifest.source.loop_id,
        run_id=manifest.source.run_id,
        runtime_config={},
        manifest=manifest,
        package_id=manifest.package_id,
    )

    assert result.passed is True
    assert [check.name for check in result.checks] == list(PREFLIGHT_CHECK_NAMES)
    assert all(check.status == PREFLIGHT_STATUS_PASS for check in result.checks)
    assert result.checks[0].context["alpha_mode"] == "multi_alpha"
    assert result.checks[0].context["leg_count"] == 2
    assert {leg["leg_id"] for leg in result.checks[0].context["legs"]} == {A1_LEG, FUND_LEG}
    assert all(leg["model_params_origin"] == "package_asset" for leg in result.checks[0].context["legs"])
    assert result.checks[0].context.get("reason_code") != "strategy_package_runtime_model_asset_ambiguous"


def test_multi_alpha_parent_preflight_missing_leg_model_fails_fast_with_leg_context(tmp_path: Path) -> None:
    resolver, manifest = _resolver_for_parent(tmp_path)
    first_model_id = manifest.alpha_components[0].model_id
    bad_manifest = manifest.model_copy(
        update={"model_asset": [model for model in manifest.model_asset if model.model_id != first_model_id]}
    )

    result = resolver.preflight_for_strategy_package(
        source_type=bad_manifest.source.source_type.value,
        source_id=bad_manifest.source.source_id,
        runtime_config={},
        manifest=bad_manifest,
        package_id=bad_manifest.package_id,
    )

    assert result.passed is False
    assert result.blocked_check is not None
    assert result.blocked_check.name == PREFLIGHT_CHECK_QE_SOURCE
    assert result.blocked_check.status == PREFLIGHT_STATUS_BLOCKED
    context = result.blocked_check.context or {}
    assert context["reason_code"] == "multi_alpha_parent_leg_model_asset_missing"
    assert context["package_id"] == bad_manifest.package_id
    assert context["alpha_mode"] == "multi_alpha"
    assert context["leg_id"] == A1_LEG
    assert context["model_id"] == first_model_id


def test_multi_alpha_parent_preflight_missing_leg_factor_fails_fast_with_leg_context(tmp_path: Path) -> None:
    resolver, manifest = _resolver_for_parent(tmp_path)
    component = manifest.alpha_components[0]
    bad_components = [
        item.model_copy(
            update={
                "lineage": item.lineage.model_copy(
                    update={"factor_artifact_refs": ["factor_a", "missing_factor_ref"]}
                )
            }
        )
        if item.alpha_id == component.alpha_id
        else item
        for item in manifest.alpha_components
    ]
    bad_manifest = manifest.model_copy(update={"alpha_components": bad_components})

    result = resolver.preflight_for_strategy_package(
        source_type=bad_manifest.source.source_type.value,
        source_id=bad_manifest.source.source_id,
        runtime_config={},
        manifest=bad_manifest,
        package_id=bad_manifest.package_id,
    )

    assert result.passed is False
    assert result.blocked_check is not None
    assert result.blocked_check.name == PREFLIGHT_CHECK_QE_SOURCE
    context = result.blocked_check.context or {}
    assert context["reason_code"] == "multi_alpha_parent_leg_factor_asset_missing"
    assert context["package_id"] == bad_manifest.package_id
    assert context["alpha_mode"] == "multi_alpha"
    assert context["leg_id"] == A1_LEG
    assert context["model_id"] == component.model_id
    assert context["factor_ref"] == "missing_factor_ref"


def test_multi_alpha_parent_preflight_missing_leg_asset_blob_fails_fast_with_leg_context(tmp_path: Path) -> None:
    resolver, manifest = _resolver_for_parent(tmp_path)
    first_model_id = manifest.alpha_components[0].model_id
    missing_ref = f"aistock-package-asset://blobs/{hashlib.sha256(b'missing-leg-model').hexdigest()}"
    bad_models = [
        model.model_copy(update={"asset_ref": missing_ref}) if model.model_id == first_model_id else model
        for model in manifest.model_asset
    ]
    bad_manifest = manifest.model_copy(update={"model_asset": bad_models})

    result = resolver.preflight_for_strategy_package(
        source_type=bad_manifest.source.source_type.value,
        source_id=bad_manifest.source.source_id,
        runtime_config={},
        manifest=bad_manifest,
        package_id=bad_manifest.package_id,
    )

    assert result.passed is False
    assert result.blocked_check is not None
    assert result.blocked_check.name == PREFLIGHT_CHECK_QE_SOURCE
    context = result.blocked_check.context or {}
    assert context["reason_code"] == "strategy_package_asset_blob_missing"
    assert context["package_id"] == bad_manifest.package_id
    assert context["leg_id"] == A1_LEG
    assert context["model_id"] == first_model_id
    assert context["asset_kind"] == "model_weight"


def test_single_alpha_preflight_still_uses_single_source_loader() -> None:
    class SingleAlphaResolver(QEExperimentRuntimeAssetResolver):
        def __init__(self) -> None:
            super().__init__(conn_factory=lambda: _ForbiddenConn(), cache_root=Path("."))
            self.single_calls: list[dict[str, Any]] = []
            self.leg_calls: list[dict[str, Any]] = []

        def load_source_for_strategy_package(self, **kwargs: Any):  # noqa: ANN201
            self.single_calls.append(kwargs)
            raise DataUnavailableError("single alpha source blocker", context={"reason_code": "unit_single_path"})

        def load_source_for_strategy_package_leg(self, **kwargs: Any):  # noqa: ANN201
            self.leg_calls.append(kwargs)
            raise AssertionError("single-alpha preflight must not call per-leg loader")

    resolver = SingleAlphaResolver()

    result = resolver.preflight_for_strategy_package(
        source_type="qe_experiment",
        source_id="single_alpha_source",
        runtime_config={},
    )

    assert result.passed is False
    assert len(resolver.single_calls) == 1
    assert resolver.leg_calls == []
    assert result.blocked_check is not None
    assert result.blocked_check.name == PREFLIGHT_CHECK_QE_SOURCE
    assert (result.blocked_check.context or {})["reason_code"] == "unit_single_path"


def test_multi_alpha_parent_preflight_blocks_model_params_override(tmp_path: Path) -> None:
    resolver, manifest = _resolver_for_parent(tmp_path)
    override_path = tmp_path / "external_params.pkl"
    override_path.write_bytes(b"external override must not be used")

    result = resolver.preflight_for_strategy_package(
        source_type=manifest.source.source_type.value,
        source_id=manifest.source.source_id,
        runtime_config={"selection_artifact_config": {"model_params_path": str(override_path)}},
        manifest=manifest,
        package_id=manifest.package_id,
    )

    assert result.passed is False
    assert result.blocked_check is not None
    assert result.blocked_check.name == PREFLIGHT_CHECK_MODEL_PARAMS
    context = result.blocked_check.context or {}
    assert context["reason_code"] == "strategy_package_runtime_model_override_forbidden"
    assert context["package_id"] == manifest.package_id
