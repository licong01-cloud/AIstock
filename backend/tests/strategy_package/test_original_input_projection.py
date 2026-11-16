from types import SimpleNamespace

import pytest

from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import FactorAsset, FittedPreprocessorAsset
from backend.services.strategy_package.original_input_projection import (
    KEY, project_original_inputs, retain_raw_projection_assets,
)
from backend.services.trading_core.errors import StrategyPackageValidationError
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


def raw_manifest():
    base = make_manifest()
    factors = [FactorAsset(factor_id=n, factor_name=n, asset_ref="aistock-package-asset://blobs/" + s * 64,
                           sha256=s * 64, size_bytes=10) for n, s in [("A", "a"), ("DIAGNOSTIC", "b")]]
    component = base.alpha_components[0].model_copy(update={
        "factor_ids": ["A", "DIAGNOSTIC"],
        "lineage": base.alpha_components[0].lineage.model_copy(update={"factor_artifact_refs": ["A", "DIAGNOSTIC"]})})
    model = base.model_asset.model_copy(update={"preprocessor_asset": FittedPreprocessorAsset(
        asset_ref="aistock-package-asset://blobs/" + "c" * 64, sha256="c" * 64, size_bytes=100)})
    return freeze_manifest(base.model_copy(update={"factor_set": factors, "alpha_components": [component],
        "model_asset": model, "source_evidence": {"custom_params": {"feature_processing": {
            "exclude_from_model": ["DIAGNOSTIC"], "clip_outlier": True}}}, "manifest_sha256": None}))


def test_projection_follows_explicit_original_request_and_preserves_model_and_raw_assets():
    original = raw_manifest()
    projected = project_original_inputs(original)
    assert len(original.factor_set) == 2
    assert [f.factor_name for f in projected.factor_set] == ["A"]
    assert projected.alpha_components[0].lineage.factor_artifact_refs == ["A"]
    assert projected.model_asset == original.model_asset
    evidence = projected.source_evidence[KEY]
    assert evidence["source_model_manifest_sha256"] == original.manifest_sha256
    assert evidence["preprocessor_sha256"] == "c" * 64
    assert evidence["exclusion_inferred_from_missing_columns"] is False
    parent = retain_raw_projection_assets(projected, [SimpleNamespace(seed_assets=[{KEY: evidence}])])
    assert {f.factor_name for f in parent.factor_set} == {"A", "DIAGNOSTIC"}
    assert parent.alpha_components[0].lineage.factor_artifact_refs == ["A"]


def test_absent_request_never_infers_exclusions():
    original = raw_manifest().model_copy(update={"source_evidence": {}})
    assert project_original_inputs(original) is original


@pytest.mark.parametrize("processing", [{"exclude_from_model": ["UNKNOWN"]},
                                       {"exclude_from_model": ["A", "DIAGNOSTIC"]},
                                       {"exclude_from_model": ["A", "A"]}, {"raw_state_controls": {}}])
def test_invalid_or_unproven_projection_fails_closed(processing):
    original = raw_manifest().model_copy(update={"source_evidence": {"custom_params": {"feature_processing": processing}}})
    with pytest.raises(StrategyPackageValidationError):
        project_original_inputs(original)
