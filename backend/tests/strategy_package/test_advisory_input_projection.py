from __future__ import annotations

import ast
from pathlib import Path

import pytest

from backend.services.advisory_dev_input_onboarding.contracts import (
    AdvisoryStrategyPackageInputProjectionV1,
    STRATEGY_PACKAGE_SELECTION_QUERY_CONTRACT_HASH,
)
from backend.services.strategy_package.advisory_input_projection import (
    CANONICAL_HISTORICAL_QUERY_CONTRACT_HASH,
    CANONICAL_HISTORICAL_QUERY_CONTRACT_PAYLOAD,
    CANONICAL_SELECTION_QUERY_CONTRACT_HASH,
    CANONICAL_SELECTION_QUERY_CONTRACT_PAYLOAD,
    HISTORICAL_RANGE_QUERY_CONTRACT_HASH,
    REASON_INPUT_PROJECTION_CONFLICT,
    REASON_INPUT_PROJECTION_UNAVAILABLE,
    SELECTION_QUERY_CONTRACT_HASH,
    AdvisoryInputProjectionError,
    StrategyPackageAdvisoryInputProjectionV1,
    StrategyPackageAdvisoryInputProjectionV2,
    project_advisory_inputs,
    project_canonical_advisory_inputs,
    project_historical_range_inputs,
)
from backend.services.canonical_equity_pit import (
    CANONICAL_PIT_AUTHORITY_ID,
    CANONICAL_PIT_RULE_VERSION,
    canonical_rule_parameters_digest,
)
from backend.services.strategy_package.canonical_pit_compatibility import build_canonical_pit_v2_manifest
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import (
    Alpha158SchemaAsset,
    AlphaCombinationPolicy,
    AlphaComponent,
    AlphaLineage,
    AlphaMode,
    BacktestSummary,
    FactorAsset,
    ModelAsset,
    RuntimeAssetManifest,
    SourceType,
    StrategyPackageManifest,
    StrategyPackageSource,
)


HASH = "a" * 64


def test_query_contract_hashes_remain_stable_after_projection_dependency_isolation() -> None:
    assert SELECTION_QUERY_CONTRACT_HASH == "2f6bcc0df667586129c40069ea133f5f5316c525ad019f1e953840251ace2f16"
    assert HISTORICAL_RANGE_QUERY_CONTRACT_HASH == "24d9739380376179884da57716bf52c0ac2082cd39e51429894a458b47d706ce"


def _component(alpha_id: str, refs: list[str]) -> AlphaComponent:
    return AlphaComponent(
        alpha_id=alpha_id,
        alpha_name=alpha_id,
        component_weight=1.0,
        factor_ids=refs,
        model_id=f"model_{alpha_id}",
        holding_period="5day",
        rebalance_frequency="1day",
        score_direction="higher_better",
        lineage=AlphaLineage(factor_artifact_refs=refs),
    )


def _runtime(*aliases: str) -> RuntimeAssetManifest:
    return RuntimeAssetManifest(
        alpha158=Alpha158SchemaAsset(
            enabled=bool(aliases),
            aliases=list(aliases),
            alias_count=len(aliases),
        )
    )


def _manifest(
    *,
    alpha_mode: AlphaMode,
    components: list[AlphaComponent],
    factors: list[FactorAsset],
    runtime_assets: RuntimeAssetManifest | None,
    source_evidence: dict | None = None,
) -> StrategyPackageManifest:
    weights = {component.alpha_id: 1.0 / len(components) for component in components}
    return StrategyPackageManifest(
        package_id="pkg_projection_unit",
        package_name="projection_unit",
        source=StrategyPackageSource(source_type=SourceType.CANDIDATE_STRATEGY_PACKAGE, source_id="unit"),
        alpha_mode=alpha_mode,
        alpha_components=components,
        alpha_combination_policy=AlphaCombinationPolicy(
            method="identity" if alpha_mode is AlphaMode.SINGLE_ALPHA else "weighted_score",
            weights=weights,
        ),
        factor_set=factors,
        model_asset=[ModelAsset(model_id=f"model_{item.alpha_id}") for item in components],
        runtime_assets=runtime_assets,
        source_evidence=source_evidence or {},
        backtest_summary=BacktestSummary(ic=0.01),
        manifest_sha256=HASH,
    )


def test_single_alpha_projection_preserves_manifest_factor_order_without_mutation() -> None:
    manifest = _manifest(
        alpha_mode=AlphaMode.SINGLE_ALPHA,
        components=[_component("single", ["factor_a", "factor_b"])],
        factors=[
            FactorAsset(factor_id="factor_a", factor_name="Momentum_120D"),
            FactorAsset(factor_id="factor_b", factor_name="Quality_20D"),
        ],
        runtime_assets=_runtime("ROC60", "RSQR60"),
    )
    before = manifest.model_dump(mode="json")

    projection = project_advisory_inputs(manifest)

    assert projection.projection_source == "ADMITTED_MANIFEST_ONLY"
    assert projection.alpha_mode is AlphaMode.SINGLE_ALPHA
    assert projection.legs[0].factor_order == ("ROC60", "RSQR60", "Momentum_120D", "Quality_20D")
    assert projection.legs[0].required_window == 120
    assert manifest.model_dump(mode="json") == before
    assert StrategyPackageAdvisoryInputProjectionV1.model_validate(
        projection.model_dump(mode="json")
    ) == projection
    advisory_projection = AdvisoryStrategyPackageInputProjectionV1.model_validate(
        projection.model_dump(mode="json")
    )
    assert advisory_projection.model_dump(mode="json") == projection.model_dump(mode="json")
    assert advisory_projection.selection_query_contract_hash == STRATEGY_PACKAGE_SELECTION_QUERY_CONTRACT_HASH


def test_canonical_v2_projection_carries_training_identity_and_frozen_historical_universe() -> None:
    source = _manifest(
        alpha_mode=AlphaMode.SINGLE_ALPHA,
        components=[_component("single", ["factor_a"])],
        factors=[FactorAsset(factor_id="factor_a", factor_name="Momentum_120D")],
        runtime_assets=_runtime("ROC60"),
    )
    source = freeze_manifest(source.model_copy(update={"manifest_sha256": None}))
    manifest = build_canonical_pit_v2_manifest(
        source,
        package_id="pkg_projection_unit_pitv2",
        package_version="2.0.0",
        dataset_binding={
            "schema_version": "qe_formal_canonical_pit_dataset_binding_v1",
            "usage_mode": "formal_training",
            "authority_id": CANONICAL_PIT_AUTHORITY_ID,
            "rule_version": CANONICAL_PIT_RULE_VERSION,
            "rule_parameters_digest": canonical_rule_parameters_digest(),
            "release_id": "qe_hmm_full_v2_20260731",
            "cutoff": "2026-07-31",
            "frozen_snapshot_digest": "b" * 64,
            "manifest_digest": "c" * 64,
        },
        qualification_method="RETRAINED",
        qualification_evidence_digest="d" * 64,
    )

    live_projection = project_canonical_advisory_inputs(manifest)
    historical_projection = project_historical_range_inputs(manifest)

    assert isinstance(live_projection, StrategyPackageAdvisoryInputProjectionV2)
    assert live_projection.selection_query_contract_hash == CANONICAL_SELECTION_QUERY_CONTRACT_HASH
    assert CANONICAL_SELECTION_QUERY_CONTRACT_PAYLOAD["logical_inputs"][0]["source_role"] == "pit_universe"
    assert live_projection.canonical_pit_binding.release_id == "qe_hmm_full_v2_20260731"
    assert historical_projection.query_contract_hash == CANONICAL_HISTORICAL_QUERY_CONTRACT_HASH
    assert CANONICAL_HISTORICAL_QUERY_CONTRACT_PAYLOAD["logical_inputs"][0]["source_role"] == "pit_universe"
    assert historical_projection.pit_universe_key == (
        "aistock_equity_pit_snapshot_qe_hmm_full_v2_20260731"
    )
    assert historical_projection.pit_universe_ensure is False


def test_native_multi_projection_preserves_component_order_and_independent_windows() -> None:
    first = _component("leg_short", ["factor_short"])
    second = _component("leg_long", ["factor_long"])
    manifest = _manifest(
        alpha_mode=AlphaMode.MULTI_ALPHA,
        components=[first, second],
        factors=[
            FactorAsset(factor_id="factor_short", factor_name="Reversal_20D"),
            FactorAsset(factor_id="factor_long", factor_name="Trend_250D"),
        ],
        runtime_assets=_runtime("ROC60"),
        source_evidence={
            "multi_alpha": {
                "legs": [
                    {"leg_id": "leg_short", "runtime_assets": _runtime("ROC60").model_dump(mode="json")},
                    {"leg_id": "leg_long", "runtime_assets": _runtime("RSQR60").model_dump(mode="json")},
                ]
            }
        },
    )

    projection = project_advisory_inputs(manifest)

    assert [item.alpha_component_id for item in projection.legs] == ["leg_short", "leg_long"]
    assert projection.legs[0].factor_order == ("ROC60", "Reversal_20D")
    assert projection.legs[0].required_window == 61
    assert projection.legs[1].factor_order == ("RSQR60", "Trend_250D")
    assert projection.legs[1].required_window == 250


@pytest.mark.parametrize(
    ("factors", "refs", "expected_reason"),
    [
        ([FactorAsset(factor_id="known", factor_name="Known_20D")], ["missing"], REASON_INPUT_PROJECTION_UNAVAILABLE),
        (
            [
                FactorAsset(factor_id="shared", factor_name="First_20D"),
                FactorAsset(factor_id="other", factor_name="shared"),
            ],
            ["shared"],
            REASON_INPUT_PROJECTION_CONFLICT,
        ),
    ],
)
def test_native_multi_missing_or_ambiguous_factor_refs_fail_explicitly(
    factors: list[FactorAsset], refs: list[str], expected_reason: str
) -> None:
    components = [_component("leg_a", refs), _component("leg_b", [factors[-1].factor_id])]
    manifest = _manifest(
        alpha_mode=AlphaMode.MULTI_ALPHA,
        components=components,
        factors=factors,
        runtime_assets=_runtime(),
        source_evidence={
            "multi_alpha": {
                "legs": [
                    {"leg_id": "leg_a", "runtime_assets": _runtime().model_dump(mode="json")},
                    {"leg_id": "leg_b", "runtime_assets": _runtime().model_dump(mode="json")},
                ]
            }
        },
    )

    with pytest.raises(AdvisoryInputProjectionError) as captured:
        project_advisory_inputs(manifest)

    assert captured.value.reason_code == expected_reason
    assert captured.value.context["package_id"] == manifest.package_id


def test_duplicate_factor_identity_is_a_program_local_projection_conflict() -> None:
    manifest = _manifest(
        alpha_mode=AlphaMode.SINGLE_ALPHA,
        components=[_component("single", ["factor_a"])],
        factors=[FactorAsset(factor_id="factor_a", factor_name="ROC60")],
        runtime_assets=_runtime("ROC60"),
    )

    with pytest.raises(AdvisoryInputProjectionError) as captured:
        project_advisory_inputs(manifest)

    assert captured.value.reason_code == REASON_INPUT_PROJECTION_CONFLICT
    assert captured.value.context["duplicates"] == ["ROC60"]


def test_projection_module_has_no_runtime_validation_or_shared_consumer_imports() -> None:
    module_path = Path(__file__).resolve().parents[2] / "services" / "strategy_package" / "advisory_input_projection.py"
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    imports = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    imports.update(
        node.module or ""
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    )
    forbidden_fragments = {
        "advisory_phase0a",
        "repository",
        "asset_store",
        "validator",
        "health",
        "live_inference",
        "multi_alpha_live",
        "selection_center",
        "simulation",
        "paper_trading",
        "hmm",
    }
    assert not {name for name in imports if any(fragment in name for fragment in forbidden_fragments)}
