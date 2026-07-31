"""K5 full-five conformance writer/readback and factory-proof closure."""

from __future__ import annotations

import pytest

from backend.execution_algos.vnpy_compat.facade_characterization import (
    build_vnpy_facade_conformance_set_v2,
    build_vnpy_facade_full_five_conformance_set_v2,
    validate_vnpy_facade_conformance_set_against_authority_v2,
)
from backend.execution_algos.vnpy_compat.facade_contracts import VnpyFacadeContractError
from backend.execution_algos.vnpy_compat.receipts import build_current_three_compatibility_receipts_v1
from backend.execution_algos.vnpy_style.plugin_factories import current_three_process_bindings_v3
from backend.execution_algos.vnpy_style.plugin_manifests import (
    current_three_creation_bindings_v3,
    current_three_descriptors_v3,
)
from backend.services.miniqmt_execution_runtime.k5_shadow_catalog import (
    K5ShadowCatalogRuntimeV1,
    build_k5_shadow_catalog_runtime_v1,
    readback_k5_shadow_conformance_set_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_registry import (
    PluginCatalogRuntimeV2,
    PluginProcessBindingsV2,
    build_plugin_catalog_v2,
)
from backend.tests.miniqmt_execution_runtime.test_vnpy_k5_shadow_catalog import _gateway


@pytest.fixture(scope="module")
def candidate() -> K5ShadowCatalogRuntimeV1:
    return build_k5_shadow_catalog_runtime_v1(gateway_catalog=_gateway())


def test_full_five_writer_and_readback_reexecute_catalog_bound_factories(
    candidate: K5ShadowCatalogRuntimeV1,
) -> None:
    rebuilt = build_vnpy_facade_full_five_conformance_set_v2(
        catalog_runtime=candidate.catalog_runtime,
        gateway_catalog=_gateway(),
        facade_contract=candidate.facade_contract,
        source_manifest=candidate.source_manifest,
        characterization_authority_v2=candidate.characterization_authority,
        algorithm_bindings_v2=candidate.conformance_authority.algorithm_bindings,
    )

    assert rebuilt == candidate.conformance_set
    assert (
        readback_k5_shadow_conformance_set_v1(
            conformance_set=rebuilt,
            gateway_catalog=_gateway(),
        )
        == rebuilt
    )


def test_full_five_writer_rejects_missing_or_wrong_process_factory_before_passed(
    candidate: K5ShadowCatalogRuntimeV1,
) -> None:
    empty = PluginCatalogRuntimeV2(
        snapshot=candidate.catalog_runtime.snapshot,
        process_bindings=PluginProcessBindingsV2({}),
    )
    bindings = candidate.catalog_runtime.process_bindings.copy_bindings_v1()
    bindings["aistock.vnpy.stop.factory"] = lambda _config: object()
    wrong = PluginCatalogRuntimeV2(
        snapshot=candidate.catalog_runtime.snapshot,
        process_bindings=PluginProcessBindingsV2(bindings),
    )

    for runtime in (empty, wrong):
        with pytest.raises(VnpyFacadeContractError) as caught:
            build_vnpy_facade_full_five_conformance_set_v2(
                catalog_runtime=runtime,
                gateway_catalog=_gateway(),
                facade_contract=candidate.facade_contract,
                source_manifest=candidate.source_manifest,
                characterization_authority_v2=candidate.characterization_authority,
                algorithm_bindings_v2=candidate.conformance_authority.algorithm_bindings,
            )
        assert caught.value.reason_code == "MINIQMT_VNPY_FACADE_BINDING_INVALID"


def test_k5_readback_rejects_malformed_carrier_with_typed_context() -> None:
    with pytest.raises(VnpyFacadeContractError) as caught:
        readback_k5_shadow_conformance_set_v1(conformance_set={}, gateway_catalog=_gateway())

    assert caught.value.reason_code == "MINIQMT_VNPY_FACADE_CONFORMANCE_AUTHORITY_INVALID"
    assert "error" in caught.value.context


def test_k4_current_three_writer_and_readback_remain_pure_and_unchanged(
    candidate: K5ShadowCatalogRuntimeV1,
) -> None:
    current = build_plugin_catalog_v2(
        descriptors=current_three_descriptors_v3(),
        creation_bindings=current_three_creation_bindings_v3(),
        process_bindings=current_three_process_bindings_v3(),
        pinned_compatibility_receipts=build_current_three_compatibility_receipts_v1(),
    )
    conformance = build_vnpy_facade_conformance_set_v2(
        catalog_runtime=current,
        gateway_catalog=_gateway(),
        facade_contract=candidate.facade_contract,
        source_manifest=candidate.source_manifest,
        characterization_authority_v2=candidate.characterization_authority,
        algorithm_bindings_v2=candidate.conformance_authority.algorithm_bindings,
    )
    authority = validate_vnpy_facade_conformance_set_against_authority_v2(
        conformance_set=conformance,
        catalog_runtime=current,
        gateway_catalog=_gateway(),
        facade_contract=candidate.facade_contract,
        source_manifest=candidate.source_manifest,
        characterization_authority_v2=candidate.characterization_authority,
    )

    assert tuple(item.algo_code for item in conformance.ordered_receipts) == (
        "BEST_LIMIT_MINIQMT",
        "SNIPER_MINIQMT",
        "TWAP_LITE_MINIQMT",
    )
    assert all(
        item.runtime_binding_disposition.value == "PURE_PLUGIN_SHADOW_CONFORMANCE"
        for item in conformance.ordered_receipts
    )
    assert authority.conformance_set == conformance
