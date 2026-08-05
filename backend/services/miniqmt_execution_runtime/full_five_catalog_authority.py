"""Shared, source-derived full-five plugin catalog authority.

K5 continues to use this builder only for shadow conformance.  K6-D consumes
the same immutable source/catalog facts through its own product coordinator;
it never treats a K5 shadow disposition as a product-route permission.
"""

from __future__ import annotations

from dataclasses import dataclass

from backend.execution_algos.vnpy_compat.facade_characterization import (
    VnpyFacadeCharacterizationAuthorityV2,
    build_vnpy_facade_algorithm_bindings_v2,
    build_vnpy_facade_characterization_requirements_v1,
    build_vnpy_facade_contract_v1,
    build_vnpy_facade_full_five_conformance_set_v2,
    build_vnpy_facade_source_manifest_v1,
    validate_vnpy_facade_full_five_conformance_set_against_authority_v2,
)
from backend.execution_algos.vnpy_compat.k5_binding_authority import k5_facade_algorithm_bindings_v2
from backend.execution_algos.vnpy_compat.k5_plugin_factories import k5_process_bindings_v1
from backend.execution_algos.vnpy_compat.k5_plugin_manifests import (
    k5_creation_bindings_v1,
    k5_descriptors_v1,
    k5_manifests_v1,
)
from backend.execution_algos.vnpy_compat.receipts import (
    build_current_three_compatibility_receipts_v1,
    build_vnpy_compatibility_receipt_v1,
)
from backend.execution_algos.vnpy_style.plugin_factories import current_three_process_bindings_v3
from backend.execution_algos.vnpy_style.plugin_manifests import (
    current_three_creation_bindings_v3,
    current_three_descriptors_v3,
    current_three_manifests_v3,
)
from backend.execution_algos.vnpy_compat.facade_contracts import (
    VnpyFacadeAlgorithmBindingV2,
    VnpyFacadeConformanceAuthorityV2,
    VnpyFacadeConformanceSetV2,
    VnpyFacadeContractError,
    VnpyFacadeContractV1,
    VnpyFacadeSourceManifestV1,
)

from .plugin_contracts import GatewayCapabilityCatalogV1
from .plugin_registry import PluginCatalogRuntimeV2, PluginProcessBindingsV2, build_plugin_catalog_v2
from .vnpy_facade_characterization_runner import (
    build_vnpy_facade_characterization_authority_fresh_process_v2,
)


FULL_FIVE_ALGO_CODES_V1 = ("BEST_LIMIT_MINIQMT", "ICEBERG", "SNIPER_MINIQMT", "STOP", "TWAP_LITE_MINIQMT")


@dataclass(frozen=True)
class FullFiveCatalogAuthorityV1:
    """One strict full-five catalog plus its source conformance authority."""

    catalog_runtime: PluginCatalogRuntimeV2
    facade_contract: VnpyFacadeContractV1
    source_manifest: VnpyFacadeSourceManifestV1
    characterization_authority: VnpyFacadeCharacterizationAuthorityV2
    facade_algorithm_bindings: tuple[VnpyFacadeAlgorithmBindingV2, ...]
    conformance_set: VnpyFacadeConformanceSetV2
    conformance_authority: VnpyFacadeConformanceAuthorityV2


def _combine_process_bindings_v1() -> PluginProcessBindingsV2:
    current = current_three_process_bindings_v3().copy_bindings_v1()
    facade = k5_process_bindings_v1().copy_bindings_v1()
    overlap = sorted(set(current) & set(facade))
    if overlap:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_CONFORMANCE_RECEIPT_INVALID",
            "full-five process binding identities overlap",
            context={"overlap": overlap},
        )
    return PluginProcessBindingsV2({**current, **facade})


def _current_three_catalog_runtime_v1() -> PluginCatalogRuntimeV2:
    return build_plugin_catalog_v2(
        descriptors=current_three_descriptors_v3(),
        creation_bindings=current_three_creation_bindings_v3(),
        process_bindings=current_three_process_bindings_v3(),
        pinned_compatibility_receipts=build_current_three_compatibility_receipts_v1(),
    )


def _assert_exact_facade_bindings_v1(
    fresh_bindings: tuple[VnpyFacadeAlgorithmBindingV2, ...],
) -> tuple[VnpyFacadeAlgorithmBindingV2, ...]:
    fresh = tuple(item for item in fresh_bindings if item.algo_code in {"ICEBERG", "STOP"})
    literal = k5_facade_algorithm_bindings_v2()
    if tuple(item.algo_code for item in fresh) != ("ICEBERG", "STOP") or fresh != literal:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_BINDING_INVALID",
            "facade bindings differ from the fresh pinned source authority",
            context={
                "expected": [item.canonical_payload_v1() for item in fresh],
                "actual": [item.canonical_payload_v1() for item in literal],
            },
        )
    return literal


def build_full_five_catalog_authority_v1(
    *,
    gateway_catalog: GatewayCapabilityCatalogV1,
) -> FullFiveCatalogAuthorityV1:
    """Rebuild the exact five-plugin source authority or publish nothing.

    This is deliberately a fresh reconstruction rather than a cached K5
    object.  The returned facade receipts remain source-characterization facts;
    K6-D must perform its own V3 command materialization after this readback.
    """

    if not isinstance(gateway_catalog, GatewayCapabilityCatalogV1):
        raise TypeError("gateway_catalog must be GatewayCapabilityCatalogV1")
    gateway_catalog = GatewayCapabilityCatalogV1.model_validate(gateway_catalog.model_dump(mode="python"), strict=True)
    current_runtime = _current_three_catalog_runtime_v1()
    source_manifest = build_vnpy_facade_source_manifest_v1()
    facade_contract = build_vnpy_facade_contract_v1(
        compatibility_requirements=tuple(item.compatibility_requirement for item in current_three_manifests_v3())
    )
    requirements = build_vnpy_facade_characterization_requirements_v1(
        catalog_runtime=current_runtime,
        source_manifest=source_manifest,
    )
    characterization = build_vnpy_facade_characterization_authority_fresh_process_v2(
        source_manifest=source_manifest,
        facade_contract=facade_contract,
        requirements=requirements,
    )
    fresh_bindings = build_vnpy_facade_algorithm_bindings_v2(
        characterization_authority_v2=characterization,
        facade_contract=facade_contract,
        source_manifest=source_manifest,
    )
    facade_bindings = _assert_exact_facade_bindings_v1(fresh_bindings)
    full_runtime = build_plugin_catalog_v2(
        descriptors=(*current_three_descriptors_v3(), *k5_descriptors_v1()),
        creation_bindings=(*current_three_creation_bindings_v3(), *k5_creation_bindings_v1()),
        process_bindings=_combine_process_bindings_v1(),
        pinned_compatibility_receipts=(
            *build_current_three_compatibility_receipts_v1(),
            *(build_vnpy_compatibility_receipt_v1(manifest=item) for item in k5_manifests_v1()),
        ),
    )
    actual_algos = tuple(item.manifest.algo_code for item in full_runtime.snapshot.registration_descriptors)
    if actual_algos != FULL_FIVE_ALGO_CODES_V1:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_CONFORMANCE_RECEIPT_INVALID",
            "full-five catalog must contain the exact ordered algorithm set",
            context={"expected_algo_codes": list(FULL_FIVE_ALGO_CODES_V1), "actual_algo_codes": list(actual_algos)},
        )
    conformance = build_vnpy_facade_full_five_conformance_set_v2(
        catalog_runtime=full_runtime,
        gateway_catalog=gateway_catalog,
        facade_contract=facade_contract,
        source_manifest=source_manifest,
        characterization_authority_v2=characterization,
        algorithm_bindings_v2=fresh_bindings,
    )
    authority = validate_vnpy_facade_full_five_conformance_set_against_authority_v2(
        conformance_set=conformance,
        catalog_runtime=full_runtime,
        gateway_catalog=gateway_catalog,
        facade_contract=facade_contract,
        source_manifest=source_manifest,
        characterization_authority_v2=characterization,
    )
    return FullFiveCatalogAuthorityV1(
        catalog_runtime=full_runtime,
        facade_contract=facade_contract,
        source_manifest=source_manifest,
        characterization_authority=characterization,
        facade_algorithm_bindings=facade_bindings,
        conformance_set=conformance,
        conformance_authority=authority,
    )


__all__ = ["FULL_FIVE_ALGO_CODES_V1", "FullFiveCatalogAuthorityV1", "build_full_five_catalog_authority_v1"]
