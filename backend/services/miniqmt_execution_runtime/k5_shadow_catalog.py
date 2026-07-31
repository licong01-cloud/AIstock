"""The single K5 full-five shadow catalog composition root.

This module is intentionally not imported by a product root.  It rebuilds the
existing current-three catalog as an input authority, re-executes K4's pinned
five-algorithm characterization, then admits the two K5 facade adapters only
when their immutable code-owned bindings equal that fresh authority.
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
from backend.execution_algos.vnpy_style.plugin_manifests import (
    current_three_creation_bindings_v3,
    current_three_descriptors_v3,
    current_three_manifests_v3,
)
from backend.execution_algos.vnpy_style.plugin_factories import current_three_process_bindings_v3
from backend.services.miniqmt_execution_runtime.plugin_registry import (
    PluginCatalogRuntimeV2,
    PluginProcessBindingsV2,
    build_plugin_catalog_v2,
)
from backend.services.miniqmt_execution_runtime.vnpy_facade_characterization_runner import (
    build_vnpy_facade_characterization_authority_fresh_process_v2,
)

from backend.execution_algos.vnpy_compat.facade_contracts import (
    VnpyFacadeAlgorithmBindingV2,
    VnpyFacadeCommandAuthorityDispositionV1,
    VnpyFacadeConformanceAuthorityV2,
    VnpyFacadeContractError,
    VnpyFacadeContractV1,
    VnpyFacadeConformanceSetV2,
    VnpyFacadeCharacterizationRequirementV1,
    VnpyFacadeRegistrationDispositionV1,
    VnpyFacadeRuntimeBindingDispositionV1,
    VnpyFacadeSourceManifestV1,
)
from backend.services.miniqmt_execution_runtime.plugin_contracts import GatewayCapabilityCatalogV1
from backend.services.miniqmt_execution_runtime.plugin_canonical import json_safe_evidence_v1

_FULL_FIVE_ALGOS = ("BEST_LIMIT_MINIQMT", "ICEBERG", "SNIPER_MINIQMT", "STOP", "TWAP_LITE_MINIQMT")


@dataclass(frozen=True)
class K5ShadowCatalogRuntimeV1:
    """Non-product aggregate of the strict catalog and K4 source authority."""

    catalog_runtime: PluginCatalogRuntimeV2
    facade_contract: VnpyFacadeContractV1
    source_manifest: VnpyFacadeSourceManifestV1
    characterization_authority: VnpyFacadeCharacterizationAuthorityV2
    k5_algorithm_bindings: tuple[VnpyFacadeAlgorithmBindingV2, ...]
    conformance_set: VnpyFacadeConformanceSetV2
    conformance_authority: VnpyFacadeConformanceAuthorityV2


def _combine_process_bindings_v1() -> PluginProcessBindingsV2:
    current = current_three_process_bindings_v3().copy_bindings_v1()
    k5 = k5_process_bindings_v1().copy_bindings_v1()
    overlap = sorted(set(current) & set(k5))
    if overlap:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_CONFORMANCE_RECEIPT_INVALID",
            "K5 process binding identities overlap current-three bindings",
            context={"overlap": overlap},
        )
    return PluginProcessBindingsV2({**current, **k5})


def _current_three_catalog_runtime_v1() -> PluginCatalogRuntimeV2:
    return build_plugin_catalog_v2(
        descriptors=current_three_descriptors_v3(),
        creation_bindings=current_three_creation_bindings_v3(),
        process_bindings=current_three_process_bindings_v3(),
        pinned_compatibility_receipts=build_current_three_compatibility_receipts_v1(),
    )


def _assert_exact_k5_fresh_bindings_v1(
    fresh_bindings: tuple[VnpyFacadeAlgorithmBindingV2, ...],
) -> tuple[VnpyFacadeAlgorithmBindingV2, ...]:
    fresh = tuple(item for item in fresh_bindings if item.algo_code in {"ICEBERG", "STOP"})
    literal = k5_facade_algorithm_bindings_v2()
    if tuple(item.algo_code for item in fresh) != ("ICEBERG", "STOP") or fresh != literal:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_BINDING_INVALID",
            "K5 code-owned binding literals differ from the fresh pinned K4 authority",
            context={
                "expected": [item.canonical_payload_v1() for item in fresh],
                "actual": [item.canonical_payload_v1() for item in literal],
            },
        )
    return literal


def _assert_k5_characterization_bridge_v1(
    requirements: tuple[VnpyFacadeCharacterizationRequirementV1, ...],
    bindings: tuple[VnpyFacadeAlgorithmBindingV2, ...],
) -> None:
    if any(not isinstance(item, VnpyFacadeCharacterizationRequirementV1) for item in requirements):
        raise TypeError("K5 characterization requirements must use the exact K4 carrier")
    requirement_by_algo = {item.algo_code: item for item in requirements}
    binding_by_algo = {item.algo_code: item for item in bindings}
    for algo_code in ("ICEBERG", "STOP"):
        requirement = requirement_by_algo.get(algo_code)
        binding = binding_by_algo.get(algo_code)
        if (
            requirement is None
            or binding is None
            or requirement.registration_disposition is not VnpyFacadeRegistrationDispositionV1.CHARACTERIZATION_ONLY_K5
            or requirement.source_identity_sha256 != binding.source_identity_sha256
        ):
            raise VnpyFacadeContractError(
                "MINIQMT_VNPY_FACADE_CONFORMANCE_RECEIPT_INVALID",
                "K5 adapter bridge must retain the exact K4 characterization-only source disposition",
                context={"algo_code": algo_code},
            )


def build_k5_shadow_catalog_runtime_v1(
    *,
    gateway_catalog: GatewayCapabilityCatalogV1,
) -> K5ShadowCatalogRuntimeV1:
    """Build the exact five-plugin candidate or fail with no partial publication."""

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
    k5_bindings = _assert_exact_k5_fresh_bindings_v1(fresh_bindings)
    _assert_k5_characterization_bridge_v1(requirements, k5_bindings)
    k5_manifests = k5_manifests_v1()
    full_runtime = build_plugin_catalog_v2(
        descriptors=(*current_three_descriptors_v3(), *k5_descriptors_v1()),
        creation_bindings=(*current_three_creation_bindings_v3(), *k5_creation_bindings_v1()),
        process_bindings=_combine_process_bindings_v1(),
        pinned_compatibility_receipts=(
            *build_current_three_compatibility_receipts_v1(),
            *(build_vnpy_compatibility_receipt_v1(manifest=item) for item in k5_manifests),
        ),
    )
    actual_algos = tuple(item.manifest.algo_code for item in full_runtime.snapshot.registration_descriptors)
    if actual_algos != _FULL_FIVE_ALGOS:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_CONFORMANCE_RECEIPT_INVALID",
            "K5 candidate catalog must contain exactly the full-five algorithm set",
            context={"actual_algo_codes": list(actual_algos)},
        )
    conformance = build_vnpy_facade_full_five_conformance_set_v2(
        catalog_runtime=full_runtime,
        gateway_catalog=gateway_catalog,
        facade_contract=facade_contract,
        source_manifest=source_manifest,
        characterization_authority_v2=characterization,
        algorithm_bindings_v2=fresh_bindings,
    )
    for receipt in conformance.ordered_receipts:
        expected_disposition = (
            VnpyFacadeRuntimeBindingDispositionV1.FACADE_BACKED_ADAPTER
            if receipt.algo_code in {"ICEBERG", "STOP"}
            else VnpyFacadeRuntimeBindingDispositionV1.PURE_PLUGIN_SHADOW_CONFORMANCE
        )
        expected_command = (
            VnpyFacadeCommandAuthorityDispositionV1.SHADOW_ONLY_K2_V1
            if receipt.algo_code in {"ICEBERG", "STOP"}
            else VnpyFacadeCommandAuthorityDispositionV1.NOT_APPLICABLE_PURE_PLUGIN
        )
        if (
            receipt.runtime_binding_disposition is not expected_disposition
            or receipt.command_authority_disposition is not expected_command
        ):
            raise VnpyFacadeContractError(
                "MINIQMT_VNPY_FACADE_CONFORMANCE_RECEIPT_INVALID",
                "K5 full-five conformance disposition drifted",
                context={"algo_code": receipt.algo_code},
            )
    conformance_authority = validate_vnpy_facade_full_five_conformance_set_against_authority_v2(
        conformance_set=conformance,
        catalog_runtime=full_runtime,
        gateway_catalog=gateway_catalog,
        facade_contract=facade_contract,
        source_manifest=source_manifest,
        characterization_authority_v2=characterization,
    )
    return K5ShadowCatalogRuntimeV1(
        catalog_runtime=full_runtime,
        facade_contract=facade_contract,
        source_manifest=source_manifest,
        characterization_authority=characterization,
        k5_algorithm_bindings=k5_bindings,
        conformance_set=conformance,
        conformance_authority=conformance_authority,
    )


def readback_k5_shadow_conformance_set_v1(
    *,
    conformance_set: object,
    gateway_catalog: GatewayCapabilityCatalogV1,
) -> VnpyFacadeConformanceSetV2:
    """Strict K5 conformance readback through the same fresh full-five root."""

    try:
        supplied = VnpyFacadeConformanceSetV2.model_validate(conformance_set, strict=True)
    except (TypeError, ValueError) as exc:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_CONFORMANCE_AUTHORITY_INVALID",
            "K5 full-five conformance readback carrier is invalid",
            context={"error": json_safe_evidence_v1(exc)},
        ) from exc
    expected = build_k5_shadow_catalog_runtime_v1(gateway_catalog=gateway_catalog).conformance_set
    if supplied != expected:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_CONFORMANCE_AUTHORITY_INVALID",
            "K5 full-five conformance readback differs from fresh source/catalog/gateway authority",
            context={
                "expected_receipt_set_sha256": expected.receipt_set_sha256,
                "actual_receipt_set_sha256": supplied.receipt_set_sha256,
            },
        )
    return expected


__all__ = [
    "K5ShadowCatalogRuntimeV1",
    "build_k5_shadow_catalog_runtime_v1",
    "readback_k5_shadow_conformance_set_v1",
]
